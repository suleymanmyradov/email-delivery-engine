# Tradeoffs and Production Deltas

- **Author:** Suleyman Myradov
- **Status:** Proof-of-work / v0 (learning system, not production)
- **Last updated:** 2026-07-11

> Every choice below was made to keep v0 small, deterministic, and locally runnable
> while still exercising the operational logic I care about. For each one I try to be
> honest about what it costs and what I would do differently for real traffic.

---

## Simulated SMTP instead of real SMTP

**What v0 does.** There are no sockets. `internal/simulate` produces a synthetic
SMTP-style response string driven by (1) deterministic hints in the recipient
local-part (`ratelimit@`, `bounce@`, `full@`, `defer@`, `spam@`, `tempfail@`), and
(2) the provider's `reputation_state` (`healthy` delivers, `degraded` returns ~60%
`421`, `down` always `421`). That string is classified exactly as a real response
would be.

**Why.** The genuinely interesting parts of an MTA for this role are *routing,
throttling, classification, retries/backoff, warmup, and anti-abuse* — the control
logic. Real SMTP would add MX resolution, TLS, connection pooling, per-IP concurrency,
and dependence on live provider relationships, none of which I can run locally or
reproduce deterministically. Simulation lets me exercise the entire pipeline, and the
incident scenario, on demand and in tests.

**Cost / what's missing.** No real deliverability signal. No TLS negotiation, no
pipelining, no connection reuse, no per-IP concurrency limits, no handling of
multi-line SMTP replies or connection-level failures (timeouts, resets, greylisting at
connect time). "Reputation" is a column, not a real IP's standing.

**Production.** Replace the simulator with a real SMTP client behind the same
`Simulate(...) DeliveryResult` seam: MX lookup with caching, TLS, connection pools
keyed by (IP, provider) with concurrency caps, real response parsing into the existing
classifier, and per-connection error handling that maps to the same classifications.
The classifier's rule table would grow into provider-specific rule sets keyed on
enhanced status codes.

## Redis Streams for transport

**What v0 does.** One stream `messages`, one consumer group `worker`. Control plane
`XADD`s `{message_id}` after commit; worker `XREADGROUP`/`XACK`; stranded entries
reclaimed with `XAUTOCLAIM`. At-least-once with a Pending Entries List and crash
recovery.

**Why Redis Streams over alternatives.**
- It gives the three things I actually needed — durability, one-owner-per-entry
  (consumer group), and crash recovery (PEL + `XAUTOCLAIM`) — in a dependency I can
  run in one Docker Compose line.
- **NATS (core)** is fire-and-forget; I'd need JetStream for durability, which is
  heavier to operate for v0. NATS is excellent and would be a reasonable choice.
- **SQS** is a great managed fit (visibility timeout ≈ PEL, redrive policy ≈ built-in
  DLQ) but is not local/self-contained and ties the demo to AWS.
- **Kafka** is the right tool at high volume (partitioned ordering, replay, huge
  throughput) but is heavy operationally and overkill for a proof-of-work v0.

**Limits of the Streams choice.**
- **No partitioning / ordering guarantees** across consumers; fine here because
  per-message ordering doesn't matter, but there's no per-tenant fairness — one large
  customer can head-of-line-block others on the single stream.
- **PEL growth**: entries that repeatedly error stay pending and are reclaimed forever
  unless something caps redelivery. v0 relies on the message reaching a terminal state
  (or dead-lettering) to stop; a poison entry that always errors *before* the pipeline
  can dead-letter it would be reclaimed indefinitely. A max-delivery cap on reclaim is
  a needed hardening.
- **No native DLQ**: dead-lettering is a Postgres status, not a separate stream.
- **Memory-bound**: the whole stream lives in Redis memory; no cheap long retention.

**Production.** For high volume, Kafka (or SQS in an AWS shop) with partitioning by
customer/domain for fairness, a real dead-letter topic/queue, and a redelivery cap.
The plane boundary (enqueue-a-message-ID after commit) stays identical, so swapping the
transport is a contained change behind the `queue` package.

## Enqueue-after-commit, no outbox (at-least-once + idempotency)

**What v0 does.** The control plane commits the DB transaction (message + `accepted` +
`queued` events), then `XADD`s. There is no shared transaction between Postgres and
Redis. If the `XADD` fails, the message is still durably `queued` and the deferred
poller re-injects it.

**Cost.** There is a window where a message is committed but not yet enqueued; it
relies on the poller as a backstop, adding latency in that (rare) case. It is not a
true transactional outbox.

**At-least-once implications.** Because Streams redelivers on crash, the worker must
tolerate re-processing. It does: the pipeline skips messages already in a terminal
state, so a re-delivered `delivered`/`bounced`/etc. message is just acked. The narrow
correctness gap is a crash *after* a (real) provider accept but *before* persisting the
outcome and acking — the message would be re-attempted and, with real SMTP, could send
twice. This is the classic at-least-once-vs-exactly-once problem; v0 does not solve it.

**Production.** A transactional **outbox** table written in the same DB transaction,
drained by a relay process, removes the commit/enqueue window. For duplicate-send
protection with real SMTP, record a per-attempt intent (message_id + attempt) before
opening the connection and treat a found-but-unfinished attempt as "verify before
resending," accepting that true exactly-once is impossible and aiming for
effectively-once.

## `FOR UPDATE` idempotency and message locking

**What v0 does.** Client idempotency uses
`SELECT ... FROM idempotency_keys WHERE key = $1 FOR UPDATE` inside the transaction, so
concurrent requests with the same key serialize on the row and only one message is
created; duplicates return the existing message with `200`. The worker's
`GetMessageForProcessing` similarly guards against two workers grabbing the same
message.

**Cost.** Row-level locking is correct but serializes hot keys; a client hammering one
idempotency key gets serialized (acceptable — that's the point). The message-processing
lock assumes the standard `FOR UPDATE SKIP LOCKED` semantics to let concurrent workers
pass over locked rows rather than block.

**Production.** Keep it — `FOR UPDATE`/`SKIP LOCKED` is the right, boring tool. Add a
TTL/cleanup for old idempotency keys, and index/partition the keys table for volume.

## Fixed-window throttle vs sliding-window / token-bucket

**What v0 does.** `throttle:<scope>:<scopeKey>:<minute_bucket>` — `INCR` + 120s TTL.
Allowed while `count <= perMinute` within the current wall-clock minute. The counter is
incremented whether or not the send is allowed.

**Costs.**
- **Boundary burst**: a fixed window allows up to `2 × perMinute` across a window
  boundary (full at the end of one minute, full again at the start of the next).
- **Small over-count under concurrency**: because throttled callers still `INCR`, and
  because multiple workers increment the same key, the counter can slightly exceed the
  true send count. This is called out in the code and accepted for v0 — erring toward
  *under*-sending during throttling is the safe direction.

**Production.** A sliding-window-log or a token-bucket (leaky-bucket) limiter for
smooth rates without boundary bursts, implemented as a single atomic Redis **Lua
script** (or Redis functions) so check-and-increment is race-free across many workers.
Token bucket also naturally models burst allowance, which providers effectively grant.

## TypeScript control plane, Go data plane

**Why TypeScript for the control plane.** The API layer is I/O-bound request/response
work — validation, idempotency, persistence, enqueue. TypeScript + Zod gives fast,
expressive request validation and schema-typed DB access (Drizzle), and it matches the
kind of developer-facing API surface this plane represents. Iteration speed matters
most here.

**Why Go for the worker.** The data plane is long-running, concurrent, and
latency/throughput-sensitive: many goroutines (consumer, deferred poller, abuse sweep,
metrics), structured concurrency with `context` cancellation for graceful shutdown, and
predictable performance without GC pauses dominating. Go's concurrency primitives and
`log/slog` structured logging fit worker workloads well, and a compiled static binary
is easy to run and deploy.

**Cost.** Two languages means two toolchains, two sets of models, and a serialization
boundary. The shared contract is deliberately tiny (a message ID on the stream + the
Postgres schema) to keep that boundary cheap.

**Production.** Same split. The two planes scale, deploy, and fail independently, which
is the point.

## Single-worker assumptions and scaling to many consumers

**What v0 assumes.** One worker instance. The consumer-group protocol *supports* many
consumers (that's why it exists), but several components were written and reasoned about
for a single worker:

- **Throttle over-count** (above) worsens with more workers hitting the same key
  concurrently — needs the atomic Lua limiter before scaling out.
- **Warmup / daily-cap accounting** uses `CountSendsToday(pool)` read-then-check, which
  is racy across workers (two workers can both read `sent < cap` and both send). Needs
  atomic reservation (DB counter with a conditional update, or a Redis counter) per pool.
- **Abuse sweep** runs on a 60s ticker *inside the worker*; with N workers it runs N
  times. It's idempotent (setting a customer to `limited` is safe to repeat) so it's
  correct but wasteful — should be a single leader-elected or externally-scheduled job.
- **Deferred poller** likewise runs per-worker; `MarkQueued` + enqueue could double-
  inject a message if two pollers grab the same row. Needs `SKIP LOCKED` claiming or a
  single poller.

**Production.** Run many worker replicas as distinct consumers in the group (Redis
distributes entries across them). Before doing so: make throttle and warmup accounting
atomic, and move the sweep and poller to leader-elected / externally-scheduled
singletons (or make their DB claims race-safe). Partition the stream for per-tenant
fairness.

## Intentionally NOT implemented

Called out plainly so the scope is honest:

- **Real DNS / SPF / DKIM / DMARC / reverse DNS.** No message signing, no auth checks,
  no alignment. "Reputation" is a manual column.
- **Real IP addresses.** IP "pools" are database rows with warmup dates; there are no
  actual sending IPs, PTR records, or per-IP reputation.
- **Feedback loops (FBLs) / complaint handling.** No ARF ingestion, no complaint-driven
  suppression.
- **Real bounce parsing.** Classification is substring matching on a synthetic string,
  not MIME/DSN parsing of real bounce messages.
- **Exactly-once delivery.** At-least-once with idempotent processing; the duplicate-
  send window under crash is not closed (see above).
- **Per-message priority / tenant fairness.** One stream, FIFO-ish; no priority lanes.
- **DLQ replay UI/API.** Dead-lettered is a terminal status. There is no operator tool
  to inspect, bulk-replay, or intentionally discard the DLQ — closing an incident
  cleanly currently needs manual database work. This is the highest-value next piece of
  operational tooling.
- **Automatic provider reputation detection.** `reputation_state` is set by hand;
  nothing yet watches deferral rates and flips a provider to `degraded`/`down`
  automatically.
- **AuthN/authZ, multi-tenancy isolation, quotas beyond the daily cap.** Out of scope
  for v0's operational focus.
