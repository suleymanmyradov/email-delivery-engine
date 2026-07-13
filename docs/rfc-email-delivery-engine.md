# RFC: email-delivery-engine

- **Author:** Suleyman Myradov
- **Status:** Proof-of-work / v0 (learning system, not production)
- **Last updated:** 2026-07-11

> This is a proof-of-work project, not a production MTA. It models the control-plane
> and operational behaviour of an email sending platform so I can reason about the
> problems Core Sending works on every day: routing, retries, backoff, throttling,
> classification, warmup, anti-abuse, and debuggability. Where v0 fakes something
> (most importantly, real SMTP), the RFC says so explicitly.

---

# Purpose

Build a small, working system that demonstrates understanding of how a high-volume
email sending platform behaves operationally — not by shipping a real MTA, but by
faithfully modelling the *control problems* behind reliable delivery:

- Accepting and de-duplicating message requests safely (idempotency).
- Handing work from an API to asynchronous workers durably.
- Selecting a route / IP pool per message and respecting warmup and daily caps.
- Enforcing per-customer / per-domain / per-provider / per-IP-pool send rates.
- Simulating provider responses, classifying them, and retrying with backoff + jitter.
- Dead-lettering when retries are exhausted.
- Protecting good senders with anti-abuse controls that slow rather than block by default.
- Making every decision explainable through events, metrics, and debug APIs.

The success criterion is not throughput. It is: *for any message, I can explain
exactly what happened and why, and the system behaves sanely under provider
degradation.*

# Background

Sending email reliably at scale is less about "send a message" and more about
"control sending behaviour safely over time." Mailbox providers throttle, defer,
greylist, and reject; recipients bounce; bad senders damage shared reputation.
A naive sender that retries hard on every failure creates retry storms, burns IP
reputation, and hurts every other customer on a shared pool.

Real platforms split responsibilities into two planes:

- A **control plane** that owns the synchronous, customer-facing contract: validate,
  authorize, de-duplicate, persist, and enqueue.
- A **data plane** that owns the asynchronous, provider-facing work: route, throttle,
  attempt delivery, classify, retry, and report.

This project mirrors that split so the interesting operational logic lives where it
belongs and can evolve independently.

# Proposal

Two planes connected by a durable queue.

```
Client
  │  POST /messages (idempotency_key)
  ▼
Control plane  ── TypeScript + Express + Zod + Postgres (Drizzle)
  │  DB transaction commits, THEN XADD {message_id}
  ▼
Redis Stream "messages" (consumer group "worker")
  │  XREADGROUP / XACK / XAUTOCLAIM
  ▼
Data plane  ── Go worker: route → warmup/cap → throttle → send(sim) → classify → retry/DLQ
  │
  ▼
Postgres (messages, message_events, delivery_attempts, suppression_list, …)
  │
  ▼
Prometheus /metrics + /debug/* APIs (explain any message/customer/domain/provider/pool)
```

**Control plane (TypeScript / Express / Postgres / Drizzle / Zod).** Owns the API
contract and the source-of-truth database. Validates input with Zod, enforces
idempotency, checks the customer exists and is allowed to send, persists the message
plus its `accepted` and `queued` events in one transaction, and enqueues the message
ID onto the stream *after* the transaction commits.

**Data plane (Go worker).** Consumes message IDs from the stream, runs each message
through the delivery pipeline, and writes an event at every decision point so the
timeline is fully reconstructable. It also runs three background loops: a deferred
poller that re-injects messages whose backoff has elapsed, an abuse sweep that limits
senders with high bounce rates, and a metrics updater.

**Transport (Redis Streams).** A consumer group gives at-least-once delivery: each
entry is handed to exactly one consumer and stays in the Pending Entries List (PEL)
until acked. Crashed consumers' entries are reclaimed via `XAUTOCLAIM`. This is the
smallest thing that gives durability + crash recovery without standing up Kafka.

# Technical Details

## The plane boundary and the Redis Streams contract

The contract between planes is a single field on a Redis Stream entry:
`{ message_id }`. The message body itself never travels through the queue — it lives
in Postgres, and the worker re-loads it by ID. This keeps the queue tiny and makes
the database the single source of truth.

The ordering rule is load-bearing:

1. Control plane opens a transaction, writes the message + `accepted` + `queued`
   events, and commits.
2. **Only after commit** does it `XADD` the message ID onto the stream.

Enqueue is never done inside the transaction. If we enqueued first and the
transaction rolled back, the worker would chase a message that does not exist. If the
`XADD` fails *after* commit, the message is still durably `queued` in Postgres and the
worker's deferred poller will pick it up on its next tick — so a Redis blip degrades
latency, not correctness. (The poller today re-injects messages in `deferred` state
whose `next_attempt_at` has passed; the same mechanism is the safety net for a failed
initial enqueue.)

The worker side:

- `XREADGROUP` with group `worker` reads new entries (`>`), batched, with a blocking
  window.
- On successful processing it `XACK`s the entry, removing it from the PEL.
- On error it leaves the entry **unacked** so a later `XAUTOCLAIM` reclaims and retries it.
- On startup and on every idle tick it runs `XAUTOCLAIM` for entries idle longer than
  a minute, recovering anything stranded by a crashed consumer.

This is **at-least-once**: a worker can crash after simulating a send but before
acking, and the entry will be re-processed. The pipeline is therefore written to be
idempotent against re-delivery (see below).

## Idempotency

Two layers protect against duplicates:

1. **Client-facing idempotency (control plane).** `POST /messages` requires an
   `idempotency_key`. Inside the transaction the control plane does
   `SELECT message_id FROM idempotency_keys WHERE key = $1 FOR UPDATE`. If a row
   exists, it rolls back and returns the existing message with `200`. If not, it
   inserts the message and the key together, so concurrent requests with the same key
   serialize on the row lock and only one message is ever created. Duplicate submits
   are safe and return the same message.

2. **Re-delivery idempotency (worker).** Because the queue is at-least-once, the first
   thing the pipeline does is load the message and skip it if it is already in a
   terminal state (`delivered`, `bounced`, `failed`, `dead_lettered`, `suppressed`,
   `blocked`). A re-delivered terminal message is simply acked. Re-delivery of a
   non-terminal message re-runs the pipeline, which is the intended behaviour.

## Delivery state machine

`messages.status` moves through these states (Postgres enum):

| Status          | Meaning                                                        | Terminal? |
|-----------------|----------------------------------------------------------------|-----------|
| `queued`        | Accepted and enqueued; waiting for a worker.                   | no        |
| `sending`       | Worker has begun an attempt (attempt counter incremented).    | no        |
| `deferred`      | Will be retried later; `next_attempt_at` is set.              | no        |
| `delivered`     | Provider accepted the message.                                | yes       |
| `bounced`       | Hard bounce; recipient added to suppression list.            | yes       |
| `failed`        | Policy rejection or unrecoverable setup error; no retry.      | yes       |
| `dead_lettered` | Retries exhausted (`MAX_ATTEMPTS` reached).                  | yes       |
| `suppressed`    | Recipient was already on the suppression list; never sent.   | yes       |
| `throttled`     | Reserved status for rate/capacity holds (see note).          | no        |
| `blocked`       | Customer is `blocked_from_sending`; abuse hard block.        | yes       |

Note on `throttled`: when a message is held for warmup/daily-cap or rate-limit
reasons, the worker currently sets status `deferred` with a `next_attempt_at` and
writes a `throttled` **event** (capacity holds do *not* consume a delivery attempt).
The `throttled` status exists in the schema for clarity/future use; the operational
signal today is the `throttled` event plus `messages_throttled_total`.

## Worker pipeline (order matters)

For each message the worker runs, in this exact order:

1. **Load message.** Skip (ack) if not found or already terminal.
2. **Load customer.** Missing customer → `failed`.
3. **Abuse hard block.** `sending_state == blocked_from_sending` → status `blocked`,
   event `blocked_by_abuse_control`, increment `abuse_blocks_total`.
4. **Marketing block.** `sending_state == blocked_from_marketing` **and**
   `type == marketing` → status `blocked`. Transactional mail from the same customer
   still flows — the block is scoped to message type.
5. **Suppression check.** Recipient on the customer's suppression list → status
   `suppressed`, never attempt a send.
6. **Route / IP-pool selection.** Choose dedicated pool if the customer has one with a
   route for the provider; else the first shared-pool route. No enabled route →
   `failed`.
7. **Customer daily send cap.** If the customer has made ≥ `daily_send_cap` send
   attempts today, reschedule (`deferred`, +1h) **without consuming an attempt**.
8. **Warmup / pool cap.** If the chosen pool is over its warmup/daily cap, reschedule
   (`deferred`, +1h) **without consuming an attempt** — this is capacity, not failure.
9. **Rate-limit throttles.** Check customer / provider / domain / IP-pool scopes; if
   any is over limit, reschedule (`deferred`, +1m) **without consuming an attempt**.
10. **Attempt send.** Mark `sending`, write `sending_attempted` event, increment
    attempt, increment `delivery_attempts_total`.
11. **Simulate delivery** against the provider's reputation state.
12. **Persist the attempt** (`delivery_attempts` row with SMTP code + raw response).
13. **Classify** the response.
14. **Act:** deliver / bounce+suppress / fail / defer+schedule-retry / dead-letter.

The distinction in steps 7–9 versus 10+ is deliberate: **capacity limits reschedule
without burning a retry; delivery failures burn a retry.** A message throttled 50
times has still made zero delivery attempts. Daily/warmup caps count actual send
attempts (from `delivery_attempts`), so a cap of _N_ permits _N_ sends per day rather
than blocking a customer that merely queued more than _N_ messages.

## Retry, backoff, and dead-lettering

Retryable outcomes are deferred with an exponential-ish schedule keyed on the number
of attempts already made:

- after attempt 1 → +30s
- after attempt 2 → +2m
- after attempt 3 → +10m
- `MAX_ATTEMPTS` (default 4) reached → **dead-letter** (`dead_lettered`,
  `messages_dead_lettered_total`).

**Full jitter** is applied to every delay: the actual delay is uniform in
`[base/2, base]`. This spreads out messages that were all deferred at the same instant
(e.g. when a provider goes degraded) so their retries do not fire in lockstep and
create a synchronized retry storm.

A deferred message gets `next_attempt_at = now + jittered_delay`, a `deferred` event,
and a `retry_scheduled` event. The worker's deferred poller wakes every 15s, finds
messages whose `next_attempt_at` has passed, flips them back to `queued`, and
`XADD`s them onto the stream for re-processing.

## Classification

Classification is a small, ordered, rule-based substring matcher over the SMTP-style
response string — deliberately simple, standing in for the large provider-specific
rule sets a real MTA maintains against RFC 3463 enhanced status codes. Outcomes:

| Classification      | Retryable | Action                                    |
|---------------------|-----------|-------------------------------------------|
| `delivered`         | –         | status `delivered`                        |
| `hard_bounce`       | no        | status `bounced` + add to suppression list |
| `soft_bounce`       | yes       | defer + retry                             |
| `provider_deferral` | yes       | defer + retry                             |
| `rate_limited`      | yes       | defer + retry (`provider_rate_limited_total`) |
| `transient_failure` | yes       | defer + retry                             |
| `policy_rejection`  | no        | status `failed`                           |

Rule order is significant: more specific phrases precede general ones (e.g.
`"rate limited"` is matched before `"try again later"`), and an empty / 2xx-looking
response defaults to `delivered`.

## Throttling

A Redis fixed-window counter enforced by the **worker**, not the control plane. Key
scheme `throttle:<scope>:<scopeKey>:<minute_bucket>`; each hit `INCR`s the key and
sets a 120s TTL so buckets self-clean. Scopes: `customer`, `provider`, `domain`,
`ip_pool`.

Throttling is enforced in the data plane on purpose: only the worker knows the chosen
route/pool/provider for a message, and only the worker can *reschedule* a throttled
message. The control plane accepting a message says nothing about when it is safe to
send it.

## Routing and warmup

Route selection prefers a **dedicated** IP pool if the customer has one and it has a
route for the provider; otherwise it uses the first **shared** pool route for the
provider. No enabled route → the message fails (a misconfiguration, surfaced loudly).

IP warmup schedule (per pool, by day since `warmup_started_at`): day 1–5 =
50 / 100 / 250 / 500 / 1000, unlimited after day 5. An explicit `daily_limit` on the
pool is also applied, and the **stricter of warmup-vs-daily-limit wins**. Reputation-
aware routing (reducing/rerouting when a provider is degraded) is modelled in the
simulator via reputation state rather than in the selector, keeping selection
deterministic for v0.

## Anti-abuse

Philosophy borrowed from Resend's stance: protect good senders, minimize friction.
Four mechanisms, escalating in severity:

- **Hard block:** a customer in `blocked_from_sending` is rejected at accept time
  (control plane, `403`) and again defensively in the worker (status `blocked`).
- **Marketing block:** a customer in `blocked_from_marketing` still sends
  transactional mail; only `type == marketing` messages are blocked. This lets an
  abusive marketing pattern be contained without breaking password resets or receipts.
- **Daily send cap:** each customer has a `daily_send_cap`; once they reach it, further
  messages defer to the next window rather than fail — a ceiling, not a rejection.
- **Soft limit:** a periodic (60s) abuse sweep computes each `trusted`/`new`
  customer's 24h hard-bounce rate; if it exceeds 20% the customer is moved to
  `limited` (slow down) rather than blocked outright.

## Observability

- **Structured logs (worker):** Go `log/slog` JSON with `message_id`, `customer_id`,
  `provider`, `recipient_domain`, `route_id`, `ip_pool_id`, `attempt`, `smtp_code`,
  and `classification` attached progressively as the pipeline learns them.
- **Worker metrics** (`/metrics` on `:9100`): `messages_delivered_total`,
  `messages_bounced_total`, `messages_deferred_total`, `messages_dead_lettered_total`,
  `messages_throttled_total`, `messages_suppressed_total`, `delivery_attempts_total`,
  `retry_scheduled_total`, `provider_rate_limited_total`, `abuse_blocks_total`,
  `abuse_limited_customers_total`, `worker_process_errors_total`, and a `queue_depth`
  gauge.
- **Control-plane metrics:** `messages_accepted_total`, `messages_queued_total`,
  `abuse_blocks_total`, `queue_depth`, `oldest_queued_message_age_seconds`.
- **Debug APIs:** `/debug/customers/:id`, `/debug/domains/:domain`,
  `/debug/providers/:provider`, `/debug/ip-pools/:pool_id`, plus per-message
  `GET /messages/:id/events` for the full timeline.

## Graceful shutdown

The worker uses `signal.NotifyContext` for SIGINT/SIGTERM. On signal it cancels the
root context (stopping the consumer, poller, sweep, and metrics loops), shuts down the
metrics HTTP server with a timeout, and waits on all goroutines before exiting.
In-flight entries that were not acked stay in the PEL and are reclaimed by the next
worker instance.

# Implementation Plan

Built in the phases from the project plan, each independently demoable:

1. **Control plane v0** — Express API, Postgres schema (Drizzle), `POST /messages`
   with idempotency, `GET /messages/:id`.
2. **Queue + worker** — Redis Streams consumer group, enqueue-after-commit, Go worker
   consuming and writing events.
3. **Delivery outcomes** — simulator, classifier, retry/backoff with jitter, DLQ.
4. **Throttling + routing** — fixed-window throttles across four scopes, shared/
   dedicated pools, warmup + daily caps, route selection.
5. **Anti-abuse + suppression** — sending states, bounce-rate-based limiting,
   suppression list, abuse events.
6. **Observability + debugging** — Prometheus metrics both planes, structured logs,
   debug endpoints, and these docs.

# General Questions

**Why two planes instead of one service?** The synchronous API contract and the
asynchronous provider-facing work have very different failure modes, latency
profiles, and scaling needs. Splitting them lets the API stay fast and simple while
the delivery logic (the hard, stateful part) evolves and scales independently. It also
matches the mental model of the role.

**Why Redis Streams and not a "real" broker?** For v0 it is the smallest thing that
provides durability, a consumer group (one-owner-per-entry), a Pending Entries List,
and crash recovery via `XAUTOCLAIM`. It is trivial to run in Docker Compose. Its
limits (and when I would reach for NATS/SQS/Kafka) are covered in `docs/tradeoffs.md`.

**Why enqueue after commit and not transactionally?** There is no shared transaction
between Postgres and Redis. Committing first and treating the DB as the source of
truth — with the deferred poller as a backstop — gives at-least-once semantics without
a distributed transaction or an outbox table. An outbox is the natural next step
(also in tradeoffs).

**Why is throttling in the worker, not the API?** Only the worker knows the chosen
route/pool/provider, and only the worker can reschedule. Rejecting at the API would
either drop mail or require the client to retry, both worse than deferring.

**Why simulate SMTP?** So the whole system — routing, throttling, classification,
retries, warmup, abuse — can be exercised deterministically and locally without real
domains, IPs, or provider relationships. See tradeoffs for the honest cost of this.

# Open Questions

Things a production system needs that v0 deliberately does not have:

- **Real SMTP delivery.** No sockets, no MX lookups, no TLS, no connection pooling or
  per-IP concurrency control. The simulator stands in for all provider behaviour.
- **DKIM signing, SPF alignment, DMARC, reverse DNS.** No message signing or
  authentication is performed; "reputation" is a database column, not a real IP's
  standing with a provider.
- **Feedback loops (FBLs) and complaint handling.** No ARF ingestion, no complaint-
  driven suppression. Real deliverability depends heavily on these.
- **Real bounce parsing.** Classification is substring matching on a synthetic string,
  not parsing of real MIME/DSN bounce messages and enhanced status codes.
- **Exactly-once concerns.** The system is at-least-once with idempotent processing.
  A message could, in a narrow window, be simulated-sent twice if a worker crashes
  after the provider "accepts" but before persisting/acking. With real SMTP this maps
  to the genuinely hard problem of duplicate sends on retry; it needs careful attempt
  de-duplication.
- **Multi-worker scaling.** The consumer-group protocol supports many consumers, but
  the throttle counters, warmup accounting, and abuse sweep have not been validated
  under concurrent workers (fixed-window over-count, sweep running on every worker,
  hot-key contention). See tradeoffs.
- **DLQ replay tooling.** Dead-lettered messages are a terminal status with no operator
  UI or API to inspect, bulk-replay, or intentionally discard them. Closing an
  incident cleanly (the Resend principle: an incident isn't closed until backlogs/DLQs
  are drained or intentionally managed) currently requires manual database work.
- **Per-message priority / fairness.** All messages share one stream; a large customer
  can head-of-line-block others. No priority lanes or per-tenant fairness.
- **Provider reputation automation.** Reputation state is set manually; nothing yet
  observes deferral rates and flips a provider to `degraded`/`down` automatically.
