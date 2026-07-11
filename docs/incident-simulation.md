# Incident Simulation: Gmail starts returning 421 rate-limited responses

- **Author:** Suleyman Myradov
- **Status:** Proof-of-work / v0 (learning system, not production)
- **Last updated:** 2026-07-11

> This walks a realistic incident through the *actual* signals and controls this
> system implements. It is written the way I would want to run it on call: detect,
> assess impact, mitigate, monitor the backlog, and only then close. Every metric,
> endpoint, and control named below exists in the code.

---

## Scenario

Gmail begins returning `421 4.7.0 rate limited` (or `421 ... try again later`) on a
large fraction of delivery attempts. This is a *transient*, provider-side condition:
Gmail is asking us to slow down, not telling us the recipients are bad. Getting the
response to this class of event right is the whole point of an MTA — over-react and
you burn reputation and backlog; under-react and you keep hammering a provider that is
already unhappy.

In this system the condition is modelled by the provider's `reputation_state`:

- `degraded` → ~60% of otherwise-deliverable attempts come back `421 rate limited`,
  the rest deliver.
- `down` → every attempt comes back `421 ... try again later`.

## How the system behaves automatically

Before any human intervenes, the pipeline already does the right thing:

1. Each `421` is classified as `rate_limited` (or `provider_deferral`) — **retryable**.
2. The message goes to `deferred` with a jittered `next_attempt_at`
   (`+30s → +2m → +10m`, each uniform in `[base/2, base]`), and gets `deferred` +
   `retry_scheduled` events.
3. Full jitter spreads the retries so the whole Gmail backlog does not retry in
   lockstep and amplify the problem.
4. After `MAX_ATTEMPTS` (default 4) a message dead-letters instead of retrying forever.
5. The deferred poller re-injects each message when its backoff elapses.

So the system self-throttles Gmail traffic via backoff even with no operator action.
The operator's job is to confirm that, decide whether to slow further, and watch the
backlog drain.

## 1. Detection

The incident shows up as a coherent pattern across worker and control-plane metrics:

- `provider_rate_limited_total` — **rising sharply**. This is the most direct signal;
  it increments every time an attempt classifies as `rate_limited`. A rate-of-change
  alert here is the primary trigger.
- `messages_deferred_total` and `retry_scheduled_total` — **rising**, because every
  `421` defers and schedules a retry.
- `messages_delivered_total` — **flat or falling** for Gmail-bound traffic.
- `queue_depth` — **climbing**, as deferred messages get re-injected faster than they
  can succeed.
- `oldest_queued_message_age_seconds` (control plane) — **climbing**, the clearest
  "we have a growing backlog" signal.
- `delivery_attempts_total` — climbing (we are attempting a lot) while deliveries stay
  flat: the tell-tale attempts-up / deliveries-flat divergence.

Confirm and scope it with the provider debug endpoint:

```
GET /debug/providers/gmail
```

which returns the current `policy.reputation_state`, `message_counts_by_status` for
Gmail, and `deferral_stats` (`total` / `deferred` / `delivered`). A large `deferred`
count relative to `delivered` for `gmail` confirms the incident is Gmail-specific and
not systemic.

Structured worker logs corroborate: filter JSON logs for
`provider=gmail classification=rate_limited` and you get one line per affected attempt
with `message_id`, `customer_id`, `attempt`, and `smtp_code`.

## 2. Assessing customer impact

The question that matters: *who is being hurt, and how badly?*

- `GET /debug/domains/gmail.com` — volume and status breakdown for the recipient
  domain, plus its bounce-rate calculation and any domain-scoped throttle rules.
  (Use the actual recipient domain(s) affected; `gmail.com` is the common one.)
- `GET /debug/customers/:id` — for the biggest Gmail senders, shows
  `message_counts_by_status` (how many of their messages are stuck in `deferred`),
  their `sending_state`, customer-scoped throttle rules, recent events, and their
  suppression entries. This tells you whether one large sender is driving the backlog
  or the impact is spread across many.
- `GET /messages/:id/events` — for a specific complaint, the full timeline showing the
  `sending_attempted → deferred → retry_scheduled` loop with SMTP codes and
  `next_attempt_at`.

A key reassurance to state early in the incident channel: **`421` is retryable and
recipients are not being suppressed.** Only hard bounces add to the suppression list;
a rate-limit storm does not poison recipient addresses. When Gmail recovers, the
deferred backlog drains on its own.

## 3. Slowing sending (mitigation)

Backoff already reduces per-message pressure. If Gmail stays unhappy, add explicit
back-pressure with a provider-scoped throttle rule so total Gmail throughput drops
below the level that triggers the limiting:

```sql
-- Cap all Gmail-bound sends to 60/min across the fleet.
INSERT INTO throttle_rules (scope, scope_key, messages_per_minute)
VALUES ('provider', 'gmail', 60);
-- If a rule already exists, lower its messages_per_minute instead.
```

The worker checks throttle rules before every send. Once this rule is in place,
Gmail-bound messages that would exceed 60/min are **rescheduled (+1m) without consuming
a delivery attempt** — they are held, not failed, so nobody's message dead-letters just
because we chose to slow down. `messages_throttled_total` will rise, which is the
signal that mitigation is active.

Complementary levers:
- Lower an existing `provider:gmail` rule rather than deferring purely on backoff, to
  smooth traffic more aggressively.
- Nothing needs to change in the classifier or backoff logic — the retry cadence and
  jitter are already tuned to avoid retry storms.

Restraint matters here: the goal is to get *below* Gmail's threshold, not to zero.
Over-throttling grows the backlog unnecessarily.

## 4. How retries are scheduled

For each affected message the loop is:

```
attempt (421) → classify rate_limited → status=deferred, next_attempt_at set
              → deferred + retry_scheduled events written
              → deferred poller (every 15s) finds it once next_attempt_at passes
              → status=queued, XADD back onto the stream
              → re-processed (throttle check may hold it again without burning an attempt)
```

Delays escalate `+30s → +2m → +10m` with full jitter. A message that keeps getting
`421` across all four attempts dead-letters (`messages_dead_lettered_total`),
which during a long provider outage is expected and is exactly what the DLQ is for —
it prevents infinite retries and gives us a bounded set to replay later.

## 5. Monitoring the backlog

Watch these until they turn the corner:

- `queue_depth` — should stop climbing once mitigation matches Gmail's tolerance, then
  fall as the backlog drains.
- `oldest_queued_message_age_seconds` — the single best "are we catching up?" gauge.
- Count of messages in `deferred` for Gmail, via `GET /debug/providers/gmail`
  (`deferral_stats`) — should trend down.
- `provider_rate_limited_total` **rate** — should flatten as Gmail recovers / as our
  throttle takes effect.
- `messages_dead_lettered_total` — watch it; a rising DLQ count means messages are
  exhausting retries and will need replay after recovery.

## 6. When the incident can be closed

An incident here is **not** closed when Gmail recovers. Following the Resend principle
that an incident isn't closed until backlogs and DLQs are drained or intentionally
managed, the close criteria are all of:

1. **Provider healthy again** — `GET /debug/providers/gmail` shows
   `reputation_state = healthy`, and `provider_rate_limited_total` has stopped rising.
2. **Backlog drained** — `queue_depth` and `oldest_queued_message_age_seconds` back to
   baseline; the `deferred` count for Gmail is near zero.
3. **Mitigation rolled back** — any temporary `provider:gmail` throttle rule added for
   the incident is removed or restored to its normal value (otherwise you have silently
   left Gmail throttled forever).
4. **DLQ reconciled** — `messages_dead_lettered_total` for the incident window is
   accounted for: either the dead-lettered messages were replayed after recovery, or a
   deliberate decision was made that they are stale and should be dropped. (v0 has no
   DLQ replay UI — see `docs/tradeoffs.md` — so this step is currently manual database
   work, and that gap is itself a finding to note in the post-incident review.)

## How to reproduce locally

Two independent ways, matching the two things the simulator keys on.

### A. Flip Gmail's reputation state (fleet-wide, realistic)

```sql
-- Degrade Gmail: ~60% of attempts come back 421 rate-limited.
INSERT INTO provider_policies (provider, reputation_state)
VALUES ('gmail', 'degraded')
ON CONFLICT (provider) DO UPDATE SET reputation_state = 'degraded', updated_at = now();

-- Or take it fully down: every attempt is 421 try-again-later.
UPDATE provider_policies SET reputation_state = 'down', updated_at = now()
WHERE provider = 'gmail';
```

Then send normal traffic to Gmail recipients and watch `provider_rate_limited_total`,
`messages_deferred_total`, and `queue_depth` climb. Recover by setting
`reputation_state = 'healthy'` and watch the deferred backlog drain via the poller.

```bash
curl -X POST localhost:3000/messages -H 'content-type: application/json' -d '{
  "customer_id": "cus_123",
  "from": "hello@example.com",
  "to": "user@gmail.com",
  "subject": "Welcome",
  "html": "<p>Hello</p>",
  "idempotency_key": "demo-gmail-1"
}'
```

### B. Force a single message via a recipient hint (deterministic, for demos/tests)

The simulator honours hints in the recipient local-part, independent of provider
state, so you can force one message down the rate-limited path without touching the
provider policy:

```bash
# Local-part contains "ratelimit" -> always "421 4.7.0 rate limited"
curl -X POST localhost:3000/messages -H 'content-type: application/json' -d '{
  "customer_id": "cus_123",
  "from": "hello@example.com",
  "to": "ratelimit@gmail.com",
  "subject": "Test",
  "html": "<p>Hi</p>",
  "idempotency_key": "demo-ratelimit-1"
}'
```

Other useful hints for exercising adjacent outcomes: `defer@`/`greylist@` →
`451 try again later` (provider_deferral), `bounce@`/`unknown@` → `550 user unknown`
(hard bounce → suppression), `full@` → `452 mailbox full` (soft bounce), `spam@` →
`554 ... spam` (policy rejection), `tempfail@` → `451 temporary failure`.

Inspect the resulting behaviour with `GET /messages/:id/events` to see the
`sending_attempted → deferred → retry_scheduled` timeline, and
`GET /debug/providers/gmail` for the aggregate picture.
