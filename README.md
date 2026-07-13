# email-delivery-engine

A small backend system that models the operational behavior of an email sending
platform: message acceptance, idempotency, queues, workers, retries, backoff,
DLQs, throttling, route selection, bounce/deferral classification, IP pool
controls, warmup, anti-abuse controls, debugging tools, and observability.

> **This is not a production MTA.** It is a learning / proof-of-work system that
> models the *control plane* and *operational behavior* of email delivery
> infrastructure. Delivery is simulated — no real SMTP connections are opened.
> See [docs/tradeoffs.md](docs/tradeoffs.md) for exactly what is and isn't real.

## Why this exists

Email looks simple from the outside, but reliable delivery is a control problem:
queues, retries, throttling, provider reputation, abuse controls, observability,
and incident response all have to work together. This project models those
concerns end to end so the behavior — not just the API — can be reasoned about.

## Architecture

```text
 client ── POST /messages ──▶ Control Plane (TypeScript / Express)
                                  │  validate · idempotency · persist
                                  ▼
                             PostgreSQL  ◀─────────────┐
                                  │                    │ status, events,
                        XADD message_id                │ attempts, suppression
                                  ▼                    │
                          Redis Stream  ── XREADGROUP ─▶ Data Plane (Go worker)
                          (consumer group)               route/pool select
                                  ▲                       warmup + throttle
                                  │ re-enqueue            simulate send
                          deferred poller ◀──────────────  classify outcome
                                                           retry / backoff / DLQ
```

- **Control plane** (`control-plane/`, TypeScript): accepts messages, validates
  (Zod), enforces idempotency, persists to Postgres, enqueues onto Redis, and
  exposes customer/debug/metrics APIs.
- **Data plane** (`data-plane/`, Go): consumes from the Redis Streams consumer
  group, selects a route/IP pool, enforces warmup + rate limits, simulates the
  send, classifies the result, and schedules retries or dead-letters — writing a
  delivery event at every step.
- **Transport**: Redis Streams consumer group (at-least-once, with `XAUTOCLAIM`
  recovery of entries stranded by a crashed worker).

## Stack

- Control plane: TypeScript, Express, PostgreSQL, Drizzle ORM, Zod
- Data plane: Go, Redis Streams (`redis/go-redis`), `pgx`, `log/slog`
- Infrastructure: Docker Compose (Postgres + Redis)

## Run it

### Option A — everything in Docker

```bash
docker compose up --build
# Postgres + Redis start, `migrate` applies migrations and seeds reference data,
# then the control plane (:3000) and worker (:9100) come up.
```

### Option B — infra in Docker, apps locally (fastest for development)

```bash
# 1. Start Postgres + Redis
docker compose up -d postgres redis

# 2. Control plane: migrate, seed, run the API
cd control-plane
cp .env.example .env
npm install
npm run db:migrate      # apply the checked-in migration
npm run db:seed         # customers, providers, IP pools, routes, throttle rules
npm run dev             # API on http://localhost:3000

# 3. Data plane: run the worker (in another shell)
cd data-plane
DATABASE_URL=postgresql://ede:ede@localhost:5432/email_delivery_engine \
REDIS_URL=redis://localhost:6379 \
  go run ./cmd/worker    # metrics on http://localhost:9100/metrics
```

## API

| Method | Path                          | Purpose                                  |
| ------ | ----------------------------- | ---------------------------------------- |
| POST   | `/messages`                   | Accept a message (idempotent)            |
| GET    | `/messages/:id`               | Message status                           |
| GET    | `/messages/:id/events`        | Full delivery event timeline (debug)     |
| GET    | `/debug/customers/:id`        | Counts, recent events, throttles, state  |
| GET    | `/debug/domains/:domain`      | Volume + bounce rate for a domain        |
| GET    | `/debug/providers/:provider`  | Reputation, deferral stats, routes       |
| GET    | `/debug/ip-pools/:pool_id`    | Warmup day, today's volume, capacity     |
| GET    | `/metrics`                    | Prometheus metrics (control plane)       |

Worker metrics are served separately at `http://localhost:9100/metrics`.

### Send a message

```bash
curl -sX POST http://localhost:3000/messages \
  -H 'content-type: application/json' \
  -d '{
    "customer_id": "cus_trusted",
    "from": "hello@example.com",
    "to": "user@gmail.com",
    "subject": "Welcome",
    "html": "<p>Hello</p>",
    "type": "transactional",
    "idempotency_key": "msg-abc-123"
  }'
```

`type` is optional (`transactional` | `marketing`, default `transactional`). A
customer in the `blocked_from_marketing` state still receives transactional
mail but has marketing mail blocked.

Re-sending with the same `idempotency_key` returns the original message instead
of creating a duplicate.

### Forcing outcomes for demos

Delivery is simulated. The recipient's **local-part** carries a hint so you can
reproduce any outcome deterministically:

| Recipient                    | Simulated result                    |
| ---------------------------- | ----------------------------------- |
| `user@gmail.com`             | delivered (`250`)                   |
| `bounce@gmail.com`           | hard bounce (`550`) → suppressed    |
| `full@gmail.com`             | soft bounce (`452`) → retry         |
| `defer@gmail.com`            | deferral (`451`) → backoff retry    |
| `ratelimit@gmail.com`        | rate-limited (`421`) → backoff retry|
| `spam@gmail.com`             | policy rejection (`554`) → failed   |

Then inspect the timeline:

```bash
curl -s http://localhost:3000/messages/<id>/events | jq
```

## Core behaviors modeled

- **Idempotency** — `POST /messages` de-duplicates on `idempotency_key` inside a
  transaction (`SELECT … FOR UPDATE`).
- **Queue + worker** — Redis Streams consumer group; enqueue happens *after* the
  DB commit so a message is never queued before it exists.
- **Retry + backoff** — attempt schedule +30s / +2m / +10m with **full jitter**;
  exhausted messages are **dead-lettered** (`MAX_ATTEMPTS`, default 4).
- **Bounce/deferral classification** — rule-based mapping of SMTP responses to
  `hard_bounce` / `soft_bounce` / `provider_deferral` / `rate_limited` /
  `transient_failure` / `policy_rejection`, each with a retryable flag.
- **Throttling** — Redis fixed-window limits by customer / provider / domain /
  IP pool; over-limit messages are rescheduled, not sent.
- **Routing + IP pools** — dedicated pool if the customer has one, else a shared
  pool; **warmup** caps (50/100/250/500/1000 per day) enforced before sending.
- **Anti-abuse** — customers over a 24h hard-bounce threshold are moved to
  `limited` (slow down, don't hard-block); `blocked_from_sending` is rejected at
  accept; `blocked_from_marketing` blocks marketing but allows transactional.
- **Daily send cap** — a per-customer cap on send *attempts* per day; over-cap
  messages defer to the next window instead of failing.
- **Suppression** — hard bounces add the recipient to a per-customer list;
  future sends to them are skipped.
- **Observability** — structured JSON logs (`request_id`, `message_id`,
  `customer_id`, `provider`, `route_id`, `ip_pool_id`, `classification`,
  `attempt`) and Prometheus metrics on both planes; debug APIs that explain
  *why* any message reached its current state.

## Tests

```bash
# Data plane — unit tests (classifier, backoff schedule + jitter, warmup caps)
cd data-plane && go test ./...

# Control plane — unit tests (mailbox detection, request validation, snake_case)
cd control-plane && npm test

# Data plane — integration tests against a live Postgres + Redis (see the stack above)
cd data-plane && \
  TEST_DATABASE_URL=postgresql://ede:ede@localhost:5432/email_delivery_engine \
  TEST_REDIS_URL=redis://localhost:6379 \
  go test -tags=integration ./internal/worker/...
```

## Docs

- [docs/rfc-email-delivery-engine.md](docs/rfc-email-delivery-engine.md) — design RFC
- [docs/incident-simulation.md](docs/incident-simulation.md) — "Gmail starts 421-ing" runbook
- [docs/tradeoffs.md](docs/tradeoffs.md) — deliberate tradeoffs & what's not implemented

## What is intentionally not implemented (v0)

Real SMTP transport, real DNS/SPF/DKIM/DMARC, real IP addresses, feedback loops
(FBLs), MIME/bounce parsing, exactly-once delivery, and a DLQ replay UI. These
are discussed in [docs/tradeoffs.md](docs/tradeoffs.md).
