import { test } from "node:test";
import assert from "node:assert/strict";
import { snakeifyKeys } from "./snakeCase";

test("recursively converts camelCase keys to snake_case", () => {
  const input = {
    mailboxProvider: "gmail",
    nextAttemptAt: "2026-01-01",
    nested: { routeId: 1, ipPoolId: 2 },
    list: [{ smtpCode: 421 }, { eventType: "deferred" }],
  };
  assert.deepEqual(snakeifyKeys(input), {
    mailbox_provider: "gmail",
    next_attempt_at: "2026-01-01",
    nested: { route_id: 1, ip_pool_id: 2 },
    list: [{ smtp_code: 421 }, { event_type: "deferred" }],
  });
});

test("leaves Date instances and primitives untouched", () => {
  const d = new Date("2026-07-11T00:00:00.000Z");
  const out = snakeifyKeys({
    createdAt: d,
    attemptCount: 3,
    isActive: true,
    routeId: null,
  }) as Record<string, unknown>;

  assert.ok(out.created_at instanceof Date);
  assert.equal((out.created_at as Date).toISOString(), d.toISOString());
  assert.equal(out.attempt_count, 3);
  assert.equal(out.is_active, true);
  assert.equal(out.route_id, null);
});

test("passes through already snake_case keys unchanged", () => {
  assert.deepEqual(snakeifyKeys({ event_type: "queued" }), {
    event_type: "queued",
  });
});
