import { test } from "node:test";
import assert from "node:assert/strict";
import { createMessageSchema } from "./schemas";

const valid = {
  customer_id: "cus_1",
  from: "hello@example.com",
  to: "user@gmail.com",
  subject: "Hi",
  html: "<p>x</p>",
  idempotency_key: "k-1",
};

test("accepts a valid payload and defaults type to transactional", () => {
  const parsed = createMessageSchema.parse(valid);
  assert.equal(parsed.type, "transactional");
  assert.equal(parsed.customer_id, "cus_1");
});

test("accepts type=marketing when provided", () => {
  const parsed = createMessageSchema.parse({ ...valid, type: "marketing" });
  assert.equal(parsed.type, "marketing");
});

test("rejects an invalid email address", () => {
  const res = createMessageSchema.safeParse({ ...valid, to: "not-an-email" });
  assert.equal(res.success, false);
});

test("rejects an unknown message type", () => {
  const res = createMessageSchema.safeParse({ ...valid, type: "promo" });
  assert.equal(res.success, false);
});

test("rejects a missing idempotency_key", () => {
  const { idempotency_key, ...rest } = valid;
  void idempotency_key;
  const res = createMessageSchema.safeParse(rest);
  assert.equal(res.success, false);
});
