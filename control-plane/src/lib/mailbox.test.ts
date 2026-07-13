import { test } from "node:test";
import assert from "node:assert/strict";
import { detectMailboxProvider, extractDomain, parseEmail } from "./mailbox";

test("detectMailboxProvider maps known domains to providers", () => {
  assert.equal(detectMailboxProvider("a@gmail.com"), "gmail");
  assert.equal(detectMailboxProvider("a@googlemail.com"), "gmail");
  assert.equal(detectMailboxProvider("a@outlook.com"), "outlook");
  assert.equal(detectMailboxProvider("a@hotmail.com"), "outlook");
  assert.equal(detectMailboxProvider("a@icloud.com"), "apple");
});

test("detectMailboxProvider falls back to 'other' for unknown domains", () => {
  assert.equal(detectMailboxProvider("a@some-corp.io"), "other");
});

test("extractDomain lowercases the domain and handles malformed input", () => {
  assert.equal(extractDomain("Foo@Gmail.COM"), "gmail.com");
  assert.equal(extractDomain("no-at-sign"), "");
});

test("parseEmail splits local part and domain", () => {
  assert.deepEqual(parseEmail("bob@example.com"), {
    local: "bob",
    domain: "example.com",
  });
});
