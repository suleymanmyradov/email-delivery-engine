import { Router } from "express";
import { randomUUID } from "crypto";
import { db, pool } from "../db";
import { messagesTable, messageEventsTable } from "../db/schema";
import { eq } from "drizzle-orm";
import { detectMailboxProvider, extractDomain } from "../lib/mailbox";
import { enqueueMessage } from "../lib/queue";
import { incCounter } from "../lib/metrics";
import { log } from "../lib/logging";
import { createMessageSchema } from "../lib/schemas";
import type { RequestWithId } from "../middleware/requestId";

export const messagesRouter = Router();

/**
 * POST /messages
 * Accept a message request, enforce idempotency, store, enqueue.
 */
messagesRouter.post("/", async (req: RequestWithId, res) => {
  const requestId = req.requestId;
  const parsed = createMessageSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "validation_error",
      details: parsed.error.issues,
      request_id: requestId,
    });
  }

  const body = parsed.data;
  const fromDomain = extractDomain(body.from);
  const recipientDomain = extractDomain(body.to);
  const mailboxProvider = detectMailboxProvider(body.to);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Idempotency check — SELECT FOR UPDATE to prevent race
    const idempResult = await client.query(
      "SELECT message_id FROM idempotency_keys WHERE key = $1 FOR UPDATE",
      [body.idempotency_key],
    );

    if (idempResult.rows.length > 0) {
      await client.query("ROLLBACK");
      const existingId = idempResult.rows[0].message_id;
      const existing = await db.query.messagesTable.findFirst({
        where: eq(messagesTable.id, existingId),
      });
      log.info("idempotent replay", {
        request_id: requestId,
        message_id: existingId,
        idempotency_key: body.idempotency_key,
      });
      return res.status(200).json(existing);
    }

    // Verify customer exists
    const customerResult = await client.query(
      "SELECT id, sending_state FROM customers WHERE id = $1",
      [body.customer_id],
    );

    if (customerResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "customer_not_found",
        customer_id: body.customer_id,
        request_id: requestId,
      });
    }

    const customer = customerResult.rows[0];

    // Check customer sending state
    if (customer.sending_state === "blocked_from_sending") {
      await client.query("ROLLBACK");
      incCounter("abuse_blocks_total");
      return res.status(403).json({
        error: "blocked_by_abuse_control",
        reason: "Customer is blocked from sending",
        request_id: requestId,
      });
    }

    const messageId = `msg_${randomUUID()}`;

    // Insert message
    await client.query(
      `INSERT INTO messages
        (id, customer_id, from_email, from_domain, to_email, recipient_domain,
         mailbox_provider, type, subject, html, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'queued')`,
      [
        messageId,
        body.customer_id,
        body.from,
        fromDomain,
        body.to,
        recipientDomain,
        mailboxProvider,
        body.type,
        body.subject,
        body.html,
      ],
    );

    // Insert idempotency key
    await client.query(
      "INSERT INTO idempotency_keys (key, message_id, customer_id) VALUES ($1, $2, $3)",
      [body.idempotency_key, messageId, body.customer_id],
    );

    // Write accepted event
    await client.query(
      `INSERT INTO message_events (message_id, event_type)
       VALUES ($1, 'accepted')`,
      [messageId],
    );

    // Write queued event
    await client.query(
      `INSERT INTO message_events (message_id, event_type)
       VALUES ($1, 'queued')`,
      [messageId],
    );

    await client.query("COMMIT");

    incCounter("messages_accepted_total");
    incCounter("messages_queued_total");
    log.info("message accepted", {
      request_id: requestId,
      message_id: messageId,
      customer_id: body.customer_id,
      mailbox_provider: mailboxProvider,
      recipient_domain: recipientDomain,
    });

    // Enqueue AFTER commit — never enqueue inside the transaction
    try {
      await enqueueMessage(messageId);
    } catch (enqueueErr) {
      // The message is stored and queued in DB; the retry scheduler will pick it up.
      log.error("enqueue failed — message will be picked up by deferred poller", {
        request_id: requestId,
        message_id: messageId,
        error: enqueueErr instanceof Error ? enqueueErr.message : String(enqueueErr),
      });
    }

    const message = await db.query.messagesTable.findFirst({
      where: eq(messagesTable.id, messageId),
    });
    return res.status(201).json(message);
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    log.error("message creation failed", {
      request_id: requestId,
      error: err instanceof Error ? err.message : String(err),
    });
    return res.status(500).json({
      error: "internal_error",
      request_id: requestId,
    });
  } finally {
    client.release();
  }
});

/**
 * GET /messages/:id
 */
messagesRouter.get("/:id", async (req: RequestWithId, res) => {
  const messageId = String(req.params.id);
  const message = await db.query.messagesTable.findFirst({
    where: eq(messagesTable.id, messageId),
  });
  if (!message) {
    return res.status(404).json({ error: "not_found" });
  }
  return res.json(message);
});

/**
 * GET /messages/:id/events
 * Returns the full event timeline for debugging.
 */
messagesRouter.get("/:id/events", async (req: RequestWithId, res) => {
  const messageId = String(req.params.id);
  const message = await db.query.messagesTable.findFirst({
    where: eq(messagesTable.id, messageId),
  });

  if (!message) {
    return res.status(404).json({ error: "not_found" });
  }

  const events = await db
    .select()
    .from(messageEventsTable)
    .where(eq(messageEventsTable.messageId, messageId))
    .orderBy(messageEventsTable.createdAt);

  return res.json(events);
});
