import { z } from "zod";

/**
 * Request schema for POST /messages. Kept in a side-effect-free module (no db
 * or env imports) so it can be unit-tested without a database connection.
 */
export const createMessageSchema = z.object({
  customer_id: z.string().min(1),
  from: z.string().email(),
  to: z.string().email(),
  subject: z.string().min(1),
  html: z.string().min(1),
  type: z.enum(["transactional", "marketing"]).default("transactional"),
  idempotency_key: z.string().min(1),
});

export type CreateMessageInput = z.infer<typeof createMessageSchema>;
