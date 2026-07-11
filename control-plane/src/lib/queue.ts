import Redis from "ioredis";
import { env } from "../env";
import { log } from "./logging";

let redisClient: Redis | null = null;

export function getRedis(): Redis {
  if (!redisClient) {
    redisClient = new Redis(env.REDIS_URL, { maxRetriesPerRequest: 3 });
    redisClient.on("error", (err) => {
      log.error("redis error", { error: err.message });
    });
  }
  return redisClient;
}

/**
 * Enqueue a message ID onto the Redis Stream for the worker to consume.
 * Called AFTER the DB transaction commits so we never enqueue a message
 * that doesn't exist in Postgres.
 */
export async function enqueueMessage(messageId: string): Promise<string> {
  const redis = getRedis();
  const id = await redis.xadd(env.QUEUE_STREAM, "*", "message_id", messageId);
  log.info("message enqueued", { message_id: messageId, stream_id: id });
  return id ?? "";
}

/**
 * Ensure the consumer group exists. Safe to call on every boot —
 * returns silently if the group already exists.
 */
export async function ensureConsumerGroup(): Promise<void> {
  const redis = getRedis();
  try {
    await redis.xgroup("CREATE", env.QUEUE_STREAM, env.QUEUE_GROUP, "$", "MKSTREAM");
    log.info("consumer group created", { stream: env.QUEUE_STREAM, group: env.QUEUE_GROUP });
  } catch (err: unknown) {
    // BUSYGROUP means the group already exists — that's fine
    if (err instanceof Error && !err.message.includes("BUSYGROUP")) {
      throw err;
    }
  }
}
