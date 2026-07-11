import { z } from 'zod';

const envSchema = z.object({
  DATABASE_URL: z.string().url('DATABASE_URL must be a valid URL'),
  REDIS_URL: z.string().url('REDIS_URL must be a valid URL'),
  PORT: z.string().default('3000').transform((val) => parseInt(val, 10)),
  QUEUE_STREAM: z.string().default('messages'),
  QUEUE_GROUP: z.string().default('worker'),
});

export const env = envSchema.parse(process.env);
