import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './src/db/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  // Map camelCase schema keys to snake_case columns so the generated DDL
  // matches the snake_case SQL used by the control plane and the Go worker.
  casing: 'snake_case',
  dbCredentials: {
    url: process.env.DATABASE_URL!,
  },
});