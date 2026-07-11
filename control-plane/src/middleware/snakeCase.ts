import type { Request, Response, NextFunction } from "express";

/**
 * Recursively convert object keys to snake_case.
 *
 * The API mixes two data sources: the Drizzle query builder (which returns
 * camelCase keys from the schema property names) and raw SQL (which returns
 * snake_case column names). This normalizes every JSON response to snake_case
 * so the public API is consistent regardless of how a handler fetched data.
 *
 * Dates, buffers, and primitives are passed through untouched.
 */
function toSnakeKey(key: string): string {
  return key.replace(/[A-Z]/g, (c) => "_" + c.toLowerCase());
}

export function snakeifyKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(snakeifyKeys);
  }
  if (
    value !== null &&
    typeof value === "object" &&
    !(value instanceof Date) &&
    !Buffer.isBuffer(value)
  ) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[toSnakeKey(k)] = snakeifyKeys(v);
    }
    return out;
  }
  return value;
}

/**
 * Express middleware: wraps res.json so every response body is snake_cased.
 */
export function snakeCaseResponse(_req: Request, res: Response, next: NextFunction): void {
  const originalJson = res.json.bind(res);
  res.json = (body: unknown) => originalJson(snakeifyKeys(body));
  next();
}
