import { Router } from "express";
import { pool } from "../db";
import { renderPrometheusMetrics, updateQueueGauges } from "../lib/metrics";

export const metricsRouter = Router();

/**
 * GET /metrics
 * Prometheus text format metrics endpoint.
 */
metricsRouter.get("/", async (_req, res) => {
  // Update queue gauges from Postgres
  await updateQueueGauges(
    async () => {
      const result = await pool.query(
        "SELECT count(*) as count FROM messages WHERE status IN ('queued', 'deferred')",
      );
      return parseInt(result.rows[0].count, 10);
    },
    async () => {
      const result = await pool.query(
        "SELECT EXTRACT(EPOCH FROM (now() - created_at))::int as age FROM messages WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1",
      );
      return result.rows.length > 0 ? parseInt(result.rows[0].age, 10) : 0;
    },
  );

  res.setHeader("content-type", "text/plain; version=0.0.4");
  res.send(renderPrometheusMetrics());
});
