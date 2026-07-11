import express from "express";
import { env } from "./env";
import { requestIdMiddleware } from "./middleware/requestId";
import { snakeCaseResponse } from "./middleware/snakeCase";
import { messagesRouter } from "./routes/messages";
import { debugRouter } from "./routes/debug";
import { metricsRouter } from "./routes/metrics";
import { initMetrics } from "./lib/metrics";
import { log } from "./lib/logging";
import { pool } from "./db";

async function main() {
  initMetrics();

  const app = express();
  app.use(express.json({ limit: "1mb" }));
  app.use(requestIdMiddleware);
  app.use(snakeCaseResponse);

  // Routes
  app.use("/messages", messagesRouter);
  app.use("/debug", debugRouter);
  app.use("/metrics", metricsRouter);

  // Health check
  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.listen(env.PORT, () => {
    log.info("control plane started", { port: env.PORT });
  });

  // Graceful shutdown
  process.on("SIGTERM", async () => {
    log.info("SIGTERM received, shutting down");
    await pool.end();
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    log.info("SIGINT received, shutting down");
    await pool.end();
    process.exit(0);
  });
}

main().catch((err) => {
  log.error("fatal startup error", {
    error: err instanceof Error ? err.message : String(err),
  });
  process.exit(1);
});
