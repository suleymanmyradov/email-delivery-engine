import { Router } from "express";
import { db, pool } from "../db";
import {
  messagesTable,
  messageEventsTable,
  customersTable,
  ipPoolsTable,
  routesTable,
  throttleRulesTable,
  providerPoliciesTable,
  suppressionListTable,
} from "../db/schema";
import { eq, sql, and, desc } from "drizzle-orm";
import { log } from "../lib/logging";
import type { RequestWithId } from "../middleware/requestId";

export const debugRouter = Router();

/**
 * GET /debug/customers/:customer_id
 * Shows message counts by status, recent events, throttle rules, sending state.
 */
debugRouter.get("/customers/:customer_id", async (req: RequestWithId, res) => {
  const customerId = String(req.params.customer_id);

  const customer = await db.query.customersTable.findFirst({
    where: eq(customersTable.id, customerId),
  });

  if (!customer) {
    return res.status(404).json({ error: "customer_not_found" });
  }

  // Message counts by status
  const statusCounts = await pool.query(
    `SELECT status, count(*) as count
     FROM messages WHERE customer_id = $1
     GROUP BY status`,
    [customerId],
  );

  // Recent events (last 20)
  const recentEvents = await pool.query(
    `SELECT me.* FROM message_events me
     JOIN messages m ON me.message_id = m.id
     WHERE m.customer_id = $1
     ORDER BY me.created_at DESC LIMIT 20`,
    [customerId],
  );

  // Throttle rules for this customer
  const throttleRules = await db
    .select()
    .from(throttleRulesTable)
    .where(eq(throttleRulesTable.scope, "customer"));

  // Suppression list entries
  const suppressions = await db
    .select()
    .from(suppressionListTable)
    .where(eq(suppressionListTable.customerId, customerId));

  return res.json({
    customer,
    message_counts_by_status: statusCounts.rows,
    recent_events: recentEvents.rows,
    throttle_rules: throttleRules,
    suppressions,
  });
});

/**
 * GET /debug/domains/:domain
 * Shows volume, bounce rate, active throttles for a recipient domain.
 */
debugRouter.get("/domains/:domain", async (req: RequestWithId, res) => {
  const domain = String(req.params.domain);

  const statusCounts = await pool.query(
    `SELECT status, count(*) as count
     FROM messages WHERE recipient_domain = $1
     GROUP BY status`,
    [domain],
  );

  const bounceRate = await pool.query(
    `SELECT
       count(*) FILTER (WHERE status IN ('bounced', 'delivered')) as total_attempted,
       count(*) FILTER (WHERE status = 'bounced') as bounced,
       CASE
         WHEN count(*) FILTER (WHERE status IN ('bounced', 'delivered')) > 0
         THEN round(
           count(*) FILTER (WHERE status = 'bounced')::numeric /
           count(*) FILTER (WHERE status IN ('bounced', 'delivered')) * 100, 2
         )
         ELSE 0
       END as bounce_rate_pct
     FROM messages WHERE recipient_domain = $1`,
    [domain],
  );

  const throttleRules = await db
    .select()
    .from(throttleRulesTable)
    .where(eq(throttleRulesTable.scope, "domain"));

  return res.json({
    domain,
    message_counts_by_status: statusCounts.rows,
    bounce_rate: bounceRate.rows[0],
    throttle_rules: throttleRules,
  });
});

/**
 * GET /debug/providers/:provider
 * Shows reputation state, recent deferral rate, active routes.
 */
debugRouter.get("/providers/:provider", async (req: RequestWithId, res) => {
  const provider = String(req.params.provider);

  const policy = await db.query.providerPoliciesTable.findFirst({
    where: eq(providerPoliciesTable.provider, provider),
  });

  const statusCounts = await pool.query(
    `SELECT status, count(*) as count
     FROM messages WHERE mailbox_provider = $1
     GROUP BY status`,
    [provider],
  );

  const deferralRate = await pool.query(
    `SELECT
       count(*) as total,
       count(*) FILTER (WHERE status = 'deferred') as deferred,
       count(*) FILTER (WHERE status = 'delivered') as delivered
     FROM messages WHERE mailbox_provider = $1`,
    [provider],
  );

  const activeRoutes = await db
    .select()
    .from(routesTable)
    .where(and(eq(routesTable.provider, provider), eq(routesTable.enabled, true)));

  return res.json({
    provider,
    policy: policy ?? { provider, reputation_state: "healthy", daily_cap: null },
    message_counts_by_status: statusCounts.rows,
    deferral_stats: deferralRate.rows[0],
    active_routes: activeRoutes,
  });
});

/**
 * GET /debug/ip-pools/:pool_id
 * Shows warmup day, daily count, capacity, assigned routes.
 */
debugRouter.get("/ip-pools/:pool_id", async (req: RequestWithId, res) => {
  const poolId = parseInt(String(req.params.pool_id), 10);
  if (isNaN(poolId)) {
    return res.status(400).json({ error: "invalid_pool_id" });
  }

  const ipPool = await db.query.ipPoolsTable.findFirst({
    where: eq(ipPoolsTable.id, poolId),
  });

  if (!ipPool) {
    return res.status(404).json({ error: "ip_pool_not_found" });
  }

  // Messages sent via this pool today
  const todayStats = await pool.query(
    `SELECT
       count(*) as sent_today,
       count(*) FILTER (WHERE status = 'delivered') as delivered,
       count(*) FILTER (WHERE status = 'bounced') as bounced
     FROM messages
     WHERE ip_pool_id = $1 AND created_at >= CURRENT_DATE`,
    [poolId],
  );

  const assignedRoutes = await db
    .select()
    .from(routesTable)
    .where(eq(routesTable.ipPoolId, poolId));

  // Calculate warmup day if applicable
  let warmupDay = null;
  if (ipPool.warmupStartedAt) {
    const daysSinceStart = Math.floor(
      (Date.now() - ipPool.warmupStartedAt.getTime()) / (1000 * 60 * 60 * 24),
    );
    warmupDay = daysSinceStart + 1;
  }

  return res.json({
    ip_pool: ipPool,
    warmup_day: warmupDay,
    today_stats: todayStats.rows[0],
    assigned_routes: assignedRoutes,
  });
});
