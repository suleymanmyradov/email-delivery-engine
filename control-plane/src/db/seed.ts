/**
 * Idempotent seed script.
 *
 * Populates the reference/control data the delivery pipeline needs to run:
 * customers, provider policies, IP pools (shared + a dedicated pool in warmup),
 * routes, and throttle rules. Safe to run repeatedly — every insert uses
 * ON CONFLICT DO NOTHING / DO UPDATE so re-seeding never duplicates rows.
 *
 * Run with:  npm run db:seed
 */
import { pool } from "./index";
import { log } from "../lib/logging";

async function seed(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // --- Customers ---------------------------------------------------------
    // A trusted high-volume sender, a brand-new sender, and one already limited.
    await client.query(`
      INSERT INTO customers (id, name, sending_state, daily_send_cap) VALUES
        ('cus_trusted', 'Acme (trusted)',        'trusted', 100000),
        ('cus_new',     'Startup (new sender)',  'new',        1000),
        ('cus_limited', 'Noisy Co (limited)',    'limited',     500)
      ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            sending_state = EXCLUDED.sending_state,
            daily_send_cap = EXCLUDED.daily_send_cap
    `);

    // --- Provider policies -------------------------------------------------
    // Reputation state drives worker routing + simulated deferral rates.
    await client.query(`
      INSERT INTO provider_policies (provider, reputation_state, daily_cap) VALUES
        ('gmail',   'healthy',  NULL),
        ('outlook', 'healthy',  NULL),
        ('yahoo',   'healthy',  NULL),
        ('apple',   'healthy',  NULL),
        ('other',   'healthy',  NULL)
      ON CONFLICT (provider) DO UPDATE
        SET reputation_state = EXCLUDED.reputation_state,
            daily_cap = EXCLUDED.daily_cap,
            updated_at = now()
    `);

    // --- IP pools ----------------------------------------------------------
    // Two shared pools, plus one dedicated pool for cus_trusted that is
    // mid-warmup (started 2 days ago -> warmup day 3).
    // Named identity columns mean we key on `name` to stay idempotent.
    await client.query(`
      INSERT INTO ip_pools (name, type, customer_id, warmup_started_at, daily_limit)
      SELECT 'shared-a', 'shared', NULL, NULL, NULL
      WHERE NOT EXISTS (SELECT 1 FROM ip_pools WHERE name = 'shared-a')
    `);
    await client.query(`
      INSERT INTO ip_pools (name, type, customer_id, warmup_started_at, daily_limit)
      SELECT 'shared-b', 'shared', NULL, NULL, NULL
      WHERE NOT EXISTS (SELECT 1 FROM ip_pools WHERE name = 'shared-b')
    `);
    await client.query(`
      INSERT INTO ip_pools (name, type, customer_id, warmup_started_at, daily_limit)
      SELECT 'dedicated-acme', 'dedicated', 'cus_trusted', now() - interval '2 days', NULL
      WHERE NOT EXISTS (SELECT 1 FROM ip_pools WHERE name = 'dedicated-acme')
    `);

    // --- Routes ------------------------------------------------------------
    // One route per provider pinned to shared-a, plus a dedicated route for Acme.
    const sharedA = (
      await client.query(`SELECT id FROM ip_pools WHERE name = 'shared-a'`)
    ).rows[0].id as number;
    const dedicatedAcme = (
      await client.query(`SELECT id FROM ip_pools WHERE name = 'dedicated-acme'`)
    ).rows[0].id as number;

    for (const provider of ["gmail", "outlook", "yahoo", "apple", "other"]) {
      await client.query(
        `INSERT INTO routes (name, provider, ip_pool_id, weight, enabled)
         SELECT $1, $2, $3, 1, true
         WHERE NOT EXISTS (SELECT 1 FROM routes WHERE name = $1)`,
        [`shared-${provider}`, provider, sharedA],
      );
      await client.query(
        `INSERT INTO routes (name, provider, ip_pool_id, weight, enabled)
         SELECT $1, $2, $3, 1, true
         WHERE NOT EXISTS (SELECT 1 FROM routes WHERE name = $1)`,
        [`dedicated-${provider}`, provider, dedicatedAcme],
      );
    }

    // --- Throttle rules ----------------------------------------------------
    // Enforced by the worker (Redis sliding-window) before each send.
    // throttle_rules has no natural unique key, so guard with NOT EXISTS to
    // keep re-seeding idempotent.
    const throttleRules: Array<[string, string, number]> = [
      ["customer", "cus_new", 60],
      ["customer", "cus_limited", 20],
      ["provider", "gmail", 100],
      ["provider", "outlook", 80],
      ["provider", "yahoo", 50],
    ];
    for (const [scope, scopeKey, mpm] of throttleRules) {
      await client.query(
        `INSERT INTO throttle_rules (scope, scope_key, messages_per_minute)
         SELECT $1::throttle_scope, $2, $3
         WHERE NOT EXISTS (
           SELECT 1 FROM throttle_rules
           WHERE scope = $1::throttle_scope AND scope_key = $2
         )`,
        [scope, scopeKey, mpm],
      );
    }

    await client.query("COMMIT");
    log.info("seed complete", {
      customers: 3,
      providers: 5,
      ip_pools: 3,
      shared_a_id: sharedA,
      dedicated_acme_id: dedicatedAcme,
    });
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

seed()
  .then(() => pool.end())
  .then(() => process.exit(0))
  .catch((err) => {
    log.error("seed failed", {
      error: err instanceof Error ? err.message : String(err),
    });
    process.exit(1);
  });
