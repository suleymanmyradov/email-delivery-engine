/**
 * Simple in-memory metrics store with Prometheus text format export.
 * For a learning project this is sufficient. In production, use prom-client
 * with a proper registry and histogram support.
 */

type MetricType = "counter" | "gauge";

interface MetricEntry {
  type: MetricType;
  help: string;
  value: number;
}

const metrics = new Map<string, MetricEntry>();

function ensureMetric(name: string, type: MetricType, help: string): void {
  if (!metrics.has(name)) {
    metrics.set(name, { type, help, value: 0 });
  }
}

export function incCounter(name: string, by = 1): void {
  ensureMetric(name, "counter", "");
  const entry = metrics.get(name)!;
  entry.value += by;
}

export function setGauge(name: string, value: number): void {
  ensureMetric(name, "gauge", "");
  const entry = metrics.get(name)!;
  entry.value = value;
}

// Pre-register all metrics from the plan with help text
const METRIC_DEFINITIONS: Array<[string, MetricType, string]> = [
  ["messages_accepted_total", "counter", "Total messages accepted by the API"],
  ["messages_queued_total", "counter", "Total messages enqueued for delivery"],
  ["messages_delivered_total", "counter", "Total messages successfully delivered"],
  ["messages_bounced_total", "counter", "Total messages that bounced"],
  ["messages_deferred_total", "counter", "Total messages deferred for retry"],
  ["messages_dead_lettered_total", "counter", "Total messages moved to DLQ"],
  ["messages_throttled_total", "counter", "Total messages throttled by rate limits"],
  ["delivery_attempts_total", "counter", "Total delivery attempts made by the worker"],
  ["retry_scheduled_total", "counter", "Total retries scheduled"],
  ["provider_rate_limited_total", "counter", "Total provider rate-limit responses received"],
  ["abuse_blocks_total", "counter", "Total sends blocked by abuse controls"],
  ["queue_depth", "gauge", "Current number of messages waiting in the queue"],
  ["oldest_queued_message_age_seconds", "gauge", "Age of the oldest queued message in seconds"],
];

export function initMetrics(): void {
  for (const [name, type, help] of METRIC_DEFINITIONS) {
    metrics.set(name, { type, help, value: 0 });
  }
}

export function renderPrometheusMetrics(): string {
  const lines: string[] = [];
  for (const [name, entry] of metrics) {
    if (entry.help) {
      lines.push(`# HELP ${name} ${entry.help}`);
    }
    lines.push(`# TYPE ${name} ${entry.type}`);
    lines.push(`${name} ${entry.value}`);
  }
  return lines.join("\n") + "\n";
}

/**
 * Update queue-related gauges by querying Postgres.
 * Called periodically and on /metrics requests.
 */
export async function updateQueueGauges(
  countQueued: () => Promise<number>,
  getOldestAge: () => Promise<number>,
): Promise<void> {
  setGauge("queue_depth", await countQueued());
  setGauge("oldest_queued_message_age_seconds", await getOldestAge());
}
