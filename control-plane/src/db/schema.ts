import {
  boolean,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const sendingStateEnum = pgEnum("sending_state", [
  "trusted",
  "new",
  "limited",
  "blocked_from_marketing",
  "blocked_from_sending",
]);

export const messageStatusEnum = pgEnum("message_status", [
  "queued",
  "sending",
  "deferred",
  "delivered",
  "bounced",
  "failed",
  "dead_lettered",
  "suppressed",
  "throttled",
  "blocked",
]);

export const messageEventTypeEnum = pgEnum("message_event_type", [
  "accepted",
  "queued",
  "throttled",
  "sending_attempted",
  "deferred",
  "delivered",
  "bounced",
  "retry_scheduled",
  "dead_lettered",
  "suppressed",
  "blocked_by_abuse_control",
]);

export const messageTypeEnum = pgEnum("message_type", [
  "transactional",
  "marketing",
]);

export const ipPoolTypeEnum = pgEnum("ip_pool_type", [
  "shared",
  "dedicated",
]);

export const providerReputationStateEnum = pgEnum("provider_reputation_state", [
  "healthy",
  "degraded",
  "down",
]);

export const throttleScopeEnum = pgEnum("throttle_scope", [
  "customer",
  "domain",
  "provider",
  "ip_pool",
]);

export const customersTable = pgTable("customers", {
  id: text().primaryKey(),
  name: text().notNull(),
  sendingState: sendingStateEnum("sending_state").notNull().default("new"),
  dailySendCap: integer().notNull().default(100),
  createdAt: timestamp().notNull().defaultNow(),
});

export const providerPoliciesTable = pgTable("provider_policies", {
  provider: text().primaryKey(),
  reputationState: providerReputationStateEnum("reputation_state")
    .notNull()
    .default("healthy"),
  dailyCap: integer(),
  updatedAt: timestamp().notNull().defaultNow(),
});

export const ipPoolsTable = pgTable("ip_pools", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  name: text().notNull(),
  type: ipPoolTypeEnum("type").notNull(),
  customerId: text().references(() => customersTable.id),
  warmupStartedAt: timestamp(),
  dailyLimit: integer(),
});

export const routesTable = pgTable("routes", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  name: text().notNull(),
  provider: text().notNull(),
  ipPoolId: integer().references(() => ipPoolsTable.id),
  weight: integer().notNull().default(1),
  enabled: boolean().notNull().default(true),
});

export const messagesTable = pgTable("messages", {
  id: text().primaryKey(),
  customerId: text()
    .notNull()
    .references(() => customersTable.id),
  fromEmail: text().notNull(),
  fromDomain: text(),
  toEmail: text().notNull(),
  recipientDomain: text(),
  mailboxProvider: text(),
  type: messageTypeEnum("type").notNull().default("transactional"),
  subject: text(),
  html: text(),
  status: messageStatusEnum("status").notNull().default("queued"),
  routeId: integer().references(() => routesTable.id),
  ipPoolId: integer().references(() => ipPoolsTable.id),
  attemptCount: integer().notNull().default(0),
  nextAttemptAt: timestamp(),
  createdAt: timestamp().notNull().defaultNow(),
  updatedAt: timestamp().notNull().defaultNow(),
});

export const messageEventsTable = pgTable("message_events", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  messageId: text()
    .notNull()
    .references(() => messagesTable.id),
  eventType: messageEventTypeEnum("event_type").notNull(),
  provider: text(),
  smtpCode: integer(),
  rawResponse: text(),
  classification: text(),
  metadata: jsonb(),
  createdAt: timestamp().notNull().defaultNow(),
});

export const deliveryAttemptsTable = pgTable("delivery_attempts", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  messageId: text()
    .notNull()
    .references(() => messagesTable.id),
  attemptNumber: integer().notNull(),
  provider: text(),
  ipPoolId: integer().references(() => ipPoolsTable.id),
  smtpCode: integer(),
  rawResponse: text(),
  classification: text(),
  startedAt: timestamp(),
  finishedAt: timestamp(),
});

export const idempotencyKeysTable = pgTable("idempotency_keys", {
  key: text().primaryKey(),
  messageId: text()
    .notNull()
    .references(() => messagesTable.id),
  customerId: text()
    .notNull()
    .references(() => customersTable.id),
  createdAt: timestamp().notNull().defaultNow(),
});

export const throttleRulesTable = pgTable("throttle_rules", {
  id: integer().primaryKey().generatedAlwaysAsIdentity(),
  scope: throttleScopeEnum("scope").notNull(),
  scopeKey: text().notNull(),
  messagesPerMinute: integer().notNull(),
});

export const suppressionListTable = pgTable(
  "suppression_list",
  {
    id: integer().primaryKey().generatedAlwaysAsIdentity(),
    customerId: text()
      .notNull()
      .references(() => customersTable.id),
    recipientEmail: text().notNull(),
    reason: text(),
    createdAt: timestamp().notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("suppression_list_customer_email_unique").on(
      table.customerId,
      table.recipientEmail,
    ),
  ],
);
