CREATE TYPE "public"."ip_pool_type" AS ENUM('shared', 'dedicated');--> statement-breakpoint
CREATE TYPE "public"."message_event_type" AS ENUM('accepted', 'queued', 'throttled', 'sending_attempted', 'deferred', 'delivered', 'bounced', 'retry_scheduled', 'dead_lettered', 'suppressed', 'blocked_by_abuse_control');--> statement-breakpoint
CREATE TYPE "public"."message_status" AS ENUM('queued', 'sending', 'deferred', 'delivered', 'bounced', 'failed', 'dead_lettered', 'suppressed', 'throttled', 'blocked');--> statement-breakpoint
CREATE TYPE "public"."message_type" AS ENUM('transactional', 'marketing');--> statement-breakpoint
CREATE TYPE "public"."provider_reputation_state" AS ENUM('healthy', 'degraded', 'down');--> statement-breakpoint
CREATE TYPE "public"."sending_state" AS ENUM('trusted', 'new', 'limited', 'blocked_from_marketing', 'blocked_from_sending');--> statement-breakpoint
CREATE TYPE "public"."throttle_scope" AS ENUM('customer', 'domain', 'provider', 'ip_pool');--> statement-breakpoint
CREATE TABLE "customers" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"sending_state" "sending_state" DEFAULT 'new' NOT NULL,
	"daily_send_cap" integer DEFAULT 100 NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "delivery_attempts" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "delivery_attempts_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"message_id" text NOT NULL,
	"attempt_number" integer NOT NULL,
	"provider" text,
	"ip_pool_id" integer,
	"smtp_code" integer,
	"raw_response" text,
	"classification" text,
	"started_at" timestamp,
	"finished_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "idempotency_keys" (
	"key" text PRIMARY KEY NOT NULL,
	"message_id" text NOT NULL,
	"customer_id" text NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "ip_pools" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ip_pools_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"name" text NOT NULL,
	"type" "ip_pool_type" NOT NULL,
	"customer_id" text,
	"warmup_started_at" timestamp,
	"daily_limit" integer
);
--> statement-breakpoint
CREATE TABLE "message_events" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "message_events_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"message_id" text NOT NULL,
	"event_type" "message_event_type" NOT NULL,
	"provider" text,
	"smtp_code" integer,
	"raw_response" text,
	"classification" text,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "messages" (
	"id" text PRIMARY KEY NOT NULL,
	"customer_id" text NOT NULL,
	"from_email" text NOT NULL,
	"from_domain" text,
	"to_email" text NOT NULL,
	"recipient_domain" text,
	"mailbox_provider" text,
	"type" "message_type" DEFAULT 'transactional' NOT NULL,
	"subject" text,
	"html" text,
	"status" "message_status" DEFAULT 'queued' NOT NULL,
	"route_id" integer,
	"ip_pool_id" integer,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"next_attempt_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "provider_policies" (
	"provider" text PRIMARY KEY NOT NULL,
	"reputation_state" "provider_reputation_state" DEFAULT 'healthy' NOT NULL,
	"daily_cap" integer,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "routes" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "routes_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"name" text NOT NULL,
	"provider" text NOT NULL,
	"ip_pool_id" integer,
	"weight" integer DEFAULT 1 NOT NULL,
	"enabled" boolean DEFAULT true NOT NULL
);
--> statement-breakpoint
CREATE TABLE "suppression_list" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "suppression_list_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"customer_id" text NOT NULL,
	"recipient_email" text NOT NULL,
	"reason" text,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "throttle_rules" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "throttle_rules_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"scope" "throttle_scope" NOT NULL,
	"scope_key" text NOT NULL,
	"messages_per_minute" integer NOT NULL
);
--> statement-breakpoint
ALTER TABLE "delivery_attempts" ADD CONSTRAINT "delivery_attempts_message_id_messages_id_fk" FOREIGN KEY ("message_id") REFERENCES "public"."messages"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "delivery_attempts" ADD CONSTRAINT "delivery_attempts_ip_pool_id_ip_pools_id_fk" FOREIGN KEY ("ip_pool_id") REFERENCES "public"."ip_pools"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "idempotency_keys" ADD CONSTRAINT "idempotency_keys_message_id_messages_id_fk" FOREIGN KEY ("message_id") REFERENCES "public"."messages"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "idempotency_keys" ADD CONSTRAINT "idempotency_keys_customer_id_customers_id_fk" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "ip_pools" ADD CONSTRAINT "ip_pools_customer_id_customers_id_fk" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "message_events" ADD CONSTRAINT "message_events_message_id_messages_id_fk" FOREIGN KEY ("message_id") REFERENCES "public"."messages"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "messages" ADD CONSTRAINT "messages_customer_id_customers_id_fk" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "messages" ADD CONSTRAINT "messages_route_id_routes_id_fk" FOREIGN KEY ("route_id") REFERENCES "public"."routes"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "messages" ADD CONSTRAINT "messages_ip_pool_id_ip_pools_id_fk" FOREIGN KEY ("ip_pool_id") REFERENCES "public"."ip_pools"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "routes" ADD CONSTRAINT "routes_ip_pool_id_ip_pools_id_fk" FOREIGN KEY ("ip_pool_id") REFERENCES "public"."ip_pools"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "suppression_list" ADD CONSTRAINT "suppression_list_customer_id_customers_id_fk" FOREIGN KEY ("customer_id") REFERENCES "public"."customers"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "suppression_list_customer_email_unique" ON "suppression_list" USING btree ("customer_id","recipient_email");