// Package worker contains the delivery pipeline: given a message ID, it runs
// the message through abuse checks, suppression, route/IP-pool selection,
// warmup + throttle limits, a simulated send, outcome classification, and
// retry/backoff or dead-lettering — writing an event at every step so the
// debug APIs can explain exactly what happened.
package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"time"

	"github.com/suleyman/email-delivery-engine/internal/backoff"
	"github.com/suleyman/email-delivery-engine/internal/classify"
	"github.com/suleyman/email-delivery-engine/internal/metrics"
	"github.com/suleyman/email-delivery-engine/internal/route"
	"github.com/suleyman/email-delivery-engine/internal/simulate"
	"github.com/suleyman/email-delivery-engine/internal/store"
	"github.com/suleyman/email-delivery-engine/internal/throttle"
)

// terminalStatuses are message states the worker must never re-process.
var terminalStatuses = map[string]bool{
	"delivered":     true,
	"bounced":       true,
	"failed":        true,
	"dead_lettered": true,
	"suppressed":    true,
	"blocked":       true,
}

// hardBounceThresholdPct is the 24h hard-bounce rate above which a customer is
// moved to the "limited" state by the abuse loop.
const hardBounceThresholdPct = 20.0

// Processor runs the delivery pipeline for individual messages.
type Processor struct {
	st          *store.Store
	limiter     *throttle.Limiter
	log         *slog.Logger
	maxAttempts int
	rng         *rand.Rand
}

// NewProcessor builds a Processor. rng must not be nil (seed it in main).
func NewProcessor(st *store.Store, limiter *throttle.Limiter, log *slog.Logger, maxAttempts int, rng *rand.Rand) *Processor {
	return &Processor{st: st, limiter: limiter, log: log, maxAttempts: maxAttempts, rng: rng}
}

// Process runs the full pipeline for one message. A returned error means the
// message could not be processed cleanly and should be retried later (the
// caller should NOT ack the queue entry); a nil error means processing reached
// a decision and the entry may be acked.
func (p *Processor) Process(ctx context.Context, messageID string) error {
	log := p.log.With("message_id", messageID)

	msg, err := p.st.GetMessageForProcessing(ctx, messageID)
	if err != nil {
		return fmt.Errorf("load message: %w", err)
	}
	if msg == nil {
		log.Warn("message not found or locked; acking")
		return nil
	}
	if terminalStatuses[msg.Status] {
		log.Debug("message already in terminal state; acking", "status", msg.Status)
		return nil
	}

	log = log.With(
		"customer_id", msg.CustomerID,
		"provider", msg.MailboxProvider,
		"recipient_domain", msg.RecipientDomain,
		"attempt_count", msg.AttemptCount,
	)

	customer, err := p.st.GetCustomer(ctx, msg.CustomerID)
	if err != nil {
		return fmt.Errorf("load customer: %w", err)
	}
	if customer == nil {
		log.Error("customer missing for message; failing")
		return p.terminate(ctx, msg, "failed", "bounced", "", classificationMeta("no_customer"), log)
	}

	// 1. Abuse control — hard block on all sending.
	if customer.SendingState == "blocked_from_sending" {
		metrics.Inc("abuse_blocks_total")
		log.Info("blocked by abuse control", "sending_state", customer.SendingState)
		return p.terminateEvent(ctx, msg, "blocked", "blocked_by_abuse_control",
			"", sql.NullInt64{}, "", "customer_blocked_from_sending",
			map[string]any{"sending_state": customer.SendingState}, log)
	}

	// 1b. Marketing block — transactional mail still flows, marketing does not.
	if customer.SendingState == "blocked_from_marketing" && msg.Type == "marketing" {
		metrics.Inc("abuse_blocks_total")
		log.Info("marketing blocked by abuse control", "sending_state", customer.SendingState, "type", msg.Type)
		return p.terminateEvent(ctx, msg, "blocked", "blocked_by_abuse_control",
			"", sql.NullInt64{}, "", "customer_blocked_from_marketing",
			map[string]any{"sending_state": customer.SendingState, "type": msg.Type}, log)
	}

	// 2. Suppression list.
	suppressed, err := p.st.IsSuppressed(ctx, msg.CustomerID, msg.ToEmail)
	if err != nil {
		return fmt.Errorf("check suppression: %w", err)
	}
	if suppressed {
		metrics.Inc("messages_suppressed_total")
		log.Info("recipient suppressed; skipping send")
		return p.terminateEvent(ctx, msg, "suppressed", "suppressed",
			"", sql.NullInt64{}, "", "recipient_on_suppression_list", nil, log)
	}

	// 3. Route / IP pool selection.
	decision, err := route.Select(ctx, p.st, msg, msg.CustomerID)
	if err != nil {
		log.Error("route selection failed; failing message", "error", err)
		return p.terminate(ctx, msg, "failed", "bounced", "", classificationMeta("no_route"), log)
	}
	log = log.With("route_id", decision.Route.ID, "ip_pool_id", decision.Pool.ID)
	now := time.Now()

	// 4. Customer daily send cap. Counts messages created today (a proxy for
	// "sent today" in v0); over-cap messages defer to the next window rather
	// than fail. Reschedules without consuming a delivery attempt.
	if customer.DailySendCap > 0 {
		sentToday, err := p.st.CountCustomerSendsToday(ctx, msg.CustomerID)
		if err != nil {
			return fmt.Errorf("count customer sends: %w", err)
		}
		if sentToday >= customer.DailySendCap {
			metrics.Inc("messages_throttled_total")
			log.Info("customer at daily send cap; deferring",
				"daily_send_cap", customer.DailySendCap, "sent_today", sentToday)
			return p.reschedule(ctx, msg, decision, now.Add(time.Hour), "daily_send_cap",
				map[string]any{"daily_send_cap": customer.DailySendCap, "sent_today": sentToday}, log)
		}
	}

	// 5. Warmup / daily cap for the chosen pool.
	if cap := route.WarmupCap(decision.Pool, now); cap >= 0 {
		sent, err := p.st.CountSendsToday(ctx, decision.Pool.ID)
		if err != nil {
			return fmt.Errorf("count pool sends: %w", err)
		}
		if sent >= cap {
			metrics.Inc("messages_throttled_total")
			log.Info("ip pool over warmup/daily cap; deferring",
				"cap", cap, "sent_today", sent, "warmup_day", route.WarmupDay(decision.Pool, now))
			// Reschedule without consuming a delivery attempt — this is a
			// capacity limit, not a delivery failure.
			return p.reschedule(ctx, msg, decision, now.Add(time.Hour), "warmup_limit",
				map[string]any{"cap": cap, "sent_today": sent}, log)
		}
	}

	// 6. Rate-limit throttles (customer / provider / domain / ip_pool scopes).
	if throttled, scope, err := p.checkThrottles(ctx, msg, decision); err != nil {
		return err
	} else if throttled {
		metrics.Inc("messages_throttled_total")
		log.Info("throttled; deferring", "scope", scope)
		return p.reschedule(ctx, msg, decision, now.Add(time.Minute), "rate_limited",
			map[string]any{"throttle_scope": scope}, log)
	}

	// 7. Attempt the (simulated) send.
	attempt := msg.AttemptCount + 1
	if err := p.st.UpdateMessageStatus(ctx, msg.ID, "sending",
		intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt, sql.NullTime{}); err != nil {
		return fmt.Errorf("mark sending: %w", err)
	}
	if err := p.st.WriteEvent(ctx, msg.ID, "sending_attempted", msg.MailboxProvider,
		sql.NullInt64{}, "", "", map[string]any{
			"attempt":    attempt,
			"route_id":   decision.Route.ID,
			"ip_pool_id": decision.Pool.ID,
		}); err != nil {
		return fmt.Errorf("write sending event: %w", err)
	}
	metrics.Inc("delivery_attempts_total")

	policy, err := p.st.GetProviderPolicy(ctx, msg.MailboxProvider)
	if err != nil {
		return fmt.Errorf("load provider policy: %w", err)
	}
	result := simulate.Simulate(msg.ToEmail, policy.ReputationState, p.rng)

	if err := p.st.WriteDeliveryAttempt(ctx, msg.ID, attempt, msg.MailboxProvider,
		intToNull(decision.Pool.ID), result.SMTPCode, result.RawResponse, result.Classification); err != nil {
		return fmt.Errorf("write delivery attempt: %w", err)
	}
	if result.Classification == string(classify.RateLimited) {
		metrics.Inc("provider_rate_limited_total")
	}

	log = log.With("attempt", attempt, "smtp_code", result.SMTPCode, "classification", result.Classification)
	return p.applyOutcome(ctx, msg, decision, attempt, result, now, log)
}

// applyOutcome updates the message based on the delivery result.
func (p *Processor) applyOutcome(
	ctx context.Context, msg *store.Message, decision *route.Decision,
	attempt int, result store.DeliveryResult, now time.Time, log *slog.Logger,
) error {
	smtp := sql.NullInt64{Int64: int64(result.SMTPCode), Valid: result.SMTPCode != 0}
	meta := map[string]any{"reason": result.Reason}

	switch classify.Kind(result.Classification) {
	case classify.Delivered:
		metrics.Inc("messages_delivered_total")
		log.Info("delivered")
		if err := p.st.UpdateMessageStatus(ctx, msg.ID, "delivered",
			intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt, sql.NullTime{}); err != nil {
			return err
		}
		return p.st.WriteEvent(ctx, msg.ID, "delivered", msg.MailboxProvider, smtp,
			result.RawResponse, result.Classification, meta)

	case classify.HardBounce:
		metrics.Inc("messages_bounced_total")
		log.Info("hard bounce; suppressing recipient")
		if err := p.st.AddSuppression(ctx, msg.CustomerID, msg.ToEmail, "hard_bounce"); err != nil {
			return fmt.Errorf("add suppression: %w", err)
		}
		if err := p.st.UpdateMessageStatus(ctx, msg.ID, "bounced",
			intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt, sql.NullTime{}); err != nil {
			return err
		}
		return p.st.WriteEvent(ctx, msg.ID, "bounced", msg.MailboxProvider, smtp,
			result.RawResponse, result.Classification, meta)

	case classify.PolicyRejection:
		metrics.Inc("messages_bounced_total")
		log.Info("policy rejection; failing without retry")
		if err := p.st.UpdateMessageStatus(ctx, msg.ID, "failed",
			intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt, sql.NullTime{}); err != nil {
			return err
		}
		return p.st.WriteEvent(ctx, msg.ID, "bounced", msg.MailboxProvider, smtp,
			result.RawResponse, result.Classification, meta)

	default: // retryable: soft_bounce, provider_deferral, rate_limited, transient_failure
		delay, exhausted := backoff.NextDelay(attempt, p.maxAttempts, p.rng)
		if exhausted {
			metrics.Inc("messages_dead_lettered_total")
			log.Warn("retries exhausted; dead-lettering")
			if err := p.st.UpdateMessageStatus(ctx, msg.ID, "dead_lettered",
				intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt, sql.NullTime{}); err != nil {
				return err
			}
			return p.st.WriteEvent(ctx, msg.ID, "dead_lettered", msg.MailboxProvider, smtp,
				result.RawResponse, result.Classification, meta)
		}

		nextAt := now.Add(delay)
		metrics.Inc("messages_deferred_total")
		metrics.Inc("retry_scheduled_total")
		log.Info("deferred; retry scheduled", "next_attempt_at", nextAt.Format(time.RFC3339), "delay", delay.String())
		if err := p.st.UpdateMessageStatus(ctx, msg.ID, "deferred",
			intToNull(decision.Route.ID), intToNull(decision.Pool.ID), attempt,
			sql.NullTime{Time: nextAt, Valid: true}); err != nil {
			return err
		}
		if err := p.st.WriteEvent(ctx, msg.ID, "deferred", msg.MailboxProvider, smtp,
			result.RawResponse, result.Classification, meta); err != nil {
			return err
		}
		return p.st.WriteEvent(ctx, msg.ID, "retry_scheduled", msg.MailboxProvider, sql.NullInt64{},
			"", result.Classification, map[string]any{
				"next_attempt_at": nextAt.Format(time.RFC3339),
				"attempt":         attempt,
			})
	}
}

// checkThrottles evaluates all matching throttle rules for the message. Returns
// (true, scopeKeyThatTripped, nil) if any scope is over its limit.
func (p *Processor) checkThrottles(ctx context.Context, msg *store.Message, decision *route.Decision) (bool, string, error) {
	rules, err := p.st.GetThrottleRules(ctx)
	if err != nil {
		return false, "", fmt.Errorf("load throttle rules: %w", err)
	}
	for _, r := range rules {
		key, ok := scopeKeyFor(r.Scope, msg, decision)
		if !ok || key != r.ScopeKey {
			continue
		}
		dec, err := p.limiter.Allow(ctx, r.Scope, r.ScopeKey, r.MessagesPerMinute)
		if err != nil {
			return false, "", err
		}
		if !dec.Allowed {
			return true, fmt.Sprintf("%s:%s", r.Scope, r.ScopeKey), nil
		}
	}
	return false, "", nil
}

// scopeKeyFor returns the value of the throttle scope for this message.
func scopeKeyFor(scope string, msg *store.Message, decision *route.Decision) (string, bool) {
	switch scope {
	case "customer":
		return msg.CustomerID, true
	case "provider":
		return msg.MailboxProvider, true
	case "domain":
		return msg.RecipientDomain, true
	case "ip_pool":
		return strconv.Itoa(decision.Pool.ID), true
	default:
		return "", false
	}
}

// reschedule defers a message without consuming a delivery attempt (used for
// warmup and throttle limits, which are capacity constraints, not failures).
func (p *Processor) reschedule(
	ctx context.Context, msg *store.Message, decision *route.Decision,
	nextAt time.Time, classification string, meta map[string]any, log *slog.Logger,
) error {
	if err := p.st.UpdateMessageStatus(ctx, msg.ID, "deferred",
		intToNull(decision.Route.ID), intToNull(decision.Pool.ID), msg.AttemptCount,
		sql.NullTime{Time: nextAt, Valid: true}); err != nil {
		return err
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["next_attempt_at"] = nextAt.Format(time.RFC3339)
	return p.st.WriteEvent(ctx, msg.ID, "throttled", msg.MailboxProvider,
		sql.NullInt64{}, "", classification, meta)
}

// terminate sets a terminal status and writes an event (no route/pool context).
func (p *Processor) terminate(ctx context.Context, msg *store.Message, status, eventType, provider string, meta map[string]any, log *slog.Logger) error {
	return p.terminateEvent(ctx, msg, status, eventType, provider, sql.NullInt64{}, "", "", meta, log)
}

func (p *Processor) terminateEvent(
	ctx context.Context, msg *store.Message, status, eventType, provider string,
	smtp sql.NullInt64, raw, classification string, meta map[string]any, log *slog.Logger,
) error {
	if err := p.st.UpdateMessageStatus(ctx, msg.ID, status,
		sql.NullInt64{}, sql.NullInt64{}, msg.AttemptCount, sql.NullTime{}); err != nil {
		return err
	}
	return p.st.WriteEvent(ctx, msg.ID, eventType, provider, smtp, raw, classification, meta)
}

// RunAbuseSweep evaluates every customer's 24h hard-bounce rate and moves
// trusted/new senders over the threshold into the limited state (slow down,
// don't fully block — matching the plan's anti-abuse philosophy).
func (p *Processor) RunAbuseSweep(ctx context.Context) {
	ids, err := p.st.GetCustomerIDs(ctx)
	if err != nil {
		p.log.Error("abuse sweep: list customers failed", "error", err)
		return
	}
	for _, id := range ids {
		c, err := p.st.GetCustomer(ctx, id)
		if err != nil || c == nil {
			continue
		}
		if c.SendingState != "trusted" && c.SendingState != "new" {
			continue
		}
		rate, err := p.st.GetCustomerBounceRate(ctx, id)
		if err != nil {
			p.log.Error("abuse sweep: bounce rate failed", "customer_id", id, "error", err)
			continue
		}
		if rate > hardBounceThresholdPct {
			if err := p.st.UpdateCustomerSendingState(ctx, id, "limited"); err != nil {
				p.log.Error("abuse sweep: limit customer failed", "customer_id", id, "error", err)
				continue
			}
			metrics.Inc("abuse_limited_customers_total")
			p.log.Warn("customer moved to limited due to bounce rate",
				"customer_id", id, "bounce_rate_pct", rate, "threshold_pct", hardBounceThresholdPct)
		}
	}
}

func intToNull(v int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(v), Valid: true}
}

func classificationMeta(reason string) map[string]any {
	return map[string]any{"reason": reason}
}
