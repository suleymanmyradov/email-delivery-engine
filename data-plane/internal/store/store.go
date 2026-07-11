package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Message represents a row from the messages table.
type Message struct {
	ID              string
	CustomerID      string
	FromEmail       string
	FromDomain      string
	ToEmail         string
	RecipientDomain string
	MailboxProvider string
	Subject         string
	HTML            string
	Status          string
	RouteID         sql.NullInt64
	IPPoolID        sql.NullInt64
	AttemptCount    int
	NextAttemptAt   sql.NullTime
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Customer represents a row from the customers table.
type Customer struct {
	ID           string
	Name         string
	SendingState string
	DailySendCap int
}

// IPPool represents a row from the ip_pools table.
type IPPool struct {
	ID              int
	Name            string
	Type            string
	CustomerID      sql.NullString
	WarmupStartedAt sql.NullTime
	DailyLimit      sql.NullInt64
}

// Route represents a row from the routes table.
type Route struct {
	ID       int
	Name     string
	Provider string
	IPPoolID sql.NullInt64
	Weight   int
	Enabled  bool
}

// ProviderPolicy represents a row from the provider_policies table.
type ProviderPolicy struct {
	Provider        string
	ReputationState string
	DailyCap        sql.NullInt64
}

// ThrottleRule represents a row from the throttle_rules table.
type ThrottleRule struct {
	ID                int
	Scope             string
	ScopeKey          string
	MessagesPerMinute int
}

// DeliveryResult is the outcome of a simulated delivery attempt.
type DeliveryResult struct {
	SMTPCode       int
	RawResponse    string
	Classification string
	Retryable      bool
	Reason         string
}

// Store wraps database access for the worker.
type Store struct {
	db *sql.DB
}

func New(databaseURL string) (*Store, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// GetMessageForProcessing fetches a message and locks it for processing.
// Returns nil if the message is already in a terminal state.
func (s *Store) GetMessageForProcessing(ctx context.Context, messageID string) (*Message, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, customer_id, from_email, from_domain, to_email, recipient_domain,
		       mailbox_provider, subject, html, status, route_id, ip_pool_id,
		       attempt_count, next_attempt_at, created_at, updated_at
		FROM messages WHERE id = $1 FOR UPDATE SKIP LOCKED`,
		messageID,
	)

	var m Message
	err := row.Scan(
		&m.ID, &m.CustomerID, &m.FromEmail, &m.FromDomain, &m.ToEmail,
		&m.RecipientDomain, &m.MailboxProvider, &m.Subject, &m.HTML,
		&m.Status, &m.RouteID, &m.IPPoolID, &m.AttemptCount,
		&m.NextAttemptAt, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan message: %w", err)
	}
	return &m, nil
}

// GetCustomer fetches a customer by ID.
func (s *Store) GetCustomer(ctx context.Context, customerID string) (*Customer, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, sending_state, daily_send_cap FROM customers WHERE id = $1`,
		customerID,
	)
	var c Customer
	err := row.Scan(&c.ID, &c.Name, &c.SendingState, &c.DailySendCap)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan customer: %w", err)
	}
	return &c, nil
}

// IsSuppressed checks if a recipient is on the customer's suppression list.
func (s *Store) IsSuppressed(ctx context.Context, customerID, recipientEmail string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM suppression_list WHERE customer_id = $1 AND recipient_email = $2)`,
		customerID, recipientEmail,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check suppression: %w", err)
	}
	return exists, nil
}

// AddSuppression adds a recipient to the suppression list.
func (s *Store) AddSuppression(ctx context.Context, customerID, recipientEmail, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO suppression_list (customer_id, recipient_email, reason)
		 VALUES ($1, $2, $3) ON CONFLICT (customer_id, recipient_email) DO NOTHING`,
		customerID, recipientEmail, reason,
	)
	return err
}

// GetThrottleRules returns all throttle rules.
func (s *Store) GetThrottleRules(ctx context.Context) ([]ThrottleRule, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, scope, scope_key, messages_per_minute FROM throttle_rules`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []ThrottleRule
	for rows.Next() {
		var r ThrottleRule
		if err := rows.Scan(&r.ID, &r.Scope, &r.ScopeKey, &r.MessagesPerMinute); err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

// GetProviderPolicy fetches the policy for a provider.
func (s *Store) GetProviderPolicy(ctx context.Context, provider string) (*ProviderPolicy, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT provider, reputation_state, daily_cap FROM provider_policies WHERE provider = $1`,
		provider,
	)
	var p ProviderPolicy
	err := row.Scan(&p.Provider, &p.ReputationState, &p.DailyCap)
	if err == sql.ErrNoRows {
		return &ProviderPolicy{Provider: provider, ReputationState: "healthy"}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan provider policy: %w", err)
	}
	return &p, nil
}

// GetDedicatedIPPool fetches the dedicated IP pool for a customer, if any.
func (s *Store) GetDedicatedIPPool(ctx context.Context, customerID string) (*IPPool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, type, customer_id, warmup_started_at, daily_limit
		 FROM ip_pools WHERE customer_id = $1 AND type = 'dedicated' LIMIT 1`,
		customerID,
	)
	var p IPPool
	err := row.Scan(&p.ID, &p.Name, &p.Type, &p.CustomerID, &p.WarmupStartedAt, &p.DailyLimit)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan dedicated ip pool: %w", err)
	}
	return &p, nil
}

// GetSharedIPPools returns all shared IP pools.
func (s *Store) GetSharedIPPools(ctx context.Context) ([]IPPool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, type, customer_id, warmup_started_at, daily_limit
		 FROM ip_pools WHERE type = 'shared'`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pools []IPPool
	for rows.Next() {
		var p IPPool
		if err := rows.Scan(&p.ID, &p.Name, &p.Type, &p.CustomerID, &p.WarmupStartedAt, &p.DailyLimit); err != nil {
			return nil, err
		}
		pools = append(pools, p)
	}
	return pools, rows.Err()
}

// GetRoutesForProvider returns enabled routes for a given provider.
func (s *Store) GetRoutesForProvider(ctx context.Context, provider string) ([]Route, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, provider, ip_pool_id, weight, enabled
		 FROM routes WHERE provider = $1 AND enabled = true`,
		provider,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var routes []Route
	for rows.Next() {
		var r Route
		if err := rows.Scan(&r.ID, &r.Name, &r.Provider, &r.IPPoolID, &r.Weight, &r.Enabled); err != nil {
			return nil, err
		}
		routes = append(routes, r)
	}
	return routes, rows.Err()
}

// UpdateMessageStatus updates the status and related fields of a message.
func (s *Store) UpdateMessageStatus(
	ctx context.Context,
	messageID string,
	status string,
	routeID sql.NullInt64,
	ipPoolID sql.NullInt64,
	attemptCount int,
	nextAttemptAt sql.NullTime,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages
		 SET status = $1, route_id = $2, ip_pool_id = $3,
		     attempt_count = $4, next_attempt_at = $5, updated_at = now()
		 WHERE id = $6`,
		status, routeID, ipPoolID, attemptCount, nextAttemptAt, messageID,
	)
	return err
}

// WriteEvent inserts a message_events row.
func (s *Store) WriteEvent(
	ctx context.Context,
	messageID string,
	eventType string,
	provider string,
	smtpCode sql.NullInt64,
	rawResponse string,
	classification string,
	metadata map[string]any,
) error {
	var metaJSON any
	if metadata != nil {
		bytes, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		metaJSON = string(bytes)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO message_events (message_id, event_type, provider, smtp_code, raw_response, classification, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		messageID, eventType, provider, smtpCode, rawResponse, classification, metaJSON,
	)
	return err
}

// WriteDeliveryAttempt inserts a delivery_attempts row.
func (s *Store) WriteDeliveryAttempt(
	ctx context.Context,
	messageID string,
	attemptNumber int,
	provider string,
	ipPoolID sql.NullInt64,
	smtpCode int,
	rawResponse string,
	classification string,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO delivery_attempts (message_id, attempt_number, provider, ip_pool_id, smtp_code, raw_response, classification, started_at, finished_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())`,
		messageID, attemptNumber, provider, ipPoolID, smtpCode, rawResponse, classification,
	)
	return err
}

// MarkQueued flips a deferred message back to queued (preserving route/pool/
// attempt) so the deferred poller won't re-pick it once it has been re-enqueued.
// Conditional on the current status to avoid racing with a concurrent consumer.
func (s *Store) MarkQueued(ctx context.Context, messageID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages SET status = 'queued', updated_at = now()
		 WHERE id = $1 AND status = 'deferred'`,
		messageID,
	)
	return err
}

// GetDeferredMessagesReady returns deferred messages whose next_attempt_at has passed.
func (s *Store) GetDeferredMessagesReady(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM messages
		 WHERE status = 'deferred' AND next_attempt_at <= now()
		 ORDER BY next_attempt_at ASC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// CountSendsToday returns the number of messages sent via a pool today.
func (s *Store) CountSendsToday(ctx context.Context, poolID int) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM messages
		 WHERE ip_pool_id = $1 AND created_at >= CURRENT_DATE`,
		poolID,
	).Scan(&count)
	return count, err
}

// CountCustomerSendsToday returns the number of messages a customer sent today.
func (s *Store) CountCustomerSendsToday(ctx context.Context, customerID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM messages
		 WHERE customer_id = $1 AND created_at >= CURRENT_DATE`,
		customerID,
	).Scan(&count)
	return count, err
}

// GetCustomerBounceRate returns the hard bounce rate for a customer over the last 24h.
func (s *Store) GetCustomerBounceRate(ctx context.Context, customerID string) (float64, error) {
	var total, bounced int
	err := s.db.QueryRowContext(ctx,
		`SELECT
		   count(*) FILTER (WHERE status IN ('bounced', 'delivered')) as total,
		   count(*) FILTER (WHERE status = 'bounced') as bounced
		 FROM messages
		 WHERE customer_id = $1 AND created_at >= now() - interval '24 hours'`,
		customerID,
	).Scan(&total, &bounced)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 0, nil
	}
	return float64(bounced) / float64(total) * 100, nil
}

// UpdateCustomerSendingState updates a customer's sending state.
func (s *Store) UpdateCustomerSendingState(ctx context.Context, customerID, state string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE customers SET sending_state = $1 WHERE id = $2`,
		state, customerID,
	)
	return err
}

// GetCustomerIDs returns all customer IDs.
func (s *Store) GetCustomerIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM customers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
