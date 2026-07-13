//go:build integration

// Integration test for the delivery pipeline. It runs the real Processor
// against a real Postgres + Redis, so it is gated behind the `integration`
// build tag and skips unless TEST_DATABASE_URL / TEST_REDIS_URL are set.
//
// Run against the docker-compose stack (see README):
//
//	TEST_DATABASE_URL=postgresql://ede:ede@localhost:5433/email_delivery_engine \
//	TEST_REDIS_URL=redis://localhost:6380 \
//	  go test -tags=integration ./internal/worker/...
//
// It seeds its own fixtures under an "itest_" prefix and cleans up afterward,
// so it is safe to run against a database that also holds seed/demo data.
package worker

import (
	"context"
	"database/sql"
	"log/slog"
	"math/rand"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/suleyman/email-delivery-engine/internal/store"
	"github.com/suleyman/email-delivery-engine/internal/throttle"
)

func setup(t *testing.T) (*Processor, *sql.DB, func()) {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	redisURL := os.Getenv("TEST_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_REDIS_URL to run integration tests")
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping db: %v", err)
	}

	cleanup := func() {
		// Order respects FKs.
		stmts := []string{
			`DELETE FROM message_events WHERE message_id LIKE 'itest_%'`,
			`DELETE FROM delivery_attempts WHERE message_id LIKE 'itest_%'`,
			`DELETE FROM idempotency_keys WHERE message_id LIKE 'itest_%'`,
			`DELETE FROM suppression_list WHERE customer_id = 'itest_cus'`,
			`DELETE FROM messages WHERE id LIKE 'itest_%'`,
			`DELETE FROM routes WHERE name = 'itest-route'`,
			`DELETE FROM ip_pools WHERE name = 'itest-shared'`,
			`DELETE FROM customers WHERE id = 'itest_cus'`,
		}
		for _, s := range stmts {
			_, _ = db.ExecContext(ctx, s)
		}
	}
	cleanup() // start from a clean slate

	// Fixtures: customer, a shared IP pool, and a gmail route on that pool.
	mustExec(t, db, `INSERT INTO customers (id, name, sending_state, daily_send_cap)
		VALUES ('itest_cus', 'Integration Test', 'trusted', 1000000)`)
	var poolID int
	if err := db.QueryRowContext(ctx,
		`INSERT INTO ip_pools (name, type) VALUES ('itest-shared', 'shared') RETURNING id`,
	).Scan(&poolID); err != nil {
		cleanup()
		t.Fatalf("insert ip pool: %v", err)
	}
	mustExec(t, db,
		`INSERT INTO routes (name, provider, ip_pool_id, weight, enabled)
		 VALUES ('itest-route', 'gmail', $1, 1, true)`, poolID)

	st, err := store.New(dbURL)
	if err != nil {
		cleanup()
		t.Fatalf("store.New: %v", err)
	}
	limiter, err := throttle.NewFromURL(redisURL)
	if err != nil {
		cleanup()
		t.Fatalf("throttle.NewFromURL: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	proc := NewProcessor(st, limiter, log, 4, rand.New(rand.NewSource(1)))

	teardown := func() {
		cleanup()
		_ = st.Close()
		_ = limiter.Close()
		_ = db.Close()
	}
	return proc, db, teardown
}

func insertMessage(t *testing.T, db *sql.DB, id, to string) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO messages (id, customer_id, from_email, to_email, recipient_domain, mailbox_provider, status)
		 VALUES ($1, 'itest_cus', 'sender@ex.com', $2, 'gmail.com', 'gmail', 'queued')`,
		id, to)
}

func statusOf(t *testing.T, db *sql.DB, id string) (string, int, sql.NullTime) {
	t.Helper()
	var status string
	var attempts int
	var next sql.NullTime
	err := db.QueryRow(`SELECT status, attempt_count, next_attempt_at FROM messages WHERE id = $1`, id).
		Scan(&status, &attempts, &next)
	if err != nil {
		t.Fatalf("read status of %s: %v", id, err)
	}
	return status, attempts, next
}

func TestPipelineDelivered(t *testing.T) {
	proc, db, teardown := setup(t)
	defer teardown()

	insertMessage(t, db, "itest_deliver", "user@gmail.com")
	if err := proc.Process(context.Background(), "itest_deliver"); err != nil {
		t.Fatalf("process: %v", err)
	}
	status, attempts, _ := statusOf(t, db, "itest_deliver")
	if status != "delivered" {
		t.Fatalf("status = %q, want delivered", status)
	}
	if attempts != 1 {
		t.Fatalf("attempt_count = %d, want 1", attempts)
	}
}

func TestPipelineHardBounceSuppresses(t *testing.T) {
	proc, db, teardown := setup(t)
	defer teardown()

	insertMessage(t, db, "itest_bounce", "bounce@gmail.com")
	if err := proc.Process(context.Background(), "itest_bounce"); err != nil {
		t.Fatalf("process: %v", err)
	}
	status, _, _ := statusOf(t, db, "itest_bounce")
	if status != "bounced" {
		t.Fatalf("status = %q, want bounced", status)
	}

	var suppressed bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM suppression_list WHERE customer_id='itest_cus' AND recipient_email='bounce@gmail.com')`,
	).Scan(&suppressed); err != nil {
		t.Fatalf("check suppression: %v", err)
	}
	if !suppressed {
		t.Fatal("expected recipient to be added to the suppression list after a hard bounce")
	}
}

func TestPipelineDeferralSchedulesRetry(t *testing.T) {
	proc, db, teardown := setup(t)
	defer teardown()

	insertMessage(t, db, "itest_defer", "defer@gmail.com")
	if err := proc.Process(context.Background(), "itest_defer"); err != nil {
		t.Fatalf("process: %v", err)
	}
	status, attempts, next := statusOf(t, db, "itest_defer")
	if status != "deferred" {
		t.Fatalf("status = %q, want deferred", status)
	}
	if attempts != 1 {
		t.Fatalf("attempt_count = %d, want 1", attempts)
	}
	if !next.Valid || !next.Time.After(time.Now()) {
		t.Fatalf("next_attempt_at should be set in the future, got %v", next)
	}
}

func TestPipelineSuppressedRecipientSkipped(t *testing.T) {
	proc, db, teardown := setup(t)
	defer teardown()

	mustExec(t, db,
		`INSERT INTO suppression_list (customer_id, recipient_email, reason)
		 VALUES ('itest_cus', 'known@gmail.com', 'test')`)
	insertMessage(t, db, "itest_supp", "known@gmail.com")
	if err := proc.Process(context.Background(), "itest_supp"); err != nil {
		t.Fatalf("process: %v", err)
	}
	status, attempts, _ := statusOf(t, db, "itest_supp")
	if status != "suppressed" {
		t.Fatalf("status = %q, want suppressed", status)
	}
	if attempts != 0 {
		t.Fatalf("attempt_count = %d, want 0 (no send attempted)", attempts)
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}
