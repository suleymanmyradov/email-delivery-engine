package route

import (
	"database/sql"
	"testing"
	"time"

	"github.com/suleyman/email-delivery-engine/internal/store"
)

// poolWarmingSince returns a pool that started warming `days` full days before
// `now`, plus a 12h margin so it sits mid-day and is not sensitive to sub-second
// skew at day boundaries.
func poolWarmingSince(now time.Time, days int) store.IPPool {
	elapsed := time.Duration(days)*24*time.Hour + 12*time.Hour
	return store.IPPool{
		WarmupStartedAt: sql.NullTime{Time: now.Add(-elapsed), Valid: true},
	}
}

func TestWarmupCapSchedule(t *testing.T) {
	now := time.Now()
	cases := []struct {
		daysAgo int
		wantCap int
	}{
		{0, 50},   // day 1
		{1, 100},  // day 2
		{2, 250},  // day 3
		{3, 500},  // day 4
		{4, 1000}, // day 5
		{5, -1},   // day 6: warmup complete, unlimited
	}
	for _, c := range cases {
		got := WarmupCap(poolWarmingSince(now, c.daysAgo), now)
		if got != c.wantCap {
			t.Errorf("warmup day (daysAgo=%d): cap=%d want %d", c.daysAgo, got, c.wantCap)
		}
	}
}

func TestWarmupCapNoWarmupUnlimited(t *testing.T) {
	p := store.IPPool{} // no warmup, no daily limit
	if got := WarmupCap(p, time.Now()); got != -1 {
		t.Errorf("expected unlimited (-1), got %d", got)
	}
}

func TestWarmupCapDailyLimitWins(t *testing.T) {
	now := time.Now()
	// Warmup day 5 allows 1000, but an explicit daily_limit of 200 is stricter.
	p := poolWarmingSince(now, 4)
	p.DailyLimit = sql.NullInt64{Int64: 200, Valid: true}
	if got := WarmupCap(p, now); got != 200 {
		t.Errorf("expected daily limit 200 to win, got %d", got)
	}
}

func TestWarmupDay(t *testing.T) {
	now := time.Now()
	if d := WarmupDay(poolWarmingSince(now, 2), now); d != 3 {
		t.Errorf("expected warmup day 3, got %d", d)
	}
	if d := WarmupDay(store.IPPool{}, time.Now()); d != 0 {
		t.Errorf("expected 0 for non-warming pool, got %d", d)
	}
}
