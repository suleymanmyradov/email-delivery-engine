// Package route selects the delivery route and IP pool for a message and
// enforces IP-warmup / daily-cap limits.
//
// Selection rules (from the plan):
//
//	If customer has a dedicated IP pool  -> use its route
//	Else                                 -> use a shared IP pool route
//	If IP pool is in warmup              -> apply the warmup day limit
//	If pool has a daily cap              -> apply it
//
// Provider-degraded routing (reduce/reroute) is modelled in the simulator via
// reputation state rather than here, to keep selection deterministic.
package route

import (
	"context"
	"fmt"
	"time"

	"github.com/suleyman/email-delivery-engine/internal/store"
)

// warmupSchedule[i] is the max sends allowed on warmup day (i+1).
// Day 6 onward is unlimited (subject to any explicit daily_limit).
var warmupSchedule = []int{50, 100, 250, 500, 1000}

// Decision is the outcome of route selection.
type Decision struct {
	Route store.Route
	Pool  store.IPPool
}

// Select chooses a route + IP pool for the message's provider on behalf of the
// customer. Returns an error if no enabled route exists for the provider.
func Select(ctx context.Context, st *store.Store, msg *store.Message, customerID string) (*Decision, error) {
	provider := msg.MailboxProvider
	if provider == "" {
		provider = "other"
	}

	routes, err := st.GetRoutesForProvider(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("load routes: %w", err)
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("no enabled route for provider %q", provider)
	}

	dedicated, err := st.GetDedicatedIPPool(ctx, customerID)
	if err != nil {
		return nil, fmt.Errorf("load dedicated pool: %w", err)
	}

	// Prefer a route pinned to the customer's dedicated pool.
	if dedicated != nil {
		for _, r := range routes {
			if r.IPPoolID.Valid && int(r.IPPoolID.Int64) == dedicated.ID {
				return &Decision{Route: r, Pool: *dedicated}, nil
			}
		}
	}

	// Otherwise pick the first route backed by a shared pool.
	shared, err := st.GetSharedIPPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("load shared pools: %w", err)
	}
	sharedByID := make(map[int]store.IPPool, len(shared))
	for _, p := range shared {
		sharedByID[p.ID] = p
	}
	for _, r := range routes {
		if r.IPPoolID.Valid {
			if p, ok := sharedByID[int(r.IPPoolID.Int64)]; ok {
				return &Decision{Route: r, Pool: p}, nil
			}
		}
	}

	return nil, fmt.Errorf("no shared or dedicated pool route available for provider %q", provider)
}

// WarmupCap returns the max sends allowed for a pool right now, or -1 for
// unlimited. It combines the warmup schedule (if the pool is warming up) with
// any explicit daily_limit, taking the more restrictive of the two.
func WarmupCap(p store.IPPool, now time.Time) int {
	cap := -1 // unlimited

	if p.WarmupStartedAt.Valid {
		day := int(now.Sub(p.WarmupStartedAt.Time).Hours()/24) + 1
		if day >= 1 && day <= len(warmupSchedule) {
			cap = warmupSchedule[day-1]
		}
		// day > len(schedule): warmup complete, no warmup-imposed cap.
	}

	if p.DailyLimit.Valid {
		dl := int(p.DailyLimit.Int64)
		if cap == -1 || dl < cap {
			cap = dl
		}
	}
	return cap
}

// WarmupDay returns the 1-indexed warmup day for a pool, or 0 if not warming up.
func WarmupDay(p store.IPPool, now time.Time) int {
	if !p.WarmupStartedAt.Valid {
		return 0
	}
	return int(now.Sub(p.WarmupStartedAt.Time).Hours()/24) + 1
}
