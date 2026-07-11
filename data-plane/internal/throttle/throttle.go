// Package throttle enforces per-scope send-rate limits using a Redis
// fixed-window counter. The worker calls this before each send; if a scope is
// over its limit the message is rescheduled rather than sent.
//
// Key scheme:  throttle:<scope>:<scopeKey>:<minute_bucket>
// Each key is INCR'd and expired after 120s so buckets self-clean.
package throttle

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is a Redis-backed fixed-window rate limiter.
type Limiter struct {
	rdb *redis.Client
}

// NewFromURL connects to Redis and returns a Limiter.
func NewFromURL(redisURL string) (*Limiter, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return &Limiter{rdb: redis.NewClient(opt)}, nil
}

func (l *Limiter) Close() error { return l.rdb.Close() }

// Decision is the result of a throttle check.
type Decision struct {
	Allowed bool
	Count   int64
	Limit   int
}

// Allow increments the counter for (scope, scopeKey) in the current minute and
// reports whether the send is within the limit. The increment happens whether
// or not the send is allowed, matching a fixed-window limiter — callers that
// reschedule instead of sending accept the small over-count this implies.
func (l *Limiter) Allow(ctx context.Context, scope, scopeKey string, perMinute int) (Decision, error) {
	bucket := time.Now().Unix() / 60
	key := fmt.Sprintf("throttle:%s:%s:%d", scope, scopeKey, bucket)

	count, err := l.rdb.Incr(ctx, key).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("incr throttle key: %w", err)
	}
	if count == 1 {
		// First hit in this window — set TTL so the bucket cleans itself up.
		if err := l.rdb.Expire(ctx, key, 120*time.Second).Err(); err != nil {
			return Decision{}, fmt.Errorf("expire throttle key: %w", err)
		}
	}

	return Decision{
		Allowed: count <= int64(perMinute),
		Count:   count,
		Limit:   perMinute,
	}, nil
}
