// Package backoff computes retry delays for deferred/transient deliveries.
//
// Schedule (from the project plan):
//
//	Attempt 1: immediate (first send)
//	Attempt 2: +30s
//	Attempt 3: +2m
//	Attempt 4: +10m
//	Attempt 5: dead-letter
//
// Full jitter is applied to each delay to avoid retry storms where many
// messages deferred at the same instant all retry in lockstep.
package backoff

import (
	"math/rand"
	"time"
)

// baseDelays[i] is the delay after the (i+1)-th attempt has failed.
// index 0 -> after attempt 1, index 1 -> after attempt 2, ...
var baseDelays = []time.Duration{
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

// NextDelay returns the delay before the next attempt given the number of
// attempts already made, and whether the message should be dead-lettered
// instead of retried. maxAttempts is the total number of attempts allowed
// before dead-lettering.
//
// rng is the source of jitter; pass nil to use the package default.
func NextDelay(attemptsMade, maxAttempts int, rng *rand.Rand) (delay time.Duration, exhausted bool) {
	if attemptsMade >= maxAttempts {
		return 0, true
	}
	idx := attemptsMade - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(baseDelays) {
		idx = len(baseDelays) - 1
	}
	base := baseDelays[idx]
	return withJitter(base, rng), false
}

// withJitter applies "full jitter": a uniformly random duration in [base/2, base].
// This spreads retries out while keeping them bounded near the intended delay.
func withJitter(base time.Duration, rng *rand.Rand) time.Duration {
	half := base / 2
	var extra int64
	if rng != nil {
		extra = rng.Int63n(int64(half) + 1)
	} else {
		extra = rand.Int63n(int64(half) + 1)
	}
	return half + time.Duration(extra)
}
