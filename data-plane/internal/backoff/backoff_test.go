package backoff

import (
	"math/rand"
	"testing"
	"time"
)

func TestNextDelayExhaustion(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	// maxAttempts = 4: after the 4th attempt fails, the message is exhausted.
	if _, exhausted := NextDelay(4, 4, rng); !exhausted {
		t.Fatalf("attempt 4 of 4 should be exhausted")
	}
	if _, exhausted := NextDelay(5, 4, rng); !exhausted {
		t.Fatalf("attempt beyond max should be exhausted")
	}
	if _, exhausted := NextDelay(1, 4, rng); exhausted {
		t.Fatalf("attempt 1 of 4 should not be exhausted")
	}
}

func TestNextDelayBoundsWithJitter(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	// After attempt 1, base delay is 30s; full jitter keeps it in [15s, 30s].
	for i := 0; i < 100; i++ {
		d, exhausted := NextDelay(1, 4, rng)
		if exhausted {
			t.Fatalf("unexpected exhaustion")
		}
		if d < 15*time.Second || d > 30*time.Second {
			t.Fatalf("attempt-1 delay %v out of [15s,30s]", d)
		}
	}
}

func TestNextDelayIncreases(t *testing.T) {
	// Lower bound of each window should be non-decreasing across attempts.
	// attempt1 base 30s -> >=15s ; attempt2 base 2m -> >=60s ; attempt3 base 10m -> >=300s
	mins := []time.Duration{15 * time.Second, 60 * time.Second, 300 * time.Second}
	rng := rand.New(rand.NewSource(7))
	for attempt := 1; attempt <= 3; attempt++ {
		d, _ := NextDelay(attempt, 4, rng)
		if d < mins[attempt-1] {
			t.Fatalf("attempt %d delay %v below expected floor %v", attempt, d, mins[attempt-1])
		}
	}
}
