// Package simulate produces a fake SMTP response for a delivery attempt.
//
// v0 does not open real SMTP connections (see docs/tradeoffs.md). Instead the
// outcome is driven by two things:
//
//  1. Deterministic hints in the recipient local-part, so demos and tests can
//     force a specific outcome (e.g. anything@... with "bounce" hard-bounces).
//  2. The provider's reputation state, so a "degraded" or "down" provider
//     yields deferrals/rate-limits at a realistic rate — this is what the
//     incident simulation exercises.
package simulate

import (
	"math/rand"
	"strconv"
	"strings"

	"github.com/suleyman/email-delivery-engine/internal/classify"
	"github.com/suleyman/email-delivery-engine/internal/store"
)

// Simulate returns the outcome of attempting to deliver toEmail via a provider
// in the given reputation state. rng supplies the randomness for degraded
// providers; pass a seeded *rand.Rand for reproducibility.
func Simulate(toEmail, providerReputation string, rng *rand.Rand) store.DeliveryResult {
	raw := rawResponse(toEmail, providerReputation, rng)
	res := classify.Classify(raw)
	return store.DeliveryResult{
		SMTPCode:       parseSMTPCode(raw),
		RawResponse:    raw,
		Classification: string(res.Classification),
		Retryable:      res.Retryable,
		Reason:         res.Reason,
	}
}

func rawResponse(toEmail, reputation string, rng *rand.Rand) string {
	local := strings.ToLower(localPart(toEmail))

	// Deterministic hints take priority so scenarios are reproducible.
	switch {
	case strings.Contains(local, "bounce"), strings.Contains(local, "unknown"):
		return "550 5.1.1 user unknown"
	case strings.Contains(local, "full"):
		return "452 4.2.2 mailbox full"
	case strings.Contains(local, "ratelimit"):
		return "421 4.7.0 rate limited"
	case strings.Contains(local, "defer"), strings.Contains(local, "greylist"):
		return "451 4.7.1 try again later"
	case strings.Contains(local, "spam"):
		return "554 5.7.1 message rejected as spam"
	case strings.Contains(local, "tempfail"):
		return "451 4.3.0 temporary failure in processing"
	}

	// Otherwise the provider's health decides the outcome.
	switch reputation {
	case "down":
		return "421 4.7.0 try again later"
	case "degraded":
		if roll(rng) < 0.6 {
			return "421 4.7.0 rate limited"
		}
		return "250 2.0.0 OK queued as delivered"
	default: // healthy
		return "250 2.0.0 OK queued as delivered"
	}
}

func roll(rng *rand.Rand) float64 {
	if rng != nil {
		return rng.Float64()
	}
	return rand.Float64()
}

func localPart(email string) string {
	if i := strings.LastIndex(email, "@"); i >= 0 {
		return email[:i]
	}
	return email
}

// parseSMTPCode extracts the leading 3-digit status code from a response.
func parseSMTPCode(raw string) int {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return 0
	}
	code, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0
	}
	return code
}
