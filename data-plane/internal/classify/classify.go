// Package classify turns a raw SMTP-style response string into a delivery
// classification the worker can act on: whether to retry, and why.
//
// This is deliberately a small, rule-based matcher rather than a model. Real
// MTAs classify against large, provider-specific rule sets keyed on SMTP
// enhanced status codes (RFC 3463) and response text; this captures the shape
// of that logic for the proof-of-work project.
package classify

import "strings"

// Kind is the outcome category of a delivery attempt.
type Kind string

const (
	Delivered        Kind = "delivered"
	HardBounce       Kind = "hard_bounce"
	SoftBounce       Kind = "soft_bounce"
	ProviderDeferral Kind = "provider_deferral"
	RateLimited      Kind = "rate_limited"
	TransientFailure Kind = "transient_failure"
	PolicyRejection  Kind = "policy_rejection"
)

// Result is the classification of a single delivery attempt.
type Result struct {
	Classification Kind   `json:"classification"`
	Retryable      bool   `json:"retryable"`
	Reason         string `json:"reason"`
}

// rule maps a lowercase substring of the response to a classification.
// Order matters: the first matching rule wins, so more specific phrases must
// precede more general ones.
type rule struct {
	substr string
	result Result
}

var rules = []rule{
	{"user unknown", Result{HardBounce, false, "Recipient address does not exist"}},
	{"no such user", Result{HardBounce, false, "Recipient address does not exist"}},
	{"mailbox unavailable", Result{HardBounce, false, "Recipient mailbox is unavailable"}},
	{"does not exist", Result{HardBounce, false, "Recipient address does not exist"}},
	{"mailbox full", Result{SoftBounce, true, "Recipient mailbox is full — retry later"}},
	{"over quota", Result{SoftBounce, true, "Recipient over quota — retry later"}},
	{"rate limited", Result{RateLimited, true, "Provider rate-limited the sender"}},
	{"too many", Result{RateLimited, true, "Provider rate-limited the sender"}},
	{"try again later", Result{ProviderDeferral, true, "Provider asked sender to try again later"}},
	{"greylist", Result{ProviderDeferral, true, "Provider greylisted the message — retry later"}},
	{"deferred", Result{ProviderDeferral, true, "Provider deferred the message"}},
	{"spam", Result{PolicyRejection, false, "Rejected by provider spam policy"}},
	{"blocked", Result{PolicyRejection, false, "Rejected by provider policy"}},
	{"policy", Result{PolicyRejection, false, "Rejected by provider policy"}},
	{"temporary failure", Result{TransientFailure, true, "Temporary failure — retry"}},
	{"temporarily", Result{TransientFailure, true, "Temporary failure — retry"}},
}

// Classify inspects a raw response string and returns its classification.
// An empty or 2xx-looking response is treated as a successful delivery.
func Classify(raw string) Result {
	lower := strings.ToLower(raw)
	for _, r := range rules {
		if strings.Contains(lower, r.substr) {
			return r.result
		}
	}
	return Result{Delivered, false, "Message accepted by provider"}
}
