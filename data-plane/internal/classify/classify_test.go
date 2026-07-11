package classify

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		raw           string
		wantKind      Kind
		wantRetryable bool
	}{
		{"550 5.1.1 user unknown", HardBounce, false},
		{"550 No such user here", HardBounce, false},
		{"550 mailbox unavailable", HardBounce, false},
		{"452 4.2.2 mailbox full", SoftBounce, true},
		{"452 over quota", SoftBounce, true},
		{"421 4.7.0 rate limited, try later", RateLimited, true},
		{"421 too many concurrent connections", RateLimited, true},
		{"451 try again later", ProviderDeferral, true},
		{"451 greylisted", ProviderDeferral, true},
		{"554 5.7.1 message rejected as spam", PolicyRejection, false},
		{"550 blocked due to policy", PolicyRejection, false},
		{"451 temporary failure in processing", TransientFailure, true},
		{"250 2.0.0 OK queued", Delivered, false},
		{"", Delivered, false},
	}

	for _, c := range cases {
		got := Classify(c.raw)
		if got.Classification != c.wantKind {
			t.Errorf("Classify(%q) kind = %q, want %q", c.raw, got.Classification, c.wantKind)
		}
		if got.Retryable != c.wantRetryable {
			t.Errorf("Classify(%q) retryable = %v, want %v", c.raw, got.Retryable, c.wantRetryable)
		}
	}
}

// Rate-limit phrasing must classify as RateLimited even though it also contains
// "try again later" — the more specific rule has to win.
func TestClassifyRateLimitPrecedence(t *testing.T) {
	got := Classify("421 rate limited; please try again later")
	if got.Classification != RateLimited {
		t.Errorf("expected RateLimited, got %q", got.Classification)
	}
}
