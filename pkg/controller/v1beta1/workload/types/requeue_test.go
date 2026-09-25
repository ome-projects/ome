package types

import (
	"testing"
	"time"
)

// TestPassRequeue pins the precedence a pass's wake-up follows: the
// earliest positive wait wins, any explicit wait beats the bare
// rate-limited backoff, and the bare form is reached only when the pass
// holds no wait at all.
func TestPassRequeue(t *testing.T) {
	cases := []struct {
		name      string
		cadence   time.Duration
		explicit  []time.Duration
		wantAfter time.Duration
		wantBare  bool
	}{
		{name: "configured cadence, nothing else due", cadence: 5 * time.Second, wantAfter: 5 * time.Second},
		{name: "earlier explicit wait beats the cadence", cadence: 5 * time.Second,
			explicit: []time.Duration{2 * time.Second}, wantAfter: 2 * time.Second},
		{name: "later explicit wait loses to the cadence", cadence: 5 * time.Second,
			explicit: []time.Duration{time.Minute}, wantAfter: 5 * time.Second},
		{name: "explicit wait beats the bare backoff",
			explicit: []time.Duration{0, 90 * time.Second}, wantAfter: 90 * time.Second},
		{name: "earliest of several explicit waits", cadence: 0,
			explicit: []time.Duration{time.Minute, 3 * time.Second, time.Hour}, wantAfter: 3 * time.Second},
		{name: "no cadence and nothing due falls back to the backoff",
			explicit: []time.Duration{0, -time.Second}, wantBare: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PassRequeue(tc.cadence, tc.explicit...)
			if tc.wantBare {
				if got.RequeueAfter != 0 {
					t.Fatalf("RequeueAfter: got %v want 0 (bare rate-limited backoff)", got.RequeueAfter)
				}
				if !got.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
					t.Fatalf("a pass with no wait must ask for the rate-limited backoff; got %+v", got)
				}
				return
			}
			if got.RequeueAfter != tc.wantAfter {
				t.Errorf("RequeueAfter: got %v want %v", got.RequeueAfter, tc.wantAfter)
			}
			if got.Requeue { //nolint:staticcheck // an explicit wait must not also set the bare flag
				t.Errorf("an explicit wait must supersede the bare backoff; got %+v", got)
			}
		})
	}
}
