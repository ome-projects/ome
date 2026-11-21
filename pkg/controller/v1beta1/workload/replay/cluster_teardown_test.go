package replay

import (
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestShortenTeardownFollowsTheAPIServer holds the driver's answer to one
// more delete of a pod that is already terminating to what a real API
// server does with it. A delete that names a shorter grace moves the
// deletion time to the first request plus the new grace, and to the instant
// of the request, with a grace of 1 s, when that time has passed. A delete
// that names no grace, the same grace or a longer one changes nothing.
//
// The pod was deleted at t+2m with a grace of 480 s, so its deletion time
// is t+10m.
func TestShortenTeardownFollowsTheAPIServer(t *testing.T) {
	first := traceStart.Add(2 * time.Minute)
	cases := []struct {
		name string
		// asked is how long after the first delete the next one arrives.
		asked time.Duration
		opts  []client.DeleteOption
		// wantAfter is the deletion time, counted from the first delete.
		wantAfter time.Duration
		wantGrace int64
		wantTrace string
	}{
		{
			name: "30 s named 5 s after the first delete", asked: 5 * time.Second,
			opts:      []client.DeleteOption{client.GracePeriodSeconds(30)},
			wantAfter: 30 * time.Second, wantGrace: 30, wantTrace: " deletionTime=t+10m0s→t+2m30s",
		},
		{
			name: "30 s named two minutes after the first delete", asked: 2 * time.Minute,
			opts:      []client.DeleteOption{client.GracePeriodSeconds(30)},
			wantAfter: 2 * time.Minute, wantGrace: 1, wantTrace: " deletionTime=t+10m0s→t+4m0s",
		},
		{name: "no grace named", asked: 5 * time.Second, wantAfter: 480 * time.Second, wantGrace: 480},
		{
			name: "the same grace", asked: 5 * time.Second,
			opts:      []client.DeleteOption{client.GracePeriodSeconds(480)},
			wantAfter: 480 * time.Second, wantGrace: 480,
		},
		{
			name: "a longer grace", asked: 5 * time.Second,
			opts:      []client.DeleteOption{client.GracePeriodSeconds(600)},
			wantAfter: 480 * time.Second, wantGrace: 480,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &driver{norm: newNormalizer(traceStart), clock: clocktesting.NewFakeClock(first.Add(tc.asked))}
			hold := &podTeardown{at: first.Add(480 * time.Second), grace: 480}
			if got := d.shortenTeardown(hold, tc.opts); got != tc.wantTrace {
				t.Errorf("the trace adds %q, want %q", got, tc.wantTrace)
			}
			if after := hold.at.Sub(first); after != tc.wantAfter {
				t.Errorf("the deletion time is %s after the first delete, want %s", after, tc.wantAfter)
			}
			if hold.grace != tc.wantGrace {
				t.Errorf("the pod shows a grace of %d s, want %d s", hold.grace, tc.wantGrace)
			}
		})
	}
}
