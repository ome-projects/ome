package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestLifecycle_LeaderGoneGraceSeconds: the field reads in whole seconds,
// with a fraction of a second counted as a whole one, so no value above
// zero reads as a grace of zero. A field that is unset or not above zero
// reads as unset.
func TestLifecycle_LeaderGoneGraceSeconds(t *testing.T) {
	grace := func(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }
	cases := []struct {
		name    string
		field   *metav1.Duration
		want    int64
		wantSet bool
	}{
		{name: "unset"},
		{name: "zero", field: grace(0)},
		{name: "negative", field: grace(-5 * time.Second)},
		{name: "30 s", field: grace(30 * time.Second), want: 30, wantSet: true},
		{name: "1.5 s", field: grace(1500 * time.Millisecond), want: 2, wantSet: true},
		{name: "400 ms", field: grace(400 * time.Millisecond), want: 1, wantSet: true},
		{name: "1 ns", field: grace(time.Nanosecond), want: 1, wantSet: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, set := Lifecycle{LeaderGoneGracePeriod: tc.field}.LeaderGoneGraceSeconds()
			if got != tc.want || set != tc.wantSet {
				t.Fatalf("LeaderGoneGraceSeconds = %d, %v, want %d, %v", got, set, tc.want, tc.wantSet)
			}
		})
	}
}
