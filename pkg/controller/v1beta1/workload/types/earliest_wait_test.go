package types

import (
	"testing"
	"time"
)

func TestEarliestPositive(t *testing.T) {
	cases := []struct {
		name               string
		current, candidate time.Duration
		want               time.Duration
	}{
		{name: "nothing yet and no candidate", want: 0},
		{name: "first positive candidate is kept", candidate: 5 * time.Second, want: 5 * time.Second},
		{name: "zero candidate keeps the current wait", current: 5 * time.Second, want: 5 * time.Second},
		{name: "negative candidate keeps the current wait", current: 5 * time.Second, candidate: -time.Second, want: 5 * time.Second},
		{name: "positive candidate replaces a negative current", current: -time.Second, candidate: 5 * time.Second, want: 5 * time.Second},
		{name: "earlier candidate wins", current: 5 * time.Second, candidate: 3 * time.Second, want: 3 * time.Second},
		{name: "later candidate loses", current: 3 * time.Second, candidate: 5 * time.Second, want: 3 * time.Second},
		{name: "equal candidate changes nothing", current: 5 * time.Second, candidate: 5 * time.Second, want: 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EarliestPositive(tc.current, tc.candidate); got != tc.want {
				t.Fatalf("EarliestPositive(%v, %v): got %v want %v", tc.current, tc.candidate, got, tc.want)
			}
		})
	}
}
