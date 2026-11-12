package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNextRetryDelay(t *testing.T) {
	p := &RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2.0}
	cases := []struct {
		attempts int32
		want     time.Duration
	}{
		{1, time.Minute},       // first failure → initial
		{2, 2 * time.Minute},   // initial * m^1
		{3, 4 * time.Minute},   // initial * m^2
		{10, 30 * time.Minute}, // capped at MaxDelay
	}
	for _, c := range cases {
		if got := p.NextRetryDelay(c.attempts); got != c.want {
			t.Errorf("NextRetryDelay(%d) = %v, want %v", c.attempts, got, c.want)
		}
	}
}

func TestRetryPolicyExhausted(t *testing.T) {
	p := &RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2.0}
	if p.Exhausted(2) {
		t.Error("2 < 3 attempts must not be exhausted")
	}
	if !p.Exhausted(3) {
		t.Error("3 >= 3 attempts must be exhausted")
	}
	var nilP *RetryPolicy
	if !nilP.Exhausted(0) {
		t.Error("nil policy (unconfigured) is always exhausted → fail-safe Held")
	}
}

func TestFindRetryBlock(t *testing.T) {
	blocks := []RetryBlock{{TargetRevision: "a"}, {TargetRevision: "b"}}
	if got := FindRetryBlock(blocks, "b"); got == nil || got.TargetRevision != "b" {
		t.Errorf("FindRetryBlock(b) = %v", got)
	}
	if got := FindRetryBlock(blocks, "c"); got != nil {
		t.Errorf("FindRetryBlock(missing) must be nil, got %v", got)
	}
	// Returned pointer aliases the slice element (callers mutate in place).
	FindRetryBlock(blocks, "a").AttemptsStarted = 7
	if blocks[0].AttemptsStarted != 7 {
		t.Error("FindRetryBlock must return a pointer into the slice")
	}
}

// TestRowOnFailingRevision names the revision a row is read on and the
// records that make it failing: the attempt's target first, else the running
// revision; Held and Backoff always, an attempt in flight only when the
// row remembers a crash of its promoted set, and nothing with no record.
func TestRowOnFailingRevision(t *testing.T) {
	readyAt := metav1.NewTime(time.Date(2026, 2, 3, 4, 0, 0, 0, time.UTC))
	crashedAfter := metav1.NewTime(readyAt.Add(time.Minute))
	crashedBefore := metav1.NewTime(readyAt.Add(-time.Minute))
	block := func(rev string, state RetryBlockState) []RetryBlock {
		return []RetryBlock{{TargetRevision: rev, State: state, AttemptsStarted: 1}}
	}
	cases := []struct {
		name   string
		row    *InstanceStatus
		blocks []RetryBlock
		want   bool
	}{
		{"nil row", nil, block("v2", RetryBlockHeld), false},
		{"no revision on the row", &InstanceStatus{}, block("v2", RetryBlockHeld), false},
		{"no record for the revision", &InstanceStatus{RunningRevision: "v1"}, block("v2", RetryBlockHeld), false},
		{"running revision held", &InstanceStatus{RunningRevision: "v2"}, block("v2", RetryBlockHeld), true},
		{"running revision in backoff", &InstanceStatus{RunningRevision: "v2"}, block("v2", RetryBlockBackoff), true},
		{"the target decides over the running revision", &InstanceStatus{RunningRevision: "v1", TargetRevision: "v2"}, block("v2", RetryBlockHeld), true},
		{"a sound target over a held running revision", &InstanceStatus{RunningRevision: "v1", TargetRevision: "v2"}, block("v1", RetryBlockHeld), false},
		{"attempt in flight, no crash remembered", &InstanceStatus{RunningRevision: "v2", ReadySince: &readyAt}, block("v2", RetryBlockRetryInProgress), false},
		{"attempt in flight, a crash of the set it replaced", &InstanceStatus{RunningRevision: "v2", ReadySince: &readyAt, LastFailure: &InstanceTermination{Time: crashedBefore}}, block("v2", RetryBlockRetryInProgress), false},
		{"attempt in flight, a crash after Ready", &InstanceStatus{RunningRevision: "v2", ReadySince: &readyAt, LastFailure: &InstanceTermination{Time: crashedAfter}}, block("v2", RetryBlockRetryInProgress), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RowOnFailingRevision(c.row, c.blocks); got != c.want {
				t.Fatalf("RowOnFailingRevision = %v, want %v", got, c.want)
			}
		})
	}
}
