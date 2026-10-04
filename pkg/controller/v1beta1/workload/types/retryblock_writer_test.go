package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func metaTimeAt(t time.Time) *metav1.Time {
	v := metav1.NewTime(t)
	return &v
}

// TestApplyUpdateFailure_UnattributedWave pins the arm for a wave the
// revision cannot be blamed for: it counts on the ladder like any failed
// attempt — AttemptsStarted advances, the wave paces at its rung and the
// one that reaches MaxAttempts Holds and reports the transition — while a
// same-wave Backoff refreshes evidence only, a Held block keeps the
// evidence that justified the hold, and a nil policy records nothing.
func TestApplyUpdateFailure_UnattributedWave(t *testing.T) {
	policy := &RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	now := metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	earlier := metav1.NewTime(now.Add(-10 * time.Minute))
	due := metav1.NewTime(now.Add(-30 * time.Second))

	cases := []struct {
		name       string
		policy     *RetryPolicy
		block      RetryBlock
		wantDisp   RetryBlockDisposition
		wantState  RetryBlockState
		wantCount  int32
		wantHeld   int32
		wantNext   *metav1.Time
		wantReason string
	}{
		{
			name:     "new block counts the first wave and paces at the first rung",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev"},
			wantDisp: RetryBlockPersist, wantState: RetryBlockBackoff, wantCount: 1,
			wantNext: metaTimeAt(now.Add(time.Minute)), wantReason: "DeadlineExceeded",
		},
		{
			name:     "retry in progress one short of MaxAttempts Holds",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 2, FirstFailureAt: &earlier},
			wantDisp: RetryBlockPersist, wantState: RetryBlockHeld, wantCount: 3, wantHeld: 3,
			wantNext: nil, wantReason: "DeadlineExceeded",
		},
		{
			name:     "same-wave Backoff refreshes evidence only",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev", State: RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &due, Reason: "first"},
			wantDisp: RetryBlockPersist, wantState: RetryBlockBackoff, wantCount: 1,
			wantNext: &due, wantReason: "DeadlineExceeded",
		},
		{
			name:     "Held keeps the evidence that justified the hold",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev", State: RetryBlockHeld, AttemptsStarted: 3, Reason: "ImagePullBackOff"},
			wantDisp: RetryBlockUnchanged, wantState: RetryBlockHeld, wantCount: 3,
			wantNext: nil, wantReason: "ImagePullBackOff",
		},
		{
			name:     "nil policy leaves the block untouched instead of holding",
			policy:   nil,
			block:    RetryBlock{TargetRevision: "rev"},
			wantDisp: RetryBlockUnchanged, wantState: "", wantCount: 0,
			wantNext: nil, wantReason: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.block
			disp, held := ApplyUpdateFailureToRetryBlock(&b, tc.policy, now, "DeadlineExceeded", CauseUnattributed)
			if disp != tc.wantDisp || held != tc.wantHeld {
				t.Fatalf("got (disposition=%v, heldAttempts=%d) want (%v, %d)", disp, held, tc.wantDisp, tc.wantHeld)
			}
			if b.State != tc.wantState || b.AttemptsStarted != tc.wantCount || b.Reason != tc.wantReason {
				t.Errorf("got (state=%q, attempts=%d, reason=%q) want (%q, %d, %q)",
					b.State, b.AttemptsStarted, b.Reason, tc.wantState, tc.wantCount, tc.wantReason)
			}
			switch {
			case tc.wantNext == nil && b.NextRetryAt != nil:
				t.Errorf("NextRetryAt: got %v want nil", b.NextRetryAt)
			case tc.wantNext != nil && (b.NextRetryAt == nil || !b.NextRetryAt.Time.Equal(tc.wantNext.Time)):
				t.Errorf("NextRetryAt: got %v want %v", b.NextRetryAt, tc.wantNext)
			}
			if disp == RetryBlockPersist && (b.LastFailureAt == nil || !b.LastFailureAt.Time.Equal(now.Time)) {
				t.Errorf("LastFailureAt: got %v want refreshed to %v", b.LastFailureAt, now)
			}
		})
	}
}

// TestApplyUpdateFailure_WorkloadWave pins the revision-blaming arm
// against the same starting shapes: AttemptsStarted advances on every
// counted wave and the policy — nil included — decides Backoff or Held.
func TestApplyUpdateFailure_WorkloadWave(t *testing.T) {
	policy := &RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	now := metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))

	cases := []struct {
		name      string
		policy    *RetryPolicy
		block     RetryBlock
		wantState RetryBlockState
		wantCount int32
		wantHeld  int32
		wantNext  *metav1.Time
	}{
		{
			name:      "new block backs off at the first rung",
			policy:    policy,
			block:     RetryBlock{TargetRevision: "rev"},
			wantState: RetryBlockBackoff, wantCount: 1, wantHeld: 0, wantNext: metaTimeAt(now.Add(time.Minute)),
		},
		{
			name:      "retry in progress one short of Held exhausts the ladder",
			policy:    policy,
			block:     RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 2},
			wantState: RetryBlockHeld, wantCount: 3, wantHeld: 3, wantNext: nil,
		},
		{
			name:      "nil policy fails safe to Held on the first wave",
			policy:    nil,
			block:     RetryBlock{TargetRevision: "rev"},
			wantState: RetryBlockHeld, wantCount: 1, wantHeld: 1, wantNext: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.block
			disp, held := ApplyUpdateFailureToRetryBlock(&b, tc.policy, now, "ImagePullBackOff", CauseWorkload)
			if disp != RetryBlockPersist || held != tc.wantHeld {
				t.Fatalf("got (disposition=%v, heldAttempts=%d) want (Persist, %d)", disp, held, tc.wantHeld)
			}
			if b.State != tc.wantState || b.AttemptsStarted != tc.wantCount || b.Reason != "ImagePullBackOff" {
				t.Errorf("got (state=%q, attempts=%d, reason=%q) want (%q, %d, ImagePullBackOff)",
					b.State, b.AttemptsStarted, b.Reason, tc.wantState, tc.wantCount)
			}
			switch {
			case tc.wantNext == nil && b.NextRetryAt != nil:
				t.Errorf("NextRetryAt: got %v want nil", b.NextRetryAt)
			case tc.wantNext != nil && (b.NextRetryAt == nil || !b.NextRetryAt.Time.Equal(tc.wantNext.Time)):
				t.Errorf("NextRetryAt: got %v want %v", b.NextRetryAt, tc.wantNext)
			}
		})
	}
}

// TestApplyUpdateFailure_EnvironmentWaveIsNeverCounted pins that a wave
// the environment caused — a scheduler hold — touches no block in any
// state, with or without a policy.
func TestApplyUpdateFailure_EnvironmentWaveIsNeverCounted(t *testing.T) {
	policy := &RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	now := metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	for _, tc := range []struct {
		name   string
		policy *RetryPolicy
		block  RetryBlock
	}{
		{"new block under a policy", policy, RetryBlock{TargetRevision: "rev"}},
		{"retry in progress one short of Held", policy, RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 2}},
		{"new block with no policy", nil, RetryBlock{TargetRevision: "rev"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.block
			b := tc.block
			disp, held := ApplyUpdateFailureToRetryBlock(&b, tc.policy, now, WaitingReasonUnschedulable, CauseEnvironment)
			if disp != RetryBlockUnchanged || held != 0 {
				t.Fatalf("got (disposition=%v, heldAttempts=%d) want (Unchanged, 0)", disp, held)
			}
			if b != before {
				t.Errorf("block mutated by an environment wave: got %+v want %+v", b, before)
			}
		})
	}
}

// TestApplyUpdateFailure_OneLadderForEveryCause pins that there is one
// counter: a workload-caused wave after an unattributed one, and an
// unattributed wave after a workload-caused one, both Hold at
// MaxAttempts.
func TestApplyUpdateFailure_OneLadderForEveryCause(t *testing.T) {
	policy := &RetryPolicy{MaxAttempts: 2, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	now := metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))

	b := RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 1, Reason: "CrashLoopBackOff"}
	if _, held := ApplyUpdateFailureToRetryBlock(&b, policy, now, "ImagePullBackOff", CauseWorkload); held != 2 || b.State != RetryBlockHeld {
		t.Fatalf("workload wave after a counted crash-loop wave: got (held=%d, state=%q) want (2, Held)", held, b.State)
	}
	b = RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 1, Reason: "ImagePullBackOff"}
	if _, held := ApplyUpdateFailureToRetryBlock(&b, policy, now, "CrashLoopBackOff", CauseUnattributed); held != 2 || b.State != RetryBlockHeld || b.Reason != "CrashLoopBackOff" {
		t.Fatalf("crash-loop wave after a counted workload wave: got (held=%d, state=%q, reason=%q) want (2, Held, CrashLoopBackOff)", held, b.State, b.Reason)
	}
}

// TestFailureCauseOf pins the classification the writer reads: the
// workload-caused set blames the revision, the environment set names the
// cluster, everything else is an attempt that failed without blame.
func TestFailureCauseOf(t *testing.T) {
	for reason, want := range map[string]FailureCause{
		"ImagePullBackOff":              CauseWorkload,
		RejectionReasonInvalidPodSpec:   CauseWorkload,
		WaitingReasonUnschedulable:      CauseEnvironment,
		PodGroupOwnershipConflictReason: CauseEnvironment,
		"CrashLoopBackOff":              CauseUnattributed,
		"RunContainerError":             CauseUnattributed,
		"DeadlineExceeded":              CauseUnattributed,
		"ContainersNotReady":            CauseUnattributed,
		"":                              CauseUnattributed,
	} {
		if got := FailureCauseOf(reason); got != want {
			t.Errorf("FailureCauseOf(%q) = %v, want %v", reason, got, want)
		}
	}
}
