package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsWorkloadCausedReason(t *testing.T) {
	for reason, want := range map[string]bool{
		"ImagePullBackOff":           true,
		"ErrImagePull":               true,
		"InvalidImageName":           true,
		"CreateContainerConfigError": true,
		"CrashLoopBackOff":           false,
		"RunContainerError":          false,
		"CreateContainerError":       false,
		"DeadlineExceeded":           false,
		"":                           false,
	} {
		if got := IsWorkloadCausedReason(reason); got != want {
			t.Errorf("IsWorkloadCausedReason(%q) = %v, want %v", reason, got, want)
		}
	}
}

func metaTimeAt(t time.Time) *metav1.Time {
	v := metav1.NewTime(t)
	return &v
}

// TestApplyUpdateFailure_EnvironmentWave pins the uncharged arm: a wave
// the revision cannot be blamed for paces the next attempt at the
// current ladder rung but never moves AttemptsStarted, never Holds, and
// leaves a Held block or a policy-less block untouched.
func TestApplyUpdateFailure_EnvironmentWave(t *testing.T) {
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
		wantNext   *metav1.Time
		wantReason string
	}{
		{
			name:     "new block paces at the first rung without counting",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev"},
			wantDisp: RetryBlockPersist, wantState: RetryBlockBackoff, wantCount: 0,
			wantNext: metaTimeAt(now.Add(time.Minute)), wantReason: "DeadlineExceeded",
		},
		{
			name:     "retry in progress one short of Held paces at its rung and never Holds",
			policy:   policy,
			block:    RetryBlock{TargetRevision: "rev", State: RetryBlockRetryInProgress, AttemptsStarted: 2, FirstFailureAt: &earlier},
			wantDisp: RetryBlockPersist, wantState: RetryBlockBackoff, wantCount: 2,
			wantNext: metaTimeAt(now.Add(2 * time.Minute)), wantReason: "DeadlineExceeded",
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
			disp, held := ApplyUpdateFailureToRetryBlock(&b, tc.policy, now, "DeadlineExceeded", false)
			if disp != tc.wantDisp || held != 0 {
				t.Fatalf("got (disposition=%v, heldAttempts=%d) want (%v, 0)", disp, held, tc.wantDisp)
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

// TestApplyUpdateFailure_WorkloadWave pins the charged arm against the
// same starting shapes: AttemptsStarted advances on every counted wave
// and the policy — nil included — decides Backoff or Held.
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
			disp, held := ApplyUpdateFailureToRetryBlock(&b, tc.policy, now, "ImagePullBackOff", true)
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
