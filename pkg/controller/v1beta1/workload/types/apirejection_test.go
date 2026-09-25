package types

import "testing"

// TestRejectionClassPermanence pins which classes end the attempt: only
// the two permanent ones. The transient classes keep the attempt alive.
func TestRejectionClassPermanence(t *testing.T) {
	for _, tc := range []struct {
		class APIRejectionClass
		want  bool
	}{
		{APIRejectionTransient, false},
		{APIRejectionThrottled, false},
		{APIRejectionCapacityBlocked, false},
		{APIRejectionPermanentWorkload, true},
		{APIRejectionPermanentEnvironment, true},
	} {
		if got := tc.class.Permanent(); got != tc.want {
			t.Errorf("%v.Permanent(): got %v want %v", tc.class, got, tc.want)
		}
	}
}

// TestInvalidPodSpecIsWorkloadCaused: a 422 blames the pod template, so
// the reason the rejection stamps on LastFailure must charge the target
// revision's retry ladder exactly as a kubelet image-pull failure does.
func TestInvalidPodSpecIsWorkloadCaused(t *testing.T) {
	if !IsWorkloadCausedReason(RejectionReasonInvalidPodSpec) {
		t.Errorf("%s: got not workload-caused want workload-caused", RejectionReasonInvalidPodSpec)
	}
	if IsWorkloadCausedReason(RejectionReasonNamespaceTerminating) {
		t.Errorf("%s: got workload-caused want environment-caused", RejectionReasonNamespaceTerminating)
	}
	if IsWorkloadCausedReason(RejectionReasonQuotaExceeded) {
		t.Errorf("%s: got workload-caused want not workload-caused", RejectionReasonQuotaExceeded)
	}
}
