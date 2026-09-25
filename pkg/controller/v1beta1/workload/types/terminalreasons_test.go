package types

import "testing"

// TestIsTerminalWaitingReason pins the shared reason set: every reader of
// this vocabulary — the escalation, the failure-record builder, the
// op-less crash-loop repair — must recognize the same wedges, and must
// not claim a transient startup state as one.
func TestIsTerminalWaitingReason(t *testing.T) {
	terminal := []string{
		"ErrImagePull",
		"ImagePullBackOff",
		"InvalidImageName",
		"CreateContainerConfigError",
		"CreateContainerError",
		"CrashLoopBackOff",
		"RunContainerError",
	}
	for _, reason := range terminal {
		if !IsTerminalWaitingReason(reason) {
			t.Errorf("IsTerminalWaitingReason(%q) = false, want true", reason)
		}
	}
	transient := []string{"", "ContainerCreating", "PodInitializing", "ImagePulling"}
	for _, reason := range transient {
		if IsTerminalWaitingReason(reason) {
			t.Errorf("IsTerminalWaitingReason(%q) = true, want false", reason)
		}
	}
}
