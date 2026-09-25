package types

import (
	"testing"
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
