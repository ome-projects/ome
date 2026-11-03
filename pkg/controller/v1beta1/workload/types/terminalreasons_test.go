package types

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// createReservedMessage is the runtime's answer, in the shape it writes it,
// to a kubelet retry that found the container name still reserved by a
// first create the runtime has not finished. It is
// testfixture.CreateReservedMessage, spelled out here because testfixture
// imports this package.
const createReservedMessage = `failed to reserve container name "ome-container_example-model-engine-0-default-0_team-a_00000000-0000-4000-8000-000000000001_0"; ` +
	`check if another CreateContainer request is in progress: ` +
	`name "ome-container_example-model-engine-0-default-0_team-a_00000000-0000-4000-8000-000000000001_0" ` +
	`is reserved for "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"`

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

// TestIsTerminalWaiting_CreateStillInFlight_NotTerminal checks the
// CreateContainerError a runtime reports for a create it has not finished:
// the name still reserved by the first call, the kubelet's call past its
// CRI deadline, and the runtime cancelling that call. Each clears once the
// first create finishes, so none is terminal.
func TestIsTerminalWaiting_CreateStillInFlight_NotTerminal(t *testing.T) {
	for _, tc := range []struct{ name, message string }{
		{name: "name still reserved by the first create", message: createReservedMessage},
		{name: "call ran past the CRI deadline", message: "context deadline exceeded"},
		{name: "runtime cancelled the call at its deadline", message: "stream terminated by RST_STREAM with error code: CANCEL"},
	} {
		w := &corev1.ContainerStateWaiting{Reason: "CreateContainerError", Message: tc.message}
		if IsTerminalWaiting(w) {
			t.Errorf("%s: IsTerminalWaiting = true, want false", tc.name)
		}
	}
}

// TestIsTerminalWaiting_OtherWaitingStates_KeepTheirReason checks that only a
// CreateContainerError for a create still in flight leaves the terminal
// set: a create the runtime refused stays terminal, every other terminal
// reason stays terminal whatever its message says, and a reason outside
// the set, or no waiting state at all, is not terminal.
func TestIsTerminalWaiting_OtherWaitingStates_KeepTheirReason(t *testing.T) {
	for reason := range terminalWaitingReasons {
		if !IsTerminalWaiting(&corev1.ContainerStateWaiting{Reason: reason}) {
			t.Errorf("%s with no message: IsTerminalWaiting = false, want true", reason)
		}
	}
	for _, tc := range []struct {
		name string
		w    *corev1.ContainerStateWaiting
		want bool
	}{
		{name: "create refused", w: &corev1.ContainerStateWaiting{Reason: "CreateContainerError", Message: "failed to create containerd container: error unpacking image: no space left on device"}, want: true},
		{name: "runtime-internal deadline inside a refused create", w: &corev1.ContainerStateWaiting{Reason: "CreateContainerError", Message: "failed to create containerd container: failed to prepare snapshot: context deadline exceeded"}, want: true},
		{name: "name refused outright", w: &corev1.ContainerStateWaiting{Reason: "CreateContainerError", Message: `failed to reserve container name "": empty name`}, want: true},
		{name: "pull past its deadline", w: &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: "failed to pull and unpack image: context deadline exceeded"}, want: true},
		{name: "start past its deadline", w: &corev1.ContainerStateWaiting{Reason: "RunContainerError", Message: "context deadline exceeded"}, want: true},
		{name: "still creating", w: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
		{name: "no waiting state"},
	} {
		if got := IsTerminalWaiting(tc.w); got != tc.want {
			t.Errorf("%s: IsTerminalWaiting = %v, want %v", tc.name, got, tc.want)
		}
	}
}
