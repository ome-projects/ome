package types

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// terminalWaitingReasons enumerates kubelet container state.Waiting
// reasons that cannot self-recover. Every entry below is a wedge that
// requires either a spec change (fix the image tag) or a node-level fix
// (registry creds).
//
// The set covers both pull-side failures (image cannot be obtained) and
// post-pull failures (image obtained but the container cannot run or
// stay running): every reason that lands a container in Waiting forever
// without hope of self-recovery belongs here. It is Kubernetes API
// semantics, identical on every cluster, so it is a package constant
// rather than config.
//
// This is the set the terminal-failure escalation, the failure-record
// builder and the op-less crash-loop repair all read, through
// IsTerminalWaiting, so a wedge one of them acts on is a wedge the others
// recognize.
var terminalWaitingReasons = map[string]struct{}{
	"ErrImagePull":               {},
	"ImagePullBackOff":           {},
	"InvalidImageName":           {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	// CrashLoopBackOff lands when the container repeatedly exits
	// non-zero and kubelet's restart backoff has converged on its
	// per-pod cap (~5 minutes between attempts). Common operator-
	// reported triggers: bad command-line args, missing config, image
	// with broken entrypoint. Permanent until the spec changes — even
	// if the pod briefly transitions through Running between attempts,
	// the kubelet's stable steady-state for a wedged container is
	// Waiting{Reason=CrashLoopBackOff}. Without terminal classification
	// here, a bumped image that pulls cleanly but crashes immediately
	// would sit at Phase=Updating until InstanceReadyTimeout expires.
	"CrashLoopBackOff": {},
	// RunContainerError signals the container runtime rejected the
	// start (corrupt manifest, missing required shared libraries,
	// exec-format mismatch — e.g., amd64 image on arm64 node).
	// Permanent until the image is fixed; the controller cannot retry
	// past a broken binary.
	"RunContainerError": {},
}

// createDeadlineMessages are the whole messages gRPC's client writes on a
// CreateContainerError when the kubelet's CreateContainer call outlived
// its deadline: the client's own deadline text, or the stream reset the
// runtime sends when the deadline it was given runs out. Only the whole
// message counts. A deadline wrapped inside a longer runtime error is the
// runtime refusing the create (a hung snapshotter, say), not a create
// still running. These are wire texts, not operator settings, so they
// are fixed here rather than config.
var createDeadlineMessages = map[string]struct{}{
	"context deadline exceeded":                               {},
	"stream terminated by RST_STREAM with error code: CANCEL": {},
}

// The runtime's answer to the kubelet's retry while the first create of
// the same container is still running: the name is reserved for that
// create. Both parts must appear; the first alone also covers names the
// runtime rejects outright.
const (
	createReservedPrefix = "failed to reserve container name"
	createReservedFor    = "is reserved for"
)

// IsTerminalWaitingReason reports whether a kubelet container waiting
// reason is one a container cannot get itself out of. A reader that holds
// the waiting state and asks whether the container can still start uses
// IsTerminalWaiting, which also reads its message.
func IsTerminalWaitingReason(reason string) bool {
	_, ok := terminalWaitingReasons[reason]
	return ok
}

// IsTerminalWaiting reports whether a container's waiting state is one the
// container cannot get itself out of: a terminal reason, unless it is a
// create the runtime has not finished. Such a create clears on its own
// once the runtime finishes it, so it waits as ContainerCreating does: the
// operation deadline bounds the wait when instanceReadyTimeout is set, and
// with none set nothing does.
func IsTerminalWaiting(w *corev1.ContainerStateWaiting) bool {
	return w != nil && IsTerminalWaitingReason(w.Reason) && !createInFlight(w)
}

// createInFlight reports whether a CreateContainerError names a create the
// runtime is still working on rather than one it refused.
func createInFlight(w *corev1.ContainerStateWaiting) bool {
	if w.Reason != "CreateContainerError" {
		return false
	}
	if _, ok := createDeadlineMessages[w.Message]; ok {
		return true
	}
	return strings.HasPrefix(w.Message, createReservedPrefix) && strings.Contains(w.Message, createReservedFor)
}
