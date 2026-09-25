package types

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
// builder and the op-less crash-loop repair all read, so a wedge one of
// them acts on is a wedge the others recognize.
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

// IsTerminalWaitingReason reports whether a kubelet container waiting
// reason is one a container cannot get itself out of.
func IsTerminalWaitingReason(reason string) bool {
	_, ok := terminalWaitingReasons[reason]
	return ok
}
