package evidence

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// PodStuckInTerminalWaiting returns (reason, true) when pod has at
// least one container — regular or init — parked in a terminal waiting
// reason for at least grace, otherwise ("", false).
//
// Stuck duration is measured from CreationTimestamp, since the kubelet
// exposes no per-state transition time on a waiting container. For
// ImagePullBackOff the kubelet retries at least once before flipping to
// BackOff (~30s on a 404), so the operator's grace is what separates a
// stuck pull from the brief PodInitializing / ContainerCreating /
// ImagePulling window. A pod with no creation stamp cannot prove any
// duration and is never stuck.
func PodStuckInTerminalWaiting(pod *corev1.Pod, now time.Time, grace time.Duration) (string, bool) {
	if pod == nil || pod.CreationTimestamp.IsZero() {
		return "", false
	}
	if now.Sub(pod.CreationTimestamp.Time) < grace {
		return "", false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting == nil {
				continue
			}
			if types.IsTerminalWaitingReason(cs.State.Waiting.Reason) {
				return cs.State.Waiting.Reason, true
			}
		}
	}
	return "", false
}

// FirstStuckPodForInstance returns the first pod in pods that is stuck
// in a terminal kubelet waiting state past the grace window, plus the
// kubelet reason string. Returns (nil, "") when no pod is stuck —
// caller skips escalation.
//
// "First" is order-deterministic by the input slice; callers that need
// a stable index should sort pods before calling. For event emission
// the order doesn't matter (the message names the specific pod).
func FirstStuckPodForInstance(pods []*corev1.Pod, now time.Time, grace time.Duration) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		// Skip pods marked for deletion — kubelet may be tearing them
		// down, and "ImagePullBackOff on a deleting pod" is not an
		// escalation signal (the pod is about to be gone anyway).
		if pod.DeletionTimestamp != nil {
			continue
		}
		if reason, stuck := PodStuckInTerminalWaiting(pod, now, grace); stuck {
			return pod, reason
		}
	}
	return nil, ""
}

// PodsForStuckCheck returns the pods whose stuck state should escalate s.
// During a gang Update surge, inspect the replacement gang exclusively: the
// source stays on the prior revision by design and may be the broken workload
// this corrective rollout is replacing. Treating that old failure as a surge
// failure makes the recovery path delete each replacement immediately. An
// empty replacement bucket is also intentional while its pods are created.
//
// Migrate also uses SurgeIndex, including as a reverse sibling pointer on its
// target status. It keeps the own-plus-sibling classification; its failure
// semantics are separate from the gang Update state machine.
func PodsForStuckCheck(s types.InstanceStatus, byIdx map[int32][]*corev1.Pod) []*corev1.Pod {
	own := byIdx[s.Index]
	if s.Operation == nil || s.Operation.SurgeIndex == nil {
		return own
	}
	sibling := byIdx[*s.Operation.SurgeIndex]
	if s.Operation.Type == types.InstanceOperationUpdate {
		return sibling
	}
	if len(sibling) == 0 {
		return own
	}
	out := make([]*corev1.Pod, 0, len(own)+len(sibling))
	out = append(out, own...)
	out = append(out, sibling...)
	return out
}

// FirstWorkloadCausedPod returns the first live (non-deleting) pod whose
// failure is deterministically scoped to the revision, plus the matched
// reason: a workload-caused container or init-container waiting reason
// (types.IsWorkloadCausedReason).
//
// Readiness limbo is deliberately NOT in this set. A pod that runs yet
// never reports ready is ambiguous between the revision and the node it
// landed on, so it must reach the relocation branch first, exactly as an
// attempt with no evidence at all does. It contributes EVIDENCE for
// whichever branch runs (FirstRunningNotReadyPod), never the branch
// choice.
func FirstWorkloadCausedPod(pods []*corev1.Pod) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if types.IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
		for _, cs := range pod.Status.InitContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if types.IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
	}
	return nil, ""
}

// FirstPodWaitingForReason returns the first live pod with a container —
// regular or init — parked in the named waiting reason. It answers "which
// pod is the one the caller already classified", so the caller's cleanup
// removes exactly that object rather than the whole set.
func FirstPodWaitingForReason(pods []*corev1.Pod, reason string) *corev1.Pod {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
			for _, status := range statuses {
				if status.State.Waiting != nil && status.State.Waiting.Reason == reason {
					return pod
				}
			}
		}
	}
	return nil
}
