package evidence

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// canonicalImage normalizes Docker Hub references for the runtime-vs-spec
// equality compare. Container runtimes may qualify a short reference with
// docker.io while the Pod spec retains the short form.
func canonicalImage(img string) string {
	const dockerHubPrefix = "docker.io/"
	if !strings.HasPrefix(img, dockerHubPrefix) {
		return img
	}
	img = strings.TrimPrefix(img, dockerHubPrefix)
	return strings.TrimPrefix(img, "library/")
}

// podRuntimeImagesMatch is the runtime-truth signal complementing
// PodImagesMatch's spec check: spec.image flips immediately on patch but
// kubelet may not have rolled the container yet, leaving the old image
// in Status.ContainerStatuses[*].Image. Only after both match has the
// in-place update actually taken effect. Statuses for containers absent
// from target (webhook-injected sidecars) are not OMENative-owned and
// are ignored — comparing them would block convergence forever.
func podRuntimeImagesMatch(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	for _, c := range target.Containers {
		if !ContainerRuntimeImageIs(pod, c.Name, c.Image) {
			return false
		}
	}
	return true
}

// ChangedContainerImages lists, by container name, the target images that
// differ from the running revision's. With no running revision every
// target container counts: nothing proves which images already run.
func ChangedContainerImages(running, target *corev1.PodSpec) map[string]string {
	if target == nil {
		return nil
	}
	runningImages := map[string]string{}
	if running != nil {
		for _, c := range running.Containers {
			runningImages[c.Name] = c.Image
		}
	}
	changed := make(map[string]string, len(target.Containers))
	for _, c := range target.Containers {
		if image, ok := runningImages[c.Name]; ok && image == c.Image {
			continue
		}
		changed[c.Name] = c.Image
	}
	return changed
}

// ContainerRuntimeImageIs reports whether the kubelet reports image for
// the named container, compared in canonical form.
func ContainerRuntimeImageIs(pod *corev1.Pod, name, image string) bool {
	if pod == nil {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == name {
			return canonicalImage(cs.Image) == canonicalImage(image)
		}
	}
	return false
}

// podRuntimeImageChangesMatch requires runtime confirmation only for
// containers whose image field differs between the running and target
// revisions. ContainerStatus.Image may name any repository tag for the
// running image, so unchanged containers are not a transition signal.
func podRuntimeImageChangesMatch(pod *corev1.Pod, running, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	for name, image := range ChangedContainerImages(running, target) {
		if !ContainerRuntimeImageIs(pod, name, image) {
			return false
		}
	}
	return true
}

// PodImagesMatch returns true when every container in target has a
// same-named pod container with the same .Image. Pod containers absent
// from target (webhook-injected sidecars) are not OMENative-owned and
// are ignored — failing on them would leave the in-place update
// declaring "patch needed" forever while patchPodImages has nothing to
// patch. Target containers absent from the pod fail the match (the pod
// can never converge to that spec in place).
func PodImagesMatch(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	got := make(map[string]string, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		got[c.Name] = c.Image
	}
	for _, c := range target.Containers {
		img, ok := got[c.Name]
		if !ok || img != c.Image {
			return false
		}
	}
	return true
}

// AnnotationsDiff computes the patch values that reconcile a pod's
// metadata.annotations toward the target revision's PodMeta:
//
//   - keys in target that are missing or have a different value on pod
//     map to their target value (add/update).
//   - keys that existed on the previous revision's PodMeta but are gone
//     from target map to nil (delete via strategic-merge JSON null).
//   - keys on pod that are foreign to both previous and target (sidecar
//     webhook injections, CNI attachments, kubelet-stamped values) are
//     left alone — they are not OMENative-owned.
//
// Returns an empty map when nothing needs to change. Order of map
// iteration doesn't affect the patch payload.
//
// Why we don't blindly replace the whole annotation set: third-party
// controllers (linkerd, istio, the Pod mutator webhook) routinely add
// annotations that aren't on any ControllerRevision. Erasing them on
// every in-place pass would break those integrations on every spec
// edit. The previous-CR diff lets OMENative remove ONLY keys it once
// authored that the user has since deleted from spec.
func AnnotationsDiff(podAnnotations, previousTargetAnnotations, newTargetAnnotations map[string]string) map[string]any {
	return metadataDiff(podAnnotations, previousTargetAnnotations, newTargetAnnotations, func(key string) bool {
		// The in-place roll's image-transition record is the roll's own
		// bookkeeping, never part of a revision's PodMeta, so the diff
		// neither adds nor deletes it.
		return key == constants.InferenceServiceInPlaceImageTransitionAnnotationKey
	})
}

// LabelsDiff is AnnotationsDiff for metadata.labels: the target's labels are
// written, the previous revision's dropped ones removed, foreign ones kept.
// Labels the workload stamps (query.WorkloadOwnedLabel) are never touched.
func LabelsDiff(podLabels, previousTargetLabels, newTargetLabels map[string]string) map[string]any {
	return metadataDiff(podLabels, previousTargetLabels, newTargetLabels, query.WorkloadOwnedLabel)
}

// metadataDiff is the add/update/delete/leave-alone rule shared by the
// label and annotation diffs; reserved names the keys neither diff may
// write or remove.
func metadataDiff(pod, previousTarget, newTarget map[string]string, reserved func(string) bool) map[string]any {
	out := map[string]any{}
	// Add/update: every key in the new target whose pod value differs.
	for k, want := range newTarget {
		if reserved(k) {
			continue
		}
		if got, ok := pod[k]; !ok || got != want {
			out[k] = want
		}
	}
	// Delete: keys that were OMENative-owned in the previous revision
	// but the user removed from spec. Skip keys that already-don't-exist
	// on the pod (no need to issue a delete for a no-op) and skip keys
	// still present in the new target (they were handled by the
	// add/update loop above).
	for k := range previousTarget {
		if reserved(k) {
			continue
		}
		if _, stillTarget := newTarget[k]; stillTarget {
			continue
		}
		if _, onPod := pod[k]; !onPod {
			continue
		}
		out[k] = nil
	}
	return out
}
