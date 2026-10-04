package evidence

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// inPlaceImageTransitionAnnotation is the marker the in-place roll writes
// on a pod to name the images it is converging toward. It is the roll's
// own bookkeeping, never part of the revision's PodMeta, so the
// annotation diff must neither add nor delete it.
var inPlaceImageTransitionAnnotation = constants.InferenceServiceInPlaceImageTransitionAnnotationKey

// CanonicalImage normalizes Docker Hub references for the runtime-vs-spec
// equality compare. Container runtimes may qualify a short reference with
// docker.io while the Pod spec retains the short form.
func CanonicalImage(img string) string {
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
			return CanonicalImage(cs.Image) == CanonicalImage(image)
		}
	}
	return false
}

// PodRuntimeImageChangesMatch requires runtime confirmation only for
// containers whose image field differs between the running and target
// revisions. ContainerStatus.Image may name any repository tag for the
// running image, so unchanged containers are not a transition signal.
func PodRuntimeImageChangesMatch(pod *corev1.Pod, running, target *corev1.PodSpec) bool {
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
	out := map[string]any{}
	// Add/update: every key in the new target whose pod value differs.
	for k, want := range newTargetAnnotations {
		if k == inPlaceImageTransitionAnnotation {
			continue
		}
		if got, ok := podAnnotations[k]; !ok || got != want {
			out[k] = want
		}
	}
	// Delete: keys that were OMENative-owned in the previous revision
	// but the user removed from spec. Skip keys that already-don't-exist
	// on the pod (no need to issue a delete for a no-op) and skip keys
	// still present in the new target (they were handled by the
	// add/update loop above).
	for k := range previousTargetAnnotations {
		if k == inPlaceImageTransitionAnnotation {
			continue
		}
		if _, stillTarget := newTargetAnnotations[k]; stillTarget {
			continue
		}
		if _, onPod := podAnnotations[k]; !onPod {
			continue
		}
		out[k] = nil
	}
	return out
}
