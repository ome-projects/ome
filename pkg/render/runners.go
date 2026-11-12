package render

import (
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

// Templates is one component's rendered pod templates: the template metadata,
// the primary (single or leader) pod, and the worker pod with its per-Instance
// count. WorkerSize is zero and Worker nil for a single-pod component.
type Templates struct {
	ObjectMeta metav1.ObjectMeta
	Primary    *corev1.PodSpec
	Worker     *corev1.PodSpec
	WorkerSize int
	// MultiPod is true when the component declares both a leader and a worker;
	// the runners are then leader and worker instead of default.
	MultiPod bool
}

// AnnotationReachesPods reports whether an annotation of a component's object
// metadata belongs on its pod template. Three kinds do: a key the Component
// declares, a key of OME's own (the ome.io group and its subdomains), and a
// key under a prefix OME lists as pod-level configuration
// (constants.PodOnlyAnnotationPrefixes). Every other annotation the component
// metadata inherits from the service stays on the service and on the objects
// built from that metadata; a pod does not carry it, so a tool rewriting it
// changes no pod template.
func AnnotationReachesPods(key string, declared map[string]string) bool {
	if _, ok := declared[key]; ok {
		return true
	}
	domain, _, ok := strings.Cut(key, "/")
	if ok && (domain == constants.OMEAPIGroupName || strings.HasSuffix(domain, "."+constants.OMEAPIGroupName)) {
		return true
	}
	return utils.IsPrefixSupported(key, constants.PodOnlyAnnotationPrefixes)
}

// PodTemplateAnnotations is the part of a component's object-metadata
// annotations that belongs on its pod template: the keys AnnotationReachesPods
// admits, in a new map. Every backend builds its pod templates through it, so
// a service annotation no Component declares reaches no pod, whichever object
// runs the pods. A nil map stays nil.
func PodTemplateAnnotations(annotations, declared map[string]string) map[string]string {
	out := maps.Clone(annotations)
	for key := range out {
		if !AnnotationReachesPods(key, declared) {
			delete(out, key)
		}
	}
	return out
}

// TemplateObjectMeta is the PodTemplateSpec.ObjectMeta stamped on every runner:
// the rendered component labels, the rendered annotations that belong on a pod
// (PodTemplateAnnotations), and the component's declared labels and annotations
// layered on top (last write wins, so an annotation edit on the component
// reaches the template and the revision hash). Name, namespace and the other
// identity fields are dropped so the owner's identity never reaches the pods.
func TemplateObjectMeta(meta metav1.ObjectMeta, ext *v1beta1.ComponentExtensionSpec) metav1.ObjectMeta {
	var declaredLabels, declaredAnnotations map[string]string
	if ext != nil {
		declaredLabels, declaredAnnotations = ext.Labels, ext.Annotations
	}
	return metav1.ObjectMeta{
		// A nil map stays nil, so absent and empty labels stay distinct.
		Labels:      overlaid(maps.Clone(meta.Labels), declaredLabels),
		Annotations: overlaid(PodTemplateAnnotations(meta.Annotations, declaredAnnotations), declaredAnnotations),
	}
}

// overlaid writes the declared entries over base, last write wins, and
// returns base. With nothing declared, base comes back as it was, nil
// included, so absent and empty maps stay distinct.
func overlaid(base, declared map[string]string) map[string]string {
	if len(declared) == 0 {
		return base
	}
	if base == nil {
		base = make(map[string]string, len(declared))
	}
	maps.Copy(base, declared)
	return base
}

// Runners converts rendered templates to the runner list an InferenceReplica
// stores: one default runner of size 1, or a leader runner of size 1 and a
// worker runner sized WorkerSize when the component is multi-pod.
func Runners(t Templates, ext *v1beta1.ComponentExtensionSpec) []v1beta1.Runner {
	tmplMeta := TemplateObjectMeta(t.ObjectMeta, ext)
	if !t.MultiPod {
		return []v1beta1.Runner{{
			Name:     v1beta1.RunnerNameDefault,
			Size:     1,
			Template: corev1.PodTemplateSpec{ObjectMeta: tmplMeta, Spec: *t.Primary},
		}}
	}
	out := []v1beta1.Runner{{
		Name:     v1beta1.RunnerNameLeader,
		Size:     1,
		Template: corev1.PodTemplateSpec{ObjectMeta: tmplMeta, Spec: *t.Primary},
	}}
	if t.Worker != nil {
		out = append(out, v1beta1.Runner{
			Name:     v1beta1.RunnerNameWorker,
			Size:     int32(t.WorkerSize),
			Template: corev1.PodTemplateSpec{ObjectMeta: tmplMeta, Spec: *t.Worker},
		})
	}
	return out
}
