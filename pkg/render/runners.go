package render

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
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

// TemplateObjectMeta is the PodTemplateSpec.ObjectMeta stamped on every runner:
// the rendered component labels and annotations with the component's declared
// labels and annotations layered on top (last write wins, so an annotation edit
// on the component reaches the template and the revision hash). Name, namespace
// and the other identity fields are dropped so the owner's identity never
// reaches the pods.
func TemplateObjectMeta(meta metav1.ObjectMeta, ext *v1beta1.ComponentExtensionSpec) metav1.ObjectMeta {
	labels := copyMap(meta.Labels)
	annotations := copyMap(meta.Annotations)
	if ext != nil {
		if len(ext.Labels) > 0 {
			if labels == nil {
				labels = make(map[string]string, len(ext.Labels))
			}
			for k, v := range ext.Labels {
				labels[k] = v
			}
		}
		if len(ext.Annotations) > 0 {
			if annotations == nil {
				annotations = make(map[string]string, len(ext.Annotations))
			}
			for k, v := range ext.Annotations {
				annotations[k] = v
			}
		}
	}
	return metav1.ObjectMeta{Labels: labels, Annotations: annotations}
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

// copyMap returns a shallow copy of in; a nil map copies to nil so callers
// keep the distinction between absent and empty labels.
func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
