package render

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// RenderEncoder renders the encoder: fine-tuned weights, template metadata and
// its pod. An encoder replica is one pod, so there is no worker. The spec must
// carry the runtime merge and deployment defaults. Inputs are copied; the
// caller's spec is not mutated.
func RenderEncoder(ctx context.Context, p *Piece, service *v1beta1.InferenceService, spec *v1beta1.EncoderSpec) (Rendered, error) {
	if p == nil || p.Client == nil || service == nil || spec == nil {
		return Rendered{}, fmt.Errorf("encoder rendering requires render inputs with a client, a service, and a merged spec")
	}
	service, spec = service.DeepCopy(), spec.DeepCopy()
	if err := ReconcileFineTunedWeights(p, service); err != nil {
		return Rendered{}, err
	}
	meta, err := ReconcileComponentObjectMeta(p, service, v1beta1.EncoderComponent, ComponentName(service, v1beta1.EncoderComponent), spec.Annotations, spec.Labels)
	if err != nil {
		return Rendered{}, err
	}
	primary, err := EncoderPodSpec(p, service, spec, &meta)
	if err != nil {
		return Rendered{}, err
	}
	return Rendered{Templates: Templates{ObjectMeta: meta, Primary: primary}, ComponentExt: &spec.ComponentExtensionSpec}, nil
}

// EncoderPodSpec renders the encoder's pod the way a single-pod decoder's is
// rendered, except for affinity: the accelerator class's node affinity stays
// unless spec.encoder sets a nodeAffinity of its own. The runner container is
// completed in place on spec (env, volume mounts, resources, accelerator
// argument overrides, parallelism), then the template is converted and the
// model volumes, node selector and accelerator affinity applied.
func EncoderPodSpec(p *Piece, isvc *v1beta1.InferenceService, spec *v1beta1.EncoderSpec, objectMeta *metav1.ObjectMeta) (*corev1.PodSpec, error) {
	basePodSpec := spec.PodSpec
	runnerSpec := spec.Runner
	if runnerSpec != nil {
		UpdateEnvVariables(p, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(p, isvc, &runnerSpec.Container, objectMeta)
		MergeRoleResources(p, isvc, v1beta1.EncoderComponent, &runnerSpec.Container)
		MergeRuntimeArgumentsOverride(p, &runnerSpec.Container)
		if !AcceleratorProvidesParallelismOverride(p) {
			// One pod per replica: one leader, no workers.
			SetParallelismEnvVar(p, &runnerSpec.Container, 1, 0)
		}
	}

	podSpec, err := (&PodSpecReconciler{}).ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(p, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(p, isvc, podSpec, v1beta1.EncoderComponent)
	UpdateEncoderAffinity(p, isvc, podSpec)

	p.Log.V(1).Info("Encoder PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}
