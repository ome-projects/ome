package render

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// RenderRouter renders the router: template metadata and its single pod. The
// spec must carry the runtime merge and deployment defaults. Inputs are
// copied; the caller's spec is not mutated. The router's service account is
// owned by its RBAC reconciler, which the caller runs and stamps onto Primary.
func RenderRouter(ctx context.Context, p *Piece, service *v1beta1.InferenceService, spec *v1beta1.RouterSpec) (Rendered, error) {
	if p == nil || p.Client == nil || service == nil || spec == nil {
		return Rendered{}, fmt.Errorf("router rendering requires render inputs with a client, a service, and a merged spec")
	}
	service, spec = service.DeepCopy(), spec.DeepCopy()
	meta, err := ReconcileComponentObjectMeta(p, service, v1beta1.RouterComponent, ComponentName(service, v1beta1.RouterComponent), spec.Annotations, spec.Labels)
	if err != nil {
		return Rendered{}, err
	}
	primary, err := RouterPodSpec(p, service, spec, &meta)
	if err != nil {
		return Rendered{}, err
	}
	t := Templates{ObjectMeta: meta, Primary: primary}
	return Rendered{Templates: t, ComponentExt: &spec.ComponentExtensionSpec}, nil
}

// RouterPodSpec renders the router's single pod: the component config is
// appended to the runner's env in place on spec, then the template is
// converted and the common volumes attached.
func RouterPodSpec(p *Piece, isvc *v1beta1.InferenceService, spec *v1beta1.RouterSpec, objectMeta *metav1.ObjectMeta) (*corev1.PodSpec, error) {
	if spec.Runner != nil {
		if spec.Config != nil {
			p.Log.V(2).Info("Adding config to router env", "inference service", isvc.Name, "namespace", isvc.Namespace)
			spec.Runner.Env = append(spec.Runner.Env, ConfigEnvVars(spec.Config)...)
		}
	}
	// Merge the runner container into the pod spec.
	podSpec, err := (&PodSpecReconciler{Log: p.Log}).ReconcilePodSpec(isvc, objectMeta, &spec.PodSpec, spec.Runner)
	if err != nil {
		return nil, err
	}

	UpdatePodSpecVolumes(p, isvc, podSpec, objectMeta)

	p.Log.V(1).Info("Router PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// ConfigEnvVars converts a component config map to env vars in sorted
// key order — map iteration order would churn the pod template hash
// across reconciles and trigger spurious rollouts.
func ConfigEnvVars(config map[string]string) []corev1.EnvVar {
	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	envs := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		envs = append(envs, corev1.EnvVar{Name: k, Value: config[k]})
	}
	return envs
}
