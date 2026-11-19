package render

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestRenderEngineAcceleratorEnvOrder(t *testing.T) {
	for _, multiPod := range []bool{false, true} {
		name := "single pod"
		if multiPod {
			name = "leader and worker"
		}
		t.Run(name, func(t *testing.T) {
			const accelerator = "test-gpu"
			format := &v1beta1.SupportedModelFormat{
				AcceleratorConfig: map[string]*v1beta1.AcceleratorModelConfig{
					accelerator: {EnvironmentOverride: map[string]string{
						"Z_NEW": "z", "M_EXISTING": "", "D_NEW": "d",
						"B_NEW": "b", "A_NEW": "a", "Z_EXISTING": "replaced", "C_NEW": "c",
					}},
				},
			}
			preservedSource := corev1.EnvVar{Name: "KEEP_SOURCE", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"},
			}}
			runner := func() *v1beta1.RunnerSpec {
				return &v1beta1.RunnerSpec{Container: corev1.Container{
					Name: "runner", Image: "example.com/engine:v1",
					Env: []corev1.EnvVar{
						{Name: "Z_EXISTING", ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "test-secret"}, Key: "value"},
						}},
						*preservedSource.DeepCopy(),
						{Name: "M_EXISTING", Value: "old"},
					},
				}}
			}
			spec := &v1beta1.EngineSpec{Runner: runner()}
			if multiPod {
				spec.Runner = nil
				spec.Leader = &v1beta1.LeaderSpec{Runner: runner()}
				spec.Worker = &v1beta1.WorkerSpec{Runner: runner()}
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"}}
			piece := &Piece{
				Client: newRenderClient(t), Log: logr.Discard(),
				InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{},
				SupportedModelFormat:   format, AcceleratorClassName: accelerator,
			}
			beforeSpec, beforeFormat := spec.DeepCopy(), format.DeepCopy()
			wantEnv := []corev1.EnvVar{
				{Name: "Z_EXISTING", Value: "replaced"},
				preservedSource,
				{Name: "M_EXISTING", Value: ""},
				{Name: "A_NEW", Value: "a"},
				{Name: "B_NEW", Value: "b"},
				{Name: "C_NEW", Value: "c"},
				{Name: "D_NEW", Value: "d"},
				{Name: "Z_NEW", Value: "z"},
			}
			var first Templates
			for i := 0; i < 64; i++ {
				got, err := RenderEngine(t.Context(), piece, service, spec)
				if err != nil {
					t.Fatal(err)
				}
				pods := []*corev1.PodSpec{got.Primary}
				if multiPod {
					pods = append(pods, got.Worker)
				}
				for _, pod := range pods {
					if pod == nil || len(pod.Containers) != 1 {
						t.Fatalf("render %d: expected one runner container", i)
					}
					if diff := cmp.Diff(wantEnv, pod.Containers[0].Env); diff != "" {
						t.Fatalf("render %d environment (-want +got):\n%s", i, diff)
					}
				}
				if i == 0 {
					first = got.Templates
				} else if diff := cmp.Diff(first, got.Templates); diff != "" {
					t.Fatalf("render %d changed the pod templates (-first +got):\n%s", i, diff)
				}
			}
			if diff := cmp.Diff(beforeSpec, spec); diff != "" {
				t.Fatalf("rendering mutated the engine input (-before +after):\n%s", diff)
			}
			if diff := cmp.Diff(beforeFormat, format); diff != "" {
				t.Fatalf("rendering mutated accelerator overrides (-before +after):\n%s", diff)
			}
		})
	}
}
