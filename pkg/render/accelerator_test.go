package render

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// PARALLELISM_SIZE is the accelerators of one pod times the pods of one
// replica, and it is set only when the pod has accelerators and the replica
// has a leader or workers to count. The leader count is the caller's rule:
// the engine always passes one, the decoder passes zero for a worker-only
// shape.
func TestSetParallelismEnvVar(t *testing.T) {
	const accelerator = "example.com/gpu"
	tests := []struct {
		name       string
		gpus       string
		leaders    int
		workers    int
		wantValue  string
		wantAbsent bool
	}{
		{name: "one leader and two workers with eight accelerators", gpus: "8", leaders: 1, workers: 2, wantValue: "24"},
		{name: "no leader and no workers sets nothing", gpus: "8", leaders: 0, workers: 0, wantAbsent: true},
		{name: "workers only with two accelerators", gpus: "2", leaders: 0, workers: 3, wantValue: "6"},
		{name: "no accelerators sets nothing", gpus: "0", leaders: 1, workers: 2, wantAbsent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Piece{
				Log: logr.Discard(),
				InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{
					AcceleratorResources: []string{accelerator},
				},
			}
			container := &corev1.Container{
				Name: "runner",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{accelerator: resource.MustParse(tt.gpus)},
				},
			}

			SetParallelismEnvVar(p, container, tt.leaders, tt.workers)

			var got *corev1.EnvVar
			for i := range container.Env {
				if container.Env[i].Name == constants.ParallelismSizeEnvVarKey {
					got = &container.Env[i]
				}
			}
			if tt.wantAbsent {
				assert.Nil(t, got, "PARALLELISM_SIZE must not be set")
				return
			}
			if assert.NotNil(t, got, "PARALLELISM_SIZE must be set") {
				assert.Equal(t, tt.wantValue, got.Value)
			}
		})
	}
}
