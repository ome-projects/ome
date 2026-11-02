package utils

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestMergeRuntimeSpecs_SchedulerNamePrecedence pins the schedulerName
// resolution order across the full merge pipeline (component merge +
// leader/worker fold + top-level back-fill): ISVC pod-spec levels beat
// runtime component-config levels, which beat the runtime's top-level
// schedulerName; with nothing set anywhere the field stays empty.
func TestMergeRuntimeSpecs_SchedulerNamePrecedence(t *testing.T) {
	log := logr.Discard()

	t.Run("top-level name reaches component, leader, worker, and router levels", func(t *testing.T) {
		runtime := &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
			EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
			},
			DecoderConfig: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptrInt(1)},
			},
			RouterConfig: &v1beta1.RouterSpec{},
		}
		isvc := &v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
				Router:  &v1beta1.RouterSpec{},
			},
		}

		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		engine, decoder, router := specs.Engine, specs.Decoder, specs.Router
		require.NoError(t, err)
		require.NotNil(t, engine)
		require.NotNil(t, decoder)
		require.NotNil(t, router)

		assert.Equal(t, "custom-scheduler", engine.SchedulerName)
		require.NotNil(t, engine.Leader)
		assert.Equal(t, "custom-scheduler", engine.Leader.SchedulerName,
			"rendered leader pods source their template from Leader.PodSpec — it must carry the runtime-level name")
		require.NotNil(t, engine.Worker)
		assert.Equal(t, "custom-scheduler", engine.Worker.SchedulerName,
			"rendered worker pods source their template from Worker.PodSpec — it must carry the runtime-level name")
		assert.Equal(t, "custom-scheduler", decoder.SchedulerName)
		assert.Equal(t, "custom-scheduler", decoder.Leader.SchedulerName)
		assert.Equal(t, "custom-scheduler", decoder.Worker.SchedulerName)
		assert.Equal(t, "custom-scheduler", router.SchedulerName)
	})

	t.Run("runtime leader and worker levels override top-level", func(t *testing.T) {
		runtime := &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "top-scheduler"},
			EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{SchedulerName: "leader-scheduler"}},
				Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
			},
		}
		isvc := &v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		}

		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		engine := specs.Engine
		require.NoError(t, err)

		assert.Equal(t, "leader-scheduler", engine.Leader.SchedulerName)
		assert.Equal(t, "top-scheduler", engine.Worker.SchedulerName,
			"a worker level left unset must still fall back to the top-level name")
	})

	t.Run("ISVC levels override every runtime level", func(t *testing.T) {
		runtime := &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "top-scheduler"},
			EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{SchedulerName: "runtime-leader-scheduler"}},
				Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
			},
			RouterConfig: &v1beta1.RouterSpec{},
		}
		isvc := &v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{SchedulerName: "isvc-leader-scheduler"}},
					Worker: &v1beta1.WorkerSpec{PodSpec: v1beta1.PodSpec{SchedulerName: "isvc-worker-scheduler"}},
				},
				Router: &v1beta1.RouterSpec{
					PodSpec: v1beta1.PodSpec{SchedulerName: "isvc-router-scheduler"},
				},
			},
		}

		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		engine, router := specs.Engine, specs.Router
		require.NoError(t, err)

		assert.Equal(t, "isvc-leader-scheduler", engine.Leader.SchedulerName)
		assert.Equal(t, "isvc-worker-scheduler", engine.Worker.SchedulerName)
		assert.Equal(t, "isvc-router-scheduler", router.SchedulerName)
	})

	t.Run("nothing set leaves every level empty", func(t *testing.T) {
		runtime := &v1beta1.ServingRuntimeSpec{
			EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
			},
		}
		isvc := &v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		}

		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		engine := specs.Engine
		require.NoError(t, err)

		assert.Empty(t, engine.SchedulerName)
		assert.Empty(t, engine.Leader.SchedulerName)
		assert.Empty(t, engine.Worker.SchedulerName)
	})

	t.Run("top-level name applies when the runtime has no engine config", func(t *testing.T) {
		runtime := &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
		}
		isvc := &v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: ptrInt(2)},
				},
			},
		}

		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		engine := specs.Engine
		require.NoError(t, err)

		assert.Equal(t, "custom-scheduler", engine.SchedulerName)
		assert.Equal(t, "custom-scheduler", engine.Leader.SchedulerName)
		assert.Equal(t, "custom-scheduler", engine.Worker.SchedulerName)
	})
}

// The three-role signature returns the blocks the role-table merge produces.
func TestMergeRuntimeSpecsReturnsTheThreeRoleBlocks(t *testing.T) {
	runtime := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
		EngineConfig:          &v1beta1.EngineSpec{},
		DecoderConfig:         &v1beta1.DecoderSpec{},
		RouterConfig:          &v1beta1.RouterSpec{},
	}
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{},
	}}
	specs, err := MergeRuntimeRoleSpecs(isvc.DeepCopy(), runtime.DeepCopy(), logr.Discard())
	require.NoError(t, err)
	engine, decoder, router, err := MergeRuntimeSpecs(isvc.DeepCopy(), runtime.DeepCopy(), logr.Discard())
	require.NoError(t, err)
	assert.Equal(t, specs.Engine, engine)
	assert.Equal(t, specs.Decoder, decoder)
	assert.Equal(t, specs.Router, router)
	assert.Equal(t, "custom-scheduler", engine.SchedulerName)
}

// The encoder merges like the decoder: the service's block wins over the
// runtime's piece, the piece alone declares no encoder, and an inline block
// without a piece is a complete template.
func TestMergeRuntimeRoleSpecs_Encoder(t *testing.T) {
	log := logr.Discard()
	runtime := &v1beta1.ServingRuntimeSpec{
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "runtime-scheduler"},
		EngineConfig:          &v1beta1.EngineSpec{},
		EncoderConfig: &v1beta1.EncoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: 8, Labels: map[string]string{"piece": "runtime"}},
			Runner:                 &v1beta1.RunnerSpec{Container: corev1.Container{Name: "encoder", Image: "example.com/encoder:v1"}},
		},
	}

	t.Run("the service's block wins over the runtime's piece", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
			Encoder: &v1beta1.EncoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: 12},
				Runner:                 &v1beta1.RunnerSpec{Container: corev1.Container{Image: "example.com/encoder:v2"}},
			},
		}}
		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		require.NoError(t, err)
		require.NotNil(t, specs.Encoder)
		assert.Equal(t, 12, specs.Encoder.MaxReplicas)
		assert.Equal(t, "runtime", specs.Encoder.Labels["piece"])
		require.NotNil(t, specs.Encoder.Runner)
		assert.Equal(t, "example.com/encoder:v2", specs.Encoder.Runner.Image)
		assert.Equal(t, "encoder", specs.Encoder.Runner.Name, "the runtime's runner name survives the merge")
		assert.Equal(t, "runtime-scheduler", specs.Encoder.SchedulerName)
	})

	t.Run("a runtime piece alone declares no encoder", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
		specs, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
		require.NoError(t, err)
		assert.Nil(t, specs.Encoder)
	})

	t.Run("an inline block without a piece is the whole template", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{},
			Encoder: &v1beta1.EncoderSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "example.com/encoder:v3"}}},
		}}
		specs, err := MergeRuntimeRoleSpecs(isvc, &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}, log)
		require.NoError(t, err)
		require.NotNil(t, specs.Encoder)
		assert.Equal(t, "example.com/encoder:v3", specs.Encoder.Runner.Image)
		assert.NotSame(t, isvc.Spec.Encoder, specs.Encoder, "the merge returns a copy, never the service's own block")
	})
}
