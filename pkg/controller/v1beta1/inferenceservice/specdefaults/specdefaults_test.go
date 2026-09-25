package specdefaults

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// fullConfig carries a value for every deploy-block field, with per-component
// differences where the accessors distinguish components.
func fullConfig() *controllerconfig.DeployConfig {
	surge := intstr.FromString("25%")
	unavailable := intstr.FromString("10%")
	return &controllerconfig.DeployConfig{
		DefaultDeploymentMode: string(constants.RawDeployment),
		Replicas: &controllerconfig.ReplicasDefaultsConfig{
			DefaultMinReplicas: ptr.To(1),
			DefaultMaxReplicas: controllerconfig.ComponentMaxReplicasDefaults{
				Engine: ptr.To(3), Decoder: ptr.To(4), Router: ptr.To(2),
			},
		},
		TerminationGracePeriodSeconds: ptr.To(int64(600)),
		MinReadySeconds:               ptr.To(int32(30)),
		UpdateStrategy: &controllerconfig.UpdateStrategyDefaultsConfig{
			Engine:  &controllerconfig.ComponentUpdateStrategyDefaults{Type: "SurgeThenDrain", MaxSurge: &surge, MaxUnavailable: &unavailable},
			Decoder: &controllerconfig.ComponentUpdateStrategyDefaults{Type: "RecreatePod", MaxSurge: &surge, MaxUnavailable: &unavailable},
			Router:  &controllerconfig.ComponentUpdateStrategyDefaults{Type: "SurgeThenDrain", MaxSurge: &surge, MaxUnavailable: &unavailable},
		},
	}
}

func TestEngine_ReplicaBounds(t *testing.T) {
	t.Run("unset bounds come from config", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.RawDeployment, fullConfig())
		assert.Equal(t, ptr.To(1), engine.MinReplicas)
		assert.Equal(t, 3, engine.MaxReplicas)
	})
	t.Run("authored bounds are kept", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 8}}
		Engine(engine, constants.RawDeployment, fullConfig())
		assert.Equal(t, ptr.To(2), engine.MinReplicas)
		assert.Equal(t, 8, engine.MaxReplicas)
	})
	t.Run("filled max is raised to an authored min above it", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(5)}}
		Engine(engine, constants.RawDeployment, fullConfig())
		assert.Equal(t, 5, engine.MaxReplicas)
	})
	t.Run("no config leaves bounds unset", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.RawDeployment, nil)
		assert.Nil(t, engine.MinReplicas)
		assert.Zero(t, engine.MaxReplicas)
		Engine(engine, constants.RawDeployment, &controllerconfig.DeployConfig{})
		assert.Nil(t, engine.MinReplicas)
		assert.Zero(t, engine.MaxReplicas)
	})
}

func TestDecoderAndRouter_ReplicaBounds(t *testing.T) {
	decoder := &v1beta1.DecoderSpec{}
	Decoder(decoder, constants.RawDeployment, fullConfig())
	assert.Equal(t, ptr.To(1), decoder.MinReplicas)
	assert.Equal(t, 4, decoder.MaxReplicas)

	router := &v1beta1.RouterSpec{}
	Router(router, constants.RawDeployment, fullConfig())
	assert.Equal(t, ptr.To(1), router.MinReplicas)
	assert.Equal(t, 2, router.MaxReplicas)
}

func TestEngine_TerminationGracePeriod(t *testing.T) {
	t.Run("fills the component pod spec", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.RawDeployment, fullConfig())
		assert.Equal(t, ptr.To(int64(600)), engine.TerminationGracePeriodSeconds)
	})
	t.Run("fills leader and worker pod specs only where unset", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: ptr.To(int64(900))}},
		}
		Engine(engine, constants.OMENative, fullConfig())
		assert.Equal(t, ptr.To(int64(600)), engine.TerminationGracePeriodSeconds)
		assert.Equal(t, ptr.To(int64(600)), engine.Leader.TerminationGracePeriodSeconds)
		assert.Equal(t, ptr.To(int64(900)), engine.Worker.TerminationGracePeriodSeconds)
	})
	t.Run("authored value is kept", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: ptr.To(int64(1200))}}
		Engine(engine, constants.RawDeployment, fullConfig())
		assert.Equal(t, ptr.To(int64(1200)), engine.TerminationGracePeriodSeconds)
	})
	t.Run("no config leaves it unset", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}}
		Engine(engine, constants.OMENative, &controllerconfig.DeployConfig{})
		assert.Nil(t, engine.TerminationGracePeriodSeconds)
		assert.Nil(t, engine.Leader.TerminationGracePeriodSeconds)
		assert.Nil(t, engine.Worker.TerminationGracePeriodSeconds)
	})
}

func TestEngine_WorkerSize(t *testing.T) {
	t.Run("declared pair without a size gets one worker", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}}
		Engine(engine, constants.OMENative, nil)
		assert.Equal(t, ptr.To(1), engine.Worker.Size)
	})
	t.Run("authored size is kept", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{Size: ptr.To(4)}}
		Engine(engine, constants.OMENative, nil)
		assert.Equal(t, ptr.To(4), engine.Worker.Size)
	})
	t.Run("a worker without a leader is left alone", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{}}
		Engine(engine, constants.OMENative, nil)
		assert.Nil(t, engine.Worker.Size)
	})
	t.Run("decoder pair without a size gets one worker", func(t *testing.T) {
		decoder := &v1beta1.DecoderSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}}
		Decoder(decoder, constants.OMENative, nil)
		assert.Equal(t, ptr.To(1), decoder.Worker.Size)
	})
}

func TestEngine_LifecycleOnlyForOMENative(t *testing.T) {
	engine := &v1beta1.EngineSpec{}
	Engine(engine, constants.RawDeployment, fullConfig())
	assert.Nil(t, engine.Lifecycle, "a RawDeployment component has no lifecycle to fill")

	engine = &v1beta1.EngineSpec{}
	Engine(engine, constants.OMENative, fullConfig())
	require.NotNil(t, engine.Lifecycle)
	assert.Equal(t, ptr.To(int32(30)), engine.Lifecycle.MinReadySeconds)
	require.NotNil(t, engine.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, engine.Lifecycle.UpdateStrategy.Type)
	require.NotNil(t, engine.Lifecycle.UpdateStrategy.RollingUpdate)
	assert.Equal(t, ptr.To(intstr.FromString("25%")), engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	assert.Nil(t, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable, "a surge strategy never reads maxUnavailable")
	assert.Nil(t, engine.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy, "fixed fallbacks belong to the workload engine, not the spec")
	assert.Nil(t, engine.Lifecycle.MigrationPolicy)
	assert.Nil(t, engine.Lifecycle.ReadyPolicy)
	assert.Nil(t, engine.Lifecycle.RestartPolicy)
}

func TestDecoder_NonSurgeStrategyTakesTheUnavailabilityBudget(t *testing.T) {
	decoder := &v1beta1.DecoderSpec{}
	Decoder(decoder, constants.OMENative, fullConfig())
	require.NotNil(t, decoder.Lifecycle)
	require.NotNil(t, decoder.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategyRecreatePod, decoder.Lifecycle.UpdateStrategy.Type)
	require.NotNil(t, decoder.Lifecycle.UpdateStrategy.RollingUpdate)
	assert.Equal(t, ptr.To(intstr.FromString("10%")), decoder.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
	assert.Nil(t, decoder.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
}

func TestEngine_AuthoredLifecycleWins(t *testing.T) {
	t.Run("authored non-surge type keeps its own budget", func(t *testing.T) {
		one := intstr.FromInt32(1)
		engine := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{
			MinReadySeconds: ptr.To(int32(5)),
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type:          v1beta1.UpdateStrategyInPlaceIfPossible,
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &one},
			},
		}}}
		Engine(engine, constants.OMENative, fullConfig())
		assert.Equal(t, ptr.To(int32(5)), engine.Lifecycle.MinReadySeconds)
		assert.Equal(t, v1beta1.UpdateStrategyInPlaceIfPossible, engine.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, &one, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Nil(t, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})
	t.Run("authored surge budget is kept", func(t *testing.T) {
		ten := intstr.FromString("10%")
		engine := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &ten}},
		}}}
		Engine(engine, constants.OMENative, fullConfig())
		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, engine.Lifecycle.UpdateStrategy.Type, "unset type takes the configured one")
		assert.Equal(t, &ten, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})
	t.Run("authored non-surge type with no budget takes the configured unavailability budget", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
		}}}
		Engine(engine, constants.OMENative, fullConfig())
		assert.Equal(t, v1beta1.UpdateStrategyRecreatePod, engine.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, ptr.To(intstr.FromString("10%")), engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Nil(t, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})
}

func TestEngine_UnconfiguredLifecycleAllocatesNothing(t *testing.T) {
	t.Run("no lifecycle config", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.OMENative, &controllerconfig.DeployConfig{})
		assert.Nil(t, engine.Lifecycle)
	})
	t.Run("empty per-component entry", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.OMENative, &controllerconfig.DeployConfig{
			UpdateStrategy: &controllerconfig.UpdateStrategyDefaultsConfig{Engine: &controllerconfig.ComponentUpdateStrategyDefaults{}},
		})
		assert.Nil(t, engine.Lifecycle)
	})
	t.Run("budget-only entry leaves the type unset", func(t *testing.T) {
		surge := intstr.FromString("25%")
		engine := &v1beta1.EngineSpec{}
		Engine(engine, constants.OMENative, &controllerconfig.DeployConfig{
			UpdateStrategy: &controllerconfig.UpdateStrategyDefaultsConfig{Engine: &controllerconfig.ComponentUpdateStrategyDefaults{MaxSurge: &surge}},
		})
		require.NotNil(t, engine.Lifecycle)
		assert.Empty(t, engine.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, &surge, engine.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})
}

func TestRouter_Lifecycle(t *testing.T) {
	router := &v1beta1.RouterSpec{}
	Router(router, constants.OMENative, fullConfig())
	require.NotNil(t, router.Lifecycle)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, router.Lifecycle.UpdateStrategy.Type)
	assert.Equal(t, ptr.To(intstr.FromString("25%")), router.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	assert.Equal(t, ptr.To(int64(600)), router.TerminationGracePeriodSeconds)
}

func TestNilComponentsAreNoOps(t *testing.T) {
	assert.NotPanics(t, func() {
		Engine(nil, constants.OMENative, fullConfig())
		Decoder(nil, constants.OMENative, fullConfig())
		Router(nil, constants.OMENative, fullConfig())
	})
}
