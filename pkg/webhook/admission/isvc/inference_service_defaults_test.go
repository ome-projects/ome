package isvc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	inferenceserviceutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// =============================================================================
// Helper Functions
// =============================================================================

// createTestScheme creates a runtime scheme with v1beta1 types for testing
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	return scheme
}

// createFakeClient creates a fake client with optional ClusterBaseModel or BaseModel objects
func createFakeClient(t *testing.T, models ...client.Object) client.Client {
	scheme := createTestScheme()
	objects := []client.Object{}
	objects = append(objects, models...)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

// Helper function to create int pointers
func intPtr(i int) *int {
	return &i
}

// testReplicaDefaults mirrors the chart-shipped deploy.replicas block
// (min=1, max engine/decoder=3, router=2).
// testUpdateStrategyDefaults mirrors the per-component block the chart ships,
// i.e. what a cluster running the default configuration supplies to the
// defaulter. The binary itself has no update-strategy defaults.
func testDeployConfig() *controllerconfig.DeployConfig {
	entry := testUpdateStrategyDefaults()
	return &controllerconfig.DeployConfig{
		UpdateStrategy: &controllerconfig.UpdateStrategyDefaultsConfig{
			Router: entry, Engine: entry, Decoder: entry,
		},
	}
}

func testUpdateStrategyDefaults() *controllerconfig.ComponentUpdateStrategyDefaults {
	surge := intstr.FromString("25%")
	unavailable := intstr.FromString("25%")
	return &controllerconfig.ComponentUpdateStrategyDefaults{
		Type:           string(v1beta1.UpdateStrategySurgeThenDrain),
		MaxSurge:       &surge,
		MaxUnavailable: &unavailable,
	}
}

func testReplicaDefaults() *controllerconfig.ReplicasDefaultsConfig {
	return &controllerconfig.ReplicasDefaultsConfig{
		DefaultMinReplicas: intPtr(1),
		DefaultMaxReplicas: controllerconfig.ComponentMaxReplicasDefaults{
			Engine:  intPtr(3),
			Decoder: intPtr(3),
			Router:  intPtr(2),
		},
	}
}

// createBasicInferenceService creates a basic InferenceService for testing
func createBasicInferenceService(name, namespace string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{},
	}
}

// =============================================================================
// Main DefaultInferenceService Tests
// =============================================================================

func TestDefaultInferenceService(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		deployConfig    *controllerconfig.DeployConfig
		wantAnnotations map[string]string
	}{
		{
			name:            "empty InferenceService should have no annotations",
			isvc:            createBasicInferenceService("test-isvc", "default"),
			deployConfig:    nil,
			wantAnnotations: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := createFakeClient(t)
			err := DefaultInferenceService(ctx, c, tt.isvc, tt.deployConfig)
			require.NoError(t, err)

			if tt.wantAnnotations == nil {
				if tt.isvc.Annotations != nil {
					assert.Empty(t, tt.isvc.Annotations, "Expected no annotations")
				}
				return
			}
			require.NotNil(t, tt.isvc.Annotations)
			for key, expectedVal := range tt.wantAnnotations {
				actualVal, exists := tt.isvc.Annotations[key]
				assert.True(t, exists, "expected annotation %q", key)
				assert.Equal(t, expectedVal, actualVal)
			}
		})
	}
}

// =============================================================================
// Deployment Mode Detection Tests
// =============================================================================

func TestDeploymentModeDetection(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		deployConfig *controllerconfig.DeployConfig
		expectedMode string
	}{
		{
			name: "engine and decoder should set PDDisaggregated",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			deployConfig: nil,
			expectedMode: string(constants.PDDisaggregated),
		},
		{
			name: "engine with leader and worker should set OMENative",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{Size: intPtr(2)},
					},
				},
			},
			deployConfig: nil,
			expectedMode: string(constants.OMENative),
		},
		{
			name: "engine without leader/worker should default to RawDeployment",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			deployConfig: &controllerconfig.DeployConfig{DefaultDeploymentMode: string(constants.RawDeployment)},
			expectedMode: string(constants.RawDeployment),
		},
		{
			name: "engine with worker size zero should default to RawDeployment",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{Size: intPtr(0)},
					},
				},
			},
			deployConfig: &controllerconfig.DeployConfig{DefaultDeploymentMode: string(constants.RawDeployment)},
			expectedMode: string(constants.RawDeployment),
		},
		{
			// A pre-existing annotation is preserved verbatim. This pins
			// the contract: admission does not rewrite or reject persisted
			// deployment-mode annotations.
			name: "existing deployment mode should not be overridden",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.MultiNode),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			deployConfig: nil,
			expectedMode: string(constants.MultiNode),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := createFakeClient(t)
			ctx := context.Background()
			err := DefaultInferenceService(ctx, c, tt.isvc, tt.deployConfig)
			require.NoError(t, err)

			require.NotNil(t, tt.isvc.ObjectMeta.Annotations, "Annotations should exist")
			mode, exists := tt.isvc.ObjectMeta.Annotations[constants.DeploymentMode]
			assert.True(t, exists, "Deployment mode annotation should exist")
			assert.Equal(t, tt.expectedMode, mode, "Expected deployment mode should match")
		})
	}
}

// TestDefaultInferenceService_OMENativeBudgetViaAnnotationAndHeuristic pins
// that a multi-node engine resolving to OMENative via the canonical top-level
// ome.io/deploymentMode annotation OR via the leader+worker heuristic (no
// spec.deploymentMode, no per-component annotation) still gets the rollout
// budget defaults. If componentResolvesToOMENative saw only the per-component
// annotation + spec field, the whole defaulter (including the 25% budgets)
// would be skipped for these shapes, leaving the rollout uncapped.
func TestDefaultInferenceService_OMENativeBudgetViaAnnotationAndHeuristic(t *testing.T) {
	mnEngine := func() *v1beta1.EngineSpec {
		return &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: intPtr(2)},
		}
	}
	tests := []struct {
		name string
		isvc *v1beta1.InferenceService
	}{
		{
			name: "top-level ome.io/deploymentMode=OMENative annotation",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
				Spec: v1beta1.InferenceServiceSpec{Engine: mnEngine()},
			},
		},
		{
			name: "leader+worker heuristic (no annotation, no spec.deploymentMode)",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{Engine: mnEngine()},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := createFakeClient(t)
			require.NoError(t, DefaultInferenceService(ctx, c, tt.isvc, testDeployConfig()))

			// Sanity: it resolved to OMENative.
			assert.Equal(t, string(constants.OMENative),
				tt.isvc.ObjectMeta.Annotations[constants.DeploymentMode])

			// The defaulter ran → the configured surge budget is stamped, so the
			// component is not left on the uncapped BudgetNoLimit.
			lc := tt.isvc.Spec.Engine.Lifecycle
			require.NotNil(t, lc, "OMENative lifecycle must be defaulted")
			require.NotNil(t, lc.UpdateStrategy)
			require.NotNil(t, lc.UpdateStrategy.RollingUpdate)
			require.NotNil(t, lc.UpdateStrategy.RollingUpdate.MaxSurge)
			assert.Equal(t, intstr.FromString("25%"), *lc.UpdateStrategy.RollingUpdate.MaxSurge)
			assert.Nil(t, lc.UpdateStrategy.RollingUpdate.MaxUnavailable,
				"SurgeThenDrain never reads MaxUnavailable, so it must not be stamped")
		})
	}
}

func TestDefaultComponents(t *testing.T) {
	t.Run("defaultEngine", func(t *testing.T) {
		tests := []struct {
			name            string
			engine          *v1beta1.EngineSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				engine:          &v1beta1.EngineSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 3,
			},
			{
				name: "existing values should be preserved",
				engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
			{
				name: "unset MaxReplicas is clamped up to MinReplicas above the default",
				engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 5,
			},
			{
				name: "unset MaxReplicas keeps the default when MinReplicas is below it",
				engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 3,
			},
			{
				name: "explicit MaxReplicas below MinReplicas is preserved for validation",
				engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
						MaxReplicas: 2,
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultEngine(tt.engine, nil, testReplicaDefaults(), nil, testUpdateStrategyDefaults())
				require.NotNil(t, tt.engine.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.engine.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.engine.MaxReplicas)
			})
		}
	})

	t.Run("defaultDecoder", func(t *testing.T) {
		tests := []struct {
			name            string
			decoder         *v1beta1.DecoderSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				decoder:         &v1beta1.DecoderSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 3,
			},
			{
				name: "existing values should be preserved",
				decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
			{
				name: "unset MaxReplicas is clamped up to MinReplicas above the default",
				decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 5,
			},
			{
				name: "explicit MaxReplicas below MinReplicas is preserved for validation",
				decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
						MaxReplicas: 2,
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultDecoder(tt.decoder, nil, testReplicaDefaults(), nil, testUpdateStrategyDefaults())
				require.NotNil(t, tt.decoder.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.decoder.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.decoder.MaxReplicas)
			})
		}
	})

	t.Run("defaultRouter", func(t *testing.T) {
		tests := []struct {
			name            string
			router          *v1beta1.RouterSpec
			wantMinReplicas int
			wantMaxReplicas int
		}{
			{
				name:            "nil MinReplicas should be set to 1",
				router:          &v1beta1.RouterSpec{},
				wantMinReplicas: 1,
				wantMaxReplicas: 2,
			},
			{
				name: "existing values should be preserved",
				router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(2),
						MaxReplicas: 5,
					},
				},
				wantMinReplicas: 2,
				wantMaxReplicas: 5,
			},
			{
				name: "unset MaxReplicas is clamped up to MinReplicas above the default",
				router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 5,
			},
			{
				name: "explicit MaxReplicas below MinReplicas is preserved for validation",
				router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinReplicas: intPtr(5),
						MaxReplicas: 2,
					},
				},
				wantMinReplicas: 5,
				wantMaxReplicas: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defaultRouter(tt.router, nil, testReplicaDefaults(), nil, testUpdateStrategyDefaults())
				require.NotNil(t, tt.router.MinReplicas)
				assert.Equal(t, tt.wantMinReplicas, *tt.router.MinReplicas)
				assert.Equal(t, tt.wantMaxReplicas, tt.router.MaxReplicas)
			})
		}
	})
}

// TestDefaultComponents_UnconfiguredReplicaDefaults pins the degrade path:
// without a deploy.replicas config block there is nothing to stamp — the
// defaulter stores the spec exactly as authored instead of injecting
// literals baked into the binary.
func TestDefaultComponents_UnconfiguredReplicaDefaults(t *testing.T) {
	t.Run("nil config leaves replica bounds as authored", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Nil(t, engine.MinReplicas)
		assert.Zero(t, engine.MaxReplicas)

		decoder := &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(5)},
		}
		defaultDecoder(decoder, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Equal(t, 5, *decoder.MinReplicas)
		assert.Zero(t, decoder.MaxReplicas)

		router := &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2), MaxReplicas: 4},
		}
		defaultRouter(router, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Equal(t, 2, *router.MinReplicas)
		assert.Equal(t, 4, router.MaxReplicas)
	})

	t.Run("partially configured block defaults only the configured fields", func(t *testing.T) {
		// Only the min default configured: max stays unset.
		minOnly := &controllerconfig.ReplicasDefaultsConfig{DefaultMinReplicas: intPtr(1)}
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil, minOnly, nil, testUpdateStrategyDefaults())
		assert.Equal(t, 1, *engine.MinReplicas)
		assert.Zero(t, engine.MaxReplicas)

		// Only the engine max default configured: min stays unset, and
		// the clamp still raises the filled max to an authored floor.
		maxOnly := &controllerconfig.ReplicasDefaultsConfig{
			DefaultMaxReplicas: controllerconfig.ComponentMaxReplicasDefaults{Engine: intPtr(3)},
		}
		clamped := &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(5)},
		}
		defaultEngine(clamped, nil, maxOnly, nil, testUpdateStrategyDefaults())
		assert.Equal(t, 5, *clamped.MinReplicas)
		assert.Equal(t, 5, clamped.MaxReplicas)

		unset := &v1beta1.EngineSpec{}
		defaultEngine(unset, nil, maxOnly, nil, testUpdateStrategyDefaults())
		assert.Nil(t, unset.MinReplicas)
		assert.Equal(t, 3, unset.MaxReplicas)
	})
}

func TestDefaultComponents_TerminationGracePeriod(t *testing.T) {
	t.Run("unconfigured leaves the field as authored", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Nil(t, engine.TerminationGracePeriodSeconds)

		authored := int64(45)
		router := &v1beta1.RouterSpec{
			PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: &authored},
		}
		defaultRouter(router, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Equal(t, int64(45), *router.TerminationGracePeriodSeconds)
	})

	t.Run("configured default fills every component", func(t *testing.T) {
		grace := int64(600)

		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil, nil, &grace, testUpdateStrategyDefaults())
		require.NotNil(t, engine.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(600), *engine.TerminationGracePeriodSeconds)

		decoder := &v1beta1.DecoderSpec{}
		defaultDecoder(decoder, nil, nil, &grace, testUpdateStrategyDefaults())
		require.NotNil(t, decoder.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(600), *decoder.TerminationGracePeriodSeconds)

		router := &v1beta1.RouterSpec{}
		defaultRouter(router, nil, nil, &grace, testUpdateStrategyDefaults())
		require.NotNil(t, router.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(600), *router.TerminationGracePeriodSeconds)
	})

	t.Run("authored value wins over the configured default", func(t *testing.T) {
		grace := int64(600)
		authored := int64(1800)
		engine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{TerminationGracePeriodSeconds: &authored},
		}
		defaultEngine(engine, nil, nil, &grace, testUpdateStrategyDefaults())
		assert.Equal(t, int64(1800), *engine.TerminationGracePeriodSeconds)
	})

	t.Run("stamped value is a copy, not the shared config pointer", func(t *testing.T) {
		grace := int64(600)
		engine := &v1beta1.EngineSpec{}
		router := &v1beta1.RouterSpec{}
		defaultEngine(engine, nil, nil, &grace, testUpdateStrategyDefaults())
		defaultRouter(router, nil, nil, &grace, testUpdateStrategyDefaults())
		assert.NotSame(t, &grace, engine.TerminationGracePeriodSeconds)
		assert.NotSame(t, engine.TerminationGracePeriodSeconds, router.TerminationGracePeriodSeconds)
	})
}

func int32Ptr(v int32) *int32 { return &v }

func assertUnresolvedShapePolicies(t *testing.T, lifecycle *v1beta1.LifecycleSpec) {
	t.Helper()
	require.NotNil(t, lifecycle)
	if lifecycle.RestartPolicy != nil {
		t.Errorf("RestartPolicy: got %q, want nil while the runner shape is unresolved", *lifecycle.RestartPolicy)
	}
	if lifecycle.ReadyPolicy != nil {
		t.Errorf("ReadyPolicy: got %q, want nil while the runner shape is unresolved", *lifecycle.ReadyPolicy)
	}
	require.NotNil(t, lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, lifecycle.UpdateStrategy.Type)
	require.NotNil(t, lifecycle.InstanceReadyTimeout)
	assert.Equal(t, 30*time.Minute, lifecycle.InstanceReadyTimeout.Duration)
	require.NotNil(t, lifecycle.MigrationPolicy)
	assert.Equal(t, v1beta1.MigrationPolicyModeAuto, lifecycle.MigrationPolicy.Mode)
}

func TestDefaultOMENativeEngineAndDecoder_DeferPoliciesWhenRunnerShapeIsUnresolved(t *testing.T) {
	mode := constants.OMENative

	t.Run("Engine", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, &mode, testReplicaDefaults(), nil, testUpdateStrategyDefaults())

		assertUnresolvedShapePolicies(t, engine.Lifecycle)
	})

	t.Run("Decoder", func(t *testing.T) {
		decoder := &v1beta1.DecoderSpec{}
		defaultDecoder(decoder, &mode, testReplicaDefaults(), nil, testUpdateStrategyDefaults())

		assertUnresolvedShapePolicies(t, decoder.Lifecycle)
	})
}

func TestDefaultOMENativeComponent(t *testing.T) {
	omenativeMode := map[string]string{constants.DeploymentMode: string(constants.OMENative)}

	t.Run("not OMENative — omenative block untouched", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.MultiNode)},
		}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())
		assert.Nil(t, ext.Lifecycle)
	})

	t.Run("no annotation — omenative block untouched", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())
		assert.Nil(t, ext.Lifecycle)
	})

	t.Run("OMENative + multi-pod — full defaults applied", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenativeMode}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())

		require.NotNil(t, ext.Lifecycle)
		spec := ext.Lifecycle

		require.NotNil(t, spec.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *spec.RestartPolicy)

		require.NotNil(t, spec.UpdateStrategy)
		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, spec.UpdateStrategy.Type)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		assert.Equal(t, int32(30), *spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		require.NotNil(t, spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
		assert.True(t, *spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)

		// The configured budget for the resolved strategy is stamped so an
		// unset RollingUpdate never resolves to the uncapped BudgetNoLimit
		// that would let a rollout drain a whole Component at once. The other
		// budget is left nil: SurgeThenDrain never reads MaxUnavailable.
		require.NotNil(t, spec.UpdateStrategy.RollingUpdate)
		require.NotNil(t, spec.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Equal(t, intstr.FromString("25%"), *spec.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Nil(t, spec.UpdateStrategy.RollingUpdate.MaxUnavailable)

		require.NotNil(t, spec.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *spec.ReadyPolicy)

		require.NotNil(t, spec.InstanceReadyTimeout)
		assert.Equal(t, 30*time.Minute, spec.InstanceReadyTimeout.Duration)

		require.NotNil(t, spec.MigrationPolicy)
		assert.Equal(t, v1beta1.MigrationPolicyModeAuto, spec.MigrationPolicy.Mode)
	})

	t.Run("OMENative + single-pod — restart/ready default to None", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenativeMode}
		defaultOMENativeComponent(ext, podShapeSingle, nil, testUpdateStrategyDefaults())

		require.NotNil(t, ext.Lifecycle)
		require.NotNil(t, ext.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy)
		require.NotNil(t, ext.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *ext.Lifecycle.ReadyPolicy)
	})

	t.Run("OMENative + existing values — preserved, nil siblings filled", func(t *testing.T) {
		customRestart := v1beta1.InstanceRestartPolicyNone
		customReady := v1beta1.InstanceReadyPolicyNone
		customTimeout := metav1.Duration{Duration: 5 * time.Minute}
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				RestartPolicy:        &customRestart,
				ReadyPolicy:          &customReady,
				InstanceReadyTimeout: &customTimeout,
			},
		}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())

		// preserved
		require.NotNil(t, ext.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy)
		require.NotNil(t, ext.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *ext.Lifecycle.ReadyPolicy)
		assert.Equal(t, 5*time.Minute, ext.Lifecycle.InstanceReadyTimeout.Duration)

		// filled
		require.NotNil(t, ext.Lifecycle.UpdateStrategy)
		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type)
		require.NotNil(t, ext.Lifecycle.MigrationPolicy)
		assert.Equal(t, v1beta1.MigrationPolicyModeAuto, ext.Lifecycle.MigrationPolicy.Mode)
	})

	t.Run("OMENative + partial UpdateStrategy — only nil leaves filled", func(t *testing.T) {
		customType := v1beta1.UpdateStrategyRecreatePod
		customGrace := int32(60)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					Type: customType,
					InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
						GracePeriodSeconds: &customGrace,
					},
				},
			},
		}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())

		assert.Equal(t, v1beta1.UpdateStrategyRecreatePod, ext.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, int32(60), *ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds)
		// MarkNotReadyDuringLifecycle was nil → defaulted to true
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
		assert.True(t, *ext.Lifecycle.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle)
		// RollingUpdate was nil → the budget RecreatePod reads is stamped.
		// RecreatePod is gated on MaxUnavailable, so MaxSurge stays nil.
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate)
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Equal(t, intstr.FromString("25%"), *ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Nil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})

	t.Run("OMENative + operator-set RollingUpdate budget — preserved, inert sibling left nil", func(t *testing.T) {
		// An operator who sets MaxSurge keeps that value. The strategy
		// resolves to SurgeThenDrain, which never reads MaxUnavailable, so
		// the sibling stays nil rather than carrying an unread bound.
		customSurge := intstr.FromInt(1)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenativeMode,
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					RollingUpdate: &v1beta1.RollingUpdate{
						MaxSurge: &customSurge,
					},
				},
			},
		}
		defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())

		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Equal(t, intstr.FromInt(1), *ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Nil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
	})
}

func TestEngineIsMultiPod(t *testing.T) {
	tests := []struct {
		name string
		spec *v1beta1.EngineSpec
		want bool
	}{
		{name: "nil", spec: nil, want: false},
		{name: "empty", spec: &v1beta1.EngineSpec{}, want: false},
		{name: "leader only", spec: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}}, want: false},
		{
			name: "worker size 0",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 0; return &v }()}},
			want: false,
		},
		{
			name: "worker size 1 without leader",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 1; return &v }()}},
			want: false,
		},
		{
			name: "worker size nil",
			spec: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{}},
			want: false,
		},
		{
			name: "leader and positive worker size",
			spec: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 1; return &v }()},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, engineIsMultiPod(tt.spec))
		})
	}
}

func TestDecoderIsMultiPod(t *testing.T) {
	tests := []struct {
		name string
		spec *v1beta1.DecoderSpec
		want bool
	}{
		{name: "nil", spec: nil, want: false},
		{name: "empty", spec: &v1beta1.DecoderSpec{}, want: false},
		{name: "leader only", spec: &v1beta1.DecoderSpec{Leader: &v1beta1.LeaderSpec{}}, want: false},
		{
			name: "positive worker size without leader",
			spec: &v1beta1.DecoderSpec{Worker: &v1beta1.WorkerSpec{Size: intPtr(1)}},
			want: false,
		},
		{
			name: "leader and positive worker size",
			spec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: intPtr(1)},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, decoderIsMultiPod(tt.spec))
		})
	}
}

func TestDefaultEngine_OMENativeAnnotationTriggersDefaults(t *testing.T) {
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		Leader: &v1beta1.LeaderSpec{},
		Worker: &v1beta1.WorkerSpec{Size: func() *int { v := 3; return &v }()},
	}
	defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
	require.NotNil(t, engine.Lifecycle)
	require.NotNil(t, engine.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *engine.Lifecycle.RestartPolicy)
}

func TestDefaultRouter_OMENativeAlwaysSinglePod(t *testing.T) {
	router := &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
	}
	defaultRouter(router, nil, nil, nil, testUpdateStrategyDefaults())
	require.NotNil(t, router.Lifecycle)
	require.NotNil(t, router.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *router.Lifecycle.RestartPolicy)
	require.NotNil(t, router.Lifecycle.ReadyPolicy)
	assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *router.Lifecycle.ReadyPolicy)
}

func TestDefaultInferenceService_UnresolvedRunnerShapePreservesRuntimeLifecyclePoliciesOnMerge(t *testing.T) {
	mode := constants.OMENative
	runtimeRestart := v1beta1.InstanceRestartPolicyRecreateInstance
	runtimeReady := v1beta1.InstanceReadyPolicyAllPodReady
	lifecycle := func() v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{
			Lifecycle: &v1beta1.LifecycleSpec{
				RestartPolicy: &runtimeRestart,
				ReadyPolicy:   &runtimeReady,
			},
		}
	}

	assertPolicies := func(t *testing.T, got *v1beta1.LifecycleSpec) {
		t.Helper()
		require.NotNil(t, got)
		require.NotNil(t, got.RestartPolicy)
		assert.Equal(t, runtimeRestart, *got.RestartPolicy)
		require.NotNil(t, got.ReadyPolicy)
		assert.Equal(t, runtimeReady, *got.ReadyPolicy)
	}

	t.Run("Engine", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &v1beta1.EngineSpec{},
		}}
		require.NoError(t, DefaultInferenceService(context.Background(), createFakeClient(t), isvc, nil))
		merged, err := inferenceserviceutils.MergeEngineSpec(
			&v1beta1.EngineSpec{ComponentExtensionSpec: lifecycle()},
			isvc.Spec.Engine,
		)
		require.NoError(t, err)
		assertPolicies(t, merged.Lifecycle)
	})

	t.Run("Decoder", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &v1beta1.EngineSpec{},
			Decoder:        &v1beta1.DecoderSpec{},
		}}
		require.NoError(t, DefaultInferenceService(context.Background(), createFakeClient(t), isvc, nil))
		merged, err := inferenceserviceutils.MergeDecoderSpec(
			&v1beta1.DecoderSpec{ComponentExtensionSpec: lifecycle()},
			isvc.Spec.Decoder,
		)
		require.NoError(t, err)
		assertPolicies(t, merged.Lifecycle)
	})
}

// TestDefaultWorkerSize_MultiPodWithoutSize covers the
// defaulter rule that Worker.Size = nil with Leader set is
// treated as "minimum-viable multi-pod" (Size=1). The downstream
// engineIsMultiPod / decoderIsMultiPod detection sees the defaulted
// Size and the gang lifecycle policies kick in.
func TestDefaultWorkerSize_MultiPodWithoutSize(t *testing.T) {
	t.Run("engine leader + worker without size — defaults Size=1", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{},
		}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 1, *engine.Worker.Size)
		// downstream multi-pod detection should kick in:
		require.NotNil(t, engine.Lifecycle)
		require.NotNil(t, engine.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *engine.Lifecycle.RestartPolicy)
		require.NotNil(t, engine.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *engine.Lifecycle.ReadyPolicy)
	})

	t.Run("engine leader + worker with size=3 — preserved", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: intPtr(3)},
		}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 3, *engine.Worker.Size)
	})

	t.Run("engine leader + worker with size=0 — preserved (validator rejects)", func(t *testing.T) {
		// The defaulter must NOT silently turn a 0 into a 1; that
		// would erase a clear operator mistake. Validator rejects
		// Size <= 0 with WorkerSizeMustBePositive.
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: intPtr(0)},
		}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		require.NotNil(t, engine.Worker.Size)
		assert.Equal(t, 0, *engine.Worker.Size)
	})

	t.Run("engine leader-only — no worker created (validator rejects)", func(t *testing.T) {
		// Pure leader-without-worker is not the multi-pod pairing the
		// defaulter handles. The validator catches it with
		// LeaderRequiresWorker.
		engine := &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Nil(t, engine.Worker)
	})

	t.Run("engine worker-only — Size left untouched (validator rejects)", func(t *testing.T) {
		// Pure worker-without-leader: the defaulter sees no pairing
		// to fill. The validator catches it with
		// WorkerRequiresLeader; the malformed Size remains for the
		// error message to surface.
		engine := &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{}}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Nil(t, engine.Worker.Size)
	})

	t.Run("decoder leader + worker without size — defaults Size=1", func(t *testing.T) {
		decoder := &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{},
		}
		defaultDecoder(decoder, nil, nil, nil, testUpdateStrategyDefaults())
		require.NotNil(t, decoder.Worker.Size)
		assert.Equal(t, 1, *decoder.Worker.Size)
		require.NotNil(t, decoder.Lifecycle)
		require.NotNil(t, decoder.Lifecycle.RestartPolicy)
		assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *decoder.Lifecycle.RestartPolicy)
		require.NotNil(t, decoder.Lifecycle.ReadyPolicy)
		assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *decoder.Lifecycle.ReadyPolicy)
	})

	t.Run("single-pod engine — Worker field unset, no panic", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{}
		defaultEngine(engine, nil, nil, nil, testUpdateStrategyDefaults())
		assert.Nil(t, engine.Worker)
		assert.Nil(t, engine.Leader)
	})
}

// TestDefaultOMENativeComponent_MultiPodDefaults pins the
// multi-pod defaulter rules as a single explicit before/after
// test, complementing TestDefaultOMENativeComponent which already
// covers the underlying mechanic.
//
// Rules pinned:
//   - RestartPolicy → RecreateInstanceOnPodRestart
//   - ReadyPolicy   → AllPodReady
//   - UpdateStrategy.Type → SurgeThenDrain (same as single-pod;
//     multi-pod gangs surge as a unit via the gang-surge path)
func TestDefaultOMENativeComponent_MultiPodDefaults(t *testing.T) {
	ext := &v1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
	}

	// before
	assert.Nil(t, ext.Lifecycle)

	defaultOMENativeComponent(ext, podShapeMulti, nil, testUpdateStrategyDefaults())

	// after
	require.NotNil(t, ext.Lifecycle)
	require.NotNil(t, ext.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyRecreateInstance, *ext.Lifecycle.RestartPolicy,
		"multi-pod must default to RecreateInstanceOnPodRestart (gang restart)")

	require.NotNil(t, ext.Lifecycle.ReadyPolicy)
	assert.Equal(t, v1beta1.InstanceReadyPolicyAllPodReady, *ext.Lifecycle.ReadyPolicy,
		"multi-pod must default to AllPodReady so Instance Ready iff every gang pod Ready")

	require.NotNil(t, ext.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type,
		"multi-pod must default to SurgeThenDrain — same as single-pod")
}

// TestDefaultOMENativeComponent_SinglePodDefaults confirms the
// multi-pod rules do NOT affect single-pod defaulting.
func TestDefaultOMENativeComponent_SinglePodDefaults(t *testing.T) {
	ext := &v1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
	}
	defaultOMENativeComponent(ext, podShapeSingle, nil, testUpdateStrategyDefaults())

	require.NotNil(t, ext.Lifecycle)
	require.NotNil(t, ext.Lifecycle.RestartPolicy)
	assert.Equal(t, v1beta1.InstanceRestartPolicyNone, *ext.Lifecycle.RestartPolicy,
		"single-pod must keep RestartPolicy=None — no gang restart needed for one pod")
	require.NotNil(t, ext.Lifecycle.ReadyPolicy)
	assert.Equal(t, v1beta1.InstanceReadyPolicyNone, *ext.Lifecycle.ReadyPolicy,
		"single-pod must keep ReadyPolicy=None — no aggregation needed for one pod")
	require.NotNil(t, ext.Lifecycle.UpdateStrategy)
	assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type,
		"single-pod UpdateStrategy must stay SurgeThenDrain")
}

// silence unused-helper lint if int32Ptr isn't referenced elsewhere
var _ = int32Ptr

func TestDefaultOMENativeComponent_UpdateStrategyIsConfigDriven(t *testing.T) {
	omenative := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	inPlaceBudget := intstr.FromString("10%")
	surgeBudget := intstr.FromString("25%")

	t.Run("unconfigured leaves the strategy as authored", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenative}
		defaultOMENativeComponent(ext, podShapeSingle, nil, nil)

		require.NotNil(t, ext.Lifecycle.UpdateStrategy)
		assert.Equal(t, v1beta1.UpdateStrategyType(""), ext.Lifecycle.UpdateStrategy.Type,
			"the binary carries no update-strategy default; it comes from configuration")
		assert.Nil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate)
	})

	t.Run("a non-surge strategy takes the unavailability budget only", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenative}
		defaultOMENativeComponent(ext, podShapeSingle, nil, &controllerconfig.ComponentUpdateStrategyDefaults{
			Type:           string(v1beta1.UpdateStrategyInPlaceIfPossible),
			MaxSurge:       &surgeBudget,
			MaxUnavailable: &inPlaceBudget,
		})

		assert.Equal(t, v1beta1.UpdateStrategyInPlaceIfPossible, ext.Lifecycle.UpdateStrategy.Type)
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate)
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Equal(t, inPlaceBudget, *ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
		assert.Nil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge,
			"a non-surge strategy never reads MaxSurge, so configuring it must not stamp it")
	})

	t.Run("an authored strategy wins over configuration", func(t *testing.T) {
		authored := intstr.FromInt(3)
		ext := &v1beta1.ComponentExtensionSpec{
			Annotations: omenative,
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					Type:          v1beta1.UpdateStrategySurgeThenDrain,
					RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &authored},
				},
			},
		}
		defaultOMENativeComponent(ext, podShapeSingle, nil, &controllerconfig.ComponentUpdateStrategyDefaults{
			Type:     string(v1beta1.UpdateStrategyInPlaceIfPossible),
			MaxSurge: &surgeBudget,
		})

		assert.Equal(t, v1beta1.UpdateStrategySurgeThenDrain, ext.Lifecycle.UpdateStrategy.Type)
		assert.Equal(t, authored, *ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
	})

	t.Run("an unset strategy takes the surge budget, matching how it dispatches", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{Annotations: omenative}
		defaultOMENativeComponent(ext, podShapeSingle, nil, &controllerconfig.ComponentUpdateStrategyDefaults{
			MaxSurge:       &surgeBudget,
			MaxUnavailable: &inPlaceBudget,
		})

		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate)
		require.NotNil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Equal(t, surgeBudget, *ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge)
		assert.Nil(t, ext.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable)
	})

	t.Run("a non-OMENative component is untouched", func(t *testing.T) {
		ext := &v1beta1.ComponentExtensionSpec{}
		defaultOMENativeComponent(ext, podShapeSingle, nil, testUpdateStrategyDefaults())
		assert.Nil(t, ext.Lifecycle)
	})
}
