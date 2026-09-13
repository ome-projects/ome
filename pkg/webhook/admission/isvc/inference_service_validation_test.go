package isvc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/validation"
)

// =============================================================================
// VALIDATOR INTERFACE TESTS
// =============================================================================

func TestValidateClusterBaseModelPVCNamespace(t *testing.T) {
	pvc := func(uri string) *v1beta1.BaseModelSpec {
		return &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr(uri)}}
	}
	clusterMeta := &metav1.ObjectMeta{Name: "shared-llama"}
	namespacedMeta := &metav1.ObjectMeta{Name: "llama", Namespace: "models"}
	isvc := func(ns string) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: ns},
			Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "shared-llama"}},
		}
	}

	tests := []struct {
		name    string
		spec    *v1beta1.BaseModelSpec
		meta    *metav1.ObjectMeta
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{name: "namespaced BaseModel: skip", spec: pvc("pvc://my-pvc/p"), meta: namespacedMeta, isvc: isvc("models")},
		{name: "ClusterBaseModel non-PVC: skip", spec: pvc("oci://n/ns/b/bucket/o/path"), meta: clusterMeta, isvc: isvc("team-foo")},
		{name: "ClusterBaseModel PVC same ns: ok", spec: pvc("pvc://team-foo:my-pvc/p"), meta: clusterMeta, isvc: isvc("team-foo")},
		{name: "ClusterBaseModel PVC missing ns prefix: defer to BaseModel webhook", spec: pvc("pvc://my-pvc/p"), meta: clusterMeta, isvc: isvc("team-foo")},
		{name: "ClusterBaseModel PVC cross-ns: reject", spec: pvc("pvc://shared:my-pvc/p"), meta: clusterMeta, isvc: isvc("team-foo"), wantErr: true},
		{name: "spec nil: skip", spec: nil, meta: clusterMeta, isvc: isvc("team-foo")},
		{name: "spec storage nil: skip", spec: &v1beta1.BaseModelSpec{}, meta: clusterMeta, isvc: isvc("team-foo")},
		{name: "spec storage uri nil: skip", spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{}}, meta: clusterMeta, isvc: isvc("team-foo")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateClusterBaseModelPVCNamespace(tc.spec, tc.meta, tc.isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestInferenceServiceValidator_ValidateCreate(t *testing.T) {
	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "valid inference service",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid name format",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "Test-ISVC", // Invalid name format
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			warnings, err := v.ValidateCreate(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Nil(t, warnings)
			}
		})
	}
}

func TestInferenceServiceValidator_ValidateUpdate(t *testing.T) {
	tests := []struct {
		name    string
		oldIsvc *v1beta1.InferenceService
		newIsvc *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "valid update",
			oldIsvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
				},
			},
			newIsvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			warnings, err := v.ValidateUpdate(context.Background(), tt.oldIsvc, tt.newIsvc)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Nil(t, warnings)
			}
		})
	}
}

// A terminating InferenceService is admitted on UPDATE whatever its spec
// resolves to. Removing the controller's finalizer is an UPDATE, so it is
// ValidateUpdate — not the no-op ValidateDelete — that gates teardown, and
// this webhook is failurePolicy=fail: rejecting that write leaves the object
// stuck in Terminating forever with its children never garbage-collected.
// Regression test for the report that disabling a referenced BaseModel made
// every InferenceService pointing at it undeletable.
func TestInferenceServiceValidator_ValidateUpdate_Terminating(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	disabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "disabled-model"},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture: stringPtr("llama"),
			ModelFormat:       v1beta1.ModelFormat{Name: "llama", Version: stringPtr("1")},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(true),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(disabledModel).
		Build()
	validator := &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.New(fakeClient),
	}

	// modelRef names the model the ISVC points at; terminating stamps the
	// deletionTimestamp the apiserver sets once `kubectl delete` has run,
	// and finalizers is the list the controller is about to empty.
	isvc := func(modelRef string, terminating bool, finalizers ...string) *v1beta1.InferenceService {
		obj := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-isvc",
				Namespace:  "default",
				Finalizers: finalizers,
			},
			Spec: v1beta1.InferenceServiceSpec{
				Model: &v1beta1.ModelRef{Name: modelRef},
				// Engine present with no runner config so the validator
				// takes the model/runtime resolution path.
				Engine: &v1beta1.EngineSpec{},
			},
		}
		if terminating {
			now := metav1.Now()
			obj.DeletionTimestamp = &now
		}
		return obj
	}

	const finalizer = "inferenceservice.finalizers"

	t.Run("live isvc still rejected", func(t *testing.T) {
		// The guard is scoped to teardown: on an object that is not being
		// deleted the disabled-model rule keeps firing as before.
		_, err := validator.ValidateUpdate(context.Background(),
			isvc("disabled-model", false, finalizer),
			isvc("disabled-model", false, finalizer))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "model disabled-model is disabled")
	})

	t.Run("finalizer removal on terminating isvc admitted", func(t *testing.T) {
		warnings, err := validator.ValidateUpdate(context.Background(),
			isvc("disabled-model", true, finalizer),
			isvc("disabled-model", true))

		assert.NoError(t, err)
		assert.Nil(t, warnings)
	})

	t.Run("terminating isvc whose model is gone admitted", func(t *testing.T) {
		// Same wedge, other cause: the model was deleted rather than
		// disabled, so resolution fails outright. Teardown must not care.
		warnings, err := validator.ValidateUpdate(context.Background(),
			isvc("deleted-model", true, finalizer),
			isvc("deleted-model", true))

		assert.NoError(t, err)
		assert.Nil(t, warnings)
	})

	t.Run("terminating isvc with spec.model removed admitted", func(t *testing.T) {
		// Not a dangling reference but no reference at all. With neither
		// spec.model nor spec.runtime set, a live ISVC is rejected by the
		// structural "at least one of" rule — which runs well before any
		// cluster lookup — so this pins that the guard clears structural
		// rules too, not just the ones that resolve cluster state.
		noModel := func(terminating bool, finalizers ...string) *v1beta1.InferenceService {
			obj := isvc("disabled-model", terminating, finalizers...)
			obj.Spec.Model = nil
			return obj
		}

		_, err := validator.ValidateUpdate(context.Background(),
			noModel(false, finalizer), noModel(false, finalizer))
		assert.Error(t, err, "a live ISVC with no model and no runtime must still be rejected")
		assert.Contains(t, err.Error(), "at least one of spec.model or spec.runtime must be set")

		warnings, err := validator.ValidateUpdate(context.Background(),
			noModel(true, finalizer), noModel(true))

		assert.NoError(t, err)
		assert.Nil(t, warnings)
	})
}

func TestInferenceServiceValidator_ValidateDelete(t *testing.T) {
	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "valid inference service",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			warnings, err := v.ValidateDelete(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Nil(t, warnings)
			}
		})
	}
}

// Traffic capability gate: the validator receives the active
// translator's typed spec.traffic capability tokens and rejects intent
// no translator can honor (reserved Metadata override) or that the
// active translator would only partially apply (multi-header hashing).
func TestValidateTrafficCapabilityGate(t *testing.T) {
	trafficISVC := func(traffic *v1beta1.TrafficSpec) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
				Traffic: traffic,
			},
		}
	}
	consistentHash := v1beta1.LoadBalancingTypeConsistentHash
	multiHeaderTraffic := &v1beta1.TrafficSpec{
		Algorithm: &consistentHash,
		ConsistentHash: &v1beta1.ConsistentHashSpec{
			Type:    v1beta1.HashTypeHeader,
			Headers: []v1beta1.HashHeader{{Name: "x-tenant"}, {Name: "x-session"}},
		},
	}
	metadataTraffic := &v1beta1.TrafficSpec{
		EndpointOverride: &v1beta1.EndpointOverrideSpec{
			Type: v1beta1.EndpointOverrideTypeMetadata,
		},
	}

	t.Run("Metadata endpoint override rejected with no capability wiring", func(t *testing.T) {
		v := &InferenceServiceValidator{}
		_, err := v.ValidateCreate(context.Background(), trafficISVC(metadataTraffic))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ReservedEndpointOverrideType")
	})

	t.Run("Metadata endpoint override rejected when the active translator does not declare it", func(t *testing.T) {
		v := &InferenceServiceValidator{
			SupportedTrafficFields: []string{constants.TrafficCapabilityEndpointOverrideHeader},
		}
		_, err := v.ValidateCreate(context.Background(), trafficISVC(metadataTraffic))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ReservedEndpointOverrideType")
	})

	t.Run("multi-header hash rejected when the active translator hashes one header", func(t *testing.T) {
		v := &InferenceServiceValidator{
			SupportedTrafficFields: []string{
				constants.TrafficCapabilityAlgorithm,
				constants.TrafficCapabilityHashHeader,
			},
		}
		_, err := v.ValidateCreate(context.Background(), trafficISVC(multiHeaderTraffic))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "UnsupportedMultiHeaderHash")
	})

	t.Run("multi-header hash admitted when the active translator declares it", func(t *testing.T) {
		v := &InferenceServiceValidator{
			SupportedTrafficFields: []string{
				constants.TrafficCapabilityAlgorithm,
				constants.TrafficCapabilityHashHeader,
				constants.TrafficCapabilityHashMultipleHeaders,
			},
		}
		_, err := v.ValidateCreate(context.Background(), trafficISVC(multiHeaderTraffic))
		assert.NoError(t, err)
	})

	t.Run("multi-header hash admitted when the provider is unknown (empty set)", func(t *testing.T) {
		// The reconciler-side BackendPolicyUnsupportedFields condition
		// covers surfacing in this configuration.
		v := &InferenceServiceValidator{}
		_, err := v.ValidateCreate(context.Background(), trafficISVC(multiHeaderTraffic))
		assert.NoError(t, err)
	})
}

// =============================================================================
// NAME VALIDATION TESTS
// =============================================================================

func TestInferenceService_NameValidation(t *testing.T) {
	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "valid name",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "valid-name",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid name with uppercase",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "Invalid-Name",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid name with special characters",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "invalid@name",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validation.ValidateInferenceServiceName(tt.isvc.Name)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// =============================================================================
// AUTOSCALER VALIDATION TESTS
// =============================================================================

func TestInferenceService_AutoscalerValidation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "no autoscaler class",
			annotations: nil,
			wantErr:     false,
		},
		{
			name:        "missing annotations map entirely",
			annotations: nil,
			wantErr:     false,
		},
		{
			name: "valid HPA autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
			},
			wantErr: false,
		},
		{
			name: "valid external autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassExternal),
			},
			wantErr: false,
		},
		{
			name: "invalid autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: "invalid-class",
			},
			wantErr: true,
			errMsg:  "is not a supported autoscaler class type",
		},
		{
			name: "HPA autoscaler with valid CPU metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: string(constants.AutoScalerMetricsCPU),
			},
			wantErr: false,
		},
		{
			name: "HPA autoscaler with valid Memory metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: string(constants.AutoScalerMetricsMemory),
			},
			wantErr: false,
		},
		{
			name: "HPA autoscaler with invalid metrics",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "invalid-metric",
			},
			wantErr: true,
			errMsg:  "is not a supported metric",
		},
		{
			name: "valid KEDA autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-isvc",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}

			err := func() error { _, err := validation.ValidateAutoscalerConfig(isvc); return err }()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInferenceService_TargetUtilizationValidation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{
			name:        "no target utilization percentage",
			annotations: nil,
			wantErr:     false,
		},
		{
			name: "valid target utilization percentage",
			annotations: map[string]string{
				constants.TargetUtilizationPercentage: "50",
			},
			wantErr: false,
		},
		{
			name: "invalid target utilization percentage (too low)",
			annotations: map[string]string{
				constants.TargetUtilizationPercentage: "0",
			},
			wantErr: true,
		},
		{
			name: "invalid target utilization percentage (too high)",
			annotations: map[string]string{
				constants.TargetUtilizationPercentage: "150",
			},
			wantErr: true,
		},
		{
			name: "invalid target utilization percentage (not a number)",
			annotations: map[string]string{
				constants.TargetUtilizationPercentage: "not-a-number",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-isvc",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}

			err := validation.ValidateAutoscalerTargetUtilizationPercentage(isvc)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test missing branches in validateHPAMetrics
func TestValidateHPAMetrics_AllMetrics(t *testing.T) {
	validMetrics := []string{
		string(constants.AutoScalerMetricsCPU),
		string(constants.AutoScalerMetricsMemory),
	}

	for _, metric := range validMetrics {
		t.Run(metric, func(t *testing.T) {
			err := validation.ValidateHPAMetrics(metric)
			assert.NoError(t, err)
		})
	}

	t.Run("invalid metric", func(t *testing.T) {
		err := validation.ValidateHPAMetrics("invalid-metric")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a supported metric")
	})
}

// =============================================================================
// ENGINE/DECODER VALIDATION TESTS
// =============================================================================

func TestInferenceService_EngineDecoderValidation(t *testing.T) {
	tests := []struct {
		name       string
		hasEngine  bool
		hasDecoder bool
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "no decoder, no engine - should pass",
			hasEngine:  false,
			hasDecoder: false,
			wantErr:    false,
		},
		{
			name:       "has engine, has decoder - should pass",
			hasEngine:  true,
			hasDecoder: true,
			wantErr:    false,
		},
		{
			name:       "no engine, has decoder - should fail",
			hasEngine:  false,
			hasDecoder: true,
			wantErr:    true,
			errMsg:     "decoder cannot be specified without engine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{},
			}

			// Add engine if needed
			if tt.hasEngine {
				isvc.Spec.Engine = &v1beta1.EngineSpec{}
			}

			// Add decoder if needed
			if tt.hasDecoder {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
			}

			err := validation.ValidateEngineDecoderConfig(&isvc.Spec)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInferenceService_OMENativeEngineDecoderCoupling(t *testing.T) {
	annot := func(mode string) v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
		}
	}
	withRunner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "test-image:latest"}}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
		errMsg  string
	}{
		{
			name: "engine and decoder both OMENative - passes",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime", AutoSync: boolPtr(false)},
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: annot(string(constants.OMENative)),
						Runner:                 withRunner,
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: annot(string(constants.OMENative)),
					},
				},
			},
		},
		{
			name: "engine OMENative, decoder unset - rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: annot(string(constants.OMENative)),
						Runner:                 withRunner,
					},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			wantErr: true,
			errMsg:  "InvalidDeploymentModeCombination",
		},
		{
			name: "engine OMENative, decoder MultiNode - rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: annot(string(constants.OMENative)),
						Runner:                 withRunner,
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: annot(string(constants.MultiNode)),
					},
				},
			},
			wantErr: true,
			errMsg:  "InvalidDeploymentModeCombination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			_, err := v.ValidateCreate(context.Background(), tt.isvc)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInferenceService_LeaderWorkerPairing(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	withRunner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "test-image:latest"}}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
		errSub  string
	}{
		{
			name: "engine: leader + worker(size=3) - valid",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime", AutoSync: boolPtr(false)},
					Engine: &v1beta1.EngineSpec{
						Runner: withRunner,
						Leader: &v1beta1.LeaderSpec{},
						Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)},
					},
				},
			},
		},
		{
			name: "engine: leader only - rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Runner: withRunner,
						Leader: &v1beta1.LeaderSpec{},
					},
				},
			},
			wantErr: true,
			errSub:  "InvalidLeaderWorkerPairing",
		},
		{
			name: "engine: worker only - rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Runner: withRunner,
						Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)},
					},
				},
			},
			wantErr: true,
			errSub:  "InvalidLeaderWorkerPairing",
		},
		{
			name: "decoder: leader only - rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{Runner: withRunner},
					Decoder: &v1beta1.DecoderSpec{
						Leader: &v1beta1.LeaderSpec{},
					},
				},
			},
			wantErr: true,
			errSub:  "decoder.leader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			_, err := v.ValidateCreate(context.Background(), tt.isvc)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errSub != "" {
					assert.Contains(t, err.Error(), tt.errSub)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHasFullRunnerConfig(t *testing.T) {
	tests := []struct {
		name     string
		engine   *v1beta1.EngineSpec
		expected bool
	}{
		{
			name:     "nil engine",
			engine:   nil,
			expected: false,
		},
		{
			name:     "empty engine",
			engine:   &v1beta1.EngineSpec{},
			expected: false,
		},
		{
			name: "engine with runner but no image",
			engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{},
			},
			expected: false,
		},
		{
			name: "engine with runner and image",
			engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Image: "test-image:latest",
					},
				},
			},
			expected: true,
		},
		{
			name: "engine with leader and worker with images",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Image: "leader-image:latest",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Image: "worker-image:latest",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "engine with leader but no worker",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Image: "leader-image:latest",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "has worker but no leader",
			engine: &v1beta1.EngineSpec{
				Worker: &v1beta1.WorkerSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Image: "worker-image:latest",
						},
					},
				},
				// No leader
			},
			expected: false,
		},
		{
			name: "has leader and worker but leader has no image",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							// No image
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Image: "worker-image:latest",
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasFullRunnerConfig(tt.engine)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// RUNTIME RESOLUTION TESTS
// =============================================================================

type runtimeLookupSpy struct {
	runtimeselector.Selector
	getRuntimeCalls int
}

func (s *runtimeLookupSpy) GetRuntime(_ context.Context, _, _, _ string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	s.getRuntimeCalls++
	return nil, false, errors.New("runtime lookup must not be called")
}

func TestInferenceService_RuntimeOnlyRuntimeState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	enabledRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "enabled-runtime"},
	}
	disabledRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "disabled-runtime"},
		Spec: v1beta1.ServingRuntimeSpec{
			Disabled: boolPtr(true),
		},
	}

	tests := []struct {
		name     string
		objects  []client.Object
		runtime  *v1beta1.ServingRuntimeRef
		wantErr  string
		noLookup bool
	}{
		{
			name:    "enabled live-synced runtime is allowed",
			objects: []client.Object{enabledRuntime},
			runtime: &v1beta1.ServingRuntimeRef{Name: "enabled-runtime"},
		},
		{
			name:    "disabled live-synced runtime is rejected",
			objects: []client.Object{disabledRuntime},
			runtime: &v1beta1.ServingRuntimeRef{Name: "disabled-runtime", AutoSync: boolPtr(true)},
			wantErr: "disabled",
		},
		{
			name:    "missing live-synced runtime is allowed",
			runtime: &v1beta1.ServingRuntimeRef{Name: "missing-runtime"},
		},
		{
			name:     "disabled source with autoSync false is allowed",
			objects:  []client.Object{disabledRuntime},
			runtime:  &v1beta1.ServingRuntimeRef{Name: "disabled-runtime", AutoSync: boolPtr(false)},
			noLookup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()
			var selector runtimeselector.Selector = runtimeselector.New(fakeClient)
			var lookupSpy *runtimeLookupSpy
			if tt.noLookup {
				lookupSpy = &runtimeLookupSpy{Selector: selector}
				selector = lookupSpy
			}
			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: selector,
			}
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "runtime-only", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: tt.runtime,
					Engine:  &v1beta1.EngineSpec{},
				},
			}

			_, err := validator.validateRuntimeAndModelResolution(context.Background(), isvc)
			if tt.noLookup {
				assert.Zero(t, lookupSpy.getRuntimeCalls)
			}
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestInferenceService_RuntimeResolution(t *testing.T) {
	// Create test models with different configurations
	enabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "enabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelType:          stringPtr("text-generation"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1"),
			},
		},
	}

	disabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "disabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelType:          stringPtr("text-generation"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1"),
			},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(true),
			},
		},
	}

	explicitlyEnabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "explicitly-enabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelType:          stringPtr("text-generation"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1"),
			},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(false), // Explicitly enabled
			},
		},
	}

	modelWithEmptyFormat := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "model-empty-format",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelType:          stringPtr("text-generation"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name: "", // Empty name
			},
		},
	}

	// Create test runtimes
	testRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					Name:    "llama",
					Version: stringPtr("1"),
				},
			},
		},
	}

	runtimeWithWrongVersion := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "wrong-version-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					Name:    "llama",
					Version: stringPtr("2"), // Wrong version
				},
			},
		},
	}

	runtimeWithNoNameMatch := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "no-name-match-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					Name:    "gpt", // Different name
					Version: stringPtr("1"),
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	tests := []struct {
		name    string
		objects []client.Object
		isvc    *v1beta1.InferenceService
		wantErr bool
		errMsg  string
	}{
		{
			name:    "no engine - should skip validation",
			objects: []client.Object{enabledModel, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			wantErr: false,
		},
		{
			name:    "engine with runtime specified - should pass",
			objects: []client.Object{enabledModel, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name: "test-runtime",
					},
					Engine: &v1beta1.EngineSpec{
						Runner: &v1beta1.RunnerSpec{
							Container: v1.Container{
								Image: "test-image:latest",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "engine with complete runner config - should pass",
			objects: []client.Object{enabledModel, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						Runner: &v1beta1.RunnerSpec{
							Container: v1.Container{
								Image: "test-image:latest",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "engine without runtime and no model - should fail",
			objects: []client.Object{enabledModel, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						// No runner, incomplete config
					},
				},
			},
			wantErr: true,
			errMsg:  "model reference is required when runtime is not specified and engine does not have complete runner configuration",
		},
		{
			name:    "model not found",
			objects: []client.Object{}, // Empty - no model
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "nonexistent-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "failed to resolve model nonexistent-model",
		},
		{
			name:    "disabled model",
			objects: []client.Object{disabledModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "disabled-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "model disabled-model is disabled",
		},
		{
			name:    "explicitly enabled model",
			objects: []client.Object{explicitlyEnabledModel, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "explicitly-enabled-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true, // Still fails because runtime matching is complex
			errMsg:  "no supporting runtime found",
		},
		{
			name:    "enabled model with no supporting runtimes",
			objects: []client.Object{enabledModel}, // No runtime
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "enabled-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found for model enabled-model",
		},
		{
			name:    "model with empty format name",
			objects: []client.Object{modelWithEmptyFormat, testRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "model-empty-format",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found",
		},
		{
			name:    "runtime with wrong version",
			objects: []client.Object{enabledModel, runtimeWithWrongVersion},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "enabled-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found",
		},
		{
			name:    "runtime with no name match",
			objects: []client.Object{enabledModel, runtimeWithNoNameMatch},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "enabled-model",
					},
					Engine: &v1beta1.EngineSpec{
						// Incomplete config to trigger resolution
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.New(fakeClient),
			}

			_, err := validator.validateRuntimeAndModelResolution(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// =============================================================================
// COMPREHENSIVE VALIDATION TESTS
// =============================================================================

func TestValidateInferenceService_ComprehensiveErrorPaths(t *testing.T) {
	// Create fake client with test data
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	validator := &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.New(fakeClient),
	}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
		errMsg  string
	}{
		{
			name: "invalid name should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "Invalid-Name", // Invalid format
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			wantErr: true,
			errMsg:  "invalid InferenceService name",
		},
		{
			name: "invalid autoscaler should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					Annotations: map[string]string{
						constants.AutoscalerClass: "invalid-class",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			wantErr: true,
			errMsg:  "is not a supported autoscaler class type",
		},
		{
			name: "invalid target utilization should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					Annotations: map[string]string{
						constants.TargetUtilizationPercentage: "150", // Invalid
					},
				},
				Spec: v1beta1.InferenceServiceSpec{},
			},
			wantErr: true,
			errMsg:  "target utilization percentage should be a [1-100] integer",
		},
		{
			name: "decoder without engine should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Decoder: &v1beta1.DecoderSpec{}, // Decoder without engine
				},
			},
			wantErr: true,
			errMsg:  "decoder cannot be specified without engine",
		},
		{
			name: "engine without runtime and model should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						// Incomplete config, no runtime, no model
					},
				},
			},
			wantErr: true,
			errMsg:  "at least one of spec.model or spec.runtime must be set",
		},
		{
			// Lean path: model omitted, runtime named explicitly. The
			// validator must accept this — model parsing + runtime
			// auto-select is skipped by the controller, and the runtime
			// is fetched as-is.
			name: "no model + runtime named (lean path) - passes",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-lean", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
					Engine:  &v1beta1.EngineSpec{},
				},
			},
		},
		{
			// Lean path failure: only spec.runtime.name=="" — caught by
			// the same Model-OR-Runtime rule.
			name: "no model + empty runtime name should fail",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-lean-bad", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: ""},
					Engine:  &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "at least one of spec.model or spec.runtime must be set",
		},
		// Explicit-pin admission validation.
		{
			name: "explicit revision pin with autoSync=false (cluster) passes",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-ok", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-llama-pd",
						AutoSync: boolPtr(false),
						Revision: stringPtr("cr-srt-llama-pd-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
		},
		{
			name: "explicit revision pin with autoSync=true rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-conflict", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-llama-pd",
						AutoSync: boolPtr(true),
						Revision: stringPtr("cr-srt-llama-pd-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "AutoSync=true would silently ignore the pin",
		},
		{
			name: "explicit revision pin with autoSync omitted (defaults true) rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-defaultsync", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-llama-pd",
						Revision: stringPtr("cr-srt-llama-pd-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "AutoSync=true would silently ignore the pin",
		},
		{
			name: "explicit revision pin naming a DIFFERENT runtime rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-wrong-runtime", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-llama-pd",
						AutoSync: boolPtr(false),
						Revision: stringPtr("cr-srt-other-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "does not match the expected naming convention",
		},
		{
			name: "explicit revision pin with malformed hash rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-bad-hash", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-llama-pd",
						AutoSync: boolPtr(false),
						Revision: stringPtr("cr-srt-llama-pd-XYZ"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "does not match the expected naming convention",
		},
		{
			name: "explicit revision pin for namespaced runtime (correct scope) passes",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-ns-ok", Namespace: "team-a"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-foo",
						Kind:     stringPtr("ServingRuntime"),
						AutoSync: boolPtr(false),
						Revision: stringPtr("r-team-a-srt-foo-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
		},
		{
			name: "explicit revision pin for namespaced runtime, wrong namespace prefix rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "pin-ns-bad", Namespace: "team-a"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "srt-foo",
						Kind:     stringPtr("ServingRuntime"),
						AutoSync: boolPtr(false),
						Revision: stringPtr("r-team-b-srt-foo-abc12345"),
					},
					Engine: &v1beta1.EngineSpec{},
				},
			},
			wantErr: true,
			errMsg:  "does not match the expected naming convention",
		},
		{
			name: "no explicit revision pin → validator no-op",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "no-pin", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-llama-pd", AutoSync: boolPtr(false)},
					Engine:  &v1beta1.EngineSpec{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings, err := validator.validateInferenceService(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Empty(t, warnings)
			}
		})
	}
}

// =============================================================================
// COMPREHENSIVE RESOLVEMODELANDRUNTIME TESTS
// =============================================================================

func TestResolveModelAndRuntime_Comprehensive(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// Simple model that will generate label "mt:llama:1:llama"
	simpleModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "simple-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	// Disabled model
	disabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "disabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(true),
			},
		},
	}

	// Runtime that matches the simple model with AutoSelect=true
	matchingRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "matching-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1), // Optional weight
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true), // Critical for matching
				},
			},
		},
	}

	// Runtime without AutoSelect (won't match)
	nonAutoSelectRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "non-autoselect-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1),
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(false), // Won't match
				},
			},
		},
	}

	tests := []struct {
		name         string
		objects      []client.Object
		isvc         *v1beta1.InferenceService
		wantErr      bool
		errMsg       string
		wantWarnings int
	}{
		{
			name:    "model not found",
			objects: []client.Object{},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "nonexistent-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "failed to resolve model nonexistent-model",
		},
		{
			name:    "model disabled",
			objects: []client.Object{disabledModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "disabled-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "model disabled-model is disabled",
		},
		{
			name:    "successful runtime resolution",
			objects: []client.Object{simpleModel, matchingRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "simple-model",
					},
				},
			},
			wantErr:      false,
			wantWarnings: 1,
		},
		{
			name:    "no supporting runtime found",
			objects: []client.Object{simpleModel, nonAutoSelectRuntime},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "simple-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found for model simple-model",
		},
		{
			name:    "no runtimes at all",
			objects: []client.Object{simpleModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "simple-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "no supporting runtime found for model simple-model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.New(fakeClient),
			}

			warnings, err := validator.resolveModelAndRuntime(context.Background(), tt.isvc, admission.Warnings{})

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.wantWarnings > 0 {
					assert.Len(t, warnings, tt.wantWarnings)
					assert.Contains(t, warnings[0], "will be auto-selected for model")
				}
			}
		})
	}
}

func TestResolveModelAndRuntime_ExplicitShardedRuntimeRequiresConfiguredCacheProvider(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	distribution := v1beta1.DistributionSharded
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sharded-model",
			Namespace: "default",
		},
		Spec: v1beta1.BaseModelSpec{
			Distribution: &distribution,
			ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
		},
	}
	cacheRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cache-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat:         &v1beta1.ModelFormat{Name: "safetensors"},
					ModelCacheProviders: []v1beta1.ModelCacheProvider{v1beta1.DragonFly},
				},
			},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{Name: "sharded-model"},
			Runtime: &v1beta1.ServingRuntimeRef{
				Name: "cache-runtime",
			},
		},
	}

	tests := []struct {
		name          string
		cacheProvider string
		wantErr       bool
		errMsg        string
	}{
		{
			name:          "configured provider accepts explicit runtime",
			cacheProvider: string(v1beta1.DragonFly),
		},
		{
			name:    "missing provider rejects explicit runtime",
			wantErr: true,
			errMsg:  "no model cache provider is configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(model.DeepCopy(), cacheRuntime.DeepCopy()).
				Build()
			selectorConfig := runtimeselector.NewConfig(fakeClient)
			selectorConfig.ModelCacheProvider = tt.cacheProvider
			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.NewWithConfig(selectorConfig),
			}

			warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc.DeepCopy(), admission.Warnings{})

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}
			assert.NoError(t, err)
			// A valid explicit runtime emits no advisory warning — the
			// redundant "is valid for model" Warning was dropped.
			assert.Empty(t, warnings)
		})
	}
}

// TestResolveModelAndRuntime_ExplicitRuntimeCompatIsAdvisory pins the
// behavior change: when a runtime is named explicitly, a pure
// supportedModelFormats mismatch (format / architecture / framework) is
// downgraded from a hard admission error to an advisory warning and the
// ISVC is admitted. The auto-select path (no explicit runtime) must still
// hard-fail when nothing supports the model.
func TestResolveModelAndRuntime_ExplicitRuntimeCompatIsAdvisory(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// llama model.
	llamaModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-model"},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	// A generic runtime that only *declares* support for a different
	// architecture/format ("gpt"). It does not enumerate "llama", so the
	// compatibility matcher reports a mismatch — but the operator named it
	// explicitly, so admission should warn-and-admit rather than reject.
	genericRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "generic-runtime"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat:       &v1beta1.ModelFormat{Name: "gpt", Version: stringPtr("1.0.0")},
					ModelArchitecture: stringPtr("gpt"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	// A runtime that genuinely declares support for llama.
	matchingRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "matching-runtime"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat:       &v1beta1.ModelFormat{Name: "llama", Version: stringPtr("1.0.0"), Weight: int64(1)},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	t.Run("explicit runtime + format mismatch is admitted with advisory warning", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(llamaModel.DeepCopy(), genericRuntime.DeepCopy()).
			Build()
		validator := &InferenceServiceValidator{
			Client:          fakeClient,
			RuntimeSelector: runtimeselector.New(fakeClient),
		}
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Model:   &v1beta1.ModelRef{Name: "llama-model"},
				Runtime: &v1beta1.ServingRuntimeRef{Name: "generic-runtime"},
			},
		}

		warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})

		assert.NoError(t, err, "explicit-runtime compat mismatch must be admitted, not rejected")
		assert.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "does not declare support for model")
		assert.Contains(t, warnings[0], "proceeding because the runtime was named explicitly")
		// The dropped success-warning phrasing must not reappear.
		assert.NotContains(t, warnings[0], "is valid for model")
	})

	t.Run("explicit runtime that matches is admitted with no warning", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(llamaModel.DeepCopy(), matchingRuntime.DeepCopy()).
			Build()
		validator := &InferenceServiceValidator{
			Client:          fakeClient,
			RuntimeSelector: runtimeselector.New(fakeClient),
		}
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Model:   &v1beta1.ModelRef{Name: "llama-model"},
				Runtime: &v1beta1.ServingRuntimeRef{Name: "matching-runtime"},
			},
		}

		warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})

		assert.NoError(t, err)
		assert.Empty(t, warnings, "a matching explicit runtime emits no advisory warning")
	})

	t.Run("explicit runtime not found still hard-fails", func(t *testing.T) {
		// A non-existent runtime is not a declared-format mismatch; it must
		// stay a hard error even on the explicit path.
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(llamaModel.DeepCopy()).
			Build()
		validator := &InferenceServiceValidator{
			Client:          fakeClient,
			RuntimeSelector: runtimeselector.New(fakeClient),
		}
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Model:   &v1beta1.ModelRef{Name: "llama-model"},
				Runtime: &v1beta1.ServingRuntimeRef{Name: "does-not-exist"},
			},
		}

		_, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("auto-select with no matching runtime still hard-fails", func(t *testing.T) {
		// Same model, only the non-matching generic runtime present, and NO
		// explicit runtime named: OME is choosing, so compat MUST gate.
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(llamaModel.DeepCopy(), genericRuntime.DeepCopy()).
			Build()
		validator := &InferenceServiceValidator{
			Client:          fakeClient,
			RuntimeSelector: runtimeselector.New(fakeClient),
		}
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Model: &v1beta1.ModelRef{Name: "llama-model"},
			},
		}

		_, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})

		assert.Error(t, err, "auto-select must still reject when nothing supports the model")
		assert.Contains(t, err.Error(), "no supporting runtime found for model llama-model")
	})
}

func TestValidateCreate_ExplicitShardedRuntimeRequiresConfiguredCacheProvider(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	distribution := v1beta1.DistributionSharded
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sharded-model",
			Namespace: "default",
		},
		Spec: v1beta1.BaseModelSpec{
			Distribution: &distribution,
			ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
		},
	}
	cacheRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cache-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat:         &v1beta1.ModelFormat{Name: "safetensors"},
					ModelCacheProviders: []v1beta1.ModelCacheProvider{v1beta1.DragonFly},
				},
			},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{Name: "sharded-model"},
			Runtime: &v1beta1.ServingRuntimeRef{
				Name: "cache-runtime",
			},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Image: "example.com/runtime:latest",
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(model, cacheRuntime).
		Build()
	selectorConfig := runtimeselector.NewConfig(fakeClient)
	validator := &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.NewWithConfig(selectorConfig),
	}

	_, err := validator.ValidateCreate(context.Background(), isvc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no model cache provider is configured")
}

// Test edge cases and warning scenarios
func TestResolveModelAndRuntime_EdgeCases(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// Model with no disabled field (should be treated as enabled)
	enabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "enabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
			// No Disabled field - should be treated as enabled
		},
	}

	// Model explicitly enabled
	explicitlyEnabledModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "explicitly-enabled-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(false), // Explicitly enabled
			},
		},
	}

	matchingRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "matching-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1), // default value is 1
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	tests := []struct {
		name         string
		objects      []client.Object
		modelName    string
		wantErr      bool
		wantWarnings int
	}{
		{
			name:         "model with no disabled field",
			objects:      []client.Object{enabledModel, matchingRuntime},
			modelName:    "enabled-model",
			wantErr:      false,
			wantWarnings: 1,
		},
		{
			name:         "explicitly enabled model",
			objects:      []client.Object{explicitlyEnabledModel, matchingRuntime},
			modelName:    "explicitly-enabled-model",
			wantErr:      false,
			wantWarnings: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.New(fakeClient),
			}

			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: tt.modelName,
					},
				},
			}

			warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.wantWarnings > 0 {
					assert.Len(t, warnings, tt.wantWarnings)
				}
			}
		})
	}
}

// Test warning preservation and multiple runtimes
func TestResolveModelAndRuntime_WarningHandling(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	runtime1 := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime-1",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1),
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	runtime2 := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime-2",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1),
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(model, runtime1, runtime2).
		Build()

	validator := &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.New(fakeClient),
	}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{
				Name: "test-model",
			},
		},
	}

	t.Run("multiple runtimes found", func(t *testing.T) {
		warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})
		assert.NoError(t, err)
		assert.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "will be auto-selected for model test-model")
	})

	t.Run("existing warnings preserved", func(t *testing.T) {
		initialWarnings := admission.Warnings{"existing warning"}
		warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, initialWarnings)
		assert.NoError(t, err)
		assert.Len(t, warnings, 2)
		assert.Equal(t, "existing warning", warnings[0])
		assert.Contains(t, warnings[1], "will be auto-selected")
	})
}

// Test namespace vs cluster model precedence
func TestResolveModelAndRuntime_NamespacePrecedence(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// Namespace-scoped model
	namespacedModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "test-namespace",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	// Cluster-scoped model with same name
	clusterModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("different"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "different",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	runtime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-runtime",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					ModelFormat: &v1beta1.ModelFormat{
						Name:    "llama",
						Version: stringPtr("1.0.0"),
						Weight:  int64(1),
					},
					ModelArchitecture: stringPtr("llama"),
					AutoSelect:        boolPtr(true),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(namespacedModel, clusterModel, runtime).
		Build()

	validator := &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.New(fakeClient),
	}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "test-namespace",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{
				Name: "test-model",
			},
		},
	}

	warnings, err := validator.resolveModelAndRuntime(context.Background(), isvc, admission.Warnings{})
	assert.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "will be auto-selected for model test-model")
}

// =============================================================================
// UTILITY TESTS
// =============================================================================

// Test GetIntReference function (0% coverage)
func TestGetIntReference(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{
			name:     "positive number",
			input:    42,
			expected: 42,
		},
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "negative number",
			input:    -10,
			expected: -10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetIntReference(tt.input)
			assert.NotNil(t, result)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// =============================================================================
// MODEL EXISTS VALIDATION TESTS
// =============================================================================

func TestValidateModelExists(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// Create test models
	clusterModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "existing-cluster-model",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	namespaceModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-namespace-model",
			Namespace: "test-namespace",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelArchitecture:  stringPtr("llama"),
			ModelParameterSize: stringPtr("7B"),
			ModelFormat: v1beta1.ModelFormat{
				Name:    "llama",
				Version: stringPtr("1.0.0"),
			},
		},
	}

	tests := []struct {
		name    string
		objects []client.Object
		isvc    *v1beta1.InferenceService
		wantErr bool
		errMsg  string
	}{
		{
			name:    "no model reference - should pass",
			objects: []client.Object{},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					// No Model reference
				},
			},
			wantErr: false,
		},
		{
			name:    "model reference with empty name - should pass",
			objects: []client.Object{},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "", // Empty name
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "referenced ClusterBaseModel exists - should pass",
			objects: []client.Object{clusterModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "existing-cluster-model",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "referenced BaseModel exists in namespace - should pass",
			objects: []client.Object{namespaceModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "test-namespace",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "existing-namespace-model",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "referenced model does not exist - should fail",
			objects: []client.Object{}, // No models
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "nonexistent-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "referenced model \"nonexistent-model\" not found in namespace \"default\"",
		},
		{
			name:    "referenced BaseModel exists but in different namespace - should fail",
			objects: []client.Object{namespaceModel}, // Model is in test-namespace
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "different-namespace", // Different namespace
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "existing-namespace-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "referenced model \"existing-namespace-model\" not found in namespace \"different-namespace\"",
		},
		{
			name:    "ClusterBaseModel is accessible from any namespace - should pass",
			objects: []client.Object{clusterModel},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "any-namespace",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "existing-cluster-model",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.New(fakeClient),
			}

			err := validator.validateModelExists(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test that validateModelExists is called during full validation
func TestValidateInferenceService_ModelExistsIntegration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	tests := []struct {
		name    string
		objects []client.Object
		isvc    *v1beta1.InferenceService
		wantErr bool
		errMsg  string
	}{
		{
			name:    "full validation rejects missing model",
			objects: []client.Object{},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "nonexistent-model",
					},
				},
			},
			wantErr: true,
			errMsg:  "referenced model \"nonexistent-model\" not found in namespace \"default\"",
		},
		{
			name: "full validation passes with existing model",
			objects: []client.Object{
				&v1beta1.ClusterBaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-model",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelArchitecture:  stringPtr("llama"),
						ModelParameterSize: stringPtr("7B"),
						ModelFormat: v1beta1.ModelFormat{
							Name:    "llama",
							Version: stringPtr("1.0.0"),
						},
					},
				},
			},
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "existing-model",
					},
					// No Engine, so runtime resolution won't be triggered
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			validator := &InferenceServiceValidator{
				Client:          fakeClient,
				RuntimeSelector: runtimeselector.New(fakeClient),
			}

			_, err := validator.validateInferenceService(context.Background(), tt.isvc)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func boolPtr(b bool) *bool {
	return &b
}

// TestValidateOverlays_StructuralChecks covers the schema-level overlay
// rejections (duplicates, self-reference, env-name collision) that fire
// before any apiserver lookups happen — no fake client needed.
func TestValidateOverlays_StructuralChecks(t *testing.T) {
	v := &InferenceServiceValidator{}
	clusterKind := "ClusterBaseModel"
	apiGroup := "ome.io"
	tests := []struct {
		name     string
		overlays []v1beta1.ModelOverlayRef
		wantErr  string // empty = expect success
	}{
		{
			name:     "empty overlays accepted",
			overlays: nil,
		},
		{
			name: "single overlay accepted",
			overlays: []v1beta1.ModelOverlayRef{
				{Name: "foo-pvc", Kind: &clusterKind, APIGroup: &apiGroup},
			},
		},
		{
			name: "empty overlay name rejected",
			overlays: []v1beta1.ModelOverlayRef{
				{Name: "", Kind: &clusterKind, APIGroup: &apiGroup},
			},
			wantErr: "overlay name cannot be empty",
		},
		{
			name: "overlay matching primary rejected",
			overlays: []v1beta1.ModelOverlayRef{
				{Name: "primary", Kind: &clusterKind, APIGroup: &apiGroup},
			},
			wantErr: "must not reference the primary model",
		},
		{
			name: "duplicate overlay name rejected",
			overlays: []v1beta1.ModelOverlayRef{
				{Name: "foo-pvc"}, {Name: "foo-pvc"},
			},
			wantErr: "declared more than once",
		},
		{
			name: "sanitized env-var collision rejected",
			overlays: []v1beta1.ModelOverlayRef{
				{Name: "foo-bar"}, {Name: "foo_bar"},
			},
			wantErr: "both sanitize to env var OVERLAY_FOO_BAR_MODEL_PATH",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "default"},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{Name: "primary", Overlays: tc.overlays},
				},
			}
			err := v.validateOverlays(isvc)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// TestDeploymentStrategyWarnings pins the footgun warning: deploymentStrategy is
// raw-only, so setting it on an OMENative/PD component is a silent no-op and must
// warn (pointing at lifecycle.updateStrategy); raw and no-strategy must not warn.
func TestDeploymentStrategyWarnings(t *testing.T) {
	strategy := func() *appsv1.DeploymentStrategy {
		return &appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType}
	}
	engineWith := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{DeploymentStrategy: strategy()},
	}
	modeAnn := func(m constants.DeploymentModeType) map[string]string {
		return map[string]string{constants.DeploymentMode: string(m)}
	}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantLen int
	}{
		{
			name: "OMENative engine with deploymentStrategy warns",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: modeAnn(constants.OMENative)},
				Spec:       v1beta1.InferenceServiceSpec{Engine: engineWith},
			},
			wantLen: 1,
		},
		{
			name: "RawDeployment engine with deploymentStrategy does not warn (honored)",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: modeAnn(constants.RawDeployment)},
				Spec:       v1beta1.InferenceServiceSpec{Engine: engineWith},
			},
			wantLen: 0,
		},
		{
			name: "OMENative without deploymentStrategy does not warn",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: modeAnn(constants.OMENative)},
				Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
			},
			wantLen: 0,
		},
		{
			name: "no resolved mode does not warn",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{Engine: engineWith},
			},
			wantLen: 0,
		},
		{
			name: "PD decoder with deploymentStrategy warns",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: modeAnn(constants.PDDisaggregated)},
				Spec: v1beta1.InferenceServiceSpec{
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{DeploymentStrategy: strategy()},
					},
				},
			},
			wantLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Len(t, deploymentStrategyWarnings(tt.isvc), tt.wantLen)
		})
	}
}

// =============================================================================
// MIGRATION-REQUEST ANNOTATION VALIDATION (mailbox admission)
// =============================================================================

// migrationAnnISVC builds a minimal ISVC that passes every other
// validation rule (runtime-only, no model, no client lookups) so each
// case exercises only the migration-request annotation checks.
// withDecoder adds Engine+Decoder so the decoder component exists.
func migrationAnnISVC(annotations map[string]string, withDecoder bool) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mig-isvc",
			Namespace:   "default",
			Annotations: annotations,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime", AutoSync: boolPtr(false)},
		},
	}
	if withDecoder {
		isvc.Spec.Engine = &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Containers: []v1.Container{{Name: "ome-container", Image: "x"}},
			},
		}
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			PodSpec: v1beta1.PodSpec{
				Containers: []v1.Container{{Name: "ome-container", Image: "x"}},
			},
		}
	}
	return isvc
}

func TestValidateUpdate_MigrationRequestAnnotations(t *testing.T) {
	const prefix = "ome.io/migration-request-v1-"
	validEngineReq := `{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"node-a"}`

	tests := []struct {
		name        string
		oldAnn      map[string]string
		newAnn      map[string]string
		withDecoder bool
		wantErr     string // empty = admitted
	}{
		{
			name:    "valid engine request added is accepted",
			newAnn:  map[string]string{prefix + "u1": validEngineReq},
			wantErr: "",
		},
		{
			name:    "garbage JSON denied with the request UUID named",
			newAnn:  map[string]string{prefix + "u2": `{not-json`},
			wantErr: "migration request u2",
		},
		{
			name:    "unknown schemaVersion denied",
			newAnn:  map[string]string{prefix + "u3": `{"schemaVersion":"v9","component":"engine","instance":0,"from_node":"n"}`},
			wantErr: "UnsupportedSchemaVersion",
		},
		{
			name:    "component the ISVC lacks is denied",
			newAnn:  map[string]string{prefix + "u4": `{"schemaVersion":"v1","component":"decoder","instance":0,"from_node":"n"}`},
			wantErr: `component "decoder" does not exist`,
		},
		{
			name:        "decoder request accepted when the spec declares a decoder",
			newAnn:      map[string]string{prefix + "u5": `{"schemaVersion":"v1","component":"decoder","instance":0,"from_node":"n"}`},
			withDecoder: true,
			wantErr:     "",
		},
		{
			name:    "unknown component name denied",
			newAnn:  map[string]string{prefix + "u6": `{"schemaVersion":"v1","component":"sidecar","instance":0,"from_node":"n"}`},
			wantErr: `component "sidecar" does not exist`,
		},
		{
			name:    "negative instance denied",
			newAnn:  map[string]string{prefix + "u7": `{"schemaVersion":"v1","component":"engine","instance":-1,"from_node":"n"}`},
			wantErr: "instance must be >= 0",
		},
		{
			name:    "bare prefix key (no UUID) denied",
			newAnn:  map[string]string{prefix: validEngineReq},
			wantErr: "carries no request UUID",
		},
		{
			name:    "pre-existing unchanged annotation is NOT re-validated",
			oldAnn:  map[string]string{prefix + "u8": `{not-json`},
			newAnn:  map[string]string{prefix + "u8": `{not-json`},
			wantErr: "",
		},
		{
			name:    "changed value of an existing key IS re-validated",
			oldAnn:  map[string]string{prefix + "u9": validEngineReq},
			newAnn:  map[string]string{prefix + "u9": `{not-json`},
			wantErr: "migration request u9",
		},
		{
			name:    "deletion of an annotation is always allowed",
			oldAnn:  map[string]string{prefix + "u10": `{not-json`},
			newAnn:  nil,
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			oldIsvc := migrationAnnISVC(tt.oldAnn, tt.withDecoder)
			newIsvc := migrationAnnISVC(tt.newAnn, tt.withDecoder)
			_, err := v.ValidateUpdate(context.Background(), oldIsvc, newIsvc)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

// TestValidateCreate_MigrationRequestAnnotations pins CREATE parity:
// migration-request annotations present at object creation get the
// same validation as ones added by an update (old=nil semantics —
// every present annotation is an add).
func TestValidateCreate_MigrationRequestAnnotations(t *testing.T) {
	const prefix = "ome.io/migration-request-v1-"
	tests := []struct {
		name    string
		ann     map[string]string
		wantErr string
	}{
		{
			name:    "valid engine request on create is accepted",
			ann:     map[string]string{prefix + "c1": `{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"node-a"}`},
			wantErr: "",
		},
		{
			name:    "garbage JSON on create is denied",
			ann:     map[string]string{prefix + "c2": `{not-json`},
			wantErr: "migration request c2",
		},
		{
			name:    "unknown schemaVersion on create is denied",
			ann:     map[string]string{prefix + "c3": `{"schemaVersion":"v9","component":"engine","instance":0,"from_node":"n"}`},
			wantErr: "UnsupportedSchemaVersion",
		},
		{
			name:    "component the ISVC lacks on create is denied",
			ann:     map[string]string{prefix + "c4": `{"schemaVersion":"v1","component":"decoder","instance":0,"from_node":"n"}`},
			wantErr: `component "decoder" does not exist`,
		},
		{
			name:    "negative instance on create is denied",
			ann:     map[string]string{prefix + "c5": `{"schemaVersion":"v1","component":"engine","instance":-1,"from_node":"n"}`},
			wantErr: "instance must be >= 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			_, err := v.ValidateCreate(context.Background(), migrationAnnISVC(tt.ann, false))
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

// =============================================================================
// REPLICA BOUNDS + POD DISRUPTION BUDGET VALIDATION TESTS
// =============================================================================

// componentExtISVC builds a minimal valid ISVC whose engine skips
// runtime/model resolution (explicit runtime ref with autoSync=false plus a
// full runner config), so per-Component extension checks are exercised in
// isolation. mutate applies the per-case component fields.
func componentExtISVC(mutate func(isvc *v1beta1.InferenceService)) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime", AutoSync: boolPtr(false)},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{Image: "test-image:latest"}},
			},
		},
	}
	if mutate != nil {
		mutate(isvc)
	}
	return isvc
}

// runCreateAndUpdate asserts the same admission outcome for both the
// create and the update webhook paths (old object is a valid baseline).
func runCreateAndUpdate(t *testing.T, newIsvc *v1beta1.InferenceService, errMsg string) {
	t.Helper()
	v := &InferenceServiceValidator{}
	oldIsvc := componentExtISVC(nil)
	ops := []struct {
		name string
		run  func() error
	}{
		{"create", func() error {
			_, err := v.ValidateCreate(context.Background(), newIsvc)
			return err
		}},
		{"update", func() error {
			_, err := v.ValidateUpdate(context.Background(), oldIsvc, newIsvc)
			return err
		}},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			err := op.run()
			if errMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), errMsg)
			}
		})
	}
}

func TestInferenceService_ComponentReplicaBoundsValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(isvc *v1beta1.InferenceService)
		errMsg string // empty = expect admission
	}{
		{
			name: "engine negative minReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = intPtr(-1)
			},
			errMsg: "spec.engine.minReplicas must be >= 0",
		},
		{
			name: "engine explicit maxReplicas below 1 rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MaxReplicas = -1
			},
			errMsg: "spec.engine.maxReplicas must be >= 1",
		},
		{
			name: "engine minReplicas greater than maxReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = intPtr(5)
				isvc.Spec.Engine.MaxReplicas = 2
			},
			errMsg: "spec.engine.minReplicas (5) must be <= maxReplicas (2)",
		},
		{
			name: "decoder negative minReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(-2)},
				}
			},
			errMsg: "spec.decoder.minReplicas must be >= 0",
		},
		{
			name: "decoder minReplicas greater than maxReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(4), MaxReplicas: 3},
				}
			},
			errMsg: "spec.decoder.minReplicas (4) must be <= maxReplicas (3)",
		},
		{
			name: "router negative minReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(-1)},
				}
			},
			errMsg: "spec.router.minReplicas must be >= 0",
		},
		{
			name: "router explicit maxReplicas below 1 rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: -3},
				}
			},
			errMsg: "spec.router.maxReplicas must be >= 1",
		},
		{
			name: "router minReplicas greater than maxReplicas rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2), MaxReplicas: 1},
				}
			},
			errMsg: "spec.router.minReplicas (2) must be <= maxReplicas (1)",
		},
		{
			name: "valid engine bounds accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = intPtr(1)
				isvc.Spec.Engine.MaxReplicas = 3
			},
		},
		{
			name: "valid decoder bounds accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(2), MaxReplicas: 4},
				}
			},
		},
		{
			name: "valid router bounds accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(1), MaxReplicas: 2},
				}
			},
		},
		{
			name: "engine minReplicas above unset maxReplicas accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				// MaxReplicas zero value means "not set"; the min<=max
				// check must not treat it as an explicit bound of 0.
				isvc.Spec.Engine.MinReplicas = intPtr(5)
			},
		},
		{
			name: "engine minReplicas zero stays legal for scale-to-zero",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinReplicas = intPtr(0)
				// KEDA opt-in satisfies the separate scale-to-zero gate;
				// the bounds check itself must admit minReplicas=0.
				isvc.Annotations = map[string]string{
					constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runCreateAndUpdate(t, componentExtISVC(tt.mutate), tt.errMsg)
		})
	}
}

func TestInferenceService_ComponentPodDisruptionBudgetValidation(t *testing.T) {
	budget := func(v intstr.IntOrString) *intstr.IntOrString { return &v }

	tests := []struct {
		name   string
		mutate func(isvc *v1beta1.InferenceService)
		errMsg string // empty = expect admission
	}{
		{
			name: "engine with both budgets rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinAvailable = budget(intstr.FromInt(1))
				isvc.Spec.Engine.MaxUnavailable = budget(intstr.FromInt(1))
			},
			errMsg: "spec.engine.minAvailable and spec.engine.maxUnavailable cannot both be set",
		},
		{
			name: "decoder with both budgets rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinAvailable:   budget(intstr.FromString("50%")),
						MaxUnavailable: budget(intstr.FromInt(1)),
					},
				}
			},
			errMsg: "spec.decoder.minAvailable and spec.decoder.maxUnavailable cannot both be set",
		},
		{
			name: "router with both budgets rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinAvailable:   budget(intstr.FromInt(2)),
						MaxUnavailable: budget(intstr.FromString("25%")),
					},
				}
			},
			errMsg: "spec.router.minAvailable and spec.router.maxUnavailable cannot both be set",
		},
		{
			name: "engine minAvailable percentage above 100 rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinAvailable = budget(intstr.FromString("150%"))
			},
			errMsg: "spec.engine.minAvailable must be a non-negative integer or a percentage",
		},
		{
			name: "engine negative maxUnavailable rejected",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MaxUnavailable = budget(intstr.FromInt(-1))
			},
			errMsg: "spec.engine.maxUnavailable must be a non-negative integer or a percentage",
		},
		{
			name: "engine minAvailable alone accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MinAvailable = budget(intstr.FromInt(1))
			},
		},
		{
			name: "engine maxUnavailable alone accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.MaxUnavailable = budget(intstr.FromString("25%"))
			},
		},
		{
			name: "decoder and router with one budget each accepted",
			mutate: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MinAvailable: budget(intstr.FromInt(1)),
					},
				}
				isvc.Spec.Router = &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						MaxUnavailable: budget(intstr.FromInt(1)),
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runCreateAndUpdate(t, componentExtISVC(tt.mutate), tt.errMsg)
		})
	}
}
