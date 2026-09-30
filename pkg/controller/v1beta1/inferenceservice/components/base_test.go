package components

import (
	"testing"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	"sigs.k8s.io/ome/pkg/render"
)

func TestUpdatePodSpecNodeSelector(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                              string
		baseModel                         *v1beta1.BaseModelSpec
		baseModelMeta                     *metav1.ObjectMeta
		fineTunedServingWithMergedWeights bool
		existingNodeSelector              map[string]string
		expectedLabelKey                  string
		expectNodeSelector                bool
	}{
		{
			name: "BaseModel with namespace adds node selector",
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "llama-3-8b",
				Namespace: "default",
			},
			expectedLabelKey:   "models.ome.io/default.basemodel.llama-3-8b",
			expectNodeSelector: true,
		},
		{
			name: "ClusterBaseModel without namespace adds node selector",
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "mixtral-8x7b",
				// No namespace for ClusterBaseModel
			},
			expectedLabelKey:   "models.ome.io/clusterbasemodel.mixtral-8x7b",
			expectNodeSelector: true,
		},
		{
			name: "Existing node selector should be preserved",
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "model-1",
				Namespace: "test-ns",
			},
			existingNodeSelector: map[string]string{
				"existing-key": "existing-value",
			},
			expectedLabelKey:   "models.ome.io/test-ns.basemodel.model-1",
			expectNodeSelector: true,
		},
		{
			name: "Skip node selector for merged fine-tuned weights",
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "base-model",
				Namespace: "default",
			},
			fineTunedServingWithMergedWeights: true,
			expectNodeSelector:                false,
		},
		{
			name: "Skip model node selector for sharded model",
			baseModel: &v1beta1.BaseModelSpec{
				Distribution: distributionPtr(v1beta1.DistributionSharded),
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "sharded-model",
				Namespace: "default",
			},
			expectNodeSelector: false,
		},
		{
			name:               "No base model",
			baseModel:          nil,
			baseModelMeta:      nil,
			expectNodeSelector: false,
		},
		{
			name: "Long model names should be handled",
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "very-long-model-name-that-exceeds-normal-length-limits-and-should-be-truncated",
				Namespace: "long-namespace-name",
			},
			expectedLabelKey:   constants.GetBaseModelLabel("long-namespace-name", "very-long-model-name-that-exceeds-normal-length-limits-and-should-be-truncated"),
			expectNodeSelector: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create BaseComponentFields
			b := &BaseComponentFields{Piece: render.Piece{
				BaseModel:                         tt.baseModel,
				BaseModelMeta:                     tt.baseModelMeta,
				FineTunedServingWithMergedWeights: tt.fineTunedServingWithMergedWeights,
				Log:                               ctrl.Log.WithName("test"),
			}}

			// Create pod spec with existing node selector if provided
			podSpec := &v1.PodSpec{}
			if tt.existingNodeSelector != nil {
				podSpec.NodeSelector = make(map[string]string)
				for k, v := range tt.existingNodeSelector {
					podSpec.NodeSelector[k] = v
				}
			}

			// Create inference service
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			}

			// Call the function
			UpdatePodSpecNodeSelector(b, isvc, podSpec, "")

			// Verify the result
			if !tt.expectNodeSelector {
				// Should not have added any node selector for model
				if podSpec.NodeSelector == nil {
					return // OK - no node selector added
				}
				// Make sure we didn't add model node selector
				for key := range podSpec.NodeSelector {
					g.Expect(key).NotTo(gomega.HavePrefix("models.ome.io/"))
				}
				return
			}

			// Should have node selector
			g.Expect(podSpec.NodeSelector).NotTo(gomega.BeNil())

			// Check that the expected label key exists with value "Ready"
			value, found := podSpec.NodeSelector[tt.expectedLabelKey]
			g.Expect(found).To(gomega.BeTrue(), "Model node selector label not found: %s", tt.expectedLabelKey)
			g.Expect(value).To(gomega.Equal("Ready"))

			// If there was existing node selector, verify it's preserved
			if tt.existingNodeSelector != nil {
				for k, v := range tt.existingNodeSelector {
					existingValue, existingFound := podSpec.NodeSelector[k]
					g.Expect(existingFound).To(gomega.BeTrue(), "Existing node selector should be preserved")
					g.Expect(existingValue).To(gomega.Equal(v))
				}
				// Should have existing labels + new model label
				g.Expect(podSpec.NodeSelector).To(gomega.HaveLen(len(tt.existingNodeSelector) + 1))
			}
		})
	}
}

func TestUpdatePodSpecNodeSelectorWithoutModel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	b := &BaseComponentFields{Piece: render.Piece{
		Runtime: &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				NodeSelector: map[string]string{
					"accelerator": "gpu",
					"zone":        "runtime-zone",
				},
			},
		},
	}}
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					NodeSelector: map[string]string{
						"zone": "isvc-zone",
					},
				},
			},
		},
	}
	podSpec := &v1.PodSpec{}

	UpdatePodSpecNodeSelector(b, isvc, podSpec, v1beta1.EngineComponent)

	g.Expect(podSpec.NodeSelector).To(gomega.Equal(map[string]string{
		"accelerator": "gpu",
		"zone":        "isvc-zone",
	}))
}

func TestUpdateModelMountsSkipShardedModel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	modelPath := "/models/sharded-model"
	b := &BaseComponentFields{Piece: render.Piece{
		BaseModel: &v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				Path: &modelPath,
			},
			Distribution: distributionPtr(v1beta1.DistributionSharded),
		},
		BaseModelMeta: &metav1.ObjectMeta{
			Name:      "sharded-model",
			Namespace: "default",
		},
		Log: ctrl.Log.WithName("test"),
	}}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
	}
	objectMeta := &metav1.ObjectMeta{}
	container := &v1.Container{}
	podSpec := &v1.PodSpec{}

	UpdateVolumeMounts(b, isvc, container, objectMeta)
	UpdatePodSpecVolumes(b, isvc, podSpec, objectMeta)

	g.Expect(container.VolumeMounts).To(gomega.BeEmpty())
	g.Expect(podSpec.Volumes).To(gomega.BeEmpty())
}

func distributionPtr(distribution v1beta1.Distribution) *v1beta1.Distribution {
	return &distribution
}

func TestProcessBaseLabels(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Test that ProcessBaseLabels adds the correct labels
	b := &BaseComponentFields{Piece: render.Piece{
		BaseModel: &v1beta1.BaseModelSpec{
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Vendor: stringPtr("meta"),
			},
		},
		BaseModelMeta: &metav1.ObjectMeta{
			Name:      "test-model",
			Namespace: "default",
			Annotations: map[string]string{
				constants.ModelCategoryAnnotation: "LARGE",
			},
		},
		RuntimeName:      "test-runtime",
		FineTunedServing: true,
		Log:              logr.Discard(),
	}}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
	}

	existingLabels := map[string]string{
		"custom-label": "custom-value",
	}

	labels, err := ProcessBaseLabels(b, isvc, v1beta1.EngineComponent, existingLabels)
	g.Expect(err).To(gomega.BeNil())

	// Check expected labels
	g.Expect(labels).To(gomega.HaveKeyWithValue("custom-label", "custom-value"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.InferenceServicePodLabelKey, "test-isvc"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.OMEComponentLabel, "engine"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.ServingRuntimeLabelKey, "test-runtime"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.FTServingLabelKey, "true"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.InferenceServiceBaseModelNameLabelKey, "test-model"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.InferenceServiceBaseModelSizeLabelKey, "LARGE"))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.BaseModelTypeLabelKey, string(constants.ServingBaseModel)))
	g.Expect(labels).To(gomega.HaveKeyWithValue(constants.BaseModelVendorLabelKey, "meta"))
}

// TestProcessBaseAnnotationsFTStrategy pins the fine-tuned strategy
// annotation contract: a string strategy is stamped; an absent or
// non-string strategy is a loud error (never a silent nil annotations
// map, never a type-assertion panic).
func TestProcessBaseAnnotationsFTStrategy(t *testing.T) {
	mkFields := func(hyperParameters string) *BaseComponentFields {
		return &BaseComponentFields{Piece: render.Piece{
			FineTunedServing: true,
			FineTunedWeights: []*v1beta1.FineTunedWeight{{
				ObjectMeta: metav1.ObjectMeta{Name: "ftw-1"},
				Spec: v1beta1.FineTunedWeightSpec{
					HyperParameters: runtime.RawExtension{Raw: []byte(hyperParameters)},
				},
			}},
			Log: logr.Discard(),
		}}
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
	}

	tests := []struct {
		name            string
		hyperParameters string
		wantErr         bool
		wantStrategy    string
	}{
		{
			name:            "string strategy stamped",
			hyperParameters: `{"strategy":"tfew"}`,
			wantStrategy:    "tfew",
		},
		{
			name:            "absent strategy is an error, not silent success",
			hyperParameters: `{"other":"x"}`,
			wantErr:         true,
		},
		{
			name:            "non-string strategy is an error, not a panic",
			hyperParameters: `{"strategy":42}`,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			var annotations map[string]string
			var err error
			g.Expect(func() {
				annotations, err = ProcessBaseAnnotations(mkFields(tt.hyperParameters), isvc, map[string]string{})
			}).NotTo(gomega.Panic())
			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
				g.Expect(annotations).To(gomega.BeNil())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(annotations).To(gomega.HaveKeyWithValue(constants.FineTunedWeightFTStrategyKey, tt.wantStrategy))
			}
		})
	}
}

// TestProcessBaseLabelsNonStringStrategy verifies a non-string strategy
// hyper-parameter is a returned error rather than a reconcile panic.
func TestProcessBaseLabelsNonStringStrategy(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := &BaseComponentFields{Piece: render.Piece{
		FineTunedServing: true,
		FineTunedWeights: []*v1beta1.FineTunedWeight{{
			ObjectMeta: metav1.ObjectMeta{Name: "ftw-1"},
			Spec: v1beta1.FineTunedWeightSpec{
				HyperParameters: runtime.RawExtension{Raw: []byte(`{"strategy":{"nested":true}}`)},
			},
		}},
		Log: logr.Discard(),
	}}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
	}

	var err error
	g.Expect(func() {
		_, err = ProcessBaseLabels(b, isvc, v1beta1.EngineComponent, nil)
	}).NotTo(gomega.Panic())
	g.Expect(err).To(gomega.HaveOccurred())
}

// TestProcessComponentLabelsDoesNotMutateISVCLabels guards the copy
// semantics: the per-Component label build must never write through to
// the live ISVC label map.
func TestProcessComponentLabelsDoesNotMutateISVCLabels(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := &BaseComponentFields{Piece: render.Piece{
		RuntimeName: "test-runtime",
		Log:         logr.Discard(),
	}}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			Labels:    map[string]string{"user-label": "user-value"},
		},
	}

	labels, err := ProcessComponentLabels(b, isvc, v1beta1.EngineComponent, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(labels).To(gomega.HaveKeyWithValue("user-label", "user-value"))
	g.Expect(labels).To(gomega.HaveKey(constants.OMEComponentLabel))
	g.Expect(isvc.Labels).To(gomega.Equal(map[string]string{"user-label": "user-value"}),
		"component label stamping must not leak into isvc.Labels")
}

func TestProcessComponentAnnotationsReservesInPlaceImageTransition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	b := &BaseComponentFields{Piece: render.Piece{RuntimeName: "test-runtime", Log: logr.Discard()}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "test-isvc", Namespace: "default",
		Annotations: map[string]string{
			constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "isvc-forged",
			"example.com/isvc": "keep",
		},
	}}
	component := map[string]string{
		constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "component-forged",
		"example.com/component": "keep",
	}

	annotations, err := ProcessComponentAnnotations(b, isvc, component)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(annotations).NotTo(gomega.HaveKey(constants.InferenceServiceInPlaceImageTransitionAnnotationKey))
	g.Expect(annotations).To(gomega.HaveKeyWithValue("example.com/isvc", "keep"))
	g.Expect(annotations).To(gomega.HaveKeyWithValue("example.com/component", "keep"))
	g.Expect(isvc.Annotations).To(gomega.HaveKeyWithValue(constants.InferenceServiceInPlaceImageTransitionAnnotationKey, "isvc-forged"))
	g.Expect(component).To(gomega.HaveKeyWithValue(constants.InferenceServiceInPlaceImageTransitionAnnotationKey, "component-forged"))
}

// TestUpdateComponentStatusLeanModel verifies that UpdateComponentStatus does
// NOT touch isvc.Status.ModelStatus when spec.model is nil — the "lean"
// runtime-only ISVC shape supported by the webhook. Prior to the fix, the
// model status writer would stamp transitionStatus=InProgress and
// modelRevisionStates.{activeModelState,targetModelState}=Pending — values
// that never advanced because no model load was in progress, lying to
// operators inspecting status.
//
// Regression matrix: spec.model present → modelStatus IS populated (pods
// gate the exact value); spec.model nil → modelStatus left as zero value.
func TestUpdateComponentStatusLeanModel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// UpdateComponentStatus resolves + writes the per-Component
	// autoscaler status, which Gets the live HPA / SO. Both types must
	// be registered in the test scheme so the live-mirror path returns
	// NotFound cleanly (which the writer treats as "no scaler yet" —
	// exercises the graceful-degradation branch).
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(kedav1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name                       string
		isvc                       *v1beta1.InferenceService
		wantModelStatusUntouched   bool
		wantTransitionStatusActive bool
	}{
		{
			name: "lean ISVC (no spec.model) — modelStatus untouched",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-test"},
					// Model is nil — lean runtime-only path.
				},
			},
			wantModelStatusUntouched: true,
		},
		{
			name: "with-model ISVC — modelStatus IS populated (Pending, no pods)",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-test"},
					Model:   &v1beta1.ModelRef{Name: "test-model"},
				},
			},
			wantTransitionStatusActive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			b := &BaseComponentFields{
				Piece:         render.Piece{DeploymentMode: constants.RawDeployment, Log: logr.Discard()},
				Client:        cl,
				StatusManager: status.NewStatusReconciler(),
			}

			err := UpdateComponentStatus(b, tt.isvc, v1beta1.EngineComponent, metav1.ObjectMeta{}, &v1beta1.ComponentExtensionSpec{})
			g.Expect(err).NotTo(gomega.HaveOccurred())

			if tt.wantModelStatusUntouched {
				// The zero-value ModelStatus must remain — no transitionStatus,
				// no modelRevisionStates. This is the "doesn't lie" contract.
				g.Expect(tt.isvc.Status.ModelStatus.TransitionStatus).To(
					gomega.BeEmpty(),
					"modelStatus.transitionStatus should NOT be written for lean (no-model) ISVCs",
				)
				g.Expect(tt.isvc.Status.ModelStatus.ModelRevisionStates).To(
					gomega.BeNil(),
					"modelStatus.modelRevisionStates should NOT be written for lean (no-model) ISVCs",
				)
				g.Expect(tt.isvc.Status.ModelStatus.ModelCopies).To(
					gomega.BeNil(),
					"modelStatus.modelCopies should NOT be written for lean (no-model) ISVCs",
				)
			}
			if tt.wantTransitionStatusActive {
				// Regression guard: with-model path still writes the lifecycle.
				// Zero pods → Pending → InProgress.
				g.Expect(tt.isvc.Status.ModelStatus.TransitionStatus).To(
					gomega.Equal(v1beta1.InProgress),
					"with-model ISVC must still drive the model-loading lifecycle",
				)
				g.Expect(tt.isvc.Status.ModelStatus.ModelRevisionStates).NotTo(gomega.BeNil())
				g.Expect(tt.isvc.Status.ModelStatus.ModelRevisionStates.TargetModelState).To(
					gomega.Equal(v1beta1.Pending),
				)
			}

			// Lock-in: the autoscaler status writer runs BEFORE the
			// lean-path return, so BOTH cases (model-less and with-model)
			// must have a populated ComponentAutoscalerStatus.
			// DeploymentMode=RawDeployment + no Autoscaler block anywhere →
			// resolved Class=hpa, SpecSource=default, ManagedBy=ome.
			engineStatus, ok := tt.isvc.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(gomega.BeTrue(),
				"every UpdateComponentStatus call must produce a Components entry")
			g.Expect(engineStatus.Autoscaler).NotTo(gomega.BeNil(),
				"ComponentAutoscalerStatus must be populated even on lean ISVCs")
			g.Expect(engineStatus.Autoscaler.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))
			g.Expect(engineStatus.Autoscaler.SpecSource).To(gomega.Equal("default"))
			g.Expect(engineStatus.Autoscaler.ManagedBy).To(gomega.Equal(v1beta1.AutoscalerManagedByOME))
			// RawDeployment scale target is the underlying Deployment.
			g.Expect(engineStatus.ScaleTargetRef).NotTo(gomega.BeNil())
			g.Expect(engineStatus.ScaleTargetRef.APIVersion).To(gomega.Equal("apps/v1"))
			g.Expect(engineStatus.ScaleTargetRef.Kind).To(gomega.Equal("Deployment"))
		})
	}
}
