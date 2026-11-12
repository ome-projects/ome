package autoscaler

import (
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// isvcAutoscalerBlock builds a distinctive ComponentAutoscaler keyed by
// the source label so tests can assert which layer produced the result.
// The block is non-trivial — non-empty Keda triggers + HPA metrics —
// so a shallow / no-op resolver returns it intact and a buggy resolver
// trips the deep-equality assertions.
func isvcAutoscalerBlock() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{
				{Type: "prometheus", Metadata: map[string]string{"source": "isvc"}},
			},
			PollingInterval: ptr.To(int32(15)),
		},
		HPA: &v1beta1.HPAAutoscaler{
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr.To(int32(73)),
						},
					},
				},
			},
		},
	}
}

// runtimeAutoscalerBlock mirrors isvcAutoscalerBlock with a distinct
// "source: runtime" trigger metadata so collisions are detectable. The
// values are intentionally different from isvcAutoscalerBlock so a
// resolver that returns the wrong layer fails the deep-equality assert.
func runtimeAutoscalerBlock() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerHPA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{
				{Type: "cron", Metadata: map[string]string{"source": "runtime"}},
			},
			CooldownPeriod: ptr.To(int32(300)),
		},
		HPA: &v1beta1.HPAAutoscaler{
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "memory",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr.To(int32(50)),
						},
					},
				},
			},
		},
	}
}

// setISVCAutoscaler stamps a Component-level Autoscaler block on the
// ISVC for the given Component. Allocates the Component spec if it
// isn't already present so tests can start from a zero-value ISVC.
func setISVCAutoscaler(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, block *v1beta1.ComponentAutoscaler) {
	switch component {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine == nil {
			isvc.Spec.Engine = &v1beta1.EngineSpec{}
		}
		isvc.Spec.Engine.ComponentExtensionSpec.Autoscaler = block
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder == nil {
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
		}
		isvc.Spec.Decoder.ComponentExtensionSpec.Autoscaler = block
	case v1beta1.RouterComponent:
		if isvc.Spec.Router == nil {
			isvc.Spec.Router = &v1beta1.RouterSpec{}
		}
		isvc.Spec.Router.ComponentExtensionSpec.Autoscaler = block
	}
}

// setRuntimeAutoscaler stamps a Component-level Autoscaler block on the
// runtime spec for the given Component. Allocates the Component config
// if it isn't already present.
func setRuntimeAutoscaler(rt *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType, block *v1beta1.ComponentAutoscaler) {
	switch component {
	case v1beta1.EngineComponent:
		if rt.EngineConfig == nil {
			rt.EngineConfig = &v1beta1.EngineSpec{}
		}
		rt.EngineConfig.ComponentExtensionSpec.Autoscaler = block
	case v1beta1.DecoderComponent:
		if rt.DecoderConfig == nil {
			rt.DecoderConfig = &v1beta1.DecoderSpec{}
		}
		rt.DecoderConfig.ComponentExtensionSpec.Autoscaler = block
	case v1beta1.RouterComponent:
		if rt.RouterConfig == nil {
			rt.RouterConfig = &v1beta1.RouterSpec{}
		}
		rt.RouterConfig.ComponentExtensionSpec.Autoscaler = block
	}
}

// TestResolveComponentAutoscaler_PriorityMatrix pins the inheritance
// chain — ISVC wins over runtime, runtime wins over the default. The
// 6 (isvc, runtime) × 3 (component) matrix exhausts every branch + every
// Component-to-spec mapping. Deep-equality assertions catch:
//   - wrong layer returned (priority bug)
//   - shallow copy returned (state-leakage bug; covered separately too)
//   - SpecSource mislabeled (operator-visible regression)
func TestResolveComponentAutoscaler_PriorityMatrix(t *testing.T) {
	components := []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	}

	cases := []struct {
		name         string
		setISVC      bool
		setRuntime   bool
		wantSource   SpecSource
		wantClass    v1beta1.AutoscalerClass
		wantBlockGen func() *v1beta1.ComponentAutoscaler // expected post-resolve block (or nil for default)
	}{
		{
			name:         "isvc-set, runtime-set: ISVC wins",
			setISVC:      true,
			setRuntime:   true,
			wantSource:   SpecSourceISVC,
			wantClass:    v1beta1.AutoscalerKEDA,
			wantBlockGen: isvcAutoscalerBlock,
		},
		{
			name:         "isvc-set, runtime-nil: ISVC wins",
			setISVC:      true,
			setRuntime:   false,
			wantSource:   SpecSourceISVC,
			wantClass:    v1beta1.AutoscalerKEDA,
			wantBlockGen: isvcAutoscalerBlock,
		},
		{
			name:         "isvc-nil, runtime-set: runtime wins",
			setISVC:      false,
			setRuntime:   true,
			wantSource:   SpecSourceRuntime,
			wantClass:    v1beta1.AutoscalerHPA,
			wantBlockGen: runtimeAutoscalerBlock,
		},
		{
			name:         "isvc-nil, runtime-nil: default {hpa, nil HPA}",
			setISVC:      false,
			setRuntime:   false,
			wantSource:   SpecSourceDefault,
			wantClass:    v1beta1.AutoscalerHPA,
			wantBlockGen: nil,
		},
	}

	for _, component := range components {
		for _, tc := range cases {
			tc := tc
			component := component
			t.Run(string(component)+"/"+tc.name, func(t *testing.T) {
				g := gomega.NewWithT(t)
				isvc := &v1beta1.InferenceService{}
				if tc.setISVC {
					setISVCAutoscaler(isvc, component, isvcAutoscalerBlock())
				}
				var rt *v1beta1.ServingRuntimeSpec
				if tc.setRuntime {
					rt = &v1beta1.ServingRuntimeSpec{}
					setRuntimeAutoscaler(rt, component, runtimeAutoscalerBlock())
				}

				got, source, _ := ResolveComponentAutoscalerWithPolicy(rt, isvc, component, nil)
				g.Expect(got).NotTo(gomega.BeNil(),
					"resolver MUST always return a non-nil ComponentAutoscaler")
				g.Expect(source).To(gomega.Equal(tc.wantSource),
					"SpecSource must match the layer that produced the block")
				g.Expect(got.Class).To(gomega.Equal(tc.wantClass),
					"Class must match the source layer's declared class")

				if tc.wantBlockGen != nil {
					g.Expect(got).To(gomega.Equal(tc.wantBlockGen()),
						"resolved block must deep-equal the source layer's input")
				} else {
					g.Expect(got.Keda).To(gomega.BeNil(),
						"default block has no Keda field")
					g.Expect(got.HPA).To(gomega.BeNil(),
						"default block has nil HPA (HPA generator materializes CPU=80%)")
				}
			})
		}
	}
}

// TestResolveComponentAutoscaler_NilISVC pins the defensive guard: a
// nil ISVC argument falls through to the runtime → default chain.
// Defensive against caller bugs at startup / unit-test scenarios.
func TestResolveComponentAutoscaler_NilISVC(t *testing.T) {
	g := gomega.NewWithT(t)

	got, source, _ := ResolveComponentAutoscalerWithPolicy(nil, nil, v1beta1.EngineComponent, nil)
	g.Expect(got).NotTo(gomega.BeNil(),
		"resolver MUST tolerate nil ISVC + nil runtime — both fall to default")
	g.Expect(source).To(gomega.Equal(SpecSourceDefault))
	g.Expect(got.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))

	// nil ISVC + runtime set → runtime wins.
	rt := &v1beta1.ServingRuntimeSpec{}
	setRuntimeAutoscaler(rt, v1beta1.EngineComponent, runtimeAutoscalerBlock())
	got, source, _ = ResolveComponentAutoscalerWithPolicy(rt, nil, v1beta1.EngineComponent, nil)
	g.Expect(source).To(gomega.Equal(SpecSourceRuntime))
	g.Expect(got).To(gomega.Equal(runtimeAutoscalerBlock()))
}

// TestResolveComponentAutoscaler_NilComponentSpec pins another defensive
// branch: an ISVC where the Component spec itself is unset (e.g., a
// router-only ISVC asked about the engine Component) must fall through
// without nil-derefing. The runtime → default chain still runs.
func TestResolveComponentAutoscaler_NilComponentSpec(t *testing.T) {
	g := gomega.NewWithT(t)

	// Router-only ISVC, asked about engine: ISVC.Spec.Engine is nil.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Router: &v1beta1.RouterSpec{},
		},
	}
	got, source, _ := ResolveComponentAutoscalerWithPolicy(nil, isvc, v1beta1.EngineComponent, nil)
	g.Expect(source).To(gomega.Equal(SpecSourceDefault),
		"missing Component spec falls through to default")
	g.Expect(got.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))

	// Same shape but runtime declares an engine Autoscaler — runtime wins.
	rt := &v1beta1.ServingRuntimeSpec{}
	setRuntimeAutoscaler(rt, v1beta1.EngineComponent, runtimeAutoscalerBlock())
	got, source, _ = ResolveComponentAutoscalerWithPolicy(rt, isvc, v1beta1.EngineComponent, nil)
	g.Expect(source).To(gomega.Equal(SpecSourceRuntime))
	g.Expect(got.Keda).NotTo(gomega.BeNil(),
		"runtime block must propagate even when the ISVC's Component spec is unset")
}

// TestResolveComponentAutoscaler_UnknownComponent pins the behavior for
// a non-engine / decoder / router Component value: fall through to the
// default branch (don't return the ISVC's engine block by accident).
func TestResolveComponentAutoscaler_UnknownComponent(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := &v1beta1.InferenceService{}
	setISVCAutoscaler(isvc, v1beta1.EngineComponent, isvcAutoscalerBlock())

	got, source, _ := ResolveComponentAutoscalerWithPolicy(nil, isvc, v1beta1.ComponentType("unknown"), nil)
	g.Expect(source).To(gomega.Equal(SpecSourceDefault),
		"unknown Component value must NOT silently inherit the engine block")
	g.Expect(got.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))
}

// TestResolveComponentAutoscaler_DeepCopySemantics pins the contract
// that the returned pointer is a deep copy of the input — mutating it
// must NOT affect the source ISVC / runtime block. Without this guard,
// a downstream caller mutating got.HPA.Metrics or got.Keda.Triggers
// would corrupt the operator's source-of-truth and break the next
// reconcile's resolution.
func TestResolveComponentAutoscaler_DeepCopySemantics(t *testing.T) {
	g := gomega.NewWithT(t)

	// ISVC-source mutation isolation.
	isvc := &v1beta1.InferenceService{}
	setISVCAutoscaler(isvc, v1beta1.EngineComponent, isvcAutoscalerBlock())
	original := isvc.Spec.Engine.ComponentExtensionSpec.Autoscaler.DeepCopy()

	got, _, _ := ResolveComponentAutoscalerWithPolicy(nil, isvc, v1beta1.EngineComponent, nil)
	got.Class = v1beta1.AutoscalerNone
	got.Keda.Triggers[0].Metadata["source"] = "mutated-by-caller"
	got.HPA.Metrics[0].Resource.Name = "mutated"

	g.Expect(isvc.Spec.Engine.ComponentExtensionSpec.Autoscaler).To(gomega.Equal(original),
		"mutating the returned pointer must NOT mutate isvc.Spec.Engine.Autoscaler")

	// Runtime-source mutation isolation.
	rt := &v1beta1.ServingRuntimeSpec{}
	setRuntimeAutoscaler(rt, v1beta1.DecoderComponent, runtimeAutoscalerBlock())
	originalRt := rt.DecoderConfig.ComponentExtensionSpec.Autoscaler.DeepCopy()

	got, _, _ = ResolveComponentAutoscalerWithPolicy(rt, &v1beta1.InferenceService{}, v1beta1.DecoderComponent, nil)
	got.Class = v1beta1.AutoscalerNone
	got.Keda.Triggers = append(got.Keda.Triggers, kedav1.ScaleTriggers{Type: "mutated"})

	g.Expect(rt.DecoderConfig.ComponentExtensionSpec.Autoscaler).To(gomega.Equal(originalRt),
		"mutating the returned pointer must NOT mutate runtime.DecoderConfig.Autoscaler")
}

// TestResolveComponentAutoscaler_DefaultBranchFreshAllocation pins
// that the default branch returns a fresh allocation on every call —
// a caller mutating one resolution's default block must NOT affect a
// subsequent resolution's default. Without this, two parallel
// Component reconciles could trip over a shared mutable default.
func TestResolveComponentAutoscaler_DefaultBranchFreshAllocation(t *testing.T) {
	g := gomega.NewWithT(t)

	first, _, _ := ResolveComponentAutoscalerWithPolicy(nil, nil, v1beta1.EngineComponent, nil)
	first.Class = v1beta1.AutoscalerNone

	second, source, _ := ResolveComponentAutoscalerWithPolicy(nil, nil, v1beta1.EngineComponent, nil)
	g.Expect(source).To(gomega.Equal(SpecSourceDefault))
	g.Expect(second.Class).To(gomega.Equal(v1beta1.AutoscalerHPA),
		"each default-branch call must produce a fresh {Class: hpa} — no shared global")
}

// TestResolveComponentAutoscaler_PerComponentIsolation pins the
// per-Component routing: setting the engine Autoscaler must NOT leak
// into the decoder / router resolution. A single ISVC with three
// different Components in different layers exercises the full mapping.
func TestResolveComponentAutoscaler_PerComponentIsolation(t *testing.T) {
	g := gomega.NewWithT(t)

	isvc := &v1beta1.InferenceService{}
	setISVCAutoscaler(isvc, v1beta1.EngineComponent, isvcAutoscalerBlock())
	rt := &v1beta1.ServingRuntimeSpec{}
	setRuntimeAutoscaler(rt, v1beta1.DecoderComponent, runtimeAutoscalerBlock())
	// Router intentionally left to fall to default.

	gotEngine, srcEngine, _ := ResolveComponentAutoscalerWithPolicy(rt, isvc, v1beta1.EngineComponent, nil)
	g.Expect(srcEngine).To(gomega.Equal(SpecSourceISVC),
		"engine has ISVC block → engine source is isvc")
	g.Expect(gotEngine.Class).To(gomega.Equal(v1beta1.AutoscalerKEDA))

	gotDecoder, srcDecoder, _ := ResolveComponentAutoscalerWithPolicy(rt, isvc, v1beta1.DecoderComponent, nil)
	g.Expect(srcDecoder).To(gomega.Equal(SpecSourceRuntime),
		"decoder has no ISVC block but runtime has one → decoder source is runtime")
	g.Expect(gotDecoder.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))

	gotRouter, srcRouter, _ := ResolveComponentAutoscalerWithPolicy(rt, isvc, v1beta1.RouterComponent, nil)
	g.Expect(srcRouter).To(gomega.Equal(SpecSourceDefault),
		"router has no block anywhere → router source is default")
	g.Expect(gotRouter.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))
	g.Expect(gotRouter.Keda).To(gomega.BeNil())
	g.Expect(gotRouter.HPA).To(gomega.BeNil())
}

// TestResolveRawComponentAutoscaler_PriorityMatrix pins the RawDeployment
// inheritance chain across every Component: typed ISVC, typed runtime, legacy
// annotation, then the existing default. The legacy annotation is an input only
// when neither typed layer supplied a block.
func TestResolveRawComponentAutoscaler_PriorityMatrix(t *testing.T) {
	components := []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	}

	tests := []struct {
		name         string
		isvcBlock    *v1beta1.ComponentAutoscaler
		runtimeBlock *v1beta1.ComponentAutoscaler
		annotations  map[string]string
		wantClass    v1beta1.AutoscalerClass
		wantSource   SpecSource
	}{
		{
			name:        "ISVC typed None beats legacy HPA",
			isvcBlock:   &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA)},
			wantClass:   v1beta1.AutoscalerNone,
			wantSource:  SpecSourceISVC,
		},
		{
			name: "runtime typed KEDA beats legacy HPA",
			runtimeBlock: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
					Type: "cron",
				}}},
			},
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA)},
			wantClass:   v1beta1.AutoscalerKEDA,
			wantSource:  SpecSourceRuntime,
		},
		{
			name:        "legacy HPA wins over default",
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassHPA)},
			wantClass:   v1beta1.AutoscalerHPA,
			wantSource:  SpecSourceLegacy,
		},
		{
			name:        "legacy KEDA wins over default",
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassKEDA)},
			wantClass:   v1beta1.AutoscalerKEDA,
			wantSource:  SpecSourceLegacy,
		},
		{
			name:        "legacy External wins over default",
			annotations: map[string]string{constants.AutoscalerClass: string(constants.AutoscalerClassExternal)},
			wantClass:   v1beta1.AutoscalerExternal,
			wantSource:  SpecSourceLegacy,
		},
		{
			name:       "no typed or legacy config preserves default HPA",
			wantClass:  v1beta1.AutoscalerHPA,
			wantSource: SpecSourceDefault,
		},
	}

	for _, component := range components {
		component := component
		for _, tt := range tests {
			tt := tt
			t.Run(string(component)+"/"+tt.name, func(t *testing.T) {
				g := gomega.NewWithT(t)
				isvc := &v1beta1.InferenceService{}
				if tt.isvcBlock != nil {
					setISVCAutoscaler(isvc, component, tt.isvcBlock.DeepCopy())
				}

				var runtimeSpec *v1beta1.ServingRuntimeSpec
				if tt.runtimeBlock != nil {
					runtimeSpec = &v1beta1.ServingRuntimeSpec{}
					setRuntimeAutoscaler(runtimeSpec, component, tt.runtimeBlock.DeepCopy())
				}

				got, source, _, err := ResolveRawComponentAutoscalerWithPolicy(runtimeSpec, isvc, component, tt.annotations, nil)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(got).NotTo(gomega.BeNil())
				g.Expect(got.Class).To(gomega.Equal(tt.wantClass))
				g.Expect(source).To(gomega.Equal(tt.wantSource))
			})
		}
	}
}

// TestResolveRawComponentAutoscaler_LegacyTargetUtilization verifies that the
// Raw-only CPU target annotation becomes an explicit typed HPA metric. The
// shared dispatcher must not need to inspect legacy annotations downstream.
func TestResolveRawComponentAutoscaler_LegacyTargetUtilization(t *testing.T) {
	g := gomega.NewWithT(t)
	annotations := map[string]string{
		constants.AutoscalerClass:             string(constants.AutoscalerClassHPA),
		constants.TargetUtilizationPercentage: "67",
	}

	got, source, _, err := ResolveRawComponentAutoscalerWithPolicy(nil, &v1beta1.InferenceService{}, v1beta1.EngineComponent, annotations, nil)

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(source).To(gomega.Equal(SpecSourceLegacy))
	g.Expect(got.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))
	g.Expect(got.HPA).NotTo(gomega.BeNil())
	g.Expect(got.HPA.Metrics).To(gomega.HaveLen(1))
	metric := got.HPA.Metrics[0]
	g.Expect(metric.Type).To(gomega.Equal(autoscalingv2.ResourceMetricSourceType))
	g.Expect(metric.Resource).NotTo(gomega.BeNil())
	g.Expect(metric.Resource.Name).To(gomega.Equal(corev1.ResourceCPU))
	g.Expect(metric.Resource.Target.Type).To(gomega.Equal(autoscalingv2.UtilizationMetricType))
	g.Expect(metric.Resource.Target.AverageUtilization).NotTo(gomega.BeNil())
	g.Expect(*metric.Resource.Target.AverageUtilization).To(gomega.Equal(int32(67)))
}

// TestResolveRawComponentAutoscaler_LegacyFreshAllocation ensures callers own
// the translated legacy block and cannot mutate a later resolution.
func TestResolveRawComponentAutoscaler_LegacyFreshAllocation(t *testing.T) {
	g := gomega.NewWithT(t)
	annotations := map[string]string{
		constants.AutoscalerClass:             string(constants.AutoscalerClassHPA),
		constants.TargetUtilizationPercentage: "67",
	}

	first, _, _, err := ResolveRawComponentAutoscalerWithPolicy(nil, &v1beta1.InferenceService{}, v1beta1.EngineComponent, annotations, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	*first.HPA.Metrics[0].Resource.Target.AverageUtilization = 10

	second, source, _, err := ResolveRawComponentAutoscalerWithPolicy(nil, &v1beta1.InferenceService{}, v1beta1.EngineComponent, annotations, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(source).To(gomega.Equal(SpecSourceLegacy))
	g.Expect(second.HPA.Metrics[0].Resource.Target.AverageUtilization).NotTo(gomega.BeNil())
	g.Expect(*second.HPA.Metrics[0].Resource.Target.AverageUtilization).To(gomega.Equal(int32(67)))
}

// TestResolveRawComponentAutoscaler_UnknownLegacyClassErrors verifies that an
// invalid effective Component annotation cannot silently fall through to the
// default HPA.
func TestResolveRawComponentAutoscaler_UnknownLegacyClassErrors(t *testing.T) {
	g := gomega.NewWithT(t)
	annotations := map[string]string{constants.AutoscalerClass: "mystery"}

	got, source, _, err := ResolveRawComponentAutoscalerWithPolicy(nil, &v1beta1.InferenceService{}, v1beta1.EngineComponent, annotations, nil)

	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring(constants.AutoscalerClass))
	g.Expect(err.Error()).To(gomega.ContainSubstring("mystery"))
	g.Expect(got).To(gomega.BeNil())
	g.Expect(source).To(gomega.Equal(SpecSourceLegacy))
}

// TestResolveRawComponentAutoscaler_TargetUtilizationWithoutClass preserves
// the legacy behavior where a CPU target annotation implicitly selects HPA.
func TestResolveRawComponentAutoscaler_TargetUtilizationWithoutClass(t *testing.T) {
	g := gomega.NewWithT(t)
	annotations := map[string]string{constants.TargetUtilizationPercentage: "61"}

	got, source, _, err := ResolveRawComponentAutoscalerWithPolicy(nil, &v1beta1.InferenceService{}, v1beta1.EngineComponent, annotations, nil)

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(source).To(gomega.Equal(SpecSourceLegacy))
	g.Expect(got.Class).To(gomega.Equal(v1beta1.AutoscalerHPA))
	g.Expect(got.HPA).NotTo(gomega.BeNil())
	g.Expect(got.HPA.Metrics).To(gomega.HaveLen(1))
	metric := got.HPA.Metrics[0]
	g.Expect(metric.Resource).NotTo(gomega.BeNil())
	g.Expect(metric.Resource.Name).To(gomega.Equal(corev1.ResourceCPU))
	g.Expect(metric.Resource.Target.AverageUtilization).NotTo(gomega.BeNil())
	g.Expect(*metric.Resource.Target.AverageUtilization).To(gomega.Equal(int32(61)))
}
