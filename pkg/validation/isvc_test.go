package validation

import (
	"errors"
	"strings"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidateInferenceServiceName(t *testing.T) {
	tests := []struct {
		name     string
		isvcName string
		wantErr  bool
	}{
		{name: "valid lowercase", isvcName: "my-model", wantErr: false},
		{name: "single char", isvcName: "a", wantErr: false},
		{name: "alphanumeric with hyphens", isvcName: "llama-3-70b", wantErr: false},
		{name: "starts with number", isvcName: "3model", wantErr: true},
		{name: "uppercase", isvcName: "MyModel", wantErr: true},
		{name: "ends with hyphen", isvcName: "model-", wantErr: true},
		{name: "empty string", isvcName: "", wantErr: true},
		{name: "underscore", isvcName: "my_model", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInferenceServiceName(tc.isvcName)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for name %q, got nil", tc.isvcName)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for name %q: %v", tc.isvcName, err)
			}
		})
	}
}

func TestValidateEngineDecoderConfig(t *testing.T) {
	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "no decoder or engine",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine without decoder",
			spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		},
		{
			name: "engine with decoder",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			name:    "decoder without engine",
			spec:    &v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEngineDecoderConfig(tc.spec)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateEngineDecoderDeploymentMode(t *testing.T) {
	withMode := func(mode string) *v1beta1.ComponentExtensionSpec {
		return &v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
		}
	}
	engineWith := func(ext *v1beta1.ComponentExtensionSpec) *v1beta1.EngineSpec {
		if ext == nil {
			return &v1beta1.EngineSpec{}
		}
		return &v1beta1.EngineSpec{ComponentExtensionSpec: *ext}
	}
	decoderWith := func(ext *v1beta1.ComponentExtensionSpec) *v1beta1.DecoderSpec {
		if ext == nil {
			return &v1beta1.DecoderSpec{}
		}
		return &v1beta1.DecoderSpec{ComponentExtensionSpec: *ext}
	}

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "no engine, no decoder",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine only (no decoder) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{Engine: engineWith(withMode(string(constants.OMENative)))},
		},
		{
			name: "both present, no annotations — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{Engine: engineWith(nil), Decoder: decoderWith(nil)},
		},
		{
			name: "both OMENative — match",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(withMode(string(constants.OMENative))),
			},
		},
		{
			name: "engine OMENative, decoder unset — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(nil),
			},
			wantErr: true,
		},
		{
			name: "engine unset, decoder OMENative — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(nil),
				Decoder: decoderWith(withMode(string(constants.OMENative))),
			},
			wantErr: true,
		},
		{
			name: "engine OMENative, decoder MultiNode — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.OMENative))),
				Decoder: decoderWith(withMode(string(constants.MultiNode))),
			},
			wantErr: true,
		},
		{
			name: "both MultiNode (no OMENative) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.MultiNode))),
				Decoder: decoderWith(withMode(string(constants.MultiNode))),
			},
		},
		{
			name: "engine MultiNode, decoder RawDeployment (no OMENative) — rule does not apply",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  engineWith(withMode(string(constants.MultiNode))),
				Decoder: decoderWith(withMode(string(constants.RawDeployment))),
			},
		},
		{
			name: "spec.deploymentMode=OMENative propagates to both — no mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				DeploymentMode: func() *constants.DeploymentModeType {
					m := constants.OMENative
					return &m
				}(),
				Engine:  engineWith(nil),
				Decoder: decoderWith(nil),
			},
		},
		{
			name: "spec.deploymentMode=OMENative; engine annotation pins Raw — mismatch",
			spec: &v1beta1.InferenceServiceSpec{
				DeploymentMode: func() *constants.DeploymentModeType {
					m := constants.OMENative
					return &m
				}(),
				Engine:  engineWith(withMode(string(constants.RawDeployment))),
				Decoder: decoderWith(nil),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEngineDecoderDeploymentMode(tc.spec)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateLeaderWorkerPairing(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	withWorker := func(size *int) *v1beta1.WorkerSpec { return &v1beta1.WorkerSpec{Size: size} }

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
		errSub  string
	}{
		{
			name: "no engine, no decoder",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine: neither leader nor worker — single-pod OK",
			spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		},
		{
			name: "engine: leader + worker(size=3) — valid multi-pod",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(sizePtr(3)),
				},
			},
		},
		{
			name: "engine: leader without worker — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantErr: true,
			errSub:  "engine.leader is set but engine.worker is not",
		},
		{
			name: "engine: worker without leader — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Worker: withWorker(sizePtr(3))},
			},
			wantErr: true,
			errSub:  "engine.worker is set but engine.leader is not",
		},
		{
			name: "engine: leader + worker with unset size — valid, size resolves at reconcile",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(nil),
				},
			},
		},
		{
			name: "engine: worker with size=0 — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: withWorker(sizePtr(0)),
				},
			},
			wantErr: true,
			errSub:  "worker.size must be a positive integer when set",
		},
		{
			name: "decoder: leader without worker — rejected",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantErr: true,
			errSub:  "decoder.leader is set but decoder.worker is not",
		},
		{
			name: "engine OK, decoder OK — both single-pod, both pass",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			name: "engine fails first — engine error surfaces (decoder not checked)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
				Decoder: &v1beta1.DecoderSpec{Worker: withWorker(sizePtr(2))},
			},
			wantErr: true,
			errSub:  "engine.leader",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderWorkerPairing(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.errSub != "" && !strings.Contains(err.Error(), tc.errSub) {
					t.Fatalf("expected error to contain %q, got %v", tc.errSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateReplicaBounds(t *testing.T) {
	intPtr := func(v int) *int { return &v }

	tests := []struct {
		name    string
		min     *int
		max     *int
		wantErr bool
	}{
		{name: "nil both", min: nil, max: nil},
		{name: "valid range", min: intPtr(1), max: intPtr(3)},
		{name: "min equals max", min: intPtr(2), max: intPtr(2)},
		{name: "min zero", min: intPtr(0), max: intPtr(1)},
		{name: "negative min", min: intPtr(-1), max: intPtr(1), wantErr: true},
		{name: "zero max", min: intPtr(0), max: intPtr(0), wantErr: true},
		{name: "min greater than max", min: intPtr(5), max: intPtr(2), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateReplicaBounds(tc.min, tc.max)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateAutoscalerConfig(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{
			name:        "no autoscaler annotation",
			annotations: map[string]string{},
		},
		{
			name: "valid HPA class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
			},
		},
		{
			name: "valid HPA with cpu metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "cpu",
			},
		},
		{
			name: "HPA with invalid metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "bogus",
			},
			wantErr: true,
		},
		{
			name: "unsupported autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: "unsupported",
			},
			wantErr: true,
		},
		{
			name: "KEDA class accepted (no validation in phase 1)",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
		},
		{
			name: "valid external class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassExternal),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			_, err := ValidateAutoscalerConfig(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateLeaderWorkerPairing_ReasonConstants confirms the
// Multi-node validation reason constants appear in the error strings the
// validator emits. Operators and the webhook test suites grep for
// these exact tokens.
func TestValidateLeaderWorkerPairing_ReasonConstants(t *testing.T) {
	sizePtr := func(v int) *int { return &v }

	tests := []struct {
		name           string
		spec           *v1beta1.InferenceServiceSpec
		wantReasons    []string
		notWantReasons []string
	}{
		{
			name: "leader without worker — LeaderRequiresWorker",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonLeaderRequiresWorker},
		},
		{
			name: "worker without leader — WorkerRequiresLeader",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)}},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonWorkerRequiresLeader},
		},
		{
			name: "leader + worker(size=0) — WorkerSizeMustBePositive",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(0)},
				},
			},
			wantReasons: []string{ReasonInvalidLeaderWorkerPairing, ReasonWorkerSizeMustBePositive},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderWorkerPairing(tc.spec)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, want := range tc.wantReasons {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("expected error to contain %q, got %v", want, err)
				}
			}
			for _, notWant := range tc.notWantReasons {
				if strings.Contains(err.Error(), notWant) {
					t.Errorf("expected error to NOT contain %q, got %v", notWant, err)
				}
			}
		})
	}
}

// TestValidateLeaderSize is structurally a no-op against the current
// API surface (LeaderSpec has no Size field; see ValidateLeaderSize
// doc). The test pins the reason constant + entry point so any
// future API evolution that adds a leader Size field surfaces a test
// failure rather than silently passing.
func TestValidateLeaderSize(t *testing.T) {
	sizePtr := func(v int) *int { return &v }

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{name: "nil spec", spec: nil},
		{name: "empty spec", spec: &v1beta1.InferenceServiceSpec{}},
		{name: "single-pod engine", spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}},
		{
			name: "multi-pod engine — leader implicitly size=1 by type",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(3)},
				},
			},
		},
		{
			name: "multi-pod decoder — leader implicitly size=1 by type",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLeaderSize(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), ReasonLeaderSizeMustBeOne) {
					t.Errorf("expected reason %q, got %v", ReasonLeaderSizeMustBeOne, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateMultiPodLeanRunner covers the rule
// that a lean (no spec.model, no spec.runtime) multi-pod ISVC must
// declare both leader.runner.image and worker.runner.image. The
// controller has no defaulting source for those images.
func TestValidateMultiPodLeanRunner(t *testing.T) {
	sizePtr := func(v int) *int { return &v }
	withRunner := func(img string) *v1beta1.RunnerSpec {
		if img == "" {
			return &v1beta1.RunnerSpec{}
		}
		return &v1beta1.RunnerSpec{Container: corev1.Container{Image: img}}
	}
	modelRef := &v1beta1.ModelRef{Name: "llama"}
	runtimeRef := &v1beta1.ServingRuntimeRef{Name: "vllm"}

	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{name: "nil spec", spec: nil},
		{
			name: "model set, no leader/worker images — allowed (controller resolves)",
			spec: &v1beta1.InferenceServiceSpec{
				Model: modelRef,
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
		{
			name: "runtime set, no leader/worker images — allowed (runtime resolves)",
			spec: &v1beta1.InferenceServiceSpec{
				Runtime: runtimeRef,
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
		},
		{
			name: "lean single-pod engine — rule does not fire (no multi-pod)",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
			},
		},
		{
			name: "lean multi-pod engine, both images set — allowed",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("leader:1")},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner("worker:1"),
					},
				},
			},
		},
		{
			name: "lean multi-pod engine, leader image missing — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner("worker:1"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod engine, worker image missing — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("leader:1")},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod engine, both runner specs but empty images — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Leader: &v1beta1.LeaderSpec{Runner: withRunner("")},
					Worker: &v1beta1.WorkerSpec{
						Size:   sizePtr(2),
						Runner: withRunner(""),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "lean multi-pod decoder, missing images — REJECT",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{
					Leader: &v1beta1.LeaderSpec{},
					Worker: &v1beta1.WorkerSpec{Size: sizePtr(2)},
				},
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMultiPodLeanRunner(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), ReasonMultiPodLeanModelNeedsRunner) {
					t.Errorf("expected reason %q in error, got %v", ReasonMultiPodLeanModelNeedsRunner, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateComponentAutoscaler is the table-driven coverage for
// Webhook validation of the per-Component Autoscaler block. Two
// invariants the cases pin:
//
//   - `none` and `external` are accepted with no shape requirements —
//     both mean the controller does not manage scaling itself.
//   - No KEDA-installed check — KEDA is always required for class=KEDA,
//     and the webhook does not probe the cluster.
func TestValidateComponentAutoscaler(t *testing.T) {
	cpu80 := resource.MustParse("80")
	validHPAMetrics := []autoscalingv2.MetricSpec{{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name: corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{
				Type:         autoscalingv2.UtilizationMetricType,
				AverageValue: &cpu80,
			},
		},
	}}
	validKedaTriggers := []kedav1.ScaleTriggers{{
		Type: "prometheus",
		Metadata: map[string]string{
			"serverAddress": "http://prom:9090",
			"query":         "rate(sglang:num_requests_waiting[1m])",
			"threshold":     "20",
		},
	}}

	tests := []struct {
		name            string
		ext             *v1beta1.ComponentExtensionSpec
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "valid hpa with nil HPA spec — defaults to CPU=80% downstream",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			},
		},
		{
			name: "valid hpa with full Metrics",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA:   &v1beta1.HPAAutoscaler{Metrics: validHPAMetrics},
				},
			},
		},
		{
			name: "valid keda with 1 trigger",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
					Keda:  &v1beta1.KedaAutoscaler{Triggers: validKedaTriggers},
				},
			},
		},
		{
			name: "valid external (Q5 status-field twin of none)",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			},
		},
		{
			name: "valid none (Q5 status-field twin of external)",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			},
		},
		{
			name: "reject keda with empty Triggers",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
					Keda:  &v1beta1.KedaAutoscaler{Triggers: nil},
				},
			},
			wantErr:         true,
			wantErrContains: "class=keda requires at least 1 trigger",
		},
		{
			name: "reject keda with nil Keda block",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
				},
			},
			wantErr:         true,
			wantErrContains: ReasonKedaTriggersRequired,
		},
		{
			name: "reject hpa Type=Resource with Resource=nil",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
						Type:     autoscalingv2.ResourceMetricSourceType,
						Resource: nil,
					}}},
				},
			},
			wantErr:         true,
			wantErrContains: "type=Resource but",
		},
		{
			name: "reject hpa Type=Pods with Pods=nil",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
						Type: autoscalingv2.PodsMetricSourceType,
					}}},
				},
			},
			wantErr:         true,
			wantErrContains: "type=Pods but",
		},
		{
			name: "reject hpa empty Type",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA:   &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{Type: ""}}},
				},
			},
			wantErr:         true,
			wantErrContains: "is empty",
		},
		{
			name: "reject hpa Type=External with External=nil",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
						Type: autoscalingv2.ExternalMetricSourceType,
					}}},
				},
			},
			wantErr:         true,
			wantErrContains: "type=External but",
		},
		{
			name: "reject keda IdleReplicaCount >= MinReplicaCount",
			ext: &v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To[int](2),
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
					Keda: &v1beta1.KedaAutoscaler{
						Triggers:         validKedaTriggers,
						IdleReplicaCount: ptr.To[int32](2),
					},
				},
			},
			wantErr:         true,
			wantErrContains: "keda.idleReplicaCount must be < minReplicas",
		},
		{
			name: "accept keda IdleReplicaCount=0 with MinReplicaCount=1",
			ext: &v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To[int](1),
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerKEDA,
					Keda: &v1beta1.KedaAutoscaler{
						Triggers:         validKedaTriggers,
						IdleReplicaCount: ptr.To[int32](0),
					},
				},
			},
		},
		{
			name: "reject class value not in enum (defense-in-depth)",
			ext: &v1beta1.ComponentExtensionSpec{
				Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerClass("knative")},
			},
			wantErr:         true,
			wantErrContains: "is not one of HPA|KEDA|External|None",
		},
		{
			name: "nil componentExt — no error",
			ext:  nil,
		},
		{
			name: "nil Autoscaler block — no error",
			ext: &v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To[int](1),
				MaxReplicas: 3,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateComponentAutoscaler(tc.ext)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr && tc.wantErrContains != "" {
				if got := err.Error(); !strings.Contains(got, tc.wantErrContains) {
					t.Fatalf("error string\n\tgot:  %q\n\twant substring: %q", got, tc.wantErrContains)
				}
			}
		})
	}
}

// TestValidateAutoscaler covers the (autoscaler, minReplicas) core
// signature that lets callers without a parent ComponentExtensionSpec
// (notably the InferenceReplica webhook) validate without synthesizing
// one to fit the ValidateComponentAutoscaler shape.
//
// The behaviour parity with ValidateComponentAutoscaler is covered by
// TestValidateComponentAutoscaler above — this test asserts the cases
// that are only reachable through the direct signature.
func TestValidateAutoscaler(t *testing.T) {
	validKedaTriggers := []kedav1.ScaleTriggers{{
		Type:     "prometheus",
		Metadata: map[string]string{"serverAddress": "x", "query": "y", "threshold": "1"},
	}}

	tests := []struct {
		name            string
		autoscaler      *v1beta1.ComponentAutoscaler
		minReplicas     *int
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "nil autoscaler is a no-op even with non-nil minReplicas",
			// IR webhook: spec.autoscaler may be nil while spec.replicas is set;
			// must return nil rather than panic on nil-deref.
			minReplicas: ptr.To[int](3),
		},
		{
			name: "keda IdleReplicaCount >= minReplicas — REJECT (IR-style call)",
			// Equivalent to the IR webhook passing spec.replicas as the floor.
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers:         validKedaTriggers,
					IdleReplicaCount: ptr.To[int32](2),
				},
			},
			minReplicas:     ptr.To[int](2),
			wantErr:         true,
			wantErrContains: "keda.idleReplicaCount must be < minReplicas",
		},
		{
			name: "keda IdleReplicaCount with nil minReplicas — skip check",
			// IR webhook with spec.replicas nil: skip the idle-vs-min check
			// rather than reject (controller will pick the default later).
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda: &v1beta1.KedaAutoscaler{
					Triggers:         validKedaTriggers,
					IdleReplicaCount: ptr.To[int32](5),
				},
			},
			minReplicas: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAutoscaler(tc.autoscaler, tc.minReplicas)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr && tc.wantErrContains != "" {
				if got := err.Error(); !strings.Contains(got, tc.wantErrContains) {
					t.Fatalf("error string\n\tgot:  %q\n\twant substring: %q", got, tc.wantErrContains)
				}
			}
		})
	}
}

// TestValidateComponentsAutoscalers covers the slice-driven dispatch
// shared by the ISVC, ServingRuntime, and IR webhooks. Asserts that
// (a) the first failure short-circuits, (b) the per-Component Name is
// prepended via %w (so callers can unwrap to ReasonXxx), (c) nil/empty
// slice + nil Autoscaler entries are no-ops, and (d) an empty Name
// returns the inner error unwrapped (so the IR site with a single check
// reads naturally).
func TestValidateComponentsAutoscalers(t *testing.T) {
	badKEDA := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
	validHPA := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}

	t.Run("nil slice is a no-op", func(t *testing.T) {
		if err := ValidateComponentsAutoscalers(nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty slice is a no-op", func(t *testing.T) {
		if err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("all entries with nil Autoscaler — no-op", func(t *testing.T) {
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Name: "engine"},
			{Name: "decoder"},
			{Name: "router"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("all valid — accept", func(t *testing.T) {
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Name: "engine", Autoscaler: validHPA},
			{Name: "decoder", Autoscaler: validHPA},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("first failure short-circuits with Name prefix", func(t *testing.T) {
		// engine is valid, decoder is invalid, router would also be
		// invalid — assert the error names "decoder" (not "router").
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Name: "engine", Autoscaler: validHPA},
			{Name: "decoder", Autoscaler: badKEDA},
			{Name: "router", Autoscaler: badKEDA},
		})
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "decoder:") {
			t.Fatalf("expected 'decoder:' prefix, got %q", err.Error())
		}
		if strings.Contains(err.Error(), "router:") {
			t.Fatalf("expected router not reached, got %q", err.Error())
		}
	})

	t.Run("unwrap returns the inner Reason error", func(t *testing.T) {
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Name: "engine", Autoscaler: badKEDA},
		})
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		// %w wrapping must preserve the Reason prefix on Unwrap.
		inner := errors.Unwrap(err)
		if inner == nil {
			t.Fatalf("expected unwrappable error, got plain %q", err.Error())
		}
		if !strings.Contains(inner.Error(), ReasonKedaTriggersRequired) {
			t.Fatalf("unwrapped error missing reason: %q", inner.Error())
		}
	})

	t.Run("empty Name returns inner error unwrapped", func(t *testing.T) {
		// Single-Component callers (IR webhook style) pass an empty Name
		// so the message reads "<reason>: ..." not ": <reason>: ...".
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Autoscaler: badKEDA},
		})
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if strings.HasPrefix(err.Error(), ":") {
			t.Fatalf("empty Name produced bare-colon prefix: %q", err.Error())
		}
	})

	t.Run("MinReplicas is per-entry, not shared", func(t *testing.T) {
		// engine has minReplicas=2 + idleReplica=2 → REJECT.
		// decoder has minReplicas=3 + idleReplica=2 → accept (but engine
		// short-circuits first).
		bad := &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaAutoscaler{
				Triggers: []kedav1.ScaleTriggers{{
					Type:     "prometheus",
					Metadata: map[string]string{"serverAddress": "x", "query": "y", "threshold": "1"},
				}},
				IdleReplicaCount: ptr.To[int32](2),
			},
		}
		err := ValidateComponentsAutoscalers([]ComponentAutoscalerCheck{
			{Name: "engine", Autoscaler: bad, MinReplicas: ptr.To[int](2)},
			{Name: "decoder", Autoscaler: bad, MinReplicas: ptr.To[int](3)},
		})
		if err == nil || !strings.Contains(err.Error(), "engine:") {
			t.Fatalf("expected engine reject, got %v", err)
		}
	})
}

// TestValidateAutoscalerAnnotationConflict covers the rule
// that the legacy ome.io/autoscalerClass annotation cannot coexist
// with a per-Component Autoscaler block; the validator preempts the
// confused "two sources of truth" state by forcing operators to pick
// one.
func TestValidateAutoscalerAnnotationConflict(t *testing.T) {
	hpaBlock := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "no annotation, no block",
			isvc: &v1beta1.InferenceService{},
		},
		{
			name: "annotation alone (legacy path)",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
					},
				},
			},
		},
		{
			name: "Autoscaler block on engine, no annotation (new path)",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: hpaBlock},
					},
				},
			},
		},
		{
			name: "annotation + engine.autoscaler block — REJECT",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: hpaBlock},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "annotation + decoder.autoscaler block — REJECT",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: hpaBlock},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "annotation + router.autoscaler block — REJECT",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassExternal),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: hpaBlock},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAutoscalerAnnotationConflict(tc.isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), ReasonAutoscalerAnnotationConflict) {
					t.Fatalf("error %q did not contain reason %q", err, ReasonAutoscalerAnnotationConflict)
				}
			}
		})
	}
}

func TestValidateAutoscalerTargetUtilizationPercentage(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{name: "no annotation", annotations: map[string]string{}},
		{name: "valid 50", annotations: map[string]string{constants.TargetUtilizationPercentage: "50"}},
		{name: "valid 1", annotations: map[string]string{constants.TargetUtilizationPercentage: "1"}},
		{name: "valid 100", annotations: map[string]string{constants.TargetUtilizationPercentage: "100"}},
		{name: "zero", annotations: map[string]string{constants.TargetUtilizationPercentage: "0"}, wantErr: true},
		{name: "101", annotations: map[string]string{constants.TargetUtilizationPercentage: "101"}, wantErr: true},
		{name: "not a number", annotations: map[string]string{constants.TargetUtilizationPercentage: "abc"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			err := ValidateAutoscalerTargetUtilizationPercentage(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestValidateLegacyAutoscalerFieldsRaw exercises the
// defense-in-depth check that rejects requests carrying the deleted
// scaleTarget / scaleMetric fields with a friendly migration message.
// Most paths NEVER trigger in practice (CRD schema pruning drops the
// unknown fields server-side), so the test is the canonical operator-
// facing record of what error a stale client would see.
func TestValidateLegacyAutoscalerFieldsRaw(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantField string // "" means no error
	}{
		{name: "empty raw", raw: ""},
		{name: "no spec at all", raw: `{}`},
		{name: "spec without components", raw: `{"spec":{"model":{"name":"m"}}}`},
		{
			name: "engine without legacy fields",
			raw:  `{"spec":{"engine":{"minReplicas":1,"maxReplicas":5}}}`,
		},
		{
			name: "engine.autoscaler block alone is fine",
			raw:  `{"spec":{"engine":{"autoscaler":{"class":"HPA"}}}}`,
		},
		{
			name:      "engine.scaleTarget=5 rejected",
			raw:       `{"spec":{"engine":{"scaleTarget":5}}}`,
			wantField: "engine.scaleTarget",
		},
		{
			name:      "engine.scaleMetric=cpu rejected",
			raw:       `{"spec":{"engine":{"scaleMetric":"cpu"}}}`,
			wantField: "engine.scaleMetric",
		},
		{
			name:      "decoder.scaleTarget rejected",
			raw:       `{"spec":{"decoder":{"scaleTarget":80}}}`,
			wantField: "decoder.scaleTarget",
		},
		{
			name:      "router.scaleMetric rejected",
			raw:       `{"spec":{"router":{"scaleMetric":"memory"}}}`,
			wantField: "router.scaleMetric",
		},
		{
			name:      "engineConfig.scaleTarget rejected (PD-disaggregated shape)",
			raw:       `{"spec":{"engineConfig":{"scaleTarget":50}}}`,
			wantField: "engineConfig.scaleTarget",
		},
		{
			name: "explicit null scaleTarget is OK (operator's explicit unset)",
			raw:  `{"spec":{"engine":{"scaleTarget":null}}}`,
		},
		{
			name: "malformed JSON is silently passed (primary decoder reports)",
			raw:  `{"spec":{`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLegacyAutoscalerFieldsRaw([]byte(tc.raw))
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error mentioning %q; got nil", tc.wantField)
			}
			if !strings.Contains(err.Error(), ReasonLegacyAutoscalerFieldsRejected) {
				t.Errorf("error %q missing reason constant %q", err, ReasonLegacyAutoscalerFieldsRejected)
			}
			if !strings.Contains(err.Error(), tc.wantField) {
				t.Errorf("error %q missing field name %q", err, tc.wantField)
			}
			if !strings.Contains(err.Error(), "spec.") {
				t.Errorf("error %q missing spec.* prefix for operator legibility", err)
			}
			if !strings.Contains(err.Error(), "autoscaler") {
				t.Errorf("error %q missing migration pointer to autoscaler block", err)
			}
		})
	}
}

// TestValidateScaleToZero covers the MinReplicas=0 KEDA gate. The two
// accept-paths are (a) the legacy whole-ISVC ome.io/autoscalerClass=keda
// annotation and (b) the typed per-Component
// spec.<component>.autoscaler.class=KEDA block. The typed path is
// evaluated PER Component: a zero-replica Component must itself be
// KEDA-autoscaled — a KEDA block on a *different* Component does not
// excuse it. Note the deliberate case mismatch: typed enum "KEDA" vs
// legacy annotation value "keda".
func TestValidateScaleToZero(t *testing.T) {
	kedaAutoscaler := func() *v1beta1.ComponentAutoscaler {
		return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}
	}
	hpaAutoscaler := func() *v1beta1.ComponentAutoscaler {
		return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	}

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
	}{
		{
			name: "no component sets MinReplicas=0 — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)},
					},
				},
			},
		},
		{
			name: "nil MinReplicas everywhere — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
					Router:  &v1beta1.RouterSpec{},
				},
			},
		},
		{
			name: "engine MinReplicas=0 with typed KEDA autoscaler — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
				},
			},
		},
		{
			name: "decoder MinReplicas=0 with typed KEDA autoscaler — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
				},
			},
		},
		{
			name: "router MinReplicas=0 with typed KEDA autoscaler — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Router: &v1beta1.RouterSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
				},
			},
		},
		{
			name: "engine MinReplicas=0 with NO KEDA (no annotation, no typed) — rejected",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "engine MinReplicas=0 with typed HPA autoscaler (not KEDA) — rejected",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  hpaAutoscaler(),
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "engine MinReplicas=0 with legacy keda annotation — accepted",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
				},
			},
		},
		{
			name: "legacy keda annotation covers every zero Component — accepted",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
				},
			},
		},
		{
			name: "legacy hpa annotation does not satisfy the KEDA gate — rejected",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "per-Component scoping: engine=0 typed-KEDA but decoder=0 without KEDA — rejected",
			// A typed KEDA block on the engine must NOT excuse a separate
			// zero-replica decoder that has no KEDA opt-in of its own.
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "per-Component scoping: both zero Components carry typed KEDA — accepted",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: ptr.To(0),
							Autoscaler:  kedaAutoscaler(),
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateScaleToZero(tc.isvc)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "InvalidScaleToZero") {
					t.Errorf("error %q missing InvalidScaleToZero reason", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
