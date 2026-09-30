package validation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	autoscalingv2 "k8s.io/api/autoscaling/v2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/constants"
)

// validKedaPolicySpec is a well-formed KEDA policy: a desired-count
// prometheus trigger with explicit ignoreNullValues and a provider ref.
func validKedaPolicySpec() *v1beta1.AutoscalerPolicySpec {
	return &v1beta1.AutoscalerPolicySpec{
		Enforcement: v1beta1.PolicyEnforcementDefault,
		Class:       v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaPolicyTemplate{
			Triggers: []v1beta1.KedaTriggerTemplate{{
				Type:                        "prometheus",
				ProviderRef:                 &v1beta1.MetricProviderRef{Name: "cluster-prometheus"},
				MetricType:                  autoscalingv2.AverageValueMetricType,
				QueryReturnsDesiredReplicas: true,
				Metadata: map[string]string{
					"threshold":        "1",
					"ignoreNullValues": "false",
					"query":            `((sum(request_activity{namespace="{{ .Namespace }}",inferenceservice="{{ .ISVCName }}"}) > bool 0) * {{ .MaxReplicas }})`,
				},
			}},
		},
	}
}

func TestValidateAutoscalerPolicySpec(t *testing.T) {
	t.Run("nil spec is valid", func(t *testing.T) {
		if err := ValidateAutoscalerPolicySpec(nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid KEDA spec", func(t *testing.T) {
		if err := ValidateAutoscalerPolicySpec(validKedaPolicySpec()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid HPA spec without template", func(t *testing.T) {
		if err := ValidateAutoscalerPolicySpec(&v1beta1.AutoscalerPolicySpec{
			Enforcement: v1beta1.PolicyEnforcementDefault,
			Class:       v1beta1.AutoscalerHPA,
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("class KEDA without template is rejected", func(t *testing.T) {
		err := ValidateAutoscalerPolicySpec(&v1beta1.AutoscalerPolicySpec{
			Enforcement: v1beta1.PolicyEnforcementDefault,
			Class:       v1beta1.AutoscalerKEDA,
		})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), render.ReasonClassTemplate) {
			t.Errorf("error %q does not carry reason %s", err, render.ReasonClassTemplate)
		}
	})

	t.Run("all issues are joined into one error", func(t *testing.T) {
		err := ValidateAutoscalerPolicySpec(&v1beta1.AutoscalerPolicySpec{
			Enforcement: v1beta1.PolicyEnforcementRequired,
			Class:       v1beta1.AutoscalerKEDA,
		})
		if err == nil {
			t.Fatal("expected error")
		}
		for _, want := range []string{render.ReasonEnforcementReserved, render.ReasonClassTemplate} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not carry reason %s", err, want)
			}
		}
	})

	t.Run("sample-render failure is rejected", func(t *testing.T) {
		spec := validKedaPolicySpec()
		spec.Keda.Triggers[0].Metadata["query"] = `sum(x{namespace="{{ .NoSuchVariable }}"})`
		err := ValidateAutoscalerPolicySpec(spec)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func isvcWithPolicyRefs(engine, decoder, router *v1beta1.AutoscalerPolicyRef) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{}
	if engine != nil {
		isvc.Spec.Engine = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: engine},
		}
	}
	if decoder != nil {
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: decoder},
		}
	}
	if router != nil {
		isvc.Spec.Router = &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: router},
		}
	}
	return isvc
}

func TestValidateAutoscalerPolicyRefs(t *testing.T) {
	namespacedRef := &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1", Kind: constants.AutoscalerPolicyKind}
	defaultedRef := &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"}
	reservedRef := &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1", Kind: "ClusterAutoscalerPolicy"}

	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		featureEnabled bool
		wantErr        bool
		wantReason     string
	}{
		{
			name:           "nil isvc",
			isvc:           nil,
			featureEnabled: false,
		},
		{
			name:           "no refs with feature disabled",
			isvc:           &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}},
			featureEnabled: false,
		},
		{
			name:           "explicit kind with feature enabled",
			isvc:           isvcWithPolicyRefs(namespacedRef, nil, nil),
			featureEnabled: true,
		},
		{
			name:           "defaulted kind with feature enabled",
			isvc:           isvcWithPolicyRefs(defaultedRef, defaultedRef, defaultedRef),
			featureEnabled: true,
		},
		{
			name:           "engine ref with feature disabled",
			isvc:           isvcWithPolicyRefs(defaultedRef, nil, nil),
			featureEnabled: false,
			wantErr:        true,
			wantReason:     ReasonAutoscalerPolicyFeatureDisabled,
		},
		{
			name:           "router ref with feature disabled",
			isvc:           isvcWithPolicyRefs(nil, nil, defaultedRef),
			featureEnabled: false,
			wantErr:        true,
			wantReason:     ReasonAutoscalerPolicyFeatureDisabled,
		},
		{
			name:           "reserved cluster kind on engine",
			isvc:           isvcWithPolicyRefs(reservedRef, nil, nil),
			featureEnabled: true,
			wantErr:        true,
			wantReason:     ReasonAutoscalerPolicyRefKindReserved,
		},
		{
			name:           "reserved cluster kind on decoder",
			isvc:           isvcWithPolicyRefs(nil, reservedRef, nil),
			featureEnabled: true,
			wantErr:        true,
			wantReason:     ReasonAutoscalerPolicyRefKindReserved,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAutoscalerPolicyRefs(tt.isvc, tt.featureEnabled)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantReason) {
					t.Errorf("error %q does not carry reason %s", err, tt.wantReason)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestAutoscalerPolicySplitCeilingWarning(t *testing.T) {
	ref := &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"}
	withPlacement := func(isvc *v1beta1.InferenceService, placement *v1beta1.PlacementSpec) *v1beta1.InferenceService {
		isvc.Spec.Placement = placement
		return isvc
	}

	t.Run("nil isvc", func(t *testing.T) {
		if got := AutoscalerPolicySplitCeilingWarning(nil); got != "" {
			t.Errorf("unexpected warning: %q", got)
		}
	})

	t.Run("no placement", func(t *testing.T) {
		if got := AutoscalerPolicySplitCeilingWarning(isvcWithPolicyRefs(ref, nil, nil)); got != "" {
			t.Errorf("unexpected warning: %q", got)
		}
	})

	t.Run("single mode with ref", func(t *testing.T) {
		isvc := withPlacement(isvcWithPolicyRefs(ref, nil, nil), &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle})
		if got := AutoscalerPolicySplitCeilingWarning(isvc); got != "" {
			t.Errorf("unexpected warning: %q", got)
		}
	})

	t.Run("split with ref and nil split block warns", func(t *testing.T) {
		isvc := withPlacement(isvcWithPolicyRefs(ref, nil, nil), &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit})
		got := AutoscalerPolicySplitCeilingWarning(isvc)
		if got == "" {
			t.Fatal("expected warning")
		}
		for _, want := range []string{"engine", "maxReplicasPerCluster", v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling} {
			if !strings.Contains(got, want) {
				t.Errorf("warning %q does not mention %s", got, want)
			}
		}
	})

	t.Run("split with ref and zero cap warns", func(t *testing.T) {
		isvc := withPlacement(isvcWithPolicyRefs(nil, ref, nil), &v1beta1.PlacementSpec{
			Mode:  v1beta1.PlacementModeSplit,
			Split: &v1beta1.SplitSpec{MaxReplicasPerCluster: 0},
		})
		got := AutoscalerPolicySplitCeilingWarning(isvc)
		if got == "" {
			t.Fatal("expected warning")
		}
		if !strings.Contains(got, "decoder") {
			t.Errorf("warning %q does not name the referencing component", got)
		}
	})

	t.Run("split with ref and positive cap is quiet", func(t *testing.T) {
		isvc := withPlacement(isvcWithPolicyRefs(ref, nil, nil), &v1beta1.PlacementSpec{
			Mode:  v1beta1.PlacementModeSplit,
			Split: &v1beta1.SplitSpec{MaxReplicasPerCluster: 3},
		})
		if got := AutoscalerPolicySplitCeilingWarning(isvc); got != "" {
			t.Errorf("unexpected warning: %q", got)
		}
	})

	t.Run("split without refs is quiet", func(t *testing.T) {
		isvc := withPlacement(&v1beta1.InferenceService{
			Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		}, &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit})
		if got := AutoscalerPolicySplitCeilingWarning(isvc); got != "" {
			t.Errorf("unexpected warning: %q", got)
		}
	})

	t.Run("warning names every referencing component", func(t *testing.T) {
		isvc := withPlacement(isvcWithPolicyRefs(ref, ref, ref), &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit})
		got := AutoscalerPolicySplitCeilingWarning(isvc)
		for _, want := range []string{"engine", "decoder", "router"} {
			if !strings.Contains(got, want) {
				t.Errorf("warning %q does not mention %s", got, want)
			}
		}
	})
}

func TestAutoscalerCeilingWarningSplitModes(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			isvc := isvcWithPolicyRefs(&v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"}, nil, nil)
			isvc.Spec.Placement = &v1beta1.PlacementSpec{Mode: mode}
			want := fmt.Sprintf("spec.placement.mode=%s with autoscalerPolicyRef on engine but spec.placement.split.maxReplicasPerCluster is unset; policy templates render against each home's derived bounds, and without a per-cluster cap that ceiling is unbounded (%s)", mode, v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling)
			if diff := cmp.Diff(want, AutoscalerPolicySplitCeilingWarning(isvc)); diff != "" {
				t.Error(diff)
			}
		})
	}
}
