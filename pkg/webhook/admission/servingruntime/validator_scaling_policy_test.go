package servingruntime

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

func runtimeProportionalPolicy(decoderRatio string) *v1beta1.ScalingPolicy {
	return &v1beta1.ScalingPolicy{
		Mode: v1beta1.ScalingProportional,
		Proportional: &v1beta1.ProportionalPolicy{
			Anchor: v1beta1.EngineComponent,
			Ratios: map[v1beta1.ComponentType]resource.Quantity{
				v1beta1.DecoderComponent: resource.MustParse(decoderRatio),
			},
		},
	}
}

func TestServingRuntime_ScalingPolicyCreate(t *testing.T) {
	tests := []struct {
		name        string
		policy      *v1beta1.ScalingPolicy
		wantAllowed bool
	}{
		{name: "nil policy allowed", policy: nil, wantAllowed: true},
		{name: "Independent allowed", policy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, wantAllowed: true},
		{name: "Proportional allowed", policy: runtimeProportionalPolicy("1"), wantAllowed: true},
		{name: "Pinned denied", policy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}},
	}
	for _, tt := range tests {
		t.Run("SR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			resp := newServingRuntimeValidator(t).Handle(context.Background(),
				createReq(t, servingRuntime("rt-sp", "default", withScalingPolicy(tt.policy))))
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(validation.ReasonScalingModeNotImplemented))
			}
		})
		t.Run("CSR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			resp := newClusterServingRuntimeValidator(t).Handle(context.Background(),
				createReq(t, clusterServingRuntime("crt-sp", withScalingPolicy(tt.policy))))
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(validation.ReasonScalingModeNotImplemented))
			}
		})
	}
}

func TestServingRuntime_ScalingPolicyUpdate(t *testing.T) {
	tests := []struct {
		name        string
		oldPolicy   *v1beta1.ScalingPolicy
		newPolicy   *v1beta1.ScalingPolicy
		wantAllowed bool
	}{
		{name: "nil to nil allowed", wantAllowed: true},
		{name: "nil to Independent allowed", newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, wantAllowed: true},
		{name: "newly set Proportional allowed", newPolicy: runtimeProportionalPolicy("1"), wantAllowed: true},
		{name: "newly set Pinned denied", newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}},
		{name: "Independent to Proportional allowed", oldPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, newPolicy: runtimeProportionalPolicy("1"), wantAllowed: true},
		{name: "unchanged stored Proportional ratchets through", oldPolicy: runtimeProportionalPolicy("1"), newPolicy: runtimeProportionalPolicy("1"), wantAllowed: true},
		{name: "unchanged stored Pinned ratchets through", oldPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}, wantAllowed: true},
		{name: "changed Proportional ratio allowed", oldPolicy: runtimeProportionalPolicy("1"), newPolicy: runtimeProportionalPolicy("2"), wantAllowed: true},
		{name: "Proportional to Independent allowed", oldPolicy: runtimeProportionalPolicy("1"), newPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}, wantAllowed: true},
		{name: "Proportional removed allowed", oldPolicy: runtimeProportionalPolicy("1"), newPolicy: nil, wantAllowed: true},
	}
	for _, tt := range tests {
		t.Run("SR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			req := updateReq(t,
				servingRuntime("rt-sp", "default", withScalingPolicy(tt.oldPolicy)),
				servingRuntime("rt-sp", "default", withScalingPolicy(tt.newPolicy)))
			resp := newServingRuntimeValidator(t).Handle(context.Background(), req)
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(validation.ReasonScalingModeNotImplemented))
			}
		})
		t.Run("CSR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			req := updateReq(t,
				clusterServingRuntime("crt-sp", withScalingPolicy(tt.oldPolicy)),
				clusterServingRuntime("crt-sp", withScalingPolicy(tt.newPolicy)))
			resp := newClusterServingRuntimeValidator(t).Handle(context.Background(), req)
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(validation.ReasonScalingModeNotImplemented))
			}
		})
	}
}

// A disabled runtime still validates a newly declared scaling policy.
func TestServingRuntime_ScalingPolicyCheckedWhileDisabled(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := servingRuntime("rt-disabled", "default", withScalingPolicy(runtimeProportionalPolicy("0")), withDisabled(true))
	resp := newServingRuntimeValidator(t).Handle(context.Background(), createReq(t, sr))
	g.Expect(resp.Allowed).To(gomega.BeFalse(), "resp=%v", resp.Result)
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("ratios"))

	csr := clusterServingRuntime("crt-disabled", withScalingPolicy(runtimeProportionalPolicy("0")), withDisabled(true))
	resp = newClusterServingRuntimeValidator(t).Handle(context.Background(), createReq(t, csr))
	g.Expect(resp.Allowed).To(gomega.BeFalse(), "resp=%v", resp.Result)
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("ratios"))
}

// A stored runtime keeps accepting writes that leave its scaling policy as
// it is.
func TestServingRuntime_ScalingPolicyRatchetAllowsUnrelatedUpdate(t *testing.T) {
	g := gomega.NewWithT(t)
	oldSR := servingRuntime("rt-ratchet", "default", withScalingPolicy(runtimeProportionalPolicy("1")))
	newSR := servingRuntime("rt-ratchet", "default", withScalingPolicy(runtimeProportionalPolicy("1")), withImage("ome/vllm:newer"))
	resp := newServingRuntimeValidator(t).Handle(context.Background(), updateReq(t, oldSR, newSR))
	g.Expect(resp.Allowed).To(gomega.BeTrue(), "resp=%v", resp.Result)
}
