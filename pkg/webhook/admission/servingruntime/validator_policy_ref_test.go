package servingruntime

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// withPolicyRef puts an autoscaler policy reference on one component config;
// an empty component leaves the runtime without one.
func withPolicyRef(component string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		ext := v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"}}
		switch component {
		case "engineConfig":
			spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
		case "decoderConfig":
			spec.DecoderConfig = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
		case "routerConfig":
			spec.RouterConfig = &v1beta1.RouterSpec{ComponentExtensionSpec: ext}
		}
	}
}

// Policy references attach on the InferenceService only; one stored on a
// runtime component would be admitted and never read, so both runtime
// webhooks refuse it and say where it belongs.
func TestServingRuntime_AutoscalerPolicyRefRejected(t *testing.T) {
	tests := []struct {
		name        string
		component   string
		wantAllowed bool
	}{
		{name: "no ref allowed", component: "", wantAllowed: true},
		{name: "engineConfig ref denied", component: "engineConfig"},
		{name: "decoderConfig ref denied", component: "decoderConfig"},
		{name: "routerConfig ref denied", component: "routerConfig"},
	}
	for _, tt := range tests {
		t.Run("SR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			sr := servingRuntime("rt-ref", "default", withPolicyRef(tt.component))
			resp := newServingRuntimeValidator(t).Handle(context.Background(), createReq(t, sr))
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(tt.component))
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring("policy refs attach on the InferenceService only"))
			}
		})
		t.Run("CSR "+tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			csr := clusterServingRuntime("crt-ref", withPolicyRef(tt.component))
			resp := newClusterServingRuntimeValidator(t).Handle(context.Background(), createReq(t, csr))
			g.Expect(resp.Allowed).To(gomega.Equal(tt.wantAllowed), "resp=%v", resp.Result)
			if !tt.wantAllowed {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(tt.component))
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring("policy refs attach on the InferenceService only"))
			}
		})
	}
}

// The check runs before the disabled shortcut, so a disabled runtime cannot
// store a reference that re-enabling would bring back.
func TestServingRuntime_AutoscalerPolicyRefCheckedWhileDisabled(t *testing.T) {
	g := gomega.NewWithT(t)

	sr := servingRuntime("rt-ref-disabled", "default", withDisabled(true), withPolicyRef("engineConfig"))
	resp := newServingRuntimeValidator(t).Handle(context.Background(), createReq(t, sr))
	g.Expect(resp.Allowed).To(gomega.BeFalse(), "resp=%v", resp.Result)

	csr := clusterServingRuntime("crt-ref-disabled", withDisabled(true), withPolicyRef("engineConfig"))
	resp = newClusterServingRuntimeValidator(t).Handle(context.Background(), createReq(t, csr))
	g.Expect(resp.Allowed).To(gomega.BeFalse(), "resp=%v", resp.Result)
}
