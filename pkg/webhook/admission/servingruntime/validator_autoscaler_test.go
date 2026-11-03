package servingruntime

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func withEngineAutoscaler(autoscaler *v1beta1.ComponentAutoscaler) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		spec.EngineConfig = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: autoscaler},
		}
	}
}

// A runtime's per-component autoscaler block meets the same shape check as
// an InferenceService's, so a malformed default is refused at admission
// instead of being inherited.
func TestClusterServingRuntimeValidator_AutoscalerShape(t *testing.T) {
	tests := []struct {
		name        string
		csr         *v1beta1.ClusterServingRuntime
		wantAllowed bool
		wantSubstr  string
	}{
		{
			name:        "valid engineConfig.autoscaler hpa default",
			csr:         clusterServingRuntime("rt-hpa", withEngineAutoscaler(&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA})),
			wantAllowed: true,
		},
		{
			name: "engineConfig.autoscaler keda with empty Triggers → denied",
			csr: clusterServingRuntime("rt-bad-keda", withEngineAutoscaler(&v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda:  &v1beta1.KedaAutoscaler{},
			})),
			wantSubstr: "engineConfig",
		},
		{
			name: "engineConfig.autoscaler unknown class → denied",
			csr: clusterServingRuntime("rt-bad-class", withEngineAutoscaler(&v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerClass("knative"),
			})),
			wantSubstr: "is not one of HPA|KEDA|External|None",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			resp := newClusterServingRuntimeValidator(t).Handle(context.Background(), createReq(t, tc.csr))

			g.Expect(resp.Allowed).To(gomega.Equal(tc.wantAllowed), "resp=%v", resp.Result)
			if tc.wantSubstr != "" {
				g.Expect(resp.Result).NotTo(gomega.BeNil())
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(tc.wantSubstr))
			}
		})
	}
}
