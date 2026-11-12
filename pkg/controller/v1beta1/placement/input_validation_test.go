package placement

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestInvalidPlacementCannotAcquireFinalizer(t *testing.T) {
	for _, tt := range []struct {
		name, placement  string
		legacyAnnotation bool
		router           bool
		wantMessage      string
	}{
		{name: "missing explicit mode", placement: `{}`},
		{name: "null affinity", placement: `{"mode":"Single","clusterAffinity":null}`},
		{name: "empty affinity", placement: `{"mode":"Single","clusterAffinity":[]}`},
		{name: "unsupported mode", placement: `{"mode":"Unexpected"}`},
		{name: "obsolete annotation beside typed intent", placement: `{"mode":"Single"}`, legacyAnnotation: true},
		{name: "a Split router weight needs a router minimum", placement: `{"mode":"Split","clusterAffinity":[{"weights":{"router":{"min":2}},"matchFields":[{"key":"metadata.name","operator":"In","values":["cluster-a"]}]}]}`,
			router: true, wantMessage: "set a positive router.minReplicas"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVC("")
			if err := json.Unmarshal([]byte(tt.placement), &source.Spec.Placement); err != nil {
				t.Fatal(err)
			}
			if source.Spec.Placement != nil {
				source.Spec.Placement.Policy = v1beta1.PlacementPolicyClusterAffinity
			}
			if tt.legacyAnnotation {
				source.Annotations[ClusterSelectorAnnotation] = ""
			}
			if tt.router {
				source.Spec.Router = &v1beta1.RouterSpec{}
			}
			before := source.DeepCopy()
			r, cp := newPlacer(testScheme(t), fakeClusters{}, source)
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			if err := cp.Get(t.Context(), client.ObjectKeyFromObject(source), got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before.Spec, got.Spec); diff != "" {
				t.Fatalf("invalid intent was normalized:\n%s", diff)
			}
			if diff := cmp.Diff(before.Finalizers, got.Finalizers); diff != "" {
				t.Fatalf("invalid source acquired ownership:\n%s", diff)
			}
			condition := got.Status.GetCondition(apis.ConditionType(v1beta1.PlacementInputValid))
			if condition == nil {
				t.Fatal("missing actionable input condition")
			}
			if diff := cmp.Diff(corev1.ConditionFalse, condition.Status); diff != "" {
				t.Fatal(diff)
			}
			if condition.Message == "" {
				t.Fatal("missing validation message")
			}
			if tt.wantMessage != "" && !strings.Contains(condition.Message, tt.wantMessage) {
				t.Fatalf("message %q does not contain %q", condition.Message, tt.wantMessage)
			}
		})
	}
}
