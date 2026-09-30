package capacity

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestHardwareSelector(t *testing.T) {
	term := func(key string, operator corev1.NodeSelectorOperator, values ...string) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: operator, Values: values}}}
	}
	nodes := []corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"hardware": "a", "generation": "3", "zone": "east"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"hardware": "b", "generation": "6"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-c", Labels: map[string]string{"hardware": "c", "generation": "10", "zone": "west"}}},
	}
	for _, tt := range []struct {
		name     string
		selector map[string]string
		terms    []corev1.NodeSelectorTerm
		want     []string
		wantErr  string
	}{
		{name: "no required hardware constraint", want: []string{"node-a", "node-b", "node-c"}},
		{name: "node selector", selector: map[string]string{"hardware": "a"}, want: []string{"node-a"}},
		{name: "outside catalog labels", selector: map[string]string{"model-ready": "true"}, want: []string{"node-a", "node-b", "node-c"}},
		{name: "in", terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn, "a", "b")}, want: []string{"node-a", "node-b"}},
		{name: "not in", terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpNotIn, "a")}, want: []string{"node-b", "node-c"}},
		{name: "exists", terms: []corev1.NodeSelectorTerm{term("zone", corev1.NodeSelectorOpExists)}, want: []string{"node-a", "node-c"}},
		{name: "does not exist", terms: []corev1.NodeSelectorTerm{term("zone", corev1.NodeSelectorOpDoesNotExist)}, want: []string{"node-b"}},
		{name: "greater than", terms: []corev1.NodeSelectorTerm{term("generation", corev1.NodeSelectorOpGt, "5")}, want: []string{"node-b", "node-c"}},
		{name: "less than", terms: []corev1.NodeSelectorTerm{term("generation", corev1.NodeSelectorOpLt, "6")}, want: []string{"node-a"}},
		{name: "or terms", terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn, "a"), term("zone", corev1.NodeSelectorOpIn, "west")}, want: []string{"node-a", "node-c"}},
		{name: "selector and affinity intersect", selector: map[string]string{"hardware": "a"}, terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn, "b")}},
		{name: "outside expression keeps hardware restriction", terms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
			{Key: "hardware", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}},
			{Key: "model-ready", Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}},
		}}}, want: []string{"node-a"}},
		{name: "outside or term permits every flavor", terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn, "a"), term("model-ready", corev1.NodeSelectorOpExists)}, want: []string{"node-a", "node-b", "node-c"}},
		{name: "outside or term keeps node selector", selector: map[string]string{"hardware": "b"}, terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn, "a"), term("model-ready", corev1.NodeSelectorOpExists)}, want: []string{"node-b"}},
		{name: "field constraints require scheduler checks", terms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}}}}, want: []string{"node-a", "node-b", "node-c"}},
		{name: "invalid hardware operator", terms: []corev1.NodeSelectorTerm{term("hardware", "Unknown", "a")}, wantErr: "Unsupported value"},
		{name: "invalid hardware values", terms: []corev1.NodeSelectorTerm{term("hardware", corev1.NodeSelectorOpIn)}, wantErr: "values set can't be empty"},
		{name: "invalid numeric value", terms: []corev1.NodeSelectorTerm{term("generation", corev1.NodeSelectorOpGt, "bad")}, wantErr: "integer"},
		{name: "irrelevant expressions are scheduler inputs", terms: []corev1.NodeSelectorTerm{term("model-ready", "Unknown")}, want: []string{"node-a", "node-b", "node-c"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := &corev1.PodSpec{NodeSelector: tt.selector}
			if tt.terms != nil {
				spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: tt.terms}}}
			}
			before := spec.DeepCopy()
			selector, err := hardwareSelector(spec, sets.New("hardware", "generation", "zone"))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var matches []string
			for i := range nodes {
				match, err := selector.Match(&nodes[i])
				if err != nil {
					t.Fatal(err)
				}
				if match {
					matches = append(matches, nodes[i].Name)
				}
			}
			if diff := cmp.Diff(tt.want, matches); diff != "" {
				t.Fatalf("hardware matches (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, spec); diff != "" {
				t.Fatalf("pod mutation (-want +got):\n%s", diff)
			}
		})
	}
}
