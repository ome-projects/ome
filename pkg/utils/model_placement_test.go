package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestModelPlacementPreservesScoutSemantics(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"pool": "models", "count": "10", "word": "z"}}, Spec: corev1.NodeSpec{Unschedulable: true, Taints: []corev1.Taint{{Key: "avoid", Effect: corev1.TaintEffectNoSchedule}}}}
	for _, tc := range []struct {
		name     string
		selector map[string]string
		terms    []corev1.NodeSelectorTerm
		want     bool
	}{
		{name: "empty terms ignore health and taints", want: true},
		{name: "selector", selector: map[string]string{"pool": "models"}, want: true},
		{name: "selector AND affinity", selector: map[string]string{"pool": "other"}, terms: []corev1.NodeSelectorTerm{{}}, want: false},
		{name: "terms OR", terms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "missing", Operator: corev1.NodeSelectorOpExists}}}, {}}, want: true},
		{name: "requirements AND", terms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpExists}, {Key: "missing", Operator: corev1.NodeSelectorOpExists}}}}, want: false},
		{name: "field name", terms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}}}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &v1beta1.StorageSpec{NodeSelector: tc.selector, NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: tc.terms}, PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 100, Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "unmatched", Operator: corev1.NodeSelectorOpExists}}}}}}}
			require.Equal(t, tc.want, ModelMatchesNodePlacement(storage, node))
		})
	}
	require.True(t, ModelMatchesNodePlacement(nil, node))
	for _, tc := range []struct {
		key    string
		op     corev1.NodeSelectorOperator
		values []string
		want   bool
	}{
		{"missing", corev1.NodeSelectorOpNotIn, []string{"anything"}, false},
		{"missing", corev1.NodeSelectorOpDoesNotExist, nil, true},
		{"pool", corev1.NodeSelectorOpDoesNotExist, nil, false},
		{"pool", corev1.NodeSelectorOpNotIn, []string{"other"}, true},
		{"count", corev1.NodeSelectorOpGt, []string{"2"}, true},
		{"count", corev1.NodeSelectorOpLt, []string{"20"}, true},
		{"count", corev1.NodeSelectorOpGt, nil, false},
		{"word", corev1.NodeSelectorOpGt, []string{"a"}, true},
		{"word", corev1.NodeSelectorOpLt, []string{"zz"}, true},
		{"metadata.name", corev1.NodeSelectorOpIn, []string{"node-a"}, true},
	} {
		t.Run(tc.key+string(tc.op), func(t *testing.T) {
			require.Equal(t, tc.want, NodeMatchesModelRequirement(node, corev1.NodeSelectorRequirement{Key: tc.key, Operator: tc.op, Values: tc.values}))
		})
	}
	node.Labels["metadata.name"] = "label-overrides-field"
	require.True(t, NodeMatchesModelRequirement(node, corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"label-overrides-field"}}))
}
