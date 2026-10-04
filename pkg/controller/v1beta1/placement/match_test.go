package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func wc(name string, ready bool, labels map[string]string) v1beta1.WorkloadCluster {
	st := metav1.ConditionFalse
	if ready {
		st = metav1.ConditionTrue
	}
	return v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: labels},
		Status:     v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{{Type: v1beta1.WorkloadClusterReady, Status: st, Reason: "Observed"}}},
	}
}

// testAffinity expresses a conjunction compactly in controller fixtures.
func testAffinity(selectors ...string) []v1beta1.ClusterAffinityTerm {
	var term v1beta1.ClusterAffinityTerm
	for _, input := range selectors {
		if input == "" {
			continue
		}
		selector, err := labels.Parse(input)
		if err != nil {
			return []v1beta1.ClusterAffinityTerm{{MatchExpressions: []v1beta1.ClusterSelectorRequirement{{Key: "invalid", Operator: "Invalid"}}}}
		}
		requirements, _ := selector.Requirements()
		for _, req := range requirements {
			op := map[selection.Operator]corev1.NodeSelectorOperator{
				selection.Equals: corev1.NodeSelectorOpIn, selection.DoubleEquals: corev1.NodeSelectorOpIn, selection.In: corev1.NodeSelectorOpIn,
				selection.NotEquals: corev1.NodeSelectorOpNotIn, selection.NotIn: corev1.NodeSelectorOpNotIn,
				selection.Exists: corev1.NodeSelectorOpExists, selection.DoesNotExist: corev1.NodeSelectorOpDoesNotExist,
				selection.GreaterThan: corev1.NodeSelectorOpGt, selection.LessThan: corev1.NodeSelectorOpLt,
			}[req.Operator()]
			r := v1beta1.ClusterSelectorRequirement{Key: req.Key(), Operator: op, Values: req.Values().List()}
			if req.Key() == metav1.ObjectNameField {
				term.MatchFields = append(term.MatchFields, r)
			} else {
				term.MatchExpressions = append(term.MatchExpressions, r)
			}
		}
	}
	if len(term.MatchExpressions)+len(term.MatchFields) == 0 {
		return nil
	}
	return []v1beta1.ClusterAffinityTerm{term}
}

func isvcReq(requirements, selector string) *v1beta1.InferenceService {
	return isvcPlacement(&v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: testAffinity(requirements, selector)})
}

func isvcPlacement(p *v1beta1.PlacementSpec) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Placement: p}}
}

func TestMatchCandidates(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("member-b", true, map[string]string{"accelerator": "gpu-a", "region": "east"}),
		wc("member-a", true, map[string]string{"accelerator": "gpu-a", "region": "west", "metadata.name": "label-name"}),
		wc("member-c", true, map[string]string{"accelerator": "gpu-b", "region": "west"}),
		wc("member-d", false, map[string]string{"accelerator": "gpu-a", "region": "west"}),
	}
	for _, tt := range []struct {
		name    string
		source  *v1beta1.InferenceService
		want    []string
		reason  MatchReason
		invalid bool
	}{
		{name: "local service", source: isvcPlacement(nil), reason: MatchReasonNoRequirements},
		{name: "unconstrained typed intent", source: isvcReq("", ""), want: []string{"member-a", "member-b", "member-c"}},
		{name: "matching labels", source: isvcReq("accelerator=gpu-a", ""), want: []string{"member-a", "member-b"}},
		{name: "conjunctive requirements", source: isvcReq("accelerator=gpu-a", "region=west"), want: []string{"member-a"}},
		{name: "object names", source: isvcReq("accelerator=gpu-a", "metadata.name in (member-b,member-c,member-d)"), want: []string{"member-b"}},
		{name: "actual metadata name label", source: isvcPlacement(&v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: []v1beta1.ClusterAffinityTerm{{MatchExpressions: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"label-name"}}}}}}), want: []string{"member-a"}},
		{name: "unready match", source: isvcReq("", "metadata.name=member-d"), reason: MatchReasonNoReadyClusters},
		{name: "no matching registration", source: isvcReq("accelerator=gpu-c", ""), reason: MatchReasonNoMatch},
		{name: "malformed affinity", source: isvcReq("!!!", ""), reason: MatchReasonMalformedSelector, invalid: true},
		{name: "empty affinity", source: isvcPlacement(&v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: []v1beta1.ClusterAffinityTerm{}}), reason: MatchReasonMalformedSelector, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := &v1beta1.WorkloadClusterList{Items: clusters}
			snapshot := before.DeepCopy()
			got, reason, err := MatchCandidates(tt.source, clusters)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("%s\n%v", diff, err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.reason, reason); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(snapshot, before); diff != "" {
				t.Fatalf("mutated registration:\n%s", diff)
			}
		})
	}
}

func TestPlacementEligibilityIncludesInvalidIntent(t *testing.T) {
	for _, tt := range []struct {
		name     string
		source   *v1beta1.InferenceService
		eligible bool
		invalid  bool
	}{
		{name: "local", source: isvcPlacement(nil)},
		{name: "unconstrained", source: isvcReq("", ""), eligible: true},
		{name: "empty block", source: isvcPlacement(&v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}), eligible: true, invalid: true},
		{name: "legacy empty annotation", source: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{ClusterSelectorAnnotation: ""}}}, eligible: true, invalid: true},
		{name: "legacy annotation beside typed intent", source: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AcceleratorRequirementsAnnotation: "accelerator=gpu-a"}}, Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}}}, eligible: true, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.eligible, IsPlacementEligible(tt.source)); diff != "" {
				t.Fatal(diff)
			}
			_, reason, err := MatchCandidates(tt.source, nil)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatal(diff)
			}
			if tt.invalid && !strings.Contains(string(reason), "Invalid") {
				t.Fatalf("missing invalid-input reason: %s", reason)
			}
		})
	}
}
