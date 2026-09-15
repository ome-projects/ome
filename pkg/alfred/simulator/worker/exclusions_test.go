package worker

import (
	"context"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

func excludedSourceGangRequest(t *testing.T, p *Profile) protocol.Request {
	t.Helper()
	r := gangRequest(t, p)
	source := testNode("source", "8")
	source.Labels["topology.example/domain"] = "aaa-source"
	replaceObject(t, &r, 0, source)
	second := testNode("source-two", "8")
	second.Labels["topology.example/domain"] = "aaa-source"
	addObject(t, &r, second)
	r.ExcludedNodes = append(r.ExcludedNodes, second.Name)
	r.ReplacementPods[0].Labels["role"] = "leader"
	r.ReplacementPods[1].Spec.Affinity = &v1.Affinity{PodAffinity: &v1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
		TopologyKey: "topology.example/domain", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "leader"}},
	}}}}
	return r
}

func TestOMEGangExclusionsApplyBeforeDomainPlanning(t *testing.T) {
	p := testProfile(t, omeConfig)
	r := excludedSourceGangRequest(t, p)
	got := evaluateTest(t, p, r)
	if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 2 {
		t.Fatalf("excluded spare source capacity blocked a valid gang: %+v", got)
	}
	for _, placement := range got.Placements {
		if placement.NodeName == "source" || placement.NodeName == "source-two" {
			t.Fatalf("excluded source placement: %+v", placement)
		}
	}
}

func TestPrivateExclusionsKeepSourceOccupancyAndInputImmutable(t *testing.T) {
	r := excludedSourceGangRequest(t, testProfile(t, omeConfig))
	snap, err := protocol.Validate(r)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := newPrivateClient(r, snap)
	node, err := client.CoreV1().Nodes().Get(context.Background(), "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if node.Spec.Unschedulable {
		t.Fatal("replacement exclusion changed the shared node view")
	}
	replacement, err := client.CoreV1().Pods("test").Get(context.Background(), r.ReplacementPods[0].Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replacement.Spec, r.ReplacementPods[0].Spec) {
		t.Fatal("exclusion changed replacement scheduling constraints")
	}
	if snap.Nodes["source"].Spec.Unschedulable {
		t.Fatal("changed original snapshot node")
	}
	pod, err := client.CoreV1().Pods("test").Get(context.Background(), "source", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pod.UID != r.SourcePods[0].UID || pod.Spec.NodeName != "source" || pod.Spec.Containers[0].Resources.Requests.Name("example.com/gpu", resource.DecimalSI).Value() != 1 {
		t.Fatal("private exclusion lost source occupancy")
	}
}

func TestReplacementExclusionsPreserveRequiredAffinityTerms(t *testing.T) {
	for _, tc := range []struct {
		name            string
		selector        *v1.NodeSelector
		fitsDestination bool
	}{
		{"no affinity", nil, true},
		{"zero terms", &v1.NodeSelector{}, false},
		{"empty term", &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{}}}, false},
		{"OR terms", &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
			{MatchExpressions: []v1.NodeSelectorRequirement{{Key: v1.LabelHostname, Operator: v1.NodeSelectorOpIn, Values: []string{"source"}}}},
			{MatchExpressions: []v1.NodeSelectorRequirement{{Key: v1.LabelHostname, Operator: v1.NodeSelectorOpIn, Values: []string{"destination"}}}},
		}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testProfile(t, defaultConfig)
			r := testRequest(t, p)
			r.ReplacementPods[0].Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: tc.selector}}
			got := evaluateTest(t, p, r)
			if (got.Decision == protocol.DecisionFeasible) != tc.fitsDestination {
				t.Fatalf("fitsDestination=%t: %+v", tc.fitsDestination, got)
			}
			for _, placement := range got.Placements {
				if placement.NodeName != "destination" {
					t.Fatalf("excluded source accepted: %+v", got)
				}
			}
		})
	}
}
