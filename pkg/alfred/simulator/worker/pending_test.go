package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

func competingPod() v1.Pod {
	p := testPod("competitor", "")
	priority := int32(1000)
	never := v1.PreemptNever
	p.Spec.Priority, p.Spec.PreemptionPolicy = &priority, &never
	return p
}

// Dropping pending occupancy, ignoring its priority, or counting its PostBind
// as replacement completion would incorrectly admit the one-GPU case.
func TestPendingCompetitionConsumesCapacity(t *testing.T) {
	for _, config := range []string{defaultConfig, omeConfig} {
		p := testProfile(t, config)
		for _, tc := range []struct {
			gpu      string
			feasible bool
		}{{"1", false}, {"2", true}} {
			t.Run(p.Identity.ConfigurationID+"/"+tc.gpu, func(t *testing.T) {
				r := testRequest(t, p)
				replaceObject(t, &r, 1, testNode("destination", tc.gpu))
				competitor := competingPod()
				competitor.Spec.NodeSelector = map[string]string{v1.LabelHostname: "destination"}
				addObject(t, &r, competitor)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				got, err := p.Evaluate(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
				if (got.Decision == protocol.DecisionFeasible) != tc.feasible {
					t.Fatalf("capacity %s: got %+v, feasible want %t", tc.gpu, got, tc.feasible)
				}
				if tc.feasible {
					if len(got.Placements) != 1 || got.Placements[0].Pod.Name != "replacement" || got.Placements[0].NodeName != "destination" {
						t.Fatalf("competitor leaked into replacement result: %+v", got)
					}
				} else if len(got.Placements) != 0 {
					t.Fatalf("negative result leaked placements: %+v", got)
				}
			})
		}
	}
}

func TestPendingCompetitionDoesNotGloballyExcludeSource(t *testing.T) {
	for _, config := range []string{defaultConfig, omeConfig} {
		p := testProfile(t, config)
		r := testRequest(t, p)
		replaceObject(t, &r, 0, testNode("source", "2"))
		competitor := competingPod()
		competitor.Spec.NodeSelector = map[string]string{v1.LabelHostname: "source"}
		addObject(t, &r, competitor)
		// The replacement depends on the competitor binding, so success proves
		// that the source was usable by the competitor, not simply ignored.
		competitor.Labels = map[string]string{"role": "competitor"}
		replaceObject(t, &r, len(r.ClusterObjects)-1, competitor)
		for i := range r.ClusterObjects {
			// Both nodes share a domain; the replacement may remain off source.
			var n v1.Node
			if err := json.Unmarshal(r.ClusterObjects[i].Raw, &n); err == nil && n.Kind == "Node" {
				n.Labels["topology.example/domain"] = "shared"
				replaceObject(t, &r, i, &n)
			}
		}
		r.ReplacementPods[0].Spec.Affinity = &v1.Affinity{PodAffinity: &v1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{TopologyKey: "topology.example/domain", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "competitor"}}}}}}
		before, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		got := evaluateTest(t, p, r)
		if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 1 || got.Placements[0].NodeName != "destination" {
			t.Fatalf("source exclusion affected competitor: %+v", got)
		}
		after, err := json.Marshal(r)
		if err != nil || string(before) != string(after) {
			t.Fatal("simulation changed its immutable input")
		}
	}
}

func TestUnscheduledCompetitorDoesNotAbortReplacement(t *testing.T) {
	for _, config := range []string{defaultConfig, omeConfig} {
		p := testProfile(t, config)
		for _, gated := range []bool{false, true} {
			r := testRequest(t, p)
			competitor := competingPod()
			if gated {
				competitor.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: "example.com/wait"}}
			} else {
				competitor.Spec.NodeSelector = map[string]string{"example.com/missing": "true"}
			}
			addObject(t, &r, competitor)
			got := evaluateTest(t, p, r)
			if got.Decision != protocol.DecisionFeasible || len(got.Placements) != 1 || got.Placements[0].Pod.Name != "replacement" {
				t.Fatalf("gated=%t: competitor blocked replacement: %+v", gated, got)
			}
		}
	}
}

func TestPendingCompetitionPreservesWholeReplacementGang(t *testing.T) {
	p := testProfile(t, omeConfig)
	for _, extraCapacity := range []bool{false, true} {
		r := gangRequest(t, p)
		if extraCapacity {
			n := testNode("destination-three", "1")
			n.Labels["topology.example/domain"] = "destination"
			addObject(t, &r, n)
		}
		competitor := competingPod()
		competitor.Spec.NodeSelector = map[string]string{v1.LabelHostname: "destination"}
		addObject(t, &r, competitor)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := p.Evaluate(ctx, r)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if (got.Decision == protocol.DecisionFeasible) != extraCapacity {
			t.Fatalf("extraCapacity=%t: %+v", extraCapacity, got)
		}
		if extraCapacity {
			if len(got.Placements) != 2 {
				t.Fatalf("partial gang result: %+v", got)
			}
			for _, placement := range got.Placements {
				if placement.Pod.Name == "competitor" || placement.NodeName == "destination" {
					t.Fatalf("competitor or occupied node reported: %+v", got)
				}
			}
		} else if len(got.Placements) != 0 {
			t.Fatalf("negative gang result leaked placements: %+v", got)
		}
	}
}

func TestPrivatePendingBindingIsIdentityAndRoleScoped(t *testing.T) {
	for _, tc := range []struct {
		name, pod, uid, node string
		allowed              bool
	}{
		{"competitor may use source", "competitor", "competitor-uid", "source", true},
		{"replacement may use destination", "replacement", "replacement-uid", "destination", true},
		{"replacement cannot use source", "replacement", "replacement-uid", "source", false},
		{"competitor wrong UID", "competitor", "stale-uid", "destination", false},
		{"source cannot be rebound", "source", "source-uid", "destination", false},
		{"unknown Pod", "unknown", "unknown-uid", "destination", false},
		{"unknown node", "competitor", "competitor-uid", "unknown", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := testRequest(t, testProfile(t, defaultConfig))
			addObject(t, &r, competingPod())
			snapshot, err := protocol.Validate(r)
			if err != nil {
				t.Fatal(err)
			}
			client, state := newPrivateClient(r, snapshot)
			binding := &v1.Binding{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: tc.pod, UID: types.UID(tc.uid)}, Target: v1.ObjectReference{Name: tc.node}}
			err = client.CoreV1().Pods("test").Bind(context.Background(), binding, metav1.CreateOptions{})
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%t: %v", tc.allowed, err)
			}
			if tc.allowed {
				pod, err := client.CoreV1().Pods("test").Get(context.Background(), tc.pod, metav1.GetOptions{})
				if err != nil || pod.Spec.NodeName != tc.node {
					t.Fatalf("private binding not recorded: %+v %v", pod, err)
				}
				if err := client.CoreV1().Pods("test").Bind(context.Background(), binding, metav1.CreateOptions{}); err == nil {
					t.Fatal("duplicate binding accepted")
				}
			} else if state.denied == nil {
				t.Fatal("unauthorized binding did not fail the simulation")
			}
			source, err := client.CoreV1().Pods("test").Get(context.Background(), "source", metav1.GetOptions{})
			if err != nil || source.Spec.NodeName != "source" || source.UID != "source-uid" {
				t.Fatal("source occupancy changed")
			}
		})
	}
}
