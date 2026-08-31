package testutil

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSnapshotBuilder(t *testing.T) {
	snap := NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8, NodeUnhealthy()).
		WithNode("node3", "a100", 4, NodeCordoned(), NodePreemptible()).
		WithInstance("prod/svc-a", v1beta1.EngineComponent, constants.RawDeployment, "node1", 2).
		WithMultiPodInstance("prod/svc-b", v1beta1.EngineComponent, constants.OMENative, 8, "node1", "node2").
		WithOtherOccupant("node2", 3).
		WithPendingPod(8, 20*time.Minute, "h100").
		ConfigureWorkload("prod/svc-a", func(w *snapshot.Workload) { w.Movable = false }).
		Build()

	node1 := snap.Nodes["node1"]
	if node1.AllocatedGPUs != 10 || node1.FreeGPUs != 0 {
		t.Fatalf("node1: allocated=%d free=%d, want 10/0 (floored)", node1.AllocatedGPUs, node1.FreeGPUs)
	}
	node2 := snap.Nodes["node2"]
	if node2.AllocatedGPUs != 11 || node2.FreeGPUs != 0 || !node2.Unhealthy {
		t.Fatalf("node2: %+v", node2)
	}
	if !snap.Nodes["node3"].Cordoned || !snap.Nodes["node3"].Preemptible {
		t.Fatalf("node3 flags: %+v", snap.Nodes["node3"])
	}

	svcA := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "svc-a"}]
	if svcA.Movable {
		t.Fatal("ConfigureWorkload should have set Movable=false")
	}
	if len(svcA.Components[v1beta1.EngineComponent].Instances) != 1 {
		t.Fatalf("svc-a instances: %+v", svcA.Components[v1beta1.EngineComponent].Instances)
	}

	svcB := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "svc-b"}]
	inst := svcB.Components[v1beta1.EngineComponent].Instances[0]
	if inst.TotalGPUs != 16 || len(inst.Pods) != 2 || inst.NodesSet["node1"] != 1 || inst.NodesSet["node2"] != 1 {
		t.Fatalf("multi-pod instance: %+v", inst)
	}
	if !svcB.Components[v1beta1.EngineComponent].ObservationValid || !inst.ObservationValid ||
		inst.Phase != v1beta1.OMENativeInstanceReady || inst.Incarnation != 1 ||
		inst.DesiredPods != 2 || inst.ObservedPods != 2 || inst.ServingPods != 2 || inst.AvailablePods != 2 {
		t.Fatalf("synthetic instance must default to structurally valid state: component=%+v instance=%+v",
			svcB.Components[v1beta1.EngineComponent], inst)
	}
	if !snap.OMENativeExecutor.Available || !snap.OMENativeAvailable {
		t.Fatalf("default executor state = %+v legacy=%t", snap.OMENativeExecutor, snap.OMENativeAvailable)
	}

	if len(snap.PendingPods) != 1 || snap.PendingPods[0].GPUsNeeded != 8 {
		t.Fatalf("pending: %+v", snap.PendingPods)
	}
	if got := snap.PendingPods[0].PendingSince; !got.Equal(ReferenceTime.Add(-20 * time.Minute)) {
		t.Fatalf("pending age: %v", got)
	}
}

func TestSnapshotBuilderStructuredExecutorAndInvalidObservation(t *testing.T) {
	renewed := ReferenceTime.Add(-time.Minute)
	snap := NewSnapshot().
		WithNode("node1", "h100", 8).
		WithMultiPodInstance("prod/svc", v1beta1.EngineComponent, constants.OMENative, 1, "node1", "node1").
		WithOMENativeExecutor(snapshot.OMENativeExecutorState{
			Available: true, WireVersion: "v2", RenewTime: renewed, Reason: "healthy",
		}).
		WithInvalidOMENativeObservation("prod/svc", v1beta1.EngineComponent, "synthetic disagreement").
		Build()

	if snap.OMENativeExecutor.WireVersion != "v2" || !snap.OMENativeExecutor.RenewTime.Equal(renewed) {
		t.Fatalf("structured executor state = %+v", snap.OMENativeExecutor)
	}
	component := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "svc"}].Components[v1beta1.EngineComponent]
	if component.ObservationValid || component.ObservationReason != "synthetic disagreement" ||
		component.Instances[0].ObservationValid || component.Instances[0].ObservationReason != "synthetic disagreement" {
		t.Fatalf("invalid synthetic observation = component=%+v instance=%+v", component, component.Instances[0])
	}
}

// A workload key must be exactly namespace/name with both segments non-empty;
// anything else would mint an invalid synthetic NamespacedName.
func TestEnsureWorkloadRejectsMalformedKeys(t *testing.T) {
	for _, key := range []string{"prod/", "/svc", "prod/svc/extra", "plain", ""} {
		t.Run(key, func(t *testing.T) {
			b := NewSnapshot().WithNode("node1", "h100", 8)
			defer func() {
				if recover() == nil {
					t.Fatalf("workload key %q must panic", key)
				}
			}()
			b.WithInstance(key, v1beta1.EngineComponent, constants.RawDeployment, "node1", 1)
		})
	}
}

// Adding a second instance to an existing component with a different
// deployment mode would silently re-label the instances already added under
// the earlier mode; the builder must refuse, mirroring WithNode.
func TestWithInstanceRejectsModeConflict(t *testing.T) {
	b := NewSnapshot().
		WithNode("node1", "h100", 8).
		WithInstance("prod/svc-a", v1beta1.EngineComponent, constants.RawDeployment, "node1", 1)

	defer func() {
		if recover() == nil {
			t.Fatal("conflicting DeploymentMode must panic, not rewrite the component")
		}
	}()
	b.WithInstance("prod/svc-a", v1beta1.EngineComponent, constants.MultiNode, "node1", 1)
}

// Redefining a node would silently discard occupancy accumulated by earlier
// builder calls while workload pods keep referencing it; the builder must
// refuse instead of producing an inconsistent snapshot.
func TestWithNodeRejectsRedefinition(t *testing.T) {
	b := NewSnapshot().
		WithNode("node1", "h100", 8).
		WithInstance("prod/svc-a", v1beta1.EngineComponent, constants.RawDeployment, "node1", 2)

	defer func() {
		if recover() == nil {
			t.Fatal("duplicate WithNode must panic, not overwrite the node")
		}
	}()
	b.WithNode("node1", "h100", 8)
}
