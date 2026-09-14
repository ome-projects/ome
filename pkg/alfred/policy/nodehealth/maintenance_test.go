package nodehealth

import (
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestMaintenanceEvacuatesWholeGangAndCopiesMarker(t *testing.T) {
	snap := testutil.NewSnapshot().
		WithNode("source-a", "h100", 8).
		WithNode("source-b", "h100", 8).
		WithNode("target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 2, "source-b", "source-a").Build()
	source := snap.Nodes["source-b"]
	source.UID = "node-incarnation"
	source.Maintenance = snapshot.NodeMaintenanceObservation{Requested: true, Triggers: []string{"label:maintenance=planned"}}
	got := evaluate(t, snap, config.Default())
	if len(got) != 2 || got[1].Remediation != nil || !got[1].Executable ||
		got[1].Reason != policy.ReasonNodeMaintenance || got[1].FromNode != "source-b" ||
		got[1].Instance != 0 || got[1].FootprintGPUs != 4 || got[1].Emergency {
		t.Fatalf("maintenance must evacuate one complete gang: %+v", got)
	}
	marker := got[0].Remediation
	if marker == nil || marker.NodeUID != "node-incarnation" || !marker.ObservedAt.Equal(snap.Timestamp) ||
		!marker.Maintenance.Requested || !reflect.DeepEqual(marker.Workloads, []string{"prod/wide"}) ||
		!marker.OMEGPUOccupantsPresent {
		t.Fatalf("complete maintenance marker = %+v", marker)
	}
	marker.Maintenance.Triggers[0] = "mutated"
	if source.Maintenance.Triggers[0] != "label:maintenance=planned" {
		t.Fatal("marker shares mutable maintenance trigger storage with snapshot")
	}
}

func TestHealthWinsOverMaintenanceForSameInstance(t *testing.T) {
	snap := testutil.NewSnapshot().
		WithNode("a-maintenance", "h100", 8).
		WithNode("z-unhealthy", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 2, "a-maintenance", "z-unhealthy").Build()
	snap.Nodes["a-maintenance"].Maintenance.Requested = true
	got := evaluate(t, snap, config.Default())
	if len(got) != 3 || got[2].Remediation != nil || got[2].Reason != policy.ReasonNodeUnhealthy ||
		got[2].FromNode != "z-unhealthy" || got[2].FootprintGPUs != 4 {
		t.Fatalf("dual-source instance must yield one health finding from its unhealthy member: %+v", got)
	}
}

func TestMaintenanceClearingAndSignalOnly(t *testing.T) {
	snap := oneOMENative(snapshot.NodeHealthClear)
	snap.Nodes["source"].Maintenance.Requested = true
	cfg := config.Default()
	cfg.Policies.NodeHealth.SignalOnly = true
	got := evaluate(t, snap, cfg)
	if len(got) != 1 || got[0].Remediation == nil || !got[0].Remediation.Maintenance.Requested {
		t.Fatalf("signalOnly must retain maintenance marker without any moves: %+v", got)
	}
	snap.Nodes["source"].Maintenance = snapshot.NodeMaintenanceObservation{}
	if got := evaluate(t, snap, cfg); len(got) != 0 {
		t.Fatalf("cleared maintenance marker persisted: %+v", got)
	}
}

func TestDualSignalClearingPreservesRemainingMaintenance(t *testing.T) {
	snap := oneOMENative(snapshot.NodeHealthUnhealthy)
	snap.Nodes["source"].Maintenance.Requested = true
	got := evaluate(t, snap, config.Default())
	if len(got) != 2 || got[0].Remediation == nil || !got[0].Remediation.Maintenance.Requested ||
		got[0].Remediation.Health.State != snapshot.NodeHealthUnhealthy || got[1].Reason != policy.ReasonNodeUnhealthy {
		t.Fatalf("dual signal must carry both observations with one health finding: %+v", got)
	}
	snap.Nodes["source"].Health = snapshot.NodeHealthObservation{State: snapshot.NodeHealthClear}
	got = evaluate(t, snap, config.Default())
	if len(got) != 2 || got[0].Remediation == nil || !got[0].Remediation.Maintenance.Requested ||
		got[1].Reason != policy.ReasonNodeMaintenance {
		t.Fatalf("cleared health suppressed remaining planned work: %+v", got)
	}
}

func TestMaintenanceUnsupportedAndUnreadyWorkloadsRemainAdvisory(t *testing.T) {
	for _, mode := range []constants.DeploymentModeType{constants.RawDeployment, constants.MultiNode, constants.OMENative} {
		t.Run(string(mode), func(t *testing.T) {
			snap := testutil.NewSnapshot().WithNode("source", "h100", 8).WithNode("target", "h100", 8).
				WithInstance("prod/model", v1beta1.EngineComponent, mode, "source", 1).Build()
			snap.Nodes["source"].Maintenance.Requested = true
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			w.Components[v1beta1.EngineComponent].Instances[0].Pods[0].Ready = false
			got := evaluate(t, snap, config.Default())
			if len(got) != 2 || got[1].Executable || got[1].AdvisoryReason == "" || got[1].Reason != policy.ReasonNodeMaintenance {
				t.Fatalf("unsupported or unready maintenance workload became executable: %+v", got)
			}
		})
	}
}

func TestMaintenanceMalformedOccupancyIsAdvisory(t *testing.T) {
	snap := oneOMENative(snapshot.NodeHealthClear)
	snap.Nodes["source"].Maintenance.Requested = true
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	comp := w.Components[v1beta1.EngineComponent]
	comp.ObservationValid = false
	comp.Instances = nil
	got := evaluate(t, snap, config.Default())
	if len(got) != 2 || got[1].Reason != policy.ReasonNodeMaintenance ||
		got[1].Instance != policy.ComponentWideInstance || got[1].FromNode != "source" ||
		got[1].AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid || got[1].Executable {
		t.Fatalf("malformed physical occupancy lost maintenance advisory: %+v", got)
	}
}

func TestMaintenanceOnlyHasNormalCandidatePriority(t *testing.T) {
	snap := testutil.NewSnapshot().WithNode("health", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("maintenance", "h100", 8).WithNode("target", "h100", 8).
		WithInstance("prod/health", v1beta1.EngineComponent, constants.OMENative, "health", 1).
		WithInstance("prod/maintenance", v1beta1.EngineComponent, constants.OMENative, "maintenance", 1).Build()
	snap.Nodes["maintenance"].Maintenance.Requested = true
	snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "maintenance"}].Priority = 1
	snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "health"}].Priority = 0
	// Policy keeps findings through cooldown; the arbiter applies the correct
	// class-specific time gate, including ordinary maintenance cooldowns.
	last := snap.Timestamp.Add(-10 * time.Minute)
	snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "maintenance"}].LastMigration = &last
	got := evaluate(t, snap, config.Default())
	if len(got) != 4 || got[2].Reason != policy.ReasonNodeUnhealthy || got[3].Reason != policy.ReasonNodeMaintenance {
		t.Fatalf("health must rank above higher-score maintenance: %+v", got)
	}
}

func TestRecreatedPhysicalPodCannotBeCoveredByStaleInstance(t *testing.T) {
	snap := oneOMENative(snapshot.NodeHealthUnhealthy)
	comp := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}].Components[v1beta1.EngineComponent]
	comp.ObservationValid = false
	comp.Instances[0].Pods[0].UID = "stale-pod"
	snap.Nodes["source"].OMEPods[0].UID = "replacement-pod"
	got := evaluate(t, snap, config.Default())
	if len(got) != 3 || got[1].Instance != policy.ComponentWideInstance ||
		got[1].AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid {
		t.Fatalf("stale instance masked same-name replacement physical occupancy: %+v", got)
	}
}

func TestMaintenanceCPUCompanionStillEvacuatesWholeGPUInstance(t *testing.T) {
	snap := testutil.NewSnapshot().WithNode("cpu-source", "", 0).
		WithNode("gpu-source", "h100", 8).WithNode("target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "cpu-source", "gpu-source").Build()
	snap.Nodes["cpu-source"].Maintenance.Requested = true
	comp := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}].Components[v1beta1.EngineComponent]
	inst := comp.Instances[0]
	for i := range inst.Pods {
		if inst.Pods[i].Node == "cpu-source" {
			inst.Pods[i].GPUs = 0
		}
	}
	inst.TotalGPUs = 1
	snap.Nodes["cpu-source"].AllocatedGPUs = 0
	snap.Nodes["cpu-source"].FreeGPUs = 0
	snap.Nodes["cpu-source"].OMEPods[0].GPUs = 0
	got := evaluate(t, snap, config.Default())
	if len(got) != 2 || !got[1].Executable || got[1].Reason != policy.ReasonNodeMaintenance ||
		got[1].FromNode != "cpu-source" || got[1].FootprintGPUs != 1 {
		t.Fatalf("CPU companion on maintenance node did not evacuate complete GPU instance: %+v", got)
	}
}
