package nodehealth

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func evaluate(t *testing.T, snap *snapshot.ClusterSnapshot, cfg *config.Config) []policy.Candidate {
	t.Helper()
	p := &Policy{}
	if got := p.Name(); got != "nodehealth" {
		t.Fatalf("policy name = %q, want nodehealth", got)
	}
	return p.Evaluate(snap, cfg)
}

func markers(candidates []policy.Candidate) []policy.Candidate {
	var out []policy.Candidate
	for _, candidate := range candidates {
		if candidate.Remediation != nil {
			out = append(out, candidate)
		}
	}
	return out
}

func findings(candidates []policy.Candidate) []policy.Candidate {
	var out []policy.Candidate
	for _, candidate := range candidates {
		if candidate.Reason == policy.ReasonNodeUnhealthy && candidate.Remediation == nil {
			out = append(out, candidate)
		}
	}
	return out
}

func oneOMENative(state snapshot.NodeHealthState) *snapshot.ClusterSnapshot {
	b := testutil.NewSnapshot().
		WithNode("source", "h100", 8).
		WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "source", 1)
	snap := b.Build()
	observation := snapshot.NodeHealthObservation{State: state}
	if state == snapshot.NodeHealthUnhealthy || state == snapshot.NodeHealthUnknown {
		status := corev1.ConditionTrue
		if state == snapshot.NodeHealthUnknown {
			status = corev1.ConditionUnknown
		}
		observation.Conditions = []snapshot.NodeConditionObservation{{
			Type: "GpuUnhealthy", Status: status, LastTransitionTime: snap.Timestamp,
		}}
	}
	if state == snapshot.NodeHealthSuspect {
		until := snap.Timestamp.Add(30 * time.Minute)
		observation.Conditions = []snapshot.NodeConditionObservation{{
			Type: "GpuUnhealthy", Status: corev1.ConditionFalse, LastTransitionTime: snap.Timestamp,
		}}
		observation.SuspectUntil = &until
	}
	snap.Nodes["source"].Health = observation
	return snap
}

func TestStateAndConfigMatrix(t *testing.T) {
	tests := []struct {
		name         string
		state        snapshot.NodeHealthState
		enabled      bool
		signalOnly   bool
		wantMarkers  int
		wantFindings int
	}{
		{name: "zero is clear", state: "", enabled: true},
		{name: "explicit clear", state: snapshot.NodeHealthClear, enabled: true},
		{name: "suspect marker", state: snapshot.NodeHealthSuspect, enabled: true, wantMarkers: 1},
		{name: "unknown marker", state: snapshot.NodeHealthUnknown, enabled: true, wantMarkers: 1},
		{name: "unhealthy marker and finding", state: snapshot.NodeHealthUnhealthy, enabled: true, wantMarkers: 1, wantFindings: 1},
		{name: "disabled", state: snapshot.NodeHealthUnhealthy, enabled: false},
		{name: "signal only", state: snapshot.NodeHealthUnhealthy, enabled: true, signalOnly: true, wantMarkers: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			*cfg.Policies.NodeHealth.Enabled = tt.enabled
			cfg.Policies.NodeHealth.SignalOnly = tt.signalOnly
			got := evaluate(t, oneOMENative(tt.state), cfg)
			if len(markers(got)) != tt.wantMarkers || len(findings(got)) != tt.wantFindings {
				t.Fatalf("Evaluate() = %+v, want %d markers and %d findings", got, tt.wantMarkers, tt.wantFindings)
			}
			for _, marker := range markers(got) {
				if marker.Policy != "nodehealth" || marker.Reason != policy.ReasonRemediationSignal ||
					marker.Executable || marker.FromNode != "source" || marker.Remediation.Node != "source" {
					t.Fatalf("marker contract = %+v", marker)
				}
				if marker.Remediation.Health.State != tt.state {
					t.Fatalf("marker health = %+v, want state %q", marker.Remediation.Health, tt.state)
				}
			}
		})
	}
}

func TestMarkerCarriesSortedUniquePhysicalGPUWorkloads(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("zeta/z", v1beta1.EngineComponent, constants.RawDeployment, "bad", 1).
		WithInstance("alpha/a", v1beta1.EngineComponent, constants.RawDeployment, "bad", 1)
	snap := b.Build()
	node := snap.Nodes["bad"]
	node.OMEPods = append(node.OMEPods,
		node.OMEPods[0],
		snapshot.PodInfo{Namespace: "ignored", Name: "cpu", GPUs: 0, ISVC: types.NamespacedName{Namespace: "ignored", Name: "cpu"}},
		snapshot.PodInfo{Namespace: "ignored", Name: "ownerless", GPUs: 1},
		// Owner-resolved occupancy can outlive or disagree with the declared
		// workload layout. Its canonical parent must still block node drain.
		snapshot.PodInfo{Namespace: "prod", Name: "owner-evidence", GPUs: 1,
			ISVC: types.NamespacedName{Namespace: "prod", Name: "missing"}, Component: v1beta1.DecoderComponent},
	)

	got := markers(evaluate(t, snap, config.Default()))
	if len(got) != 1 {
		t.Fatalf("markers = %+v, want one", got)
	}
	want := []string{"alpha/a", "prod/missing", "zeta/z"}
	if !reflect.DeepEqual(got[0].Remediation.Workloads, want) {
		t.Fatalf("marker workloads = %v, want %v", got[0].Remediation.Workloads, want)
	}
}

func TestMarkerRetainsUnresolvedOMEGPUOccupancy(t *testing.T) {
	snap := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		Build()
	snap.Nodes["bad"].OMEPods = []snapshot.PodInfo{{
		Namespace: "prod",
		Name:      "orphaned-ir-pod",
		Node:      "bad",
		GPUs:      1,
	}}

	got := markers(evaluate(t, snap, config.Default()))
	if len(got) != 1 {
		t.Fatalf("markers = %+v, want one", got)
	}
	marker := got[0].Remediation
	if len(marker.Workloads) != 0 {
		t.Fatalf("unresolved occupant invented workload identity: %v", marker.Workloads)
	}
	if !marker.OMEGPUOccupantsPresent {
		t.Fatal("unresolved OME GPU occupant was mistaken for an empty node")
	}
}

func TestMultiBadNodeInstanceProducesOneFindingWithExactSource(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad-b", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("bad-a", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 2, "bad-b", "bad-a")
	got := evaluate(t, b.Build(), config.Default())
	if len(markers(got)) != 2 {
		t.Fatalf("markers = %+v, want one for each unhealthy node", markers(got))
	}
	found := findings(got)
	if len(found) != 1 || found[0].FromNode != "bad-a" || found[0].Instance != 0 {
		t.Fatalf("deduplicated finding = %+v, want one instance from bad-a", found)
	}
	if !found[0].Executable || found[0].FootprintGPUs != 4 {
		t.Fatalf("steady atomic instance should be executable with complete footprint: %+v", found[0])
	}
}

func TestOMENativeAdvisoryMatrix(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*snapshot.ClusterSnapshot, *config.Config)
	}{
		{name: "surface disabled", want: policy.AdvisoryMigrationSurfaceDisabled, mutate: func(_ *snapshot.ClusterSnapshot, cfg *config.Config) {
			*cfg.OMENativeMigrationEnabled = false
		}},
		{name: "executor unavailable", want: policy.AdvisoryOMENativeUnavailable, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			s.OMENativeExecutor.Available = false
		}},
		{name: "invalid observation", want: policy.AdvisoryOMENativeObservationInvalid, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			w := s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			w.Components[v1beta1.EngineComponent].ObservationValid = false
		}},
		{name: "nonsteady instance", want: policy.AdvisoryOMENativeStateIneligible, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			w := s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			w.Components[v1beta1.EngineComponent].Instances[0].Phase = v1beta1.OMENativeInstanceUpdating
		}},
		{name: "workload immovable", want: policy.AdvisoryOMENativeStateIneligible, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}].Movable = false
		}},
		{name: "active migration", want: policy.AdvisoryOMENativeStateIneligible, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}].ActiveMigrations = []snapshot.InFlight{{UUID: "busy"}}
		}},
		{name: "malformed migration", want: policy.AdvisoryOMENativeStateIneligible, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}].MalformedRequests = map[string]string{"bad": "invalid"}
		}},
		{name: "terminating member", want: policy.AdvisoryOMENativeStateIneligible, mutate: func(s *snapshot.ClusterSnapshot, _ *config.Config) {
			w := s.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			w.Components[v1beta1.EngineComponent].Instances[0].Pods[0].Terminating = true
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := oneOMENative(snapshot.NodeHealthUnhealthy)
			cfg := config.Default()
			tt.mutate(snap, cfg)
			got := findings(evaluate(t, snap, cfg))
			if len(got) != 1 || got[0].Executable || got[0].AdvisoryReason != tt.want {
				t.Fatalf("finding = %+v, want one advisory %q", got, tt.want)
			}
		})
	}
}

func TestInvalidOMENativeWithoutResolvableInstanceIsComponentAdvisory(t *testing.T) {
	for _, observationReason := range []string{
		"inference replica is missing",
		"inference replica status is stale",
		"inference replica is duplicated",
	} {
		t.Run(observationReason, func(t *testing.T) {
			b := testutil.NewSnapshot().
				WithNode("bad-b", "h100", 8, testutil.NodeUnhealthy()).
				WithNode("bad-a", "h100", 8, testutil.NodeUnhealthy()).
				WithNode("target", "h100", 8).
				WithMultiPodInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, 1, "bad-b", "bad-a")
			snap := b.Build()
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			comp := w.Components[v1beta1.EngineComponent]
			// Missing, stale, and duplicate IR failures all preserve physical
			// node occupancy but intentionally expose no stable Instance row.
			comp.IR = nil
			comp.StatusFresh = false
			comp.ObservationValid = false
			comp.ObservationReason = observationReason
			comp.Instances = nil
			w.MigrationStateValid = false

			got := evaluate(t, snap, config.Default())
			if len(markers(got)) != 2 {
				t.Fatalf("markers = %+v, want one for each physical unhealthy node", markers(got))
			}
			for _, marker := range markers(got) {
				if !reflect.DeepEqual(marker.Remediation.Workloads, []string{"prod/model"}) {
					t.Fatalf("marker workloads = %v, want physical occupant prod/model", marker.Remediation.Workloads)
				}
			}

			found := findings(got)
			if len(found) != 1 {
				t.Fatalf("findings = %+v, want one truthful component-wide advisory", found)
			}
			candidate := found[0]
			if candidate.Workload.String() != "prod/model" || candidate.Component != v1beta1.EngineComponent ||
				candidate.Instance != policy.ComponentWideInstance || candidate.Mode != constants.OMENative ||
				candidate.FromNode != "bad-a" || candidate.Executable ||
				candidate.AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid ||
				candidate.FootprintGPUs != 0 || len(candidate.HintTargetNodes) != 0 ||
				len(candidate.PlacementTargetNodes) != 0 {
				t.Fatalf("component-wide invalid-observation identity = %+v", candidate)
			}
		})
	}
}

func TestInvalidOMENativeWithResolvableInstancesStaysPerInstance(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 1).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 1)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	w.Components[v1beta1.EngineComponent].ObservationValid = false

	got := findings(evaluate(t, snap, config.Default()))
	if len(got) != 2 || got[0].Instance != 0 || got[1].Instance != 1 {
		t.Fatalf("findings = %+v, want exactly the two resolvable Instance identities", got)
	}
	for _, candidate := range got {
		if candidate.AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid || candidate.Executable {
			t.Fatalf("invalid resolved Instance must remain advisory: %+v", candidate)
		}
	}
}

func TestInvalidEmptyOMENativeInstanceFallsBackToComponentAdvisory(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 1)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	comp := w.Components[v1beta1.EngineComponent]
	comp.ObservationValid = false
	comp.ObservationReason = "OMENative pod identity is invalid"
	// A validated IR status row can survive while the failed Pod join leaves
	// that Instance with no Pods, nodes, or GPU footprint. Node occupancy still
	// retains the physical OME GPU Pod.
	comp.Instances = []*snapshot.Instance{{
		Index:            0,
		Incarnation:      1,
		Phase:            v1beta1.OMENativeInstanceReady,
		Admitted:         true,
		DesiredPods:      1,
		StatusPods:       1,
		ObservationValid: false,
		NodesSet:         map[string]int{},
	}}

	got := findings(evaluate(t, snap, config.Default()))
	if len(got) != 1 || got[0].Instance != policy.ComponentWideInstance ||
		got[0].AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid ||
		got[0].FromNode != "bad" || got[0].FootprintGPUs != 0 {
		t.Fatalf("uncovered physical occupancy = %+v, want one component-wide invalid-observation advisory", got)
	}
}

func TestInvalidOMENativeReportsFallbackOnlyForUncoveredPhysicalPods(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad-a", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("bad-b", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad-b", 1)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	w.Components[v1beta1.EngineComponent].ObservationValid = false
	snap.Nodes["bad-a"].OMEPods = append(snap.Nodes["bad-a"].OMEPods, snapshot.PodInfo{
		Namespace: "prod",
		Name:      "unjoined",
		Node:      "bad-a",
		GPUs:      1,
		ISVC:      w.NamespacedName,
		Component: v1beta1.EngineComponent,
	})

	got := findings(evaluate(t, snap, config.Default()))
	if len(got) != 2 {
		t.Fatalf("findings = %+v, want one resolved Instance advisory plus one fallback for the unjoined Pod", got)
	}
	byInstance := make(map[int32]policy.Candidate, len(got))
	for _, candidate := range got {
		byInstance[candidate.Instance] = candidate
	}
	if byInstance[0].FromNode != "bad-b" || byInstance[0].FootprintGPUs != 1 {
		t.Fatalf("resolved Instance advisory = %+v", byInstance[0])
	}
	fallback, ok := byInstance[policy.ComponentWideInstance]
	if !ok || fallback.FromNode != "bad-a" || fallback.FootprintGPUs != 0 ||
		fallback.AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid {
		t.Fatalf("uncovered-Pod fallback = %+v, present=%t", fallback, ok)
	}
}

func TestRawAndLWSAreInstanceAdvisories(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/raw", v1beta1.EngineComponent, constants.RawDeployment, "bad", 1).
		WithInstance("prod/lws", v1beta1.EngineComponent, constants.MultiNode, "bad", 1)
	got := findings(evaluate(t, b.Build(), config.Default()))
	if len(got) != 2 {
		t.Fatalf("findings = %+v, want Raw and LWS advisories", got)
	}
	reasons := map[constants.DeploymentModeType]string{}
	for _, finding := range got {
		if finding.Executable || finding.Instance != 0 {
			t.Fatalf("unsupported workload finding must be instance-scoped advisory: %+v", finding)
		}
		reasons[finding.Mode] = finding.AdvisoryReason
	}
	if reasons[constants.RawDeployment] != policy.AdvisoryRawDeploymentMigrationUnsupported ||
		reasons[constants.MultiNode] != policy.AdvisoryLWSMigrationUnsupported {
		t.Fatalf("advisory reasons = %v", reasons)
	}
}

func TestLWSRecommendationsDisabledKeepsRemediationMarkerOnly(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/lws", v1beta1.EngineComponent, constants.MultiNode, "bad", 1)
	cfg := config.Default()
	*cfg.LWSRecommendationsEnabled = false

	got := evaluate(t, b.Build(), cfg)
	if len(findings(got)) != 0 {
		t.Fatalf("disabled LWS recommendations produced workload Candidates: %+v", findings(got))
	}
	marker := markers(got)
	if len(marker) != 1 || !reflect.DeepEqual(marker[0].Remediation.Workloads, []string{"prod/lws"}) {
		t.Fatalf("node remediation marker lost physical LWS blocker: %+v", marker)
	}
}

func TestModelAndPVCAdvisories(t *testing.T) {
	key := snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "weights"}
	tests := []struct {
		name  string
		avail *snapshot.ModelAvailability
		want  string
	}{
		{name: "unresolved", avail: &snapshot.ModelAvailability{Key: key, ResolveError: "missing"}, want: policy.AdvisoryModelUnresolved},
		{name: "rwo pinned", avail: &snapshot.ModelAvailability{Key: key, Backend: snapshot.BackendPVC, VolumePinned: true}, want: policy.AdvisoryVolumePinned},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewSnapshot().
				WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
				WithNode("target", "h100", 8).
				WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 1).
				WithModel("prod/model", tt.avail)
			got := findings(evaluate(t, b.Build(), config.Default()))
			if len(got) != 1 || got[0].Executable || got[0].AdvisoryReason != tt.want {
				t.Fatalf("finding = %+v, want %q", got, tt.want)
			}
		})
	}
}

func TestNoSurgeHeadroomIsAdvisory(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("full", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 8)
	b.WithOtherOccupant("full", 1)
	got := findings(evaluate(t, b.Build(), config.Default()))
	if len(got) != 1 || got[0].Executable || got[0].AdvisoryReason != policy.AdvisoryNoSurgeHeadroom {
		t.Fatalf("finding = %+v, want NoSurgeHeadroom advisory", got)
	}
}

func TestTargetHintsExcludeEveryNonclearHealthState(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("a-unknown", "h100", 8, testutil.NodeUnknown()).
		WithNode("b-suspect", "h100", 8, testutil.NodeSuspect()).
		WithNode("c-clear", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "bad", 1)
	b.WithOtherOccupant("a-unknown", 7)
	b.WithOtherOccupant("b-suspect", 7)
	b.WithOtherOccupant("c-clear", 7)
	got := findings(evaluate(t, b.Build(), config.Default()))
	if len(got) != 1 || !got[0].Executable || !reflect.DeepEqual(got[0].HintTargetNodes, []string{"c-clear"}) {
		t.Fatalf("finding = %+v, want only clear target hint", got)
	}
}

func TestFindingOrderUsesExecutabilityPriorityFootprintAndIdentity(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("bad-a", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("bad-b", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("bad-c", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("bad-raw", "h100", 8, testutil.NodeUnhealthy()).
		WithNode("target", "h100", 8).
		WithInstance("prod/z-low", v1beta1.EngineComponent, constants.OMENative, "bad-a", 1).
		WithInstance("prod/b-high-large", v1beta1.EngineComponent, constants.OMENative, "bad-b", 2).
		WithInstance("prod/a-high-small", v1beta1.EngineComponent, constants.OMENative, "bad-c", 1).
		WithInstance("prod/raw", v1beta1.EngineComponent, constants.RawDeployment, "bad-raw", 1)
	b.ConfigureWorkload("prod/z-low", func(w *snapshot.Workload) { w.Priority = 0.2 })
	b.ConfigureWorkload("prod/b-high-large", func(w *snapshot.Workload) { w.Priority = 0.9 })
	b.ConfigureWorkload("prod/a-high-small", func(w *snapshot.Workload) { w.Priority = 0.9 })
	b.ConfigureWorkload("prod/raw", func(w *snapshot.Workload) { w.Priority = 1.0 })
	got := findings(evaluate(t, b.Build(), config.Default()))
	if len(got) != 4 {
		t.Fatalf("findings = %+v, want four", got)
	}
	want := []string{"prod/a-high-small", "prod/b-high-large", "prod/z-low", "prod/raw"}
	var names []string
	for _, finding := range got {
		names = append(names, finding.Workload.String())
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("finding order = %v, want %v", names, want)
	}
	if got[0].Score != 0.9 || got[1].Score != 0.9 || got[2].Score != 0.2 || got[3].Score != 1.0 {
		t.Fatalf("scores must carry numeric workload priority: %+v", got)
	}
}

func TestCooldownAndMaintenanceDoNotSuppressHealthFinding(t *testing.T) {
	for _, age := range []time.Duration{3 * time.Minute, 10 * time.Minute} {
		t.Run(age.String(), func(t *testing.T) {
			snap := oneOMENative(snapshot.NodeHealthUnhealthy)
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			last := snap.Timestamp.Add(-age)
			w.LastMigration = &last
			cfg := config.Default()
			*cfg.Policies.Defragmentation.FragmentationThreshold = 1
			cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Fri"}, Start: "00:00", End: "00:01"}}
			got := findings(evaluate(t, snap, cfg))
			if len(got) != 1 || !got[0].Executable {
				t.Fatalf("health policy must leave cooldown to Arbiter at age %s: %+v", age, got)
			}
		})
	}
}
