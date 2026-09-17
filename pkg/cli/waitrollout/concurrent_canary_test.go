package waitrollout

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	report "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func pinnedConcurrentCanaries(t *testing.T, routerPhase ome.RolloutPhase) *ome.InferenceService {
	t.Helper()
	v := independent()
	v.Spec.Router = &ome.RouterSpec{}
	ordering := ome.RolloutGroupOrderingConcurrent
	v.Spec.Rollout = &ome.RolloutSpec{GroupOrdering: &ordering, Groups: []ome.RolloutGroup{
		{Components: []ome.ComponentType{ome.RouterComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}},
		{Components: []ome.ComponentType{ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}},
	}}
	engine := ome.ComponentStatusSpec{
		RolloutPhase: ome.RolloutPhaseStable, LatestRolledoutRevision: "chat-engine-rev-bbbbbbbb",
		Traffic: []ome.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100}},
		Canary:  &ome.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", CurrentStep: 1, ObservedTrafficWeight: 100},
	}
	router := ome.ComponentStatusSpec{
		RolloutPhase: routerPhase, LatestRolledoutRevision: "chat-router-rev-cccccccc",
		Traffic: []ome.ComponentTrafficTarget{{RevisionName: "chat-router-rev-cccccccc", Percent: 100}},
		Canary:  &ome.CanaryStatus{StableRevisionHash: "cccccccc", CanaryRevisionHash: "dddddddd", CurrentStep: 0},
	}
	if routerPhase == ome.RolloutPhaseStable {
		router.LatestRolledoutRevision = "chat-router-rev-dddddddd"
		router.Traffic[0].RevisionName = "chat-router-rev-dddddddd"
		router.Canary = &ome.CanaryStatus{CanaryRevisionHash: "dddddddd", CurrentStep: 1, ObservedTrafficWeight: 100}
	}
	if routerPhase == ome.RolloutPhaseRolledBack {
		router.Canary.RolledBackRevisionHash = "dddddddd"
	}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: engine, ome.RouterComponent: router}
	v.Status.Canary = router.Canary.DeepCopy() // Legacy alias names only the router.
	stamp := metav1.NewTime(now.Add(-10 * time.Second))
	run := &ome.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: stamp, PinnedAt: stamp,
		TargetRevisions: []ome.RolloutRunTarget{{Component: ome.RouterComponent, Revision: "dddddddd"}, {Component: ome.EngineComponent, Revision: "bbbbbbbb"}},
	}
	for _, group := range v.Spec.Rollout.Groups {
		digest, err := rolloutpolicy.ProgressionDigest(&group)
		require.NoError(t, err)
		run.Plan.Groups = append(run.Plan.Groups, ome.RolloutRunGroup{Source: ome.RolloutPlanSourceInline, PortableDigest: digest, Group: group})
	}
	v.Status.Rollout = &ome.RolloutStatus{ActiveRun: run}
	return v
}

func TestPinnedConcurrentCanariesMatchReportedTerminalPredicates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		phase     ome.RolloutPhase
		requested report.WaitRequested
		state     report.RolloutState
	}{
		{"stable", ome.RolloutPhaseStable, report.WaitRequestedRolloutStable, report.RolloutStateSucceeded},
		{"failed", ome.RolloutPhaseFailed, report.WaitRequestedRolloutFailed, report.RolloutStateFailed},
		{"rolledback", ome.RolloutPhaseRolledBack, report.WaitRequestedRolloutRolledBack, report.RolloutStateRolledBack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := pinnedConcurrentCanaries(t, tc.phase)
			before := v.DeepCopy()
			decision, observed, err := Evaluate(v, tc.requested, now)
			require.NoError(t, err)
			require.True(t, decision.Matched, "%+v", observed)
			require.Equal(t, "Valid", observed.Validity)
			require.Equal(t, tc.state, observed.Summary.ReportedState)
			require.Equal(t, report.RolloutStateUnknown, observed.Summary.State)
			require.Equal(t, report.RolloutEpochUnverifiable, observed.Summary.Epoch)
			require.Equal(t, 2, observed.Inspection.PinnedGroups)
			require.Equal(t, 2, observed.Inspection.Targets)
			require.Equal(t, before, v)
		})
	}
}

func TestPinnedConcurrentCanariesNeverBorrowAliasOrIgnoreSiblingCorruption(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*ome.InferenceService)
		valid string
	}{
		{"missing failed unit", func(v *ome.InferenceService) {
			component := v.Status.Components[ome.RouterComponent]
			component.Canary = nil // Global alias still points to the router.
			v.Status.Components[ome.RouterComponent] = component
		}, "Unavailable"},
		{"malformed stable sibling", func(v *ome.InferenceService) {
			component := v.Status.Components[ome.EngineComponent]
			component.Canary.CanaryRevisionHash = "not-a-hash"
			v.Status.Components[ome.EngineComponent] = component
		}, "Invalid"},
		{"invalid pinned digest", func(v *ome.InferenceService) {
			v.Status.Rollout.ActiveRun.Plan.Groups[1].PortableDigest = "not-the-plan"
		}, "Invalid"},
		{"missing concurrent declaration", func(v *ome.InferenceService) {
			v.Spec.Rollout.GroupOrdering = nil
		}, "Invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := pinnedConcurrentCanaries(t, ome.RolloutPhaseFailed)
			tc.edit(v)
			decision, observed, err := Evaluate(v, report.WaitRequestedRolloutFailed, now)
			require.NoError(t, err)
			require.False(t, decision.Matched)
			require.Equal(t, tc.valid, observed.Validity)
		})
	}
}
