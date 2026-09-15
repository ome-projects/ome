package rolloutprojection_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func secondaryOnlyInferenceService(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	v := multiComponentCanaryInferenceService()
	v.Spec.Decoder = nil
	g := &v.Spec.Rollout.Groups[0]
	g.Components = []omev1beta1.ComponentType{omev1beta1.RouterComponent, omev1beta1.EngineComponent}
	g.Canary.Steps[0].Pause = &omev1beta1.RolloutPause{}
	digest, err := rolloutpolicy.ProgressionDigest(g)
	require.NoError(t, err)
	now := fixedClock().Now()
	v.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: metav1.NewTime(now.Add(-time.Minute)), PinnedAt: metav1.NewTime(now.Add(-30 * time.Second)), Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: *g.DeepCopy()}}}, TargetRevisions: []omev1beta1.RolloutRunTarget{{Component: omev1beta1.RouterComponent, Revision: "aaaaaaaa", StableRevision: "aaaaaaaa"}, {Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "cccccccc"}}}}
	v.Status.Canary.CanaryRevisionHash = "aaaaaaaa"
	v.Status.Canary.TargetID = "ct1:" + rolloutpolicy.ShortHash([]byte("engine=bbbbbbbb;router=aaaaaaaa"))
	x := metav1.NewTime(now.Add(-20 * time.Second))
	v.Status.Canary.StepEnteredTime = &x
	v.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{omev1beta1.EngineComponent: {}, omev1beta1.RouterComponent: {RolloutPhase: omev1beta1.RolloutPhasePaused, Traffic: []omev1beta1.ComponentTrafficTarget{{RevisionName: "chat-router-rev-aaaaaaaa", Percent: 100}}}}
	return v
}

func TestProjectSecondaryOnlyCanaryRequiresCompletePinnedProof(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*omev1beta1.InferenceService)
		valid bool
	}{
		{"actor shape", func(*omev1beta1.InferenceService) {}, true},
		{"unbound", func(v *omev1beta1.InferenceService) { v.Status.Rollout = nil }, false},
		{"unchanged secondary", func(v *omev1beta1.InferenceService) {
			v.Status.Rollout.ActiveRun.TargetRevisions[1].StableRevision = "bbbbbbbb"
		}, false},
		{"wrong target id", func(v *omev1beta1.InferenceService) { v.Status.Canary.TargetID = "ct1:000000000000" }, false},
		{"wrong primary stable", func(v *omev1beta1.InferenceService) {
			v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "dddddddd"
		}, false},
		{"wrong digest", func(v *omev1beta1.InferenceService) { v.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "bad" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := secondaryOnlyInferenceService(t)
			tc.edit(v)
			projected, err := rolloutprojection.Project(v, fixedClock())
			require.NoError(t, err)
			malformed := false
			for _, issue := range projected.Content.Issues {
				if issue.Code != reportv1alpha1.RolloutIssueEpochUnverifiable {
					malformed = true
				}
			}
			require.Equal(t, !tc.valid, malformed)
		})
	}
}
