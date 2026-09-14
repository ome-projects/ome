package canaryevidence_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestPrimaryUsesControllerPriorityAndRejectsAmbiguity(t *testing.T) {
	tests := []struct {
		name       string
		components []omev1beta1.ComponentType
		want       omev1beta1.ComponentType
		valid      bool
	}{
		{name: "router", components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent, omev1beta1.RouterComponent}, want: omev1beta1.RouterComponent, valid: true},
		{name: "engine", components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent}, want: omev1beta1.EngineComponent, valid: true},
		{name: "decoder", components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}, want: omev1beta1.DecoderComponent, valid: true},
		{name: "empty", valid: false},
		{name: "duplicate", components: []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.EngineComponent}, want: omev1beta1.EngineComponent, valid: false},
		{name: "unknown", components: []omev1beta1.ComponentType{"secret-component"}, valid: false},
		{name: "known and unknown", components: []omev1beta1.ComponentType{omev1beta1.EngineComponent, "secret-component"}, want: omev1beta1.EngineComponent, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := canaryevidence.Primary(tt.components)
			assert.Equal(t, tt.valid, valid)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProjectPhaseAndBindingsAreClosed(t *testing.T) {
	tests := []struct {
		api          omev1beta1.RolloutPhase
		report       reportv1alpha1.RolloutPhase
		needsStatus  bool
		bindsTraffic bool
		bindsStep    bool
	}{
		{omev1beta1.RolloutPhaseStable, reportv1alpha1.RolloutPhaseStable, false, false, false},
		{omev1beta1.RolloutPhaseCanarying, reportv1alpha1.RolloutPhaseCanarying, true, true, true},
		{omev1beta1.RolloutPhaseBlueGreenStandby, reportv1alpha1.RolloutPhaseBlueGreenStandby, false, false, false},
		{omev1beta1.RolloutPhasePending, reportv1alpha1.RolloutPhasePending, true, false, false},
		{omev1beta1.RolloutPhasePaused, reportv1alpha1.RolloutPhasePaused, true, true, true},
		{omev1beta1.RolloutPhasePromoting, reportv1alpha1.RolloutPhasePromoting, true, true, true},
		{omev1beta1.RolloutPhaseRollingBack, reportv1alpha1.RolloutPhaseRollingBack, true, true, false},
		{omev1beta1.RolloutPhaseRolledBack, reportv1alpha1.RolloutPhaseRolledBack, true, true, false},
		{omev1beta1.RolloutPhaseFailed, reportv1alpha1.RolloutPhaseFailed, true, false, false},
		{"SECRET_PHASE", reportv1alpha1.RolloutPhaseUnknown, false, false, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.api), func(t *testing.T) {
			projected := canaryevidence.ProjectPhase(tt.api)
			assert.Equal(t, tt.report, projected)
			assert.Equal(t, tt.needsStatus, canaryevidence.PhaseNeedsStatus(projected))
			assert.Equal(t, tt.bindsTraffic, canaryevidence.PhaseBindsTraffic(projected))
			assert.Equal(t, tt.bindsStep, canaryevidence.PhaseBindsStepTraffic(projected))
		})
	}
}

func TestRevisionHashAcceptsOnlyCanonicalServiceNames(t *testing.T) {
	assert.True(t, canaryevidence.SafeRevisionHash("deadbeef"))
	assert.False(t, canaryevidence.SafeRevisionHash("DEADBEEF"))
	assert.Equal(t, "deadbeef", canaryevidence.RevisionHash("chat", omev1beta1.EngineComponent, "chat-engine-rev-deadbeef"))
	assert.Empty(t, canaryevidence.RevisionHash("chat", omev1beta1.EngineComponent, "chat-router-rev-deadbeef"))
	assert.Empty(t, canaryevidence.RevisionHash("chat", omev1beta1.EngineComponent, "short"))

	owner := strings.Repeat("long-", 20) + "chat"
	raw := owner + "-engine-rev-deadbeef"
	bounded := constants.TruncateNameWithMaxLength(raw, validation.DNS1035LabelMaxLength)
	require.Len(t, bounded, validation.DNS1035LabelMaxLength)
	assert.Equal(t, "deadbeef", canaryevidence.RevisionHash(owner, omev1beta1.EngineComponent, bounded))
}

func TestObservedTrafficAndPhaseResidue(t *testing.T) {
	duration := metav1.Duration{}
	steps := []omev1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 20, Pause: &omev1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &omev1beta1.RolloutPause{Duration: &duration}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}

	tests := []struct {
		name    string
		phase   reportv1alpha1.RolloutPhase
		status  *omev1beta1.CanaryStatus
		traffic bool
		residue bool
	}{
		{name: "nil", phase: reportv1alpha1.RolloutPhaseCanarying, traffic: false, residue: false},
		{name: "current", phase: reportv1alpha1.RolloutPhasePaused, status: canaryStatus(1, 50), traffic: true, residue: true},
		{name: "one write advance", phase: reportv1alpha1.RolloutPhaseCanarying, status: canaryStatus(1, 20), traffic: true, residue: true},
		{name: "wrong step traffic", phase: reportv1alpha1.RolloutPhasePaused, status: canaryStatus(1, 20), traffic: false, residue: true},
		{name: "paused final step", phase: reportv1alpha1.RolloutPhasePaused, status: canaryStatus(2, 100), traffic: true, residue: false},
		{name: "promoting requires full traffic", phase: reportv1alpha1.RolloutPhasePromoting, status: canaryStatus(1, 50), traffic: false, residue: false},
		{name: "promoting terminal step requires full traffic", phase: reportv1alpha1.RolloutPhasePromoting, status: canaryStatus(2, 80), traffic: false, residue: false},
		{name: "canarying final without residue", phase: reportv1alpha1.RolloutPhaseCanarying, status: canaryStatus(2, 100), traffic: true, residue: false},
		{name: "manual promotion identity", phase: reportv1alpha1.RolloutPhaseCanarying, status: promotedStatus(1, 20, "bbbbbbbb"), traffic: true, residue: true},
		{name: "wrong manual promotion identity", phase: reportv1alpha1.RolloutPhaseCanarying, status: promotedStatus(1, 20, "cccccccc"), traffic: true, residue: false},
		{name: "rollback residue outside rollback", phase: reportv1alpha1.RolloutPhasePending, status: rolledBackStatus(0), traffic: true, residue: false},
		{name: "repin hold pending needs run proof", phase: reportv1alpha1.RolloutPhasePending, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold paused needs run proof", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold failed needs run proof", phase: reportv1alpha1.RolloutPhaseFailed, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold canarying needs run proof", phase: reportv1alpha1.RolloutPhaseCanarying, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold cannot remain promoting", phase: reportv1alpha1.RolloutPhasePromoting, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold cannot remain stable", phase: reportv1alpha1.RolloutPhaseStable, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold cannot use blue green phase", phase: reportv1alpha1.RolloutPhaseBlueGreenStandby, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold cannot use unknown phase", phase: reportv1alpha1.RolloutPhaseUnknown, status: heldStatus(2, 30), traffic: false, residue: false},
		{name: "repin hold must precede target traffic", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(1, 50), traffic: false, residue: false},
		{name: "repin hold cannot reduce target traffic", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(0, 30), traffic: false, residue: false},
		{name: "repin hold rejects negative observed traffic", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(2, -1), traffic: false, residue: false},
		{name: "repin hold rejects out of range observed traffic", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(2, 101), traffic: false, residue: false},
		{name: "repin hold rejects missing current step", phase: reportv1alpha1.RolloutPhasePaused, status: heldStatus(3, 30), traffic: false, residue: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.traffic, canaryevidence.ObservedTrafficMatchesStep(tt.phase, steps, tt.status))
			assert.Equal(t, tt.residue, canaryevidence.ValidPhaseStepResidue(tt.phase, steps, tt.status))
		})
	}
}

func TestPreStepHoldCannotBeQualifiedWithoutRunEvidence(t *testing.T) {
	steps := []omev1beta1.RolloutGroupStep{{
		Capacity: intstr.FromString("100%"), Traffic: 100,
	}}
	status := heldStatus(0, 0)
	status.RolledBackRevisionHash = status.CanaryRevisionHash

	for _, phase := range []reportv1alpha1.RolloutPhase{
		reportv1alpha1.RolloutPhaseRollingBack,
		reportv1alpha1.RolloutPhaseRolledBack,
	} {
		t.Run(string(phase), func(t *testing.T) {
			assert.False(t, canaryevidence.ObservedTrafficMatchesStep(phase, steps, status))
			assert.False(t, canaryevidence.ValidPhaseStepResidue(phase, steps, status))
		})
	}
}

func TestPreStepHoldPromotionResidueNeedsRunEvidence(t *testing.T) {
	steps := []omev1beta1.RolloutGroupStep{{
		Capacity: intstr.FromString("100%"), Traffic: 100,
	}}
	status := heldStatus(0, 20)
	// advanceStep records any live promote annotation, including when an
	// automatic gate caused the advance. A later shorter-plan repin can clamp
	// that advanced index back to zero without clearing the durable record.
	status.PromotedThrough = "old-opaque-command"

	assert.False(t, canaryevidence.ValidPhaseStepResidue(
		reportv1alpha1.RolloutPhaseCanarying, steps, status,
	))

	status.PreStepHold = false
	assert.False(t, canaryevidence.ValidPhaseStepResidue(
		reportv1alpha1.RolloutPhaseCanarying, steps, status,
	))
}

func TestPausedNonRaisingRepinBoundaryRequiresBoundEpoch(t *testing.T) {
	baseSteps := []omev1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 10},
		{Capacity: intstr.FromString("50%"), Traffic: 30},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	tests := []struct {
		name   string
		phase  reportv1alpha1.RolloutPhase
		mutate func(*omev1beta1.InferenceService, *omev1beta1.CanaryStatus, []omev1beta1.RolloutGroupStep)
		valid  bool
	}{
		{name: "bound lowering repin", phase: reportv1alpha1.RolloutPhasePaused, valid: true},
		{name: "freeze is a global pause", phase: reportv1alpha1.RolloutPhasePaused, valid: true, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Annotations[constants.PausedRolloutAnnotation] = constants.PausedRolloutFreezeValue
		}},
		{name: "pause missing", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			delete(isvc.Annotations, constants.PausedRolloutAnnotation)
		}},
		{name: "pause value unrecognized", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Annotations[constants.PausedRolloutAnnotation] = "True"
		}},
		{name: "initial pin is not a repin", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = isvc.Status.Rollout.ActiveRun.OpenedAt
		}},
		{name: "pin does not postdate step", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, status *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = *status.StepEnteredTime
		}},
		{name: "run identity malformed", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun.RunID = "chat-not-a-run"
		}},
		{name: "target epoch differs", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = "cccccccc"
		}},
		{name: "repinned components may be reordered", phase: reportv1alpha1.RolloutPhasePaused, valid: true, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			run := isvc.Status.Rollout.ActiveRun
			run.TargetRevisions = []omev1beta1.RolloutRunTarget{
				{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb"},
				{Component: omev1beta1.DecoderComponent, Revision: "cccccccc"},
			}
			pinned := &run.Plan.Groups[0]
			pinned.Group.Components = []omev1beta1.ComponentType{
				omev1beta1.DecoderComponent, omev1beta1.EngineComponent,
			}
			digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
			require.NoError(t, err)
			pinned.PortableDigest = digest
		}},
		{name: "pinned digest differs", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "rp1:000000000000"
		}},
		{name: "digest-correct plan is controller-invalid", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, projected []omev1beta1.RolloutGroupStep) {
			projected[2].Traffic = 80
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Steps[2].Traffic = 80
			digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
			require.NoError(t, err)
			pinned.PortableDigest = digest
		}},
		{name: "raising repin uses pre-step hold", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, projected []omev1beta1.RolloutGroupStep) {
			projected[1].Traffic = 60
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[1].Traffic = 60
		}},
		{name: "pre-step hold has its own validation", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(_ *omev1beta1.InferenceService, status *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			status.PreStepHold = true
		}},
		{name: "rollback residue contradicts phase", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(_ *omev1beta1.InferenceService, status *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			status.RolledBackRevisionHash = status.CanaryRevisionHash
		}},
		{name: "promoting requires full observed traffic", phase: reportv1alpha1.RolloutPhasePromoting},
		{name: "phase cannot be preserved evidence", phase: reportv1alpha1.RolloutPhasePending},
		{name: "projected steps differ from pin", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(_ *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, projected []omev1beta1.RolloutGroupStep) {
			projected[0].Traffic = 5
		}},
		{name: "active run missing", phase: reportv1alpha1.RolloutPhasePaused, mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.CanaryStatus, _ []omev1beta1.RolloutGroupStep) {
			isvc.Status.Rollout.ActiveRun = nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectedSteps := append([]omev1beta1.RolloutGroupStep{}, baseSteps...)
			pinnedSteps := append([]omev1beta1.RolloutGroupStep{}, baseSteps...)
			opened := metav1.NewTime(time.Date(2026, time.September, 14, 16, 40, 0, 0, time.UTC))
			entered := metav1.NewTime(time.Date(2026, time.September, 14, 16, 45, 0, 0, time.UTC))
			pinned := metav1.NewTime(time.Date(2026, time.September, 14, 16, 50, 0, 0, time.UTC))
			oldGroup := omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 20},
					{Capacity: intstr.FromString("50%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}
			oldDigest, digestErr := rolloutpolicy.ProgressionDigest(&oldGroup)
			require.NoError(t, digestErr)
			pinnedGroup := omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary:     &omev1beta1.GroupCanary{Steps: pinnedSteps},
			}
			pinnedDigest, digestErr := rolloutpolicy.ProgressionDigest(&pinnedGroup)
			require.NoError(t, digestErr)
			runIdentity := "engine=bbbbbbbb;" + oldDigest + opened.Time.UTC().Format(time.RFC3339)
			status := &omev1beta1.CanaryStatus{
				StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb",
				CurrentStep: 1, ObservedTrafficWeight: 50, StepEnteredTime: &entered,
			}
			mode := constants.OMENative
			isvc := &omev1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "chat", Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"},
				},
				Spec: omev1beta1.InferenceServiceSpec{
					DeploymentMode: &mode,
					Engine:         &omev1beta1.EngineSpec{},
					Decoder:        &omev1beta1.DecoderSpec{},
				},
				Status: omev1beta1.InferenceServiceStatus{
					Canary: status,
					Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
						omev1beta1.EngineComponent: {
							Traffic: []omev1beta1.ComponentTrafficTarget{
								{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50},
								{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 50},
							},
						},
					},
					Rollout: &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
						RunID: "chat-" + rolloutpolicy.ShortHash([]byte(runIdentity)), OpenedAt: opened, PinnedAt: pinned,
						TargetRevisions: []omev1beta1.RolloutRunTarget{{
							Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb",
						}},
						Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
							Source:         omev1beta1.RolloutPlanSourceInline,
							PortableDigest: pinnedDigest,
							Group:          pinnedGroup,
						}}},
					}},
				},
			}
			if tt.mutate != nil {
				tt.mutate(isvc, status, projectedSteps)
			}

			assert.Equal(t, tt.valid, canaryevidence.ValidRepinBoundary(
				isvc, omev1beta1.EngineComponent, tt.phase, projectedSteps, status,
				isvc.Status.Components[omev1beta1.EngineComponent].Traffic,
			))
		})
	}
}

func TestPreStepHoldBindsTypedTrafficInEverySupportedPhase(t *testing.T) {
	status := heldStatus(2, 30)
	traffic := []omev1beta1.ComponentTrafficTarget{
		{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 70},
		{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 30},
	}
	for _, phase := range []reportv1alpha1.RolloutPhase{
		reportv1alpha1.RolloutPhasePending,
		reportv1alpha1.RolloutPhaseCanarying,
		reportv1alpha1.RolloutPhasePaused,
		reportv1alpha1.RolloutPhaseFailed,
	} {
		t.Run(string(phase), func(t *testing.T) {
			assert.True(t, canaryevidence.StatusBindsTraffic(phase, status))
			assert.True(t, canaryevidence.ActiveTrafficMatches(
				"chat", omev1beta1.EngineComponent, phase, status, traffic,
			))
		})
	}

	assert.False(t, canaryevidence.StatusBindsTraffic(reportv1alpha1.RolloutPhasePending, canaryStatus(0, 20)))
	assert.False(t, canaryevidence.ActiveTrafficMatches(
		"chat", omev1beta1.EngineComponent, reportv1alpha1.RolloutPhasePending,
		status, nil,
	))
	assert.False(t, canaryevidence.ActiveTrafficMatches(
		"chat", omev1beta1.EngineComponent, reportv1alpha1.RolloutPhasePending,
		status, []omev1beta1.ComponentTrafficTarget{
			{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 60},
			{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 40},
		},
	))
}

func TestActiveTrafficMatchesOneExactEpoch(t *testing.T) {
	status := canaryStatus(0, 20)
	valid := []omev1beta1.ComponentTrafficTarget{
		{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 80},
		{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 20},
	}
	assert.True(t, canaryevidence.ActiveTrafficMatches("chat", omev1beta1.EngineComponent, reportv1alpha1.RolloutPhaseCanarying, status, valid))

	tests := []struct {
		name   string
		mutate func(*omev1beta1.CanaryStatus, []omev1beta1.ComponentTrafficTarget) []omev1beta1.ComponentTrafficTarget
	}{
		{name: "different canary epoch", mutate: func(status *omev1beta1.CanaryStatus, traffic []omev1beta1.ComponentTrafficTarget) []omev1beta1.ComponentTrafficTarget {
			status.CanaryRevisionHash = "cccccccc"
			return traffic
		}},
		{name: "wrong weight", mutate: func(_ *omev1beta1.CanaryStatus, traffic []omev1beta1.ComponentTrafficTarget) []omev1beta1.ComponentTrafficTarget {
			traffic[1].Percent = 25
			return traffic
		}},
		{name: "duplicate", mutate: func(_ *omev1beta1.CanaryStatus, traffic []omev1beta1.ComponentTrafficTarget) []omev1beta1.ComponentTrafficTarget {
			traffic[1].RevisionName = traffic[0].RevisionName
			return traffic
		}},
		{name: "malformed revision", mutate: func(_ *omev1beta1.CanaryStatus, traffic []omev1beta1.ComponentTrafficTarget) []omev1beta1.ComponentTrafficTarget {
			traffic[1].RevisionName = "SECRET_REVISION"
			return traffic
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidateStatus := *status
			candidateTraffic := append([]omev1beta1.ComponentTrafficTarget(nil), valid...)
			candidateTraffic = tt.mutate(&candidateStatus, candidateTraffic)
			assert.False(t, canaryevidence.ActiveTrafficMatches(
				"chat", omev1beta1.EngineComponent, reportv1alpha1.RolloutPhaseCanarying,
				&candidateStatus, candidateTraffic,
			))
		})
	}

	rollback := canaryStatus(0, 0)
	rollback.RolledBackRevisionHash = rollback.CanaryRevisionHash
	assert.True(t, canaryevidence.ActiveTrafficMatches(
		"chat", omev1beta1.EngineComponent, reportv1alpha1.RolloutPhaseRollingBack,
		rollback, []omev1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100}},
	))
}

func TestCompletedStatusMatchesSentinelAndTrafficTogether(t *testing.T) {
	steps := []omev1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 20},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	status := &omev1beta1.CanaryStatus{
		CanaryRevisionHash: "bbbbbbbb", CurrentStep: 2, ObservedTrafficWeight: 100,
	}
	traffic := []omev1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100}}

	assert.True(t, canaryevidence.ValidCompletedStep(steps[1]))
	assert.True(t, canaryevidence.CompletedTrafficMatches("chat", omev1beta1.EngineComponent, "bbbbbbbb", traffic))
	assert.True(t, canaryevidence.CompletedStatusMatches("chat", omev1beta1.EngineComponent, steps, status, traffic))
	held := *status
	held.PreStepHold = true
	assert.False(t, canaryevidence.CompletedStatusMatches("chat", omev1beta1.EngineComponent, steps, &held, traffic))

	active := *status
	active.CurrentStep = 0
	active.StableRevisionHash = "aaaaaaaa"
	assert.False(t, canaryevidence.CompletedStatusMatches("chat", omev1beta1.EngineComponent, steps, &active, traffic))
	assert.False(t, canaryevidence.CompletedStatusMatches("chat", omev1beta1.EngineComponent, nil, status, traffic))
	assert.False(t, canaryevidence.CompletedStatusMatches("chat", omev1beta1.EngineComponent, steps, nil, traffic))
	assert.False(t, canaryevidence.CompletedTrafficMatches("chat", omev1beta1.EngineComponent, "bbbbbbbb", append(traffic, traffic...)))

	assert.True(t, canaryevidence.ValidCompletedStep(omev1beta1.RolloutGroupStep{Capacity: intstr.FromInt(3), Traffic: 100}))
	assert.False(t, canaryevidence.ValidCompletedStep(omev1beta1.RolloutGroupStep{Capacity: intstr.FromInt(0), Traffic: 100}))
	assert.False(t, canaryevidence.ValidCompletedStep(omev1beta1.RolloutGroupStep{Capacity: intstr.FromString("50%"), Traffic: 100}))
	assert.False(t, canaryevidence.ValidCompletedStep(omev1beta1.RolloutGroupStep{Capacity: intstr.FromString("SECRET"), Traffic: 100}))
}

func canaryStatus(step, traffic int32) *omev1beta1.CanaryStatus {
	return &omev1beta1.CanaryStatus{
		StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb",
		CurrentStep: step, ObservedTrafficWeight: traffic,
	}
}

func heldStatus(step, traffic int32) *omev1beta1.CanaryStatus {
	status := canaryStatus(step, traffic)
	status.PreStepHold = true
	return status
}

func promotedStatus(step, traffic int32, promotedThrough string) *omev1beta1.CanaryStatus {
	status := canaryStatus(step, traffic)
	status.PromotedThrough = promotedThrough
	return status
}

func rolledBackStatus(step int32) *omev1beta1.CanaryStatus {
	status := canaryStatus(step, 20)
	status.RolledBackRevisionHash = status.CanaryRevisionHash
	return status
}
