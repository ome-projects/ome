package canaryevidence_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
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
		{name: "promoting before final", phase: reportv1alpha1.RolloutPhasePromoting, status: canaryStatus(1, 50), traffic: true, residue: false},
		{name: "canarying final without residue", phase: reportv1alpha1.RolloutPhaseCanarying, status: canaryStatus(2, 100), traffic: true, residue: false},
		{name: "manual promotion identity", phase: reportv1alpha1.RolloutPhaseCanarying, status: promotedStatus(1, 20, "bbbbbbbb"), traffic: true, residue: true},
		{name: "wrong manual promotion identity", phase: reportv1alpha1.RolloutPhaseCanarying, status: promotedStatus(1, 20, "cccccccc"), traffic: true, residue: false},
		{name: "rollback residue outside rollback", phase: reportv1alpha1.RolloutPhasePending, status: rolledBackStatus(0), traffic: true, residue: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.traffic, canaryevidence.ObservedTrafficMatchesStep(tt.phase, steps, tt.status))
			assert.Equal(t, tt.residue, canaryevidence.ValidPhaseStepResidue(tt.phase, steps, tt.status))
		})
	}
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
