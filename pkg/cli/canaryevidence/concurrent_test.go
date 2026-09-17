package canaryevidence_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestActivePinnedTrafficMatchesSecondaryOnlyInConcurrentUnit(t *testing.T) {
	mode := constants.OMENative
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	router := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	engine := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	routerDigest, err := rolloutpolicy.ProgressionDigest(&router)
	require.NoError(t, err)
	engineDigest, err := rolloutpolicy.ProgressionDigest(&engine)
	require.NoError(t, err)
	now := metav1.NewTime(time.Date(2026, 9, 17, 20, 0, 0, 0, time.UTC))
	isvc := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat"},
		Spec: omev1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Router:         &omev1beta1.RouterSpec{}, Engine: &omev1beta1.EngineSpec{}, Decoder: &omev1beta1.DecoderSpec{},
			Rollout: &omev1beta1.RolloutSpec{GroupOrdering: &ordering, Groups: []omev1beta1.RolloutGroup{router, engine}},
		},
		Status: omev1beta1.InferenceServiceStatus{Rollout: &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
			RunID: "chat-0123456789ab", OpenedAt: now, PinnedAt: now,
			Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{
				{Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: routerDigest, Group: router},
				{Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: engineDigest, Group: engine},
			}},
			TargetRevisions: []omev1beta1.RolloutRunTarget{
				{Component: omev1beta1.RouterComponent, Revision: "dddddddd", StableRevision: "cccccccc"},
				{Component: omev1beta1.EngineComponent, Revision: "aaaaaaaa", StableRevision: "aaaaaaaa"},
				{Component: omev1beta1.DecoderComponent, Revision: "bbbbbbbb", StableRevision: "cccccccc"},
			},
		}}},
	}
	status := &omev1beta1.CanaryStatus{
		StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "aaaaaaaa", ObservedTrafficWeight: 50,
		TargetID: "ct1:" + rolloutpolicy.ShortHash([]byte("decoder=bbbbbbbb;engine=aaaaaaaa")),
	}
	traffic := []omev1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100}}

	assert.True(t, canaryevidence.ActivePinnedTrafficMatches(
		isvc, omev1beta1.EngineComponent, reportv1alpha1.RolloutPhasePaused, status, traffic,
	))
	status.TargetID = "ct1:000000000000"
	assert.False(t, canaryevidence.ActivePinnedTrafficMatches(
		isvc, omev1beta1.EngineComponent, reportv1alpha1.RolloutPhasePaused, status, traffic,
	))
}
