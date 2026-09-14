package pinnedevidence_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestValidCanaryRepinBindsPlanTargetAndEpoch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService, *metav1.Time)
		valid  bool
	}{
		{name: "complete repin evidence", valid: true},
		{name: "initial pin", mutate: func(isvc *omev1beta1.InferenceService, _ *metav1.Time) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = isvc.Status.Rollout.ActiveRun.OpenedAt
		}},
		{name: "pin at step entry", mutate: func(isvc *omev1beta1.InferenceService, entered *metav1.Time) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = *entered
		}},
		{name: "step entry missing", mutate: func(_ *omev1beta1.InferenceService, entered *metav1.Time) {
			*entered = metav1.Time{}
		}},
		{name: "primary target differs", mutate: func(isvc *omev1beta1.InferenceService, _ *metav1.Time) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = "dddddddd"
		}},
		{name: "projected steps differ", mutate: func(isvc *omev1beta1.InferenceService, _ *metav1.Time) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[1].Traffic = 40
			refreshDigest(t, &isvc.Status.Rollout.ActiveRun.Plan.Groups[0])
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc, steps, entered := validRepinISVC(t)
			projected := append([]omev1beta1.RolloutGroupStep{}, steps...)
			if tt.mutate != nil {
				tt.mutate(isvc, &entered)
			}
			assert.Equal(t, tt.valid, pinnedevidence.ValidCanaryRepin(
				isvc, omev1beta1.EngineComponent, projected, "bbbbbbbb", &entered,
			))
		})
	}
}

func TestValidActiveRunRejectsImpossibleEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "run identity malformed", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.RunID = "chat-not-a-hash"
		}},
		{name: "pin predates open", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = metav1.NewTime(
				isvc.Status.Rollout.ActiveRun.OpenedAt.Add(-time.Minute),
			)
		}},
		{name: "digest differs", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "rp1:000000000000"
		}},
		{name: "resolved group retains policy ref", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded-canary", Progression: omev1beta1.RolloutProgressionCanary,
			}
		}},
		{name: "inline source carries policy metadata", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyGeneration = 1
		}},
		{name: "policy progression differs", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Progression = omev1beta1.RolloutProgressionBlueGreen
		}},
		{name: "policy kind invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Kind = "ClusterRolloutPolicy"
		}},
		{name: "policy name invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Name = "bad/name"
		}},
		{name: "policy capacity absolute", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Steps[0].Capacity = intstr.FromInt(1)
			refreshDigest(t, pinned)
		}},
		{name: "policy server address forbidden", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{ServerAddress: "https://internal"}
			refreshDigest(t, pinned)
		}},
		{name: "policy auth reference forbidden", mutate: func(isvc *omev1beta1.InferenceService) {
			asPolicy(isvc)
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
				AuthRef: &corev1.SecretKeySelector{Key: "token"},
			}
			refreshDigest(t, pinned)
		}},
		{name: "group order unsupported", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Order = []omev1beta1.ComponentType{
				omev1beta1.EngineComponent,
			}
		}},
		{name: "target missing", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions = isvc.Status.Rollout.ActiveRun.TargetRevisions[:1]
		}},
		{name: "target duplicate", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions[1].Component = omev1beta1.EngineComponent
		}},
		{name: "target revision malformed", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions[1].Revision = "SECRET_REVISION"
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc, _, _ := validRepinISVC(t)
			tt.mutate(isvc)
			assert.False(t, pinnedevidence.ValidActiveRun(isvc))
		})
	}
}

func TestValidActiveRunAcceptsControllerShapes(t *testing.T) {
	isvc, _, _ := validRepinISVC(t)
	assert.True(t, pinnedevidence.ValidActiveRun(isvc))

	asPolicy(isvc)
	assert.True(t, pinnedevidence.ValidActiveRun(isvc))

	// A derived ISVC recovers policy provenance from an annotation, which
	// carries the policy name but not a declared progression.
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Progression = ""
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyGeneration = 0
	assert.True(t, pinnedevidence.ValidActiveRun(isvc))
}

func validRepinISVC(
	t *testing.T,
) (*omev1beta1.InferenceService, []omev1beta1.RolloutGroupStep, metav1.Time) {
	t.Helper()
	steps := []omev1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 10},
		{Capacity: intstr.FromString("50%"), Traffic: 30},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	group := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{
			omev1beta1.DecoderComponent, omev1beta1.EngineComponent,
		},
		Canary: &omev1beta1.GroupCanary{Steps: steps},
	}
	digest, err := rolloutpolicy.ProgressionDigest(&group)
	require.NoError(t, err)
	mode := constants.OMENative
	opened := metav1.NewTime(time.Date(2026, time.September, 14, 16, 40, 0, 0, time.UTC))
	entered := metav1.NewTime(time.Date(2026, time.September, 14, 16, 45, 0, 0, time.UTC))
	pinnedAt := metav1.NewTime(time.Date(2026, time.September, 14, 16, 50, 0, 0, time.UTC))
	isvc := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat"},
		Spec: omev1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &omev1beta1.EngineSpec{},
			Decoder:        &omev1beta1.DecoderSpec{},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Rollout: &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
				RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: pinnedAt,
				TargetRevisions: []omev1beta1.RolloutRunTarget{
					{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb"},
					{Component: omev1beta1.DecoderComponent, Revision: "cccccccc"},
				},
				Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
					Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
				}}},
			}},
		},
	}
	return isvc, steps, entered
}

func asPolicy(isvc *omev1beta1.InferenceService) {
	pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	pinned.Source = omev1beta1.RolloutPlanSourcePolicy
	pinned.PolicyGeneration = 7
	pinned.PolicyRef = &omev1beta1.RolloutPolicyRef{
		Name: "guarded-canary", Progression: omev1beta1.RolloutProgressionCanary,
	}
}

func refreshDigest(t *testing.T, pinned *omev1beta1.RolloutRunGroup) {
	t.Helper()
	digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
	require.NoError(t, err)
	pinned.PortableDigest = digest
}
