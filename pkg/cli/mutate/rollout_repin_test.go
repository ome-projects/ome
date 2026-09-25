package mutate

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var repinNow = time.Date(2026, time.September, 25, 18, 0, 0, 0, time.UTC)

func repinFixture(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	service := safeTarget()
	mode := constants.OMENative
	service.Spec.DeploymentMode = &mode
	service.Spec.Engine = &omev1beta1.EngineSpec{}
	oldGroup := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	liveGroup := *oldGroup.DeepCopy()
	liveGroup.Canary.Steps[0].Capacity = intstr.FromString("25%")
	liveGroup.Canary.Steps[0].Traffic = 25
	oldDigest, err := rolloutpolicy.ProgressionDigest(&oldGroup)
	require.NoError(t, err)
	liveDigest, err := rolloutpolicy.ProgressionDigest(&liveGroup)
	require.NoError(t, err)
	opened := metav1.NewTime(repinNow.Add(-2 * time.Minute))
	pinned := metav1.NewTime(repinNow.Add(-time.Minute))
	service.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{liveGroup}}
	service.Status.Rollout = &omev1beta1.RolloutStatus{
		ActiveRun: &omev1beta1.RolloutRun{
			RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: pinned,
			TargetRevisions: []omev1beta1.RolloutRunTarget{{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "aaaaaaaa"}},
			Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: oldDigest, Group: oldGroup,
			}}},
		},
		Groups: []omev1beta1.RolloutGroupResolution{{Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: liveDigest}},
	}
	readyAt := apis.VolatileTime{Inner: metav1.NewTime(repinNow.Add(-50 * time.Second))}
	driftAt := apis.VolatileTime{Inner: metav1.NewTime(repinNow.Add(-30 * time.Second))}
	service.Status.Conditions = duckv1.Conditions{
		{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: omev1beta1.RolloutPlanReasonPinned, Message: "run chat-0123456789ab pinned", LastTransitionTime: readyAt},
		{
			Type:   apis.ConditionType(omev1beta1.RolloutPlanDriftCondition),
			Status: corev1.ConditionTrue,
			Reason: omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
			Message: "groups[0]: live render " + liveDigest + " differs from pinned " + oldDigest +
				"; the edit applies at the next run (or via ome.io/rollout-repin)",
			LastTransitionTime: driftAt,
		},
	}
	return service
}

func TestPrepareRolloutRepinBuildsExactDigestCASPatch(t *testing.T) {
	service := repinFixture(t)
	plan, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.NoError(t, err)

	liveDigest := service.Status.Rollout.Groups[0].ObservedDigest
	wantCombined := rolloutpolicy.CombinedDigest([]string{liveDigest})
	require.NotEqual(t, "now", wantCombined)
	require.JSONEq(t, `[
      {"op":"test","path":"/metadata/uid","value":"uid-chat"},
      {"op":"test","path":"/metadata/resourceVersion","value":"42"},
      {"op":"add","path":"/metadata/annotations","value":{}},
      {"op":"add","path":"/metadata/annotations/ome.io~1rollout-repin","value":"`+wantCombined+`"}
    ]`, string(plan.Patch()))
	require.Equal(t, reportv1alpha1.RolloutActionDetails{
		RunID: "chat-0123456789ab", PinnedPlanDigest: rolloutpolicy.CombinedDigest([]string{service.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest}),
		RequestedPlanDigest: wantCombined, GroupCount: 1,
	}, plan.Details())
	require.Equal(t, "chat", plan.Target().Name)

	copy := plan.Patch()
	copy[0] = 'x'
	require.NotEqual(t, copy, plan.Patch(), "callers must not mutate the guarded patch")
	encoded, err := json.Marshal(plan)
	require.ErrorIs(t, err, ErrRepinPlan)
	require.NotContains(t, string(encoded), wantCombined)
	_, err = plan.MarshalYAML()
	require.ErrorIs(t, err, ErrRepinPlan)
	for _, rendered := range []string{plan.String(), plan.GoString()} {
		require.NotContains(t, rendered, wantCombined)
	}
}

func TestPrepareRolloutRepinAcceptsCurrentPolicyResolutionWithoutPolicyRead(t *testing.T) {
	service := repinFixture(t)
	ref := &omev1beta1.RolloutPolicyRef{Kind: "RolloutPolicy", Name: "progressive", Progression: omev1beta1.RolloutProgressionCanary}
	service.Spec.Rollout.Groups[0].Canary = nil
	service.Spec.Rollout.Groups[0].PolicyRef = ref.DeepCopy()
	service.Status.Rollout.Groups[0] = omev1beta1.RolloutGroupResolution{
		Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy, PolicyRef: ref.DeepCopy(), ObservedDigest: "rp1:bbbbbbbbbbbb",
	}
	service.Status.Conditions[1].Reason = omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun
	service.Status.Conditions[1].Message = "groups[0]: live render rp1:bbbbbbbbbbbb differs from pinned " +
		service.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest +
		"; the edit applies at the next run (or via ome.io/rollout-repin)"

	plan, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.NoError(t, err)
	require.Equal(t, rolloutpolicy.CombinedDigest([]string{"rp1:bbbbbbbbbbbb"}), plan.Details().RequestedPlanDigest)
}

func TestPrepareRolloutRepinComposesImplicitDefaultBlueGreenDigest(t *testing.T) {
	service := repinFixture(t)
	defaultLive := omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent}}
	defaultPinned, err := rolloutpolicy.ComposeGroup(&defaultLive, nil)
	require.NoError(t, err)
	defaultDigest, err := rolloutpolicy.ProgressionDigest(&defaultPinned)
	require.NoError(t, err)
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	service.Spec.Router = &omev1beta1.RouterSpec{}
	service.Spec.Rollout.GroupOrdering = &ordering
	service.Spec.Rollout.Groups = append([]omev1beta1.RolloutGroup{defaultLive}, service.Spec.Rollout.Groups...)
	service.Status.Rollout.ActiveRun.Plan.Groups = append([]omev1beta1.RolloutRunGroup{{
		Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: defaultDigest, Group: defaultPinned,
	}}, service.Status.Rollout.ActiveRun.Plan.Groups...)
	service.Status.Rollout.ActiveRun.TargetRevisions = append([]omev1beta1.RolloutRunTarget{{
		Component: omev1beta1.RouterComponent, Revision: "cccccccc", StableRevision: "dddddddd",
	}}, service.Status.Rollout.ActiveRun.TargetRevisions...)
	// The current controller's resolution view uses an empty digest for the
	// implicit default spelling. The guarded request must hash the resolved
	// explicit blue-green progression instead of hashing the empty source arm.
	service.Status.Rollout.Groups = append([]omev1beta1.RolloutGroupResolution{{
		Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: "",
	}}, service.Status.Rollout.Groups...)
	service.Status.Rollout.Groups[1].Index = 1
	// The controller's raw liveSourceDigest is empty for the implicit default,
	// so its drift scan reports group 0 even though the CLI's composed render
	// proves that group unchanged and will repin the actual change in group 1.
	service.Status.Conditions[1].Message = "groups[0]: live render (unresolvable) differs from pinned " +
		service.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest +
		"; the edit applies at the next run (or via ome.io/rollout-repin)"

	plan, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.NoError(t, err)
	wantRequested := rolloutpolicy.CombinedDigest([]string{defaultDigest, service.Status.Rollout.Groups[1].ObservedDigest})
	require.Equal(t, wantRequested, plan.Details().RequestedPlanDigest)
	require.Equal(t, 2, plan.Details().GroupCount)
	require.Contains(t, string(plan.Patch()), wantRequested)
}

func TestRepinLiveSpecRejectsMalformedRolloutPolicyRefs(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*omev1beta1.RolloutPolicyRef)
	}{
		{"name", func(ref *omev1beta1.RolloutPolicyRef) { ref.Name = "bad/name" }},
		{"kind", func(ref *omev1beta1.RolloutPolicyRef) { ref.Kind = "PRIVATE_KIND" }},
		{"progression", func(ref *omev1beta1.RolloutPolicyRef) { ref.Progression = "PRIVATE_PROGRESSION" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := repinFixture(t)
			service.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
				Kind: "RolloutPolicy", Name: "progressive", Progression: omev1beta1.RolloutProgressionCanary,
			}
			tc.edit(service.Spec.Rollout.Groups[0].PolicyRef)
			require.ErrorIs(t, validateRepinLiveSpec(service), ErrRepinEvidence)
		})
	}
}

func TestPrepareRolloutRepinFailsClosedOnUnsafeEvidence(t *testing.T) {
	tests := []struct {
		name string
		edit func(*omev1beta1.InferenceService)
		want error
	}{
		{"no active run", func(v *omev1beta1.InferenceService) { v.Status.Rollout.ActiveRun = nil }, ErrRepinIdle},
		{"empty live plan", func(v *omev1beta1.InferenceService) { v.Spec.Rollout.Groups = nil; v.Status.Rollout.Groups = nil }, ErrRepinEmptyPlan},
		{"empty pinned plan", func(v *omev1beta1.InferenceService) { v.Status.Rollout.ActiveRun.Plan.Groups = nil }, ErrRepinEvidence},
		{"missing ready", func(v *omev1beta1.InferenceService) { v.Status.Conditions = v.Status.Conditions[1:] }, ErrRepinEvidence},
		{"duplicate ready", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions = append(v.Status.Conditions, v.Status.Conditions[0])
		}, ErrRepinEvidence},
		{"ready wrong reason", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[0].Reason = omev1beta1.RolloutPlanReasonNoRun
		}, ErrRepinEvidence},
		{"ready wrong message", func(v *omev1beta1.InferenceService) { v.Status.Conditions[0].Message = "PRIVATE" }, ErrRepinEvidence},
		{"ready before run", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[0].LastTransitionTime.Inner = metav1.NewTime(repinNow.Add(-3 * time.Minute))
		}, ErrRepinEvidence},
		{"missing drift", func(v *omev1beta1.InferenceService) { v.Status.Conditions = v.Status.Conditions[:1] }, ErrRepinEvidence},
		{"drift false", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[1].Status = corev1.ConditionFalse
			v.Status.Conditions[1].Reason = omev1beta1.RolloutPlanDriftReasonInSync
		}, ErrRepinInSync},
		{"drift future", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[1].LastTransitionTime.Inner = metav1.NewTime(repinNow.Add(time.Second))
		}, ErrRepinEvidence},
		{"drift before pin", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[1].LastTransitionTime.Inner = metav1.NewTime(repinNow.Add(-90 * time.Second))
		}, ErrRepinEvidence},
		{"stale drift message", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[1].Message = "PRIVATE stale drift"
		}, ErrRepinEvidence},
		{"missing resolution", func(v *omev1beta1.InferenceService) { v.Status.Rollout.Groups = nil }, ErrRepinEvidence},
		{"wrong resolution index", func(v *omev1beta1.InferenceService) { v.Status.Rollout.Groups[0].Index = 1 }, ErrRepinEvidence},
		{"wrong resolution source", func(v *omev1beta1.InferenceService) {
			v.Status.Rollout.Groups[0].Source = omev1beta1.RolloutPlanSourcePolicy
		}, ErrRepinEvidence},
		{"malformed current digest", func(v *omev1beta1.InferenceService) { v.Status.Rollout.Groups[0].ObservedDigest = "now" }, ErrRepinEvidence},
		{"inline digest mismatch", func(v *omev1beta1.InferenceService) { v.Status.Rollout.Groups[0].ObservedDigest = "rp1:bbbbbbbbbbbb" }, ErrRepinEvidence},
		{"condition reason mismatches first drift", func(v *omev1beta1.InferenceService) {
			v.Status.Conditions[1].Reason = omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun
		}, ErrRepinEvidence},
		{"already in sync", func(v *omev1beta1.InferenceService) {
			v.Status.Rollout.Groups[0].ObservedDigest = v.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest
			v.Spec.Rollout.Groups[0] = *v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.DeepCopy()
		}, ErrRepinInSync},
		{"component topology changed", func(v *omev1beta1.InferenceService) {
			v.Spec.Rollout.Groups[0].Components = []omev1beta1.ComponentType{omev1beta1.DecoderComponent}
		}, ErrRepinTopology},
		{"order topology changed", func(v *omev1beta1.InferenceService) {
			v.Spec.Rollout.Groups[0].Order = []omev1beta1.ComponentType{omev1beta1.EngineComponent}
		}, ErrRepinTopology},
		{"progression topology changed", func(v *omev1beta1.InferenceService) {
			v.Spec.Rollout.Groups[0].Canary = nil
			v.Spec.Rollout.Groups[0].BlueGreen = &omev1beta1.GroupBlueGreen{}
		}, ErrRepinTopology},
		{"soak topology changed", func(v *omev1beta1.InferenceService) {
			v.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: time.Second}
		}, ErrRepinTopology},
		{"ratio topology changed", func(v *omev1beta1.InferenceService) {
			value := int32(5)
			v.Spec.Rollout.Groups[0].MaintainRatio = &omev1beta1.MaintainRatio{Tolerance: &value}
		}, ErrRepinTopology},
		{"pending repin", func(v *omev1beta1.InferenceService) {
			v.Annotations = map[string]string{constants.RolloutRepinAnnotation: "rp1:aaaaaaaaaaaa"}
		}, ErrRepinPending},
		{"pending promote", func(v *omev1beta1.InferenceService) {
			v.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "bbbbbbbb"}
		}, ErrRepinPending},
		{"pending rollback", func(v *omev1beta1.InferenceService) {
			v.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
		}, ErrRepinPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := repinFixture(t)
			tt.edit(service)
			_, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
			require.ErrorIs(t, err, tt.want)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}

func TestPrepareRolloutRepinRejectsMultipleCanaryGroupsEvenWhenDisjoint(t *testing.T) {
	service := repinFixture(t)
	secondLive := *service.Spec.Rollout.Groups[0].DeepCopy()
	secondLive.Components = []omev1beta1.ComponentType{omev1beta1.RouterComponent}
	secondPinned := *service.Status.Rollout.ActiveRun.Plan.Groups[0].DeepCopy()
	secondPinned.Group.Components = []omev1beta1.ComponentType{omev1beta1.RouterComponent}
	secondDigest, err := rolloutpolicy.ProgressionDigest(&secondLive)
	require.NoError(t, err)
	service.Spec.Router = &omev1beta1.RouterSpec{}
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	service.Spec.Rollout.GroupOrdering = &ordering
	service.Spec.Rollout.Groups = append(service.Spec.Rollout.Groups, secondLive)
	service.Status.Rollout.ActiveRun.Plan.Groups = append(service.Status.Rollout.ActiveRun.Plan.Groups, secondPinned)
	service.Status.Rollout.ActiveRun.TargetRevisions = append(service.Status.Rollout.ActiveRun.TargetRevisions, omev1beta1.RolloutRunTarget{Component: omev1beta1.RouterComponent, Revision: "cccccccc", StableRevision: "dddddddd"})
	service.Status.Rollout.Groups = append(service.Status.Rollout.Groups, omev1beta1.RolloutGroupResolution{Index: 1, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: secondDigest})

	_, err = PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.ErrorIs(t, err, ErrRepinMultipleCanaries)
}

func TestRolloutRepinPreviewIsBoundedAndPrivate(t *testing.T) {
	service := repinFixture(t)
	service.Annotations = map[string]string{"private.example/token": strings.Repeat("SECRET", 100)}
	plan, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "moirai", reportv1alpha1.DryRunServer))
	require.Contains(t, out.String(), "ome.io/rollout-repin")
	require.Contains(t, out.String(), plan.Details().RequestedPlanDigest)
	require.Contains(t, out.String(), "controller CAS covers progression renders")
	require.NotContains(t, out.String(), "SECRET")
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}
}

func TestRolloutRepinPlanDetailsAreDetached(t *testing.T) {
	service := repinFixture(t)
	plan, err := PrepareRolloutRepin(service, reportv1alpha1.ClockFunc(func() time.Time { return repinNow }))
	require.NoError(t, err)
	details := plan.Details()
	service.Status.Rollout.ActiveRun.RunID = "changed"
	require.Equal(t, "chat-0123456789ab", details.RunID)
	require.False(t, reflect.ValueOf(details).IsZero())
}
