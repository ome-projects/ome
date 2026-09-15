package mutate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

var testNow = time.Date(2026, 9, 15, 21, 0, 0, 0, time.UTC)
var testClock = reportv1alpha1.ClockFunc(func() time.Time { return testNow })

func activeEvidence(v *v1beta1.InferenceService) ReplicaEvidence {
	return ReplicaEvidence{complete: true, active: true, uid: string(v.UID), resourceVersion: v.ResourceVersion, sources: map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "bbbbbbbb"}}
}

func nativeTarget(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState) {
	t.Helper()
	v := safeTarget()
	mode := constants.OMENative
	v.Spec.DeploymentMode = &mode
	v.Spec.Engine = &v1beta1.EngineSpec{}
	autoSync := true
	kind := "ServingRuntime"
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "simple", Kind: &kind, AutoSync: &autoSync}
	v.Status.ObservedGeneration = v.Generation
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36"}}}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	live := effective.NewRuntimeResolver(clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build())
	_, err := live.ResolveLive(context.Background(), v)
	require.NoError(t, err)
	resolver, err := effective.NewRuntimePinResolver(kubefake.NewClientset().AppsV1(), live, "ome", paging.Limits{PageSize: 16, MaxItems: 16, MaxPages: 2, RequestTimeout: time.Second})
	require.NoError(t, err)
	state, err := resolver.Resolve(context.Background(), v, effective.RuntimeResolveOptions{})
	require.NoError(t, err)
	_, err = state.RequireActive()
	require.NoError(t, err)
	return v, state
}

func pinnedCanary(t *testing.T, v *v1beta1.InferenceService) {
	t.Helper()
	group := v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("50%"), Traffic: 50}, {Capacity: intstr.FromString("100%"), Traffic: 100}}}}
	digest, err := rolloutpolicy.ProgressionDigest(&group)
	require.NoError(t, err)
	v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: metav1.NewTime(testNow.Add(-time.Minute)), PinnedAt: metav1.NewTime(testNow.Add(-30 * time.Second)), Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Group: group, Source: v1beta1.RolloutPlanSourceInline, PortableDigest: digest}}}, TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "bbbbbbbb"}}}}
	v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhaseCanarying, LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa", LatestReadyRevision: "chat-engine-rev-bbbbbbbb", Traffic: []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50}, {RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 50}}}}
	v.Status.Canary = &v1beta1.CanaryStatus{TargetID: "ct1:" + rolloutpolicy.ShortHash([]byte("engine=bbbbbbbb")), StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb", ObservedTrafficWeight: 50}
}

func TestNativeParentGenerationIsNotChildStatusFreshness(t *testing.T) {
	v, state := nativeTarget(t)
	v.Status.ObservedGeneration = 0 // Current OMENative leaves this parent field unset.
	_, err := PrepareRollout(v, state, activeEvidence(v), "pause", false, true, testClock)
	require.NoError(t, err)
}

func TestPinnedWorkValidatesAllCanaryEvidenceBeforePositiveIRWork(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*v1beta1.InferenceService)
	}{
		{"missing canary", func(v *v1beta1.InferenceService) { v.Status.Canary = nil }},
		{"wrong target set", func(v *v1beta1.InferenceService) { v.Status.Canary.TargetID = "ct1:aaaaaaaaaaaa" }},
		{"wrong target revision", func(v *v1beta1.InferenceService) { v.Status.Canary.CanaryRevisionHash = "cccccccc" }},
		{"future entered time", func(v *v1beta1.InferenceService) {
			value := metav1.NewTime(testNow.Add(time.Hour))
			v.Status.Canary.StepEnteredTime = &value
		}},
		{"out of range", func(v *v1beta1.InferenceService) { v.Status.Canary.CurrentStep = 100 }},
		{"wrong traffic binding", func(v *v1beta1.InferenceService) { v.Status.Canary.ObservedTrafficWeight = 25 }},
		{"unknown component phase", func(v *v1beta1.InferenceService) {
			value := v.Status.Components[v1beta1.EngineComponent]
			value.RolloutPhase = "UNKNOWN_PRIVATE"
			v.Status.Components[v1beta1.EngineComponent] = value
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			v, state := nativeTarget(t)
			pinnedCanary(t, v)
			change.edit(v)
			_, err := PrepareRollout(v, state, activeEvidence(v), "pause", false, true, testClock)
			require.ErrorIs(t, err, ErrStale)
		})
	}
}

func TestPinnedCanaryUsesCurrentOwnedIRAndPreservesFrozenPlan(t *testing.T) {
	v, state := nativeTarget(t)
	pinnedCanary(t, v)
	v.Status.ObservedGeneration = 0
	v.Spec.Rollout = &v1beta1.RolloutSpec{} // Live intent can drift during an open frozen run.
	work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(replicaFor(v)).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	work.active = false // The applied canary step can hold despite converged IR revisions.
	_, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.NoError(t, err)
	work.sources = nil
	_, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.ErrorIs(t, err, ErrStale)
}

func TestFailedCanaryDoesNotRefuseDistinctCurrentLifecycleOperation(t *testing.T) {
	v, state := nativeTarget(t)
	pinnedCanary(t, v)
	component := v.Status.Components[v1beta1.EngineComponent]
	component.RolloutPhase = v1beta1.RolloutPhaseFailed
	v.Status.Components[v1beta1.EngineComponent] = component
	ir := replicaFor(v)
	ir.Status.CurrentRevision = ir.Status.UpdateRevision
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, Operation: &v1beta1.InstanceOperation{ID: "restart-0-123", Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2 * time.Second)), LastProgressAt: metav1.NewTime(testNow.Add(-time.Second))}}}
	work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	require.Equal(t, 1, work.operations)
	_, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.NoError(t, err)
}

func TestNativeRuntimeLiveAndPinnedWriterContractAreDifferent(t *testing.T) {
	v, state := nativeTarget(t)
	components, err := RequireNativeRuntime(v, state)
	require.NoError(t, err)
	require.Equal(t, []string{"engine"}, components)
	active, err := state.RequireActive()
	require.NoError(t, err)
	active.Origin = "UnknownOrigin"
	require.ErrorIs(t, requireActiveOrigin(active, state), ErrRuntime)
	unknown := "UnknownRuntime"
	v.Spec.Runtime.Kind = &unknown
	_, err = RequireNativeRuntime(v, state)
	require.ErrorIs(t, err, ErrRuntime)
	v.Spec.Runtime.Kind = nil
	unknown = "other.io"
	v.Spec.Runtime.APIGroup = &unknown
	_, err = RequireNativeRuntime(v, state)
	require.ErrorIs(t, err, ErrRuntime)
	state.PinMode = effective.RuntimePinModeManagedPin
	_, err = RequireNativeRuntime(v, state)
	require.ErrorIs(t, err, ErrRuntime)

	for _, consistent := range []bool{true, false} {
		v, _ = nativeTarget(t)
		auto := false
		v.Spec.Runtime.AutoSync = &auto
		spec := v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36"}}}}
		_, short, err := runtimerevision.Hash(&spec)
		require.NoError(t, err)
		name := runtimerevision.Name(runtimerevision.KindServingRuntime, "prod", "simple", short)
		body, err := json.Marshal(&spec)
		require.NoError(t, err)
		revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome", Labels: map[string]string{constants.RuntimeRevisionOfLabelKey: "simple", constants.RuntimeRevisionOfKindLabelKey: "ServingRuntime", constants.RuntimeRevisionOfNamespaceLabelKey: "prod", constants.RuntimeRevisionHashLabelKey: short}, Annotations: map[string]string{constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue}}, Revision: 1, Data: runtime.RawExtension{Raw: body}}
		if !consistent {
			revision.Annotations = nil
		}
		v.Status.PinnedRevisionName = name
		scheme := runtime.NewScheme()
		require.NoError(t, v1beta1.AddToScheme(scheme))
		live := effective.NewRuntimeResolver(clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: spec}).Build())
		resolver, err := effective.NewRuntimePinResolver(kubefake.NewClientset(revision).AppsV1(), live, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: time.Second})
		require.NoError(t, err)
		state, err = resolver.Resolve(context.Background(), v, effective.RuntimeResolveOptions{})
		require.NoError(t, err)
		_, err = RequireNativeRuntime(v, state)
		if consistent {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, ErrRuntime)
		}
	}
}

// Removing pause-state/mailbox/active-work checks must fail before patching.
func TestPrepareRolloutGuardAndExactPatches(t *testing.T) {
	v, state := nativeTarget(t)
	work := activeEvidence(v)
	plan, err := PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.NoError(t, err)
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations","value":{}},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-paused","value":"true"}]`, string(plan.Patch()))
	v.Annotations = map[string]string{"unrelated": "private"}
	plan, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.NoError(t, err)
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-paused","value":"true"}]`, string(plan.Patch()))
	for _, pause := range []string{"true", "freeze"} {
		v.Annotations = map[string]string{constants.PausedRolloutAnnotation: pause, "unrelated": "private"}
		_, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
		require.ErrorIs(t, err, ErrPaused)
		plan, err = PrepareRollout(v, state, ReplicaEvidence{}, "resume", false, true, testClock)
		require.NoError(t, err)
		require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"remove","path":"/metadata/annotations/ome.io~1rollout-paused"}]`, string(plan.Patch()))
	}
	v.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true", constants.RolloutPromoteAnnotation: "cccccccc", constants.RolloutRollbackAnnotation: "false"}
	_, err = PrepareRollout(v, state, work, "resume", false, true, testClock)
	require.ErrorIs(t, err, ErrPending)
	_, err = PrepareRollout(v, state, work, "resume", true, false, testClock)
	require.ErrorIs(t, err, ErrStrongConfirmation)
	plan, err = PrepareRollout(v, state, work, "resume", true, true, testClock)
	require.NoError(t, err)
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"remove","path":"/metadata/annotations/ome.io~1rollout-paused"},{"op":"remove","path":"/metadata/annotations/ome.io~1rollout-promote"},{"op":"remove","path":"/metadata/annotations/ome.io~1rollout-rollback"}]`, string(plan.Patch()))
	copy := plan.Patch()
	copy[0] = 'x'
	require.Equal(t, byte('['), plan.Patch()[0])
}

func TestPrepareRolloutRefusesIdleStalePendingAndUnboundRuntime(t *testing.T) {
	cases := []struct {
		name   string
		change func(*v1beta1.InferenceService)
		action string
		work   ReplicaEvidence
		want   error
	}{
		{"idle", func(*v1beta1.InferenceService) {}, "pause", ReplicaEvidence{complete: true}, ErrIdle},
		{"incomplete", func(*v1beta1.InferenceService) {}, "pause", ReplicaEvidence{active: true}, ErrBounds},
		{"stale private proof", func(*v1beta1.InferenceService) {}, "pause", ReplicaEvidence{complete: true, active: true, uid: "old-owner", resourceVersion: "42"}, ErrStale},
		{"unpaused resume", func(*v1beta1.InferenceService) {}, "resume", ReplicaEvidence{}, ErrNotPaused},
		{"empty mailbox", func(v *v1beta1.InferenceService) {
			v.Annotations = map[string]string{constants.RolloutRollbackAnnotation: ""}
		}, "pause", ReplicaEvidence{complete: true, active: true}, ErrPending},
		{"hostile discard", func(v *v1beta1.InferenceService) {
			v.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true", constants.RolloutPromoteAnnotation: "Bearer SECRET_TOKEN"}
		}, "resume", ReplicaEvidence{}, ErrUnsafeValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, state := nativeTarget(t)
			tc.change(v)
			if tc.name == "idle" {
				tc.work.sources = map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "aaaaaaaa"}
			}
			if tc.work.complete && tc.work.uid == "" {
				tc.work.uid = string(v.UID)
				tc.work.resourceVersion = v.ResourceVersion
			}
			_, err := PrepareRollout(v, state, tc.work, tc.action, tc.name == "hostile discard", true, testClock)
			require.ErrorIs(t, err, tc.want)
		})
	}
	v, state := nativeTarget(t)
	v.ResourceVersion = "43"
	_, err := PrepareRollout(v, state, ReplicaEvidence{complete: true, active: true}, "pause", false, true, testClock)
	require.ErrorIs(t, err, ErrRuntime)
	v, _ = nativeTarget(t)
	_, err = PrepareRollout(v, nil, ReplicaEvidence{}, "pause", false, true, testClock)
	require.ErrorIs(t, err, ErrRuntime)
}
