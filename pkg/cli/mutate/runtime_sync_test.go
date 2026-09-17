package mutate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/yaml"
)

func syncMutationSource(t *testing.T, v *v1beta1.InferenceService) effective.RuntimeSyncEvidence {
	t.Helper()
	spec := &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "old"}}}}
	raw, err := json.Marshal(spec)
	require.NoError(t, err)
	_, hash, err := runtimerevision.Hash(spec)
	require.NoError(t, err)
	name := runtimerevision.Name(runtimerevision.KindClusterServingRuntime, "", "runtime", hash)
	revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome", UID: "revision-uid", ResourceVersion: "11", Labels: map[string]string{constants.RuntimeRevisionOfLabelKey: "runtime", constants.RuntimeRevisionOfKindLabelKey: "ClusterServingRuntime", constants.RuntimeRevisionOfNamespaceLabelKey: "", constants.RuntimeRevisionHashLabelKey: hash}, Annotations: map[string]string{constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue}}, Revision: 1, Data: kruntime.RawExtension{Raw: raw}}
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime", AutoSync: ptr.To(false)}
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Status.PinnedRevisionName = name
	v.Status.Conditions = []knapis.Condition{{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionTrue, Reason: "RevisionMismatch"}}
	live := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", UID: "runtime-uid", ResourceVersion: "12", Generation: 2}, Spec: *spec.DeepCopy()}
	live.Spec.EngineConfig.Runner.Image = "new"
	scheme := kruntime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()
	resolver, err := effective.NewRuntimeSyncResolver(kfake.NewSimpleClientset(revision).AppsV1(), client, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	e, err := resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	return e
}

func TestRuntimeSyncNativeOwnedStableAndPortable(t *testing.T) {
	v := safeTarget()
	ir := replicaFor(v)
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("")} // controller's empty, non-active semantics.
	work, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	require.True(t, work.valid)
	portable := omefake.NewSimpleClientset()
	work, err = CollectRuntimeSyncReplicaEvidence(context.Background(), portable.OmeV1beta1(), v, nil, testClock)
	require.NoError(t, err)
	require.True(t, work.valid)
	require.Empty(t, portable.Actions())
}

func TestRuntimeSyncNativeRefusesColumnarTransientRow(t *testing.T) {
	for _, phase := range []v1beta1.OMENativeInstancePhase{v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceDeleting} {
		t.Run(string(phase), func(t *testing.T) {
			v := safeTarget()
			ir := replicaFor(v)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: phase}}
			storeColumnarReplica(t, ir)
			stored := ir.DeepCopy()

			_, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
			require.ErrorIs(t, err, ErrStale)
			require.Equal(t, stored, ir)
		})
	}
}

func TestRuntimeSyncNativeRefusesActiveUnselectedReplica(t *testing.T) {
	v := safeTarget()
	engine := replicaFor(v)
	engine.Status.UpdateRevision = engine.Status.CurrentRevision
	decoder := replicaFor(v)
	decoder.Name = "chat-decoder"
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.CurrentRevision = "chat-decoder-aaaaaaaa"
	decoder.Status.UpdateRevision = decoder.Status.CurrentRevision
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceCreating}}
	storeColumnarReplica(t, decoder)

	_, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(engine, decoder).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.ErrorIs(t, err, ErrStale)
}

func TestRuntimeSyncNativeSnapshotIncludesUnselectedReplica(t *testing.T) {
	v := safeTarget()
	engine := replicaFor(v)
	engine.Status.UpdateRevision = engine.Status.CurrentRevision
	decoder := replicaFor(v)
	decoder.Name = "chat-decoder"
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.CurrentRevision = "chat-decoder-aaaaaaaa"
	decoder.Status.UpdateRevision = decoder.Status.CurrentRevision
	decoder.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	storeColumnarReplica(t, decoder)

	before, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(engine, decoder).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	decoder.ResourceVersion = "changed"
	after, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(engine, decoder).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	require.False(t, before.SameSnapshot(after))
}

func TestRuntimeSyncNativeRefusesRetryBackoff(t *testing.T) {
	for _, delay := range []time.Duration{time.Minute, -time.Minute} {
		t.Run(delay.String(), func(t *testing.T) {
			v := safeTarget()
			ir := replicaFor(v)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
			storeColumnarReplica(t, ir)
			ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: ir.Status.CurrentRevision, State: v1beta1.RetryBlockBackoff, NextRetryAt: ptr.To(metav1.NewTime(testClock.Now().Add(delay))), FirstFailureAt: ptr.To(metav1.NewTime(testClock.Now().Add(-2 * time.Minute))), LastFailureAt: ptr.To(metav1.NewTime(testClock.Now().Add(-time.Minute)))}}

			_, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
			require.ErrorIs(t, err, ErrStale)
		})
	}
}

func TestRuntimeSyncNativeRefusesIncompleteUnsafeAndActiveEvidence(t *testing.T) {
	cases := []struct {
		name string
		edit func(*v1beta1.InferenceReplica)
	}{
		{"stale IR generation", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 0 }},
		{"stale parent stamp", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "0"
		}},
		{"wrong owner UID", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].UID = "other" }},
		{"wrong name", func(ir *v1beta1.InferenceReplica) { ir.Name = "other" }},
		{"unknown component", func(ir *v1beta1.InferenceReplica) { ir.Spec.Component = "PRIVATE" }},
		{"rollback", func(ir *v1beta1.InferenceReplica) {
			ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("chat-engine-aaaaaaaa")}
		}},
		{"held", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: "chat-engine-aaaaaaaa", State: v1beta1.RetryBlockHeld}}
		}},
		{"unknown retry", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: "chat-engine-aaaaaaaa", State: "PRIVATE"}}
		}},
		{"release present empty", func(ir *v1beta1.InferenceReplica) { ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "" }},
		{"active revision", func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = "chat-engine-bbbbbbbb" }},
		{"active row without op", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceCreating}}
		}},
		{"malformed row", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: -1, Phase: v1beta1.OMENativeInstanceReady}}
		}},
		{"huge private", func(ir *v1beta1.InferenceReplica) { ir.Annotations["private"] = strings.Repeat("PRIVATE", 200000) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := safeTarget()
			ir := replicaFor(v)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			tc.edit(ir)
			_, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
	for _, scenario := range []string{"missing", "duplicate", "incomplete"} {
		t.Run(scenario, func(t *testing.T) {
			v := safeTarget()
			ir := replicaFor(v)
			ir.Status.UpdateRevision = ir.Status.CurrentRevision
			client := omefake.NewSimpleClientset()
			client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, kruntime.Object, error) {
				list := &v1beta1.InferenceReplicaList{}
				if scenario == "duplicate" {
					list.Items = []v1beta1.InferenceReplica{*ir, *ir}
				}
				if scenario == "incomplete" {
					list.Continue = "PRIVATE_NEXT"
				}
				return true, list, nil
			})
			_, err := CollectRuntimeSyncReplicaEvidence(context.Background(), client.OmeV1beta1(), v, []string{"engine"}, testClock)
			require.Error(t, err)
		})
	}
}

func TestRuntimeSyncPlanCASOpaquePrivacyAndExactPreview(t *testing.T) {
	for _, annotations := range []map[string]string{nil, {"unrelated": "PRIVATE_VALUE"}, {constants.RuntimeSyncAnnotationKey: "PRIVATE_OLD"}} {
		v := safeTarget()
		v.Annotations = annotations
		if annotations[constants.RuntimeSyncAnnotationKey] != "" {
			v.Status.LastRuntimeSyncToken = "PRIVATE_OLD"
		}
		e := syncMutationSource(t, v)
		work, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset().OmeV1beta1(), v, e.NativeComponents(), testClock)
		require.NoError(t, err)
		uuid, err := NewRuntimeSyncRequestID(e, bytes.NewReader(make([]byte, 16)))
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-4000-8000-000000000000", uuid)
		plan, err := PrepareRuntimeSync(v, e, work, uuid, testClock)
		require.NoError(t, err)
		patch := plan.Patch()
		require.Contains(t, string(patch), `"path":"/metadata/uid"`)
		require.Contains(t, string(patch), `"path":"/metadata/resourceVersion"`)
		if annotations == nil {
			require.Contains(t, string(patch), `"path":"/metadata/annotations","value":{}`)
		}
		if annotations[constants.RuntimeSyncAnnotationKey] != "" {
			require.Contains(t, string(patch), `"op":"test","path":"/metadata/annotations/ome.io~1runtime-sync","value":"PRIVATE_OLD"`)
		}
		patch[0] = 'x'
		require.NotEqual(t, patch, plan.Patch())
		var preview bytes.Buffer
		require.NoError(t, plan.WritePreview(&preview, strings.Repeat("c", 100), "ome", reportv1alpha1.DryRunClient))
		require.NotContains(t, preview.String(), "PRIVATE")
		for _, line := range strings.Split(preview.String(), "\n") {
			require.LessOrEqual(t, len(line), 80)
		}
		_, err = json.Marshal(plan)
		require.Error(t, err)
		_, err = yaml.Marshal(work)
		require.Error(t, err)
		require.NotContains(t, fmt.Sprintf("%+v %#v %v", plan, plan, work), "PRIVATE")
		result := plan.Result(reportv1alpha1.DryRunClient, testClock)
		require.False(t, result.Accepted)
		require.False(t, result.Applied)
		require.Equal(t, uuid, result.RequestID)
		_, err = NewRuntimeSyncRequestID(e, bytes.NewReader(nil))
		require.Error(t, err)
		_, err = PrepareRuntimeSync(v, e, RuntimeSyncReplicaEvidence{}, uuid, testClock)
		require.Error(t, err)
	}
	var output bytes.Buffer
	require.Error(t, (RuntimeSyncPlan{}).WritePreview(&output, "dev-fra", "ome", reportv1alpha1.DryRunNone))
	require.Empty(t, output.String())
}

func TestRuntimeSyncPlanRejectsMailboxAndStickyRollback(t *testing.T) {
	for _, scenario := range []string{"promote empty", "rollback false", "repin empty", "active run", "last rolledback", "sticky rolledback"} {
		v := safeTarget()
		v.Annotations = map[string]string{}
		switch scenario {
		case "promote empty":
			v.Annotations[constants.RolloutPromoteAnnotation] = ""
		case "rollback false":
			v.Annotations[constants.RolloutRollbackAnnotation] = "false"
		case "repin empty":
			v.Annotations[constants.RolloutRepinAnnotation] = ""
		case "active run":
			v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{}}
		case "last rolledback":
			v.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunRolledBack}}
		default:
			v.Status.Canary = &v1beta1.CanaryStatus{RolledBackRevisionHash: "aaaaaaaa"}
		}
		e := syncMutationSource(t, v)
		work, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset().OmeV1beta1(), v, e.NativeComponents(), testClock)
		require.NoError(t, err)
		_, err = PrepareRuntimeSync(v, e, work, "00000000-0000-4000-8000-000000000000", testClock)
		require.Error(t, err)
	}
}

func TestRuntimeSyncEntropyCollisionAndOpaqueZeroGuards(t *testing.T) {
	v := safeTarget()
	v.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: runtimeSyncTokenPrefix + "00000000-0000-4000-8000-000000000000"}
	v.Status.LastRuntimeSyncToken = v.Annotations[constants.RuntimeSyncAnnotationKey]
	e := syncMutationSource(t, v)
	_, err := NewRuntimeSyncRequestID(e, bytes.NewReader(make([]byte, 64)))
	require.ErrorContains(t, err, "unique")
	entropy := append(make([]byte, 16), bytes.Repeat([]byte{1}, 16)...)
	uuid, err := NewRuntimeSyncRequestID(e, bytes.NewReader(entropy))
	require.NoError(t, err)
	require.NotEqual(t, "00000000-0000-4000-8000-000000000000", uuid)
	_, err = NewRuntimeSyncRequestID(e, nil)
	require.Error(t, err)
	_, err = NewRuntimeSyncRequestID(effective.RuntimeSyncEvidence{}, bytes.NewReader(make([]byte, 64)))
	require.Error(t, err)
	work, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset().OmeV1beta1(), v, nil, nil)
	require.NoError(t, err)
	require.True(t, work.SameSnapshot(work))
	require.False(t, work.SameSnapshot(RuntimeSyncReplicaEvidence{}))
	_, err = work.MarshalYAML()
	require.Error(t, err)
	_, err = (RuntimeSyncPlan{}).MarshalYAML()
	require.Error(t, err)
	require.NotContains(t, fmt.Sprintf("%#v", work), "PRIVATE")
	require.Empty(t, (RuntimeSyncPlan{}).Result(reportv1alpha1.DryRunNone, nil).Action)
	_, err = PrepareRuntimeSync(nil, e, work, uuid, nil)
	require.Error(t, err)
}

func TestRuntimeSyncNativeBoundedCollectionFailures(t *testing.T) {
	v := safeTarget()
	ir := replicaFor(v)
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	work, err := CollectRuntimeSyncReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, nil)
	require.NoError(t, err)
	require.True(t, work.SameSnapshot(work))
	for _, scenario := range []string{"large page", "private error", "owner bounds", "retry bounds", "cancel", "nil context", "nil client", "invalid component", "duplicate component"} {
		t.Run(scenario, func(t *testing.T) {
			current := ir.DeepCopy()
			ctx := context.Background()
			components := []string{"engine"}
			switch scenario {
			case "owner bounds":
				current.OwnerReferences = make([]metav1.OwnerReference, 17)
			case "retry bounds":
				current.Status.RetryBlocks = make([]v1beta1.RetryBlock, 257)
			case "nil context":
				ctx = nil
			case "nil client":
				_, err := CollectRuntimeSyncReplicaEvidence(ctx, nil, v, components, testClock)
				require.Error(t, err)
				return
			case "cancel":
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(ctx)
				stop()
			case "invalid component":
				components = []string{"PRIVATE"}
			case "duplicate component":
				components = []string{"engine", "engine"}
			}
			client := omefake.NewSimpleClientset(current)
			if scenario == "large page" || scenario == "private error" {
				client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, kruntime.Object, error) {
					if scenario == "private error" {
						return true, nil, errors.New("PRIVATE_API")
					}
					return true, &v1beta1.InferenceReplicaList{Items: make([]v1beta1.InferenceReplica, 17)}, nil
				})
			}
			_, err := CollectRuntimeSyncReplicaEvidence(ctx, client.OmeV1beta1(), v, components, testClock)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}
