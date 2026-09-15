package mutate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func scaleFixture(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState, *v1beta1.InferenceReplica, *autoscalingv1.Scale, effective.ManualScaleSource) {
	t.Helper()
	parent, _ := nativeTarget(t)
	parent.Spec.Engine.MinReplicas = ptr.To(1)
	parent.Spec.Engine.MaxReplicas = 10
	parent.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	parent.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhaseStable,
		ScaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"},
		Autoscaler:     &v1beta1.ComponentAutoscalerStatus{Class: v1beta1.AutoscalerNone, ManagedBy: "none", SpecSource: "isvc"}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	reader := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: parent.Namespace, UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}}).Build()
	live, err := effective.NewBoundedRuntimeResolver(reader, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	pins, err := effective.NewRuntimePinResolver(kubefake.NewClientset().AppsV1(), live, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), parent, effective.RuntimeResolveOptions{})
	require.NoError(t, err)
	source, err := effective.ResolveManualScaleSource(parent, state, v1beta1.EngineComponent)
	require.NoError(t, err)
	replica := replicaFor(parent)
	replica.Status.CurrentRevision = replica.Status.UpdateRevision
	replica.Spec.Replicas = ptr.To[int32](1)
	replica.Spec.Autoscaler = parent.Spec.Engine.Autoscaler.DeepCopy()
	scale := &autoscalingv1.Scale{TypeMeta: metav1.TypeMeta{APIVersion: "autoscaling/v1", Kind: "Scale"}, ObjectMeta: metav1.ObjectMeta{Name: replica.Name, Namespace: replica.Namespace, UID: replica.UID, ResourceVersion: replica.ResourceVersion}, Spec: autoscalingv1.ScaleSpec{Replicas: 1}, Status: autoscalingv1.ScaleStatus{Replicas: 1, Selector: "private-scale-selector"}}
	return parent, state, replica, scale, source
}

func TestPrepareScaleExactDefensiveCASPlan(t *testing.T) {
	parent, state, replica, scale, source := scaleFixture(t)
	evidence, err := InspectScaleEvidence(parent, replica, scale, source, testClock)
	require.NoError(t, err)
	plan, err := PrepareScale(parent, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	if err != nil {
		projection, _ := rolloutprojection.Project(parent, testClock)
		t.Logf("safe rollout issues: %+v", projection.Content.Issues)
	}
	require.NoError(t, err)
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-ir"},{"op":"test","path":"/metadata/resourceVersion","value":"81"},{"op":"replace","path":"/spec/replicas","value":3}]`, string(plan.Patch()))
	patch := plan.Patch()
	patch[0] = 'x'
	require.Equal(t, byte('['), plan.Patch()[0])
	copy := plan.Details()
	copy.Sources[0].UID = "altered"
	copy.Warnings[0] = "altered"
	require.NotEqual(t, "altered", plan.Details().Sources[0].UID)
	require.NotEqual(t, "altered", plan.Details().Warnings[0])
	replica.Spec.Replicas = ptr.To[int32](8)
	require.Equal(t, int32(1), plan.Details().PriorReplicas)
	for _, value := range []any{evidence, plan, source} {
		_, err := json.Marshal(value)
		require.Error(t, err)
		require.NotContains(t, fmt.Sprintf("%v %#v", value, value), "private")
	}
}

func TestScaleExactIRAndScaleRefusalMatrix(t *testing.T) {
	changes := []struct {
		name   string
		change func(*v1beta1.InferenceReplica, *autoscalingv1.Scale)
	}{
		{"wrong IR name", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Name = "other" }},
		{"wrong IR namespace", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Namespace = "other" }},
		{"wrong IR GVK", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Kind = "Deployment" }},
		{"missing IR UID", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.UID = "" }},
		{"missing IR RV", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.ResourceVersion = "" }},
		{"wrong owner UID", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.OwnerReferences[0].UID = "other" }},
		{"duplicate controller", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) {
			r.OwnerReferences = append(r.OwnerReferences, r.OwnerReferences[0])
		}},
		{"parent mismatch", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Spec.ParentRef.Name = "other" }},
		{"component mismatch", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Spec.Component = v1beta1.DecoderComponent }},
		{"missing stamp", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Annotations = nil }},
		{"stale generation", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Status.ObservedGeneration-- }},
		{"future generation", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Status.ObservedGeneration++ }},
		{"deletion", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) {
			now := metav1.NewTime(testNow)
			r.DeletionTimestamp = &now
		}},
		{"nil desired", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Spec.Replicas = nil }},
		{"zero desired", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Spec.Replicas = ptr.To[int32](0) }},
		{"negative counters", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) { r.Status.ReadyReplicas = -1 }},
		{"malformed publication", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) {
			r.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: "private-unknown-phase"}}
		}},
		{"payload mismatch", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) {
			r.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
		}},
		{"Scale name", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.Name = "other" }},
		{"Scale namespace", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.Namespace = "other" }},
		{"Scale GVK", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.APIVersion = "ome.io/v1beta1" }},
		{"Scale UID", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.UID = "different" }},
		{"Scale changed RV same counts", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.ResourceVersion = "82" }},
		{"Scale desired", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.Spec.Replicas = 2 }},
		{"Scale reported", func(_ *v1beta1.InferenceReplica, s *autoscalingv1.Scale) { s.Status.Replicas = 2 }},
		{"oversize complete IR", func(r *v1beta1.InferenceReplica, _ *autoscalingv1.Scale) {
			r.Annotations["private"] = strings.Repeat("x", 1<<20)
		}},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			p, _, r, s, source := scaleFixture(t)
			tc.change(r, s)
			_, err := InspectScaleEvidence(p, r, s, source, testClock)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private-")
		})
	}
}

func TestScaleOwnershipBoundsPartitionsAndReportedDiscrepancy(t *testing.T) {
	p, state, r, s, source := scaleFixture(t)
	r.Status.Replicas = 2
	s.Status.Replicas = 2
	r.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 9, Phase: v1beta1.OMENativeInstanceReady}}
	evidence, err := InspectScaleEvidence(p, r, s, source, testClock)
	require.NoError(t, err)
	for _, pair := range [][2]bool{{false, false}, {false, true}, {true, false}} {
		_, err := PrepareScale(p, state, evidence, v1beta1.EngineComponent, 3, pair[0], pair[1], testClock)
		require.ErrorIs(t, err, ErrScaleOwnership)
	}
	for _, requested := range []int32{0, -1, 11} {
		_, err := PrepareScale(p, state, evidence, v1beta1.EngineComponent, requested, true, true, testClock)
		require.ErrorIs(t, err, ErrScaleBounds)
	}
	plan, err := PrepareScale(p, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
	require.Contains(t, plan.Details().Issues, "ReportedDesiredCountDiscrepancy")
	for _, partition := range []int32{-1, 4} {
		evidence.replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(partition)}
		_, err := PrepareScale(p, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
		require.ErrorIs(t, err, ErrScaleBounds)
	}
	evidence.replica.Spec.Pacing.Partition = ptr.To[int32](3)
	_, err = PrepareScale(p, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, "dev-fra", "ome", reportv1alpha1.DryRunClient))
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, len(line), 80, line)
	}
	require.NotContains(t, out.String(), "private-scale-selector")
}

func TestScaleLifecycleRetainedHistoryAndActiveWork(t *testing.T) {
	for _, kind := range []v1beta1.InstanceOperationType{v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationUpdate, v1beta1.InstanceOperationDelete} {
		for _, failed := range []bool{false, true} {
			t.Run(string(kind)+fmt.Sprint(failed), func(t *testing.T) {
				p, _, r, s, source := scaleFixture(t)
				phase := map[v1beta1.InstanceOperationType]v1beta1.OMENativeInstancePhase{v1beta1.InstanceOperationCreate: v1beta1.OMENativeInstanceCreating, v1beta1.InstanceOperationRestart: v1beta1.OMENativeInstanceRestarting, v1beta1.InstanceOperationUpdate: v1beta1.OMENativeInstanceUpdating, v1beta1.InstanceOperationDelete: v1beta1.OMENativeInstanceDeleting}[kind]
				if failed {
					phase = v1beta1.OMENativeInstanceFailed
				}
				r.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: phase, Operation: &v1beta1.InstanceOperation{ID: "op-1", Type: kind, Step: "Drain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9)), TargetRevision: r.Status.UpdateRevision}}}
				_, err := InspectScaleEvidence(p, r, s, source, testClock)
				if failed && (kind == v1beta1.InstanceOperationCreate || kind == v1beta1.InstanceOperationRestart) {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrScaleWork)
				}
			})
		}
	}
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed, v1beta1.MigrationPhaseRelocated, v1beta1.MigrationPhaseSurgePending} {
		t.Run(string(phase), func(t *testing.T) {
			p, _, r, s, source := scaleFixture(t)
			r.Status.Migrations = []v1beta1.MigrationStatus{validMigration(phase)}
			_, err := InspectScaleEvidence(p, r, s, source, testClock)
			if phase.Terminal() {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestScaleTransientPhaseWithoutOperationRefuses(t *testing.T) {
	for _, phase := range []v1beta1.OMENativeInstancePhase{v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceDeleting} {
		t.Run(string(phase), func(t *testing.T) {
			p, _, r, s, source := scaleFixture(t)
			r.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: phase}}
			_, err := InspectScaleEvidence(p, r, s, source, testClock)
			require.Error(t, err)
		})
	}
}

func TestScalePacingRollbackMailboxRefuses(t *testing.T) {
	p, _, r, s, source := scaleFixture(t)
	r.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("chat-engine-aaaaaaaa")}
	_, err := InspectScaleEvidence(p, r, s, source, testClock)
	require.Error(t, err)
}

func TestScaleCompletedPinnedMultiComponentPlanIsNotActiveWork(t *testing.T) {
	p, _, r, _, _ := scaleFixture(t)
	p.Spec.Decoder = &v1beta1.DecoderSpec{}
	pinnedCanary(t, p)
	group := &p.Status.Rollout.ActiveRun.Plan.Groups[0]
	group.Group.Components = append(group.Group.Components, v1beta1.DecoderComponent)
	digest, err := rolloutpolicy.ProgressionDigest(&group.Group)
	require.NoError(t, err)
	group.PortableDigest = digest
	p.Status.Rollout.ActiveRun.TargetRevisions = append(p.Status.Rollout.ActiveRun.TargetRevisions, v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "bbbbbbbb"})
	engine := p.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseStable
	engine.LatestRolledoutRevision = "chat-engine-rev-bbbbbbbb"
	engine.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100}}
	p.Status.Components[v1beta1.EngineComponent] = engine
	p.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{RolloutPhase: v1beta1.RolloutPhaseStable, LatestRolledoutRevision: "chat-decoder-rev-bbbbbbbb", LatestReadyRevision: "chat-decoder-rev-bbbbbbbb", Traffic: []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-decoder-rev-bbbbbbbb", Percent: 100}}}
	p.Status.Canary.CurrentStep = 2
	p.Status.Canary.ObservedTrafficWeight = 100
	p.Status.Canary.StableRevisionHash = ""
	p.Status.Canary.TargetID = "ct1:" + rolloutpolicy.ShortHash([]byte("decoder=bbbbbbbb;engine=bbbbbbbb"))
	require.True(t, pinnedevidence.ValidActiveRun(p), "fixture must satisfy the real pure pinned-plan validator")
	projection, err := rolloutprojection.Project(p, testClock)
	require.NoError(t, err)
	for _, issue := range projection.Content.Issues {
		require.Equal(t, reportv1alpha1.RolloutIssueEpochUnverifiable, issue.Code, "fixture must have no malformed applicable rollout evidence")
	}
	for _, observed := range projection.Content.Groups {
		require.Equal(t, reportv1alpha1.RolloutPhaseStable, observed.Phase)
	}
	sibling := r.DeepCopy()
	sibling.Name = "chat-decoder"
	sibling.UID = "uid-decoder"
	sibling.ResourceVersion = "91"
	sibling.Spec.Component = v1beta1.DecoderComponent
	sibling.Status.CurrentRevision = "chat-decoder-bbbbbbbb"
	sibling.Status.UpdateRevision = sibling.Status.CurrentRevision
	require.NoError(t, inspectScaleTargetReplica(p, r, v1beta1.EngineComponent, r.Name, testClock))
	require.NoError(t, inspectScaleTargetReplica(p, sibling, v1beta1.DecoderComponent, sibling.Name, testClock))
	evidence := ScaleEvidence{parent: p, replica: r, replicas: map[v1beta1.ComponentType]*v1beta1.InferenceReplica{v1beta1.EngineComponent: r.DeepCopy(), v1beta1.DecoderComponent: sibling.DeepCopy()}}
	require.NoError(t, requireScaleStable(p, evidence, v1beta1.EngineComponent, testClock))
	delete(evidence.replicas, v1beta1.DecoderComponent)
	require.ErrorIs(t, requireScaleStable(p, evidence, v1beta1.EngineComponent, testClock), ErrStale, "the other target must retain authoritative IR evidence")
}
