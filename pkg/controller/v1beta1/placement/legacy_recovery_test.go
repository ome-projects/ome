package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestLegacySingleUpdatesFailedMember(t *testing.T) {
	for _, tt := range []struct {
		name       string
		component  v1beta1.ComponentType
		annotation bool
		model      bool
		columnar   bool
	}{
		{name: "failed engine", component: v1beta1.EngineComponent},
		{name: "columnar failed engine", component: v1beta1.EngineComponent, columnar: true},
		{name: "failed router", component: v1beta1.RouterComponent},
		{name: "annotation placement", component: v1beta1.EngineComponent, annotation: true},
		{name: "model load failure", model: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(v1beta1.PlacementModeSingle)
			source.Finalizers = []string{PlacementFinalizer}
			source.Spec.Rollout = srcISVCWithRolloutRef("").Spec.Rollout
			source.Status.Placement = &v1beta1.PlacementStatus{
				Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted}},
			}
			if tt.annotation {
				source.Spec.Placement = nil
				source.Annotations[AcceleratorRequirementsAnnotation] = "accelerator=gpu-a"
			}
			worker := workerWithReplicaCounts(t, scheme, "svc.a.example.com", 3, 3)
			if tt.component == v1beta1.RouterComponent {
				source.Spec.Router = &v1beta1.RouterSpec{}
				member := &v1beta1.InferenceService{}
				require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(source), member))
				for _, object := range observedWorkerObjects(member, irWithInstances(v1beta1.RouterComponent, true, true))[2:] {
					require.NoError(t, worker.Create(t.Context(), object))
				}
			}
			other := emptyWorker(scheme)
			mutations := 0
			peers := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
				"a": workloadcluster.NewNeverCachingClient(mutationCountingClient{WithWatch: worker, mutations: &mutations}),
				"b": workloadcluster.NewNeverCachingClient(other),
			}}
			oldPolicy := canaryRolloutPolicy(testRolloutPolicyName)
			newPolicy := canaryRolloutPolicy("canary-corrected")
			newPolicy.Spec.Canary.Steps[0].Traffic = 25
			labels := map[string]string{
				"accelerator": "gpu-a",
				constants.WorkloadClusterRolloutPolicyLabel: WorkloadClusterRolloutPolicyCapability,
			}
			r, cp := newPlacer(scheme, peers, source, oldPolicy, newPolicy, readyWC("a", labels), readyWC("b", labels))
			r.InstanceStatusDecoder = irstatus.NewDecoder(uint64(*source.Spec.Engine.MinReplicas))
			_, err := r.Reconcile(t.Context(), req())
			require.NoError(t, err)
			before := &v1beta1.InferenceService{}
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(source), before))
			require.Equal(t, oldPolicy.Spec.Canary, before.Spec.Rollout.Groups[0].Canary)
			before.Annotations["worker.example.com/state"] = "retained"
			require.NoError(t, worker.Update(t.Context(), before))
			if tt.model {
				before.Status.ModelStatus.TransitionStatus = v1beta1.BlockedByFailedLoad
				require.NoError(t, worker.Status().Update(t.Context(), before))
			} else {
				ir := &v1beta1.InferenceReplica{}
				require.NoError(t, worker.Get(t.Context(), client.ObjectKey{Namespace: source.Namespace, Name: source.Name + "-" + string(tt.component)}, ir))
				ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
				ir.Status.InstanceStatuses[1].Phase = v1beta1.OMENativeInstanceReady
				if tt.columnar {
					for i := 2; i < len(ir.Status.InstanceStatuses); i++ {
						ir.Status.InstanceStatuses[i].Phase = v1beta1.OMENativeInstanceReady
					}
					ir.Status.InstanceStatusColumns, err = irstatus.EncodeColumns(ir.Status.InstanceStatuses, uint64(len(ir.Status.InstanceStatuses)))
					require.NoError(t, err)
					ir.Status.InstanceStatusEncoding = ptr.To(v1beta1.InstanceStatusEncodingColumnarV2)
					ir.Status.InstanceStatuses = nil
				}
				require.NoError(t, worker.Update(t.Context(), ir))
			}
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(source), before))
			live := &v1beta1.InferenceService{}
			require.NoError(t, cp.Get(t.Context(), client.ObjectKeyFromObject(source), live))
			live.Spec.Rollout.Groups[0].PolicyRef.Name = newPolicy.Name
			live.Spec.Engine.Runner.Container.Image = "image-corrected"
			live.Labels = map[string]string{"config-version": "corrected"}
			live.Generation++
			require.NoError(t, cp.Update(t.Context(), live))
			mutations = 0

			_, err = r.Reconcile(t.Context(), req())
			require.NoError(t, err)
			after := &v1beta1.InferenceService{}
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(source), after))
			require.Equal(t, newPolicy.Spec.Canary, after.Spec.Rollout.Groups[0].Canary)
			require.Nil(t, after.Spec.Rollout.Groups[0].PolicyRef)
			require.Equal(t, "0="+newPolicy.Name+"@"+mustPortableDigest(t, &newPolicy.Spec), after.Annotations[constants.RolloutPlanSourceAnnotation])
			require.Equal(t, "image-corrected", after.Spec.Engine.Runner.Container.Image)
			require.Equal(t, "corrected", after.Labels["config-version"])
			require.Equal(t, "retained", after.Annotations["worker.example.com/state"])
			require.Equal(t, before.UID, after.UID)
			require.Empty(t, cmp.Diff(before.Status, after.Status), "member status must remain owned by its controller")
			require.Equal(t, 1, mutations, "only the existing member receives an update")
			require.False(t, hasDerived(t, other), "a failed member must not trigger fanout")
			placement := cpPlacement(t, cp)
			require.Equal(t, "a", placement.Cluster)
			require.Equal(t, v1beta1.PlacementPhaseFailed, placement.Phase)
			require.Nil(t, placement.Endpoint)
			require.Nil(t, cpStatusURL(t, cp))

			_, err = r.Reconcile(t.Context(), req())
			require.NoError(t, err)
			require.Equal(t, 1, mutations, "unchanged failed members must not be rewritten")
		})
	}
}

type legacyRecoveryUnreadableClient struct{ client.WithWatch }

func (legacyRecoveryUnreadableClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return fmt.Errorf("member unavailable")
}

func TestLegacySingleFailedMemberPreservesPreflight(t *testing.T) {
	for _, condition := range []string{"disconnected", "unreadable", "ineligible", "missing policy", "missing capability"} {
		t.Run(condition, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(v1beta1.PlacementModeSingle)
			source.Finalizers = []string{PlacementFinalizer}
			source.Spec.Rollout = srcISVCWithRolloutRef("").Spec.Rollout
			source.Status.Placement = &v1beta1.PlacementStatus{
				Cluster: "a", Phase: v1beta1.PlacementPhaseFailed,
				Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitting}},
			}
			member := DeriveISVC(source, "", "")
			member.UID = "member-uid"
			member.Spec.Engine.Runner.Container.Image = "image-original"
			member.Status.ModelStatus.TransitionStatus = v1beta1.BlockedByFailedLoad
			worker, other := emptyWorker(scheme), emptyWorker(scheme)
			require.NoError(t, worker.Create(t.Context(), member))
			require.NoError(t, worker.Status().Update(t.Context(), member))
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(member), member))
			require.Equal(t, v1beta1.BlockedByFailedLoad, member.Status.ModelStatus.TransitionStatus)
			mutations := 0
			var access client.WithWatch = mutationCountingClient{WithWatch: worker, mutations: &mutations}
			if condition == "unreadable" {
				access = legacyRecoveryUnreadableClient{WithWatch: access}
			}
			peers := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
				"a": workloadcluster.NewNeverCachingClient(access), "b": workloadcluster.NewNeverCachingClient(other),
			}}
			if condition == "disconnected" {
				delete(peers.m, "a")
			}
			labels := map[string]string{"accelerator": "gpu-a", constants.WorkloadClusterRolloutPolicyLabel: WorkloadClusterRolloutPolicyCapability}
			winner, candidate := readyWC("a", labels), readyWC("b", labels)
			if condition == "ineligible" {
				winner.Status.Conditions[0].Status = metav1.ConditionFalse
			}
			if condition == "missing capability" {
				winner.Labels = map[string]string{"accelerator": "gpu-a"}
			}
			objects := []client.Object{source, winner, candidate}
			if condition != "missing policy" {
				objects = append(objects, canaryRolloutPolicy(testRolloutPolicyName))
			}
			r, cp := newPlacer(scheme, peers, objects...)
			_, err := r.Reconcile(t.Context(), req())
			require.NoError(t, err)
			after := &v1beta1.InferenceService{}
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(member), after))
			require.Empty(t, cmp.Diff(member, after))
			require.Zero(t, mutations)
			require.False(t, hasDerived(t, other))
			require.Equal(t, "a", cpPlacement(t, cp).Cluster)
		})
	}
}

func TestLegacyTerminalUpdateFences(t *testing.T) {
	for _, condition := range []string{"source changed", "source opted in", "foreign member", "planned member", "member changed", "member replaced", "member deleted"} {
		t.Run(condition, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(v1beta1.PlacementModeSingle)
			r, cp := newPlacer(scheme, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, source)
			require.NoError(t, cp.Get(t.Context(), client.ObjectKeyFromObject(source), source))
			worker := emptyWorker(scheme)
			member := DeriveISVC(source, "", "")
			member.UID = "member-uid"
			member.Spec.Engine.Runner.Container.Image = "image-original"
			if condition == "foreign member" {
				member.Labels[PlacementOriginLabel] = "another-source"
				member.Annotations[PlacementOriginUIDAnnotation] = "another-source"
			}
			if condition == "planned member" {
				encoded, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{SourceUID: source.UID, ClusterUID: "cluster-uid", PlanID: "plan", Revision: 1})
				require.NoError(t, err)
				member.Annotations[constants.PlacementExecution] = encoded
			}
			require.NoError(t, worker.Create(t.Context(), member))
			require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(member), member))
			observed := member.DeepCopy()
			switch condition {
			case "source changed", "source opted in":
				live := source.DeepCopy()
				live.Generation++
				if condition == "source opted in" {
					live.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
				}
				require.NoError(t, cp.Update(t.Context(), live))
			case "member changed":
				member.Annotations["worker.example.com/state"] = "changed"
				require.NoError(t, worker.Update(t.Context(), member))
			case "member replaced", "member deleted":
				require.NoError(t, worker.Delete(t.Context(), member))
				if condition == "member replaced" {
					member.UID = "replacement-uid"
					member.ResourceVersion = ""
					require.NoError(t, worker.Create(t.Context(), member))
					// The fake client's per-object versions need another write to
					// model globally unique API-server resource versions.
					member.Annotations["worker.example.com/state"] = "replacement"
					require.NoError(t, worker.Update(t.Context(), member))
				}
			}

			err := r.legacyUpdateTerminalMember(t.Context(), "a", worker, source, observed)
			require.Error(t, err)
			if condition == "member deleted" {
				require.False(t, hasDerived(t, worker), "recovery must not recreate a deleted member")
			} else {
				after := &v1beta1.InferenceService{}
				require.NoError(t, worker.Get(t.Context(), client.ObjectKeyFromObject(member), after))
				require.Empty(t, cmp.Diff(member, after), "a rejected correction must preserve the current object")
			}
		})
	}
}
