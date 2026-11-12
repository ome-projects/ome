package canary

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// stableLink names which link of the stable chooser answers for the member
// whose replica was projected again.
type stableLink int

const (
	// linkLastRun: the rolled-back run record carries the member's stable.
	linkLastRun stableLink = iota
	// linkCanaryRecord: no run record is on file; the primary's canary
	// record carries the stable identity.
	linkCanaryRecord
	// linkRolledOut: a secondary with no run record and no canary record of
	// its own; its rolled-out revision fields carry the stable.
	linkRolledOut
)

// serviceOwned gives a ControllerRevision the service as a plain owner
// beside the replica that controls it, as the replica controller mints it.
func serviceOwned(cr *appsv1.ControllerRevision, isvc *v1beta1.InferenceService) *appsv1.ControllerRevision {
	cr.OwnerReferences = append(cr.OwnerReferences, metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: isvc.Name, UID: isvc.UID,
	})
	return cr
}

// reprojectedReplica is the replica the projector creates again for a
// Component under a standing rollback: a new UID, the service as its
// controller, and the stable revision as its rollback target, so the
// replica controller builds its Instances on that revision.
func reprojectedReplica(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, specHash, stableHash string) *v1beta1.InferenceReplica {
	replica := ir(isvc.Namespace, isvc.Name, comp, specHash)
	replica.UID = canaryIRUID(replica.Name) + "-2"
	replica.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
	replica.Spec.ParentRef = &v1beta1.ParentReference{Name: isvc.Name}
	replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(int32(0)), RollbackToRevision: ptr.To(isvc.Name + "-" + string(comp) + "-" + stableHash)}
	replica.Status.CurrentRevision = isvc.Name + "-" + string(comp) + "-" + stableHash
	return replica
}

// reProjectedHoldFixture builds a settled rollback hold whose replica was
// deleted and projected again: the engine's canary record holds the
// rejection of version 2 with version 1 as the stable revision, the run
// closed RolledBack. The revisions the deleted replica minted are the
// service's, so they are still there, controlled by the deleted replica and
// owned by the service. The projector created the new replica pointed at
// version 1, and its 4 Instances came up on it. Under linkRolledOut the
// unit also has a decoder, projected again the same way, whose stable
// revision only its rolled-out record names.
func reProjectedHoldFixture(t *testing.T, name string, link stableLink) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n4, n2 := 4, 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.UID = types.UID(name + "-isvc-uid")
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	engine.LatestRolledoutRevision = coordination.PerRevisionServiceName(name, v1beta1.EngineComponent, "old")
	engine.LatestReadyRevision = engine.LatestRolledoutRevision
	isvc.Status.Components[v1beta1.EngineComponent] = engine

	objs := []runtime.Object{isvc,
		reprojectedReplica(isvc, v1beta1.EngineComponent, "new", "old"),
		serviceOwned(canaryControllerRevision(ns, name, "engine", "old", 1), isvc),
		serviceOwned(canaryControllerRevision(ns, name, "engine", "new", 2), isvc),
	}
	for _, pod := range []string{"0", "1", "2", "3"} {
		objs = append(objs, canaryPod(ns, name, "engine", "old", "old-"+pod))
	}
	switch link {
	case linkLastRun:
		isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
			Outcome:         v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"}},
		}}
	case linkCanaryRecord:
		// No run record on file: a status written by a controller without
		// the run model, or one whose record was cleared.
	case linkRolledOut:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
		isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
			Outcome:         v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"}},
		}}
		decoder := v1beta1.ComponentStatusSpec{}
		decoder.LatestRolledoutRevision = coordination.PerRevisionServiceName(name, v1beta1.DecoderComponent, "decold")
		decoder.LatestReadyRevision = decoder.LatestRolledoutRevision
		isvc.Status.Components[v1beta1.DecoderComponent] = decoder
		objs = append(objs,
			reprojectedReplica(isvc, v1beta1.DecoderComponent, "decnew", "decold"),
			canaryPod(ns, name, "decoder", "decold", "decold-0"),
			canaryPod(ns, name, "decoder", "decold", "decold-1"),
			serviceOwned(canaryControllerRevision(ns, name, "decoder", "decold", 1), isvc),
			serviceOwned(canaryControllerRevision(ns, name, "decoder", "decnew", 2), isvc),
		)
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	return isvc, c
}

// A settled rollback survives the replica's deletion and projection again.
// The revisions the deleted replica minted are the service's, so the new
// replica finds version 1 and the projection pointed it there; the dispatch
// resolves the member's stable revision from the rolled-back run's record,
// from the canary record when none is on file, or from a secondary's own
// rolled-out record, finds it retained for the new replica, keeps the
// revert target and reads the unit RolledBack on the first pass. The record
// stays level with the revision every Instance runs.
func TestDispatch_ASettledHoldOnAReProjectedReplicaKeepsItsRevertTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		link   stableLink
		member v1beta1.ComponentType
	}{
		{"the rolled-back run record names the engine's stable revision", linkLastRun, v1beta1.EngineComponent},
		{"the canary record names the engine's stable revision", linkCanaryRecord, v1beta1.EngineComponent},
		{"the decoder's rolled-out record names its stable revision", linkRolledOut, v1beta1.DecoderComponent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, c := reProjectedHoldFixture(t, "reprojected", tc.link)
			record := isvc.Status.Components[tc.member].LatestRolledoutRevision
			target := *mustReplica(t, c, isvc, tc.member).Spec.Pacing.RollbackToRevision
			deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
			for pass := 0; pass < 2; pass++ {
				if _, err := Dispatch(context.Background(), deps); err != nil {
					t.Fatalf("Dispatch pass %d: %v", pass, err)
				}
				cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
				if cs == nil || cs.Failed != nil || cs.RolledBackRevisionHash != "new" {
					t.Fatalf("pass %d: the hold must stand with the rejection kept and no Failed record, got %+v", pass, cs)
				}
				if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRolledBack {
					t.Fatalf("pass %d: phase %q, want RolledBack", pass, got)
				}
				replica := mustReplica(t, c, isvc, tc.member)
				if replica.Spec.Pacing == nil || replica.Spec.Pacing.RollbackToRevision == nil || *replica.Spec.Pacing.RollbackToRevision != target {
					t.Fatalf("pass %d: the new replica must stay pointed at %s, got %+v", pass, target, replica.Spec.Pacing)
				}
				if got := isvc.Status.Components[tc.member].LatestRolledoutRevision; got != record {
					t.Fatalf("pass %d: the record must stay level with the revision the Instances run, got %q want %q", pass, got, record)
				}
			}
		})
	}
}

// A replica projected again before the projection pointed it at the stable
// revision comes up on the spec's rejected revision; the dispatch finds the
// stable revision retained for it all the same, writes the revert target on
// the first pass and reads the revert in flight, so the replica controller
// rolls the Instances back instead of the unit stopping Failed.
func TestDispatch_AReProjectedReplicaWithoutATargetIsPointedAtTheStableRevision(t *testing.T) {
	isvc, c := reProjectedHoldFixture(t, "reprojected", linkLastRun)
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	replica.Spec.Pacing.RollbackToRevision = nil
	replica.Status.CurrentRevision = isvc.Name + "-engine-new"
	if err := c.Update(context.Background(), replica); err != nil {
		t.Fatal(err)
	}
	for _, pod := range []string{"0", "1", "2", "3"} {
		if err := c.Delete(context.Background(), canaryPod(isvc.Namespace, isvc.Name, "engine", "old", "old-"+pod)); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(context.Background(), canaryPod(isvc.Namespace, isvc.Name, "engine", "new", "new-"+pod)); err != nil {
			t.Fatal(err)
		}
	}
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs == nil || cs.Failed != nil {
		t.Fatalf("the unit must not stop Failed while its stable revision is retained for the new replica, got %+v", cs)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("phase %q, want RollingBack while the Instances still run the rejected revision", got)
	}
	replica = mustReplica(t, c, isvc, v1beta1.EngineComponent)
	if replica.Spec.Pacing == nil || replica.Spec.Pacing.RollbackToRevision == nil || *replica.Spec.Pacing.RollbackToRevision != isvc.Name+"-engine-old" {
		t.Fatalf("the dispatch must point the new replica at the stable revision, got %+v", replica.Spec.Pacing)
	}
}

func mustReplica(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) *v1beta1.InferenceReplica {
	t.Helper()
	replica := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(comp)}, replica); err != nil {
		t.Fatal(err)
	}
	return replica
}
