package canary

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A canary of version 2 was rolled back onto version 1 and the hold has
// settled: no run is open, the unit reads RolledBack, and every pass
// resolves each member's stable revision to keep the revert signal on the
// replica. Then the stable revision's ControllerRevision leaves the history
// (the retention sweep collected it, or an operator deleted it) while the
// Instances run another revision, so the member's rolled-out record names a
// revision no Instance runs and reads Ahead. The dispatch resolves the
// stable revision from the link the member has, finds no retained
// ControllerRevision for it, stops the unit Failed for want of a stable
// revision, writes no revert target and leaves the record where it was.

// stableGoneHoldFixture builds the settled hold after the stable revision
// left the history: the engine's canary record holds the rejection, the
// engine runs 4 Instances of another revision, and only that revision's
// ControllerRevision is retained. Under linkRolledOut the unit also has a
// decoder whose stable revision only its rolled-out record names and whose
// ControllerRevision is gone; the engine's stable revision is retained so
// the Failed stop is the decoder's.
func stableGoneHoldFixture(t *testing.T, name string, link stableLink) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n4, n2 := 4, 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	engine.LatestRolledoutRevision = coordination.PerRevisionServiceName(name, v1beta1.EngineComponent, "old")
	engine.LatestReadyRevision = engine.LatestRolledoutRevision
	isvc.Status.Components[v1beta1.EngineComponent] = engine

	engineRevision := "fresh"
	objs := []runtime.Object{isvc}
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
		// The engine keeps its stable revision; the decoder, with no run
		// record and no canary record of its own, is the member whose
		// stable revision left the history.
		engineRevision = "old"
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
		decoderIR := ir(ns, name, v1beta1.DecoderComponent, "decfresh")
		decoderIR.Status.CurrentRevision = name + "-decoder-decfresh"
		objs = append(objs, decoderIR,
			canaryPod(ns, name, "decoder", "decfresh", "decfresh-0"),
			canaryPod(ns, name, "decoder", "decfresh", "decfresh-1"),
			canaryControllerRevision(ns, name, "decoder", "decfresh", 1),
			canaryControllerRevision(ns, name, "engine", "old", 1),
			canaryControllerRevision(ns, name, "engine", "new", 2),
		)
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, engineRevision)
	engineIR.Status.CurrentRevision = name + "-engine-" + engineRevision
	objs = append(objs, engineIR)
	for _, pod := range []string{"0", "1", "2", "3"} {
		objs = append(objs, canaryPod(ns, name, "engine", engineRevision, engineRevision+"-"+pod))
	}
	if link != linkRolledOut {
		objs = append(objs, canaryControllerRevision(ns, name, "engine", "fresh", 1))
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	return isvc, c
}

func TestDispatch_ASettledHoldWhoseStableRevisionIsGoneStopsFailedAtEveryLink(t *testing.T) {
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
			isvc, c := stableGoneHoldFixture(t, "stable-gone", tc.link)
			record := isvc.Status.Components[tc.member].LatestRolledoutRevision
			deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
			for pass := 0; pass < 2; pass++ {
				if _, err := Dispatch(context.Background(), deps); err != nil {
					t.Fatalf("Dispatch pass %d: %v", pass, err)
				}
				cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
				if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing || cs.RolledBackRevisionHash != "new" {
					t.Fatalf("pass %d: the hold must stop Failed for want of a stable revision with the rejection kept, got %+v", pass, cs)
				}
				if got := phaseOf(isvc); got != v1beta1.RolloutPhaseFailed {
					t.Fatalf("pass %d: phase %q, want Failed", pass, got)
				}
				replica := &v1beta1.InferenceReplica{}
				if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(tc.member)}, replica); err != nil {
					t.Fatal(err)
				}
				if replica.Spec.Pacing != nil && replica.Spec.Pacing.RollbackToRevision != nil {
					t.Fatalf("pass %d: no revert target must be written for a stable revision the history no longer carries: %+v", pass, replica.Spec.Pacing)
				}
				if got := isvc.Status.Components[tc.member].LatestRolledoutRevision; got != record {
					t.Fatalf("pass %d: the record must stay where the fleet left it, got %q want %q", pass, got, record)
				}
			}
		})
	}
}
