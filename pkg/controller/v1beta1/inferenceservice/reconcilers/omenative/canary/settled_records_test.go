package canary

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The rolled-out record of a canary-owned Component is the executor's to
// write. Inside an armed ladder it is the memory of a traffic shift; outside
// one it mirrors the InferenceReplica, so a record cannot keep naming a
// revision the fleet left.

// settledIR is an InferenceReplica whose current and target revision agree
// on hash, with its status observed at its generation.
func settledIR(ns, isvc string, comp v1beta1.ComponentType, hash string) *v1beta1.InferenceReplica {
	r := ir(ns, isvc, comp, hash)
	r.Status.CurrentRevision = r.Status.UpdateRevision
	r.Generation, r.Status.ObservedGeneration = 1, 1
	return r
}

func revisionServiceName(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) string {
	return coordination.PerRevisionServiceName(irprojector.RoleReplicaPrefix(isvc, comp), comp, hash)
}

// idleRecordFixture seeds a single-engine canary service with no ladder
// armed, its engine settled on "cur" with two Ready pods, and a rolled-out
// record still naming "stale".
func idleRecordFixture(t *testing.T, name string, objects ...runtime.Object) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n2 := 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {LatestRolledoutRevision: revisionServiceName(isvc, v1beta1.EngineComponent, "stale")},
	}
	objects = append(objects, isvc, settledIR(ns, name, v1beta1.EngineComponent, "cur"),
		canaryPod(ns, name, "engine", "cur", "cur-0"),
		canaryPod(ns, name, "engine", "cur", "cur-1"))
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()
	return isvc, c
}

func dispatchOnce(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) {
	t.Helper()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
}

// With no ladder armed, a member settled on one revision is recorded on it:
// the stale record is demoted to the previous revision and the ready
// revision is brought level, and no canary state is armed in the process.
func TestDispatch_IdleMemberRecordFollowsItsInferenceReplica(t *testing.T) {
	isvc, c := idleRecordFixture(t, "idle-record")
	dispatchOnce(t, isvc, c)

	cs := isvc.Status.Components[v1beta1.EngineComponent]
	want := revisionServiceName(isvc, v1beta1.EngineComponent, "cur")
	if cs.LatestRolledoutRevision != want {
		t.Fatalf("LatestRolledoutRevision = %q, want %q (the revision the engine stands on)", cs.LatestRolledoutRevision, want)
	}
	if stale := revisionServiceName(isvc, v1beta1.EngineComponent, "stale"); cs.PreviousRolledoutRevision != stale {
		t.Fatalf("PreviousRolledoutRevision = %q, want the demoted %q", cs.PreviousRolledoutRevision, stale)
	}
	if cs.LatestReadyRevision != want {
		t.Fatalf("LatestReadyRevision = %q, want %q", cs.LatestReadyRevision, want)
	}
	if cs.Canary != nil {
		t.Fatalf("an idle unit must arm no canary state, got %+v", cs.Canary)
	}
}

// A record already level with the InferenceReplica is left alone: the
// previous revision is not collapsed onto it.
func TestDispatch_IdleMemberRecordAlreadyLevelIsUnchanged(t *testing.T) {
	isvc, c := idleRecordFixture(t, "idle-level")
	cur := revisionServiceName(isvc, v1beta1.EngineComponent, "cur")
	isvc.Status.Components[v1beta1.EngineComponent] = v1beta1.ComponentStatusSpec{
		LatestRolledoutRevision:   cur,
		PreviousRolledoutRevision: revisionServiceName(isvc, v1beta1.EngineComponent, "older"),
	}
	dispatchOnce(t, isvc, c)

	cs := isvc.Status.Components[v1beta1.EngineComponent]
	if cs.LatestRolledoutRevision != cur || cs.PreviousRolledoutRevision != revisionServiceName(isvc, v1beta1.EngineComponent, "older") {
		t.Fatalf("a level record must not move, got latest=%q previous=%q", cs.LatestRolledoutRevision, cs.PreviousRolledoutRevision)
	}
}

// A member whose pods still span two revisions has not settled, and a member
// with no Ready pod on its revision owns no traffic yet: neither is recorded.
func TestDispatch_IdleMemberNotSettledKeepsItsRecord(t *testing.T) {
	ns := "default"
	cases := []struct {
		name string
		pods []runtime.Object
	}{{
		name: "a pod of another revision remains",
		pods: []runtime.Object{canaryPod(ns, "idle-mixed", "engine", "stale", "stale-2")},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, c := idleRecordFixture(t, "idle-mixed", tc.pods...)
			dispatchOnce(t, isvc, c)
			if got, stale := isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision, revisionServiceName(isvc, v1beta1.EngineComponent, "stale"); got != stale {
				t.Fatalf("LatestRolledoutRevision = %q, want the untouched %q", got, stale)
			}
		})
	}
	t.Run("no pod is Ready", func(t *testing.T) {
		name := "idle-unready"
		n2 := 2
		isvc := canaryISVC(twoStep(), nil)
		isvc.Namespace, isvc.Name = ns, name
		isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
		stale := revisionServiceName(isvc, v1beta1.EngineComponent, "stale")
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {LatestRolledoutRevision: stale}}
		unready := canaryPod(ns, name, "engine", "cur", "cur-0")
		unready.Status.Conditions = nil
		c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
			isvc, settledIR(ns, name, v1beta1.EngineComponent, "cur"), unready).Build()
		dispatchOnce(t, isvc, c)
		if got := isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision; got != stale {
			t.Fatalf("LatestRolledoutRevision = %q, want the untouched %q", got, stale)
		}
	})
}

// Every member of an idle unit follows its own InferenceReplica, the
// secondary through the pair the dispatcher observed for it.
func TestDispatch_IdlePairRecordsFollowBothInferenceReplicas(t *testing.T) {
	ns, name := "default", "idle-pair"
	n2 := 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent:  {LatestRolledoutRevision: revisionServiceName(isvc, v1beta1.EngineComponent, "stale")},
		v1beta1.DecoderComponent: {LatestRolledoutRevision: revisionServiceName(isvc, v1beta1.DecoderComponent, "dstale")},
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc,
		settledIR(ns, name, v1beta1.EngineComponent, "cur"),
		settledIR(ns, name, v1beta1.DecoderComponent, "dcur"),
		canaryPod(ns, name, "engine", "cur", "cur-0"), canaryPod(ns, name, "engine", "cur", "cur-1"),
		canaryPod(ns, name, "decoder", "dcur", "dcur-0"), canaryPod(ns, name, "decoder", "dcur", "dcur-1"),
	).Build()
	dispatchOnce(t, isvc, c)

	for comp, hash := range map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "cur", v1beta1.DecoderComponent: "dcur"} {
		if got, want := isvc.Status.Components[comp].LatestRolledoutRevision, revisionServiceName(isvc, comp, hash); got != want {
			t.Fatalf("%s LatestRolledoutRevision = %q, want %q", comp, got, want)
		}
	}
}

// Inside an armed ladder the record is the ladder's: it names the stable
// revision the ladder shifts traffic from until the promotion or the revert
// completes, even on a pass where every pod of the member runs the canary
// revision.
func TestDispatch_ArmedMemberRecordIsLeftToTheLadder(t *testing.T) {
	ns := "default"
	cases := []struct {
		name string
		pods []string
	}{
		{name: "pods split across the pair", pods: []string{"old", "old", "new", "new"}},
		{name: "every pod on the canary revision", pods: []string{"new", "new", "new", "new"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := "armed-record"
			n4 := 4
			isvc := canaryISVC(twoStep(), nil)
			isvc.Namespace, isvc.Name = ns, name
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
			old := revisionServiceName(isvc, v1beta1.EngineComponent, "old")
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					LatestRolledoutRevision: old,
					RolloutPhase:            v1beta1.RolloutPhasePaused,
					Canary:                  &v1beta1.CanaryStatus{CanaryRevisionHash: "new", StableRevisionHash: "old", CurrentStep: 0, ObservedTrafficWeight: 50},
				},
			}
			pinActiveRun(isvc)
			isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			}
			engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
			engineIR.Status.CurrentRevision = name + "-engine-old"
			objects := []runtime.Object{isvc, engineIR,
				canaryControllerRevision(ns, name, "engine", "old", 1),
				canaryControllerRevision(ns, name, "engine", "new", 2)}
			for i, hash := range tc.pods {
				objects = append(objects, canaryPod(ns, name, "engine", hash, hash+"-"+string(rune('0'+i))))
			}
			c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()
			dispatchOnce(t, isvc, c)

			if got := isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision; got != old {
				t.Fatalf("LatestRolledoutRevision = %q, want the ladder's %q", got, old)
			}
		})
	}
}
