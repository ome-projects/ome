package rolloutrun

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A run opens for the whole InferenceService, so one group's retarget pins
// a target for every grouped Component. The tests here cover the Components
// that did not move: their stable revision is read from the InferenceReplica
// they stand on, and the status records answer only where the IR cannot.

const (
	// The revision the engine fleet stands on, current and target alike.
	engineSettledRev = "llm-a-engine-cccccccc"
	// A rolled-out record that names a revision no Instance runs any more.
	engineStaleRecord = "llm-a-engine-99999999"
	routerOldRev      = "llm-a-router-r1111111"
	routerNewRev      = "llm-a-router-r2222222"
)

// groupedFixture is isvcFixture over several rollout groups, with a router
// so a router group has a Component to pin.
func groupedFixture(groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
	isvc := isvcFixture(groups[0])
	concurrent := v1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{GroupOrdering: &concurrent, Groups: groups}
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

// irCounted publishes one Component's IR snapshot with explicit replica
// counters: updated < replicas is a Component whose revision pair agrees
// while Instances still trail it.
func irCounted(comp v1beta1.ComponentType, current, target string, replicas, updated int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a-" + string(comp), Namespace: "ns", Generation: 1},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			CurrentRevision:    current,
			UpdateRevision:     target,
			Replicas:           replicas,
			UpdatedReplicas:    updated,
		},
	}
}

func pinnedByComponent(t *testing.T, isvc *v1beta1.InferenceService) map[v1beta1.ComponentType]v1beta1.RolloutRunTarget {
	t.Helper()
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		t.Fatal("no run pinned")
	}
	out := map[v1beta1.ComponentType]v1beta1.RolloutRunTarget{}
	for _, target := range isvc.Status.Rollout.ActiveRun.TargetRevisions {
		out[target.Component] = target
	}
	return out
}

// openRouterRunBesideEngine opens the run a router-only change produces on a
// service with a router group and an engine group, where the engine's IR
// pair agrees on engineSettledRev and its status records are engineStatus.
func openRouterRunBesideEngine(t *testing.T, engineStatus v1beta1.ComponentStatusSpec, engineReplicas, engineUpdated int32) *v1beta1.InferenceService {
	t.Helper()
	isvc := groupedFixture(
		v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: canaryBody(10, 100)},
		v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryBody(10, 100)},
	)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: engineStatus,
	}
	in := testInputs(t, isvc,
		irCounted(v1beta1.RouterComponent, routerOldRev, routerNewRev, 2, 0),
		irCounted(v1beta1.EngineComponent, engineSettledRev, engineSettledRev, engineReplicas, engineUpdated),
	)
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Opened || out.Adopted {
		t.Fatalf("the router change must open a fresh run, got %+v", out)
	}
	return isvc
}

// unitIdleInRun mirrors the canary executor's arming rule: a unit whose
// every pinned member has revision == stable has no work in the run.
func unitIdleInRun(active *v1beta1.RolloutRun, g *v1beta1.RolloutGroup) bool {
	members := map[v1beta1.ComponentType]bool{}
	for _, c := range g.Components {
		members[c] = true
	}
	seen := false
	for _, target := range active.TargetRevisions {
		if !members[target.Component] {
			continue
		}
		seen = true
		if target.Revision != target.StableRevision {
			return false
		}
	}
	return seen
}

// The engine fleet stands whole on one revision and only the router moved.
// The engine's pinned stable is the revision it stands on, not the one its
// rolled-out record remembers: the record would make a revision with no
// Instance the engine's rollback target and arm a ladder with no work.
func TestOpenPinsSettledUnitAtItsOwnRevisionOverStaleRolledOutRecord(t *testing.T) {
	isvc := openRouterRunBesideEngine(t, v1beta1.ComponentStatusSpec{
		LatestRolledoutRevision: engineStaleRecord,
	}, 4, 4)

	pinned := pinnedByComponent(t, isvc)
	engine := pinned[v1beta1.EngineComponent]
	if engine.Revision != "cccccccc" || engine.StableRevision != "cccccccc" {
		t.Fatalf("engine pinned %+v, want revision and stable both at the revision it stands on", engine)
	}
	router := pinned[v1beta1.RouterComponent]
	if router.Revision != "r2222222" || router.StableRevision != "r1111111" {
		t.Fatalf("router pinned %+v, want r2222222 over stable r1111111", router)
	}
}

// The same with the stale identity in the unit's canary record.
func TestOpenPinsSettledUnitAtItsOwnRevisionOverStaleCanaryRecord(t *testing.T) {
	isvc := openRouterRunBesideEngine(t, v1beta1.ComponentStatusSpec{
		Canary: &v1beta1.CanaryStatus{StableRevisionHash: "99999999"},
	}, 4, 4)

	engine := pinnedByComponent(t, isvc)[v1beta1.EngineComponent]
	if engine.Revision != "cccccccc" || engine.StableRevision != "cccccccc" {
		t.Fatalf("engine pinned %+v, want revision and stable both at the revision it stands on", engine)
	}
}

// Pinning revision == stable is what keeps the unit out of the run: the
// executor arms a ladder only for a unit whose pinned revision differs from
// its pinned stable.
func TestSettledUnitHasNoWorkInAnotherGroupsRun(t *testing.T) {
	isvc := openRouterRunBesideEngine(t, v1beta1.ComponentStatusSpec{
		LatestRolledoutRevision: engineStaleRecord,
	}, 4, 4)

	active := isvc.Status.Rollout.ActiveRun
	if !unitIdleInRun(active, &isvc.Spec.Rollout.Groups[1]) {
		t.Fatal("the settled engine unit must read idle in the router's run")
	}
	if unitIdleInRun(active, &isvc.Spec.Rollout.Groups[0]) {
		t.Fatal("the router unit retargeted and must arm")
	}
}

// A revision pair that agrees while Instances trail is a pending roll, the
// state a mid-roll revert leaves behind, not a settled unit: its stable
// still comes from the record, so the revert arms.
func TestStragglingUnitResolvesStableFromItsRecord(t *testing.T) {
	isvc := openRouterRunBesideEngine(t, v1beta1.ComponentStatusSpec{
		LatestRolledoutRevision: engineStaleRecord,
	}, 4, 1)

	engine := pinnedByComponent(t, isvc)[v1beta1.EngineComponent]
	if engine.StableRevision != "99999999" {
		t.Fatalf("straggling engine stable = %q, want the recorded 99999999", engine.StableRevision)
	}
}

// A run adopting a ladder in flight keeps the records only for the unit
// whose ladder it adopts. The router, rolled fully onto its canary revision
// mid-ladder, resolves the stable it shifts traffic from out of its canary
// record; the engine, settled on its revision in another unit, is pinned
// where it stands and sits the run out, whatever its rolled-out record says.
func TestAdoptingRunKeepsRecordsOnlyForTheUnitInFlight(t *testing.T) {
	isvc := groupedFixture(
		v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: canaryBody(10, 100)},
		v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryBody(10, 100)},
	)
	// The router's ladder is live with no run pinned: the open adopts it.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {Canary: &v1beta1.CanaryStatus{CanaryRevisionHash: "r2222222", StableRevisionHash: "r1111111", CurrentStep: 0}},
		v1beta1.EngineComponent: {LatestRolledoutRevision: engineStaleRecord},
	}
	in := testInputs(t, isvc,
		irCounted(v1beta1.RouterComponent, routerNewRev, routerNewRev, 1, 1),
		irCounted(v1beta1.EngineComponent, engineSettledRev, engineSettledRev, 4, 4),
	)
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Opened || !out.Adopted {
		t.Fatalf("a live ladder with no run must be adopted, got %+v", out)
	}
	pinned := pinnedByComponent(t, isvc)
	router := pinned[v1beta1.RouterComponent]
	if router.Revision != "r2222222" || router.StableRevision != "r1111111" {
		t.Fatalf("adopted router pinned %+v, want r2222222 over the stable its record names, r1111111", router)
	}
	engine := pinned[v1beta1.EngineComponent]
	if engine.Revision != "cccccccc" || engine.StableRevision != "cccccccc" {
		t.Fatalf("engine pinned %+v, want revision and stable both at the revision it stands on", engine)
	}
	active := isvc.Status.Rollout.ActiveRun
	if !unitIdleInRun(active, &isvc.Spec.Rollout.Groups[1]) {
		t.Fatal("the settled engine unit must read idle in the adopted run")
	}
	if unitIdleInRun(active, &isvc.Spec.Rollout.Groups[0]) {
		t.Fatal("the adopted router unit has its ladder to finish")
	}
}

func TestComponentStableRevision(t *testing.T) {
	withRecord := func() *v1beta1.InferenceService {
		isvc := isvcFixture(v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     canaryBody(10, 100),
		})
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {LatestRolledoutRevision: engineStaleRecord},
		}
		return isvc
	}
	// withoutRecord is a Component nothing has recorded yet: no rolled-out
	// revision fields and no canary record.
	withoutRecord := func() *v1beta1.InferenceService {
		return isvcFixture(v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     canaryBody(10, 100),
		})
	}
	// withCanaryRecord carries both records, so the unit's stable identity
	// is asked before the rolled-out revision.
	withCanaryRecord := func() *v1beta1.InferenceService {
		isvc := withRecord()
		isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "cccccccc", StableRevisionHash: "77777777"}
		return isvc
	}

	cases := []struct {
		name     string
		isvc     func() *v1beta1.InferenceService
		pinned   string
		target   targetPair
		adopting bool
		want     string
	}{{
		name:   "a current revision off the pin is the stable",
		pinned: "bbbbbbbb",
		target: targetPair{current: "aaaaaaaa", target: "bbbbbbbb", replicas: 2, updated: 0},
		want:   "aaaaaaaa",
	}, {
		name:   "settled on the pin: the IR, not the record",
		pinned: "cccccccc",
		target: targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 4},
		want:   "cccccccc",
	}, {
		name:   "settled with no Instance at all is still settled",
		pinned: "cccccccc",
		target: targetPair{current: "cccccccc", target: "cccccccc"},
		want:   "cccccccc",
	}, {
		name:   "stragglers are a pending roll: the record stands",
		pinned: "cccccccc",
		target: targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 1},
		want:   "99999999",
	}, {
		name:     "an adopting run keeps the record",
		pinned:   "cccccccc",
		target:   targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 4},
		adopting: true,
		want:     "99999999",
	}, {
		name:   "no IR revision falls through to the record",
		pinned: "cccccccc",
		target: targetPair{},
		want:   "99999999",
	}, {
		name:   "stragglers with no record fall back to the current revision",
		isvc:   withoutRecord,
		pinned: "cccccccc",
		target: targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 1},
		want:   "cccccccc",
	}, {
		name:   "no IR revision and no record pins no stable",
		isvc:   withoutRecord,
		pinned: "cccccccc",
		target: targetPair{},
		want:   "",
	}, {
		name:     "an adopting run with nothing on record pins no stable",
		isvc:     withoutRecord,
		pinned:   "cccccccc",
		target:   targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 4},
		adopting: true,
		want:     "",
	}, {
		name:     "an adopting unit's stable identity answers before the record",
		isvc:     withCanaryRecord,
		pinned:   "cccccccc",
		target:   targetPair{current: "cccccccc", target: "cccccccc", replicas: 4, updated: 4},
		adopting: true,
		want:     "77777777",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := withRecord
			if tc.isvc != nil {
				isvc = tc.isvc
			}
			got := componentStableRevision(isvc(), v1beta1.EngineComponent, tc.pinned, tc.target, tc.adopting)
			if got != tc.want {
				t.Fatalf("componentStableRevision = %q, want %q", got, tc.want)
			}
		})
	}
}
