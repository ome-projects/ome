package replay

import (
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// scaleUnderAPushWalk pushes version 2 on four engine Instances and rolls
// Instance 3, the highest index, onto it. Instances 0, 1 and 2 still run
// version 1.
const scaleUnderAPushWalk = `scenario: scale-under-a-push
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 4, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        rollingUpdate: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - member.surgePodCreated: {component: engine, index: 3}
      - member.readyOnTarget: {component: engine, index: 3}
      - member.oldPodsGone: {component: engine, index: 3}
`

// scaledTo adds one tick per count to a walk, each scaling the engine to
// that count.
func scaledTo(walk string, first int, counts ...int) string {
	current := 4
	for i, count := range counts {
		variant := "down"
		if count > current {
			variant = "up"
		}
		walk += fmt.Sprintf("  - tick: %d\n    events:\n      - spec.replicas[%s]: {component: engine, to: %d}\n", first+i, variant, count)
		current = count
	}
	return walk
}

// engineRows lists the engine's published rows as index@version, in the
// order the replica carries them: v1 for a row on the current revision, v2
// for a row on the pushed one, and none for a row that runs no revision yet.
func engineRows(t *testing.T, view TickView) string {
	t.Helper()
	ir := view.Replicas[v1beta1.EngineComponent]
	if ir == nil {
		t.Fatal("the engine replica is missing from the view")
	}
	rows := make([]string, 0, len(ir.Status.InstanceStatuses))
	for _, row := range ir.Status.InstanceStatuses {
		version := "another"
		switch row.RunningRevision {
		case "":
			version = "none"
		case ir.Status.CurrentRevision:
			version = "v1"
		case ir.Status.UpdateRevision:
			version = "v2"
		}
		rows = append(rows, fmt.Sprintf("%d@%s", row.Index, version))
	}
	return strings.Join(rows, " ")
}

// A scale-down under a push removes an Instance the push has not moved.
// Four engines, the highest index on version 2, scaled to three: Instance 2
// leaves, the highest one still on version 1, and Instance 3 keeps serving
// version 2 from its pod. The engine's planner decides it, so the replay
// shows what the cluster shows.
func TestAScaleDownRemovesAnInstanceThePushHasNotMoved(t *testing.T) {
	view := lastTick(t, scaledTo(scaleUnderAPushWalk, 4, 3))
	if got, want := engineRows(t, view), "0@v1 1@v1 3@v2"; got != want {
		t.Fatalf("engine rows after the scale to three = %q, want %q: the scale must remove an Instance on version 1, not the one the push moved", got, want)
	}
	pods := enginePods(view)
	want := []string{"svc-a-engine-0-default-0", "svc-a-engine-1-default-0", "svc-a-engine-3-default-1"}
	if got := podNames(pods); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("engine pods after the scale to three = %v, want %v", got, want)
	}
	if !isReady(pods["svc-a-engine-3-default-1"]) {
		t.Errorf("the version 2 pod of Instance 3 is not Ready after the scale")
	}
}

// A scale-down below the count the push has not moved ends the version 1
// side: scaled to one, the Instance on version 2 is the one that stays.
func TestAScaleDownToOneKeepsTheInstanceOnTheNewVersion(t *testing.T) {
	view := lastTick(t, scaledTo(scaleUnderAPushWalk, 4, 1))
	if got, want := engineRows(t, view), "3@v2"; got != want {
		t.Fatalf("engine rows after the scale to one = %q, want %q", got, want)
	}
}

// A scale-down under a push whose retry record counts a failure removes the
// Instances on the failing version first. Four engines, Instance 3 on
// version 2; then the replacement of Instance 0 never starts and its attempt
// ends. Scaled to two, Instance 0 and Instance 3 leave, and the two healthy
// Instances on version 1 stay. The planner reads the member's retry record
// to rank them so.
func TestAScaleDownOnAFailingVersionRemovesItsInstancesFirst(t *testing.T) {
	walk := scaleUnderAPushWalk + `  - tick: 4
    events:
      - member.surgePodCreated: {component: engine, index: 0}
      - member.surgeFailed: {component: engine, index: 0}
`
	view := lastTick(t, scaledTo(walk, 5, 2))
	if got, want := engineRows(t, view), "1@v1 2@v1"; got != want {
		t.Fatalf("engine rows after the scale to two = %q, want %q: the Instance whose attempt ended and the Instance on the failing version leave before a healthy one", got, want)
	}
}

// A scale-up fills the lowest free index, as the engine's planner does:
// after Instance 2 left, the next Instance is Instance 2 again, a Create
// attempt that runs no revision until its pod is Ready.
func TestAScaleUpFillsTheLowestFreeIndex(t *testing.T) {
	view := lastTick(t, scaledTo(scaleUnderAPushWalk, 4, 3, 4))
	if got, want := engineRows(t, view), "0@v1 1@v1 3@v2 2@none"; got != want {
		t.Fatalf("engine rows after the scale back to four = %q, want %q", got, want)
	}
}

// With no push in flight every Instance ranks alike, and the highest index
// leaves first.
func TestAScaleDownAtRestRemovesTheHighestIndex(t *testing.T) {
	settled := strings.SplitAfter(scaleUnderAPushWalk, "    note: settled\n")[0]
	view := lastTick(t, scaledTo(settled, 2, 2))
	if got, want := engineRows(t, view), "0@v1 1@v1"; got != want {
		t.Fatalf("engine rows after the scale to two at rest = %q, want %q", got, want)
	}
}

// scaleUnderARollbackWalk stages eight engines at the first step of a
// canary, four on version 2 beside four on version 1, and then the operator
// rolls the canary back. The service pass of that tick points the replica
// at version 1 again while the four version 2 engines still run.
const scaleUnderARollbackWalk = `scenario: scale-under-a-rollback
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 8, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        canary:
          steps:
            - {capacity: "50%", traffic: 50, pause: {duration: 20s}}
            - {capacity: "100%", traffic: 100}
  config: {requeue: 10s, parkedRequeue: 1m, defaultReadyTimeout: 10m}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - ctrl.resync
  - tick: 4
    events:
      - capacity.reached: {component: engine}
  - tick: 5
    events:
      - verb.rollback
`

// engineVersions counts the engine's published rows by the image their
// running revision renders, as "v1=<n> v2=<n>". A pod's revision label
// names the image its revision renders.
func engineVersions(t *testing.T, view TickView) string {
	t.Helper()
	ir := view.Replicas[v1beta1.EngineComponent]
	if ir == nil {
		t.Fatal("the engine replica is missing from the view")
	}
	images := map[string]string{}
	for _, pod := range view.Pods[v1beta1.EngineComponent] {
		images[pod.Labels[query.LabelRevisionHash]] = pod.Spec.Containers[0].Image
	}
	counts := map[string]int{}
	for _, row := range ir.Status.InstanceStatuses {
		counts[images[query.RevisionFromName(row.RunningRevision).Hash()]]++
	}
	return fmt.Sprintf("v1=%d v2=%d", counts["registry.example.com/runtime:v1"], counts["registry.example.com/runtime:v2"])
}

// A scale-down while a rollback stands removes the Instances on the version
// the rollback leaves. Eight engines stand at a 50 percent step, four on
// each version, and the operator rolls back. One pass later the count drops
// to four: the four version 2 engines leave and version 1 keeps serving.
// The planner must hear that the rollout heads for version 1. Told the
// pushed version instead, it removes the four version 1 engines.
func TestAScaleDownUnderARollbackRemovesTheVersionRolledBackFrom(t *testing.T) {
	if got, want := engineVersions(t, lastTick(t, scaleUnderARollbackWalk)), "v1=4 v2=4"; got != want {
		t.Fatalf("engine rows when the rollback is asked = %q, want %q: the walk must hold both versions at the scale", got, want)
	}
	walk := scaleUnderARollbackWalk + "  - tick: 6\n    events:\n      - spec.replicas[down]: {component: engine, to: 4}\n"
	if got, want := engineVersions(t, lastTick(t, walk)), "v1=4 v2=0"; got != want {
		t.Fatalf("engine rows after the scale to four under the rollback = %q, want %q: the scale must remove the engines on the version the rollback leaves", got, want)
	}
}

// gangPushWalk pushes version 2 on two engine gangs. Gang 0 builds its
// replacement gang at the free index 2, the replacement never starts, and
// the third attempt ends with the retry record Held.
const gangPushWalk = `scenario: scale-after-a-failed-gang-surge
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        rollingUpdate: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - member.surgePodCreated: {component: engine, index: 0, replacement: 2}
  - tick: 4
    events:
      - member.surgeFailed: {component: engine, index: 0, reason: ImagePullBackOff}
      - member.held[set]: {component: engine, attempts: 3}
`

// A gang surge that fails is given up whole, as the engine gives it up. The
// replacement gang's row and pod leave, and gang 0 reads Ready on version 1
// with no target and no operation, beside the reason the attempt ended on.
func TestAFailedGangSurgeIsGivenUp(t *testing.T) {
	view := lastTick(t, gangPushWalk)
	if got, want := engineRows(t, view), "0@v1 1@v1"; got != want {
		t.Fatalf("engine rows after the failed gang surge = %q, want %q: the replacement's row must leave", got, want)
	}
	row := view.Replicas[v1beta1.EngineComponent].Status.InstanceStatuses[0]
	if row.Phase != v1beta1.OMENativeInstanceReady || row.TargetRevision != "" || row.Operation != nil {
		t.Errorf("gang 0 reads phase=%s target=%q operation=%v, want Ready with no target and no operation", row.Phase, row.TargetRevision, row.Operation)
	}
	if row.LastFailure == nil || row.LastFailure.Reason != "ImagePullBackOff" {
		t.Errorf("gang 0 carries the failure %v, want the reason ImagePullBackOff", row.LastFailure)
	}
	want := []string{"svc-a-engine-0-default-0", "svc-a-engine-1-default-0"}
	if got := podNames(enginePods(view)); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("engine pods after the failed gang surge = %v, want %v: the replacement's pod must leave", got, want)
	}
}

// A scale-down after a failed gang surge removes the highest index. Two
// gangs, the surge of gang 0 failed three times and the retries stopped,
// scaled to one: both gangs read Ready on version 1, so gang 1 leaves and
// gang 0 stays, as the engine's planner ranks rows that read alike.
func TestAScaleDownAfterAFailedGangSurgeRemovesTheHighestIndex(t *testing.T) {
	view := lastTick(t, scaledTo(gangPushWalk, 5, 1))
	if got, want := engineRows(t, view), "0@v1"; got != want {
		t.Fatalf("engine rows after the scale to one = %q, want %q: a gang set back to Ready ranks with the healthy gang, and the highest index leaves", got, want)
	}
}
