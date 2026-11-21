package replay

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// failedGangSurgeWalk is two engine gangs with no rollout group. Version 2
// is pushed at tick 2 and gang 0 builds its replacement as a row of its own
// at index 2. The events of ticks 3 and 4 are the test's own.
const failedGangSurgeWalk = `scenario: failed-gang-surge
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
  config: {requeue: 10s}
timeline:
  - tick: 1
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - member.surgePodCreated: {component: engine, index: 0, replacement: 2}
`

// runFailedGangSurge plays the walk with the given events at ticks 3 and 4.
// It returns the retry records the engine's replica publishes before each
// of the two passes, and the trace.
func runFailedGangSurge(t *testing.T, third, fourth string) ([2][]v1beta1.RetryBlock, string) {
	t.Helper()
	walk := failedGangSurgeWalk + "  - tick: 3\n    events:\n" + third + "  - tick: 4\n"
	if fourth != "" {
		walk += "    events:\n" + fourth
	}
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	var records [2][]v1beta1.RetryBlock
	trace, err := Run(context.Background(), s, Options{Observer: func(view TickView) {
		if view.Tick >= 3 {
			records[view.Tick-3] = view.Replicas[v1beta1.EngineComponent].Status.RetryBlocks
		}
	}})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	return records, trace
}

// publishedRowsAt is the rows text of the engine's publication on one tick.
func publishedRowsAt(t *testing.T, trace, tick string) string {
	t.Helper()
	for _, line := range strings.Split(trace, "\n") {
		if strings.HasPrefix(line, tick+" publish component=engine ") {
			_, rows, _ := strings.Cut(line, "rows=")
			return rows
		}
	}
	t.Fatalf("no publication of the engine on %s:\n%s", tick, trace)
	return ""
}

// Both rows of a failed gang surge read Failed while the replacement is
// torn down, and the retry record counts nothing yet. The record counts the
// failure once, when the surge is given up.
func TestAFailedGangSurgeIsCountedWhenItIsGivenUp(t *testing.T) {
	failed := "      - member.failed: {component: engine, index: 0, reason: DeadlineExceeded, unrecorded: true}\n" +
		"      - member.failed: {component: engine, index: 2, reason: DeadlineExceeded, unrecorded: true}\n"
	records, trace := runFailedGangSurge(t, failed, "      - member.surgeFailed: {component: engine, index: 0, reason: DeadlineExceeded}\n")
	if rows := publishedRowsAt(t, trace, "tick=3"); !strings.Contains(rows, "0:Failed@") || !strings.Contains(rows, "2:Failed@") {
		t.Errorf("the pair does not read Failed on tick 3: %s", rows)
	}
	if len(records[0]) != 0 {
		t.Errorf("the retry record counts %+v before the surge is given up", records[0])
	}
	if rows := publishedRowsAt(t, trace, "tick=4"); !strings.Contains(rows, "0:Ready@") || strings.Contains(rows, "2:") {
		t.Errorf("the surge is not given up on tick 4: %s", rows)
	}
	if len(records[1]) != 1 || records[1][0].State != v1beta1.RetryBlockBackoff || records[1][0].AttemptsStarted != 1 {
		t.Errorf("the retry record reads %+v after the surge is given up, want one failed attempt in backoff", records[1])
	}
	if !strings.Contains(trace, "tick=3 apply member.failed component=engine index=0 phase=Failed reason=DeadlineExceeded unrecorded") {
		t.Errorf("the trace does not name the failure as unrecorded:\n%s", trace)
	}
}

// A failed surge given up as unrecorded leaves no retry record, as the
// engine leaves none when the source started a newer attempt before the
// replacement was gone.
func TestAGangSurgeGivenUpUnrecordedLeavesNoRetryRecord(t *testing.T) {
	records, trace := runFailedGangSurge(t, "      - member.surgeFailed: {component: engine, index: 0, reason: DeadlineExceeded, unrecorded: true}\n", "")
	if rows := publishedRowsAt(t, trace, "tick=3"); !strings.Contains(rows, "0:Ready@") || strings.Contains(rows, "2:") {
		t.Errorf("the surge is not given up on tick 3: %s", rows)
	}
	for i, got := range records {
		if len(got) != 0 {
			t.Errorf("the retry record reads %+v on tick %d, and an unrecorded failure counts nothing", got, i+3)
		}
	}
}

// A failure with no mark is counted when it is staged, on a gang pair as on
// a single pod.
func TestAFailureWithNoMarkIsCountedAtOnce(t *testing.T) {
	records, _ := runFailedGangSurge(t, "      - member.failed: {component: engine, index: 0, reason: DeadlineExceeded}\n", "")
	if len(records[0]) != 1 || records[0][0].AttemptsStarted != 1 {
		t.Errorf("the retry record reads %+v, want one failed attempt", records[0])
	}
}

// singlePodSurgeWalk is two single-pod engines with no rollout group.
// Version 2 is pushed at tick 2 and Instance 0 builds its surge pod on its
// own row. The event of tick 3 is the test's own.
const singlePodSurgeWalk = `scenario: failed-single-pod-surge
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
  config: {requeue: 10s}
timeline:
  - tick: 1
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - member.surgePodCreated: {component: engine, index: 0}
  - tick: 3
    events:
`

// runWalk parses and plays a walk of this file.
func runWalk(t *testing.T, walk string, observe func(TickView)) (string, error) {
	t.Helper()
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	return Run(context.Background(), s, Options{Observer: observe})
}

// The engine counts the failed attempt of a single pod in the pass that
// writes it Failed, so the mark is refused on a row outside a gang surge.
func TestAnUnrecordedFailureOutsideAGangSurgeIsRefused(t *testing.T) {
	for _, event := range []string{"member.failed", "member.surgeFailed"} {
		walk := singlePodSurgeWalk + "      - " + event + ": {component: engine, index: 0, reason: DeadlineExceeded, unrecorded: true}\n"
		_, err := runWalk(t, walk, nil)
		if err == nil || !strings.Contains(err.Error(), "unrecorded") || !strings.Contains(err.Error(), "gang surge") {
			t.Errorf("%s with the mark on a single pod: got %v, want a refusal that names the gang surge", event, err)
		}
	}
}

// The same failure with no mark is staged, and the retry record counts it
// in the same pass.
func TestAFailureOutsideAGangSurgeWithNoMarkIsCounted(t *testing.T) {
	for _, event := range []string{"member.failed", "member.surgeFailed"} {
		walk := singlePodSurgeWalk + "      - " + event + ": {component: engine, index: 0, reason: DeadlineExceeded}\n"
		var records []v1beta1.RetryBlock
		trace, err := runWalk(t, walk, func(view TickView) {
			if view.Tick == 3 {
				records = view.Replicas[v1beta1.EngineComponent].Status.RetryBlocks
			}
		})
		if err != nil {
			t.Errorf("%s with no mark on a single pod: %v\n%s", event, err, trace)
			continue
		}
		if len(records) != 1 || records[0].AttemptsStarted != 1 {
			t.Errorf("%s with no mark on a single pod: the retry record reads %+v, want one failed attempt", event, records)
		}
	}
}

// recreateBesideTheFailedReplacement is the third tick of a walk in which a
// recreate takes the source of a failed gang surge while its failed
// replacement still stands: both rows read Failed, then the source's old
// pod is gone and its new pod stands at its own index.
const recreateBesideTheFailedReplacement = "      - member.failed: {component: engine, index: 0, reason: DeadlineExceeded, unrecorded: true}\n" +
	"      - member.failed: {component: engine, index: 2, reason: DeadlineExceeded, unrecorded: true}\n" +
	"      - member.capacityLost[oldRevision]: {component: engine, index: 0, gone: true}\n" +
	"      - member.surgePodCreated: {component: engine, index: 0, ordinal: 0}\n"

// A recreate may take the source of a failed gang surge before the failed
// replacement is gone. The source then publishes its recreate with no
// replacement index. When the replacement leaves, the source stays on the
// recreate and no retry record counts the surge.
func TestAFailedReplacementLeavesBesideASourceARecreateTook(t *testing.T) {
	walk := failedGangSurgeWalk + "  - tick: 3\n    events:\n" + recreateBesideTheFailedReplacement +
		"  - tick: 4\n    events:\n      - member.surgeFailed: {component: engine, index: 0, reason: DeadlineExceeded, unrecorded: true}\n"
	type published struct {
		phase      v1beta1.OMENativeInstancePhase
		attempt    bool
		surgeIndex *int32
	}
	rows := map[int]map[int32]published{}
	records := map[int]int{}
	trace, err := runWalk(t, walk, func(view TickView) {
		status := view.Replicas[v1beta1.EngineComponent].Status
		rows[view.Tick] = map[int32]published{}
		for _, row := range status.InstanceStatuses {
			p := published{phase: row.Phase, attempt: row.Operation != nil}
			if row.Operation != nil {
				p.surgeIndex = row.Operation.SurgeIndex
			}
			rows[view.Tick][row.Index] = p
		}
		records[view.Tick] = len(status.RetryBlocks)
	})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	source, beside := rows[3][0], rows[3][2]
	if source.phase != v1beta1.OMENativeInstanceUpdating || !source.attempt || source.surgeIndex != nil {
		t.Errorf("tick 3: the source reads %+v, want Updating on an attempt with no replacement index", source)
	}
	if beside.phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("tick 3: the replacement row reads %+v, want Failed beside the source", beside)
	}
	if _, stands := rows[4][2]; stands {
		t.Errorf("tick 4: the replacement row still stands: %+v", rows[4])
	}
	if after := rows[4][0]; after.phase != v1beta1.OMENativeInstanceUpdating || after.surgeIndex != nil {
		t.Errorf("tick 4: the source reads %+v, want it still Updating on its recreate", after)
	}
	if records[3] != 0 || records[4] != 0 {
		t.Errorf("the retry record counts %d and %d failures on ticks 3 and 4, and no record counts this surge", records[3], records[4])
	}
}

// The engine never counts a failed surge whose source a recreate took, so
// a give-up without the mark is refused there.
func TestACountedGiveUpBesideARecreateIsRefused(t *testing.T) {
	walk := failedGangSurgeWalk + "  - tick: 3\n    events:\n" + recreateBesideTheFailedReplacement +
		"  - tick: 4\n    events:\n      - member.surgeFailed: {component: engine, index: 0, reason: DeadlineExceeded}\n"
	_, err := runWalk(t, walk, nil)
	if err == nil || !strings.Contains(err.Error(), "unrecorded: true") || !strings.Contains(err.Error(), "recreate") {
		t.Errorf("a counted give-up beside a recreate: got %v, want a refusal that asks for unrecorded: true", err)
	}
}
