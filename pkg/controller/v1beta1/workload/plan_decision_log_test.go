package workload_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recordingSink is a logr sink that keeps every line at or under its
// verbosity and takes none above it, as the manager's sink does at the
// info level.
type recordingSink struct {
	verbosity int
	lines     []recordedLine
}

// recordedLine is one line a sink took: its message and its key-value
// pairs in order.
type recordedLine struct {
	message string
	values  []any
}

func (s *recordingSink) Init(logr.RuntimeInfo) {}

func (s *recordingSink) Enabled(level int) bool { return level <= s.verbosity }

func (s *recordingSink) Info(_ int, message string, values ...any) {
	s.lines = append(s.lines, recordedLine{message: message, values: values})
}

func (s *recordingSink) Error(error, string, ...any) {}

func (s *recordingSink) WithValues(...any) logr.LogSink { return s }

func (s *recordingSink) WithName(string) logr.LogSink { return s }

// linesWithMessage is the lines the sink took with the message given, in
// order.
func (s *recordingSink) linesWithMessage(message string) []recordedLine {
	var lines []recordedLine
	for _, line := range s.lines {
		if line.message == message {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestPlan_Update_ExplainsEveryInstanceAtVerbosityOneAndNoneBelowIt: the
// update selection writes one debug line per planned Instance saying what
// it did with it, in the same words whether the sink takes the line or
// not, and writes no such line to a sink that takes nothing above the info
// level, which is the manager's.
func TestPlan_Update_ExplainsEveryInstanceAtVerbosityOneAndNoneBelowIt(t *testing.T) {
	const instances = 8
	const running, pushed = "svc-engine-rev-a", "svc-engine-rev-b"
	moments := []struct {
		name, target, message string
		values                func(index int32) []any
	}{
		{name: "at rest", target: running, message: "update not triggered", values: func(index int32) []any {
			return []any{
				"component", types.ComponentEngine, "instance", index,
				"phase", types.InstancePhaseReady, "runningRevision", running,
				"target", running, "hasOperation", false,
				"retryAfter", time.Duration(0), "cleanupOnly", false,
			}
		}},
		{name: "under a push", target: pushed, message: "update selected", values: func(index int32) []any {
			return []any{
				"component", types.ComponentEngine, "instance", index, "target", pushed,
				"startingFresh", true, "coordGateExempt", false, "servingUnproven", false, "recreateFallback", false,
				workload.ReplacesUnservingPodSetLogKeyForTest, false, "rank", workload.StartRankServing,
			}
		}},
	}
	for _, moment := range moments {
		for _, verbosity := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/verbosity %d", moment.name, verbosity), func(t *testing.T) {
				sink := &recordingSink{verbosity: verbosity}
				ctx := logr.NewContext(context.Background(), logr.New(sink))
				input := fleetInput(instances, instances, 1, running)
				plan, err := workload.BuildPlan(types.ComponentEngine, input.DesiredSpec, input.ObservedState)
				if err != nil {
					t.Fatal(err)
				}
				target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: moment.target, Namespace: "ns"}}
				if _, err := workload.Plan(ctx, input, plan, target, workload.SnapshotWithPodsForTest(input, fleetPods(instances, 1))); err != nil {
					t.Fatal(err)
				}
				var want []recordedLine
				if verbosity >= 1 {
					for index := int32(0); index < instances; index++ {
						want = append(want, recordedLine{message: moment.message, values: moment.values(index)})
					}
				}
				if diff := cmp.Diff(want, sink.linesWithMessage(moment.message), cmp.AllowUnexported(recordedLine{})); diff != "" {
					t.Errorf("lines %q (-want +got):\n%s", moment.message, diff)
				}
			})
		}
	}
}
