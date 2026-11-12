package inferencereplica

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// deadlineWrite is one deadline write the step made: the Instance and the
// deadline it was given.
type deadlineWrite struct {
	index    int32
	deadline metav1.Time
}

// deadlineWritesOf runs the deadline reading over rows with the held set
// and records every write in order, applied to a copy of the rows as the
// adapter's mirror applies them.
func deadlineWritesOf(t *testing.T, rows []workloadtypes.InstanceStatus, held map[int32]bool, timeout time.Duration, now time.Time) []deadlineWrite {
	t.Helper()
	store := make([]workloadtypes.InstanceStatus, len(rows))
	for i := range rows {
		store[i] = rows[i]
		if rows[i].Operation != nil {
			op := *rows[i].Operation
			store[i].Operation = &op
		}
	}
	var writes []deadlineWrite
	input := workloadtypes.ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range store {
				if store[i].Index != idx {
					continue
				}
				if mutate(&store[i]) {
					writes = append(writes, deadlineWrite{index: idx, deadline: store[i].Operation.Deadline})
				}
				return nil
			}
			return fmt.Errorf("instance %d is not among the rows", idx)
		},
	}
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, rows, held, timeout); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	return writes
}

// everyRowHeldInstances is the held set read off every row of the status,
// the reading rowsWithAnOperation and heldInstances together must agree
// with: an Instance with a pod behind an admission gate is held, a
// gang-surge source is held when the bucket its surge index names is held
// at its turn in row order, and a pause holds every Instance.
func everyRowHeldInstances(statuses []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod, paused bool) map[int32]bool {
	held := map[int32]bool{}
	for idx, pods := range byIndex {
		if slices.ContainsFunc(pods, workloadtypes.PodAdmissionGated) {
			held[idx] = true
		}
	}
	for _, row := range statuses {
		op := row.Operation
		surgeSource := op != nil && op.Type == v1beta1.InstanceOperationUpdate && op.SurgeIndex != nil
		if paused || (surgeSource && held[*op.SurgeIndex]) {
			held[row.Index] = true
		}
	}
	return held
}

// deadlineCase is one generated input of the deadline step: the rows of a
// status, the Component's pods by Instance, the pause and the window.
type deadlineCase struct {
	statuses []v1beta1.OMENativeInstanceStatus
	byIndex  map[int32][]*corev1.Pod
	paused   bool
	timeout  time.Duration
}

// deadlineCaseIndexes is the index space a generated case draws its rows,
// surge indexes and pods from, wider than its row count so a surge index
// points at a row that exists, at one that does not, or at itself.
const deadlineCaseIndexes = 10

// randomDeadlineCase draws one case: up to seven rows with distinct
// indexes, each with any phase, any revisions and, on most, an operation of
// any type, step, deadline, surge index, waiting token and refusal record;
// pods on some indexes, some behind an admission gate; a pause on some; a
// window, none, or a negative one.
func randomDeadlineCase(r *rand.Rand, now time.Time) deadlineCase {
	phases := []v1beta1.OMENativeInstancePhase{"", v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating,
		v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting,
		v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting}
	operationTypes := []v1beta1.InstanceOperationType{v1beta1.InstanceOperationCreate, v1beta1.InstanceOperationUpdate,
		v1beta1.InstanceOperationRestart, v1beta1.InstanceOperationMigrate, v1beta1.InstanceOperationDelete}
	steps := []string{"", workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, workloadtypes.UpdateStepSurgeDrainSettle,
		workloadtypes.UpdateStepGangSurgeTarget, workloadtypes.UpdateStepGangSurgeTargetCleanup, workloadtypes.UpdateStepInPlace,
		workloadtypes.UpdateStepDrain, workloadtypes.UpdateStepParked, "CreateSurge"}
	deadlines := []metav1.Time{{}, metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(time.Hour))}
	waits := append([]string{"", "", "other"}, workloadtypes.ExternalHoldTokens()...)
	revisions := []string{"", "rev-a", "rev-b"}

	indexes := r.Perm(deadlineCaseIndexes)[:r.IntN(8)]
	c := deadlineCase{paused: r.IntN(10) < 3, timeout: []time.Duration{0, -time.Minute, 30 * time.Minute}[r.IntN(3)]}
	for _, idx := range indexes {
		row := v1beta1.OMENativeInstanceStatus{
			Index: int32(idx), Incarnation: 1, Phase: phases[r.IntN(len(phases))],
			RunningRevision: revisions[r.IntN(len(revisions))], TargetRevision: revisions[r.IntN(len(revisions))],
		}
		if r.IntN(10) < 7 {
			row.Operation = &v1beta1.InstanceOperation{
				Type: operationTypes[r.IntN(len(operationTypes))], Step: steps[r.IntN(len(steps))],
				Deadline: deadlines[r.IntN(len(deadlines))], Waiting: waits[r.IntN(len(waits))],
				TargetRevision: revisions[r.IntN(len(revisions))],
			}
			if r.IntN(2) == 0 {
				surge := int32(r.IntN(deadlineCaseIndexes))
				row.Operation.SurgeIndex = &surge
			}
			if r.IntN(5) == 0 {
				at := metav1.NewTime(now)
				row.Operation.CapacityRefusedAt = &at
			}
		}
		c.statuses = append(c.statuses, row)
	}
	c.byIndex = map[int32][]*corev1.Pod{}
	for idx := 0; idx < deadlineCaseIndexes; idx++ {
		if r.IntN(10) >= 6 {
			continue
		}
		for n := 1 + r.IntN(3); n > 0; n-- {
			pod := &corev1.Pod{}
			if r.IntN(10) < 3 {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
			}
			c.byIndex[int32(idx)] = append(c.byIndex[int32(idx)], pod)
		}
	}
	return c
}

// The deadline step reads only the rows with an operation open, and
// decides the same writes as a reading of every row: over generated cases,
// the writes made over rowsWithAnOperation with heldInstances, in order,
// equal the writes made over every row converted with the held set read
// off every row. The cases reach every outcome: writes that stop a clock
// and writes that start one, sources held through their surge bucket,
// rows held by a waiting token or a refusal, cases with no write, cases
// with no operation at all.
func TestRowsWithAnOperation_DecideTheSameDeadlinesAsEveryRow(t *testing.T) {
	const cases = 300000
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(20, 15))
	reached := map[string]int{}
	for n := 0; n < cases; n++ {
		c := randomDeadlineCase(r, now)
		every := v1beta1convert.InstanceStatusSliceToWorkload(c.statuses)
		want := deadlineWritesOf(t, every, everyRowHeldInstances(c.statuses, c.byIndex, c.paused), c.timeout, now)

		rows := rowsWithAnOperation(c.statuses)
		var got []deadlineWrite
		if len(rows) > 0 {
			got = deadlineWritesOf(t, rows, heldInstances(rows, c.byIndex, c.paused), c.timeout, now)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("case %d: the rows with an operation decide %v, every row decides %v\nrows %s\npods by index %v paused %v timeout %s",
				n, got, want, describeDeadlineRows(c.statuses), gatedByIndex(c.byIndex), c.paused, c.timeout)
		}
		noteDeadlineOutcomes(reached, c, rows, got)
	}
	for _, outcome := range []string{"a clock stopped", "a clock started", "a source held through its surge bucket", "a row held by its token", "no write", "no operation open", "a pause"} {
		if reached[outcome] == 0 {
			t.Errorf("no generated case reached %q; the generator is too narrow", outcome)
		}
	}
	t.Logf("%d cases: %v", cases, reached)
}

// noteDeadlineOutcomes counts the outcomes a case reached, so the test can
// hold the generator to reaching each of them.
func noteDeadlineOutcomes(reached map[string]int, c deadlineCase, rows []workloadtypes.InstanceStatus, writes []deadlineWrite) {
	for _, s := range writes {
		if s.deadline.IsZero() {
			reached["a clock stopped"]++
		} else {
			reached["a clock started"]++
		}
	}
	if len(writes) == 0 {
		reached["no write"]++
	}
	if len(rows) == 0 {
		reached["no operation open"]++
	}
	if c.paused {
		reached["a pause"]++
	}
	held := heldInstances(rows, c.byIndex, c.paused)
	for i := range rows {
		op := rows[i].Operation
		if workloadtypes.OperationExternallyHeld(op) {
			reached["a row held by its token"]++
		}
		if op.Type == workloadtypes.InstanceOperationUpdate && op.SurgeIndex != nil && held[*op.SurgeIndex] && !c.paused {
			reached["a source held through its surge bucket"]++
		}
	}
}

// describeDeadlineRows prints the rows of a failing case.
func describeDeadlineRows(statuses []v1beta1.OMENativeInstanceStatus) string {
	out := ""
	for _, s := range statuses {
		out += fmt.Sprintf("\n  %d %s running=%q target=%q", s.Index, s.Phase, s.RunningRevision, s.TargetRevision)
		if op := s.Operation; op != nil {
			out += fmt.Sprintf(" op{%s %q deadline=%v waiting=%q refused=%v", op.Type, op.Step, op.Deadline.Time, op.Waiting, op.CapacityRefusedAt != nil)
			if op.SurgeIndex != nil {
				out += fmt.Sprintf(" surge=%d", *op.SurgeIndex)
			}
			out += "}"
		}
	}
	return out
}

// gatedByIndex prints, per Instance with pods, how many of its pods wait
// behind an admission gate.
func gatedByIndex(byIndex map[int32][]*corev1.Pod) map[int32]string {
	out := map[int32]string{}
	for idx, pods := range byIndex {
		gated := 0
		for _, p := range pods {
			if workloadtypes.PodAdmissionGated(p) {
				gated++
			}
		}
		out[idx] = fmt.Sprintf("%d of %d gated", gated, len(pods))
	}
	return out
}

// The rows with an operation open are the rows the step reads, converted
// as every row is: the same rows in the same order, each equal to its
// conversion among every row, and none when no row has an operation.
func TestRowsWithAnOperation_AreTheConvertedRowsThatCarryOne(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(20, 16))
	for n := 0; n < 20000; n++ {
		c := randomDeadlineCase(r, now)
		every := v1beta1convert.InstanceStatusSliceToWorkload(c.statuses)
		var want []workloadtypes.InstanceStatus
		for i := range every {
			if every[i].Operation != nil {
				want = append(want, every[i])
			}
		}
		got := rowsWithAnOperation(c.statuses)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d: rowsWithAnOperation() = %+v, want %+v", n, got, want)
		}
	}
}
