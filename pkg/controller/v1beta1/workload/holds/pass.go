package holds

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Row is one observed Instance as the hold pass sees it.
type Row struct {
	// Status is the pass-start row, read in place from the pass's
	// observation. Every authority judges on it and none writes into it;
	// the write itself is re-judged against the fresh row inside mark.
	Status *types.InstanceStatus

	// Desired is the row's desired canonical pod count.
	Desired int32

	// Instance is the planned Instance, or nil when the plan no longer
	// covers this row.
	Instance *types.InstancePlan

	// Excluded marks a leaving Instance the scale-down has not admitted
	// yet: it receives no lifecycle mutation before the wave admits it.
	Excluded bool
}

// PassInput is everything the hold pass reads.
type PassInput struct {
	Deps  types.Deps
	Input types.ReconcileInput
	Plan  types.ComponentPlan
	Rows  []Row

	// Pods is the reconcile's one memoized cached pod observation,
	// bucketed by Instance index. It is a function rather than a map so
	// an authority that can rule a row out from the row alone never
	// triggers the List.
	Pods func(ctx context.Context) (map[int32][]*corev1.Pod, error)

	drainer *drain.Batcher
}

// rowPods returns the row's own pods and the pods whose health answers
// for it. The two differ during a gang surge: the source stays on the
// prior revision by design, so the replacement bucket is what says
// whether the row is capacity.
func (in PassInput) rowPods(ctx context.Context, row Row) (own, attributed []*corev1.Pod, err error) {
	byIdx, err := in.Pods(ctx)
	if err != nil {
		return nil, nil, err
	}
	return byIdx[row.Status.Index], evidence.PodsForStuckCheck(*row.Status, byIdx), nil
}

// serving reports whether the row is already in rotation at its desired
// count, which retires every hold re-read from the cluster: the
// bookkeeping an authority judged on may be stale, but a stray
// unplaceable pod or a group reading cannot stop a workload that is
// already serving. The admission wait is not one of them: it answers for
// a write a serving row's roll still needs, so its write site releases it.
func (in PassInput) serving(ctx context.Context, row Row) (bool, error) {
	_, attributed, err := in.rowPods(ctx, row)
	if err != nil {
		return false, err
	}
	return query.PodSetFullyServing(attributed, row.Desired), nil
}

// heldSince is the moment stamped on a hold's evidence when the wait
// itself carries no time of its own: the one already recorded when the
// row holds the token, otherwise now.
//
// Nothing measures a window from it. The escalation pass measures a
// scheduler wait stamped this way from the attempt's placement wait, which
// decidePlacementWait records. Every other hold stamped this way resolves
// itself and never escalates.
// It is re-read so that re-observing an unchanged hold produces an
// identical record and the status no-op guard keeps the pass write-free.
func heldSince(s types.InstanceStatus, token string, now time.Time) metav1.Time {
	if s.Operation != nil && s.Operation.Waiting == token &&
		s.LastFailure != nil && s.LastFailure.Reason == token && !s.LastFailure.Time.IsZero() {
		return s.LastFailure.Time
	}
	return metav1.NewTime(now)
}

// Result reports which authority owns each row's token after the pass.
// The escalation pass reads it: a row parked on a wait only an operator
// or another controller can end is not a row to repair.
type Result struct {
	held map[int32]map[string]bool
}

// Holding reports whether the row carries token after the hold pass.
func (r Result) Holding(idx int32, token string) bool {
	return r.held[idx][token]
}

func (r *Result) record(idx int32, token string) {
	if r.held == nil {
		r.held = make(map[int32]map[string]bool)
	}
	if r.held[idx] == nil {
		r.held[idx] = make(map[string]bool)
	}
	r.held[idx][token] = true
}

// RowsForPlan builds the pass's rows from the plan and the observed
// statuses: a row's desired pod count is the plan's when the plan still
// covers its index and the observed count otherwise, and excluded marks
// the leaving Instances the scale-down has not admitted yet.
func RowsForPlan(plan types.ComponentPlan, statuses []types.InstanceStatus, excluded map[int32]struct{}) []Row {
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	planned := make(map[int32]*types.InstancePlan, len(plan.Instances))
	for i := range plan.Instances {
		planned[plan.Instances[i].Index] = &plan.Instances[i]
	}
	rows := make([]Row, 0, len(statuses))
	for i := range statuses {
		row := &statuses[i]
		_, skip := excluded[row.Index]
		rows = append(rows, Row{
			Status:   row,
			Desired:  status.DesiredFor(desiredByIdx, row.Index, row.PodCount),
			Instance: planned[row.Index],
			Excluded: skip,
		})
	}
	return rows
}

// Run is the hold pass: one evaluation of every authority over every
// observed row, at the top of the reconcile and before any verb pass.
//
// Per row, every authority reads its own condition first, then every
// release runs, then every record. Releasing before recording is what
// lets a row that stops waiting on one authority and starts waiting on
// another hand over inside one pass; left unheld for even one pass, the
// deadline-parking step reads the empty token as admission and restarts
// a whole InstanceReadyTimeout.
func Run(ctx context.Context, in PassInput) (Result, error) {
	in.drainer = drain.NewBatcher(in.Deps.Reader(), in.Input.Key.Namespace)
	var out Result
	// The readings of one row at a time, in the pass's one buffer: nothing
	// reads a row's readings once its row is done.
	readings := make([]reading, len(table))
	for _, row := range in.Rows {
		clear(readings)
		if err := runRow(ctx, in, row, readings, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

// runRow runs read, then release, then record for one row, with readings
// as its scratch: one slot per authority in table order, every slot zero
// on entry, and a slot left zero for an authority the row skips. The
// attempt's placement wait (placementWait) is written once when it is
// recorded and once when it is retired, never pass after pass. Each rides
// on the write of a placement hold recorded or released in the same pass
// when there is one, and is a status write of its own otherwise: retiring
// it once the member rebuilt for a name the sweep freed is placed, with no
// hold left to release, is one.
func runRow(ctx context.Context, in PassInput, row Row, readings []reading, out *Result) error {
	for i, a := range table {
		if row.skips(a) {
			continue
		}
		r, err := a.report(ctx, in, row)
		if err != nil {
			return fmt.Errorf("read %s hold (instance=%d): %w", a.token, row.Status.Index, err)
		}
		readings[i] = r
	}
	wait, err := decidePlacementWait(ctx, in, row, readings)
	if err != nil {
		return fmt.Errorf("read placement wait (instance=%d): %w", row.Status.Index, err)
	}
	for i, a := range table {
		if !readings[i].release {
			continue
		}
		if err := release(ctx, in.Input, row.Status.Index, a.token, wait.clearsWith(a.token)); err != nil {
			return fmt.Errorf("release %s hold (instance=%d): %w", a.token, row.Status.Index, err)
		}
	}
	for i, a := range table {
		if !readings[i].waiting {
			continue
		}
		_, held, err := mark(ctx, in.Input, in.Plan, row.Status.Index, a, readings[i].evidence, wait.sinceWith(a.token))
		if err != nil {
			return fmt.Errorf("record %s hold (instance=%d): %w", a.token, row.Status.Index, err)
		}
		if held {
			out.record(row.Status.Index, a.token)
		}
	}
	if err := wait.apply(ctx, in, row, readings); err != nil {
		return fmt.Errorf("retire placement wait (instance=%d): %w", row.Status.Index, err)
	}
	return nil
}

// skips reports whether the row is one this authority never touches: a
// leaving Instance the scale-down has not admitted receives no mutation
// before admission, and a row nobody owns — Failed, or an attempt parked
// after its disposition — has no attempt left for a fact to hold. The
// pause reaches all of them — it releases its own token wherever it left
// one.
func (r Row) skips(a authority) bool {
	if a.everyRow {
		return false
	}
	return r.Excluded || types.Owner(r.Status) == types.OwnerNone
}
