package holds

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Row is one observed Instance as the hold pass sees it.
type Row struct {
	// Status is the pass-start row. Every authority judges on it; the
	// write itself is re-judged against the fresh row inside mark.
	Status types.InstanceStatus

	// Desired is the row's desired canonical pod count.
	Desired int32

	// Instance is the planned Instance, or nil when the plan no longer
	// covers this row.
	Instance *types.InstancePlan

	// Excluded marks a deferred scale-down victim: it receives no
	// lifecycle mutation before the wave admits it.
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
	return byIdx[row.Status.Index], evidence.PodsForStuckCheck(row.Status, byIdx), nil
}

// serving reports whether the row is already in rotation at its desired
// count, which retires every fact-bearing hold: the bookkeeping an
// authority judged on may be stale, but a stray unplaceable pod or a
// group reading cannot stop a workload that is already serving.
func (in PassInput) serving(ctx context.Context, row Row) (bool, error) {
	_, attributed, err := in.rowPods(ctx, row)
	if err != nil {
		return false, err
	}
	return query.PodSetFullyServing(attributed, row.Desired), nil
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
// the deferred scale-down victims.
func RowsForPlan(plan types.ComponentPlan, statuses []types.InstanceStatus, excluded map[int32]struct{}) []Row {
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	planned := make(map[int32]*types.InstancePlan, len(plan.Instances))
	for i := range plan.Instances {
		planned[plan.Instances[i].Index] = &plan.Instances[i]
	}
	rows := make([]Row, 0, len(statuses))
	for _, row := range statuses {
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
	for _, row := range in.Rows {
		if err := runRow(ctx, in, row, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

// runRow runs release-then-record for one row.
func runRow(ctx context.Context, in PassInput, row Row, out *Result) error {
	readings := make([]reading, len(table))
	for i, a := range table {
		if row.skips(a) {
			continue
		}
		r, err := a.report(ctx, in, row)
		if err != nil {
			return fmt.Errorf("read %s hold (instance=%d): %w", a.token, row.Status.Index, err)
		}
		readings[i] = r
		if r.release {
			if err := release(ctx, in.Input, row.Status.Index, a.token); err != nil {
				return fmt.Errorf("release %s hold (instance=%d): %w", a.token, row.Status.Index, err)
			}
		}
	}
	for i, a := range table {
		if !readings[i].waiting {
			continue
		}
		_, held, err := mark(ctx, in.Input, in.Plan, row.Status.Index, a, readings[i].evidence)
		if err != nil {
			return fmt.Errorf("record %s hold (instance=%d): %w", a.token, row.Status.Index, err)
		}
		if held {
			out.record(row.Status.Index, a.token)
		}
	}
	return nil
}

// skips reports whether the row is one this authority never touches: a
// deferred scale-down victim receives no mutation before admission, and
// a Failed row has no attempt left for a fact to hold. The pause reaches
// both — it releases its own token wherever it left one.
func (r Row) skips(a authority) bool {
	if a.everyRow {
		return false
	}
	return r.Excluded || r.Status.Phase == types.InstancePhaseFailed
}
