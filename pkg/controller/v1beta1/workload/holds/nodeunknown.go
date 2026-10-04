package holds

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// nodeUnknownHold is a name a silent node still holds: a pod in phase
// Unknown, or one whose Ready the control plane withdrew because its
// kubelet stopped heartbeating (evidence.SilentKubeletTargetPods). Neither is
// terminal — the node stopped reporting and the container may still be
// running — so recycling the name the way a Failed or Succeeded occupant
// is recycled would risk two pods of one identity. The name is freed only
// by the force-delete sweep, on proven node death; until then the row
// reports the wait instead of spending its deadline in silence.
var nodeUnknownHold = authority{
	token:  types.WaitingReasonNodeUnknown,
	mayOwn: anyAttemptOwner,
	report: reportNodeUnknown,
}

// reportNodeUnknown reads the row's own pods for one occupying a name
// the rebuild needs. Only a TARGET name counts: a pod at any other
// ordinal — a surge source, a slot no longer asked for — blocks nothing.
//
// The sweep that frees a held name runs later in the same pass, in the
// verb pass that needs it, and this reading is taken ahead of it. So it
// asks the sweep's own question of every held pod and reports only the
// names the sweep will leave: a pod force-deleted this pass is a name
// freed, not a wait, and recording it would park the attempt's deadline
// for one pass and restart it on the next. A pod whose node posts Ready
// is the kubelet's own lag, not a wait either.
func reportNodeUnknown(ctx context.Context, in PassInput, row Row) (reading, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	var held []*corev1.Pod
	for _, pod := range evidence.SilentKubeletTargetPods(own, targetPodNames(in, row), in.Input.ForceDelete) {
		kind, err := evidence.ReadSilentPod(ctx, in.Deps.Reader(), pod, in.Input.ForceDelete, in.Input.Now())
		if err != nil {
			return reading{}, err
		}
		if evidence.SilentPodHeld(pod, kind) {
			held = append(held, pod)
		}
	}
	if len(held) == 0 {
		return reading{release: true}, nil
	}
	return reading{waiting: true, evidence: types.NodeUnknownTermination(held[0])}, nil
}

// target is one pod the row's plan asks for: its name and the Runner
// slot it renders from.
type target struct {
	name    string
	runner  types.RunnerPlan
	ordinal int32
}

// targets enumerates the pods the row's plan asks for.
//
// Single-pod Runners read the ordinal slot from the row's recorded
// ActiveOrdinal, which SurgeThenDrain alternates between 0 and 1 across
// rollouts; while such a row is mid-surge its replacement is being built
// at the other slot, so that pod is asked for too. Multi-pod Runners
// occupy every 0..Size-1 ordinal. A row the plan no longer covers asks
// for no pod at all.
func targets(in PassInput, row Row) []target {
	if row.Instance == nil {
		return nil
	}
	var out []target
	add := func(runner types.RunnerPlan, ordinal int32) {
		out = append(out, target{
			name:    query.PodName(in.Input.Key.OwnerName, in.Plan.Component, row.Status.Index, runner.Name, ordinal),
			runner:  runner,
			ordinal: ordinal,
		})
	}
	for _, runner := range row.Instance.Runners {
		if runner.Size == 1 {
			add(runner, row.Status.ActiveOrdinal)
			if singlePodSurgeInFlight(row.Status) {
				add(runner, 1-row.Status.ActiveOrdinal)
			}
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			add(runner, o)
		}
	}
	return out
}

// targetPodNames is the set of names targets asks for.
func targetPodNames(in PassInput, row Row) map[string]struct{} {
	names := make(map[string]struct{})
	for _, t := range targets(in, row) {
		names[t.name] = struct{}{}
	}
	return names
}

// singlePodSurgeInFlight reports whether the row is running the per-pod
// SurgeThenDrain cycle, which builds the replacement at the ordinal
// opposite the active one on the same index. A gang surge builds under
// its own index and is read there.
func singlePodSurgeInFlight(s types.InstanceStatus) bool {
	return s.Operation != nil &&
		s.Operation.Type == types.InstanceOperationUpdate &&
		s.Operation.SurgeIndex == nil &&
		status.SurgeUpdateStep(s.Operation.Step)
}
