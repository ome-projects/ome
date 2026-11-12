package replay

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// budgetEpoch is one stretch of the spec's rolling-update budgets, in
// Instances against the replica count of the time, from the instant the
// spec began to carry them.
type budgetEpoch struct {
	since          time.Time
	maxUnavailable int32
	maxSurge       int32
}

// BudgetsHeldTo are the rolling-update budgets, in Instances, the
// capacity and surge-ceiling invariants hold a tick to: the spec's budget
// now, widened to the budget an attempt still open was admitted under,
// because an attempt keeps the budget it opened under until it lands, and
// a stopped attempt's replacement keeps its surge slot until it is gone.
// escalation.BudgetNoLimit means the layer caps nothing.
type BudgetsHeldTo struct {
	MaxUnavailable int32
	MaxSurge       int32
}

// recordBudgetEpoch notes the budgets the desired spec carries at this
// pass, as a new epoch when they differ from the last one recorded.
func (d *driver) recordBudgetEpoch(desired types.WorkloadDesiredSpec) {
	var rolling *types.RollingUpdate
	if strategy := desired.Lifecycle.UpdateStrategy; strategy != nil {
		rolling = strategy.RollingUpdate
	}
	epoch := budgetEpoch{
		since:          d.clock.Now(),
		maxUnavailable: escalation.PerComponentMaxUnavailableBudget(rolling, desired.Replicas),
		maxSurge:       escalation.PerComponentMaxSurgeBudget(rolling, desired.Replicas),
	}
	if n := len(d.budgetEpochs); n > 0 {
		last := d.budgetEpochs[n-1]
		if last.maxUnavailable == epoch.maxUnavailable && last.maxSurge == epoch.maxSurge {
			return
		}
	}
	d.budgetEpochs = append(d.budgetEpochs, epoch)
}

// budgetsHeldTo reads the budgets the tick is held to: the current
// epoch's, widened by the epoch each update attempt still on a row opened
// under and, for a row carrying no attempt, by the epoch a replacement
// still standing beyond its pod set was created in. Nil before the first
// epoch is recorded.
func (d *driver) budgetsHeldTo(rows []types.InstanceStatus, pods map[int32][]*corev1.Pod, podsPerInstance int32) *BudgetsHeldTo {
	n := len(d.budgetEpochs)
	if n == 0 {
		return nil
	}
	current := d.budgetEpochs[n-1]
	out := &BudgetsHeldTo{MaxUnavailable: current.maxUnavailable, MaxSurge: current.maxSurge}
	for i := range rows {
		if op := rows[i].Operation; op != nil && op.Type == types.InstanceOperationUpdate {
			epoch := d.epochAt(op.StartedAt.Time)
			out.MaxUnavailable = widerBudget(out.MaxUnavailable, epoch.maxUnavailable)
			out.MaxSurge = widerBudget(out.MaxSurge, epoch.maxSurge)
			continue
		}
		if created, ok := standingReplacement(pods[rows[i].Index], podsPerInstance); ok {
			out.MaxSurge = widerBudget(out.MaxSurge, d.epochAt(created).maxSurge)
		}
	}
	return out
}

// standingReplacement is the creation instant of the newest live pod of an
// Instance holding more live pods than its set, and whether there is one.
func standingReplacement(pods []*corev1.Pod, podsPerInstance int32) (time.Time, bool) {
	var live int32
	var newest time.Time
	for _, pod := range pods {
		if !livePod(pod) {
			continue
		}
		live++
		if created := pod.CreationTimestamp.Time; created.After(newest) {
			newest = created
		}
	}
	return newest, podsPerInstance > 0 && live > podsPerInstance
}

// epochAt is the epoch that applied at an instant: the last one begun at or
// before it, or the first when the instant predates every epoch, as a
// seeded attempt's start does.
func (d *driver) epochAt(at time.Time) budgetEpoch {
	found := d.budgetEpochs[0]
	for _, epoch := range d.budgetEpochs {
		if !epoch.since.After(at) {
			found = epoch
		}
	}
	return found
}

// widerBudget is the looser of two budgets; no limit is the loosest.
func widerBudget(a, b int32) int32 {
	if a == escalation.BudgetNoLimit || b == escalation.BudgetNoLimit {
		return escalation.BudgetNoLimit
	}
	return max(a, b)
}
