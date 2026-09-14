package acceleratorquota

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/capacity"
)

// Collectors for the numbers the quota passes already compute. Every one is
// observed at the controller call site that produced it, so pkg/quota stays a
// library with no metrics dependency of its own.
//
// The capacity and backend families below carry no plane label. Capacity
// derivation and materialization both run in workload mode only -- r.Capacity
// and r.Materialize are populated on that branch alone -- so a mode label could
// only ever hold one value, and a constant label is a column that costs
// cardinality and answers nothing. The budget family further down does carry
// it, because both modes publish budgets and mean different things by them.
//
// The capacity gauges are a full snapshot of the last pass, so each is Reset
// before it is repopulated: a flavor being added has to drive the series it
// used to own back to zero, and a stale series that merely stops updating
// would read as a fleet that still has unattributed accelerators.
var (
	capacityAllocatable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_allocatable",
		Help: "Allocatable accelerators on nodes that could accept work at the last capacity pass, by resource and ResourceFlavor. Allocatable, not capacity, so it will not reconcile against kube_node_status_capacity.",
	}, []string{"resource", "flavor"})

	capacityUnavailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unavailable",
		Help: "Accelerators on nodes that matched a ResourceFlavor but were cordoned or not Ready. Budgets are sized against allocatable, so this is the parked remainder.",
	}, []string{"resource", "flavor"})

	capacityNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_nodes",
		Help: "Nodes behind the accelerator totals, by resource, ResourceFlavor and whether they can accept work. This is what separates a one-machine shortfall from a rack.",
	}, []string{"resource", "flavor", "state"})

	capacityUnattributed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unattributed",
		Help: "Allocatable accelerators on nodes that no ResourceFlavor claims, so they contribute zero capacity to every budget. Non-zero means a missing or ambiguous ResourceFlavor, not a shrinking fleet.",
	}, []string{"resource", "reason"})

	capacityUnattributedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_capacity_unattributed_nodes",
		Help: "Distinct nodes whose accelerators could not be attributed to a ResourceFlavor. Counts nodes, not node-resource pairs, so a node advertising two unclaimed resources is counted once.",
	}, []string{"reason"})

	backendAppliedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ome_quota_backend_applied_total",
		Help: "Kueue objects written by the materializer. Writes are server-side applies issued unconditionally, so this tracks pass frequency times render size, not how often the tree changed.",
	})

	backendSweptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_quota_backend_swept_total",
		Help: "Kueue objects reaped as orphans, by what triggered the sweep: a materialization pass finding objects the tree no longer names, or a node being deleted.",
	}, []string{"trigger"})
)

// Per-node accelerator accounting, written from the same pass that builds
// status.budgets so the series and the CR cannot disagree.
//
// The identifying label is quota, not node: this package already reports
// ome_quota_capacity_nodes, which counts machines, and one word meaning both a
// Kubernetes node and a quota-tree node three panels apart is a trap. quota is
// the AcceleratorQuota object name, which is cluster-scoped and
// webhook-validated, so it needs no namespace and cannot run unbounded.
//
// These do carry plane. Both modes publish budgets and the same series means a
// different thing in each: on a workload plane nominal is this cluster's share
// and admitted is what its Kueue queues hold, while on a management plane
// nominal is the authored fleet total and usage arrives only through the
// member-status funnel. A fleet-wide query that mixed them would be summing
// two different quantities.
//
// A node's ancestry carries no label of its own. Nodes are named by their path,
// so quota already spells out where in the tree a budget sits, and a second
// label derived from the first would only add a way for the two to disagree.
//
// Lent is deliberately absent. The model has no lent figure -- an ancestor's
// borrowed is recomputed rather than summed from its children, because a loan
// between two siblings is internal to the subtree that contains them. Adding a
// Go-side lent would invent a number the tree does not have; it is derivable in
// PromQL over a subtree for anyone who wants it.
var (
	budgetNominal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_budget_nominal",
		Help: "Accelerators allowed for this node and flavor: the fleet total where the tree is authored, this cluster's share on a projection.",
	}, budgetLabels)

	budgetAdmitted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_budget_admitted",
		Help: "Accelerators currently admitted against the materialized queues, including anything borrowed.",
	}, budgetLabels)

	budgetReserved = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_budget_reserved",
		Help: "Accelerators held by workloads carrying a quota reservation, admitted or not. Never below admitted; the gap is work that owns accelerators but has not started, so a tenant at its ceiling reads as idle unless this is read too.",
	}, budgetLabels)

	budgetBorrowed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_quota_budget_borrowed",
		Help: "Accelerators admitted above this node's own nominal, taken from idle siblings and reclaimable on contention.",
	}, budgetLabels)
)

var budgetLabels = []string{"plane", "quota", "role", "resource", "flavor"}

// budgetVectors is every vector keyed by quota, so a sweep cannot miss one that
// was added later.
func budgetVectors() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{budgetNominal, budgetAdmitted, budgetReserved, budgetBorrowed}
}

// recorded maps each quota name with live budget series to the role it was last
// published under, so a pass can drop the ones the tree no longer names and
// spot the ones whose role flipped. The reconcile is whole-tree and serialised
// on one key, but the mutex costs nothing and removes the assumption.
var (
	recordedMu sync.Mutex
	recorded   = map[string]string{}
)

// Sweep triggers. Stable strings -- dashboards and alerts key off them.
const (
	// SweepTriggerMaterialize is a sweep at the end of a materialization pass,
	// removing objects the current tree no longer names.
	SweepTriggerMaterialize = "materialize"
	// SweepTriggerFinalize is a sweep while releasing a deleted node's
	// finalizer, removing that node's own objects.
	SweepTriggerFinalize = "finalize"
)

// Node states behind a capacity total. Stable strings.
const (
	// NodeStateSchedulable counts nodes that could accept work.
	NodeStateSchedulable = "schedulable"
	// NodeStateUnavailable counts nodes that matched the flavor but were
	// cordoned or not Ready.
	NodeStateUnavailable = "unavailable"
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		capacityAllocatable,
		capacityUnavailable,
		capacityNodes,
		capacityUnattributed,
		capacityUnattributedNodes,
		backendAppliedTotal,
		backendSweptTotal,
		budgetNominal,
		budgetAdmitted,
		budgetReserved,
		budgetBorrowed,
	)
}

// recordBudgets publishes one node's accelerator accounting.
//
// Called where status.budgets is computed rather than after the write lands,
// and deliberately so: most passes change nothing, and a manager that only
// published on change would come back from a restart with every budget series
// missing until someone edited a spec. The gauge reports what the controller
// resolved this pass, which is what the CR carries whenever the write succeeds.
func recordBudgets(plane, quota, role string, budgets []v1beta1.AcceleratorBudgetStatus) {
	if plane == "" || quota == "" {
		return
	}

	// A leaf converted to a grouping keeps its name while role changes, and role
	// is part of the key, so the series it used to occupy would keep reporting
	// alongside its replacement and double the node in any sum. Drop the old one
	// first.
	recordedMu.Lock()
	previous, seen := recorded[quota]
	changed := seen && previous != role
	recorded[quota] = role
	recordedMu.Unlock()

	if changed {
		dropSeriesFor(quota)
	}

	for _, b := range budgets {
		labels := []string{plane, quota, role, b.ResourceName, b.ResourceFlavor}
		budgetNominal.WithLabelValues(labels...).Set(b.Nominal.AsApproximateFloat64())
		budgetAdmitted.WithLabelValues(labels...).Set(b.Admitted.AsApproximateFloat64())
		budgetReserved.WithLabelValues(labels...).Set(b.Reserved.AsApproximateFloat64())
		budgetBorrowed.WithLabelValues(labels...).Set(b.Borrowed.AsApproximateFloat64())
	}
}

// deleteQuotaSeries drops every budget series for one node, on the path that
// removes it from the tree.
func deleteQuotaSeries(quota string) {
	recordedMu.Lock()
	delete(recorded, quota)
	recordedMu.Unlock()

	dropSeriesFor(quota)
}

// dropSeriesFor removes every budget series carrying one quota name, whatever
// role it was published under. Matching on quota alone is what lets it clean up
// after a node whose role flipped, whose stale series is keyed on a role no
// caller still holds.
func dropSeriesFor(quota string) {
	for _, v := range budgetVectors() {
		v.DeletePartialMatch(prometheus.Labels{"quota": quota})
	}
}

// sweepBudgets drops series for nodes the tree no longer names.
//
// The finalizer path is the ordinary way a node's series go away; this catches
// the rest -- a finalizer force-stripped by hand, or a delete that landed while
// this manager was down. Cheap here because the pass already holds the
// authoritative node set, which a per-object reconciler never does.
func sweepBudgets(live map[string]struct{}) {
	recordedMu.Lock()
	var stale []string
	for name := range recorded {
		if _, ok := live[name]; !ok {
			stale = append(stale, name)
			delete(recorded, name)
		}
	}
	recordedMu.Unlock()

	for _, name := range stale {
		dropSeriesFor(name)
	}
}

// recordCapacity publishes one whole capacity pass.
//
// Takes the full result rather than the two slices separately, because the
// gauges are a snapshot: partial publication would leave the unattributed
// series describing one pass and the allocatable series another.
func recordCapacity(observed capacity.Result) {
	capacityAllocatable.Reset()
	capacityUnavailable.Reset()
	capacityNodes.Reset()
	capacityUnattributed.Reset()
	capacityUnattributedNodes.Reset()

	for _, c := range observed.Capacities {
		capacityAllocatable.WithLabelValues(c.ResourceName, c.ResourceFlavor).
			Set(c.Allocatable.AsApproximateFloat64())
		capacityUnavailable.WithLabelValues(c.ResourceName, c.ResourceFlavor).
			Set(c.Unavailable.AsApproximateFloat64())
		capacityNodes.WithLabelValues(c.ResourceName, c.ResourceFlavor, NodeStateSchedulable).
			Set(float64(c.Nodes))
		capacityNodes.WithLabelValues(c.ResourceName, c.ResourceFlavor, NodeStateUnavailable).
			Set(float64(c.UnavailableNodes))
	}

	// Unattributed carries one entry per (node, resource). Accelerators sum across
	// those entries, but nodes must not: a node advertising two unclaimed
	// resources is one node, and counting entries would double it.
	accelerators := map[[2]string]float64{}
	nodes := map[string]map[string]struct{}{}
	for _, u := range observed.Unattributed {
		accelerators[[2]string{u.ResourceName, u.Reason}] += u.Quantity.AsApproximateFloat64()
		if nodes[u.Reason] == nil {
			nodes[u.Reason] = map[string]struct{}{}
		}
		nodes[u.Reason][u.Node] = struct{}{}
	}
	for key, total := range accelerators {
		capacityUnattributed.WithLabelValues(key[0], key[1]).Set(total)
	}
	for reason, set := range nodes {
		capacityUnattributedNodes.WithLabelValues(reason).Set(float64(len(set)))
	}
}

// recordApplied counts the objects one materialization pass wrote.
func recordApplied(objects int) {
	if objects <= 0 {
		return
	}
	backendAppliedTotal.Add(float64(objects))
}

// recordSwept counts the objects one sweep reaped. A sweep that finds nothing
// is the steady state, so it is not recorded -- the counter would otherwise be
// indistinguishable from one that never ran.
func recordSwept(trigger string, objects int) {
	if objects <= 0 {
		return
	}
	backendSweptTotal.WithLabelValues(trigger).Add(float64(objects))
}
