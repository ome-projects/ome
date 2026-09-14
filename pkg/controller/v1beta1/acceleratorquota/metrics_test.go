package acceleratorquota

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/capacity"
)

// Unit tests for the collectors registered at package init in metrics.go.
// Nothing here needs envtest or a reconciler: the recorders are pure functions
// of a capacity.Result or an int, which is the whole reason instrumentation
// lives at the controller call site rather than inside pkg/quota.

// sampleFor reads one sample out of the controller-runtime registry by metric
// name and label subset. Returns 0 when the family exists but no sample
// matches, and -1 only when Gather itself fails, so a missing metric is
// distinguishable from a metric sitting at zero.
// seriesFor counts the series matching a label subset. sampleFor reads the
// first match and so cannot see a duplicate whose value happens to agree.
func seriesFor(t *testing.T, name string, labels prometheus.Labels) int {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		return -1
	}
	count := 0
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want != l.GetValue() {
					matched = false
					break
				}
			}
			if matched {
				count++
			}
		}
	}
	return count
}

func sampleFor(t *testing.T, name string, labels prometheus.Labels) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want != l.GetValue() {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

// The registration itself is the contract the scrape path depends on: a metric
// that is declared but never registered is served by nothing, which is the
// failure this whole package exists to stop repeating.
func TestMetricsAreRegistered(t *testing.T) {
	recordCapacity(capacity.Result{
		Capacities: []capacity.Capacity{{
			ResourceName: "example.com/accelerator", ResourceFlavor: "flavor-a",
			Allocatable: qty("1"),
		}},
		Unattributed: []capacity.Unattributed{{
			Node: "node-a", ResourceName: "example.com/accelerator",
			Quantity: qty("1"), Reason: capacity.ReasonNoFlavor,
		}},
	})
	recordApplied(1)
	recordSwept(SweepTriggerMaterialize, 1)

	for _, name := range []string{
		"ome_quota_capacity_allocatable",
		"ome_quota_capacity_unavailable",
		"ome_quota_capacity_nodes",
		"ome_quota_capacity_unattributed",
		"ome_quota_capacity_unattributed_nodes",
		"ome_quota_backend_applied_total",
		"ome_quota_backend_swept_total",
	} {
		families, err := ctrlmetrics.Registry.Gather()
		if err != nil {
			t.Fatalf("gathering: %v", err)
		}
		found := false
		for _, mf := range families {
			if mf.GetName() == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s is not registered in the controller-runtime registry", name)
		}
	}
}

func TestRecordCapacitySplitsSchedulableFromParked(t *testing.T) {
	recordCapacity(capacity.Result{Capacities: []capacity.Capacity{{
		ResourceName:     "example.com/gpu",
		ResourceFlavor:   "flavor-split",
		Allocatable:      qty("24"),
		Unavailable:      qty("8"),
		Nodes:            3,
		UnavailableNodes: 1,
	}}})

	l := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split"}
	if got := sampleFor(t, "ome_quota_capacity_allocatable", l); got != 24 {
		t.Errorf("allocatable accelerators = %v, want 24", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unavailable", l); got != 8 {
		t.Errorf("unavailable accelerators = %v, want 8", got)
	}

	sched := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split", "state": NodeStateSchedulable}
	if got := sampleFor(t, "ome_quota_capacity_nodes", sched); got != 3 {
		t.Errorf("schedulable nodes = %v, want 3", got)
	}
	parked := prometheus.Labels{"resource": "example.com/gpu", "flavor": "flavor-split", "state": NodeStateUnavailable}
	if got := sampleFor(t, "ome_quota_capacity_nodes", parked); got != 1 {
		t.Errorf("unavailable nodes = %v, want 1", got)
	}
}

// Unattributed carries one entry per (node, resource). Accelerators sum across those
// entries; nodes must not, or a node advertising two unclaimed resources is
// counted twice and the fleet looks bigger than it is.
func TestRecordCapacityCountsNodesOnceAcrossResources(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "dual", ResourceName: "example.com/a", Quantity: qty("4"), Reason: capacity.ReasonNoFlavor},
		{Node: "dual", ResourceName: "example.com/b", Quantity: qty("2"), Reason: capacity.ReasonNoFlavor},
		{Node: "single", ResourceName: "example.com/a", Quantity: qty("4"), Reason: capacity.ReasonNoFlavor},
	}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed_nodes",
		prometheus.Labels{"reason": capacity.ReasonNoFlavor}); got != 2 {
		t.Errorf("unattributed nodes = %v, want 2 (dual counted once)", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/a", "reason": capacity.ReasonNoFlavor}); got != 8 {
		t.Errorf("unattributed accelerators for resource a = %v, want 8 (summed across nodes)", got)
	}
}

// The two reasons must not be merged: a missing ResourceFlavor and an ambiguous
// one need different fixes, and the whole point of the metric is to say which.
func TestRecordCapacitySeparatesUnattributedReasons(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "n1", ResourceName: "example.com/r", Quantity: qty("1"), Reason: capacity.ReasonNoFlavor},
		{Node: "n2", ResourceName: "example.com/r", Quantity: qty("5"), Reason: capacity.ReasonAmbiguous},
	}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/r", "reason": capacity.ReasonNoFlavor}); got != 1 {
		t.Errorf("no-flavor accelerators = %v, want 1", got)
	}
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/r", "reason": capacity.ReasonAmbiguous}); got != 5 {
		t.Errorf("ambiguous accelerators = %v, want 5", got)
	}
}

// A flavor being added has to drive the series it used to own back to zero. A
// gauge that merely stops updating keeps reporting unattributed accelerators on a
// cluster that has just been fixed.
func TestRecordCapacityClearsResolvedSeries(t *testing.T) {
	recordCapacity(capacity.Result{Unattributed: []capacity.Unattributed{
		{Node: "n1", ResourceName: "example.com/transient", Quantity: qty("9"), Reason: capacity.ReasonNoFlavor},
	}})
	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/transient", "reason": capacity.ReasonNoFlavor}); got != 9 {
		t.Fatalf("precondition: accelerators = %v, want 9", got)
	}

	// The operator adds the flavor; the next pass attributes everything.
	recordCapacity(capacity.Result{Capacities: []capacity.Capacity{{
		ResourceName: "example.com/transient", ResourceFlavor: "now-claimed", Allocatable: qty("9"),
	}}})

	if got := sampleFor(t, "ome_quota_capacity_unattributed",
		prometheus.Labels{"resource": "example.com/transient", "reason": capacity.ReasonNoFlavor}); got != 0 {
		t.Errorf("accelerators after the flavor was added = %v, want the series gone", got)
	}
}

// A sweep that finds nothing is the steady state. Recording it would make a
// counter that never moves indistinguishable from one whose sweep never ran.
func TestRecordSweptIgnoresEmptySweeps(t *testing.T) {
	l := prometheus.Labels{"trigger": SweepTriggerFinalize}
	before := sampleFor(t, "ome_quota_backend_swept_total", l)

	recordSwept(SweepTriggerFinalize, 0)
	if got := sampleFor(t, "ome_quota_backend_swept_total", l); got != before {
		t.Errorf("an empty sweep moved the counter: %v -> %v", before, got)
	}

	recordSwept(SweepTriggerFinalize, 3)
	if got := sampleFor(t, "ome_quota_backend_swept_total", l); got != before+3 {
		t.Errorf("swept counter = %v, want %v", got, before+3)
	}
}

// The two triggers answer different questions -- routine orphan collection
// versus a node being deleted -- so they must not share a series.
func TestRecordSweptSeparatesTriggers(t *testing.T) {
	mat := prometheus.Labels{"trigger": SweepTriggerMaterialize}
	fin := prometheus.Labels{"trigger": SweepTriggerFinalize}
	beforeMat := sampleFor(t, "ome_quota_backend_swept_total", mat)
	beforeFin := sampleFor(t, "ome_quota_backend_swept_total", fin)

	recordSwept(SweepTriggerMaterialize, 2)

	if got := sampleFor(t, "ome_quota_backend_swept_total", mat); got != beforeMat+2 {
		t.Errorf("materialize sweeps = %v, want %v", got, beforeMat+2)
	}
	if got := sampleFor(t, "ome_quota_backend_swept_total", fin); got != beforeFin {
		t.Errorf("a materialize sweep leaked into the finalize series: %v -> %v", beforeFin, got)
	}
}

func TestRecordAppliedIgnoresEmptyPasses(t *testing.T) {
	before := sampleFor(t, "ome_quota_backend_applied_total", nil)

	recordApplied(0)
	if got := sampleFor(t, "ome_quota_backend_applied_total", nil); got != before {
		t.Errorf("an empty pass moved the counter: %v -> %v", before, got)
	}

	recordApplied(7)
	if got := sampleFor(t, "ome_quota_backend_applied_total", nil); got != before+7 {
		t.Errorf("applied counter = %v, want %v", got, before+7)
	}
}

func budgetLabelsFor(quota, resource string) prometheus.Labels {
	return prometheus.Labels{"quota": quota, "resource": resource}
}

func oneBudget(t *testing.T, nominal, admitted, reserved, borrowed string) []v1beta1.AcceleratorBudgetStatus {
	t.Helper()
	return []v1beta1.AcceleratorBudgetStatus{{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Nominal:        qty(nominal),
		Admitted:       qty(admitted),
		Reserved:       qty(reserved),
		Borrowed:       qty(borrowed),
	}}
}

// The four figures must land on four distinct series. Collapsing any pair would
// make "at its ceiling" and "holding accelerators it has not started using" read the
// same.
func TestRecordBudgetsPublishesEveryFigure(t *testing.T) {
	recordBudgets(string(ModeWorkload), "tenant-a", string(v1beta1.AcceleratorQuotaRoleClusterQueue), oneBudget(t, "16", "8", "8", "0"))

	l := budgetLabelsFor("tenant-a", "google.com/tpu")
	for _, tc := range []struct {
		metric string
		want   float64
	}{
		{"ome_quota_budget_nominal", 16},
		{"ome_quota_budget_admitted", 8},
		{"ome_quota_budget_reserved", 8},
		{"ome_quota_budget_borrowed", 0},
	} {
		if got := sampleFor(t, tc.metric, l); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.metric, got, tc.want)
		}
	}
}

// The same series name means the authored fleet total on one plane and one
// cluster's share on the other, so the two must not merge.
func TestRecordBudgetsSeparatesPlanes(t *testing.T) {
	recordBudgets(string(ModeWorkload), "shared", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "16", "0", "0", "0"))
	recordBudgets(string(ModeManagement), "shared", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "64", "0", "0", "0"))

	wl := prometheus.Labels{"quota": "shared", "plane": string(ModeWorkload)}
	mg := prometheus.Labels{"quota": "shared", "plane": string(ModeManagement)}
	if got := sampleFor(t, "ome_quota_budget_nominal", wl); got != 16 {
		t.Errorf("workload nominal = %v, want 16", got)
	}
	if got := sampleFor(t, "ome_quota_budget_nominal", mg); got != 64 {
		t.Errorf("management nominal = %v, want 64", got)
	}
}

// A blank plane or quota would land a series keyed on nothing, which no
// dashboard can select and no sweep can find again.
func TestRecordBudgetsDropsUnlabelledSamples(t *testing.T) {
	recordBudgets("", "no-plane", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "1", "0", "0", "0"))
	recordBudgets(string(ModeWorkload), "", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "1", "0", "0", "0"))

	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": "no-plane"}); got != 0 {
		t.Errorf("a sample with no plane was published: %v", got)
	}
	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"plane": string(ModeWorkload), "quota": ""}); got != 0 {
		t.Errorf("a sample with no quota name was published: %v", got)
	}
}

// Deleting a node has to take its series with it, or a deleted tenant's
// allowance is reported forever.
func TestDeleteQuotaSeriesDropsEveryVector(t *testing.T) {
	recordBudgets(string(ModeWorkload), "doomed", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "8", "4", "4", "0"))
	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": "doomed"}); got != 8 {
		t.Fatalf("precondition: nominal = %v, want 8", got)
	}

	deleteQuotaSeries("doomed")

	for _, m := range []string{
		"ome_quota_budget_nominal", "ome_quota_budget_admitted",
		"ome_quota_budget_reserved", "ome_quota_budget_borrowed",
	} {
		if got := sampleFor(t, m, prometheus.Labels{"quota": "doomed"}); got != 0 {
			t.Errorf("%s survived the delete: %v", m, got)
		}
	}
}

// The finalizer is the ordinary path; this is the one that catches a node
// force-stripped, or deleted while the manager was down.
func TestSweepBudgetsDropsNodesNoLongerInTheTree(t *testing.T) {
	recordBudgets(string(ModeWorkload), "stays", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "8", "0", "0", "0"))
	recordBudgets(string(ModeWorkload), "vanished", string(v1beta1.AcceleratorQuotaRoleClusterQueue),
		oneBudget(t, "4", "0", "0", "0"))

	sweepBudgets(map[string]struct{}{"stays": {}})

	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": "vanished"}); got != 0 {
		t.Errorf("a node absent from the tree kept its series: %v", got)
	}
	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": "stays"}); got != 8 {
		t.Errorf("the sweep removed a node that is still in the tree: %v", got)
	}
}

// Nodes are named by their path, so the quota label already says where a budget
// sits and no second ancestry label is published beside it — two of them would
// only give a query a way to disagree with itself.
func TestRecordBudgetsCarriesNoSeparatePathLabel(t *testing.T) {
	recordBudgets(string(ModeWorkload), "root.team.nested",
		string(v1beta1.AcceleratorQuotaRoleClusterQueue), oneBudget(t, "12", "0", "0", "0"))

	if got := sampleFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": "root.team.nested"}); got != 12 {
		t.Errorf("nominal selected by node name = %v, want 12", got)
	}
}

// A drained leaf converted to a grouping keeps its name while role changes, and
// role is part of the series key, so the role it vacated has to stop reporting
// -- otherwise one node is counted twice in every sum until the stale series
// ages out of the store.
func TestRecordBudgetsRoleFlipLeavesOneSeries(t *testing.T) {
	const quota = "convertible"
	recordBudgets(string(ModeWorkload), quota,
		string(v1beta1.AcceleratorQuotaRoleClusterQueue), oneBudget(t, "8", "0", "0", "0"))
	recordBudgets(string(ModeWorkload), quota,
		string(v1beta1.AcceleratorQuotaRoleCohort), oneBudget(t, "8", "0", "0", "0"))

	if got := sampleFor(t, "ome_quota_budget_nominal", prometheus.Labels{
		"quota": quota, "role": string(v1beta1.AcceleratorQuotaRoleClusterQueue),
	}); got != 0 {
		t.Errorf("the vacated role kept reporting: %v", got)
	}
	if got := sampleFor(t, "ome_quota_budget_nominal", prometheus.Labels{
		"quota": quota, "role": string(v1beta1.AcceleratorQuotaRoleCohort),
	}); got != 8 {
		t.Errorf("nominal under the new role = %v, want 8", got)
	}
	// Counted, not sampled: two series each reading 8 are indistinguishable from
	// one by value, and it is the second series that does the damage in a sum.
	if got := seriesFor(t, "ome_quota_budget_nominal",
		prometheus.Labels{"quota": quota}); got != 1 {
		t.Errorf("the node has %d series, want 1 -- the flip left the old role behind", got)
	}
}

// Republishing a node that has not moved must not disturb its series, since
// that is what every resync tick does.
func TestRecordBudgetsRepublishesInPlace(t *testing.T) {
	recordBudgets(string(ModeWorkload), "steady", string(v1beta1.AcceleratorQuotaRoleClusterQueue), oneBudget(t, "8", "2", "2", "0"))
	recordBudgets(string(ModeWorkload), "steady", string(v1beta1.AcceleratorQuotaRoleClusterQueue), oneBudget(t, "8", "6", "6", "0"))

	if got := sampleFor(t, "ome_quota_budget_admitted",
		prometheus.Labels{"quota": "steady"}); got != 6 {
		t.Errorf("admitted = %v, want 6", got)
	}
}
