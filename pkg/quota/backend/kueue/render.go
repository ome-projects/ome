package kueue

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueac "sigs.k8s.io/kueue/client-go/applyconfiguration/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// Objects are the apply configurations one Plan renders to, grouped by kind so
// the caller can order the writes: a Cohort must exist before the object naming
// it as parent.
type Objects struct {
	Cohorts       []*kueueac.CohortApplyConfiguration
	ClusterQueues []*kueueac.ClusterQueueApplyConfiguration
	LocalQueues   []*kueueac.LocalQueueApplyConfiguration

	// Skipped records budgets dropped because their flavor does not exist on
	// this cluster, keyed by node name. Rendering them anyway would produce an
	// inactive ClusterQueue, which admits nothing and reports the reason only
	// in Kueue's own status — so the caller raises FlavorMissing instead.
	Skipped map[string][]string
}

// Render maps the plan onto Kueue objects. Pure: no client, no clock, no
// defaults of its own.
//
// flavors is the set of ResourceFlavor names that exist on this cluster. It is
// a parameter rather than a lookup so the mapping stays testable, and because
// the caller already reads flavors for capacity derivation.
func Render(plan backend.Plan, flavors map[string]struct{}, opts Options) Objects {
	out := Objects{Skipped: map[string][]string{}}

	for _, node := range plan.Write {
		if node.Role() == v1beta1.AcceleratorQuotaRoleCohort {
			out.Cohorts = append(out.Cohorts, renderCohort(node, opts))
			continue
		}
		cq, skipped := renderClusterQueue(node, flavors, opts)
		out.ClusterQueues = append(out.ClusterQueues, cq)
		if len(skipped) > 0 {
			out.Skipped[node.Name()] = skipped
		}
		out.LocalQueues = append(out.LocalQueues, renderLocalQueues(node, opts)...)
	}
	return out
}

// renderCohort emits pure topology. An internal node's budgets deliberately do
// not materialize: Kueue's cohort quota is additive rather than a ceiling, so
// writing the authored guardrail number would hand out quota on top of what the
// children already contribute — the opposite of the cap an admin wrote.
func renderCohort(node *tree.Node, opts Options) *kueueac.CohortApplyConfiguration {
	ac := kueueac.Cohort(node.Name()).WithLabels(labelsFor(node, opts))
	spec := kueueac.CohortSpec()
	if parent := parentName(node); parent != "" {
		spec = spec.WithParentName(kueuev1beta2.CohortReference(parent))
	}
	return ac.WithSpec(spec)
}

// renderClusterQueue emits a leaf's budget as exactly one resource group.
//
// One group, not one per flavor: Kueue scopes its uniqueness checks across all
// of a queue's groups, so a resource and a flavor may each appear in only one.
// The cover resources must be present for any pod to be assignable, and every
// serving pod requests them, so the cover pins itself to a single group and
// every budgeted resource has to join it. Splitting by flavor yields groups
// whose flavors can never satisfy a pod that also asks for cpu — a shape Kueue
// accepts and the scheduler then never uses.
func renderClusterQueue(node *tree.Node, flavors map[string]struct{}, opts Options) (*kueueac.ClusterQueueApplyConfiguration, []string) {
	budgets, skipped := usableBudgets(node, flavors)

	spec := kueueac.ClusterQueueSpec().
		WithNamespaceSelector(namespaceSelector(opts.EnrolledNamespaces))
	if parent := parentName(node); parent != "" {
		spec = spec.WithCohortName(kueuev1beta2.CohortReference(parent))
	}
	if group := resourceGroup(budgets, opts); group != nil {
		spec = spec.WithResourceGroups(group)
	}

	return kueueac.ClusterQueue(node.Name()).
		WithLabels(labelsFor(node, opts)).
		WithSpec(spec), skipped
}

// resourceGroup builds the single group. Every flavor must enumerate every
// covered resource, so a flavor that funds no accelerator still carries the
// cover, and an accelerator another flavor funds is carried at zero rather than
// omitted.
func resourceGroup(budgets []v1beta1.AcceleratorBudget, opts Options) *kueueac.ResourceGroupApplyConfiguration {
	if len(budgets) == 0 {
		return nil
	}

	covered := coveredResources(budgets, opts)
	byFlavor := map[string]map[string]v1beta1.AcceleratorBudget{}
	for _, b := range budgets {
		if byFlavor[b.ResourceFlavor] == nil {
			byFlavor[b.ResourceFlavor] = map[string]v1beta1.AcceleratorBudget{}
		}
		byFlavor[b.ResourceFlavor][b.ResourceName] = b
	}

	flavorNames := make([]string, 0, len(byFlavor))
	for name := range byFlavor {
		flavorNames = append(flavorNames, name)
	}
	sort.Strings(flavorNames)

	group := kueueac.ResourceGroup().WithCoveredResources(covered...)
	for _, flavor := range flavorNames {
		quotas := make([]*kueueac.ResourceQuotaApplyConfiguration, 0, len(covered))
		for _, name := range covered {
			quotas = append(quotas, resourceQuota(name, byFlavor[flavor], opts))
		}
		group = group.WithFlavors(kueueac.FlavorQuotas().
			WithName(kueuev1beta2.ResourceFlavorReference(flavor)).
			WithResources(quotas...))
	}
	return group
}

func resourceQuota(name corev1.ResourceName, funded map[string]v1beta1.AcceleratorBudget, opts Options) *kueueac.ResourceQuotaApplyConfiguration {
	q := kueueac.ResourceQuota().WithName(name)

	if cover, ok := opts.CoverResources[name]; ok {
		return q.WithNominalQuota(cover)
	}
	zero := resource.MustParse("0")
	budget, ok := funded[string(name)]
	if !ok {
		// Covered by the group because another flavor funds it; this flavor
		// does not, and Kueue requires the entry to be present regardless.
		//
		// Nothing to borrow with, either. Without the explicit limit a leaf
		// budgeted none of a flavor could still take a sibling's, which is a
		// stranger grant than an over-large one: nobody authored any share of
		// this flavor here at all.
		return q.WithNominalQuota(zero).WithBorrowingLimit(zero)
	}
	q = q.WithNominalQuota(budget.Nominal)
	// Zero rather than absent, and the difference is the whole allowance. OME's
	// nominal means "this leaf's share"; Kueue reads a null borrowingLimit as
	// "at least this much, plus whatever the cohort is not using". Leaving the
	// field off translates a ceiling into a floor.
	//
	// It reads worse than it sounds, because this plane sets no preemption and
	// Kueue defaults reclaimWithinCohort to Never: an unbounded borrower cannot
	// be made to give a share back, so nominal would be neither a ceiling for
	// the borrower nor a floor for the lender. Burst stays available, per
	// budget, by authoring the limit.
	limit := zero
	if budget.BorrowingLimit != nil {
		limit = *budget.BorrowingLimit
	}
	q = q.WithBorrowingLimit(limit)
	if budget.LendingLimit != nil {
		q = q.WithLendingLimit(*budget.LendingLimit)
	}
	return q
}

// coveredResources is the cover plus every budgeted resource, sorted so two
// renders of the same tree produce the same object and the apply is a no-op.
func coveredResources(budgets []v1beta1.AcceleratorBudget, opts Options) []corev1.ResourceName {
	seen := map[corev1.ResourceName]struct{}{}
	for name := range opts.CoverResources {
		seen[name] = struct{}{}
	}
	for _, b := range budgets {
		seen[corev1.ResourceName(b.ResourceName)] = struct{}{}
	}
	out := make([]corev1.ResourceName, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// usableBudgets drops budgets whose flavor is absent from the cluster and
// reports them. An empty flavor set means flavors could not be read at all, in
// which case nothing is dropped — treating that as "every flavor is missing"
// would zero every tenant's quota on a transient read failure.
func usableBudgets(node *tree.Node, flavors map[string]struct{}) ([]v1beta1.AcceleratorBudget, []string) {
	budgets := node.Quota.Spec.Budgets
	if len(flavors) == 0 {
		return budgets, nil
	}
	usable := make([]v1beta1.AcceleratorBudget, 0, len(budgets))
	var skipped []string
	for _, b := range budgets {
		if _, ok := flavors[b.ResourceFlavor]; !ok {
			skipped = append(skipped, b.ResourceFlavor)
			continue
		}
		usable = append(usable, b)
	}
	sort.Strings(skipped)
	return usable, skipped
}

// namespaceSelector admits the cluster's enrolled namespaces by name.
//
// It is never omitted. A null selector admits nothing, and Kueue reports that
// nowhere: the queue exists, its LocalQueues resolve, and every workload simply
// stays pending. This is not a tenant boundary — every leaf's queue selects the
// same set, and which leaf a workload charges is decided by the queue name it
// carries — but it does make the enrollment boundary something Kueue enforces
// rather than something implied by where LocalQueues happen to exist.
func namespaceSelector(namespaces []string) *metav1ac.LabelSelectorApplyConfiguration {
	values := append([]string(nil), namespaces...)
	sort.Strings(values)
	return metav1ac.LabelSelector().WithMatchExpressions(
		metav1ac.LabelSelectorRequirement().
			WithKey(corev1.LabelMetadataName).
			WithOperator(metav1.LabelSelectorOpIn).
			WithValues(values...))
}

// renderLocalQueues gives the leaf one LocalQueue per enrolled namespace, named
// after the leaf.
//
// Named after the leaf, because the name is the tenant: a workload reaches its
// budget by carrying that name in kueue.x-k8s.io/queue-name, and Kueue offers no
// way to name a ClusterQueue directly — spec.queueName is a LocalQueueName
// resolved in the workload's own namespace. Many leaves may therefore
// coexist in one namespace, each with its own pointer.
//
// The full cross product is the honest shape rather than over-materialization:
// tenancy is not namespace-scoped, so any leaf's workloads may appear in any
// enrolled namespace.
func renderLocalQueues(node *tree.Node, opts Options) []*kueueac.LocalQueueApplyConfiguration {
	namespaces := append([]string(nil), opts.EnrolledNamespaces...)
	sort.Strings(namespaces)

	out := make([]*kueueac.LocalQueueApplyConfiguration, 0, len(namespaces))
	for _, ns := range namespaces {
		out = append(out, kueueac.LocalQueue(node.Name(), ns).
			WithLabels(labelsFor(node, opts)).
			WithSpec(kueueac.LocalQueueSpec().
				WithClusterQueue(kueuev1beta2.ClusterQueueReference(node.Name()))))
	}
	return out
}

// parentName is the node's parent, or empty for the reserved root. Read from
// the spec rather than the linked parent so an unreachable node renders the
// edge its author wrote.
func parentName(node *tree.Node) string {
	if node.Quota.Spec.ParentRef == nil {
		return ""
	}
	return node.Quota.Spec.ParentRef.Name
}

// labelsFor marks an object as this manager's and records which node it came
// from. The node label is the sweep's selector and the reverse index from a
// Kueue object back to the CR that owns it.
func labelsFor(node *tree.Node, opts Options) map[string]string {
	return map[string]string{
		v1beta1.AcceleratorQuotaManagedByLabel: opts.FieldManager,
		v1beta1.AcceleratorQuotaNodeLabel:      node.Name(),
	}
}
