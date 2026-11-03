// Package acceleratorquota reconciles the AcceleratorQuota tree's observed
// state. It assembles every node on the cluster, re-runs the invariant checks
// the admission webhook runs, and writes each node's position and condition.
//
// It materializes nothing. Admission is fast feedback but not a serialization
// point — two children admitted concurrently can each pass containment and
// together bust their parent, and a parent can be deleted out from under a
// child — so the tree has to be re-checked by something that sees it whole,
// after the fact. That re-check is this controller's whole job for now; the
// Kueue materializer and the management-plane projector build on the same
// assembled tree.
package acceleratorquota

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
	"sigs.k8s.io/ome/pkg/quota/usage"
)

// Mode selects which half of the quota plane a manager runs. It is a manager
// flag rather than config because it decides which controllers exist.
type Mode string

const (
	// ModeWorkload renders the local tree into Kueue on a serving cluster. It is
	// the only mode that writes Kueue objects, and only on its own cluster.
	ModeWorkload Mode = "workload"

	// ModeManagement holds the authored fleet tree and projects per-cluster
	// shares onto members. It writes no Kueue object anywhere.
	ModeManagement Mode = "management"
)

// Deliberately no +kubebuilder:rbac markers here, unlike every sibling in this
// directory. controller-gen scans ./pkg/controller/... into the ome-manager
// ClusterRole, but this controller runs in ome-quota-manager, a separate binary
// with its own ServiceAccount. A marker added here would silently hand
// ome-manager the quota plane's permissions — today AcceleratorQuota, later
// cluster-wide write on Kueue's ClusterQueue and Cohort — and that write grant
// belongs only to the process that renders Kueue, not to the one also running
// the InferenceService reconcilers and the fail-closed pod mutating webhook.
//
// The grants live in charts/ome-quota-manager/templates/rbac.yaml, and
// charts/ome-resources/tests/render_test.sh fails if they ever appear in the
// ome-manager role.

// Reconciler keeps every AcceleratorQuota's status in step with the tree they
// collectively describe.
//
// Reconciliation is whole-tree, not per-object: one node's edit can change
// another node's verdict — re-parenting moves a subtree's paths, and a budget
// change can bust a parent that was fine a moment ago — so every pass rebuilds
// the tree and reconciles every node's status. A fleet is a handful of nodes,
// so this costs one LIST.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// APIReader is an uncached reader. The tree verdict must not be computed
	// from a stale cache: a node admitted a moment ago but not yet in the
	// informer cache would be invisible, and this controller would then clear a
	// Degraded condition that is still true.
	APIReader client.Reader

	// Options carry the operator-configured root name and depth bound. The
	// package invents neither.
	Options tree.Options

	// ResyncInterval re-runs the checks on an otherwise-idle plane, so a
	// violation only a clock can clear is noticed without an edit. Zero leaves
	// reconciles purely event-driven.
	ResyncInterval time.Duration

	// Capacity configures deriving this cluster's own accelerator capacity onto
	// the reserved root. Unset leaves the controller reporting tree position
	// only, which is what a management-mode manager wants: it holds the
	// authored fleet tree and has no local silicon to measure.
	Capacity CapacityOptions

	// Project configures fanning the authored tree out across a fleet. Unset is
	// the workload-mode shape: a member holds no transport, which is the
	// structural half of the loop guard — a projection has nothing to be
	// re-projected by.
	Project ProjectOptions

	// Materialize configures rendering the tree into an enforcement backend.
	// Unset leaves the tree observed but unenforced, which is both the
	// management-mode shape and the way a workload cluster runs before an
	// operator opts into writing Kueue objects.
	Materialize MaterializeOptions

	// Mode is which half of the plane this manager runs, carried here only so
	// the budget metrics can say which. The option structs above already encode
	// the behavioural difference; this names it for a reader of the series,
	// where the same metric means the authored fleet total in one mode and one
	// cluster's share in the other.
	Mode Mode
}

// Reconcile lists every AcceleratorQuota through the uncached reader and runs
// one pass over the whole tree. Which node changed does not narrow the work:
// one node's edit can move another node's result.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("acceleratorquota", req.Name)

	var list v1beta1.AcceleratorQuotaList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing AcceleratorQuotas: %w", err)
	}
	pass := &quotaPass{reconciler: r, log: log, nodes: list.Items}
	return pass.run(ctx)
}

// quotaPass is one reconcile pass over the whole AcceleratorQuota tree: the
// nodes the pass listed at its start and what its steps decide as they run.
// The steps share the record; a step that ends the pass records the result
// the controller returns.
type quotaPass struct {
	reconciler *Reconciler
	log        logr.Logger

	// nodes is every AcceleratorQuota on the cluster as the pass listed it.
	nodes []v1beta1.AcceleratorQuota
	// live is the tree's members; deleting is every node on its way out,
	// which only the release step touches.
	live, deleting []v1beta1.AcceleratorQuota

	built      *tree.Tree
	violations tree.Violations
	// frozen maps a node to the violation the tree's Frozen reports for it,
	// its own or the nearest ancestor's. Such a node reports the violation
	// and keeps the objects it last had.
	frozen map[string]tree.Violation

	// errs collects the failures that must not stop the pass before every
	// node is written: claiming, releasing, materializing, projecting, the
	// status writes and the fleet capacity sum. They end the pass together.
	errs []error

	// outcomes is what the backend did with each node; nil when the plane
	// materializes nothing, which reads as no outcome for every node.
	outcomes map[string]backend.NodeOutcome
	// fleet is what each member reports about each node; nil on a plane
	// that projects nothing.
	fleet       fleetView
	fleetRollup map[string]map[usage.Key]usage.Total
	// reading is what the backend holds per node; empty when it could not
	// be read this pass, which leaves the published figures standing.
	reading usageReading

	// retryFleetCapacity is set when the fleet capacity write hit a
	// conflict; the pass then asks to run again at once.
	retryFleetCapacity bool
	// result is what the pass returns when a step ends it.
	result ctrl.Result
}

// passStep is one step of a pass. It reports whether the pass goes on; a
// step that ends the pass has recorded the result on the pass first.
type passStep func(context.Context) (bool, error)

// run takes the pass through its steps in order and stops at the first one
// that ends it. The list is the whole order of a pass: the finalizers are
// claimed before anything is written, the deleting nodes are released before
// the tree is rendered, and every node's status is written before the
// collected failures end the pass.
func (p *quotaPass) run(ctx context.Context) (ctrl.Result, error) {
	_, err := runPassSteps(ctx, []passStep{
		p.splitLiveFromDeleting,
		p.keepAuthoredNodes,
		p.buildTree,
		p.addCapacityViolations,
		p.findFrozenNodes,
		p.claimLiveNodes,
		p.releaseDeletingNodes,
		p.materializeTree,
		p.projectTree,
		p.rollFleetUsage,
		p.readConsumption,
		p.writeNodeStatuses,
		p.sweepBudgetSeries,
		p.collectFleetCapacity,
		p.failOnCollectedErrors,
		p.logViolations,
		p.createReservedRoot,
		p.deriveCapacity,
		p.scheduleNextPass,
	})
	return p.result, err
}

// runPassSteps runs the steps in order and reports whether every one let
// the pass go on. It stops at the first step that ends the pass or fails
// and returns that step's error.
func runPassSteps(ctx context.Context, steps []passStep) (bool, error) {
	for _, step := range steps {
		if next, err := step(ctx); !next || err != nil {
			return false, err
		}
	}
	return true, nil
}

// end records the result a step ends the pass with and reports that the
// pass does not go on. A step that fails without ending the pass leaves
// the result empty.
func (p *quotaPass) end(result ctrl.Result, err error) (bool, error) {
	p.result = result
	return false, err
}

// claimsNodes reports whether this plane writes something a delete must
// take back: Kueue objects on a workload plane, projections on a management
// plane. Only such a plane puts a finalizer on its nodes.
func (p *quotaPass) claimsNodes() bool {
	return p.reconciler.Materialize.Enabled() || p.reconciler.Project.Enabled()
}

// splitLiveFromDeleting sets every node being deleted aside. Such a node is
// not part of the tree it is leaving: rendering it again would write the
// objects its finalizer is about to take back, and the release would read
// as failed.
func (p *quotaPass) splitLiveFromDeleting(context.Context) (bool, error) {
	p.live, p.deleting = partitionByDeletion(p.nodes)
	return true, nil
}

// keepAuthoredNodes drops the management plane's own projections when they
// come back to it: a hub registered against itself, or one cluster running
// both halves. Splitting an already split share again would compound it on
// every pass. A member keeps them, because its projections are its tree.
func (p *quotaPass) keepAuthoredNodes(context.Context) (bool, error) {
	if p.reconciler.Project.Enabled() {
		p.live = authoredHere(p.live)
	}
	return true, nil
}

// buildTree assembles the live nodes into the tree and collects the
// invariant violations. A failure here is a configuration error, not a
// property of the nodes, so it ends the pass with an error and leaves every
// status as it was.
func (p *quotaPass) buildTree(context.Context) (bool, error) {
	built, violations, err := tree.Build(p.live, p.reconciler.Options)
	if err != nil {
		return false, fmt.Errorf("assembling the quota tree: %w", err)
	}
	p.built, p.violations = built, violations
	return true, nil
}

// addCapacityViolations adds the budgets the cluster cannot afford to the
// violations, so the tree reports them like any other violated invariant.
// The numbers are the root's last observed capacity: a value one pass old
// is the right input for a check that ignores momentary dips.
func (p *quotaPass) addCapacityViolations(context.Context) (bool, error) {
	if p.reconciler.Capacity.Enabled() && p.built.Root != nil {
		p.violations = append(p.violations,
			capacityViolations(p.built.Nodes(), p.built.Root.Quota.Status.Capacity)...)
	}
	return true, nil
}

// findFrozenNodes maps every node to the violation the tree's Frozen reports
// for it, its own or the nearest ancestor's.
func (p *quotaPass) findFrozenNodes(context.Context) (bool, error) {
	p.frozen = p.built.Frozen(p.violations)
	return true, nil
}

// claimLiveNodes puts the finalizer on every live node before anything is
// written, so a delete that arrives during the pass still finds it. A
// failed claim is collected and the other nodes are still claimed.
func (p *quotaPass) claimLiveNodes(ctx context.Context) (bool, error) {
	if !p.claimsNodes() {
		return true, nil
	}
	for i := range p.live {
		if err := p.reconciler.ensureFinalizer(ctx, &p.live[i]); err != nil {
			p.errs = append(p.errs, fmt.Errorf("claiming node %s: %w", p.live[i].Name, err))
		}
	}
	return true, nil
}

// releaseDeletingNodes runs the finalizer of every node being deleted, and
// keeps every object a live node still names: a departing node and a live
// one can name the same object for a moment. The release runs before the
// tree is rendered, so it never deletes what this pass wrote.
func (p *quotaPass) releaseDeletingNodes(ctx context.Context) (bool, error) {
	if !p.claimsNodes() {
		return true, nil
	}
	keep := sets.New[string]()
	for _, node := range p.built.Nodes() {
		keep.Insert(node.Name())
	}
	for i := range p.deleting {
		if err := p.reconciler.finalize(ctx, &p.deleting[i], keep); err != nil {
			p.errs = append(p.errs, fmt.Errorf("releasing node %s: %w", p.deleting[i].Name, err))
		}
	}
	return true, nil
}

// materializeTree renders the tree into the backend on a workload plane. It
// runs only when every claim and release went through, so no object is
// written for a node without its finalizer.
func (p *quotaPass) materializeTree(ctx context.Context) (bool, error) {
	if !p.reconciler.Materialize.Enabled() || len(p.errs) > 0 {
		return true, nil
	}
	outcomes, err := p.reconciler.materialize(ctx, p.built, p.frozen)
	if err != nil {
		p.errs = append(p.errs, err)
	}
	p.outcomes = outcomes
	return true, nil
}

// projectTree fans the tree out onto the members on a management plane. A
// failure is collected, not returned: the per-cluster failures are already
// in the fleet view, and the status writes that follow matter most when a
// member refuses the tree.
func (p *quotaPass) projectTree(ctx context.Context) (bool, error) {
	if !p.reconciler.Project.Enabled() {
		return true, nil
	}
	fleet, err := p.reconciler.project(ctx, p.built, p.frozen)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("projecting the quota tree: %w", err))
	}
	p.fleet = fleet
	return true, nil
}

// rollFleetUsage sums what the members hold up the tree. Only a leaf carries
// figures, so without the roll a grouping tier and the root report nothing
// while the leaves beneath them run the whole fleet. The roll is the one the
// workload plane uses on its own queues: an ancestor's borrowed figure is
// recomputed, not summed, because a loan between two siblings is internal
// to the subtree that contains them.
func (p *quotaPass) rollFleetUsage(context.Context) (bool, error) {
	p.fleetRollup = usage.Roll(p.built, fleetLeafUsage(p.fleet))
	return true, nil
}

// readConsumption reads what the backend holds for each node. A backend
// that cannot be read costs this pass its usage figures and nothing else:
// the conditions and tree positions are still written.
func (p *quotaPass) readConsumption(ctx context.Context) (bool, error) {
	reading, err := p.reconciler.rollUsage(ctx, p.built)
	if err != nil {
		p.log.V(1).Info("consumption not read this pass", "reason", err.Error())
	}
	p.reading = reading
	return true, nil
}

// writeNodeStatuses writes every node's position and conditions. One node's
// failed write is collected and the rest are still written, so the nodes
// after a failure never keep a stale result.
func (p *quotaPass) writeNodeStatuses(ctx context.Context) (bool, error) {
	for _, node := range p.built.Nodes() {
		name := node.Name()
		if err := p.reconciler.reconcileStatus(ctx, node, p.frozen, p.outcomes[name], p.reading,
			p.fleet[name], p.fleetRollup[name]); err != nil {
			p.errs = append(p.errs, fmt.Errorf("writing status for node %s: %w", name, err))
		}
	}
	return true, nil
}

// sweepBudgetSeries drops the budget metrics of every node that left the
// tree without passing through its finalizer: force-stripped, or deleted
// while this manager was down. The pass holds the authoritative node set.
func (p *quotaPass) sweepBudgetSeries(context.Context) (bool, error) {
	inTree := make(map[string]struct{}, len(p.built.Nodes()))
	for _, node := range p.built.Nodes() {
		inTree[node.Name()] = struct{}{}
	}
	sweepBudgets(inTree)
	return true, nil
}

// collectFleetCapacity sums the members' capacity onto the root on a
// management plane. A conflict means the root moved under the pass, so the
// pass runs again at once and re-reads the root and the members rather than
// retry a sum against a stale root. Any other failure is collected.
func (p *quotaPass) collectFleetCapacity(ctx context.Context) (bool, error) {
	if !p.reconciler.Project.Enabled() || p.built.Root == nil {
		return true, nil
	}
	err := p.reconciler.reconcileFleetCapacity(ctx, p.built.Root.Quota.UID)
	switch {
	case err == nil:
	case apierrors.IsConflict(err):
		p.retryFleetCapacity = true
	default:
		p.errs = append(p.errs, fmt.Errorf("collecting fleet capacity: %w", err))
	}
	return true, nil
}

// failOnCollectedErrors ends the pass with every collected failure, so the
// whole pass is retried once every node has been tried. Each failure names
// its stage: claiming, releasing and status writing touch different verbs
// on different resources, and one prefix over all of them would send a
// reader after the wrong permission.
func (p *quotaPass) failOnCollectedErrors(context.Context) (bool, error) {
	if len(p.errs) == 0 {
		return true, nil
	}
	return false, fmt.Errorf("reconciling the quota tree: %w", errors.Join(p.errs...))
}

// logViolations logs how many violations the tree carries and on which nodes.
func (p *quotaPass) logViolations(context.Context) (bool, error) {
	if len(p.violations) > 0 {
		p.log.V(1).Info("quota tree has violations", "count", len(p.violations), "nodes", p.violations.Nodes())
	}
	return true, nil
}

// createReservedRoot creates the node that means "the whole cluster" when a
// capacity-reporting cluster has none, and ends the pass: a cluster reports
// what it has whether or not anyone has authored a budget. An unavailable
// webhook is a wait, not an error; the startup pass makes the same create
// and the condition clears on its own. A name held by a node that is not
// the tree root cannot be fixed by creating or retrying, so the pass says
// so and keeps the resync, the only thing that notices the tree repaired.
func (p *quotaPass) createReservedRoot(ctx context.Context) (bool, error) {
	if !p.reconciler.Capacity.Enabled() || p.built.Root != nil {
		return true, nil
	}
	created, err := p.reconciler.ensureRoot(ctx)
	if err != nil {
		if !webhookUnavailable(err) {
			return p.end(ctrl.Result{}, err)
		}
		p.log.V(1).Info("waiting to create the reserved root",
			"root", p.reconciler.Options.RootName, "reason", err.Error())
		return p.end(ctrl.Result{RequeueAfter: rootBootstrapRetry}, nil)
	}
	if !created {
		p.log.Info("capacity derivation is idle: the reserved root name is held by a node that is not the tree root",
			"root", p.reconciler.Options.RootName)
	}
	return p.end(ctrl.Result{RequeueAfter: p.reconciler.nextResync()}, nil)
}

// deriveCapacity samples this cluster's accelerator capacity onto the root
// on a capacity-reporting cluster. A failed sample ends the pass with its
// error.
func (p *quotaPass) deriveCapacity(ctx context.Context) (bool, error) {
	if !p.reconciler.Capacity.Enabled() || p.built.Root == nil {
		return true, nil
	}
	if err := p.reconciler.reconcileCapacity(ctx, p.built.Root.Quota); err != nil {
		return false, err
	}
	return true, nil
}

// scheduleNextPass ends the pass with its requeue: at once when the fleet
// capacity write hit a conflict, else after the resync interval, where zero
// leaves the pass event-driven.
func (p *quotaPass) scheduleNextPass(context.Context) (bool, error) {
	if p.retryFleetCapacity {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return p.end(ctrl.Result{RequeueAfter: p.reconciler.nextResync()}, nil)
}

// reconcileStatus writes one node's observed position and condition. It patches
// only when something changed, so an idle fleet does not rewrite status on every
// resync tick and spin the watch.
func (r *Reconciler) reconcileStatus(ctx context.Context, node *tree.Node, frozen map[string]tree.Violation,
	outcome backend.NodeOutcome, reading usageReading,
	onClusters map[string]clusterState, rolled map[usage.Key]usage.Total,
) error {
	current := node.Quota
	updated := current.DeepCopy()

	updated.Status.ObservedGeneration = current.Generation
	updated.Status.Parent = ""
	if node.Parent != nil {
		updated.Status.Parent = node.Parent.Name()
	}
	// Only rewritten when consumption was actually read. A pass that could not
	// reach the backend leaves the previous figures standing rather than
	// blanking them: "unknown" and "nothing held" are different answers, and
	// only one of them is safe to publish on a failed read.
	if totals, ok := reading.forNode(node.Name()); ok {
		updated.Status.Budgets = budgetStatus(node, totals)
		recordBudgets(string(r.Mode), node.Name(), string(node.Role()), updated.Status.Budgets)
	}

	// Only a projecting plane has members to report, and only it should clear
	// the field: a workload-mode manager writing nil here would erase what the
	// management plane published about the same node name.
	if r.Project.Enabled() {
		updated.Status.Clusters = clusterStatuses(onClusters, current.Status.Clusters)
		// The management plane's own budget figures, which no backend can give
		// it: it runs no Kueue and materializes nothing, so its numbers are the
		// fleet's members added up.
		updated.Status.Budgets = fleetBudgets(node.Quota.Spec.Budgets, onClusters, rolled)
	}

	violation, isFrozen := frozen[node.Name()]
	setConditions(&updated.Status, violation, isFrozen)
	if r.Materialize.Enabled() {
		setMaterializationStatus(&updated.Status, current.Generation, outcome, isFrozen, metav1.Now())
		updated.Status.SourceGeneration = materializedSourceGeneration(current, updated.Status)
	}

	if equalStatus(current.Status, updated.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFrom(current)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// The node was deleted or rewritten mid-pass; the next reconcile
			// sees the newer state. Neither is worth failing the pass over.
			return nil
		}
		return err
	}
	return nil
}

// setConditions stamps Ready and Degraded as a mutually exclusive pair, so a
// reader never has to reconcile two conditions that disagree.
//
// A node frozen by an ancestor reports the ancestor's cause rather than a
// generic message: the operator has to fix the ancestor, and saying so is the
// difference between a two-minute fix and a hunt.
func setConditions(status *v1beta1.AcceleratorQuotaStatus, violation tree.Violation, isFrozen bool) {
	now := metav1.Now()
	ready := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "node is a valid member of the quota tree",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}
	degraded := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaDegraded,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "no invariant violated",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}

	if isFrozen {
		ready.Status = metav1.ConditionFalse
		ready.Reason = violation.Reason
		ready.Message = violation.Message
		degraded.Status = metav1.ConditionTrue
		degraded.Reason = violation.Reason
		degraded.Message = violation.Message
	}

	apimeta.SetStatusCondition(&status.Conditions, ready)
	apimeta.SetStatusCondition(&status.Conditions, degraded)
}

// materializedSourceGeneration echoes the generation the management plane
// stamped on a projection, but only once this pass actually materialized it.
//
// The number is the only thing that makes remote progress comparable: a
// projection's own generation is object-local and says nothing about which
// source revision it reflects. Echoing it before the objects were written would
// report progress that has not happened -- so a node that is frozen, or that
// failed to materialize, keeps whatever it last earned rather than advancing.
//
// Empty on a node nobody projected: an admin-authored node has no source
// elsewhere, and a number invented here would claim otherwise.
func materializedSourceGeneration(current *v1beta1.AcceleratorQuota,
	status v1beta1.AcceleratorQuotaStatus,
) int64 {
	stamped := current.Annotations[v1beta1.AcceleratorQuotaSourceGenerationAnnotation]
	if stamped == "" {
		return 0
	}
	if !apimeta.IsStatusConditionTrue(status.Conditions, v1beta1.AcceleratorQuotaMaterialized) {
		return current.Status.SourceGeneration
	}
	// A projector writes this annotation, so an unparsable value means someone
	// edited it by hand. Holding the previous number is more honest than
	// reporting zero, which reads as "materialized nothing yet".
	n, err := strconv.ParseInt(stamped, 10, 64)
	if err != nil {
		return current.Status.SourceGeneration
	}
	return n
}

// clusterStatuses renders one node's per-member rows, sorted so two passes over
// the same fleet produce the same object.
//
// A member that reported nothing this pass keeps the generations it last
// earned. The alternative -- zeroing them -- would read as "this member has
// materialized nothing", which is a much stronger claim than "we could not ask
// it", and the two call for opposite responses from an operator.
func clusterStatuses(observed map[string]clusterState,
	previous []v1beta1.AcceleratorQuotaClusterStatus,
) []v1beta1.AcceleratorQuotaClusterStatus {
	if len(observed) == 0 {
		return previous
	}
	was := make(map[string]v1beta1.AcceleratorQuotaClusterStatus, len(previous))
	for _, p := range previous {
		was[p.Cluster] = p
	}

	out := make([]v1beta1.AcceleratorQuotaClusterStatus, 0, len(observed))
	for cluster, st := range observed {
		row := v1beta1.AcceleratorQuotaClusterStatus{
			Cluster:                cluster,
			AppliedGeneration:      st.appliedGeneration,
			AppliedTime:            st.appliedTime,
			MaterializedGeneration: st.materializedGeneration,
			Message:                st.message,
		}
		if prev, ok := was[cluster]; ok {
			if row.AppliedGeneration == 0 {
				row.AppliedGeneration = prev.AppliedGeneration
				row.AppliedTime = prev.AppliedTime
			}
			if row.MaterializedGeneration == 0 {
				row.MaterializedGeneration = prev.MaterializedGeneration
			}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cluster < out[j].Cluster })
	return out
}

// fleetLeafUsage reshapes the per-cluster view into what the roll expects: one
// entry per node, summed across the members holding it.
//
// Summing across CLUSTERS is not the same as summing across children, which the
// roll refuses to do for borrowed. Borrowing is cluster-local -- a Kueue cohort
// lives on one member -- so two members each lending to the same tenant are two
// separate loans, and the fleet really is running that much above its shares.
func fleetLeafUsage(fleet fleetView) map[string]map[usage.Key]usage.Observed {
	if len(fleet) == 0 {
		return nil
	}
	out := map[string]map[usage.Key]usage.Observed{}
	for node, byCluster := range fleet {
		for _, st := range byCluster {
			for _, cb := range st.budgets {
				if cb.resourceName == "" {
					continue
				}
				if out[node] == nil {
					out[node] = map[usage.Key]usage.Observed{}
				}
				k := usage.Key{ResourceName: cb.resourceName, ResourceFlavor: cb.resourceFlavor}
				agg := out[node][k]
				agg.Admitted.Add(cb.admitted)
				agg.Reserved.Add(cb.reserved)
				agg.Borrowed.Add(cb.borrowed)
				out[node][k] = agg
			}
		}
	}
	return out
}

// fleetBudgets is the hub's view of one node's budgets: what the admin
// authorized, and what the fleet is doing with it.
//
// Nominal is the authored total rather than the sum of the shares. They agree
// when every member reported, and when one did not the split is held -- so
// summing would quietly report a smaller fleet total for as long as a member
// was missing.
// The authored number is what the admin wrote and stays what the hub reports.
//
// The consumption figures ARE sums, because consumption is a fact about the
// fleet: work admitted on one member is work the fleet is running. Borrowed
// sums too, and means the same thing one tier up -- how much of what is running
// sits above the share that authorized it.
func fleetBudgets(authored []v1beta1.AcceleratorBudget,
	onClusters map[string]clusterState, rolled map[usage.Key]usage.Total,
) []v1beta1.AcceleratorBudgetStatus {
	if len(authored) == 0 {
		return nil
	}
	out := make([]v1beta1.AcceleratorBudgetStatus, 0, len(authored))
	for _, b := range authored {
		key := budgetKey(b.ResourceName, b.ResourceFlavor)
		row := v1beta1.AcceleratorBudgetStatus{
			ResourceName:   b.ResourceName,
			ResourceFlavor: b.ResourceFlavor,
			Nominal:        b.Nominal.DeepCopy(),
		}
		// The breakdown is this node's own, so only a leaf has one: a share is
		// apportioned to a leaf, and an ancestor is projected budget-less. An
		// ancestor with per-cluster rows would have to invent a nominal nobody
		// assigned it.
		for cluster, st := range onClusters {
			cb, ok := st.budgets[key]
			if !ok {
				continue
			}
			row.PerCluster = append(row.PerCluster, v1beta1.AcceleratorClusterBudgetStatus{
				Cluster:  cluster,
				Nominal:  cb.nominal.DeepCopy(),
				Admitted: cb.admitted.DeepCopy(),
				Reserved: cb.reserved.DeepCopy(),
				Borrowed: cb.borrowed.DeepCopy(),
			})
		}
		sort.Slice(row.PerCluster, func(i, j int) bool {
			return row.PerCluster[i].Cluster < row.PerCluster[j].Cluster
		})

		// The totals come from the roll, which is what carries a leaf's figures
		// up to the tiers above it.
		if t, ok := rolled[usage.Key{ResourceName: b.ResourceName, ResourceFlavor: b.ResourceFlavor}]; ok {
			row.Admitted = t.Admitted.DeepCopy()
			row.Reserved = t.Reserved.DeepCopy()
			row.Borrowed = t.Borrowed.DeepCopy()
		}
		out = append(out, row)
	}
	return out
}

// equalPerCluster compares one budget's member rows.
func equalPerCluster(a, b []v1beta1.AcceleratorClusterBudgetStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Cluster != b[i].Cluster ||
			a[i].Nominal.Cmp(b[i].Nominal) != 0 ||
			a[i].Admitted.Cmp(b[i].Admitted) != 0 ||
			a[i].Reserved.Cmp(b[i].Reserved) != 0 ||
			a[i].Borrowed.Cmp(b[i].Borrowed) != 0 {
			return false
		}
	}
	return true
}

// equalClusters compares the per-member rows, excluding AppliedTime. Including
// it would make every successful pass a write, since a clean re-apply restamps
// the time whether or not anything moved -- and this status is watched, so a
// write per pass is a reconcile per pass forever.
func equalClusters(a, b []v1beta1.AcceleratorQuotaClusterStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Cluster != b[i].Cluster ||
			a[i].AppliedGeneration != b[i].AppliedGeneration ||
			a[i].MaterializedGeneration != b[i].MaterializedGeneration ||
			a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}

// equalStatus compares the fields this controller owns. Condition timestamps are
// excluded deliberately: SetStatusCondition only moves LastTransitionTime when
// the status actually flips, so comparing the rest is what makes a no-op pass a
// genuine no-op.
func equalStatus(a, b v1beta1.AcceleratorQuotaStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration || a.Parent != b.Parent {
		return false
	}
	if a.SourceGeneration != b.SourceGeneration {
		return false
	}

	if !equalMaterialization(a.Materialization, b.Materialization) {
		return false
	}
	if !equalBudgets(a.Budgets, b.Budgets) {
		return false
	}
	if !equalClusters(a.Clusters, b.Clusters) {
		return false
	}
	for _, condType := range []string{
		v1beta1.AcceleratorQuotaReady,
		v1beta1.AcceleratorQuotaDegraded,
		v1beta1.AcceleratorQuotaMaterialized,
	} {
		ca := apimeta.FindStatusCondition(a.Conditions, condType)
		cb := apimeta.FindStatusCondition(b.Conditions, condType)
		if ca == nil || cb == nil {
			if ca != cb {
				return false
			}
			continue
		}
		if ca.Status != cb.Status || ca.Reason != cb.Reason || ca.Message != cb.Message ||
			ca.ObservedGeneration != cb.ObservedGeneration {
			return false
		}
	}
	return true
}

// SetupWithManager wires the reconciler to watch AcceleratorQuotas.
//
// Every event enqueues the same key, because the unit of work is the tree, not
// the object: editing one node can change another's verdict, and a per-object
// queue would leave the second node reporting a stale condition until it
// happened to be touched. Collapsing to one key also means a burst of edits
// coalesces into a single rebuild.
// WithTransportEvents wires the channel the Connector signals when the set of
// reachable members moves, so a fleet that becomes whole again is re-split at
// once rather than on the next resync tick. Without it the controller is
// poll-only, which is correct but slow to recover.
func WithTransportEvents(ch <-chan event.GenericEvent) Option {
	return func(o *options) { o.transportEvents = ch }
}

// WithMemberEvents wires the channel a status funnel fills when an
// AcceleratorQuota changes on any member, so the hub's account of the fleet
// follows the fleet instead of a clock.
//
// Consumption moves without anything in the tree changing, and none of this
// controller's own triggers notice: it is woken by edits to the hub's own
// nodes and by its resync tick, neither of which fires when a tenant's work
// starts or finishes on a member. The workload plane has the same problem one
// layer down and solves it the same way, by watching the objects that carry the
// figures rather than re-reading them on a timer.
//
// This does not feed back on itself. The hub's own applies to a member are
// server-side applies, and one that changes nothing does not bump
// resourceVersion, so it raises no event to wake the pass that made it.
func WithMemberEvents(ch <-chan event.GenericEvent) Option {
	return func(o *options) { o.memberEvents = ch }
}

// Option configures SetupWithManager.
type Option func(*options)

type options struct {
	transportEvents <-chan event.GenericEvent
	memberEvents    <-chan event.GenericEvent
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...Option) error {
	if r.Capacity.ReportInterval < 0 {
		return fmt.Errorf("capacity report interval must be nonnegative")
	}
	var cfg options
	for _, o := range opts {
		o(&cfg)
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.APIReader == nil {
		return fmt.Errorf("acceleratorquota: APIReader is required (the tree verdict must not be read from a cache)")
	}
	if r.Options.RootName == "" {
		return fmt.Errorf("acceleratorquota: %w", tree.ErrRootNameUnset)
	}
	b := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.AcceleratorQuota{}).
		Named("acceleratorquota")
	if r.Capacity.Enabled() {
		// A RunnableFunc is leader-elected, so a standby replica does not race
		// the leader for the root; AlreadyExists is benign in any case.
		if err := mgr.Add(manager.RunnableFunc(r.bootstrapRoot)); err != nil {
			return fmt.Errorf("acceleratorquota: scheduling the root check: %w", err)
		}
		b = r.watchNodeCapacity(b, mgr)
	}
	b = r.watchUsage(b)
	b = r.watchMembers(b, cfg.memberEvents)
	b = r.watchTransport(b, cfg.transportEvents)
	return b.Complete(r)
}

// enqueueRoot maps every event to the root's request. The pass rebuilds the
// whole tree from the root, so which object moved does not narrow the work, and
// a burst of events collapses into one queued pass rather than one each.
func enqueueRoot[T client.Object](rootName string) handler.TypedEventHandler[T, reconcile.Request] {
	return handler.TypedEnqueueRequestsFromMapFunc(func(context.Context, T) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: rootName}}}
	})
}

// watchNodeCapacity queues a pass when a node's accelerator capacity moves.
//
// The predicate is not an optimisation to revisit later. Node status is among
// the highest-volume writes in a cluster and almost none of it moves a number
// this controller reads, so without it a large fleet would rebuild the tree
// continuously.
func (r *Reconciler) watchNodeCapacity(b *builder.Builder, mgr ctrl.Manager) *builder.Builder {
	return b.WatchesRawSource(source.Kind(
		mgr.GetCache(),
		&corev1.Node{},
		enqueueRoot[*corev1.Node](r.Options.RootName),
		predicate.TypedFuncs[*corev1.Node]{
			UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
				return nodeCapacityChanged(e.ObjectOld, e.ObjectNew, r.Capacity.Resources)
			},
		},
	))
}

// watchUsage queues a pass when the usage backend's figures move.
//
// Consumption changes without anything in the tree changing, and nothing else
// here would notice: the reconcile runs on AcceleratorQuota edits and on node
// capacity, neither of which a workload admission touches. Left to the
// resync tick alone, a queue's figures are up to a full interval stale, long
// enough that a fleet with work running reads as an idle one. A backend that
// cannot say what to watch is not watched, and its figures refresh on the
// resync tick.
func (r *Reconciler) watchUsage(b *builder.Builder) *builder.Builder {
	watcher, ok := r.Materialize.Backend.(backend.UsageWatcher)
	if !ok {
		return b
	}
	obj, usageChanged := watcher.WatchUsage()
	return b.Watches(obj, enqueueRoot[client.Object](r.Options.RootName), builder.WithPredicates(usageChanged))
}

// watchMembers queues a pass when an AcceleratorQuota changes on a member, so
// the hub's account of the fleet follows the fleet instead of a clock. A nil
// channel leaves the hub on its resync tick.
func (r *Reconciler) watchMembers(b *builder.Builder, events <-chan event.GenericEvent) *builder.Builder {
	if events == nil {
		return b
	}
	return b.WatchesRawSource(source.Channel(events, enqueueRoot[client.Object](r.Options.RootName)))
}

// watchTransport queues a pass when a member connects or drops, so a
// reconnection does not wait out the resync interval. A nil channel leaves the
// hub on its resync tick.
func (r *Reconciler) watchTransport(b *builder.Builder, events <-chan event.GenericEvent) *builder.Builder {
	if events == nil {
		return b
	}
	return b.WatchesRawSource(source.Channel(events, enqueueRoot[client.Object](r.Options.RootName)))
}
