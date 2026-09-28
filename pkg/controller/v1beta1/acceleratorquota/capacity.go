package acceleratorquota

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/capacity"
)

// rootBootstrapRetry paces the root check while it waits. Not a knob, and
// deliberately not one: the only thing it waits on is this process's own
// admission path coming up, which is a startup transient measured in seconds.
// Every other trigger for the same check is event-driven or the resync tick.
const rootBootstrapRetry = 2 * time.Second

// webhookUnavailable reports whether the apiserver refused a write because it
// could not reach an admission webhook.
//
// At startup that is this component refusing its own write: the reserved-root
// create is gated by a fail-closed webhook this same process serves, and until
// the replica is a ready endpoint of its own webhook Service the apiserver has
// nowhere to send the review. It clears itself within seconds, so it is a wait
// rather than a failure. Every other refusal — a rejected spec, a denied
// create — stays an error.
//
// The prefix is the apiserver's own, applied to every admission dial failure
// whatever the cause underneath: no endpoints yet, no listener yet, a caBundle
// that does not verify the served chain.
func webhookUnavailable(err error) bool {
	return apierrors.IsInternalError(err) && strings.Contains(err.Error(), "failed calling webhook")
}

// CapacityOptions configure deriving the cluster's own accelerator capacity
// onto the reserved root. Absent config disables derivation rather than
// guessing, so a manager left unconfigured behaves exactly as it did before
// this existed.
type CapacityOptions struct {
	// Resources are the extended resource names that count as accelerators, in
	// full: "google.com/tpu", "nvidia.com/gpu". Empty disables derivation
	// entirely — without it nothing distinguishes a node's accelerators from
	// its cpu and memory, and guessing is not an option for a number budgets
	// are checked against.
	Resources []string

	// HysteresisPercent is how far the observed capacity must fall below the
	// recorded high-water mark before the mark follows it down. Zero disables
	// damping, so the mark tracks every dip — including the ones caused by a
	// drain or a reboot.
	HysteresisPercent int32

	// ReportInterval bounds how long unchanged hardware observations can remain
	// unpublished. Zero disables periodic timestamp refreshes.
	ReportInterval time.Duration
}

// Enabled reports whether capacity derivation is configured.
func (o CapacityOptions) Enabled() bool { return len(o.Resources) > 0 }

// ensureRoot creates the reserved root when capacity derivation is on and no
// node claims that name.
//
// Capacity is a property of the cluster, not of anything an admin authored, so
// it has to be reportable before any tree exists — a management plane sizing a
// Proportional split needs each member's capacity precisely when that member
// has no budget yet. Waiting for someone to author a root would make the
// bootstrap circular.
//
// It is created bare: role Cohort, no parentRef, no budgets. A budget here is
// the admin's fleet total to write, and inventing one would be this controller
// authorizing work.
// It reports whether it created the node. Not created is two different
// situations the caller has to tell apart: another replica won the race, which
// is fine, or the name is already held by a node that is not the tree root —
// one carrying a parentRef, or caught in a cycle — which no amount of retrying
// will resolve.
func (r *Reconciler) ensureRoot(ctx context.Context) (bool, error) {
	root := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: r.Options.RootName},
		Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
	}
	if err := r.Create(ctx, root); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, fmt.Errorf("creating the reserved root: %w", err)
	}
	return true, nil
}

// bootstrapRoot is the startup pass that brings the reserved root into being on
// a cluster where nothing has happened yet to fire a reconcile.
//
// It retries rather than failing, because the write it makes is gated by this
// component's own validating webhook: failurePolicy=Fail, with a caBundle this
// same process injects only once the manager is running. So the first attempts
// are refused for a reason that clears itself — the caBundle is still the
// chart's placeholder, or the serving cert has not been projected back down to
// the webhook server yet. Surfacing that from a Runnable would stop the manager
// and take the rotator down with it, and the restart would lose the identical
// race: a startup that cannot converge on its own. The create is idempotent, so
// retrying until it lands costs nothing.
func (r *Reconciler) bootstrapRoot(ctx context.Context) error {
	return r.bootstrapRootEvery(ctx, rootBootstrapRetry)
}

// bootstrapRootEvery is bootstrapRoot with the cadence named by the caller, so
// the contract that a refused create is survivable can be exercised without
// waiting out the production interval.
func (r *Reconciler) bootstrapRootEvery(ctx context.Context, every time.Duration) error {
	// The poll's only error is the manager stopping, which is not this check
	// failing.
	_ = wait.PollUntilContextCancel(ctx, every, true,
		func(ctx context.Context) (bool, error) {
			if _, err := r.ensureRoot(ctx); err != nil {
				r.Log.V(1).Info("waiting to create the reserved root",
					"root", r.Options.RootName, "reason", err.Error())
				return false, nil
			}
			return true, nil
		})
	return nil
}

// reconcileCapacity samples the cluster's nodes and records the result on the
// reserved root.
//
// Only the root carries capacity: it is the one node that means "this whole
// cluster", and a per-node figure would invite the reading that a leaf's
// capacity is somehow its own rather than a share of one pool.
//
// Status only, never spec. On a management plane the root's spec is the
// admin-authored fleet total; if the workload controller wrote spec too, the
// two planes would fight over one object with last-writer-wins. Splitting them
// across spec and status lets both write the same CR indefinitely.
func (r *Reconciler) reconcileCapacity(ctx context.Context, root *v1beta1.AcceleratorQuota) error {
	var nodes corev1.NodeList
	if err := r.APIReader.List(ctx, &nodes); err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	flavors, err := r.flavors(ctx)
	if err != nil {
		return err
	}

	observed, err := capacity.Sum(nodes.Items, flavors, capacity.Options{
		AcceleratorResources: r.Capacity.Resources,
	})
	if err != nil {
		return fmt.Errorf("summing node capacity: %w", err)
	}

	// Publish before merging. mergeCapacity folds the observation into the
	// high-water marks and only the merged figure reaches status, so the
	// unattributed slice and the raw node counts have no other reader.
	recordCapacity(observed)

	now := metav1.Now()
	updated := root.DeepCopy()
	samples, attribution := attributeCapacity(observed, flavors, r.Capacity.Resources)
	updated.Status.Capacity = mergeCapacity(
		root.Status.Capacity, observed.Capacities, r.Capacity.HysteresisPercent, now)
	recorded := map[string]bool{}
	for i := range updated.Status.Capacity {
		sample := &updated.Status.Capacity[i]
		key := budgetKey(sample.ResourceName, sample.ResourceFlavor)
		recorded[key] = true
		sample.Attribution = attribution[key]
	}
	// Add first observations of empty pools after high-water tracking; a pool
	// without nodes must retain the quota tracker's established historical mark.
	for _, sample := range samples {
		key := budgetKey(sample.ResourceName, sample.ResourceFlavor)
		if !recorded[key] {
			updated.Status.Capacity = append(updated.Status.Capacity, v1beta1.AcceleratorCapacityStatus{
				ResourceName: sample.ResourceName, ResourceFlavor: sample.ResourceFlavor,
				ObservedAt: now.DeepCopy(), Attribution: attribution[key],
			})
		}
	}
	slices.SortFunc(updated.Status.Capacity, func(a, b v1beta1.AcceleratorCapacityStatus) int {
		if order := cmp.Compare(a.ResourceName, b.ResourceName); order != 0 {
			return order
		}
		return cmp.Compare(a.ResourceFlavor, b.ResourceFlavor)
	})

	if equalCapacity(root.Status.Capacity, updated.Status.Capacity) &&
		!capacityReportDue(root.Status.Capacity, now.Time, r.Capacity.ReportInterval) {
		return nil
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFrom(root)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// The root was rewritten mid-pass; the next reconcile samples again.
			return nil
		}
		return fmt.Errorf("writing root capacity: %w", err)
	}
	return nil
}

// capacityReportDue refreshes only completed samples. A failed read cannot
// extend their freshness, and an absent report cannot claim observed hardware.
func capacityReportDue(reports []v1beta1.AcceleratorCapacityStatus, now time.Time, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	for _, report := range reports {
		if report.ObservedAt == nil || report.ObservedAt.After(now) || now.Sub(report.ObservedAt.Time) >= interval {
			return true
		}
	}
	return false
}

func (r *Reconciler) nextResync() time.Duration {
	interval := r.ResyncInterval
	if report := r.Capacity.ReportInterval; r.Capacity.Enabled() && report > 0 && (interval <= 0 || report < interval) {
		interval = report
	}
	return interval
}

// flavors reads the Kueue ResourceFlavors capacity is attributed to.
//
// Read, never authored, and never defaulted: a flavor names a hardware class by
// node labels, OME does not own node labelling, and a compiled-in flavor name
// would be a number this controller invented and then reported as observed
// fact.
//
// A cluster without Kueue installed has no flavors, which is not an error —
// quota without Kueue materializes nothing anyway. Every accelerator then comes
// back unattributed, which the caller reports rather than silently rounding to
// zero.
func (r *Reconciler) flavors(ctx context.Context) ([]capacity.Flavor, error) {
	var list kueuev1beta2.ResourceFlavorList
	if err := r.APIReader.List(ctx, &list); err != nil {
		if apimeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing ResourceFlavors: %w", err)
	}
	out := make([]capacity.Flavor, 0, len(list.Items))
	for i := range list.Items {
		f := &list.Items[i]
		out = append(out, capacity.Flavor{Name: f.Name, UID: f.UID, NodeLabels: f.Spec.NodeLabels})
	}
	return out, nil
}

// mergeCapacity folds an observation into the marks already on the root.
func mergeCapacity(
	previous []v1beta1.AcceleratorCapacityStatus,
	observed []capacity.Capacity,
	hysteresisPercent int32,
	now metav1.Time,
) []v1beta1.AcceleratorCapacityStatus {
	marks := make([]capacity.Mark, 0, len(previous))
	for _, p := range previous {
		marks = append(marks, capacity.Mark{
			ResourceName:   p.ResourceName,
			ResourceFlavor: p.ResourceFlavor,
			Allocatable:    p.Allocatable,
			HighWaterMark:  p.HighWaterMark,
		})
	}

	tracked := capacity.TrackAll(marks, observed, hysteresisPercent)
	if len(tracked) == 0 {
		return nil
	}
	out := make([]v1beta1.AcceleratorCapacityStatus, 0, len(tracked))
	for _, t := range tracked {
		out = append(out, v1beta1.AcceleratorCapacityStatus{
			ResourceName:   t.ResourceName,
			ResourceFlavor: t.ResourceFlavor,
			Allocatable:    t.Allocatable,
			HighWaterMark:  t.HighWaterMark,
			ObservedAt:     now.DeepCopy(),
		})
	}
	return out
}

// equalCapacity compares everything except ObservedAt, which changes on every
// sample. Including it would rewrite the root's status on every pass and spin
// the watch that triggered the pass.
func equalCapacity(a, b []v1beta1.AcceleratorCapacityStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ResourceName != b[i].ResourceName ||
			a[i].ResourceFlavor != b[i].ResourceFlavor ||
			a[i].Allocatable.Cmp(b[i].Allocatable) != 0 ||
			a[i].HighWaterMark.Cmp(b[i].HighWaterMark) != 0 ||
			!apiequality.Semantic.DeepEqual(a[i].Attribution, b[i].Attribution) {
			return false
		}
	}
	return true
}

// nodeCapacityChanged reports whether an update could change the derived total.
//
// Without it every kubelet status write would rebuild the whole tree: node
// updates are among the highest-volume events in a cluster, and almost none of
// them move a number this controller reads.
func nodeCapacityChanged(old, updated *corev1.Node, resources []string) bool {
	if old.Spec.Unschedulable != updated.Spec.Unschedulable {
		return true
	}
	if nodeReady(old) != nodeReady(updated) {
		return true
	}
	// Labels decide which flavor a node belongs to, so a relabel moves capacity
	// between pools without changing any quantity.
	if !equalStringMaps(old.Labels, updated.Labels) {
		return true
	}
	for _, name := range resources {
		res := corev1.ResourceName(name)
		before, had := old.Status.Allocatable[res]
		after, has := updated.Status.Allocatable[res]
		if had != has {
			return true
		}
		if had && before.Cmp(after) != 0 {
			return true
		}
	}
	return false
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
