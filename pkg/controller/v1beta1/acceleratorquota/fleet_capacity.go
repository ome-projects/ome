package acceleratorquota

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// identityClusterClients refuses a connection established for another registry
// object, even when the cluster name and credentials are unchanged.
type identityClusterClients interface {
	ClientForUID(string, types.UID) (workloadcluster.SelectivelyCachingClient, bool)
}

type capacityPair struct {
	resource string
	flavor   string
}

type memberCapacity map[capacityPair]v1beta1.AcceleratorClusterCapacityStatus

// reconcileFleetCapacity publishes member observations independently of quota
// projection. An unreachable member must not prevent healthy reports advancing.
func (r *Reconciler) reconcileFleetCapacity(ctx context.Context, rootUID types.UID) error {
	transport, ok := r.Project.Clusters.(identityClusterClients)
	if !ok || rootUID == "" {
		return nil
	}
	root, err := r.liveRoot(ctx, rootUID)
	if err != nil || root == nil {
		return err
	}
	registered, err := r.capacityRegistry(ctx)
	if err != nil {
		return err
	}
	collection := fleetCollection{transport: transport, rootUID: rootUID, registered: registered}
	collection.retain(root)
	r.readMembers(ctx, &collection)

	// The registry may change during remote reads. A replacement cannot
	// inherit a report fetched through the departed registration's client.
	current, err := r.capacityRegistry(ctx)
	if err != nil {
		return err
	}
	collection.dropDeparted(current)
	updated := root.DeepCopy()
	updated.Status.Capacity = aggregateCapacity(collection.reports, len(current))
	if apiequality.Semantic.DeepEqual(root.Status.Capacity, updated.Status.Capacity) {
		return nil
	}
	// The root version fences concurrent collectors and object recreation.
	return r.Status().Patch(ctx, updated, client.MergeFromWithOptions(root, client.MergeFromWithOptimisticLock{}))
}

// liveRoot reads the local root and returns nil when there is nothing to
// publish onto: the root is gone, it is another object than the one the
// pass built, or it is being deleted.
func (r *Reconciler) liveRoot(ctx context.Context, rootUID types.UID) (*v1beta1.AcceleratorQuota, error) {
	var root v1beta1.AcceleratorQuota
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: r.Options.RootName}, &root); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if root.UID != rootUID || !root.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return &root, nil
}

// fleetCollection is one pass's gathering of member capacity: the
// registrations the pass started from, the transport that reaches them, and
// the report kept per member.
type fleetCollection struct {
	transport  identityClusterClients
	rootUID    types.UID
	registered map[string]types.UID
	reports    map[string]memberCapacity
}

// retain keeps, per registered member, the rows the root already publishes
// for it, each marked unavailable until this pass reads the member again.
// Rows of a member whose registration changed are dropped.
func (c *fleetCollection) retain(root *v1beta1.AcceleratorQuota) {
	c.reports = make(map[string]memberCapacity, len(c.registered))
	for _, cell := range root.Status.Capacity {
		for _, row := range cell.PerCluster {
			if row.ClusterUID == "" || row.ClusterUID != c.registered[row.Cluster] {
				continue
			}
			if c.reports[row.Cluster] == nil {
				c.reports[row.Cluster] = memberCapacity{}
			}
			retained := row.DeepCopy()
			retained.ReportAvailable = false
			c.reports[row.Cluster][capacityPair{cell.ResourceName, cell.ResourceFlavor}] = *retained
		}
	}
}

// readMembers replaces each member's retained report with a fresh read
// where the member is connected and its report is usable. A member that
// cannot be read keeps its retained rows, marked unavailable.
func (r *Reconciler) readMembers(ctx context.Context, c *fleetCollection) {
	for cluster, uid := range c.registered {
		remote, ok := c.transport.ClientForUID(cluster, uid)
		if !ok {
			continue
		}
		report, err := r.readMemberCapacity(ctx, remote, c.rootUID, cluster, uid)
		if err != nil {
			r.Log.V(1).Info("member capacity report unavailable", "cluster", cluster, "reason", err.Error())
			continue
		}
		c.reports[cluster] = report
	}
}

// dropDeparted applies the registry as re-read after the member reads: a
// member whose registration is gone or replaced loses its report, and one
// whose connection is gone keeps its rows marked unavailable.
func (c *fleetCollection) dropDeparted(current map[string]types.UID) {
	for cluster, rows := range c.reports {
		if current[cluster] == "" || current[cluster] != c.registered[cluster] {
			delete(c.reports, cluster)
			continue
		}
		if _, ok := c.transport.ClientForUID(cluster, current[cluster]); !ok {
			for key, row := range rows {
				row.ReportAvailable = false
				rows[key] = row
			}
		}
	}
}

func (r *Reconciler) capacityRegistry(ctx context.Context) (map[string]types.UID, error) {
	var list v1beta1.WorkloadClusterList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return nil, err
	}
	out := make(map[string]types.UID, len(list.Items))
	for _, cluster := range list.Items {
		if cluster.UID != "" && cluster.DeletionTimestamp.IsZero() {
			out[cluster.Name] = cluster.UID
		}
	}
	return out, nil
}

// readMemberCapacity reads one member's own capacity report and turns it
// into this pass's rows for the member.
func (r *Reconciler) readMemberCapacity(ctx context.Context, remote workloadcluster.SelectivelyCachingClient,
	localUID types.UID, cluster string, clusterUID types.UID,
) (memberCapacity, error) {
	key := client.ObjectKey{Name: r.Options.RootName}
	var root v1beta1.AcceleratorQuota
	if err := remote.Get(ctx, key, &root); err != nil {
		return nil, err
	}
	if !localCapacityReport(&root, localUID) {
		return nil, fmt.Errorf("member root is not an identified local capacity report")
	}
	// Metadata reads bypass the transport cache. A cached report is usable
	// only while the member still holds that exact source object version.
	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(acceleratorQuotaGVK)
	if err := remote.Get(ctx, key, metadata); err != nil {
		return nil, err
	}
	if reportMoved(&root, metadata) {
		return nil, fmt.Errorf("member capacity report changed during collection")
	}
	return memberSamples(&root, cluster, clusterUID)
}

// localCapacityReport reports whether root is a member's own capacity
// report: an identified, versioned, live cohort root without a parent,
// which is neither this plane's root nor a copy this plane projected onto
// the member.
func localCapacityReport(root *v1beta1.AcceleratorQuota, localUID types.UID) bool {
	_, projected := root.Labels[v1beta1.AcceleratorQuotaOriginLabel]
	return root.UID != "" && root.UID != localUID && root.ResourceVersion != "" && root.DeletionTimestamp.IsZero() &&
		root.Spec.ParentRef == nil && root.Spec.Role == v1beta1.AcceleratorQuotaRoleCohort && !projected
}

// reportMoved reports whether the member's root is not the object version
// the report was read from: another UID, another version, or a deletion
// under way.
func reportMoved(root *v1beta1.AcceleratorQuota, metadata *metav1.PartialObjectMetadata) bool {
	return metadata.UID != root.UID || metadata.ResourceVersion != root.ResourceVersion || !metadata.DeletionTimestamp.IsZero()
}

// memberSamples turns the member root's capacity cells into rows for the
// member. Every cell must be one local sample of a resource and a flavor.
func memberSamples(root *v1beta1.AcceleratorQuota, cluster string, clusterUID types.UID) (memberCapacity, error) {
	report := memberCapacity{}
	for _, cell := range root.Status.Capacity {
		if len(cell.PerCluster) != 0 || cell.ResourceName == "" || cell.ResourceFlavor == "" {
			return nil, fmt.Errorf("member capacity is not a local resource/flavor sample")
		}
		pair := capacityPair{cell.ResourceName, cell.ResourceFlavor}
		if _, duplicate := report[pair]; duplicate {
			return nil, fmt.Errorf("member capacity contains duplicate resource/flavor samples")
		}
		report[pair] = v1beta1.AcceleratorClusterCapacityStatus{
			Cluster: cluster, ClusterUID: clusterUID, ReportUID: root.UID,
			ReportResourceVersion: root.ResourceVersion, ReportAvailable: true,
			Allocatable: cell.Allocatable.DeepCopy(), HighWaterMark: cell.HighWaterMark.DeepCopy(),
			ObservedAt: cell.ObservedAt.DeepCopy(), Attribution: cell.Attribution.DeepCopy(),
		}
	}
	return report, nil
}

func aggregateCapacity(reports map[string]memberCapacity, registered int) []v1beta1.AcceleratorCapacityStatus {
	byPair := map[capacityPair]*v1beta1.AcceleratorCapacityStatus{}
	for _, rows := range reports {
		for pair, row := range rows {
			cell := byPair[pair]
			if cell == nil {
				cell = &v1beta1.AcceleratorCapacityStatus{ResourceName: pair.resource, ResourceFlavor: pair.flavor}
				byPair[pair] = cell
			}
			cell.Allocatable.Add(row.Allocatable)
			cell.HighWaterMark.Add(row.HighWaterMark)
			cell.PerCluster = append(cell.PerCluster, *row.DeepCopy())
		}
	}
	var out []v1beta1.AcceleratorCapacityStatus
	for _, cell := range byPair {
		slices.SortFunc(cell.PerCluster, func(a, b v1beta1.AcceleratorClusterCapacityStatus) int {
			return cmp.Compare(a.Cluster, b.Cluster)
		})
		complete := len(cell.PerCluster) == registered
		for _, row := range cell.PerCluster {
			if !row.ReportAvailable || row.ObservedAt == nil {
				complete = false
				break
			}
			if cell.ObservedAt == nil || row.ObservedAt.Before(cell.ObservedAt) {
				cell.ObservedAt = row.ObservedAt.DeepCopy()
			}
		}
		if !complete {
			cell.ObservedAt = nil
		}
		out = append(out, *cell)
	}
	slices.SortFunc(out, compareCapacityRows)
	return out
}
