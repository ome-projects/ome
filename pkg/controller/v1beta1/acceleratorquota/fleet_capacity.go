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
	var root v1beta1.AcceleratorQuota
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: r.Options.RootName}, &root); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if root.UID != rootUID || !root.DeletionTimestamp.IsZero() {
		return nil
	}
	registered, err := r.capacityRegistry(ctx)
	if err != nil {
		return err
	}

	reports := make(map[string]memberCapacity, len(registered))
	for _, cell := range root.Status.Capacity {
		for _, row := range cell.PerCluster {
			if row.ClusterUID == "" || row.ClusterUID != registered[row.Cluster] {
				continue
			}
			if reports[row.Cluster] == nil {
				reports[row.Cluster] = memberCapacity{}
			}
			retained := row.DeepCopy()
			retained.ReportAvailable = false
			reports[row.Cluster][capacityPair{cell.ResourceName, cell.ResourceFlavor}] = *retained
		}
	}
	for cluster, uid := range registered {
		remote, ok := transport.ClientForUID(cluster, uid)
		if !ok {
			continue
		}
		report, err := r.readMemberCapacity(ctx, remote, rootUID, cluster, uid)
		if err != nil {
			r.Log.V(1).Info("member capacity report unavailable", "cluster", cluster, "reason", err.Error())
			continue
		}
		reports[cluster] = report
	}

	// The registry may change during remote reads. A replacement cannot
	// inherit a report fetched through the departed registration's client.
	current, err := r.capacityRegistry(ctx)
	if err != nil {
		return err
	}
	for cluster, rows := range reports {
		if current[cluster] == "" || current[cluster] != registered[cluster] {
			delete(reports, cluster)
			continue
		}
		if _, ok := transport.ClientForUID(cluster, current[cluster]); !ok {
			for key, row := range rows {
				row.ReportAvailable = false
				rows[key] = row
			}
		}
	}
	updated := root.DeepCopy()
	updated.Status.Capacity = aggregateCapacity(reports, len(current))
	if apiequality.Semantic.DeepEqual(root.Status.Capacity, updated.Status.Capacity) {
		return nil
	}
	// The root version fences concurrent collectors and object recreation.
	return r.Status().Patch(ctx, updated, client.MergeFromWithOptions(&root, client.MergeFromWithOptimisticLock{}))
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

func (r *Reconciler) readMemberCapacity(ctx context.Context, remote workloadcluster.SelectivelyCachingClient,
	localUID types.UID, cluster string, clusterUID types.UID,
) (memberCapacity, error) {
	key := client.ObjectKey{Name: r.Options.RootName}
	var root v1beta1.AcceleratorQuota
	if err := remote.Get(ctx, key, &root); err != nil {
		return nil, err
	}
	_, projected := root.Labels[v1beta1.AcceleratorQuotaOriginLabel]
	if root.UID == "" || root.UID == localUID || root.ResourceVersion == "" || !root.DeletionTimestamp.IsZero() ||
		root.Spec.ParentRef != nil || root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort || projected {
		return nil, fmt.Errorf("member root is not an identified local capacity report")
	}
	// Metadata reads bypass the transport cache. A cached report is usable
	// only while the member still holds that exact source object version.
	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(acceleratorQuotaGVK)
	if err := remote.Get(ctx, key, metadata); err != nil {
		return nil, err
	}
	if metadata.UID != root.UID || metadata.ResourceVersion != root.ResourceVersion || !metadata.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("member capacity report changed during collection")
	}
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
	slices.SortFunc(out, func(a, b v1beta1.AcceleratorCapacityStatus) int {
		if order := cmp.Compare(a.ResourceName, b.ResourceName); order != 0 {
			return order
		}
		return cmp.Compare(a.ResourceFlavor, b.ResourceFlavor)
	})
	return out
}
