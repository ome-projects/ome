// Package capacity normalizes identified hardware observations into nominal
// whole replica units without subtracting usage or quota.
package capacity

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Pool is one component's resolved accelerator demand. Entries sharing a
// resource/flavor pair consume the same pool and are combined before division.
type Pool struct {
	ResourceName   string
	ResourceFlavor string
	Quantity       resource.Quantity
	FlavorUID      types.UID
	NodeLabels     map[string]string
	FlavorSetHash  string
}

// Demand identifies the resolved runtime and placement-unit shape on a member.
// Its fingerprint must change when the runtime or resource mapping changes.
// PrimaryUnits is the number of primary-component replicas one unit of this
// demand contains; zero means one.
type Demand struct {
	Fingerprint  string
	Pools        []Pool
	PrimaryUnits int64
}

// Evidence records the hardware and demand used for one resource/flavor ratio.
type Evidence struct {
	ResourceName          string
	ResourceFlavor        string
	Demand                int64
	Allocatable           int64
	ObservedAt            metav1.Time
	ReportUID             types.UID
	ReportResourceVersion string
	Attribution           v1beta1.AcceleratorCapacityAttribution
}

// Sample is one member's nominal whole-replica capacity and its evidence.
// Evidence Allocatable is scaled by the demand's primary multiplicity so each
// pool's Allocatable/Demand quotient counts primary replicas.
type Sample struct {
	ClusterUID        types.UID
	DemandFingerprint string
	// DemandContract is supplied by the rendering resolver before this sample
	// can authorize member workloads. The hardware reader does not render pods.
	DemandContract *v1beta1.PlacementDemandContract
	Weight         int64
	Pools          []Evidence
}

// Reader adapts the management root's per-cluster quota reports. MaxAge and the
// root name must be configured; absent configuration cannot establish freshness.
type Reader struct {
	// Client must read the management root without an informer cache.
	Client   client.Reader
	RootName string
	MaxAge   time.Duration
}

// Read requires usable inputs for every matched registration. Any missing,
// stale, or incompatible input rejects the whole reading, including partial
// results. The caller retains its last accepted placement plan on error.
func (r Reader) Read(ctx context.Context, matched []v1beta1.WorkloadCluster, demands map[string]Demand, now time.Time) (map[string]Sample, error) {
	if !r.configured(now) {
		return nil, fmt.Errorf("capacity reader requires a client, root name, positive maximum age, and current time")
	}
	out := make(map[string]Sample, len(matched))
	if len(matched) == 0 {
		return out, nil
	}
	root, err := r.liveRoot(ctx)
	if err != nil {
		return nil, err
	}
	for _, cluster := range matched {
		if !identifiedLiveRegistration(cluster) {
			return nil, fmt.Errorf("capacity requires an identified live registration for cluster %q", cluster.Name)
		}
		if _, duplicate := out[cluster.Name]; duplicate {
			return nil, fmt.Errorf("duplicate matched registration %q", cluster.Name)
		}
		sample, err := normalize(cluster, demands[cluster.Name], root.Status.Capacity, now, r.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("cluster %q capacity unknown: %w", cluster.Name, err)
		}
		out[cluster.Name] = sample
	}
	return out, nil
}

// configured reports whether the reader has what a reading needs: a client,
// the root's name, a positive maximum age and the current time.
func (r Reader) configured(now time.Time) bool {
	return r.Client != nil && r.RootName != "" && r.MaxAge > 0 && !now.IsZero()
}

// liveRoot reads the fleet capacity root once per reading and requires it to
// be an identified, live cohort root with no parent.
func (r Reader) liveRoot(ctx context.Context) (*v1beta1.AcceleratorQuota, error) {
	root := &v1beta1.AcceleratorQuota{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: r.RootName}, root); err != nil {
		return nil, fmt.Errorf("read fleet capacity: %w", err)
	}
	if root.UID == "" || !root.DeletionTimestamp.IsZero() || root.Spec.ParentRef != nil || root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort {
		return nil, fmt.Errorf("fleet capacity root is not an identified live root")
	}
	return root, nil
}

// identifiedLiveRegistration reports whether a matched registration is named,
// identified by a UID and not being deleted.
func identifiedLiveRegistration(cluster v1beta1.WorkloadCluster) bool {
	return cluster.Name != "" && cluster.UID != "" && cluster.DeletionTimestamp.IsZero()
}

// pair keys pools and report rows by accelerator resource and hardware flavor.
type pair struct {
	resource string
	flavor   string
}

// compare orders pairs by resource name, then flavor name.
func (p pair) compare(q pair) int {
	if order := cmp.Compare(p.resource, q.resource); order != 0 {
		return order
	}
	return cmp.Compare(p.flavor, q.flavor)
}

// normalize divides one member's reported hardware by its combined demand,
// pool by pool, and keeps the smallest whole quotient as the member's weight.
func normalize(cluster v1beta1.WorkloadCluster, demand Demand, reports []v1beta1.AcceleratorCapacityStatus, now time.Time, maxAge time.Duration) (Sample, error) {
	if demand.Fingerprint == "" || len(demand.Pools) == 0 || demand.PrimaryUnits < 0 {
		return Sample{}, fmt.Errorf("resolved accelerator demand and fingerprint are required")
	}
	units := max(demand.PrimaryUnits, 1)
	pools, err := combinePools(demand.Pools)
	if err != nil {
		return Sample{}, err
	}
	reading := clusterReading{clusterUID: cluster.UID, rows: clusterRows(reports, cluster.Name), now: now, maxAge: maxAge}
	sample := Sample{ClusterUID: cluster.UID, DemandFingerprint: demand.Fingerprint, Weight: math.MaxInt64}
	var first *v1beta1.AcceleratorClusterCapacityStatus
	for _, key := range sortedKeys(pools) {
		pool := pools[key]
		required, exact := wholeUnits(pool.Quantity)
		if !exact || required <= 0 {
			return Sample{}, fmt.Errorf("combined demand for %s/%s exceeds whole int64 units", key.resource, key.flavor)
		}
		row, err := reading.verifiedRow(key, pool)
		if err != nil {
			return Sample{}, err
		}
		available, exact := wholeUnits(row.Allocatable)
		if !exact || available < 0 || available > math.MaxInt64/units {
			return Sample{}, fmt.Errorf("allocatable for %s/%s must be nonnegative whole units within int64", key.resource, key.flavor)
		}
		available *= units
		if first != nil && !sameReport(first, row) {
			return Sample{}, fmt.Errorf("required pools belong to different source reports or attribution mappings")
		}
		if first == nil {
			first = row
		}
		sample.Weight = min(sample.Weight, available/required)
		sample.Pools = append(sample.Pools, Evidence{
			ResourceName: key.resource, ResourceFlavor: key.flavor, Demand: required, Allocatable: available,
			ObservedAt: *row.ObservedAt, ReportUID: row.ReportUID, ReportResourceVersion: row.ReportResourceVersion,
			Attribution: *row.Attribution.DeepCopy(),
		})
	}
	return sample, nil
}

// sameReport reports whether two rows come from the same report, at the same
// version and under the same attribution mapping.
func sameReport(a, b *v1beta1.AcceleratorClusterCapacityStatus) bool {
	return a.ReportUID == b.ReportUID && a.ReportResourceVersion == b.ReportResourceVersion &&
		a.Attribution.FlavorSetHash == b.Attribution.FlavorSetHash
}

// combinePools sums the demand of pools that share a resource and flavor.
// Pools of one pair must agree on the flavor's identity and mapping.
func combinePools(pools []Pool) (map[pair]Pool, error) {
	out := map[pair]Pool{}
	for _, pool := range pools {
		if pool.ResourceName == "" || pool.ResourceFlavor == "" || pool.FlavorUID == "" || pool.FlavorSetHash == "" {
			return nil, fmt.Errorf("resource, flavor identity, and mapping fingerprint are required")
		}
		quantity, exact := wholeUnits(pool.Quantity)
		if !exact || quantity <= 0 {
			return nil, fmt.Errorf("demand for %s/%s must be positive whole units within int64", pool.ResourceName, pool.ResourceFlavor)
		}
		key := pair{pool.ResourceName, pool.ResourceFlavor}
		combined, exists := out[key]
		if !exists {
			pool.Quantity = pool.Quantity.DeepCopy()
			out[key] = pool
			continue
		}
		if combined.FlavorUID != pool.FlavorUID || combined.FlavorSetHash != pool.FlavorSetHash || !maps.Equal(combined.NodeLabels, pool.NodeLabels) {
			return nil, fmt.Errorf("components disagree on mapping for %s/%s", key.resource, key.flavor)
		}
		combined.Quantity.Add(pool.Quantity)
		out[key] = combined
	}
	return out, nil
}

// sortedKeys lists the pool keys by resource name, then flavor name.
func sortedKeys(pools map[pair]Pool) []pair {
	keys := slices.Collect(maps.Keys(pools))
	slices.SortFunc(keys, pair.compare)
	return keys
}

// clusterReading is one member's rows of the root's capacity reports and the
// freshness rule they are read under.
type clusterReading struct {
	clusterUID types.UID
	rows       map[pair][]*v1beta1.AcceleratorClusterCapacityStatus
	now        time.Time
	maxAge     time.Duration
}

// clusterRows indexes one cluster's rows of the capacity reports by resource
// and flavor; a pair with more than one row is a duplicate report.
func clusterRows(reports []v1beta1.AcceleratorCapacityStatus, cluster string) map[pair][]*v1beta1.AcceleratorClusterCapacityStatus {
	rows := map[pair][]*v1beta1.AcceleratorClusterCapacityStatus{}
	for _, report := range reports {
		key := pair{report.ResourceName, report.ResourceFlavor}
		for _, candidate := range report.PerCluster {
			if candidate.Cluster == cluster {
				rows[key] = append(rows[key], candidate.DeepCopy())
			}
		}
	}
	return rows
}

// verifiedRow is the cluster's one report row for a pool, when that row is
// identified, available, fresh, and attributed to the pool's flavor mapping.
func (r clusterReading) verifiedRow(key pair, pool Pool) (*v1beta1.AcceleratorClusterCapacityStatus, error) {
	rows := r.rows[key]
	if len(rows) > 1 {
		return nil, fmt.Errorf("duplicate report for %s/%s", key.resource, key.flavor)
	}
	var row *v1beta1.AcceleratorClusterCapacityStatus
	if len(rows) == 1 {
		row = rows[0]
	}
	if row == nil || row.ClusterUID != r.clusterUID || !row.ReportAvailable || row.ReportUID == "" || row.ReportResourceVersion == "" {
		return nil, fmt.Errorf("identified available report is missing for %s/%s", key.resource, key.flavor)
	}
	if row.ObservedAt == nil || row.ObservedAt.After(r.now) || r.now.Sub(row.ObservedAt.Time) >= r.maxAge {
		return nil, fmt.Errorf("report for %s/%s is missing a fresh observation time", key.resource, key.flavor)
	}
	mapping := row.Attribution
	if mapping == nil || !mapping.Complete || mapping.FlavorUID != pool.FlavorUID || mapping.FlavorSetHash != pool.FlavorSetHash || !maps.Equal(mapping.NodeLabels, pool.NodeLabels) {
		return nil, fmt.Errorf("report mapping is unverified or incompatible for %s/%s", key.resource, key.flavor)
	}
	return row, nil
}

// wholeUnits accepts every exact Quantity representation, including decimal
// storage, while rejecting fractional counts and arithmetic overflow.
func wholeUnits(q resource.Quantity) (int64, bool) {
	if q.Sign() < 0 || q.Cmp(*resource.NewQuantity(math.MaxInt64, resource.DecimalSI)) > 0 {
		return 0, false
	}
	value := q.Value()
	return value, q.Cmp(*resource.NewQuantity(value, resource.DecimalSI)) == 0
}
