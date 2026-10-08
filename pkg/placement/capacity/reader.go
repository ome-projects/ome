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
	if r.Client == nil || r.RootName == "" || r.MaxAge <= 0 || now.IsZero() {
		return nil, fmt.Errorf("capacity reader requires a client, root name, positive maximum age, and current time")
	}
	out := make(map[string]Sample, len(matched))
	if len(matched) == 0 {
		return out, nil
	}
	var root v1beta1.AcceleratorQuota
	if err := r.Client.Get(ctx, client.ObjectKey{Name: r.RootName}, &root); err != nil {
		return nil, fmt.Errorf("read fleet capacity: %w", err)
	}
	if root.UID == "" || !root.DeletionTimestamp.IsZero() || root.Spec.ParentRef != nil || root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort {
		return nil, fmt.Errorf("fleet capacity root is not an identified live root")
	}
	for _, cluster := range matched {
		if cluster.Name == "" || cluster.UID == "" || !cluster.DeletionTimestamp.IsZero() {
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

type pair struct {
	resource string
	flavor   string
}

func normalize(cluster v1beta1.WorkloadCluster, demand Demand, reports []v1beta1.AcceleratorCapacityStatus, now time.Time, maxAge time.Duration) (Sample, error) {
	if demand.Fingerprint == "" || len(demand.Pools) == 0 || demand.PrimaryUnits < 0 {
		return Sample{}, fmt.Errorf("resolved accelerator demand and fingerprint are required")
	}
	units := max(demand.PrimaryUnits, 1)
	pools := map[pair]Pool{}
	for _, pool := range demand.Pools {
		if pool.ResourceName == "" || pool.ResourceFlavor == "" || pool.FlavorUID == "" || pool.FlavorSetHash == "" {
			return Sample{}, fmt.Errorf("resource, flavor identity, and mapping fingerprint are required")
		}
		quantity, exact := wholeUnits(pool.Quantity)
		if !exact || quantity <= 0 {
			return Sample{}, fmt.Errorf("demand for %s/%s must be positive whole units within int64", pool.ResourceName, pool.ResourceFlavor)
		}
		key := pair{pool.ResourceName, pool.ResourceFlavor}
		if combined, exists := pools[key]; exists {
			if combined.FlavorUID != pool.FlavorUID || combined.FlavorSetHash != pool.FlavorSetHash || !maps.Equal(combined.NodeLabels, pool.NodeLabels) {
				return Sample{}, fmt.Errorf("components disagree on mapping for %s/%s", key.resource, key.flavor)
			}
			combined.Quantity.Add(pool.Quantity)
			pools[key] = combined
		} else {
			pool.Quantity = pool.Quantity.DeepCopy()
			pools[key] = pool
		}
	}
	keys := slices.Collect(maps.Keys(pools))
	slices.SortFunc(keys, func(a, b pair) int {
		if a.resource != b.resource {
			return cmp.Compare(a.resource, b.resource)
		}
		return cmp.Compare(a.flavor, b.flavor)
	})
	sample := Sample{ClusterUID: cluster.UID, DemandFingerprint: demand.Fingerprint, Weight: math.MaxInt64}
	for _, key := range keys {
		pool := pools[key]
		required, exact := wholeUnits(pool.Quantity)
		if !exact || required <= 0 {
			return Sample{}, fmt.Errorf("combined demand for %s/%s exceeds whole int64 units", key.resource, key.flavor)
		}
		var row *v1beta1.AcceleratorClusterCapacityStatus
		for _, report := range reports {
			if report.ResourceName != key.resource || report.ResourceFlavor != key.flavor {
				continue
			}
			for _, candidate := range report.PerCluster {
				if candidate.Cluster != cluster.Name {
					continue
				}
				if row != nil {
					return Sample{}, fmt.Errorf("duplicate report for %s/%s", key.resource, key.flavor)
				}
				row = candidate.DeepCopy()
			}
		}
		if row == nil || row.ClusterUID != cluster.UID || !row.ReportAvailable || row.ReportUID == "" || row.ReportResourceVersion == "" {
			return Sample{}, fmt.Errorf("identified available report is missing for %s/%s", key.resource, key.flavor)
		}
		if row.ObservedAt == nil || row.ObservedAt.After(now) || now.Sub(row.ObservedAt.Time) >= maxAge {
			return Sample{}, fmt.Errorf("report for %s/%s is missing a fresh observation time", key.resource, key.flavor)
		}
		mapping := row.Attribution
		if mapping == nil || !mapping.Complete || mapping.FlavorUID != pool.FlavorUID || mapping.FlavorSetHash != pool.FlavorSetHash || !maps.Equal(mapping.NodeLabels, pool.NodeLabels) {
			return Sample{}, fmt.Errorf("report mapping is unverified or incompatible for %s/%s", key.resource, key.flavor)
		}
		available, exact := wholeUnits(row.Allocatable)
		if !exact || available < 0 || available > math.MaxInt64/units {
			return Sample{}, fmt.Errorf("allocatable for %s/%s must be nonnegative whole units within int64", key.resource, key.flavor)
		}
		available *= units
		if len(sample.Pools) > 0 && (sample.Pools[0].ReportUID != row.ReportUID || sample.Pools[0].ReportResourceVersion != row.ReportResourceVersion || sample.Pools[0].Attribution.FlavorSetHash != mapping.FlavorSetHash) {
			return Sample{}, fmt.Errorf("required pools belong to different source reports or attribution mappings")
		}
		sample.Weight = min(sample.Weight, available/required)
		sample.Pools = append(sample.Pools, Evidence{
			ResourceName: key.resource, ResourceFlavor: key.flavor, Demand: required, Allocatable: available,
			ObservedAt: *row.ObservedAt, ReportUID: row.ReportUID, ReportResourceVersion: row.ReportResourceVersion,
			Attribution: *mapping.DeepCopy(),
		})
	}
	return sample, nil
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
