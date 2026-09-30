package placement

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/placement/resolution"
)

// CapacityConfig locates the shared-name quota roots and member configuration.
// Every duration is explicit; missing configuration holds capacity placement.
type CapacityConfig struct {
	RootName        string
	MaxAge          time.Duration
	StabilityWindow time.Duration
	RefreshInterval time.Duration
}

func (r *Reconciler) capacityNow() time.Time {
	if r.CapacityClock != nil {
		return r.CapacityClock.Now()
	}
	return time.Now()
}

func (c *CapacityConfig) Validate() error {
	if c == nil || strings.TrimSpace(c.RootName) == "" || c.MaxAge <= 0 || c.StabilityWindow <= 0 || c.RefreshInterval <= 0 {
		return fmt.Errorf("capacity placement requires root name and positive freshness, stability, and refresh durations")
	}
	if c.RefreshInterval >= c.MaxAge {
		return fmt.Errorf("capacity refresh interval must be shorter than maximum report age")
	}
	return nil
}

func (r *Reconciler) readSplitCapacity(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, now time.Time) (map[string]capacity.Sample, error) {
	if err := r.Capacity.Validate(); err != nil {
		return nil, err
	}
	selector, err := placementSelector(source)
	if err != nil || selector == nil {
		return nil, fmt.Errorf("capacity placement requires valid cluster affinity: %v", err)
	}
	desired, err := r.derivedFor(source)
	if err != nil {
		return nil, err
	}
	matched := []v1beta1.WorkloadCluster{}
	demands := map[string]capacity.Demand{}
	resolved := map[string]*resolution.ResolvedDemand{}
	for _, cluster := range clusters {
		if _, matches := selector.Match(&cluster); !matches || !cluster.DeletionTimestamp.IsZero() {
			continue
		}
		if _, duplicate := resolved[cluster.Name]; duplicate {
			return nil, fmt.Errorf("duplicate capacity registration %q", cluster.Name)
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		demand, err := r.resolveCapacityMember(cctx, source, desired, cluster.Name, cluster.UID)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("cluster %q demand: %w", cluster.Name, err)
		}
		matched = append(matched, cluster)
		resolved[cluster.Name], demands[cluster.Name] = demand, demand.Demand
	}
	reader := capacity.Reader{Client: r.APIReader, RootName: r.Capacity.RootName, MaxAge: r.Capacity.MaxAge}
	samples, err := reader.Read(ctx, matched, demands, now)
	if err != nil {
		return nil, err
	}
	for _, cluster := range matched {
		demand, sample := resolved[cluster.Name], samples[cluster.Name]
		if err := matchCapacityReport(sample, demand); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", cluster.Name, err)
		}
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := demand.Check(cctx)
		if err == nil {
			_, err = r.plannedClient(cctx, cluster.Name, cluster.UID)
		}
		cancel()
		if err != nil {
			return nil, fmt.Errorf("cluster %q capacity inputs changed: %w", cluster.Name, err)
		}
		sample.DemandContract = demand.Contract.DeepCopy()
		samples[cluster.Name] = sample
	}
	return samples, nil
}

func matchCapacityReport(sample capacity.Sample, demand *resolution.ResolvedDemand) error {
	if sample.DemandFingerprint != demand.Contract.Fingerprint || len(sample.Pools) == 0 {
		return fmt.Errorf("hardware sample and member demand do not share a fingerprint")
	}
	for _, pool := range sample.Pools {
		if pool.ReportUID != demand.Report.UID || pool.ReportResourceVersion != demand.Report.ResourceVersion {
			return fmt.Errorf("fleet hardware and member demand refer to different source reports")
		}
	}
	return nil
}

func (r *Reconciler) resolveCapacityMember(ctx context.Context, source, desired *v1beta1.InferenceService, name string, uid types.UID) (*resolution.ResolvedDemand, error) {
	cl, err := r.plannedClient(ctx, name, uid)
	if err != nil {
		return nil, err
	}
	return r.resolveCapacityOn(ctx, cl, source, desired)
}

func (r *Reconciler) resolveCapacityOn(ctx context.Context, cl client.Client, source, desired *v1beta1.InferenceService) (*resolution.ResolvedDemand, error) {
	if err := r.Capacity.Validate(); err != nil {
		return nil, err
	}
	standing := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(desired), standing); apierrors.IsNotFound(err) {
		standing = nil
	} else if err != nil {
		return nil, err
	} else if !isOurDerived(standing, source) || standing.UID == "" || !standing.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("capacity member is not an identified live derived service")
	}
	resolver := resolution.Resolver{Client: cl, OperatorNamespace: r.MemberOperatorNamespace}
	demand, err := resolver.ResolveDemand(ctx, desired, standing, r.Capacity.RootName)
	if err != nil {
		return nil, err
	}
	if err := protocol.ValidateNativeModes(demand.Unit.Modes); err != nil {
		return nil, err
	}
	if err := protocol.ValidateDemandComponents(desired, demand.Contract); err != nil {
		return nil, err
	}
	return demand, nil
}

// checkCapacityApplication resolves against the exact transport used to write.
// A connection replacement or dependency edit cannot apply an older rendering.
func (r *Reconciler) checkCapacityApplication(ctx context.Context, cl client.Client, source, desired *v1beta1.InferenceService, assignment *v1beta1.CandidateAllocationStatus) error {
	if assignment.Capacity == nil {
		return nil
	}
	demand, err := r.resolveCapacityOn(ctx, cl, source, desired)
	if err != nil {
		return err
	}
	contract := demand.Contract.DeepCopy()
	slices.SortFunc(contract.Components, func(a, b v1beta1.PlacementComponentDemand) int { return cmp.Compare(a.Component, b.Component) })
	if !equality.Semantic.DeepEqual(assignment.Capacity.DemandContract, contract) {
		return fmt.Errorf("member rendering differs from accepted capacity demand")
	}
	for _, pool := range assignment.Capacity.Pools {
		if pool.ReportUID != demand.Report.UID {
			return fmt.Errorf("accepted member capacity report changed before application")
		}
		matched := false
		for _, hardware := range demand.Hardware {
			if hardware.ResourceName != pool.ResourceName || hardware.ResourceFlavor != pool.ResourceFlavor {
				continue
			}
			attribution := hardware.Attribution.DeepCopy()
			if attribution != nil && len(attribution.NodeLabels) == 0 {
				attribution.NodeLabels = nil
			}
			if matched || hardware.Allocatable.Cmp(*resource.NewQuantity(pool.Allocatable, resource.DecimalSI)) != 0 || !equality.Semantic.DeepEqual(attribution, &pool.Attribution) {
				return fmt.Errorf("accepted member hardware changed before application")
			}
			now := r.capacityNow()
			if hardware.ObservedAt == nil || hardware.ObservedAt.After(now) || now.Sub(hardware.ObservedAt.Time) >= r.Capacity.MaxAge {
				return fmt.Errorf("member hardware report is stale before application")
			}
			matched = true
		}
		if !matched {
			return fmt.Errorf("accepted member hardware pool is missing before application")
		}
	}
	return nil
}
