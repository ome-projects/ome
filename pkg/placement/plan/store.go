// Package plan persists allocation authority before a controller writes members.
package plan

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	// ErrStaleSnapshot requires a fresh reconcile before any member mutation.
	ErrStaleSnapshot = errors.New("placement source or plan changed")
	// ErrSchemaUnsupported means the API server did not retain allocation authority.
	ErrSchemaUnsupported = errors.New("placement plan did not survive API storage")
)

// Proposal contains the complete allocation, including retained outgoing homes.
// Callers carry original floors across partial application and retargets.
type Proposal struct {
	Mode                       v1beta1.PlacementMode
	Winner                     string
	SingleMove                 *v1beta1.PlacementSingleMoveStatus
	AdoptionDigest             string
	InputDigest                string
	PauseSurge                 bool
	OriginalUnassignedReplicas int32
	UnassignedReplicas         int32
	Assignments                map[string]v1beta1.CandidateAllocationStatus
}

// Store requires a direct Reader: an informer cannot verify persistence or fence
// a conflicting status writer while its cache is behind the API server.
type Store struct {
	Client client.Client
	Reader client.Reader
}

// SameSnapshot compares source incarnation, intent metadata, and allocation revision.
// ResourceVersion may differ because independent status writers share the object.
func SameSnapshot(expected, current *v1beta1.InferenceService) bool {
	if expected.UID != current.UID || expected.Generation != current.Generation || !current.DeletionTimestamp.IsZero() || !maps.Equal(expected.Annotations, current.Annotations) || !maps.Equal(expected.Labels, current.Labels) {
		return false
	}
	a, b := planOf(expected), planOf(current)
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID && a.Revision == b.Revision
}

// SameAllocation verifies all persisted authority, independently of changing
// serving observations. A retained plan ID cannot stand in for pruned evidence.
func SameAllocation(expected, current *v1beta1.InferenceService) bool {
	return expected != nil && current != nil && planOf(expected) != nil && planOf(current) != nil && equalAllocation(expected.Status.Placement, current.Status.Placement)
}

// Persist returns only a freshly read, verified allocation. Repeated proposals
// retain their revision; a conflict never rebases old intent onto a newer plan.
func (s Store) Persist(ctx context.Context, source *v1beta1.InferenceService, proposal Proposal) (*v1beta1.InferenceService, error) {
	if s.Client == nil || s.Reader == nil {
		return nil, fmt.Errorf("placement plan store requires a client and direct reader")
	}
	if source == nil || source.UID == "" || source.Generation <= 0 || !source.Spec.Placement.UsesClusterAffinity() || !source.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("placement plan requires an identified live source with typed intent")
	}
	if source.Labels[constants.PlacementOrigin] != "" || source.Annotations[constants.PlacementOriginUID] != "" {
		return nil, fmt.Errorf("derived services cannot own placement plans")
	}
	allocation, err := prepare(source, proposal)
	if err != nil {
		return nil, err
	}
	digest := allocation.Plan.ID
	key := client.ObjectKeyFromObject(source)
	var persisted *v1beta1.InferenceService
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &v1beta1.InferenceService{}
		if err := s.Reader.Get(ctx, key, current); err != nil {
			return err
		}
		if !SameSnapshot(source, current) {
			return ErrStaleSnapshot
		}
		previous := planOf(current)
		if previous != nil && previous.ID == fmt.Sprintf("%s.%d", digest, previous.Revision) {
			// Fresh report heartbeats do not replace the evidence of an accepted plan.
			if !equivalent(current.Status.Placement, allocation) {
				return ErrSchemaUnsupported
			}
			persisted = current
			return nil
		}
		allocation.Plan.Revision = 1
		if previous != nil {
			if previous.Revision <= 0 || previous.Revision == math.MaxInt64 || previous.SourceUID != source.UID {
				return fmt.Errorf("stored placement plan has invalid identity or exhausted revision")
			}
			allocation.Plan.Revision = previous.Revision + 1
		}
		allocation.Plan.ID = fmt.Sprintf("%s.%d", digest, allocation.Plan.Revision)
		mergeAllocation(current, allocation)
		if err := s.Client.Status().Update(ctx, current); err != nil {
			return err
		}
		// Decode into a new object: unmarshalling a response into the submitted
		// object can leave fields that an older CRD silently pruned in memory.
		persisted = &v1beta1.InferenceService{}
		if err := s.Reader.Get(ctx, key, persisted); err != nil {
			return err
		}
		authority := source.DeepCopy()
		authority.Status.Placement = allocation
		if !SameSnapshot(authority, persisted) {
			if planOf(persisted) == nil && persisted.UID == source.UID && persisted.Generation == source.Generation {
				return ErrSchemaUnsupported
			}
			return ErrStaleSnapshot
		}
		if !equalAllocation(persisted.Status.Placement, allocation) {
			return ErrSchemaUnsupported
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return persisted, nil
}

func planOf(source *v1beta1.InferenceService) *v1beta1.PlacementPlanStatus {
	if source.Status.Placement == nil {
		return nil
	}
	return source.Status.Placement.Plan
}

func mergeAllocation(source *v1beta1.InferenceService, allocation *v1beta1.PlacementStatus) {
	if source.Status.Placement == nil {
		source.Status.Placement = &v1beta1.PlacementStatus{}
	}
	old := map[string]v1beta1.CandidatePlacement{}
	for _, candidate := range source.Status.Placement.Candidates {
		old[candidate.Cluster] = candidate
	}
	candidates := make([]v1beta1.CandidatePlacement, 0, len(allocation.Candidates))
	for _, desired := range allocation.Candidates {
		candidate := old[desired.Cluster]
		if candidate.Allocation == nil || candidate.Allocation.ClusterUID != desired.Allocation.ClusterUID {
			if candidate.Allocation == nil && allocation.Plan.Mode == v1beta1.PlacementModeAll {
				candidate.Cluster = desired.Cluster
				candidate.ObservationKnown, candidate.AppliedPlanID = false, ""
			} else {
				candidate = v1beta1.CandidatePlacement{Cluster: desired.Cluster}
			}
		}
		candidate.Allocation = desired.Allocation.DeepCopy()
		candidates = append(candidates, candidate)
	}
	source.Status.Placement.Plan = allocation.Plan.DeepCopy()
	source.Status.Placement.Candidates = candidates
	if allocation.Plan.Mode == v1beta1.PlacementModeSingle {
		source.Status.Placement.Cluster = allocation.Plan.Winner
	}
}

func equalAllocation(a, b *v1beta1.PlacementStatus) bool {
	if a == nil || b == nil || !equality.Semantic.DeepEqual(a.Plan, b.Plan) || len(a.Candidates) != len(b.Candidates) {
		return false
	}
	byName := map[string]*v1beta1.CandidateAllocationStatus{}
	for _, candidate := range a.Candidates {
		if _, duplicate := byName[candidate.Cluster]; duplicate {
			return false
		}
		byName[candidate.Cluster] = candidate.Allocation
	}
	for _, candidate := range b.Candidates {
		allocation, exists := byName[candidate.Cluster]
		if !exists || !equality.Semantic.DeepEqual(allocation, candidate.Allocation) {
			return false
		}
		delete(byName, candidate.Cluster)
	}
	return true
}
