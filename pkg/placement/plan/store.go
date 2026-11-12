// Package plan persists allocation authority before a controller writes members.
package plan

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"

	"k8s.io/apimachinery/pkg/api/equality"
	k8stypes "k8s.io/apimachinery/pkg/types"
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

// Proposal is the complete allocation the controller asks the store to keep,
// the clusters a move is retiring included. Callers carry the original
// replica counts across partial application and retargets.
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

// SameSnapshot reports whether current is still the object expected was read
// from, with the same plan identity: the same UID and generation, not being
// deleted, the same labels and annotations, and the same plan ID and
// revision. ResourceVersion may differ because independent status writers
// share the object.
func SameSnapshot(expected, current *v1beta1.InferenceService) bool {
	return sameSource(expected, current) && samePlanIdentity(planOf(expected), planOf(current))
}

// sameSource reports whether current is still the object expected was read
// from: the same UID and generation, not being deleted, and the same labels
// and annotations.
func sameSource(expected, current *v1beta1.InferenceService) bool {
	return expected.UID == current.UID &&
		expected.Generation == current.Generation &&
		current.DeletionTimestamp.IsZero() &&
		maps.Equal(expected.Annotations, current.Annotations) &&
		maps.Equal(expected.Labels, current.Labels)
}

// samePlanIdentity reports two plans with the same ID and revision, or no
// plan on either side.
func samePlanIdentity(a, b *v1beta1.PlacementPlanStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID && a.Revision == b.Revision
}

// SameAllocation reports whether current holds the plan and every
// allocation expected holds, whatever the serving observations say. A
// retained plan ID alone does not prove the allocations survived storage.
func SameAllocation(expected, current *v1beta1.InferenceService) bool {
	return expected != nil && current != nil && planOf(expected) != nil && planOf(current) != nil && equalAllocation(expected.Status.Placement, current.Status.Placement)
}

// Persist stores proposal as the source's plan and returns the plan read back
// from the API server. A proposal equal to the plan already accepted keeps
// that plan and its revision; any other takes the next revision. A source or
// plan that moved under the write is ErrStaleSnapshot; a stored plan that
// reads back different from what was written is ErrSchemaUnsupported.
func (s Store) Persist(ctx context.Context, source *v1beta1.InferenceService, proposal Proposal) (*v1beta1.InferenceService, error) {
	if s.Client == nil || s.Reader == nil {
		return nil, fmt.Errorf("placement plan store requires a client and direct reader")
	}
	if err := planOwner(source); err != nil {
		return nil, err
	}
	allocation, err := prepare(source, proposal)
	if err != nil {
		return nil, err
	}
	// The digest names the proposal; the stored ID appends the revision.
	digest := allocation.Plan.ID
	var persisted *v1beta1.InferenceService
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var attemptErr error
		persisted, attemptErr = s.persistOnce(ctx, source, allocation, digest)
		return attemptErr
	})
	if err != nil {
		return nil, err
	}
	return persisted, nil
}

// planOwner rejects a source that cannot hold a placement plan: one without
// a UID or a generation, without cluster-affinity intent, being deleted, or
// derived from another service.
func planOwner(source *v1beta1.InferenceService) error {
	if source == nil || source.UID == "" || source.Generation <= 0 || !source.Spec.Placement.UsesClusterAffinity() || !source.DeletionTimestamp.IsZero() {
		return fmt.Errorf("placement plan requires an identified live source with typed intent")
	}
	if derived(source) {
		return fmt.Errorf("derived services cannot own placement plans")
	}
	return nil
}

// derived reports a service that placement created from another service; it
// carries the origin label or the origin UID annotation.
func derived(source *v1beta1.InferenceService) bool {
	return source.Labels[constants.PlacementOrigin] != "" || source.Annotations[constants.PlacementOriginUID] != ""
}

// persistOnce is one attempt of Persist: read the source fresh, keep the
// plan already accepted or write allocation as the next revision, then read
// the write back. A conflict on the status write makes Persist try again.
func (s Store) persistOnce(ctx context.Context, source *v1beta1.InferenceService, allocation *v1beta1.PlacementStatus, digest string) (*v1beta1.InferenceService, error) {
	key := client.ObjectKeyFromObject(source)
	current := &v1beta1.InferenceService{}
	if err := s.Reader.Get(ctx, key, current); err != nil {
		return nil, err
	}
	if !SameSnapshot(source, current) {
		return nil, ErrStaleSnapshot
	}
	previous := planOf(current)
	if accepted(previous, digest) {
		// A proposal that only refreshed its report's liveness fields keeps
		// the accepted plan, as long as storage kept that plan whole.
		if !equivalent(current.Status.Placement, allocation) {
			return nil, ErrSchemaUnsupported
		}
		return current, nil
	}
	revision, err := nextRevision(previous, source.UID)
	if err != nil {
		return nil, err
	}
	allocation.Plan.Revision = revision
	allocation.Plan.ID = planID(digest, revision)
	mergeAllocation(current, allocation)
	if err := s.Client.Status().Update(ctx, current); err != nil {
		return nil, err
	}
	return s.readBack(ctx, key, source, allocation)
}

// planID is the stored plan identity: the proposal digest and the revision.
func planID(digest string, revision int64) string {
	return fmt.Sprintf("%s.%d", digest, revision)
}

// accepted reports whether previous is the stored form of the proposal that
// digest names.
func accepted(previous *v1beta1.PlacementPlanStatus, digest string) bool {
	return previous != nil && previous.ID == planID(digest, previous.Revision)
}

// nextRevision is the revision a new plan takes: 1 with no plan stored, else
// one past the stored plan's. A stored plan without a valid revision, with
// its revision exhausted, or from another source is refused.
func nextRevision(previous *v1beta1.PlacementPlanStatus, sourceUID k8stypes.UID) (int64, error) {
	if previous == nil {
		return 1, nil
	}
	if previous.Revision <= 0 || previous.Revision == math.MaxInt64 || previous.SourceUID != sourceUID {
		return 0, fmt.Errorf("stored placement plan has invalid identity or exhausted revision")
	}
	return previous.Revision + 1, nil
}

// readBack reads the written plan into a new object and checks that the API
// server kept it whole: the same source, the plan identity just written and
// every allocation. Decoding into the submitted object would keep in memory
// the fields an older CRD silently pruned.
func (s Store) readBack(ctx context.Context, key client.ObjectKey, source *v1beta1.InferenceService, allocation *v1beta1.PlacementStatus) (*v1beta1.InferenceService, error) {
	persisted := &v1beta1.InferenceService{}
	if err := s.Reader.Get(ctx, key, persisted); err != nil {
		return nil, err
	}
	written := source.DeepCopy()
	written.Status.Placement = allocation
	if !SameSnapshot(written, persisted) {
		if planDropped(source, persisted) {
			return nil, ErrSchemaUnsupported
		}
		return nil, ErrStaleSnapshot
	}
	if !equalAllocation(persisted.Status.Placement, allocation) {
		return nil, ErrSchemaUnsupported
	}
	return persisted, nil
}

// planDropped reports a read-back of the same source object with no plan on
// it: the API server did not store the plan field.
func planDropped(source, persisted *v1beta1.InferenceService) bool {
	return planOf(persisted) == nil && persisted.UID == source.UID && persisted.Generation == source.Generation
}

// planOf is the stored plan of a source, nil when it has none.
func planOf(source *v1beta1.InferenceService) *v1beta1.PlacementPlanStatus {
	if source.Status.Placement == nil {
		return nil
	}
	return source.Status.Placement.Plan
}

// mergeAllocation writes allocation into the source's placement status: the
// plan, the candidates in allocation's order with each cluster's stored
// candidate carried over, and under Single the chosen cluster.
func mergeAllocation(source *v1beta1.InferenceService, allocation *v1beta1.PlacementStatus) {
	if source.Status.Placement == nil {
		source.Status.Placement = &v1beta1.PlacementStatus{}
	}
	stored := map[string]v1beta1.CandidatePlacement{}
	for _, candidate := range source.Status.Placement.Candidates {
		stored[candidate.Cluster] = candidate
	}
	candidates := make([]v1beta1.CandidatePlacement, 0, len(allocation.Candidates))
	for _, desired := range allocation.Candidates {
		candidate := carriedCandidate(stored[desired.Cluster], desired, allocation.Plan.Mode)
		candidate.Allocation = desired.Allocation.DeepCopy()
		candidates = append(candidates, candidate)
	}
	source.Status.Placement.Plan = allocation.Plan.DeepCopy()
	source.Status.Placement.Candidates = candidates
	if allocation.Plan.Mode == v1beta1.PlacementModeSingle {
		source.Status.Placement.Cluster = allocation.Plan.Winner
	}
}

// carriedCandidate is what a cluster's stored candidate keeps under a new
// allocation. A candidate whose allocation names the same cluster UID keeps
// everything. One with no allocation keeps its other fields under All, with
// its observation and applied-plan record cleared. Any other candidate starts
// from the cluster name alone.
func carriedCandidate(stored, desired v1beta1.CandidatePlacement, mode v1beta1.PlacementMode) v1beta1.CandidatePlacement {
	switch {
	case stored.Allocation != nil && stored.Allocation.ClusterUID == desired.Allocation.ClusterUID:
		return stored
	case stored.Allocation == nil && mode == v1beta1.PlacementModeAll:
		stored.Cluster = desired.Cluster
		stored.ObservationKnown, stored.AppliedPlanID = false, ""
		return stored
	default:
		return v1beta1.CandidatePlacement{Cluster: desired.Cluster}
	}
}

// equalAllocation reports two placement statuses with the same plan and the
// same allocation per cluster, in any order; a cluster listed twice never
// matches.
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
