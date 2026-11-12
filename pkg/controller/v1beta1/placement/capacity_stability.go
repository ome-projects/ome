package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
)

type capacityPending struct {
	uid    types.UID
	digest string
	since  time.Time
}

// stableCapacity debounces hardware changes while retaining accepted authority.
// Restarting resets a pending window; persisted accepted hardware needs no wait.
func (r *Reconciler) stableCapacity(source *v1beta1.InferenceService, samples map[string]capacity.Sample, now time.Time) error {
	current, err := recordedHardware(samples)
	if err != nil {
		return err
	}
	digest := hardwareDigest(current)
	key := client.ObjectKeyFromObject(source)
	r.capacityMu.Lock()
	defer r.capacityMu.Unlock()
	if !hasAcceptedPlan(source) || digest == hardwareDigest(acceptedHardware(source, current)) {
		delete(r.capacityPending, key)
		return nil
	}
	return r.waitForStableHardware(key, source.UID, digest, now)
}

// recordedHardware is each sampled member's hardware as the plan records it.
func recordedHardware(samples map[string]capacity.Sample) (map[string]*v1beta1.CandidateAllocationStatus, error) {
	current := map[string]*v1beta1.CandidateAllocationStatus{}
	for name, sample := range samples {
		recorded, err := recordedCapacitySample(sample)
		if err != nil {
			return nil, err
		}
		current[name] = &v1beta1.CandidateAllocationStatus{ClusterUID: sample.ClusterUID, Capacity: recorded}
	}
	return current, nil
}

// hasAcceptedPlan reports whether the source carries a plan whose hardware
// a change is measured against.
func hasAcceptedPlan(source *v1beta1.InferenceService) bool {
	return source.Status.Placement != nil && source.Status.Placement.Plan != nil
}

// acceptedHardware is the hardware the plan accepted for the members the
// match still names. Removed members do not contribute to the new
// matched-set hardware ratios.
func acceptedHardware(source *v1beta1.InferenceService, matched map[string]*v1beta1.CandidateAllocationStatus) map[string]*v1beta1.CandidateAllocationStatus {
	previous := map[string]*v1beta1.CandidateAllocationStatus{}
	if source.Status.Placement == nil {
		return previous
	}
	for _, candidate := range source.Status.Placement.Candidates {
		if _, stillMatched := matched[candidate.Cluster]; stillMatched {
			previous[candidate.Cluster] = candidate.Allocation
		}
	}
	return previous
}

// waitForStableHardware holds a changed digest until it has stood for the
// stability window. A digest, source or clock that differs from the pending
// record starts the window again. The caller holds capacityMu.
func (r *Reconciler) waitForStableHardware(key types.NamespacedName, uid types.UID, digest string, now time.Time) error {
	pending, exists := r.capacityPending[key]
	if !exists || pending.uid != uid || pending.digest != digest || now.Before(pending.since) {
		if r.capacityPending == nil {
			r.capacityPending = map[types.NamespacedName]capacityPending{}
		}
		r.capacityPending[key] = capacityPending{uid: uid, digest: digest, since: now}
		return fmt.Errorf("hardware change is waiting for the configured stability window")
	}
	if now.Sub(pending.since) < r.Capacity.StabilityWindow {
		return fmt.Errorf("hardware change is waiting for the configured stability window")
	}
	return nil
}

func (r *Reconciler) forgetCapacity(key types.NamespacedName) {
	r.capacityMu.Lock()
	defer r.capacityMu.Unlock()
	delete(r.capacityPending, key)
}

func hardwareDigest(assignments map[string]*v1beta1.CandidateAllocationStatus) string {
	type hardware struct {
		Cluster string
		UID     types.UID
		Pools   []v1beta1.PlacementCapacityPool
	}
	items := []hardware{}
	for _, name := range slices.Sorted(maps.Keys(assignments)) {
		assignment := assignments[name]
		if assignment == nil || assignment.Capacity == nil {
			continue
		}
		entry := hardware{Cluster: name, UID: assignment.ClusterUID, Pools: assignment.Capacity.DeepCopy().Pools}
		for i := range entry.Pools {
			entry.Pools[i].Demand = 0
			entry.Pools[i].ObservedAt = metav1.Time{}
			entry.Pools[i].ReportResourceVersion = ""
		}
		items = append(items, entry)
	}
	// Only concrete API structs enter this fixed comparison format.
	encoded, _ := json.Marshal(items)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
