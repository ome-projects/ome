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
	current := map[string]*v1beta1.CandidateAllocationStatus{}
	previous := map[string]*v1beta1.CandidateAllocationStatus{}
	if source.Status.Placement != nil {
		for _, candidate := range source.Status.Placement.Candidates {
			previous[candidate.Cluster] = candidate.Allocation
		}
	}
	for name, sample := range samples {
		evidence, err := capacityEvidence(sample)
		if err != nil {
			return err
		}
		current[name] = &v1beta1.CandidateAllocationStatus{ClusterUID: sample.ClusterUID, Capacity: evidence}
	}
	// Removed homes do not contribute to the new matched-set hardware ratios.
	for name := range previous {
		if _, matched := current[name]; !matched {
			delete(previous, name)
		}
	}
	digest := hardwareDigest(current)
	key := client.ObjectKeyFromObject(source)
	r.capacityMu.Lock()
	defer r.capacityMu.Unlock()
	if source.Status.Placement == nil || source.Status.Placement.Plan == nil || digest == hardwareDigest(previous) {
		delete(r.capacityPending, key)
		return nil
	}
	pending, exists := r.capacityPending[key]
	if !exists || pending.uid != source.UID || pending.digest != digest || now.Before(pending.since) {
		if r.capacityPending == nil {
			r.capacityPending = map[types.NamespacedName]capacityPending{}
		}
		r.capacityPending[key] = capacityPending{uid: source.UID, digest: digest, since: now}
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
