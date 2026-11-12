package placement

import (
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
)

// stabilityWaitText is what the operator reads while a hardware change is
// measured against the stability window.
const stabilityWaitText = "hardware change is waiting for the configured stability window"

// stabilitySource is a capacity-split source whose plan accepted member-a's
// hardware at weight 4.
func stabilitySource(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	source := plannedTestSource()
	attachCapacityContract(t, source)
	return source
}

// acceptMemberHardware adds a member whose plan accepted hardware of the given
// weight.
func acceptMemberHardware(t *testing.T, source *v1beta1.InferenceService, name string, weight int64) {
	t.Helper()
	recorded, err := recordedCapacitySample(plannedCapacitySample(*plannedTestRegistration(), weight))
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name,
		Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-a-uid", CurrentReplicas: 2, DesiredReplicas: 2, Capacity: recorded}})
}

// hardwareAt is one member's fresh sample at the given weight.
func hardwareAt(weight int64) capacity.Sample {
	return plannedCapacitySample(*plannedTestRegistration(), weight)
}

func expectStabilityWait(t *testing.T, err error) {
	t.Helper()
	if err == nil || err.Error() != stabilityWaitText {
		t.Fatalf("stableCapacity = %v, want %q", err, stabilityWaitText)
	}
}

func expectStable(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("stableCapacity = %v, want nil", err)
	}
}

// TestCapacityStabilityWaitNamesTheWindow holds the text of the wait on a new
// hardware change and inside its window, and the end of the wait at the
// window.
func TestCapacityStabilityWaitNamesTheWindow(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	changed := map[string]capacity.Sample{"member-a": hardwareAt(8)}
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	expectStabilityWait(t, r.stableCapacity(source, changed, start))
	expectStabilityWait(t, r.stableCapacity(source, changed, start.Add(window/2)))
	expectStable(t, r.stableCapacity(source, changed, start.Add(window)))
}

// TestCapacityStabilityIgnoresAMemberThatLeftTheMatch: a member the plan
// accepted but the match no longer names is not a hardware change, and the
// read that finds the rest unchanged forgets any window in progress.
func TestCapacityStabilityIgnoresAMemberThatLeftTheMatch(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	acceptMemberHardware(t, source, "member-b", 8)
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	bothChanged := map[string]capacity.Sample{"member-a": hardwareAt(8), "member-b": hardwareAt(8)}
	expectStabilityWait(t, r.stableCapacity(source, bothChanged, start))
	expectStable(t, r.stableCapacity(source, map[string]capacity.Sample{"member-a": hardwareAt(4)}, start.Add(window/2)))
	// The window of the first change was forgotten: the same change starts a new one.
	expectStabilityWait(t, r.stableCapacity(source, bothChanged, start.Add(window)))
}

// TestCapacityStabilityWaitsForAMemberThatJoins: a member the match names
// for the first time is a hardware change even when the others stand still.
func TestCapacityStabilityWaitsForAMemberThatJoins(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	joined := map[string]capacity.Sample{"member-a": hardwareAt(4), "member-b": hardwareAt(4)}
	expectStabilityWait(t, r.stableCapacity(source, joined, time.Unix(1000, 0)))
}

// TestCapacityStabilityWithoutAPlanDoesNotWait: hardware is debounced only
// against a plan the source accepted; without one the read passes and forgets
// any window in progress.
func TestCapacityStabilityWithoutAPlanDoesNotWait(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	changed := map[string]capacity.Sample{"member-a": hardwareAt(8)}
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	expectStabilityWait(t, r.stableCapacity(source, changed, start))
	plan := source.Status.Placement.Plan
	source.Status.Placement.Plan = nil
	expectStable(t, r.stableCapacity(source, changed, start.Add(window/2)))
	source.Status.Placement.Plan = plan
	expectStabilityWait(t, r.stableCapacity(source, changed, start.Add(window)))
}

// TestCapacityStabilityRestartsTheWindowOnASecondChange: hardware that
// changes again inside the window starts the window over.
func TestCapacityStabilityRestartsTheWindowOnASecondChange(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	expectStabilityWait(t, r.stableCapacity(source, map[string]capacity.Sample{"member-a": hardwareAt(8)}, start))
	again := map[string]capacity.Sample{"member-a": hardwareAt(16)}
	expectStabilityWait(t, r.stableCapacity(source, again, start.Add(window/2)))
	expectStabilityWait(t, r.stableCapacity(source, again, start.Add(window)))
	expectStable(t, r.stableCapacity(source, again, start.Add(window/2+window)))
}

// TestCapacityStabilityKeepsAWindowPerSource: two sources measure their own
// changes; one reaching its window does not release the other.
func TestCapacityStabilityKeepsAWindowPerSource(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	first := stabilitySource(t)
	second := stabilitySource(t)
	second.Name = "svc-2"
	changed := map[string]capacity.Sample{"member-a": hardwareAt(8)}
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	expectStabilityWait(t, r.stableCapacity(first, changed, start))
	expectStabilityWait(t, r.stableCapacity(second, changed, start.Add(window/2)))
	expectStable(t, r.stableCapacity(first, changed, start.Add(window)))
	expectStabilityWait(t, r.stableCapacity(second, changed, start.Add(window)))
	expectStable(t, r.stableCapacity(second, changed, start.Add(window/2+window)))
}

// TestCapacityStabilityRefusesAMalformedSampleAndKeepsTheWindow: a sample the
// plan cannot record is refused with the recorder's words before the window
// is touched, so the window of a change in progress still ends on time.
func TestCapacityStabilityRefusesAMalformedSampleAndKeepsTheWindow(t *testing.T) {
	r := &Reconciler{Capacity: providerConfig()}
	source := stabilitySource(t)
	changed := map[string]capacity.Sample{"member-a": hardwareAt(8)}
	start := time.Unix(1000, 0)
	window := providerConfig().StabilityWindow
	expectStabilityWait(t, r.stableCapacity(source, changed, start))
	malformed := hardwareAt(8)
	malformed.DemandFingerprint = ""
	err := r.stableCapacity(source, map[string]capacity.Sample{"member-a": malformed}, start.Add(window/2))
	// Only the opening of the recorder's text is matched; its last word is
	// the code's own and is not repeated here.
	if want := "capacity requires a demand fingerprint, nonnegative replicas, and pool "; err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("stableCapacity = %v, want a text opening with %q", err, want)
	}
	expectStable(t, r.stableCapacity(source, changed, start.Add(window)))
}

// TestCapacityStabilityAnAcceptedMemberWithoutHardwareIsNew: a candidate the
// plan holds without recorded hardware, or without an allocation, counts as
// hardware seen for the first time.
func TestCapacityStabilityAnAcceptedMemberWithoutHardwareIsNew(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.CandidatePlacement)
	}{
		{name: "no recorded hardware", edit: func(c *v1beta1.CandidatePlacement) { c.Allocation.Capacity = nil }},
		{name: "no allocation", edit: func(c *v1beta1.CandidatePlacement) { c.Allocation = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reconciler{Capacity: providerConfig()}
			source := stabilitySource(t)
			tt.edit(&source.Status.Placement.Candidates[0])
			expectStabilityWait(t, r.stableCapacity(source, map[string]capacity.Sample{"member-a": hardwareAt(4)}, time.Unix(1000, 0)))
		})
	}
}
