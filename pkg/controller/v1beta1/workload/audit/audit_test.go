package audit

import (
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// fixedNow gives the ValidateCapacity tests a deterministic "now" so the
// trailing-window calculation is reproducible.
var fixedNow = time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

// mkRecord builds a QUEUED Manual migration record (no surge allocated,
// no AllocatedAt) whose StartedAt is offset from fixedNow. Terminal
// phases get a CompletedAt one minute later.
func mkRecord(uuid string, phase types.MigrationPhase, startedOffset time.Duration) types.MigrationRecord {
	r := types.MigrationRecord{
		RequestUUID: uuid,
		Trigger:     types.MigrationTriggerManual,
		Phase:       phase,
		StartedAt:   metav1.NewTime(fixedNow.Add(startedOffset)),
	}
	if phase.Terminal() {
		completed := metav1.NewTime(fixedNow.Add(startedOffset + time.Minute))
		r.CompletedAt = &completed
	}
	return r
}

// mkAllocatedRecord is mkRecord with an allocated surge — an EXECUTING
// record: SurgeInstance set and AllocatedAt at the given offset.
func mkAllocatedRecord(uuid string, phase types.MigrationPhase, startedOffset, allocatedOffset time.Duration) types.MigrationRecord {
	r := mkRecord(uuid, phase, startedOffset)
	idx := int32(9)
	r.SurgeInstance = &idx
	at := metav1.NewTime(fixedNow.Add(allocatedOffset))
	r.AllocatedAt = &at
	return r
}

func TestValidateCapacity_NoRecordsAllowed(t *testing.T) {
	ok, reason := ValidateCapacity(nil, "u-new", fixedNow)
	if !ok || reason != "" {
		t.Errorf("no records should admit; got ok=%v reason=%q", ok, reason)
	}
}

func TestValidateCapacity_InFlightCapTrips(t *testing.T) {
	// Three ALLOCATED non-terminal records == in-flight cap; the FOURTH
	// request (its own record excluded from the count) is rejected.
	// Three allocated non-terminal records on one IR can't occur under
	// the serial dispatcher — constructed directly to pin the cap.
	records := []types.MigrationRecord{
		mkAllocatedRecord("u1", types.MigrationPhaseSurgePending, -10*time.Minute, -9*time.Minute),
		mkAllocatedRecord("u2", types.MigrationPhaseSurgeReady, -5*time.Minute, -4*time.Minute),
		mkAllocatedRecord("u3", types.MigrationPhaseDraining, -1*time.Minute, -1*time.Minute),
		mkRecord("u4", types.MigrationPhaseAccepted, -time.Second),
	}
	ok, reason := ValidateCapacity(records, "u4", fixedNow)
	if ok {
		t.Fatalf("expected reject when in-flight cap reached; got ok=true")
	}
	if reason == "" {
		t.Errorf("rejection reason must be non-empty")
	}
}

func TestValidateCapacity_QueuedDoesNotCountAsInFlight(t *testing.T) {
	// Non-terminal but UNALLOCATED records are queued intent, not
	// execution — they hold no resources and never consume a slot.
	records := []types.MigrationRecord{
		mkRecord("u1", types.MigrationPhaseAccepted, -10*time.Minute),
		mkRecord("u2", types.MigrationPhaseAccepted, -5*time.Minute),
		mkRecord("u3", types.MigrationPhaseAccepted, -3*time.Minute),
		mkRecord("u4", types.MigrationPhaseAccepted, -time.Second),
	}
	ok, reason := ValidateCapacity(records, "u4", fixedNow)
	if !ok {
		t.Errorf("queued (unallocated) records must not consume in-flight capacity: %s", reason)
	}
}

func TestValidateCapacity_OwnRecordExcluded(t *testing.T) {
	// Two other executing records + the request's own Accepted record:
	// in-flight (excluding self) is 2 < cap 3, so the 3rd admits.
	records := []types.MigrationRecord{
		mkAllocatedRecord("u1", types.MigrationPhaseSurgePending, -10*time.Minute, -9*time.Minute),
		mkAllocatedRecord("u2", types.MigrationPhaseSurgeReady, -5*time.Minute, -4*time.Minute),
		mkRecord("u3", types.MigrationPhaseAccepted, -time.Second),
	}
	ok, reason := ValidateCapacity(records, "u3", fixedNow)
	if !ok {
		t.Errorf("a request's own record must not count against it: %s", reason)
	}
}

func TestValidateCapacity_TerminalDoesNotCountAsInFlight(t *testing.T) {
	// Terminal records (Completed / Failed / Relocated) free their slot
	// structurally — the wedged-slot capacity leak cannot exist — even
	// with a surge still recorded. AllocatedAt outside the window keeps
	// the per-hour cap out of the picture.
	records := []types.MigrationRecord{
		mkAllocatedRecord("u1", types.MigrationPhaseCompleted, -30*time.Hour, -30*time.Hour),
		mkAllocatedRecord("u2", types.MigrationPhaseFailed, -25*time.Hour, -25*time.Hour),
		mkRecord("u3", types.MigrationPhaseRelocated, -20*time.Hour),
	}
	ok, _ := ValidateCapacity(records, "u-new", fixedNow)
	if !ok {
		t.Errorf("terminal records must not consume in-flight capacity")
	}
}

// Auto relocation records are born terminal (Relocated) and never
// allocate a surge, so they are structurally invisible to BOTH caps:
// not in-flight (terminal), not in the per-hour window (no
// AllocatedAt) — even a burst of fresh Auto records admits new Manual
// work. Auto churn is bounded separately by maxAttempts per instance.
func TestValidateCapacity_AutoRecordsCountTowardNeitherCap(t *testing.T) {
	records := make([]types.MigrationRecord, 0, DefaultPerHourCap+DefaultInFlightCap)
	for i := 0; i < DefaultPerHourCap+DefaultInFlightCap; i++ {
		completed := metav1.NewTime(fixedNow.Add(-time.Duration(i+1) * time.Minute))
		records = append(records, types.MigrationRecord{
			RequestUUID:    "u-auto-" + strconv.Itoa(i),
			Trigger:        types.MigrationTriggerAuto,
			Phase:          types.MigrationPhaseRelocated,
			SourceInstance: 0,
			FromNode:       "node-a",
			Attempt:        int32(i + 1),
			StartedAt:      completed,
			CompletedAt:    &completed,
		})
	}
	ok, reason := ValidateCapacity(records, "u-new", fixedNow)
	if !ok {
		t.Errorf("Auto records must count toward neither cap: %s", reason)
	}
}

func TestValidateCapacity_PerHourCapTrips(t *testing.T) {
	// DefaultPerHourCap records (any phase) whose AllocatedAt lies inside
	// the trailing window: per-hour cap reached even though nothing is in
	// flight (all terminal).
	records := make([]types.MigrationRecord, 0, DefaultPerHourCap)
	for i := 0; i < DefaultPerHourCap; i++ {
		off := -time.Duration(i+1) * time.Minute
		records = append(records, mkAllocatedRecord("u-recent-"+strconv.Itoa(i), types.MigrationPhaseCompleted, off, off))
	}
	ok, reason := ValidateCapacity(records, "u-new", fixedNow)
	if ok {
		t.Fatalf("expected reject when per-hour cap reached; got ok=true")
	}
	if reason == "" {
		t.Errorf("rejection reason must be non-empty")
	}
}

func TestValidateCapacity_OlderThanWindowDoesNotCount(t *testing.T) {
	// Records whose execution started before the trailing window don't
	// count toward the per-hour cap.
	records := make([]types.MigrationRecord, 0, DefaultPerHourCap)
	for i := 0; i < DefaultPerHourCap; i++ {
		off := -(CapacityRateWindow + 30*time.Minute + time.Duration(i)*time.Minute)
		records = append(records, mkAllocatedRecord("u-old-"+strconv.Itoa(i), types.MigrationPhaseCompleted, off, off))
	}
	ok, _ := ValidateCapacity(records, "u-new", fixedNow)
	if !ok {
		t.Errorf("records allocated before the window should not consume per-hour capacity")
	}
}

func TestValidateCapacity_BurstAcceptedExecutedSlowly(t *testing.T) {
	// 11 requests accepted in one burst (StartedAt all inside the
	// window — a per-hour counter keyed on StartedAt alone would
	// reject), executed slowly: 6 already done with AllocatedAt spread
	// 20 minutes apart so only two executions fall inside the trailing
	// window, 4 still queued. The 11th's gate admits — per-hour counts
	// executions, not accepts.
	records := make([]types.MigrationRecord, 0, 11)
	for i := 0; i < 6; i++ {
		rec := mkAllocatedRecord("u-done-"+strconv.Itoa(i), types.MigrationPhaseCompleted,
			-30*time.Minute, -time.Duration(20+i*20)*time.Minute)
		records = append(records, rec)
	}
	for i := 0; i < 4; i++ {
		records = append(records, mkRecord("u-queued-"+strconv.Itoa(i), types.MigrationPhaseAccepted, -30*time.Minute))
	}
	records = append(records, mkRecord("u-next", types.MigrationPhaseAccepted, -30*time.Minute))
	ok, reason := ValidateCapacity(records, "u-next", fixedNow)
	if !ok {
		t.Errorf("a burst-accepted, slowly-executed batch must not trip the per-hour cap: %s", reason)
	}
}

func TestValidateCapacity_QueuedBurstDoesNotTripCaps(t *testing.T) {
	// THE BATCH REGRESSION: 11 requests accepted in one burst, none
	// executing yet (no surge allocated). Queued-Accepted records hold no
	// resources — the dispatcher executes serially — so neither cap may
	// count them: the oldest request's fresh-path gate must admit.
	records := make([]types.MigrationRecord, 0, 11)
	for i := 0; i < 11; i++ {
		records = append(records, mkRecord("u-burst-"+strconv.Itoa(i), types.MigrationPhaseAccepted, -time.Duration(i+1)*time.Minute))
	}
	ok, reason := ValidateCapacity(records, "u-burst-10", fixedNow)
	if !ok {
		t.Errorf("queued-Accepted records must be unbounded (execution not started); got rejection: %s", reason)
	}
}

func TestParseTime_RFC3339(t *testing.T) {
	in := "2026-05-21T12:00:00Z"
	got, ok := parseTime(in)
	if !ok {
		t.Fatalf("RFC3339 parse failed for %q", in)
	}
	if !got.Equal(fixedNow) {
		t.Errorf("got %v want %v", got, fixedNow)
	}
}

func TestParseTime_RFC3339Nano(t *testing.T) {
	in := "2026-05-21T12:00:00.123456789Z"
	got, ok := parseTime(in)
	if !ok {
		t.Fatalf("RFC3339Nano parse failed for %q", in)
	}
	if got.Year() != 2026 || got.Month() != 5 || got.Day() != 21 {
		t.Errorf("got %v want 2026-05-21", got)
	}
}

func TestParseTime_EmptyOrInvalid(t *testing.T) {
	for _, in := range []string{"", "not-a-timestamp", "2026/05/21"} {
		if _, ok := parseTime(in); ok {
			t.Errorf("parse of %q should fail", in)
		}
	}
}
