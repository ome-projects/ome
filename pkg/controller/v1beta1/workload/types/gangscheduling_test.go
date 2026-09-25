package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Whether a group's name is usable this pass is one question; what
// happens to the row is the escalation's. These pin the first.
func TestGangObservationsBlocksPodsOnAnUnusableName(t *testing.T) {
	obs := NewGangObservations()
	for idx, state := range map[int32]GangState{
		0: GangStateTerminating,
		1: GangStateOwnershipConflict,
		2: GangStateFailed,
	} {
		obs.Record(idx, GangObservation{State: state})
		if !obs.BlocksPods(idx) {
			t.Errorf("%s holds the name and must block member creates", state)
		}
	}
	obs.Record(9, GangObservation{})
	if obs.BlocksPods(9) {
		t.Error("an unclassified group blocks nothing")
	}
	if obs.BlocksPods(404) {
		t.Error("an index the pass never recorded blocks nothing")
	}
}

func TestGangObservationsAreNilSafe(t *testing.T) {
	var obs *GangObservations
	obs.Record(0, GangObservation{State: GangStateFailed})
	if (obs.For(0) != GangObservation{}) || obs.BlocksPods(0) {
		t.Error("a pass with no observations answers the zero reading")
	}
}

func TestOperationGangHeld(t *testing.T) {
	if OperationGangHeld(nil) || OperationGangHeld(&InstanceOperation{Waiting: WaitingReasonPaused}) {
		t.Error("only the PodGroup-terminating token is the gang hold")
	}
	if !OperationGangHeld(&InstanceOperation{Waiting: WaitingReasonPodGroupTerminating}) {
		t.Error("the token reads as gang-held")
	}
}

// A gang-surge source is judged on the surge index's group, not its own:
// its own group is the one its serving pods already belong to.
func TestGangReadingForSkipsASurgeSource(t *testing.T) {
	surgeIdx := int32(3)
	input := ReconcileInput{Gangs: NewGangObservations()}
	input.Gangs.Record(0, GangObservation{State: GangStateFailed})

	source := InstanceStatus{Index: 0, Operation: &InstanceOperation{SurgeIndex: &surgeIdx}}
	if got := GangReadingFor(input, source); got.State != "" {
		t.Errorf("a surge source takes no reading of its own: got %q", got.State)
	}
	plain := InstanceStatus{Index: 0}
	if got := GangReadingFor(input, plain); got.State != GangStateFailed {
		t.Errorf("a plain row reads its own group: got %q", got.State)
	}
}

func TestGangTerminationCarriesTheGroupsOwnExplanation(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	rec := GangTermination("PodGroupFailed", "minMember not met", at)
	if rec.Reason != "PodGroupFailed" || rec.Message != "minMember not met" || !rec.Time.Equal(&at) {
		t.Fatalf("record must carry reason, message and moment verbatim: %+v", *rec)
	}
}
