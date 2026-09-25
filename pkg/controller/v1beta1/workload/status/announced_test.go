package status

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	standingReason = types.EventReasonGangSplitRisk
	attemptReason  = types.EventReasonPodGroupReset
)

// An attempt-scoped message is delivered once per operation and no more.
func TestAnnounce_DeliversOncePerOperation(t *testing.T) {
	row := &types.InstanceStatus{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{ID: "create-0-1"},
	}
	if !markAnnounced(row, attemptReason) {
		t.Fatal("the first delivery must record")
	}
	if markAnnounced(row, attemptReason) {
		t.Fatal("a second delivery in the same episode must be refused")
	}
	if !Announced(*row, attemptReason) {
		t.Fatal("the row must report the message as delivered")
	}
	if Announced(*row, types.EventReasonDrainOverdue) {
		t.Fatal("one message's marker must not answer for another")
	}
	if !markAnnounced(row, types.EventReasonDrainOverdue) {
		t.Fatal("a different message is a different marker")
	}
	if len(row.Announced) != 2 {
		t.Fatalf("markers = %v, want both", row.Announced)
	}
}

// A new operation is a new attempt episode: the previous one's markers
// stop matching, and the write that has something to say collects them.
func TestAnnounce_ForgetsTheAttemptItReplaced(t *testing.T) {
	row := &types.InstanceStatus{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{ID: "create-0-1"},
	}
	markAnnounced(row, attemptReason)

	row.Operation = &types.InstanceOperation{ID: "update-0-2"}
	if Announced(*row, attemptReason) {
		t.Fatal("a marker of the episode that ended must not answer for this one")
	}
	if !markAnnounced(row, attemptReason) {
		t.Fatal("the new episode must be able to deliver the same message")
	}
	if len(row.Announced) != 1 || row.Announced[0] != string(attemptReason)+"@update-0-2" {
		t.Fatalf("markers = %v, want only the current attempt's", row.Announced)
	}
}

// A standing message keys on the incarnation whatever the row's
// operation does: an attempt opening or concluding neither re-delivers
// it nor drops its marker, and only a rebuild starts a fresh episode.
func TestAnnounce_StandingMessageOutlivesTheAttempt(t *testing.T) {
	row := &types.InstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       types.InstancePhaseCreating,
		Operation:   &types.InstanceOperation{ID: "create-0-1"},
	}
	if !markAnnounced(row, standingReason) {
		t.Fatal("the first delivery must record")
	}
	if got := row.Announced[0]; got != string(standingReason)+"@#1" {
		t.Fatalf("marker = %q, want the incarnation episode", got)
	}

	// The attempt concludes.
	row.Operation = nil
	row.Phase = types.InstancePhaseReady
	if !Announced(*row, standingReason) {
		t.Fatal("an attempt concluding must not change a standing warning's episode")
	}
	// A new attempt opens, and an attempt-scoped write prunes.
	row.Operation = &types.InstanceOperation{ID: "update-0-2"}
	if !markAnnounced(row, attemptReason) {
		t.Fatal("the attempt message must record on the new operation")
	}
	if !Announced(*row, standingReason) {
		t.Fatalf("the standing marker must survive the prune; markers = %v", row.Announced)
	}

	// A rebuild is a new Instance.
	row.Incarnation = 2
	if Announced(*row, standingReason) {
		t.Fatal("a rebuilt Instance must not inherit the previous one's markers")
	}
	if !markAnnounced(row, standingReason) {
		t.Fatal("the rebuilt Instance must be told again")
	}
	if len(row.Announced) != 2 {
		t.Fatalf("markers = %v, want the live attempt's and the new incarnation's", row.Announced)
	}
}

// An attempt-scoped message on a row with no operation is about the
// Instance as built, so it keys on the incarnation.
func TestAnnounce_AttemptMessageWithNoOperationKeysOnIncarnation(t *testing.T) {
	row := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, Incarnation: 3}
	if !markAnnounced(row, attemptReason) {
		t.Fatal("the first delivery must record")
	}
	if got := row.Announced[0]; got != string(attemptReason)+"@#3" {
		t.Fatalf("marker = %q, want the incarnation episode", got)
	}
	row.Incarnation = 4
	if Announced(*row, attemptReason) {
		t.Fatal("a rebuilt Instance must not inherit the previous one's markers")
	}
}

// Announce reports whether THIS pass is the one that recorded the
// message, because that is what the caller emits from.
func TestAnnounce_ReportsTheWriteThatEarnedTheMessage(t *testing.T) {
	rows := map[int32]types.InstanceStatus{
		0: {Index: 0, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{ID: "create-0-1"}},
	}
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			row := rows[idx]
			if mutate(&row) {
				rows[idx] = row
			}
			return nil
		},
	}
	first, err := Announce(context.Background(), input, 0, standingReason)
	if err != nil || !first {
		t.Fatalf("first announce = %v, %v", first, err)
	}
	second, err := Announce(context.Background(), input, 0, standingReason)
	if err != nil || second {
		t.Fatalf("second announce = %v, %v", second, err)
	}
}

// A slot the writer had to append — no phase — is a slot deleted out from
// under the pass, and is never resurrected by a marker.
func TestAnnounce_RefusesAnEmptySlot(t *testing.T) {
	written := false
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			row := types.InstanceStatus{Index: 4}
			written = mutate(&row)
			return nil
		},
	}
	announced, err := Announce(context.Background(), input, 4, standingReason)
	if err != nil || announced || written {
		t.Fatalf("announce on an empty slot = %v, written=%v, err=%v", announced, written, err)
	}
}

// The observation short-circuits the write, so a message already
// delivered costs no mutation on any later pass.
func TestAnnounce_SkipsTheWriteWhenTheObservationAlreadyCarriesIt(t *testing.T) {
	observed := types.InstanceStatus{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{ID: "create-0-1"},
	}
	markAnnounced(&observed, standingReason)
	calls := 0
	input := types.ReconcileInput{
		ObservedState:  types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{observed}},
		MutateInstance: func(context.Context, int32, func(*types.InstanceStatus) bool) error { calls++; return nil },
	}
	announced, err := Announce(context.Background(), input, 0, standingReason)
	if err != nil || announced || calls != 0 {
		t.Fatalf("announce = %v, mutations=%d, err=%v", announced, calls, err)
	}
}

// An adapter with no row writer has no row to remember on, so the
// message is delivered rather than dropped.
func TestAnnounce_DeliversWithoutARowWriter(t *testing.T) {
	announced, err := Announce(context.Background(), types.ReconcileInput{}, 0, standingReason)
	if err != nil || !announced {
		t.Fatalf("announce = %v, %v", announced, err)
	}
}
