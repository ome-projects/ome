package replay

import (
	"context"
	"strings"
	"testing"
)

// TestApplyOperationDeadlineNeedsASlack keeps the harness from inventing a
// behavioral value the scenario did not state.
func TestApplyOperationDeadlineNeedsASlack(t *testing.T) {
	d := &driver{store: NewRowStore(nil, nil, nil)}
	_, err := applyOperationDeadline(context.Background(), d, TimelineEvent{ID: "timer.operationDeadline"})
	if err == nil || !strings.Contains(err.Error(), "no in-flight operation") {
		t.Fatalf("expected the no-deadline error, got %v", err)
	}
}

// TestApplyStuckTerminatingNeedsADeletionInFlight keeps the event an
// observation about a deletion already issued rather than a way to conjure
// one: a pod nobody has deleted has nothing to be stuck at.
func TestApplyStuckTerminatingNeedsADeletionInFlight(t *testing.T) {
	d := &driver{opts: DefaultOptions(), terminating: map[string]*podTeardown{}}
	_, err := applyStuckTerminating(context.Background(), d, TimelineEvent{
		ID:   "pod.stuckTerminating",
		Args: EventArgs{Pod: &PodRef{Index: 0}},
	})
	if err == nil || !strings.Contains(err.Error(), "is not Terminating") {
		t.Fatalf("expected the not-Terminating error, got %v", err)
	}
}

// TestApplyForceDeleteNeedsAConfiguredPolicy holds the same line for the
// clock: the overdue slack is the operator's value, so an unconfigured
// escalation has no deadline to move onto.
func TestApplyForceDeleteNeedsAConfiguredPolicy(t *testing.T) {
	d := &driver{opts: DefaultOptions()}
	_, err := applyForceDelete(context.Background(), d, TimelineEvent{ID: "timer.forceDelete"})
	if err == nil || !strings.Contains(err.Error(), "config.forceDelete is not set") {
		t.Fatalf("expected the unconfigured-policy error, got %v", err)
	}
}
