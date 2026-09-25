package replay

import (
	"context"
	"testing"

	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRemoveInstanceForgetsTheExpectation pins the adapter behavior the
// driver stands in for: a row that left the owner status takes its
// create/delete expectation with it, so a later Instance reusing the index
// does not inherit a wait nobody will satisfy.
func TestRemoveInstanceForgetsTheExpectation(t *testing.T) {
	ctx := context.Background()
	scenario, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(ctx, scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	if err := d.store.MutateInstance(ctx, 0, func(s *types.InstanceStatus) bool {
		s.Phase = types.InstancePhaseCreating
		return true
	}); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	d.expectations.ExpectCreates(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0, 1)
	if d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("an outstanding create must not read as satisfied")
	}

	input := types.ReconcileInput{OwnerObject: d.owner}
	d.store.Install(&input)
	d.wrapCallbacks(&input)
	if _, err := input.RemoveInstance(ctx, 0); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("removing the row left its expectation behind")
	}
}
