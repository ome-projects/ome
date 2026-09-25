package replay

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

// The gang.podGroup family writes only the group states the engine
// classifies. A scenario naming anything else has to fail with the list,
// or an author reads a silently inert event as a passing assertion.

func TestApplyPodGroupNeedsTheInstanceItsGroupBelongsTo(t *testing.T) {
	_, err := applyPodGroup(context.Background(), nil, TimelineEvent{Variant: "Deleted"})
	if err == nil || !strings.Contains(err.Error(), "Instance index") {
		t.Fatalf("a group event with no Instance must name what it is missing: got %v", err)
	}
}

func TestApplyPodGroupRefusesAnUnwrittenState(t *testing.T) {
	ctx := context.Background()
	scenario, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(ctx, scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	idx := int32(0)
	if err := d.cli.Create(ctx, &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: d.podGroupName(idx)},
	}); err != nil {
		t.Fatalf("announce the group: %v", err)
	}
	ev := TimelineEvent{Variant: "Scheduled"}
	ev.Args.Instance = &idx
	_, err = applyPodGroup(ctx, d, ev)
	if err == nil {
		t.Fatal("an unwritten group state must fail the scenario")
	}
	if !strings.Contains(err.Error(), "Terminating") {
		t.Fatalf("the error must list the states the driver writes: got %v", err)
	}
}

func TestPodGroupNameMatchesTheEngineComposition(t *testing.T) {
	ctx := context.Background()
	scenario, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(ctx, scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	name := d.podGroupName(2)
	if !strings.HasPrefix(name, d.opts.OwnerName) || !strings.HasSuffix(name, "-2") {
		t.Fatalf("group name must compose owner and index: got %q for owner %q", name, d.opts.OwnerName)
	}
}
