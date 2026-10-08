package replay

import (
	"context"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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

// retiringMarkerGroup walks a gang surge into a cleanup marker and then stages
// one gang.podGroup event on the retired group, named by the %s verb.
const retiringMarkerGroup = `
scenario: retiring-marker-group
arrows: [T-empty-gang-surge-target, T-gangtarget-cleanup-superseded, T-gangcleanup-done]
initial:
  spec:
    replicas: 1
    workers: 1
    topologyKey: topology.example.com/fabric-domain
    gangScheduling: true
    schedulerName: gang-scheduler
    strategy: SurgeThenDrain
    image: registry.example.com/runtime:v1
    instanceReadyTimeout: 30m
  config:
    requeueOperation: 5s
    requeueGate: 3s
timeline:
  - tick: 1
  - tick: 2
    events:
      - pod.ready[source]: {pod: {index: 0, runner: leader}}
      - pod.ready[source]: {pod: {index: 0, runner: worker}}
      - endpoint.rotationIn[source]: {pod: {index: 0, runner: leader}}
  - tick: 3
    events:
      - spec.revision: {image: registry.example.com/runtime:v2}
  - tick: 4
  - tick: 5
    events:
      - spec.revision: {image: registry.example.com/runtime:v3}
  - tick: 6
    events:
      - gang.podGroup[%s]: {instance: 1}
  - tick: 7
  - tick: 8
    events:
      - pod.deleted[target]: {pod: {index: 1, runner: leader}}
      - pod.deleted[target]: {pod: {index: 1, runner: worker}}
`

// A gang-surge cleanup marker finalizes its own PodGroup, so the pass neither
// re-creates the group once it is gone nor judges the marker on a foreign
// controller: the index is in the adapter's terminal-owned set.
func TestReconcileGangsSkipsARetiringMarkersGroup(t *testing.T) {
	opts := DefaultOptions()
	group := query.PodGroupName(opts.OwnerName, opts.Component, 1)
	for _, tc := range []struct {
		variant string
		absent  string
	}{
		{variant: "Deleted", absent: "podgroup create name=" + group},
		{variant: "OwnershipConflict", absent: "PodGroupOwnershipConflict"},
	} {
		t.Run(tc.variant, func(t *testing.T) {
			scenario, err := Parse([]byte(fmt.Sprintf(retiringMarkerGroup, tc.variant)), Vocabulary{})
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			trace, err := Run(context.Background(), scenario, opts)
			if err != nil {
				t.Fatalf("run: %v\ntrace:\n%s", err, trace)
			}
			staged := "apply gang.podGroup[" + tc.variant + "] podGroup=" + group
			at := strings.Index(trace, staged)
			if at < 0 {
				t.Fatalf("the group event was not staged:\n%s", trace)
			}
			before, after := trace[:at], trace[at:]
			if !strings.Contains(before, "step=GangSurgeTargetCleanup") {
				t.Fatalf("the marker must be a cleanup claim before the group event lands:\n%s", before)
			}
			if strings.Contains(after, tc.absent) {
				t.Fatalf("a retiring marker's group must not produce %q:\n%s", tc.absent, after)
			}
			if !strings.Contains(after, "row=1 remove") {
				t.Fatalf("the retirement must still remove the marker:\n%s", after)
			}
		})
	}
}
