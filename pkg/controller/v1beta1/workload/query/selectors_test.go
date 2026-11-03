package query

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/labels"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestLeaderRoutingSelector(t *testing.T) {
	base := map[string]string{LabelManagedBy: ManagedByOMENative}

	got := LeaderRoutingSelector(base, true)
	want := map[string]string{
		LabelManagedBy:  ManagedByOMENative,
		LabelRunner:     workload.RunnerLeader,
		LabelPodOrdinal: "0",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("with ordinal: got %v want %v", got, want)
	}

	got = LeaderRoutingSelector(base, false)
	want = map[string]string{
		LabelManagedBy: ManagedByOMENative,
		LabelRunner:    workload.RunnerLeader,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("without ordinal: got %v want %v", got, want)
	}

	if !reflect.DeepEqual(base, map[string]string{LabelManagedBy: ManagedByOMENative}) {
		t.Errorf("base selector must not be modified: %v", base)
	}
}

// The selector admits exactly the leader pod of every Instance: the rank-0
// worker shares ordinal 0 and is kept out by the runner key alone.
func TestLeaderRoutingSelector_AdmitsOnlyTheLeader(t *testing.T) {
	pod := func(runner, ordinal string) labels.Set {
		return labels.Set{LabelManagedBy: ManagedByOMENative, LabelRunner: runner, LabelPodOrdinal: ordinal}
	}
	withOrdinal := labels.SelectorFromSet(LeaderRoutingSelector(map[string]string{LabelManagedBy: ManagedByOMENative}, true))
	withoutOrdinal := labels.SelectorFromSet(LeaderRoutingSelector(map[string]string{LabelManagedBy: ManagedByOMENative}, false))

	cases := []struct {
		name                  string
		pod                   labels.Set
		wantWith, wantWithout bool
	}{
		{"leader ordinal 0", pod(workload.RunnerLeader, "0"), true, true},
		{"worker ordinal 0", pod(workload.RunnerWorker, "0"), false, false},
		{"worker ordinal 1", pod(workload.RunnerWorker, "1"), false, false},
		{"single-pod default runner", pod(workload.RunnerDefault, "0"), false, false},
		{"leader without an ordinal label", labels.Set{LabelManagedBy: ManagedByOMENative, LabelRunner: workload.RunnerLeader}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withOrdinal.Matches(tc.pod); got != tc.wantWith {
				t.Errorf("with ordinal: matches=%v want %v", got, tc.wantWith)
			}
			if got := withoutOrdinal.Matches(tc.pod); got != tc.wantWithout {
				t.Errorf("without ordinal: matches=%v want %v", got, tc.wantWithout)
			}
		})
	}
}
