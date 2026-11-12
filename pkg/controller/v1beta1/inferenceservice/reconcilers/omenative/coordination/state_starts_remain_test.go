package coordination

import "testing"

// TestComponentObservation_StartsRemain reads the one rule the signal
// applies per member: a roll in flight with an Instance beyond the partition
// not yet on the target.
func TestComponentObservation_StartsRemain(t *testing.T) {
	cases := []struct {
		name string
		obs  ComponentObservation
		want bool
	}{
		{"keeps its template", ComponentObservation{SpecReplicas: 12, NewRevisionPods: 12}, false},
		{"rolling with Instances left", ComponentObservation{RolloutInFlight: true, SpecReplicas: 12, NewRevisionPods: 4}, true},
		{"rolling with every Instance on the target", ComponentObservation{RolloutInFlight: true, SpecReplicas: 12, NewRevisionPods: 12}, false},
		{"resting at its partition", ComponentObservation{RolloutInFlight: true, SpecReplicas: 12, Partition: 4, NewRevisionPods: 8}, false},
		{"one movable Instance left under a partition", ComponentObservation{RolloutInFlight: true, SpecReplicas: 12, Partition: 4, NewRevisionPods: 7}, true},
		{"spec not yet observed, rows count", ComponentObservation{RolloutInFlight: true, DesiredReplicas: 3, NewRevisionPods: 2}, true},
		{"scaled below its promoted count", ComponentObservation{RolloutInFlight: true, SpecReplicas: 6, NewRevisionPods: 7}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.obs.startsRemain(); got != tc.want {
				t.Errorf("startsRemain=%v, want %v", got, tc.want)
			}
		})
	}
}
