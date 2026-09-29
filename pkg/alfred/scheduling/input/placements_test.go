package input

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

// Catches accidentally joining by array order, omitting gang/CPU members, or
// accepting a replacement identity/resource shape that is not the source's.
func TestSourcePlacementTargets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*scheduling.Request, *scheduling.Result)
		valid  bool
	}{
		{name: "complete shuffled", valid: true},
		{name: "missing placement", change: func(_ *scheduling.Request, r *scheduling.Result) { r.Placements = r.Placements[:1] }},
		{name: "duplicate placement", change: func(_ *scheduling.Request, r *scheduling.Result) { r.Placements[1] = r.Placements[0] }},
		{name: "wrong UID", change: func(_ *scheduling.Request, r *scheduling.Result) { r.Placements[0].Pod.UID = "other" }},
		{name: "missing source", change: func(r *scheduling.Request, _ *scheduling.Result) { r.SourcePods = r.SourcePods[:1] }},
		{name: "duplicate member", change: func(r *scheduling.Request, _ *scheduling.Result) {
			r.ReplacementPods[1].Labels[labelRunner] = r.ReplacementPods[0].Labels[labelRunner]
			r.ReplacementPods[1].Labels[labelPodOrdinal] = r.ReplacementPods[0].Labels[labelPodOrdinal]
		}},
		{name: "changed resources", change: func(r *scheduling.Request, _ *scheduling.Result) {
			r.ReplacementPods[0].Spec.Containers[0].Resources.Requests = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects, source := validGangSourceObjects()
			r, err := BuildRequest(captureSourceFixture(t, objects), source, testProfiles(true), "placement", captureTime, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			want := map[types.NamespacedName]string{
				{Namespace: r.SourcePods[0].Namespace, Name: r.SourcePods[0].Name}: "target-a",
				{Namespace: r.SourcePods[1].Namespace, Name: r.SourcePods[1].Name}: "target-b",
			}
			result := scheduling.Result{SchemaVersion: r.SchemaVersion, RequestID: r.RequestID, SnapshotID: r.SnapshotID,
				SnapshotTime: r.SnapshotTime, Profile: r.Profile, Decision: scheduling.DecisionFeasible, Reason: scheduling.SimulationReasonPlacementFound}
			for i, target := range []string{"target-a", "target-b"} {
				p := r.ReplacementPods[i]
				result.Placements = append(result.Placements, scheduling.Placement{Pod: scheduling.PodIdentity{Namespace: p.Namespace, Name: p.Name, UID: p.UID}, NodeName: target})
			}
			// Shuffle independently, so no two parallel-array assumptions survive.
			r.SourcePods[0], r.SourcePods[1] = r.SourcePods[1], r.SourcePods[0]
			result.Placements[0], result.Placements[1] = result.Placements[1], result.Placements[0]
			if tc.change != nil {
				tc.change(&r, &result)
			}
			got, err := SourcePlacementTargets(r, result)
			if (err == nil) != tc.valid {
				t.Fatalf("targets=%v error=%v, want valid=%t", got, err, tc.valid)
			}
			if tc.valid {
				if len(got) != 2 {
					t.Fatalf("incomplete source mapping: %v", got)
				}
				for key, target := range want {
					if got[key] != target {
						t.Fatalf("source %s target=%q, want %q", key, got[key], target)
					}
				}
			}
		})
	}
}
