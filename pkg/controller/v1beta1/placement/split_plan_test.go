package placement

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
)

func plannedCapacitySample(cluster v1beta1.WorkloadCluster, weight int64) capacity.Sample {
	return capacity.Sample{ClusterUID: cluster.UID, Weight: weight, DemandFingerprint: strings.Repeat("a", 64),
		DemandContract: &v1beta1.PlacementDemandContract{Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)}}},
		Pools: []capacity.Evidence{{ResourceName: "example.com/gpu", ResourceFlavor: "gpu-a", Demand: 2, Allocatable: weight * 2,
			ReportUID: "report-a", ReportResourceVersion: "1", ObservedAt: metav1.NewTime(time.Unix(100, 0)),
			Attribution: v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true},
		}},
	}
}

func TestDesiredSplitPlanUsesMatchedSet(t *testing.T) {
	for _, tt := range []struct {
		name       string
		mutate     func(*v1beta1.InferenceService, []v1beta1.WorkloadCluster)
		want       map[string]int32
		unassigned int32
		invalid    bool
	}{
		{name: "equal exact floor", want: map[string]int32{"member-a": 6, "member-b": 6, "member-c": 6}},
		{name: "unready home retains share", mutate: func(_ *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) {
			clusters[1].Status.Conditions[0].Status = "False"
		}, want: map[string]int32{"member-a": 6, "member-b": 6, "member-c": 6}},
		{name: "surplus admission cannot cover another share", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Status.Placement = &v1beta1.PlacementStatus{Candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", AdmittedReplicas: 12}, {Cluster: "member-b", AdmittedReplicas: 2}, {Cluster: "member-c", AdmittedReplicas: 6}}}
		}, want: map[string]int32{"member-a": 6, "member-b": 6, "member-c": 6}},
		{name: "lexical remainder and explicit zero", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.Split.Replicas = ptr.To(int32(2))
		}, want: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 0}},
		{name: "cap conflict rejects whole plan", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.Split.MaxReplicasPerCluster = 5
		}, invalid: true},
		{name: "unresolved floor", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) { s.Spec.Placement.Split.Replicas = nil }, invalid: true},
		{name: "explicit engine floor", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.Split.Replicas = nil
			s.Spec.Engine.MinReplicas = ptr.To(2)
		}, want: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 0}},
		{name: "no matches leaves floor unassigned", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.ClusterAffinity = testAffinity("accelerator=gpu-b")
		}, want: map[string]int32{}, unassigned: 18},
		{name: "unidentified registration", mutate: func(_ *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) { clusters[0].UID = "" }, invalid: true},
		{name: "capacity cannot fall back to equal", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
		}, invalid: true},
		{name: "weighted floor", mutate: func(s *v1beta1.InferenceService, _ []v1beta1.WorkloadCluster) {
			s.Spec.Placement.ClusterAffinity = nil
			for i, name := range []string{"member-a", "member-b", "member-c"} {
				s.Spec.Placement.ClusterAffinity = append(s.Spec.Placement.ClusterAffinity, v1beta1.ClusterAffinityTerm{Weight: ptr.To([]int32{3, 1, 2}[i]), MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}})
			}
		}, want: map[string]int32{"member-a": 9, "member-b": 3, "member-c": 6}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCSplit("accelerator=gpu-a", 18)
			clusters := []v1beta1.WorkloadCluster{wc("member-c", true, map[string]string{"accelerator": "gpu-a"}), wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", true, map[string]string{"accelerator": "gpu-a"})}
			if tt.mutate != nil {
				tt.mutate(source, clusters)
			}
			before := source.DeepCopy()
			got, err := desiredSplitPlan(source, clusters, nil)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("%s\n%v", diff, err)
			}
			if tt.invalid {
				return
			}
			targets := map[string]int32{}
			for name, assignment := range got.Assignments {
				targets[name] = assignment.DesiredReplicas
			}
			if diff := cmp.Diff(tt.want, targets); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.unassigned, got.UnassignedReplicas); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatalf("mutated source:\n%s", diff)
			}
		})
	}
}

func TestDesiredCapacityPlan(t *testing.T) {
	for _, tt := range []struct {
		name       string
		weights    []int64
		want       map[string]int32
		unassigned int32
	}{
		{name: "proportional hardware", weights: []int64{8, 4, 0}, want: map[string]int32{"member-a": 8, "member-b": 4, "member-c": 0}},
		{name: "request exceeds hardware", weights: []int64{2, 1, 0}, want: map[string]int32{"member-a": 8, "member-b": 4, "member-c": 0}},
		{name: "all zero capacity", weights: []int64{0, 0, 0}, want: map[string]int32{"member-a": 0, "member-b": 0, "member-c": 0}, unassigned: 12},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCSplit("", 12)
			source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
			clusters := []v1beta1.WorkloadCluster{wc("member-a", true, nil), wc("member-b", false, nil), wc("member-c", true, nil)}
			samples := map[string]capacity.Sample{}
			for i, c := range clusters {
				samples[c.Name] = plannedCapacitySample(c, tt.weights[i])
			}
			got, err := desiredSplitPlan(source, clusters, samples)
			if err != nil {
				t.Fatal(err)
			}
			targets := map[string]int32{}
			for name, a := range got.Assignments {
				targets[name] = a.DesiredReplicas
			}
			if diff := cmp.Diff(tt.want, targets); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.unassigned, got.UnassignedReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestDesiredPlanRetainsTransitionAccounting(t *testing.T) {
	for _, tt := range []struct {
		name          string
		replace       bool
		foreignSource bool
	}{
		{name: "retarget retains original and current floors"},
		{name: "replacement registration cannot inherit allocation", replace: true},
		{name: "source recreation cannot inherit allocation", foreignSource: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCSplit("accelerator=gpu-a", 6)
			source.Status.Placement = &v1beta1.PlacementStatus{
				Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID, OriginalUnassignedReplicas: 2},
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-a-uid", OriginalReplicas: 5, CurrentReplicas: 7, DesiredReplicas: 7}},
					{Cluster: "member-z", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-z-uid", OriginalReplicas: 2, CurrentReplicas: 2, DesiredReplicas: 2}},
				},
			}
			clusters := []v1beta1.WorkloadCluster{wc("member-a", true, map[string]string{"accelerator": "gpu-a"}), wc("member-b", false, map[string]string{"accelerator": "gpu-a"})}
			if tt.replace {
				clusters[0].UID = "replacement"
			}
			if tt.foreignSource {
				source.UID = "replacement"
			}
			got, err := desiredSplitPlan(source, clusters, nil)
			if diff := cmp.Diff(tt.replace || tt.foreignSource, err != nil); diff != "" {
				t.Fatalf("%s\n%v", diff, err)
			}
			if err != nil {
				return
			}
			want := map[string][3]int32{"member-a": {5, 7, 3}, "member-b": {0, 0, 3}, "member-z": {2, 2, 0}}
			floors := map[string][3]int32{}
			for name, a := range got.Assignments {
				floors[name] = [3]int32{a.OriginalReplicas, a.CurrentReplicas, a.DesiredReplicas}
			}
			if diff := cmp.Diff(want, floors); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(int32(2), got.OriginalUnassignedReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestDesiredCapacityPlanDigest(t *testing.T) {
	for _, tt := range []struct {
		name      string
		edit      func(*capacity.Sample)
		unchanged bool
	}{
		{name: "heartbeat refresh preserves desired intent", edit: func(sample *capacity.Sample) {
			for i := range sample.Pools {
				sample.Pools[i].ObservedAt = metav1.NewTime(time.Unix(200, 0))
				sample.Pools[i].ReportResourceVersion = "2"
			}
		}, unchanged: true},
		{name: "pool ordering preserves desired intent", edit: func(sample *capacity.Sample) {
			slices.Reverse(sample.Pools)
		}, unchanged: true},
		{name: "hardware changes invalidate desired intent", edit: func(sample *capacity.Sample) { sample.Pools[0].Allocatable++ }},
		{name: "flavor identity changes invalidate desired intent", edit: func(sample *capacity.Sample) { sample.Pools[0].Attribution.FlavorUID = "replacement" }},
		{name: "demand changes invalidate desired intent", edit: func(sample *capacity.Sample) {
			sample.DemandFingerprint = strings.Repeat("c", 64)
			sample.DemandContract.Fingerprint = sample.DemandFingerprint
		}},
		{name: "member rendering changes invalidate desired intent", edit: func(sample *capacity.Sample) {
			sample.DemandContract.Components[0].RenderingHash = strings.Repeat("c", 64)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCSplit("", 3)
			source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
			cluster := wc("member-a", true, nil)
			sample := plannedCapacitySample(cluster, 8)
			sample.Pools = nil
			for _, pool := range [][2]string{{"example.com/accelerator-b", "b"}, {"example.com/accelerator-a", "b"}, {"example.com/accelerator-a", "a"}} {
				sample.Pools = append(sample.Pools, capacity.Evidence{
					ResourceName: pool[0], ResourceFlavor: pool[1], Demand: 2, Allocatable: 16,
					ReportUID: "report", ReportResourceVersion: "1", ObservedAt: metav1.NewTime(time.Unix(100, 0)),
					Attribution: v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor", NodeLabels: map[string]string{"zone": "a"}, FlavorSetHash: "set", Complete: true},
				})
			}
			before, err := desiredSplitPlan(source, []v1beta1.WorkloadCluster{cluster}, map[string]capacity.Sample{cluster.Name: sample})
			if err != nil {
				t.Fatal(err)
			}
			tt.edit(&sample)
			after, err := desiredSplitPlan(source, []v1beta1.WorkloadCluster{cluster}, map[string]capacity.Sample{cluster.Name: sample})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.unchanged, before.InputDigest == after.InputDigest); diff != "" {
				t.Errorf("intent changed (-want +got):\n%s", diff)
			}
			evidence := after.Assignments[cluster.Name].Capacity
			pools := make([][2]string, 0, len(evidence.Pools))
			for _, pool := range evidence.Pools {
				pools = append(pools, [2]string{pool.ResourceName, pool.ResourceFlavor})
			}
			want := [][2]string{{"example.com/accelerator-a", "a"}, {"example.com/accelerator-a", "b"}, {"example.com/accelerator-b", "b"}}
			if diff := cmp.Diff(want, pools); diff != "" {
				t.Error(diff)
			}
			evidence.Pools[0].Attribution.NodeLabels["zone"] = "changed"
			for _, pool := range sample.Pools {
				if diff := cmp.Diff("a", pool.Attribution.NodeLabels["zone"]); diff != "" {
					t.Errorf("plan mutated sample:\n%s", diff)
				}
			}
		})
	}
}
