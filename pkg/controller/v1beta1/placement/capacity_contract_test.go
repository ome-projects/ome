package placement

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestCapacityPlanRejectsUnboundDemand(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*capacity.Sample)
	}{
		{name: "missing contract", edit: func(s *capacity.Sample) { s.DemandContract = nil }},
		{name: "different demand fingerprint", edit: func(s *capacity.Sample) { s.DemandContract.Fingerprint = strings.Repeat("c", 64) }},
		{name: "missing engine", edit: func(s *capacity.Sample) { s.DemandContract.Components = nil }},
		{name: "undeclared decoder", edit: func(s *capacity.Sample) { s.DemandContract.Components[0].Component = v1beta1.DecoderComponent }},
		{name: "missing pool evidence", edit: func(s *capacity.Sample) { s.Pools = nil }},
		{name: "weight inconsistent with hardware", edit: func(s *capacity.Sample) { s.Weight++ }},
		{name: "different registration", edit: func(s *capacity.Sample) { s.ClusterUID = "replacement" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCSplit("", 3)
			source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
			cluster := wc("member-a", true, nil)
			sample := plannedCapacitySample(cluster, 4)
			tt.edit(&sample)
			before := source.DeepCopy()
			got, err := desiredSplitPlan(source, []v1beta1.WorkloadCluster{cluster}, map[string]capacity.Sample{cluster.Name: sample})
			if err == nil || !strings.Contains(err.Error(), "CapacityUnknown") {
				t.Fatalf("expected capacity hold: %v", err)
			}
			if diff := cmp.Diff(plan.Proposal{}, got); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestCapacityPlanRetainsOutgoingContract(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSplitByCapacity, v1beta1.PlacementModeSplit} {
		t.Run(string(mode), func(t *testing.T) {
			source := plannedTestSource()
			source.Spec.Placement.Mode = mode
			outgoing := source.Status.Placement.Candidates[0].Allocation
			oldSample := plannedCapacitySample(*plannedTestRegistration(), 4)
			oldSample.DemandContract.Components = append(oldSample.DemandContract.Components, v1beta1.PlacementComponentDemand{Component: v1beta1.DecoderComponent, RenderingHash: strings.Repeat("c", 64)})
			var err error
			outgoing.Capacity, err = capacityEvidence(oldSample)
			if err != nil {
				t.Fatal(err)
			}
			cluster := wc("member-b", true, nil)
			sample := plannedCapacitySample(cluster, 8)
			before := source.DeepCopy()
			got, err := desiredSplitPlan(source, []v1beta1.WorkloadCluster{cluster}, map[string]capacity.Sample{cluster.Name: sample})
			if err != nil {
				t.Fatal(err)
			}
			retained := got.Assignments["member-a"]
			want := outgoing.DeepCopy()
			want.DesiredReplicas = 0
			if mode == v1beta1.PlacementModeSplit {
				want.Capacity = nil
			}
			if diff := cmp.Diff(*want, retained); diff != "" {
				t.Fatal(diff)
			}
			if mode == v1beta1.PlacementModeSplitByCapacity {
				if diff := cmp.Diff(sample.DemandContract, got.Assignments["member-b"].Capacity.DemandContract); diff != "" {
					t.Fatal(diff)
				}
				got.Assignments["member-b"].Capacity.DemandContract.Components[0].RenderingHash = "changed"
				retained.Capacity.DemandContract.Components[0].RenderingHash = "changed"
				if diff := cmp.Diff(plannedCapacitySample(cluster, 8), sample); diff != "" {
					t.Fatalf("sample aliased: %s", diff)
				}
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatalf("source aliased: %s", diff)
			}
		})
	}
}

func attachCapacityContract(t *testing.T, source *v1beta1.InferenceService) {
	t.Helper()
	source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
	sample := plannedCapacitySample(*plannedTestRegistration(), 4)
	evidence, err := capacityEvidence(sample)
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement.Candidates[0].Allocation.Capacity = evidence
}

func TestPlannedMutationsRecheckCompleteAuthority(t *testing.T) {
	for _, action := range []struct {
		name string
		run  func(*Reconciler, *v1beta1.InferenceService, v1beta1.CandidatePlacement) error
	}{
		{name: "apply", run: func(r *Reconciler, s *v1beta1.InferenceService, c v1beta1.CandidatePlacement) error {
			return r.placePlannedOn(t.Context(), s, c)
		}},
		{name: "pause", run: func(r *Reconciler, s *v1beta1.InferenceService, c v1beta1.CandidatePlacement) error {
			return r.syncPlannedPolicy(t.Context(), s, c)
		}},
		{name: "delete", run: func(r *Reconciler, s *v1beta1.InferenceService, c v1beta1.CandidatePlacement) error {
			return r.deletePlannedOn(t.Context(), s, c, true)
		}},
	} {
		t.Run(action.name, func(t *testing.T) {
			for _, tt := range []struct {
				name string
				read int
				edit func(*v1beta1.InferenceService)
			}{
				{name: "contract pruned", read: 1, edit: func(s *v1beta1.InferenceService) {
					s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract = nil
				}},
				{name: "contract pruned before write", read: 2, edit: func(s *v1beta1.InferenceService) {
					s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract = nil
				}},
				{name: "rendering changed before write", read: 2, edit: func(s *v1beta1.InferenceService) {
					s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract.Components[0].RenderingHash = strings.Repeat("c", 64)
				}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					source := plannedTestSource()
					attachCapacityContract(t, source)
					candidate := &source.Status.Placement.Candidates[0]
					if action.name == "delete" {
						candidate.Allocation.CurrentReplicas = 0
						candidate.Allocation.DesiredReplicas = 0
						candidate.Allocation.DrainRequested = true
					}
					member := DeriveISVC(source, "", "")
					member.UID = "member-service"
					worker := emptyWorker(testScheme(t))
					if err := worker.Create(t.Context(), member); err != nil {
						t.Fatal(err)
					}
					before := member.DeepCopy()
					var config *CapacityConfig
					if action.name == "apply" {
						config = configureContractApplication(t, source, worker)
					}
					connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: candidate.Allocation.ClusterUID}
					r, _ := newPlacer(testScheme(t), connections, source, plannedTestRegistration())
					r.Capacity = config
					r.MemberOperatorNamespace = "operator-system"
					reads := 0
					r.APIReader = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(source, plannedTestRegistration()).WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if err := c.Get(ctx, key, obj, opts...); err != nil {
							return err
						}
						if live, ok := obj.(*v1beta1.InferenceService); ok {
							reads++
							if reads >= tt.read {
								tt.edit(live)
							}
						}
						return nil
					}}).Build()
					err := action.run(r, source, *candidate)
					if !errors.Is(err, plan.ErrSchemaUnsupported) {
						t.Fatalf("want storage authority error, got %v", err)
					}
					stored := &v1beta1.InferenceService{}
					if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), stored); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(before, stored); diff != "" {
						t.Fatalf("rejected mutation changed member: %s", diff)
					}
				})
			}
		})
	}
}

func TestPlannedContractTransport(t *testing.T) {
	for _, tt := range []struct {
		name     string
		outgoing bool
	}{{name: "matched member"}, {name: "outgoing member", outgoing: true}} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			attachCapacityContract(t, source)
			candidate := source.Status.Placement.Candidates[0]
			member := DeriveISVC(source, "", "")
			member.UID = "member-service"
			if tt.outgoing {
				candidate.Allocation.DesiredReplicas = 0
				candidate.Allocation.Capacity.DemandContract.Components = append(candidate.Allocation.Capacity.DemandContract.Components, v1beta1.PlacementComponentDemand{Component: v1beta1.DecoderComponent, RenderingHash: strings.Repeat("c", 64)})
				member.Spec.Decoder = &v1beta1.DecoderSpec{}
			}
			worker := emptyWorker(testScheme(t))
			if err := worker.Create(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			before := member.DeepCopy()
			var config *CapacityConfig
			if !tt.outgoing {
				config = configureContractApplication(t, source, worker)
			}
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: candidate.Allocation.ClusterUID}
			r, _ := newPlacer(testScheme(t), connections, source, plannedTestRegistration())
			r.Capacity = config
			r.MemberOperatorNamespace = "operator-system"
			var err error
			if tt.outgoing {
				err = r.syncPlannedPolicy(t.Context(), source, candidate)
			} else {
				err = r.placePlannedOn(t.Context(), source, candidate)
			}
			if err != nil {
				t.Fatal(err)
			}
			stored := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), stored); err != nil {
				t.Fatal(err)
			}
			got, err := protocol.FromDerived(stored)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(candidate.Allocation.Capacity.DemandContract, got.Demand); diff != "" {
				t.Fatal(diff)
			}
			if tt.outgoing {
				delete(stored.Annotations, constants.PlacementExecution)
				stored.ResourceVersion = before.ResourceVersion
				if diff := cmp.Diff(before, stored); diff != "" {
					t.Fatalf("outgoing content changed: %s", diff)
				}
			}
			policy := executionPolicy(source, candidate.Allocation)
			policy.Demand.Components[0].RenderingHash = "changed"
			if diff := cmp.Diff(candidate.Allocation.Capacity.DemandContract, got.Demand); diff != "" {
				t.Fatalf("policy aliases source: %s", diff)
			}
		})
	}
}
