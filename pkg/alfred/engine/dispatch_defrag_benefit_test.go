package engine

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The arithmetic plan puts one GPU into target's one-GPU hole, but a source
// selector can force the real scheduler onto an empty node instead. That only
// exchanges the seven- and eight-GPU holes and buys no additional capacity.
func TestDispatcherRequiresBenefitFromRealSchedulerPlacement(t *testing.T) {
	for _, gang := range []bool{false, true} {
		for _, scenario := range []string{"feasible but no benefit", "useful placement", "prepared retry", "skewed observation", "namespace churn", "lower accepted benefit"} {
			if gang && scenario == "lower accepted benefit" {
				continue // The hand-derived unequal-benefit fixture has one member.
			}
			t.Run(fmt.Sprintf("%s/gang=%t", scenario, gang), func(t *testing.T) {
				useful := scenario == "useful placement" || scenario == "prepared retry" || scenario == "namespace churn"
				ctx := context.Background()
				d, cl, _, _, cfg, arbiter := dispatchFixture(t, gang)
				backend, profiles := churnWorkerSimulator(t, gang)
				cfg.Scheduling = profiles
				constraints := func(spec *corev1.PodSpec) {
					spec.NodeSelector = map[string]string{"alfred.test/placement": "allowed"}
					if gang {
						if spec.Affinity == nil {
							spec.Affinity = &corev1.Affinity{}
						}
						// Gang admission alone does not require distinct nodes. Make
						// that placement constraint explicit in both stable templates
						// and observed Pods, preserving the worker's zone affinity.
						spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
							TopologyKey:   corev1.LabelHostname,
							LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"ome.io/inferenceservice": "a", "component": "engine"}},
						}}}
					}
				}
				var ir v1beta1.InferenceReplica
				if err := cl.Client.Get(ctx, types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
					t.Fatal(err)
				}
				ir.Status.InstanceStatuses[0].TargetRevision = ""
				ir.Status.Replicas, ir.Status.ReadyReplicas, ir.Status.ServingReplicas = 1, 1, 1
				ir.Status.AvailableReplicas, ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = 1, 1, 1
				for i := range ir.Spec.Runners {
					constraints(&ir.Spec.Runners[i].Template.Spec)
				}
				if err := cl.Client.Update(ctx, &ir); err != nil {
					t.Fatal(err)
				}
				sourceNames := []string{"source-pod"}
				if gang {
					sourceNames = append(sourceNames, "source-worker")
				}
				for _, name := range sourceNames {
					var source corev1.Pod
					if err := cl.Client.Get(ctx, client.ObjectKey{Namespace: "prod", Name: name}, &source); err != nil {
						t.Fatal(err)
					}
					constraints(&source.Spec)
					if err := cl.Client.Update(ctx, &source); err != nil {
						t.Fatal(err)
					}
				}
				var alternate corev1.Node
				if err := cl.Client.Get(ctx, client.ObjectKey{Name: "target"}, &alternate); err != nil {
					t.Fatal(err)
				}
				alternate.Name, alternate.UID, alternate.ResourceVersion = "alternate", "alternate", ""
				alternate.Labels[corev1.LabelHostname] = alternate.Name
				if err := cl.Client.Create(ctx, &alternate); err != nil {
					t.Fatal(err)
				}
				nodeNames := []string{"source", "target", "alternate"}
				if gang {
					nodeNames = append(nodeNames, "source-worker", "target-worker")
				}
				for _, name := range nodeNames {
					var node corev1.Node
					if err := cl.Client.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
						t.Fatal(err)
					}
					node.Labels["alfred.test/placement"] = "excluded"
					if name == "source" || name == "source-worker" || name == "target-worker" || name == "target" && useful || name == "alternate" && !useful {
						node.Labels["alfred.test/placement"] = "allowed"
					}
					if err := cl.Client.Update(ctx, &node); err != nil {
						t.Fatal(err)
					}
				}
				occupant := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "other-occupant", UID: "other-occupant"},
					Spec: corev1.PodSpec{NodeName: "target", SchedulerName: "default-scheduler", Containers: []corev1.Container{{Name: "other", Image: "example.invalid/other:v1",
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("7")}}}}},
					Status: corev1.PodStatus{Phase: corev1.PodRunning}}
				if err := cl.Client.Create(ctx, occupant); err != nil {
					t.Fatal(err)
				}
				*cfg.Policies.Defragmentation.FragmentationThreshold = 0.1
				cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{8}
				cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"8": 1}
				*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 0
				if scenario == "lower accepted benefit" {
					extra := occupant.DeepCopy()
					extra.Name, extra.UID, extra.ResourceVersion = "alternate-occupant", "alternate-occupant", ""
					extra.Spec.NodeName = "alternate"
					extra.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("4")
					if err := cl.Client.Create(ctx, extra); err != nil {
						t.Fatal(err)
					}
					cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{4, 8}
					cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"4": 0.5, "8": 0.5}
					*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 1
				}
				d.Policies = []policy.Policy{&defrag.Policy{}}
				observed, err := d.freshObservation(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				candidates := d.Policies[0].Evaluate(observed, cfg)
				wantBenefit := 0.5
				if gang {
					wantBenefit = 8.0 / 31.0
				}
				if len(candidates) != 1 || !candidates[0].Executable || math.Abs(candidates[0].Benefit-wantBenefit) > 1e-9 || candidates[0].Score <= 0 {
					t.Fatalf("fixture must have positive arithmetic benefit before real simulation: %+v", candidates)
				}
				diagnostic := &policy.SchedulingDiagnostics{Status: "Feasible", Reason: "PlacementFound", SnapshotID: "advisory-only"}
				candidates[0].Scheduling = diagnostic
				calls := 0
				var skew *defragSkewReader
				if scenario == "skewed observation" {
					skew = &defragSkewReader{Reader: d.Reader}
					d.Reader = skew
				}
				d.Simulator = simulationFunc(func(ctx context.Context, request scheduling.Request) (scheduling.Result, error) {
					calls++
					result, err := backend.Evaluate(ctx, request)
					want := "alternate"
					if useful {
						want = "target"
					}
					wantNodes := map[string]bool{want: true}
					if gang {
						wantNodes["target-worker"] = true
					}
					gotNodes := map[string]bool{}
					for _, placement := range result.Placements {
						gotNodes[placement.NodeName] = true
					}
					if err != nil || scheduling.ValidateResult(request, result) != nil || result.Decision != scheduling.DecisionFeasible ||
						request.RequireGang != gang || len(request.SourcePods) != len(sourceNames) || len(request.ReplacementPods) != len(sourceNames) ||
						len(result.Placements) != len(sourceNames) || !reflect.DeepEqual(gotNodes, wantNodes) {
						t.Fatalf("real worker must prove complete feasible placement on %v: %+v, %v", wantNodes, result, err)
					}
					if scenario == "namespace churn" {
						var namespace corev1.Namespace
						if err := cl.Client.Get(ctx, client.ObjectKey{Name: "prod"}, &namespace); err != nil {
							t.Fatal(err)
						}
						namespace.Annotations = map[string]string{"test/diagnostic": "updated"}
						if err := cl.Client.Update(ctx, &namespace); err != nil {
							t.Fatal(err)
						}
					}
					return result, nil
				})
				cl.failBeforeApply = scenario == "prepared retry"
				out, decisions := d.Execute(ctx, observed, candidates, cfg, arbiter)
				decision := decisionFor(t, decisions, "prod/a")
				_, journal, err := loadDispatchJournal(ctx, cl.Client, "ome")
				if err != nil || calls != 1 {
					t.Fatalf("invalid acceptance path: calls=%d journal error=%v", calls, err)
				}
				if scenario == "prepared retry" {
					before := onlyRetryEntry(t, cl.Client)
					if before.Phase != dispatchPrepared || before.LastAttempt == nil || cl.patches != 1 {
						t.Fatalf("failed submission did not retain intent: %+v", before)
					}
					cl.failBeforeApply = false
					for _, restored := range []bool{false, true} {
						useful = restored
						for _, name := range []string{"target", "alternate"} {
							var node corev1.Node
							if err := cl.Client.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
								t.Fatal(err)
							}
							node.Labels["alfred.test/placement"] = "excluded"
							if name == "target" && restored || name == "alternate" && !restored {
								node.Labels["alfred.test/placement"] = "allowed"
							}
							if err := cl.Client.Update(ctx, &node); err != nil {
								t.Fatal(err)
							}
						}
						retryOut, retryDecisions := d.Execute(ctx, observed, nil, cfg, &Arbiter{Ledger: NewLedger()})
						retryDecision := decisionFor(t, retryDecisions, "prod/a")
						after := onlyRetryEntry(t, cl.Client)
						if after.UUID != before.UUID || after.Payload != before.Payload || after.SourceFingerprint != before.SourceFingerprint {
							t.Fatalf("retry changed original intent: %+v", after)
						}
						if !restored {
							if calls != 2 || cl.patches != 1 || after.Phase != dispatchPrepared || !reflect.DeepEqual(before.LastAttempt, after.LastAttempt) ||
								retryDecision.DispatchStatus != "withheld" || retryDecision.DispatchReason != "PolicyNoLongerEligible" {
								t.Fatalf("nonbeneficial retry submitted or changed attempt: %+v calls=%d patches=%d", after, calls, cl.patches)
							}
						} else if calls != 3 || cl.patches != 2 || after.Phase != dispatchSubmitted || retryDecision.RequestUUID != before.UUID {
							t.Fatalf("restored benefit did not submit original intent: %+v calls=%d patches=%d", after, calls, cl.patches)
						}
						if restored {
							assertAcceptedBenefit(t, retryOut, retryDecision, wantBenefit)
						}
					}
					return
				}
				if skew != nil && skew.podLists != 4 {
					t.Fatalf("scoring read live occupancy instead of captured state: %d lists", skew.podLists)
				}
				if useful || scenario == "lower accepted benefit" {
					if decision.DispatchStatus != "submitted" || cl.patches != 1 || len(journal.Entries) != 1 {
						t.Fatalf("useful real placement was not dispatched once: %+v, %+v", decision, journal)
					}
					if scenario == "lower accepted benefit" {
						// Before [7,1,4], actual after [8,1,3]: the equal-weight
						// 4/8 ladder gains 1/3, not the arithmetic plan's 1/2.
						assertAcceptedBenefit(t, out, decision, 1.0/3.0)
						if !reflect.DeepEqual(out[0].Scheduling, diagnostic) || !reflect.DeepEqual(decision.Candidate.Scheduling, diagnostic) {
							t.Fatal("accepted score update lost separate advisory diagnostics")
						}
					}
				} else if decision.DispatchStatus != "withheld" || decision.DispatchReason != "PolicyNoLongerEligible" || cl.patches != 0 || len(journal.Entries) != 0 {
					t.Fatalf("zero-benefit placement authorized migration: decision=%+v patches=%d journal=%+v", decision, cl.patches, journal)
				}
			})
		}
	}
}

func assertAcceptedBenefit(t *testing.T, candidates []policy.Candidate, decision Decision, benefit float64) {
	t.Helper()
	if len(candidates) != 1 {
		t.Fatalf("want one accepted candidate, got %+v", candidates)
	}
	for _, c := range []policy.Candidate{candidates[0], decision.Candidate} {
		if math.Abs(c.Benefit-benefit) > 1e-9 || math.Abs(c.Score-(benefit-0.15)) > 1e-9 || c.Emergency {
			t.Fatalf("reported candidate lost accepted placement score: %+v; want benefit=%v", c, benefit)
		}
	}
}

// A policy observation between the two captures can disagree with both. It
// must not replace the accepted simulation baseline for the benefit check.
type defragSkewReader struct {
	client.Reader
	podLists int
}

func (r *defragSkewReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		r.podLists++
		if r.podLists == 1 || r.podLists == 3 {
			for i := range pods.Items {
				if pods.Items[i].Name == "other-occupant" {
					pods.Items[i].Spec.NodeName = "alternate"
				}
			}
		}
	}
	return nil
}
