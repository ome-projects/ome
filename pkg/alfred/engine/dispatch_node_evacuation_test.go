package engine

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/policy/nodehealth"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestFreshReplayPreservesEvacuationCause(t *testing.T) {
	d, _, snap, original, cfg, _ := dispatchFixture(t, false)
	original.Policy, original.Reason = "nodehealth", policy.ReasonNodeMaintenance
	other := original
	other.Policy, other.Reason = "defragmentation", policy.ReasonFragmentation
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{other, original}}}
	got, ok := d.freshCandidate(snap, original, cfg)
	if !ok || got.Policy != original.Policy || got.Reason != original.Reason {
		t.Fatalf("fresh replay changed maintenance authorization: %+v", got)
	}
	d.Policies = []policy.Policy{&stubPolicy{out: []policy.Candidate{other}}}
	if _, ok := d.freshCandidate(snap, original, cfg); ok {
		t.Fatal("another policy must not authorize a withdrawn maintenance request")
	}
}

func TestJournalRestoresEvacuationCause(t *testing.T) {
	for _, reason := range []string{policy.ReasonNodeMaintenance, policy.ReasonNodeUnhealthy, policy.ReasonFragmentation} {
		t.Run(reason, func(t *testing.T) {
			_, _, _, c, _, _ := dispatchFixture(t, false)
			c.Reason, c.Policy = reason, "nodehealth"
			if reason == policy.ReasonFragmentation {
				c.Policy = "defragmentation"
			}
			e, err := newDispatchEntry(c, "owner", "a-engine", "ir", "fingerprint", testNow)
			if err != nil {
				t.Fatal(err)
			}
			got := entryCandidate(e)
			if got.Policy != c.Policy || got.Reason != reason {
				t.Fatalf("journal lost original policy/reason: %+v", got)
			}
		})
	}
}

func TestDispatcherObservationUsesConfiguredNodeExclusions(t *testing.T) {
	d, cl, _, _, cfg, _ := dispatchFixture(t, false)
	var target corev1.Node
	if err := cl.Client.Get(context.Background(), client.ObjectKey{Name: "target"}, &target); err != nil {
		t.Fatal(err)
	}
	target.Labels["maintenance.example.com/state"] = "patching"
	target.Status.Conditions = append(target.Status.Conditions, corev1.NodeCondition{
		Type: "GpuUnhealthy", Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(testNow.Add(-35 * time.Minute)),
	})
	status := target.Status
	if err := cl.Client.Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	target.Status = status
	if err := cl.Client.Status().Update(context.Background(), &target); err != nil {
		t.Fatal(err)
	}
	cfg.Policies.NodeHealth.NodeSuspicionWindowMinutes = 60
	value := "patching"
	cfg.Policies.NodeHealth.Maintenance.Triggers = []config.MaintenanceTrigger{{Name: "patching", Label: &config.MaintenanceLabel{Key: "maintenance.example.com/state", Value: &value}}}
	fresh, err := d.freshObservation(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := fresh.Nodes["target"]
	if got.Health.State != snapshot.NodeHealthSuspect || !got.Maintenance.Requested || !got.UnavailableAsTarget() {
		t.Fatalf("dispatcher lost configured health/maintenance evidence: %+v", got)
	}
}

func TestDispatcherSubmitsRealNodeEvacuation(t *testing.T) {
	for _, cause := range []string{"unhealthy", "label", "taint", "condition"} {
		for _, gang := range []bool{false, true} {
			t.Run(cause+map[bool]string{false: "/single", true: "/gang"}[gang], func(t *testing.T) {
				d, cl, _, _, cfg, arbiter := dispatchFixture(t, gang)
				var ir v1beta1.InferenceReplica
				if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
					t.Fatal(err)
				}
				ir.Status.InstanceStatuses[0].TargetRevision = ""
				ir.Status.Replicas, ir.Status.ReadyReplicas, ir.Status.ServingReplicas = 1, 1, 1
				ir.Status.AvailableReplicas, ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = 1, 1, 1
				if err := cl.Client.Update(context.Background(), &ir); err != nil {
					t.Fatal(err)
				}
				var node corev1.Node
				if err := cl.Client.Get(context.Background(), client.ObjectKey{Name: "source"}, &node); err != nil {
					t.Fatal(err)
				}
				wantReason := policy.ReasonNodeMaintenance
				trigger := config.MaintenanceTrigger{Name: "patching"}
				switch cause {
				case "unhealthy":
					wantReason = policy.ReasonNodeUnhealthy
					node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "GpuUnhealthy", Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(testNow)})
				case "label":
					node.Labels["maintenance.example.com/state"] = "patching"
					value := "patching"
					trigger.Label = &config.MaintenanceLabel{Key: "maintenance.example.com/state", Value: &value}
				case "taint":
					node.Spec.Taints = []corev1.Taint{{Key: "maintenance.example.com/patching", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
					trigger.Taint = &config.MaintenanceTaint{Key: "maintenance.example.com/patching", Effect: corev1.TaintEffectNoSchedule}
				case "condition":
					node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "Patching", Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(testNow)})
					trigger.Condition = &config.MaintenanceCondition{Type: "Patching", Status: corev1.ConditionTrue}
				}
				if cause != "unhealthy" {
					cfg.Policies.NodeHealth.Maintenance.Triggers = []config.MaintenanceTrigger{trigger}
				}
				status := node.Status
				if err := cl.Client.Update(context.Background(), &node); err != nil {
					t.Fatal(err)
				}
				node.Status = status
				if err := cl.Client.Status().Update(context.Background(), &node); err != nil {
					t.Fatal(err)
				}
				d.Policies = []policy.Policy{&nodehealth.Policy{}, &defrag.Policy{}}
				observed, err := d.freshObservation(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				if cause != "unhealthy" && observed.Nodes["source"].Health.State != snapshot.NodeHealthClear {
					t.Fatal("healthy maintenance node was marked unhealthy")
				}
				candidates := d.Policies[0].Evaluate(observed, cfg)
				var moves []policy.Candidate
				for _, c := range candidates {
					if c.Remediation == nil {
						moves = append(moves, c)
					}
				}
				if len(moves) != 1 || !moves[0].Executable || moves[0].Reason != wantReason {
					t.Fatalf("expected one complete-instance move: %+v", moves)
				}
				d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
					wantMembers := 1
					if gang {
						wantMembers = 2
					}
					if len(r.ReplacementPods) != wantMembers || len(r.ExcludedNodes) != 1 || r.ExcludedNodes[0] != "source" {
						t.Fatalf("simulation diverged from whole-instance v1 contract: %+v", r)
					}
					return feasiblePrediction(r), nil
				})
				_, decisions := d.Execute(context.Background(), observed, candidates, cfg, arbiter)
				got := decisionFor(t, decisions, "prod/a")
				if got.DispatchStatus != "submitted" || cl.patches != 1 || got.Candidate.Reason != wantReason {
					t.Fatalf("evacuation was not submitted exactly once: %+v patches=%d", got, cl.patches)
				}
				_, journal, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
				if err != nil || len(journal.Entries) != 1 {
					t.Fatalf("missing durable request: %+v %v", journal, err)
				}
				req, err := requestForEntry(journal.Entries[0])
				if err != nil || req.Reason != wantReason || req.FromNode != "source" {
					t.Fatalf("wrong migration API payload: %+v %v", req, err)
				}
			})
		}
	}
}
