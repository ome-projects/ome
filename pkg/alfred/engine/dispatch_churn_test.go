package engine

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/process"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Harmless observation updates must not starve dispatch, while changes to the
// simulated occupancy or scheduling rules must still invalidate the result.
func TestDispatcherRevalidatesSchedulingStateDuringChurn(t *testing.T) {
	for _, tc := range []struct {
		name         string
		pod          func(*corev1.Pod)
		node         func(*corev1.Node)
		source       bool
		newPending   bool
		realWorker   bool
		gang         bool
		wantDispatch bool
	}{
		{name: "pod resource version", pod: func(*corev1.Pod) {}, wantDispatch: true},
		{name: "pod managed fields", pod: func(p *corev1.Pod) {
			p.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "status-observer", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1", FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:status":{}}`)}}}
		}, wantDispatch: true},
		{name: "node heartbeat", node: func(n *corev1.Node) {
			n.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(testNow.Add(time.Second))
		}, wantDispatch: true},
		{name: "node heartbeat real worker", node: func(n *corev1.Node) {
			n.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(testNow.Add(time.Second))
		}, realWorker: true, wantDispatch: true},
		{name: "new pending competitor", newPending: true},
		{name: "new pending competitor real worker", newPending: true, realWorker: true},
		{name: "node heartbeat real gang worker", node: func(n *corev1.Node) {
			n.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(testNow.Add(time.Second))
		}, realWorker: true, gang: true, wantDispatch: true},
		{name: "new pending competitor real gang worker", newPending: true, realWorker: true, gang: true},
		{name: "pod binding", pod: func(p *corev1.Pod) { p.Spec.NodeName = "target" }},
		{name: "pod resources", pod: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("2")
		}},
		{name: "pod spec", pod: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "example.invalid/changed:v2" }},
		{name: "pod affinity", pod: func(p *corev1.Pod) {
			p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelHostname, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"ome.io/inferenceservice": "a"}}}}}}
		}},
		{name: "pod labels", pod: func(p *corev1.Pod) { p.Labels = map[string]string{"app": "changed"} }},
		{name: "pod annotations", pod: func(p *corev1.Pod) { p.Annotations = map[string]string{"example.com/input": "changed"} }},
		{name: "source identity", source: true, pod: func(p *corev1.Pod) { p.UID = "replacement-source-uid" }},
		{name: "node transition time", node: func(n *corev1.Node) { n.Status.Conditions[0].LastTransitionTime = metav1.NewTime(testNow) }},
		{name: "node condition reason", node: func(n *corev1.Node) { n.Status.Conditions[0].Reason = "ChangedReason" }},
		{name: "unhealthy target", node: func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionFalse }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			d, cl, observed, candidate, cfg, arbiter := dispatchFixture(t, tc.gang)
			backend := d.Simulator
			if tc.realWorker {
				backend, cfg.Scheduling = churnWorkerSimulator(t, tc.gang)
			}
			unrelated := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "unrelated", UID: "unrelated-uid"},
				Spec: corev1.PodSpec{NodeName: "source", SchedulerName: "default-scheduler", Containers: []corev1.Container{{Name: "other", Image: "example.invalid/other:v1",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			if tc.gang {
				unrelated.Spec.SchedulerName = "ome-scheduler"
			}
			if err := cl.Client.Create(ctx, unrelated); err != nil {
				t.Fatal(err)
			}
			mutated := false
			d.Simulator = simulationFunc(func(ctx context.Context, request scheduling.Request) (scheduling.Result, error) {
				if tc.gang && (!request.RequireGang || len(request.SourcePods) != 2 || len(request.ReplacementPods) != 2) {
					t.Fatalf("churn test did not simulate the complete replacement gang: %+v", request)
				}
				if tc.newPending {
					pending := unrelated.DeepCopy()
					pending.Name, pending.UID, pending.ResourceVersion = "new-competitor", "new-competitor-uid", ""
					pending.Spec.NodeName, pending.Status.Phase = "", corev1.PodPending
					if err := cl.Client.Create(ctx, pending); err != nil {
						t.Fatal(err)
					}
				}
				if tc.pod != nil {
					key := client.ObjectKey{Namespace: "prod", Name: "unrelated"}
					if tc.source {
						key.Name = "source-pod"
					}
					var pod corev1.Pod
					if err := cl.Client.Get(ctx, key, &pod); err != nil {
						t.Fatal(err)
					}
					before := pod.ResourceVersion
					tc.pod(&pod)
					if err := cl.Client.Update(ctx, &pod); err != nil {
						t.Fatal(err)
					}
					if pod.ResourceVersion == before {
						t.Fatal("fixture did not advance the Pod resource version")
					}
				}
				if tc.node != nil {
					var node corev1.Node
					if err := cl.Client.Get(ctx, client.ObjectKey{Name: "target"}, &node); err != nil {
						t.Fatal(err)
					}
					tc.node(&node)
					if err := cl.Client.Status().Update(ctx, &node); err != nil {
						t.Fatal(err)
					}
				}
				mutated = true
				result, err := backend.Evaluate(ctx, request)
				if err != nil || result.Decision != scheduling.DecisionFeasible {
					t.Fatalf("fixture must reach revalidation with a feasible simulation: %+v, %v", result, err)
				}
				return result, nil
			})
			_, decisions := d.Execute(ctx, observed, []policy.Candidate{candidate}, cfg, arbiter)
			decision := decisionFor(t, decisions, "prod/a")
			if !mutated {
				t.Fatalf("fixture never reached the change during simulation: %+v", decision)
			}
			var owner v1beta1.InferenceService
			if err := cl.Client.Get(ctx, candidate.Workload, &owner); err != nil {
				t.Fatal(err)
			}
			annotations := map[string]string{}
			for key, value := range owner.Annotations {
				if strings.HasPrefix(key, "ome.io/migration-request-v1-") {
					annotations[key] = value
				}
			}
			_, journal, err := loadDispatchJournal(ctx, cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantDispatch {
				if cl.patches != 0 || len(annotations) != 0 || len(journal.Entries) != 0 {
					t.Fatalf("changed scheduling state authorized migration: decision=%+v annotations=%v journal=%+v", decision, annotations, journal)
				}
				return
			}
			if decision.DispatchStatus != "submitted" || cl.patches != 1 || len(annotations) != 1 || len(journal.Entries) != 1 {
				t.Fatalf("harmless churn prevented one durable submission: decision=%+v patches=%d annotations=%v journal=%+v", decision, cl.patches, annotations, journal)
			}
			entry := journal.Entries[0]
			payload := annotations["ome.io/migration-request-v1-"+entry.UUID]
			var migration migrationRequest
			if err := json.Unmarshal([]byte(payload), &migration); err != nil {
				t.Fatal(err)
			}
			if entry.Phase != "submitted" || entry.LastAttempt == nil || entry.Payload != payload || decision.RequestUUID != entry.UUID ||
				migration.SchemaVersion != "v1" || migration.Component != "engine" || migration.Instance != 0 || migration.FromNode != "source" || migration.RequestedBy != "alfred" {
				t.Fatalf("submission lost durable migration identity: entry=%+v payload=%s", entry, payload)
			}
		})
	}
}

func churnWorkerSimulator(t *testing.T, gang bool) (scheduling.Simulator, scheduling.Config) {
	t.Helper()
	binary := os.Getenv("ALFRED_SIMULATOR_BINARY")
	if binary == "" {
		t.Skip("set ALFRED_SIMULATOR_BINARY to exercise churn with the compiled worker")
	}
	configName := "default-scheduler.yaml"
	if gang {
		configName = "ome-scheduler.yaml"
	}
	configPath, err := filepath.Abs(filepath.Join("..", "simulator", "examples", configName))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--backend", "test-dispatch-churn", "--scheduler-config", configPath, "--print-profile")
	cmd.Env = []string{}
	encoded, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var printed struct {
		Identity       scheduling.ProfileIdentity `json:"identity"`
		GangScheduling bool                       `json:"gangScheduling"`
	}
	if err := json.Unmarshal(encoded, &printed); err != nil {
		t.Fatal(err)
	}
	registry, err := process.NewRegistry(ctx, []process.Backend{{BinaryPath: binary, SchedulerConfigPath: configPath, Identity: printed.Identity, GangScheduling: printed.GangScheduling}}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return registry, scheduling.Config{Profiles: map[string]scheduling.Profile{printed.Identity.SchedulerName: {
		Backend: printed.Identity.Backend, SchedulerVersion: printed.Identity.SchedulerVersion,
		ConfigurationID: printed.Identity.ConfigurationID, GangScheduling: printed.GangScheduling,
	}}}
}
