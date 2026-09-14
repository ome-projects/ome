package engine

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/process"
)

// CI builds the nested module first. This exercises the production loop,
// registry, lossless builder and real upstream/OME scheduling profiles.
func TestPredictionWorkerIntegration(t *testing.T) {
	binary := os.Getenv("ALFRED_SIMULATOR_BINARY")
	if binary == "" {
		t.Skip("set ALFRED_SIMULATOR_BINARY to exercise the compiled worker")
	}
	for _, tc := range []struct {
		name           string
		gang, capacity bool
	}{
		{"single_feasible", false, true}, {"single_infeasible", false, false},
		{"gang_feasible", true, true}, {"partial_gang_rejected", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, reader, c := predictionScenario(t, tc.gang)
			configName := "default-scheduler.yaml"
			if tc.gang {
				configName = "ome-scheduler.yaml"
			}
			configPath, err := filepath.Abs(filepath.Join("..", "simulator", "examples", configName))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--backend", "test-recommendation", "--scheduler-config", configPath, "--print-profile")
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
			if !tc.capacity {
				// Removing this destination leaves fewer distinct target nodes than
				// OMEGangPack requires for the complete gang.
				var node corev1.Node
				live := reader.Reader.(client.Client)
				if err := live.Get(ctx, types.NamespacedName{Name: "target"}, &node); err != nil {
					t.Fatal(err)
				}
				gpu := "0"
				node.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse(gpu)
				if err := live.Status().Update(ctx, &node); err != nil {
					t.Fatal(err)
				}
			}
			loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{out: []policy.Candidate{c}})
			profiles := scheduling.Config{Profiles: map[string]scheduling.Profile{printed.Identity.SchedulerName: {Backend: printed.Identity.Backend, SchedulerVersion: printed.Identity.SchedulerVersion, ConfigurationID: printed.Identity.ConfigurationID, GangScheduling: printed.GangScheduling}}}
			data, err := json.Marshal(map[string]any{"schemaVersion": 1, "mode": "execute", "scheduling": profiles})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := loop.Store.Update(data); err != nil {
				t.Fatal(err)
			}
			loop.Predictions = &PredictionStage{Reader: reader, Simulator: registry, Now: loop.Now}
			loop.RunOnce(ctx)
			got := readSchedulingRecommendation(t, reporter)
			if got.Outcome != OutcomeAdvisory || got.AdvisoryReason != c.AdvisoryReason || got.Scheduling == nil {
				t.Fatalf("unsafe recommendation: %+v", got)
			}
			if tc.capacity && got.Scheduling.Status != string(scheduling.DecisionFeasible) {
				t.Fatalf("expected feasible: %+v", got.Scheduling)
			}
			if tc.capacity {
				count := 1
				if tc.gang {
					count = 2
				}
				if len(got.Scheduling.Placements) != count || got.Scheduling.SnapshotID == "" || got.Scheduling.SnapshotTime == nil {
					t.Fatalf("missing complete prediction: %+v", got.Scheduling)
				}
				for _, placement := range got.Scheduling.Placements {
					if placement.NodeName != "target" && placement.NodeName != "target-worker" {
						t.Fatalf("predicted a source placement: %+v", placement)
					}
				}
			} else if len(got.Scheduling.Placements) != 0 {
				t.Fatal("negative prediction published partial placements")
			}
			if !tc.capacity && got.Scheduling.Status != string(scheduling.DecisionInfeasible) && got.Scheduling.Status != string(scheduling.DecisionUnsupported) {
				t.Fatalf("partial/insufficient capacity accepted: %+v", got.Scheduling)
			}
			if loop.Arbiter.Ledger.DispatchesWithinHour(testNow) != 0 || len(loop.Arbiter.Ledger.ActiveClaims()) != 0 {
				t.Fatal("real prediction consumed migration budgets")
			}
			var source corev1.Pod
			if err := reader.Get(ctx, types.NamespacedName{Namespace: "prod", Name: "source-pod"}, &source); err != nil {
				t.Fatal(err)
			}
			if source.UID != "pod-uid" || source.Spec.NodeName != "source" || source.DeletionTimestamp != nil {
				t.Fatal("simulation changed live source")
			}
		})
	}
}
