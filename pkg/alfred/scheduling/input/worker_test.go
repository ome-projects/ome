package input

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The real worker is a different Go module. CI supplies its built binary so
// these tests exercise Capture -> BuildRequest -> strict JSON -> real scheduler
// -> ValidateResult without importing or faking the scheduler implementation.
func TestWorkerIntegration(t *testing.T) {
	binary := os.Getenv("ALFRED_SIMULATOR_BINARY")
	if binary == "" {
		t.Skip("set ALFRED_SIMULATOR_BINARY to run the real scheduler integration")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("ALFRED_SIMULATOR_BINARY must be an absolute path")
	}
	for _, tc := range []struct {
		name      string
		gang      bool
		capacity  bool
		execution bool
	}{
		{"single_fits_elsewhere", false, true, false},
		{"single_source_occupancy_blocks_same_zone", false, false, false},
		{"whole_gang_fits", true, true, false},
		{"partial_gang_capacity_rejected", true, false, false},
		{"migration_single_fits", false, true, true},
		{"migration_whole_gang_fits", true, true, true},
		{"migration_partial_gang_rejected", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			configName := "default-scheduler.yaml"
			if tc.gang {
				objects, source = validGangSourceObjects()
				configName = "ome-scheduler.yaml"
			}
			configureWorkerObjects(objects, tc.gang, tc.capacity)
			config, err := filepath.Abs(filepath.Join("..", "..", "simulator", "examples", configName))
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"--backend", "test-predictive", "--scheduler-config", config}
			var profile struct {
				Identity       scheduling.ProfileIdentity `json:"identity"`
				GangScheduling bool                       `json:"gangScheduling"`
			}
			if err := json.Unmarshal(runWorker(t, binary, append(args, "--print-profile"), nil), &profile); err != nil {
				t.Fatal(err)
			}
			profiles := scheduling.Config{Profiles: map[string]scheduling.Profile{
				profile.Identity.SchedulerName: {
					Backend: profile.Identity.Backend, SchedulerVersion: profile.Identity.SchedulerVersion,
					ConfigurationID: profile.Identity.ConfigurationID, GangScheduling: profile.GangScheduling,
				},
			}}
			snap, err := Capture(context.Background(), captureReader(t, objects...), func() time.Time { return captureTime })
			if err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(snap)
			if err != nil {
				t.Fatal(err)
			}
			var request scheduling.Request
			if tc.execution {
				request, err = BuildExecutionRequest(snap, source, profiles, "real-worker-"+tc.name, []string{"target-a"}, captureTime.Add(time.Second), time.Minute)
			} else {
				request, err = BuildRequest(snap, source, profiles, "real-worker-"+tc.name, captureTime.Add(time.Second), time.Minute)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, sourcePod := range request.SourcePods {
				found := false
				for _, raw := range request.ClusterObjects {
					var pod corev1.Pod
					if err := json.Unmarshal(raw.Raw, &pod); err != nil {
						t.Fatal(err)
					}
					if pod.Kind == "Pod" && pod.UID == sourcePod.UID {
						found = reflect.DeepEqual(pod, sourcePod)
						break
					}
				}
				if !found {
					t.Fatal("request lost or changed source occupancy")
				}
			}
			input, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var result scheduling.Result
			if err := json.Unmarshal(runWorker(t, binary, append(args, "--timeout", "3s"), input), &result); err != nil {
				t.Fatal(err)
			}
			if result.RequestID != request.RequestID || result.SnapshotID != request.SnapshotID || result.Profile != request.Profile || !result.SnapshotTime.Equal(&request.SnapshotTime) {
				t.Fatal("worker did not preserve request identity")
			}
			if tc.capacity {
				if err := scheduling.ValidateResult(request, result); err != nil {
					t.Fatalf("feasible fixture rejected: %v; result=%+v", err, result)
				}
				wantCount := 1
				if tc.gang {
					wantCount = 2
				}
				if len(result.Placements) != wantCount {
					t.Fatalf("got %d placements, want %d", len(result.Placements), wantCount)
				}
				for _, placement := range result.Placements {
					if !strings.HasPrefix(placement.NodeName, "target-") {
						t.Fatalf("placement violated source occupancy/topology: %+v", placement)
					}
				}
			} else {
				if result.Decision != scheduling.DecisionUnsupported && result.Decision != scheduling.DecisionInfeasible {
					t.Fatalf("insufficient replacement capacity accepted: %+v", result)
				}
				if len(result.Placements) != 0 {
					t.Fatal("failed simulation leaked partial placements")
				}
				if scheduling.ValidateResult(request, result) == nil {
					t.Fatal("non-feasible result authorized")
				}
			}
			after, err := json.Marshal(snap)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("simulation mutated original snapshot")
			}
		})
	}
}

func runWorker(t *testing.T, binary string, args []string, input []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("worker failed: %v\n%s", err, stderr.String())
	}
	return output
}

func configureWorkerObjects(objects []client.Object, gang, capacity bool) {
	for _, object := range objects {
		switch obj := object.(type) {
		case *corev1.Node:
			obj.Labels["kubernetes.io/hostname"] = obj.Name
			obj.Labels["accelerator"] = "gpu"
			obj.Status.Allocatable[corev1.ResourcePods] = resource.MustParse("100")
			obj.Status.Capacity[corev1.ResourcePods] = resource.MustParse("100")
			obj.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse("1")
			obj.Status.Capacity["nvidia.com/gpu"] = resource.MustParse("1")
			if strings.HasPrefix(obj.Name, "target-") {
				cpu := "2"
				if !capacity && (!gang || obj.Name == "target-b") {
					cpu = "1"
				}
				obj.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse(cpu)
				obj.Status.Capacity[corev1.ResourceCPU] = resource.MustParse(cpu)
			}
		case *corev1.Pod:
			obj.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
			obj.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1")
			if !gang {
				// With the source still present, source-b is in the same occupied
				// zone and must not fit even though it has spare CPU and memory.
				obj.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
						TopologyKey:   "topology.kubernetes.io/zone",
						LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"ome.io/inferenceservice": "svc"}},
					}},
				}}
			}
		case *v1beta1.InferenceReplica:
			for i := range obj.Spec.Runners {
				obj.Spec.Runners[i].Template.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
				obj.Spec.Runners[i].Template.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1")
			}
		}
	}
}
