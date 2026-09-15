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
		pending   string
	}{
		{name: "single_fits_elsewhere", capacity: true},
		{name: "single_source_occupancy_blocks_same_zone"},
		{name: "whole_gang_fits", gang: true, capacity: true},
		{name: "partial_gang_capacity_rejected", gang: true},
		{name: "migration_single_fits", capacity: true, execution: true},
		{name: "migration_whole_gang_fits", gang: true, capacity: true, execution: true},
		{name: "migration_partial_gang_rejected", gang: true, execution: true},
		{name: "single_pending_competitor_fits", capacity: true, pending: "fits"},
		{name: "single_pending_competitor_blocks", capacity: true, pending: "blocks"},
		{name: "gang_pending_competitor_fits", gang: true, capacity: true, pending: "fits"},
		{name: "gang_pending_competitor_blocks", gang: true, capacity: true, pending: "blocks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			configName := "default-scheduler.yaml"
			if tc.gang {
				objects, source = validGangSourceObjects()
				configName = "ome-scheduler.yaml"
			}
			configureWorkerObjects(objects, tc.gang, tc.capacity)
			if tc.pending != "" {
				objects = addWorkerPendingCompetitor(objects, tc.gang, tc.pending == "blocks")
			}
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
			if tc.capacity && tc.pending != "blocks" {
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
					if tc.pending != "" && !tc.gang && placement.NodeName != "target-b" {
						t.Fatalf("replacement ignored the pending competitor occupying target-a: %+v", placement)
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

func addWorkerPendingCompetitor(objects []client.Object, gang, blocks bool) []client.Object {
	scheduler := "default-scheduler"
	if gang {
		scheduler = "ome-scheduler"
	}
	pending := &corev1.Pod{ObjectMeta: captureMeta("pending-competitor"), Spec: sourcePodSpec(scheduler), Status: corev1.PodStatus{Phase: corev1.PodPending}}
	priority := int32(200)
	pending.Spec.Priority = &priority
	pending.Spec.NodeSelector[corev1.LabelHostname] = "target-a"
	pending.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("1")
	pending.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1")
	for _, object := range objects {
		node, ok := object.(*corev1.Node)
		if !ok {
			continue
		}
		if blocks && !gang && node.Name == "target-b" {
			// target-a is the only replacement slot until the competitor takes it.
			node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1")
			node.Status.Capacity[corev1.ResourceCPU] = resource.MustParse("1")
		}
		if !blocks && gang && node.Name == "target-a" {
			// The competitor and both gang members have three slots in total.
			node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("4")
			node.Status.Capacity[corev1.ResourceCPU] = resource.MustParse("4")
			node.Status.Allocatable["nvidia.com/gpu"] = resource.MustParse("2")
			node.Status.Capacity["nvidia.com/gpu"] = resource.MustParse("2")
		}
	}
	return append(objects, pending)
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
