package inferencereplica

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
)

// storedStatusesWithAnchors is the codec's file of complete ColumnarV2
// statuses as writers stored them, one per payload form and row fixture.
const storedStatusesWithAnchors = "../irstatus/testdata/stored_statuses.json"

// storedAnchorRows maps the row fixture a stored status was written for to
// its rows: the readySince of each is the instant the pass must read back.
var storedAnchorRows = map[string]func() []v1beta1.OMENativeInstanceStatus{
	"readySince/50": func() []v1beta1.OMENativeInstanceStatus { return irstatustest.ReadySinceRows(50) },
	"fullFeature":   irstatustest.FullFeatureRows,
}

// TestStoredAnchorsReachTheEngineRows reads a replica stored in every
// ColumnarV2 form the codec's fixtures hold through the decoding reader a
// pass uses, and requires each engine row's readySince to be the stored
// instant: the anchor the restart reader dates a runner restart against,
// so a restart after it is read as one and a restart before it is boot.
func TestStoredAnchorsReachTheEngineRows(t *testing.T) {
	data, err := os.ReadFile(filepath.FromSlash(storedStatusesWithAnchors))
	if err != nil {
		t.Fatalf("read %s: %v", storedStatusesWithAnchors, err)
	}
	stored := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("parse %s: %v", storedStatusesWithAnchors, err)
	}
	names := make([]string, 0, len(stored))
	for name := range stored {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatalf("%s holds no stored status", storedStatusesWithAnchors)
	}
	for _, name := range names {
		_, fixture, _ := strings.Cut(name, "/")
		rowsOf, ok := storedAnchorRows[fixture]
		if !ok {
			t.Fatalf("%s names the row fixture %q, which this test does not know", name, fixture)
		}
		t.Run(name, func(t *testing.T) { assertStoredAnchorsReachTheEngine(t, stored[name], rowsOf()) })
	}
}

func assertStoredAnchorsReachTheEngine(t *testing.T, raw json.RawMessage, want []v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	ctx := context.Background()
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "model-engine", Namespace: "default"}}
	if err := json.Unmarshal(raw, &ir.Status); err != nil {
		t.Fatalf("unmarshal the stored status: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	reader := irstatus.NewReader(c, irstatus.NewDecoder(uint64(len(want))))

	fresh := &v1beta1.InferenceReplica{}
	encoding, err := irstatus.GetDecoded(ctx, reader, client.ObjectKeyFromObject(ir), fresh)
	if err != nil || encoding != irstatus.EncodingColumnarV2 {
		t.Fatalf("GetDecoded() = (%q, %v), want ColumnarV2", encoding, err)
	}
	observed := observedFromIR(fresh)
	if len(observed.InstanceStatuses) != len(want) {
		t.Fatalf("the engine sees %d rows, want %d", len(observed.InstanceStatuses), len(want))
	}
	anchors := 0
	for i, row := range observed.InstanceStatuses {
		wantAnchor := want[i].ReadySince
		if row.Index != want[i].Index {
			t.Fatalf("row %d is Instance %d, want %d", i, row.Index, want[i].Index)
		}
		if (row.ReadySince == nil) != (wantAnchor == nil) {
			t.Fatalf("Instance %d: readySince present = %t, want %t", row.Index, row.ReadySince != nil, wantAnchor != nil)
		}
		if wantAnchor == nil {
			continue
		}
		anchors++
		if !row.ReadySince.UTC().Equal(wantAnchor.UTC()) {
			t.Fatalf("Instance %d: readySince %s, want %s", row.Index, row.ReadySince.UTC(), wantAnchor.UTC())
		}
		after, before := runnerRestartedAt(wantAnchor.Add(time.Minute)), runnerRestartedAt(wantAnchor.Add(-time.Minute))
		if _, restarted := workloadops.RunnerRestartedSinceReady(after, row.ReadySince); !restarted {
			t.Fatalf("Instance %d: a runner restart one minute after the stored anchor is not read as a restart after Ready", row.Index)
		}
		if _, restarted := workloadops.RunnerRestartedSinceReady(before, row.ReadySince); restarted {
			t.Fatalf("Instance %d: a runner restart one minute before the stored anchor is read as a restart after Ready", row.Index)
		}
	}
	if anchors == 0 {
		t.Fatal("the fixture carries no readySince, so it proves nothing about the anchors")
	}
}

// runnerRestartedAt is a pod created an hour before the anchors whose
// runner container's last run ended at finished.
func runnerRestartedAt(finished time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "model-engine-0", CreationTimestamp: metav1.NewTime(finished.Add(-24 * time.Hour))},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:                 constants.MainContainerName,
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 137, FinishedAt: metav1.NewTime(finished)}},
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(finished.Add(time.Second))}},
		}}},
	}
}
