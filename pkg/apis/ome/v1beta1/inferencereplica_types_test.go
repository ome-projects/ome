package v1beta1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestParentReference_JSONShape pins the exact wire shape of the
// ParentReference sub-object so a future field rename doesn't silently
// break stored InferenceReplica objects.
func TestParentReference_JSONShape(t *testing.T) {
	pr := ParentReference{Name: "llama"}
	data, err := json.Marshal(&pr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"name":"llama"}`
	if string(data) != want {
		t.Errorf("ParentReference JSON shape changed:\n want %s\n got %s", want, string(data))
	}
}

// columnarStatusFixture exercises every ColumnarV2 field, including a
// non-ascending row order and entries carrying each exceptional record.
func columnarStatusFixture() InferenceReplicaStatus {
	at := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	encoding := InstanceStatusEncodingColumnarV2
	admitted := "0-3,7"
	activeOrdinalOne := "1"
	exitCode := int32(143)
	return InferenceReplicaStatus{
		Replicas:               5,
		ReadyReplicas:          4,
		CurrentRevision:        "example-engine-2f32f6fe",
		UpdateRevision:         "example-engine-9b1c0d2e",
		InstanceStatusEncoding: &encoding,
		InstanceStatusColumns: &InstanceStatusColumns{
			Members:  "0-3,7",
			RowOrder: []int32{7, 0, 1, 2, 3},
			Phases: []InstanceStatusPhaseGroup{
				{Value: OMENativeInstanceReady, Indexes: "0-3"},
				{Value: OMENativeInstanceUpdating, Indexes: "7"},
			},
			RunningRevisions: []InstanceStatusRevisionGroup{{Value: "example-engine-2f32f6fe", Indexes: "0-3,7"}},
			TargetRevisions:  []InstanceStatusRevisionGroup{{Value: "example-engine-9b1c0d2e", Indexes: "7"}},
			Incarnations: []InstanceStatusIncarnationGroup{
				{Value: -1, Indexes: "2"},
				{Value: 1, Indexes: "0-1"},
				{Value: 2, Indexes: "3,7"},
			},
			PodCounts:          []InstanceStatusCountGroup{{Value: 1, Indexes: "0-3,7"}},
			ServingPodCounts:   []InstanceStatusCountGroup{{Value: 1, Indexes: "0-3"}},
			AvailablePodCounts: []InstanceStatusCountGroup{{Value: 1, Indexes: "0-3"}},
			Admitted:           &admitted,
			ActiveOrdinalOne:   &activeOrdinalOne,
			Entries: []InstanceStatusEntry{
				{
					Index: 3,
					Conditions: []metav1.Condition{{
						Type: "AllPodsReady", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: at,
					}},
					ReadySince: &at,
				},
				{
					Index: 7,
					Operation: &InstanceOperation{
						ID: "op-7", Type: InstanceOperationUpdate, Step: "WaitReady",
						StartedAt: at, LastProgressAt: at, Deadline: at, TargetRevision: "example-engine-9b1c0d2e",
					},
					LastFailure: &InstanceTermination{
						PodName: "example-engine-7", ContainerName: "ome-container", Reason: "Error",
						ExitCode: &exitCode, Message: "exit status 143", Time: at,
					},
				},
			},
		},
	}
}

func TestInferenceReplicaStatus_ColumnarV2JSONRoundTrip(t *testing.T) {
	in := columnarStatusFixture()
	data, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), `"instanceStatuses"`) {
		t.Fatalf("ColumnarV2 status must not carry the dense list: %s", data)
	}

	var out InferenceReplicaStatus
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !equality.Semantic.DeepEqual(in, out) {
		t.Fatalf("round trip changed the status:\n in  %+v\n out %+v", in, out)
	}
	again, err := json.Marshal(&out)
	if err != nil {
		t.Fatalf("second marshal: %v", err)
	}
	if string(again) != string(data) {
		t.Fatalf("second marshal differs:\n first  %s\n second %s", data, again)
	}

	copied := in.DeepCopy()
	if !equality.Semantic.DeepEqual(in, *copied) {
		t.Fatal("DeepCopy changed the status")
	}
	copied.InstanceStatusColumns.Entries[1].LastFailure.PodName = "changed"
	if in.InstanceStatusColumns.Entries[1].LastFailure.PodName != "example-engine-7" {
		t.Fatal("DeepCopy shares nested entry records with the original")
	}
}

// TestInstanceStatusColumns_JSONShape pins the wire keys and their order,
// which the recorded ColumnarV2 sizes depend on.
func TestInstanceStatusColumns_JSONShape(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	admitted := "0-499"
	activeOrdinalOne := "6,43,175"
	exitCode := int32(143)
	columns := InstanceStatusColumns{
		Members:            "0-499",
		Phases:             []InstanceStatusPhaseGroup{{Value: OMENativeInstanceReady, Indexes: "0-499"}},
		RunningRevisions:   []InstanceStatusRevisionGroup{{Value: "example-engine-2f32f6fe", Indexes: "0-499"}},
		Incarnations:       []InstanceStatusIncarnationGroup{{Value: 1, Indexes: "0-8,10-114,116-499"}, {Value: 2, Indexes: "9,115"}},
		PodCounts:          []InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		ServingPodCounts:   []InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		AvailablePodCounts: []InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		Admitted:           &admitted,
		ActiveOrdinalOne:   &activeOrdinalOne,
		Entries: []InstanceStatusEntry{{
			Index:       9,
			LastFailure: &InstanceTermination{PodName: "example-engine-9", ContainerName: "ome-container", Reason: "Error", ExitCode: &exitCode, Time: at},
		}},
	}
	data, err := json.Marshal(&columns)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"members":"0-499",` +
		`"phases":[{"value":"Ready","indexes":"0-499"}],` +
		`"runningRevisions":[{"value":"example-engine-2f32f6fe","indexes":"0-499"}],` +
		`"incarnations":[{"value":1,"indexes":"0-8,10-114,116-499"},{"value":2,"indexes":"9,115"}],` +
		`"podCounts":[{"value":1,"indexes":"0-499"}],` +
		`"servingPodCounts":[{"value":1,"indexes":"0-499"}],` +
		`"availablePodCounts":[{"value":1,"indexes":"0-499"}],` +
		`"admitted":"0-499","activeOrdinalOne":"6,43,175",` +
		`"entries":[{"index":9,"lastFailure":{"podName":"example-engine-9","containerName":"ome-container","reason":"Error","exitCode":143,"time":"2026-01-02T03:04:05Z"}}]}`
	if string(data) != want {
		t.Errorf("InstanceStatusColumns JSON shape changed:\n want %s\n got  %s", want, data)
	}
}

// A DenseV1 status must serialize exactly as before the union existed: the
// marker and columns add no bytes when absent.
func TestInferenceReplicaStatus_DenseV1AddsNoUnionBytes(t *testing.T) {
	status := InferenceReplicaStatus{
		Replicas:         1,
		InstanceStatuses: []OMENativeInstanceStatus{{Index: 0, Phase: OMENativeInstanceReady}},
	}
	data, err := json.Marshal(&status)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"replicas":1,"instanceStatuses":[{"index":0,"phase":"Ready"}]}`
	if string(data) != want {
		t.Errorf("DenseV1 JSON shape changed:\n want %s\n got  %s", want, data)
	}
}

// Typed decoding must keep absent and explicitly empty column fields apart;
// the codec rejects the explicit empty spelling rather than defaulting it.
func TestInstanceStatusColumns_ExplicitEmptyIsDistinguishable(t *testing.T) {
	var absent, explicit InstanceStatusColumns
	if err := json.Unmarshal([]byte(`{"members":"0","phases":[{"value":"Ready","indexes":"0"}]}`), &absent); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"members":"0","phases":[{"value":"Ready","indexes":"0"}],"rowOrder":[],"entries":[],"admitted":""}`), &explicit); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if absent.RowOrder != nil || absent.Entries != nil || absent.Admitted != nil {
		t.Fatalf("absent fields decoded as present: %+v", absent)
	}
	if explicit.RowOrder == nil || len(explicit.RowOrder) != 0 || explicit.Entries == nil || len(explicit.Entries) != 0 || explicit.Admitted == nil || *explicit.Admitted != "" {
		t.Fatalf("explicit empty fields decoded as absent: %+v", explicit)
	}
}
