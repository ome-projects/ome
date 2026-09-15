package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestDispatchPayloadUsesExistingPublicWireContract(t *testing.T) {
	c := cand("prod/a", "source", "target")
	c.Reason = "node maintenance"
	e, err := newDispatchEntry(c, "isvc-uid", "a-engine", "ir-uid", "fingerprint", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(e.UUID); err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(e.Payload), &fields); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"schemaVersion": "v1", "component": "engine", "instance": float64(0),
		"from_node": "source", "hint_target_nodes": []any{"target"},
		"reason": "node maintenance", "requested_at": "2026-01-01T12:00:00Z", "requested_by": "alfred",
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("migration wire fields = %v, want %v", fields, want)
	}
	if e.Phase != "prepared" || e.LastAttempt != nil {
		t.Fatalf("new intent must not imply a write: %+v", e)
	}
}

func TestDispatchPatchIsAdditiveAndIdentityConditioned(t *testing.T) {
	_, reader, c := predictionFixture(t)
	cl := reader.Reader.(client.Client)
	var isvc v1beta1.InferenceService
	if err := cl.Get(context.Background(), c.Workload, &isvc); err != nil {
		t.Fatal(err)
	}
	isvc.Annotations = map[string]string{"user.example/note": "keep"}
	if err := cl.Update(context.Background(), &isvc); err != nil {
		t.Fatal(err)
	}
	original := isvc.DeepCopy()
	e, err := newDispatchEntry(c, isvc.UID, "a-engine", "ir-uid", "fingerprint", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if err := submitDispatch(context.Background(), cl, &isvc, e); err != nil {
		t.Fatal(err)
	}
	var got v1beta1.InferenceService
	if err := cl.Get(context.Background(), c.Workload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations["user.example/note"] != "keep" || got.Annotations["ome.io/migration-request-v1-"+e.UUID] != e.Payload || !reflect.DeepEqual(got.Spec, original.Spec) {
		t.Fatalf("unexpected mutation: %+v", got)
	}
	if err := submitDispatch(context.Background(), cl, &got, e); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	// A stale resource version must not silently overwrite a concurrent edit.
	other := e
	other.UUID = uuid.NewString()
	if err := submitDispatch(context.Background(), cl, original, other); err == nil {
		t.Fatal("stale patch accepted")
	}
	e.Payload = `{}`
	if err := submitDispatch(context.Background(), cl, &got, e); err == nil {
		t.Fatal("different value replaced existing UUID")
	}
}

func TestDispatchEntryRejectsInvalidWireInputs(t *testing.T) {
	for _, mutate := range []func(*policy.Candidate){
		func(c *policy.Candidate) { c.Instance = -1 },
		func(c *policy.Candidate) { c.HintTargetNodes = []string{"source"} },
		func(c *policy.Candidate) { c.Component = "unknown" },
	} {
		c := cand("prod/a", "source", "target")
		mutate(&c)
		if _, err := newDispatchEntry(c, types.UID("owner"), "a-engine", types.UID("ir"), "fingerprint", metav1.NewTime(testNow).Time); err == nil {
			t.Fatalf("accepted invalid candidate: %+v", c)
		}
	}
}
