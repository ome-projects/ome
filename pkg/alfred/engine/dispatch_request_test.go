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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

func TestDispatchPayloadUsesExistingPublicWireContract(t *testing.T) {
	c := cand("prod/a", "source", "target")
	e, err := newDispatchEntry(c, "isvc-uid", "a-engine", "ir-uid", "fingerprint", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(e.UUID); err != nil {
		t.Fatal(err)
	}
	request, err := audit.ParseMigrationRequest(e.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if request.SchemaVersion != "v1" || request.Component != "engine" || request.Instance != 0 || request.FromNode != "source" || request.RequestedBy != "alfred" || !reflect.DeepEqual(request.HintTargetNodes, []string{"target"}) {
		t.Fatalf("wrong migration payload: %+v", request)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(e.Payload), &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 8 {
		t.Fatalf("unexpected API fields: %v", fields)
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
