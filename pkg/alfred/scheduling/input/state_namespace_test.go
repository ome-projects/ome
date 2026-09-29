package input

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const stateNamespace = `{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"other","uid":"namespace-uid","resourceVersion":"10","labels":{"tenant":"other"}},"status":{"phase":"Active"}}`

// External annotation observers must not starve scheduling. The complete raw
// observations and their hashes still differ; only the placement comparison
// ignores this audited field on Namespace, never labels or other object kinds.
func TestSameSchedulingStateNamespaceAnnotations(t *testing.T) {
	for _, annotations := range []string{`null`, `{}`, `{"example.com/checkpoint":"one"}`, `{"example.com/checkpoint":"two","example.com/another":"value"}`} {
		t.Run(annotations, func(t *testing.T) {
			changed := strings.Replace(stateNamespace, `"metadata":{`, `"metadata":{"annotations":`+annotations+`,`, 1)
			a, b := stateSnapshot(t, stateNamespace), stateSnapshot(t, changed)
			beforeA, _ := json.Marshal(a)
			beforeB, _ := json.Marshal(b)
			if !SameSchedulingState(a, b) || !SameSchedulingState(b, a) {
				t.Fatal("Namespace annotation-only change prevented progress")
			}
			if a.ID == b.ID {
				t.Fatal("annotation-only change lost its lossless snapshot identity")
			}
			afterA, _ := json.Marshal(a)
			afterB, _ := json.Marshal(b)
			if string(beforeA) != string(afterA) || string(beforeB) != string(afterB) {
				t.Fatal("comparison mutated the source snapshots")
			}
			if err := b.Validate(captureTime, time.Minute); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSameSchedulingStateRejectsMalformedNamespaceAnnotations(t *testing.T) {
	for _, annotations := range []string{`[]`, `"bad"`, `4`, `true`, `{"key":null}`, `{"key":1}`, `{"key":false}`, `{"key":{}}`, `{"key":[]}`} {
		t.Run(annotations, func(t *testing.T) {
			bad := stateSnapshot(t, strings.Replace(stateNamespace, `"metadata":{`, `"metadata":{"annotations":`+annotations+`,`, 1))
			if SameSchedulingState(bad, bad) || SameSchedulingState(stateSnapshot(t, stateNamespace), bad) {
				t.Fatal("malformed Namespace annotations authorized placement")
			}
		})
	}
}

func TestSameSchedulingStatePreservesNamespaceDependencies(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"labels", `"tenant":"other"`, `"tenant":"prod"`},
		{"identity", `"namespace-uid"`, `"successor-uid"`},
		{"deletion", `"metadata":{`, `"metadata":{"deletionTimestamp":"2026-09-15T00:00:00Z",`},
		{"lifecycle", `"phase":"Active"`, `"phase":"Terminating"`},
		{"unknown metadata", `"metadata":{`, `"metadata":{"futureSchedulingInput":true,`},
		{"nested annotations", `"status":{`, `"status":{"futureSchedulingInput":{"annotations":{"key":"value"}},`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := strings.Replace(stateNamespace, tc.from, tc.to, 1)
			if changed == stateNamespace || SameSchedulingState(stateSnapshot(t, stateNamespace), stateSnapshot(t, changed)) {
				t.Fatal("material or unknown Namespace input was ignored")
			}
		})
	}
	for _, kind := range []struct{ version, kind string }{{"v1", "Pod"}, {"v1", "Node"}, {"v1", "Service"}, {"apps/v1", "ReplicaSet"}, {"scheduling.x-k8s.io/v1alpha1", "PodGroup"}} {
		t.Run(kind.kind, func(t *testing.T) {
			object := strings.Replace(strings.Replace(stateNamespace, `"kind":"Namespace"`, `"kind":"`+kind.kind+`"`, 1), `"apiVersion":"v1"`, `"apiVersion":"`+kind.version+`"`, 1)
			changed := strings.Replace(object, `"metadata":{`, `"metadata":{"annotations":{"example.com/input":"changed"},`, 1)
			if SameSchedulingState(stateSnapshot(t, object), stateSnapshot(t, changed)) {
				t.Fatal("annotations outside Namespace lost their placement fence")
			}
		})
	}
}
