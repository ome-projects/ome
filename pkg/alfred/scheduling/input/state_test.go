package input

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
)

const statePod = `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod","namespace":"team","uid":"pod-uid","resourceVersion":"10"},"spec":{"nodeName":"node","containers":[{"name":"main","resources":{"requests":{"cpu":"1"}}}]},"status":{"phase":"Running","nominatedNodeName":"","conditions":[{"type":"Ready","status":"True","lastHeartbeatTime":"2026-09-15T00:00:00Z"}],"containerStatuses":[{"name":"main","allocatedResources":{"cpu":"1"}}]}}`

const stateNode = `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node","uid":"node-uid","resourceVersion":"10","labels":{"zone":"a"}},"spec":{"unschedulable":false},"status":{"allocatable":{"cpu":"4"},"conditions":[{"type":"Ready","status":"True","lastHeartbeatTime":"2026-09-15T00:00:00Z","lastTransitionTime":"2026-09-14T00:00:00Z","reason":"Healthy","message":"healthy"}]}}`

func stateSnapshot(t *testing.T, objects ...string) *Snapshot {
	t.Helper()
	s := &Snapshot{StartedAt: captureTime, CompletedAt: captureTime}
	for _, object := range objects {
		s.Objects = append(s.Objects, runtime.RawExtension{Raw: []byte(object)})
	}
	var err error
	s.ID, err = s.contentID()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The comparison must tolerate only audited noise, without weakening the
// lossless snapshot identity or mutating either observation.
func TestSameSchedulingStateIgnoresBookkeeping(t *testing.T) {
	for _, kind := range []struct{ apiVersion, kind string }{
		{"v1", "Node"}, {"v1", "Pod"}, {"v1", "Namespace"}, {"v1", "Service"},
		{"v1", "ReplicationController"}, {"apps/v1", "ReplicaSet"},
		{"apps/v1", "StatefulSet"}, {"scheduling.x-k8s.io/v1alpha1", "PodGroup"},
	} {
		t.Run(kind.kind, func(t *testing.T) {
			object := fmt.Sprintf(`{"apiVersion":%q,"kind":%q,"metadata":{"name":"other","uid":"other-uid","resourceVersion":"10"}}`, kind.apiVersion, kind.kind)
			changed := strings.Replace(object, `"resourceVersion":"10"`, `"resourceVersion":"11","managedFields":[{"manager":"controller","operation":"Update","time":"2026-09-15T00:00:01Z"}]`, 1)
			a := stateSnapshot(t, object, statePod)
			b := stateSnapshot(t, statePod, changed) // API list order is immaterial.
			beforeA, _ := json.Marshal(a)
			beforeB, _ := json.Marshal(b)
			if !SameSchedulingState(a, b) || !SameSchedulingState(b, a) {
				t.Fatal("bookkeeping update invalidated scheduling state")
			}
			if a.ID == b.ID {
				t.Fatal("different lossless snapshots reused an identity")
			}
			afterA, _ := json.Marshal(a)
			afterB, _ := json.Marshal(b)
			if string(beforeA) != string(afterA) || string(beforeB) != string(afterB) {
				t.Fatal("comparison mutated a snapshot")
			}
			for _, s := range []*Snapshot{a, b} {
				if err := s.Validate(captureTime, time.Minute); err != nil {
					t.Fatalf("comparison invalidated snapshot: %v", err)
				}
				if err := s.Validate(captureTime.Add(time.Minute+time.Nanosecond), time.Minute); err == nil {
					t.Fatal("comparison weakened snapshot freshness")
				}
			}
		})
	}
}

func TestSameSchedulingStateIgnoresOnlyNodeHeartbeat(t *testing.T) {
	a := stateSnapshot(t, stateNode, statePod)
	b := stateSnapshot(t, strings.Replace(stateNode, "2026-09-15T00:00:00Z", "2026-09-15T00:00:01Z", 1), statePod)
	if !SameSchedulingState(a, b) {
		t.Fatal("heartbeat-only update invalidated scheduling state")
	}
	// An unknown field with the same name on a Pod is not in the allowlist.
	b = stateSnapshot(t, stateNode, strings.Replace(statePod, "2026-09-15T00:00:00Z", "2026-09-15T00:00:01Z", 1))
	if SameSchedulingState(a, b) {
		t.Fatal("ignored heartbeat-named field outside Node conditions")
	}
}

// These cases catch accidentally dropping whole metadata/spec/status objects,
// losing unknown fields in a typed projection, or rounding large JSON numbers.
func TestSameSchedulingStatePreservesMeaningfulChanges(t *testing.T) {
	for _, tc := range []struct {
		name, object, from, to string
	}{
		{"identity", statePod, `"pod-uid"`, `"successor-uid"`},
		{"binding", statePod, `"nodeName":"node"`, `"nodeName":"other"`},
		{"pending", statePod, `"nodeName":"node"`, `"nodeName":""`},
		{"requests", statePod, `"cpu":"1"`, `"cpu":"2"`},
		{"phase", statePod, `"phase":"Running"`, `"phase":"Failed"`},
		{"nomination", statePod, `"nominatedNodeName":""`, `"nominatedNodeName":"other"`},
		{"allocated resources", statePod, `"allocatedResources":{"cpu":"1"}`, `"allocatedResources":{"cpu":"2"}`},
		{"pod readiness", statePod, `"status":"True"`, `"status":"False"`},
		{"priority", statePod, `"spec":{`, `"spec":{"priority":100,`},
		{"affinity", statePod, `"spec":{`, `"spec":{"affinity":{"nodeAffinity":{}},`},
		{"labels", stateNode, `"zone":"a"`, `"zone":"b"`},
		{"annotations", stateNode, `"metadata":{`, `"metadata":{"annotations":{"maintenance":"yes"},`},
		{"deletion", statePod, `"metadata":{`, `"metadata":{"deletionTimestamp":"2026-09-15T00:00:00Z",`},
		{"owner", statePod, `"metadata":{`, `"metadata":{"ownerReferences":[{"uid":"new-owner"}],`},
		{"capacity", stateNode, `"cpu":"4"`, `"cpu":"3"`},
		{"cordon", stateNode, `"unschedulable":false`, `"unschedulable":true`},
		{"taint", stateNode, `"spec":{`, `"spec":{"taints":[{"key":"maintenance","effect":"NoSchedule"}],`},
		{"node health", stateNode, `"status":"True"`, `"status":"False"`},
		{"condition type", stateNode, `"type":"Ready"`, `"type":"DiskPressure"`},
		{"transition", stateNode, "2026-09-14T00:00:00Z", "2026-09-15T00:00:00Z"},
		{"condition reason", stateNode, `"reason":"Healthy"`, `"reason":"Unknown"`},
		{"condition message", stateNode, `"message":"healthy"`, `"message":"unhealthy"`},
		{"unknown status", stateNode, `"status":{`, `"status":{"futureSchedulingInput":true,`},
		{"large integer", strings.Replace(statePod, `"spec":{`, `"spec":{"futureSchedulingInput":9007199254740992,`, 1), "9007199254740992", "9007199254740993"},
		{"nested bookkeeping", strings.Replace(statePod, `"spec":{`, `"spec":{"futureSchedulingInput":{"resourceVersion":"10"},`, 1), `"futureSchedulingInput":{"resourceVersion":"10"}`, `"futureSchedulingInput":{"resourceVersion":"11"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := strings.Replace(tc.object, tc.from, tc.to, 1)
			if changed == tc.object {
				t.Fatal("fixture mutation did not change the object")
			}
			if SameSchedulingState(stateSnapshot(t, tc.object), stateSnapshot(t, changed)) {
				t.Fatal("meaningful change retained execution authorization")
			}
		})
	}
}

func TestSameSchedulingStateRejectsMembershipAndInvalidObjects(t *testing.T) {
	a := stateSnapshot(t, statePod, stateNode)
	for _, tc := range []struct {
		name string
		b    *Snapshot
	}{
		{"nil", nil},
		{"removed", stateSnapshot(t, statePod)},
		{"added pending", stateSnapshot(t, statePod, stateNode, strings.ReplaceAll(strings.Replace(statePod, `"nodeName":"node"`, `"nodeName":""`, 1), "pod", "pending"))},
		{"duplicate identity", stateSnapshot(t, statePod, statePod)},
		{"unknown kind", stateSnapshot(t, statePod, strings.Replace(stateNode, `"kind":"Node"`, `"kind":"FutureNode"`, 1))},
		{"unknown version", stateSnapshot(t, statePod, strings.Replace(stateNode, `"apiVersion":"v1"`, `"apiVersion":"v2"`, 1))},
		{"missing name", stateSnapshot(t, statePod, strings.Replace(stateNode, `"name":"node",`, "", 1))},
		{"bad metadata", stateSnapshot(t, statePod, `{"apiVersion":"v1","kind":"Node","metadata":[]}`)},
		{"bad conditions", stateSnapshot(t, statePod, `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node"},"status":{"conditions":["bad"]}}`)},
		{"bad JSON", &Snapshot{Objects: []runtime.RawExtension{{Raw: []byte(statePod)}, {Raw: []byte(`{`)}}}},
		{"trailing JSON", &Snapshot{Objects: []runtime.RawExtension{{Raw: []byte(statePod)}, {Raw: []byte(stateNode + `{}`)}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if SameSchedulingState(a, tc.b) || SameSchedulingState(tc.b, a) {
				t.Fatal("accepted incomplete or invalid scheduling observation")
			}
			// Invalid data must not be authorized merely because it is identical.
			if tc.name != "removed" && tc.name != "added pending" && SameSchedulingState(tc.b, tc.b) {
				t.Fatal("accepted identical invalid observations")
			}
		})
	}
	if !SameSchedulingState(a, a) || !reflect.DeepEqual(a.Objects[0].Raw, []byte(statePod)) {
		t.Fatal("valid unchanged state was rejected or mutated")
	}
}
