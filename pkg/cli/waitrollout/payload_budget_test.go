package waitrollout

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	report "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

func TestRolledBackWaitAllowsBoundedManagedFields(t *testing.T) {
	v := canary(ome.RolloutPhaseRolledBack)
	wantDecision, wantObservation, err := Evaluate(v, report.WaitRequestedRolloutRolledBack, now)
	require.NoError(t, err)
	require.True(t, wantDecision.Matched)

	// Kubernetes managed-fields entries contain opaque JSON, not a collection
	// of rollout records. A live rollback had a 2,653-byte status field set.
	fields := map[string]any{}
	for i := range 200 {
		fields[fmt.Sprintf("f:PRIVATE_MANAGED_FIELD_%03d", i)] = map[string]any{}
	}
	raw, err := json.Marshal(map[string]any{"f:status": fields})
	require.NoError(t, err)
	require.Greater(t, len(raw), 2048)
	v.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager: "manager", Operation: metav1.ManagedFieldsOperationUpdate,
		APIVersion: "ome.io/v1beta1", Subresource: "status", FieldsType: "FieldsV1",
		FieldsV1: &metav1.FieldsV1{Raw: raw},
	}}
	before := v.DeepCopy()

	decision, observation, err := Evaluate(v, report.WaitRequestedRolloutRolledBack, now)

	require.NoError(t, err)
	require.Equal(t, wantDecision, decision)
	require.Equal(t, wantObservation, observation, "managed fields are not rollout evidence")
	require.Equal(t, before, v, "inspection must not strip or mutate source metadata")
	encoded, err := json.Marshal(observation)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "PRIVATE_MANAGED_FIELD")
}

func TestPayloadBudgetCountsOpaqueBytesAcrossFields(t *testing.T) {
	type opaqueByte uint8
	const byteLimit = 2 * 1024 * 1024
	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{"byte slice above structural cap", make([]byte, 2049), true},
		{"byte array above structural cap", [2049]byte{}, true},
		{"uint8 alias slice", make([]opaqueByte, 2049), true},
		{"uint8 alias array", [2049]opaqueByte{}, true},
		{"named byte slice above node cap", make(json.RawMessage, 65537), true},
		{"exact byte budget", make([]byte, byteLimit), true},
		{"over byte budget", make([]byte, byteLimit+1), false},
		{"two byte fields exact budget", struct{ First, Second []byte }{make([]byte, byteLimit/2), make([]byte, byteLimit/2)}, true},
		{"two byte fields over budget", struct{ First, Second []byte }{make([]byte, byteLimit/2), make([]byte, byteLimit/2+1)}, false},
		{"bytes then string exact budget", struct {
			Raw  []byte
			Text string
		}{make([]byte, byteLimit-1), "x"}, true},
		{"bytes then string over budget", struct {
			Raw  []byte
			Text string
		}{make([]byte, byteLimit-1), "xx"}, false},
		{"string then bytes over budget", struct {
			Text string
			Raw  []byte
		}{"xx", make([]byte, byteLimit-1)}, false},
		{"nonbyte slice exact structural cap", make([]bool, 2048), true},
		{"nonbyte slice over structural cap", make([]bool, 2049), false},
		{"nonbyte array over structural cap", [2049]bool{}, false},
		{"nonbyte aggregate node budget", make([][2048]bool, 33), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := payloadBudget{}
			require.Equal(t, test.want, budget.inspect(reflect.ValueOf(test.value), 0, ""))
		})
	}
}

func TestRolloutWaitRejectsOverBudgetManagedFields(t *testing.T) {
	v := canary(ome.RolloutPhaseRolledBack)
	v.ManagedFields = []metav1.ManagedFieldsEntry{{
		FieldsV1: &metav1.FieldsV1{Raw: make([]byte, 2*1024*1024+1)},
	}}
	before := v.DeepCopy()

	decision, _, err := Evaluate(v, report.WaitRequestedRolloutRolledBack, now)

	require.False(t, decision.Matched)
	require.ErrorAs(t, err, new(*waitengine.Error))
	require.EqualError(t, err, "RolloutInspectionLimit")
	require.Equal(t, before, v)
}

func TestOpaqueBytesDoNotBypassSiblingStructureBounds(t *testing.T) {
	oversizedMap := map[int]int{}
	for i := range 257 {
		oversizedMap[i] = i
	}
	var tooDeep any = true
	for range 33 {
		tooDeep = []any{tooDeep}
	}
	for _, value := range []any{oversizedMap, tooDeep, make([][2048]bool, 33)} {
		payload := struct {
			Raw   []byte
			Other any
		}{make([]byte, 2049), value}
		budget := payloadBudget{}
		require.False(t, budget.inspect(reflect.ValueOf(payload), 0, ""))
	}
}
