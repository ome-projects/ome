package mutate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/constants"
)

// Removing the pre-plan count budget must refuse rather than expand a large
// pure desired plan. The RED fixture is intentionally only one over budget.
func TestMigrationReviewDesiredCountBeforePurePlan(t *testing.T) {
	v, _ := nativeTarget(t)
	ir, pods, cr := migrationSources(v)
	count := int32(2049)
	ir.Spec.Replicas = &count
	e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
	require.ErrorIs(t, err, ErrBounds)
	require.Equal(t, 1, exitcode.FromError(err))
	require.False(t, e.complete)
}

func TestMigrationReviewDesiredCountMaxIntFastRefusal(t *testing.T) {
	v, _ := nativeTarget(t)
	ir, pods, cr := migrationSources(v)
	count := int32(2147483647)
	ir.Spec.Replicas = &count
	started := time.Now()
	e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
	require.ErrorIs(t, err, ErrBounds)
	require.Equal(t, 1, exitcode.FromError(err))
	require.False(t, e.complete)
	require.Less(t, time.Since(started), time.Second)
}

func TestMigrationReviewPlanningCountBoundaryPreservesSparseIdentity(t *testing.T) {
	for _, count := range []*int32{nil, new(int32), func() *int32 { v := int32(2048); return &v }()} {
		v, _ := nativeTarget(t)
		ir, pods, cr := migrationSources(v)
		ir.Spec.Replicas = count
		ir.Status.InstanceStatuses[0].Index = 2147483647
		pods[0].Name = "chat-engine-2147483647-default-0"
		pods[0].Labels["ome.io/instance-index"] = "2147483647"
		e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 2147483647}, testClock)
		require.NoError(t, err)
		require.True(t, e.complete)
		require.Equal(t, int32(2147483647), e.source.Index)
	}
}

// Struct decoder case folding must not grant exact retained payload authority.
func TestMigrationReviewRetainedTopKeysExact(t *testing.T) {
	for _, field := range []string{`"Component":"engine"`, `"Component":"decoder"`, `"Requested_By":"kubectl-ome"`, `"Requested_By":"other"`, `"SchemaVersion":"v1"`, `"Instance":3`, `"From_Node":"node-a"`, `"Hint_Target_Nodes":[]`, `"Reason":""`, `"Requested_At":"2026-09-15T21:00:00Z"`} {
		t.Run(field, func(t *testing.T) {
			raw := strings.TrimSuffix(migrationTestPayload, "}") + "," + field + "}"
			_, err := parseMigrationRequest(raw)
			require.Error(t, err)
		})
	}
}

func TestMigrationReviewExactReorderedOptionalEmptySchema(t *testing.T) {
	r, err := parseMigrationRequest(`{"requested_by":"kubectl-ome","reason":"","hint_target_nodes":[],"requested_at":"","from_node":"node-a","instance":3,"component":"engine","schemaVersion":"v1"}`)
	require.NoError(t, err)
	require.Equal(t, "engine", r.Component)
	require.Equal(t, int32(3), r.Instance)
	require.Empty(t, r.Reason)
	require.Empty(t, r.HintNodes)
	require.Empty(t, r.RequestedAt)
}

// Required nonempty audit input is a ledger object, never a null document.
// Empty actor object encodings and unknown additive audit fields remain valid.
func TestMigrationReviewAuditRootObject(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		code int
	}{{`null`, 1}, {` null `, 1}, {`{}`, 0}, {`{"entries":null}`, 0}, {`{"entries":[]}`, 0}, {`{"entries":null,"future":{"nested":[true]}}`, 0}, {`[]`, 1}, {`"private"`, 1}} {
		t.Run(tc.raw, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			v.Annotations = map[string]string{constants.MigrationRequestAnnotationPrefix + migrationTestID: migrationTestPayload}
			controller := true
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: v.Name + "-ome-migration-audit", Namespace: v.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: v.Name, UID: v.UID, Controller: &controller}}}, Data: map[string]string{"history.json": tc.raw}}
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr, cm), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome", RequestID: migrationTestID}, testClock)
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Equal(t, tc.code == 0, e.complete)
			if tc.code != 0 {
				require.Error(t, err)
			}
		})
	}
}
