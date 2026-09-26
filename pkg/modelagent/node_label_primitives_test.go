package modelagent

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestStrictReadinessWithdrawal(t *testing.T) {
	for _, state := range []ModelStateOnNode{Deleted, Updating} {
		for _, boundary := range []string{"success", "API error", "ineffective patch", "changed intent", "replaced node"} {
			t.Run(string(state)+"/"+boundary, func(t *testing.T) {
				ctx := context.Background()
				model := createTestBaseModel()
				model.UID = "model-uid"
				label := constants.GetBaseModelLabel(model.Namespace, model.Name)
				requestLabel := constants.GetModelArtifactRequestLabel(model.UID)
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1",
					Labels: map[string]string{label: "Ready", requestLabel: "R1", "unrelated": "keep"}}}
				client := fake.NewSimpleClientset(node)
				reconciler := NewNodeLabelReconciler(node.Name, client, 2, zaptest.NewLogger(t).Sugar())
				current := true
				patches := 0
				failure := apierrors.NewServiceUnavailable("test outage")
				client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					patches++
					switch boundary {
					case "API error":
						return true, nil, failure
					case "ineffective patch":
						return true, node, nil
					case "changed intent":
						current = false
					case "replaced node":
						replacement := node.DeepCopy()
						replacement.UID = "new-node"
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), replacement, ""))
					default:
						return false, nil, nil
					}
					return true, nil, apierrors.NewConflict(corev1.Resource("nodes"), node.Name, fmt.Errorf("snapshot changed"))
				})
				op := &NodeLabelOp{BaseModel: model, ModelStateOnNode: state, validateCurrent: func() error {
					if !current {
						return fmt.Errorf("intent changed")
					}
					return nil
				}}
				err := reconciler.WithdrawReadiness(ctx, op)
				if boundary == "success" {
					require.NoError(t, err)
					actual, err := client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
					require.NoError(t, err)
					require.Equal(t, "keep", actual.Labels["unrelated"])
					require.NotEqual(t, "Ready", actual.Labels[label])
					if state == Deleted {
						require.NotContains(t, actual.Labels, label)
						require.NotContains(t, actual.Labels, requestLabel)
					}
				} else {
					require.Error(t, err)
					if boundary == "API error" {
						require.ErrorIs(t, err, failure)
					}
					if boundary == "changed intent" || boundary == "replaced node" {
						require.Equal(t, 1, patches, "retry must validate before another patch")
					}
				}
			})
		}
	}
}

func TestNodeReadinessPublishesRequestPair(t *testing.T) {
	ctx := context.Background()
	model := createTestBaseModel()
	model.UID = "model-uid"
	label := constants.GetBaseModelLabel(model.Namespace, model.Name)
	requestLabel := constants.GetModelArtifactRequestLabel(model.UID)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1",
		Labels: map[string]string{label: "Ready", requestLabel: "R1"}}}
	client := fake.NewSimpleClientset(node)
	reconciler := NewNodeLabelReconciler(node.Name, client, 1, zaptest.NewLogger(t).Sugar())
	op := &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready, nodeUID: node.UID, artifactRehydrationID: "R2"}
	require.NoError(t, reconciler.ReconcileNodeLabels(op))
	actual, err := client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", actual.Labels[label])
	require.Equal(t, "R2", actual.Labels[requestLabel])
	op.ModelStateOnNode, op.requestLabelOnly = Deleted, true
	require.NoError(t, reconciler.ReconcileNodeLabels(op))
	actual, err = client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", actual.Labels[label], "old UID cleanup must preserve the name-based label")
	require.NotContains(t, actual.Labels, requestLabel)
}
