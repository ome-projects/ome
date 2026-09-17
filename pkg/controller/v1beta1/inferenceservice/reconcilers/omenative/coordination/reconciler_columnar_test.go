package coordination

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// TestReconcile_GroupObservationDecodesColumnarV2 pins the observation read
// path at the reconcile level: the cached client carries no row decoder, so
// the reconcile must pair it with the decoder the live reader carries. With a
// ColumnarV2-stored InferenceReplica in a declared group, the pass succeeds
// with a decoding reader and fails closed without one.
func TestReconcile_GroupObservationDecodesColumnarV2(t *testing.T) {
	newFixture := func() (*v1beta1.InferenceService, runtime.Object, runtime.Object) {
		isvc := testOMENativeISVC()
		isvc.Spec.Rollout = singleEngineGroup()
		revision := isvc.Name + "-engine-engineHash"
		pod := buildPod(isvc, v1beta1.EngineComponent, "engineHash", 0)
		engineIR := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas:        1,
				ReadyReplicas:   1,
				CurrentRevision: revision,
				UpdateRevision:  revision,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revision},
				},
			},
		}
		return isvc, columnarTwin(t, engineIR), pod
	}

	t.Run("decodes through the live reader's decoder", func(t *testing.T) {
		isvc, stored, pod := newFixture()
		c := testClient(pod, stored)
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC:                     isvc,
			Client:                   c,
			Reader:                   irstatus.NewReader(c, irstatus.NewDecoder(64)),
			Now:                      time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
			ComponentRunnerPorts:     testComponentRunnerPorts(),
		}); err != nil {
			t.Fatalf("reconcile with a ColumnarV2-stored InferenceReplica: %v", err)
		}
		if isvc.Status.RolloutCoordination == nil || len(isvc.Status.RolloutCoordination.Groups) != 1 {
			t.Fatalf("group observation must be recorded: %+v", isvc.Status.RolloutCoordination)
		}
	})

	t.Run("fails closed without a decoder", func(t *testing.T) {
		isvc, stored, pod := newFixture()
		c := testClient(pod, stored)
		_, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC:                     isvc,
			Client:                   c,
			Reader:                   c,
			Now:                      time.Now(),
			ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent),
			ComponentRunnerPorts:     testComponentRunnerPorts(),
		})
		if err == nil || !strings.Contains(err.Error(), "cardinality limit") {
			t.Fatalf("a reader without a decoder must fail the pass on a ColumnarV2 object, got %v", err)
		}
	})
}
