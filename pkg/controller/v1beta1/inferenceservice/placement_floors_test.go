package inferenceservice

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementReplicaFloorRecovery(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary service", true: "repaired authority"}[held], func(t *testing.T) {
			service := &v1beta1.InferenceService{}
			service.Status.Conditions = []apis.Condition{{Type: apis.ConditionReady, Status: corev1.ConditionTrue}}
			want := service.DeepCopy()
			if held {
				placementBackendConditions.Manage(&service.Status).SetCondition(apis.Condition{Type: v1beta1.PlacementReplicaFloorsReady, Status: corev1.ConditionFalse})
			}
			clearPlacementReplicaFloorsHold(service)
			if held {
				condition := service.Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
				if condition == nil || condition.Status != corev1.ConditionTrue || condition.Reason != "ReplicaFloorsAccepted" {
					t.Fatalf("recovery: %+v", condition)
				}
				service.Status.Conditions = slices.DeleteFunc(service.Status.Conditions, func(c apis.Condition) bool { return c.Type == v1beta1.PlacementReplicaFloorsReady })
			}
			if diff := cmp.Diff(want, service); diff != "" {
				t.Fatalf("serving status changed: %s", diff)
			}
		})
	}
}
