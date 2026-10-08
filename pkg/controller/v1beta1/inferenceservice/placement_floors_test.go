package inferenceservice

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
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

func TestPlacementReplicaFloorsFollowEachComponent(t *testing.T) {
	floors := []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 30}, {Component: v1beta1.DecoderComponent, Replicas: 20}, {Component: v1beta1.RouterComponent, Replicas: 1}}
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceServiceSpec)
		wantErr bool
	}{
		{name: "ratio floors match their components"},
		{name: "missing decoder", edit: func(spec *v1beta1.InferenceServiceSpec) { spec.Decoder = nil }, wantErr: true},
		{name: "decoder cannot borrow the engine floor", edit: func(spec *v1beta1.InferenceServiceSpec) { spec.Decoder.MinReplicas = ptr.To(30) }, wantErr: true},
		{name: "router keeps its own floor", edit: func(spec *v1beta1.InferenceServiceSpec) { spec.Router.MinReplicas = ptr.To(2) }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", ReplicaFloors: floors}
			raw, err := protocol.Encode(policy)
			if err != nil {
				t.Fatal(err)
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PlacementOriginUID: "source-a", constants.PlacementExecution: raw}}}
			spec := v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(30)}},
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(20)}},
				Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}},
			}
			if tt.edit != nil {
				tt.edit(&spec)
			}
			err = checkPlacementReplicaFloors(service, spec.Engine, spec.Decoder, spec.Router)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("member floor check (-want +got): %s: %v", diff, err)
			}
		})
	}
}
