package inferenceservice

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestPlacementReplicaFloorRecovery(t *testing.T) {
	for _, tt := range []struct {
		name   string
		seeded *apis.Condition
	}{
		{name: "ordinary service"},
		{name: "repaired authority", seeded: &apis.Condition{Type: v1beta1.PlacementReplicaFloorsReady, Status: corev1.ConditionFalse}},
		{name: "policy granted after waiting", seeded: &apis.Condition{Type: v1beta1.PlacementReplicaFloorsReady, Status: corev1.ConditionUnknown, Reason: "ExecutionPolicyPending"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{}
			service.Status.Conditions = []apis.Condition{{Type: apis.ConditionReady, Status: corev1.ConditionTrue}}
			want := service.DeepCopy()
			if tt.seeded != nil {
				placementBackendConditions.Manage(&service.Status).SetCondition(*tt.seeded)
			}
			clearPlacementReplicaFloorsHold(service)
			if tt.seeded != nil {
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

func TestPlacementExecutionPendingWrites(t *testing.T) {
	for _, tt := range []struct {
		name                                        string
		conflict, missing, fail, repeat, noRecorder bool
		want                                        ctrl.Result
		wantErr                                     bool
		wantWarnings                                int
	}{
		{name: "condition written and warned once", wantWarnings: 1},
		{name: "repeated pass neither writes nor warns again", repeat: true, wantWarnings: 1},
		{name: "without recorder", noRecorder: true},
		{name: "conflict requeues", conflict: true, want: ctrl.Result{Requeue: true}},
		{name: "deleted member", missing: true},
		{name: "write error", fail: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "uid"}}
			writes := fakeclient.CountingVerb(fakeclient.StatusPatch)
			opts := []fakeclient.Option{fakeclient.WithObjects(service), fakeclient.WithStatusSubresource(service), writes}
			var refusal error
			switch {
			case tt.conflict:
				refusal = apierrors.NewConflict(schema.GroupResource{Resource: "inferenceservices"}, service.Name, errors.New("changed"))
			case tt.missing:
				refusal = apierrors.NewNotFound(schema.GroupResource{Resource: "inferenceservices"}, service.Name)
			case tt.fail:
				refusal = errors.New("unavailable")
			}
			if refusal != nil {
				opts = append(opts, fakeclient.FailingVerb(fakeclient.StatusPatch, fakeclient.EveryCall, refusal))
			}
			cl := fakeclient.New(t, opts...)
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(service), service); err != nil {
				t.Fatal(err)
			}
			r := InferenceServiceReconciler{Client: cl}
			var recorder *record.FakeRecorder
			if !tt.noRecorder {
				recorder = record.NewFakeRecorder(4)
				r.Recorder = recorder
			}
			got, err := r.notePlacementExecutionPending(t.Context(), service)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error = %v: %s", err, diff)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			if tt.repeat {
				if got, err := r.notePlacementExecutionPending(t.Context(), service); err != nil || !got.IsZero() {
					t.Fatalf("repeat = %v, %v", got, err)
				}
			}
			if diff := cmp.Diff(1, writes.Count()); diff != "" {
				t.Fatalf("status writes (-want +got):\n%s", diff)
			}
			if recorder != nil {
				if diff := cmp.Diff(tt.wantWarnings, pendingPolicyWarnings(recorder)); diff != "" {
					t.Fatalf("warnings (-want +got):\n%s", diff)
				}
			}
			if tt.conflict || tt.missing || tt.fail {
				return
			}
			stored := &v1beta1.InferenceService{}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(service), stored); err != nil {
				t.Fatal(err)
			}
			condition := stored.Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			if condition == nil || condition.Status != corev1.ConditionUnknown || condition.Reason != placementExecutionPendingReason || condition.Message != placementExecutionPendingMessage {
				t.Fatalf("stored condition: %+v", condition)
			}
		})
	}
}
