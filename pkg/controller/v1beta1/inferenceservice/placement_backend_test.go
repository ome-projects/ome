package inferenceservice

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestPlacementBackendReconcile(t *testing.T) {
	type fixture struct {
		service *v1beta1.InferenceService
		runtime *v1beta1.ClusterServingRuntime
		deploy  string
	}
	stampFloors := func(f *fixture, floor int32) {
		raw, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: floor > 0,
			ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: floor}}})
		if err != nil {
			t.Fatal(err)
		}
		f.service.Annotations[constants.PlacementExecution] = raw
	}
	for _, tt := range []struct {
		name          string
		change        func(*fixture)
		wantHold      bool
		wantFloorHold bool
		wantVirtual   bool
	}{
		{name: "native proceeds"},
		{name: "legacy member retains raw deployment", change: func(f *fixture) {
			delete(f.service.Annotations, constants.PlacementPolicy)
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "legacy member retains multi-node deployment", change: func(f *fixture) {
			delete(f.service.Annotations, constants.PlacementPolicy)
			f.service.Spec.DeploymentMode = ptr.To(constants.MultiNode)
		}},
		{name: "explicit zero overrides runtime floor", change: func(f *fixture) {
			f.service.Spec.Engine.MinReplicas = ptr.To(0)
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(4)
			stampFloors(f, 0)
		}},
		{name: "runtime zero overrides configured floor", change: func(f *fixture) {
			f.deploy = `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":2}}`
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(0)
			stampFloors(f, 0)
		}},
		{name: "unaccepted zero floor holds", wantFloorHold: true, change: func(f *fixture) {
			f.service.Spec.Engine.MinReplicas = ptr.To(0)
			stampFloors(f, 2)
		}},
		{name: "ordinary local zero ignores accepted floor", change: func(f *fixture) {
			f.service.Spec.Engine.MinReplicas = ptr.To(0)
			stampFloors(f, 2)
			delete(f.service.Annotations, constants.PlacementOriginUID)
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "configured replica floor", change: func(f *fixture) {
			f.deploy = `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":2}}`
			stampFloors(f, 2)
		}},
		{name: "changed configured floor", wantFloorHold: true, change: func(f *fixture) {
			f.deploy = `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":4}}`
			stampFloors(f, 2)
		}},
		{name: "accepted runtime floor", change: func(f *fixture) {
			f.deploy = `{"defaultDeploymentMode":"RawDeployment","replicas":{"defaultMinReplicas":2}}`
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(4)
			stampFloors(f, 4)
		}},
		{name: "source floor overrides runtime", change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(4)
			f.service.Spec.Engine.MinReplicas = ptr.To(2)
			stampFloors(f, 2)
		}},
		{name: "runtime floor exceeds authority", wantFloorHold: true, change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(4)
			stampFloors(f, 2)
		}},
		{name: "runtime floor below authority", wantFloorHold: true, change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(1)
			stampFloors(f, 2)
		}},
		{name: "unresolved floor cannot use a default", wantFloorHold: true, change: func(f *fixture) { stampFloors(f, 2) }},
		{name: "resolved router missing from authority", wantFloorHold: true, change: func(f *fixture) {
			f.service.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}}
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(2)
			stampFloors(f, 2)
		}},
		{name: "ordinary local ignores accepted floor", change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.MinReplicas = ptr.To(4)
			stampFloors(f, 2)
			delete(f.service.Annotations, constants.PlacementOriginUID)
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "legacy member label", change: func(f *fixture) {
			f.service.Annotations = nil
			f.service.Labels = map[string]string{constants.PlacementOrigin: "source-a"}
		}},
		{name: "raw engine", wantHold: true, change: func(f *fixture) { f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment) }},
		{name: "multinode engine", wantHold: true, change: func(f *fixture) { f.service.Spec.DeploymentMode = ptr.To(constants.MultiNode) }},
		{name: "virtual service", wantHold: true, change: func(f *fixture) { f.service.Spec.DeploymentMode = ptr.To(constants.VirtualDeployment) }},
		{name: "virtual service annotation", wantHold: true, change: func(f *fixture) {
			f.service.Annotations[constants.DeploymentMode] = string(constants.VirtualDeployment)
		}},
		{name: "raw runtime engine overrides native spec", wantHold: true, change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
		}},
		{name: "raw runtime decoder", wantHold: true, change: func(f *fixture) {
			f.service.Spec.Decoder = &v1beta1.DecoderSpec{}
			f.runtime.Spec.DecoderConfig = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
		}},
		{name: "raw router", wantHold: true, change: func(f *fixture) {
			f.service.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
		}},
		{name: "undeclared raw decoder ignored", change: func(f *fixture) {
			f.runtime.Spec.DecoderConfig = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
		}},
		{name: "component override restores native", change: func(f *fixture) {
			f.runtime.Spec.EngineConfig.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
			f.service.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
		}},
		{name: "local raw unchanged", change: func(f *fixture) {
			f.service.Annotations = nil
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "local multinode unchanged", change: func(f *fixture) {
			f.service.Annotations = nil
			f.service.Spec.DeploymentMode = ptr.To(constants.MultiNode)
		}},
		{name: "local execution annotation ignored", change: func(f *fixture) {
			f.service.Annotations = map[string]string{constants.PlacementExecution: "{"}
			f.service.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
		}},
		{name: "local virtual unchanged", wantVirtual: true, change: func(f *fixture) {
			f.service.Annotations = nil
			f.service.Spec.DeploymentMode = ptr.To(constants.VirtualDeployment)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, corev1.AddToScheme} {
				if err := add(s); err != nil {
					t.Fatal(err)
				}
			}
			f := fixture{
				deploy:  `{"defaultDeploymentMode":"RawDeployment"}`,
				service: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "member-uid", Finalizers: []string{inferenceServiceFinalizer}, Annotations: map[string]string{constants.PlacementOriginUID: "source-a", constants.PlacementPolicy: string(v1beta1.PlacementPolicyClusterAffinity)}}, Spec: v1beta1.InferenceServiceSpec{DeploymentMode: ptr.To(constants.OMENative), Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime-a"}, Model: &v1beta1.ModelRef{Name: "model-a"}, Engine: &v1beta1.EngineSpec{}}},
				runtime: &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example:v1"}}}}},
			}
			f.service.Status.Conditions = append(f.service.Status.Conditions, apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue, Reason: "Serving"})
			f.service.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {}}
			if tt.change != nil {
				tt.change(&f)
			}
			childWrite := errors.New("model configuration write reached")
			writes := 0
			reject := func() error { writes++; return childWrite }
			cl := clientfake.NewClientBuilder().WithScheme(s).WithObjects(f.service, f.runtime,
				&v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a"}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}}).WithStatusSubresource(f.service).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error { return reject() },
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error { return reject() },
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					return reject()
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return reject() },
			}).Build()
			before := &v1beta1.InferenceService{}
			key := client.ObjectKeyFromObject(f.service)
			if err := cl.Get(t.Context(), key, before); err != nil {
				t.Fatal(err)
			}
			r := &InferenceServiceReconciler{Client: cl, APIReader: cl, Scheme: s, Log: logr.Discard(), Recorder: record.NewFakeRecorder(20), RuntimeSelector: runtimeselector.New(cl), Clientset: fake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace}, Data: map[string]string{"deploy": f.deploy}})}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			wantWrites := 1
			if tt.wantHold || tt.wantFloorHold || tt.wantVirtual {
				wantWrites = 0
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, childWrite) {
				t.Fatalf("error = %v, want continuation to model configuration", err)
			}
			if diff := cmp.Diff(wantWrites, writes); diff != "" {
				t.Fatalf("child writes (-want +got):\n%s", diff)
			}
			stored := &v1beta1.InferenceService{}
			if err := cl.Get(t.Context(), key, stored); err != nil {
				t.Fatal(err)
			}
			condition := stored.Status.GetCondition(v1beta1.PlacementBackendReady)
			if tt.wantHold {
				if condition == nil || condition.Status != corev1.ConditionFalse || condition.Reason != "UnsupportedPlacementBackend" {
					t.Fatalf("backend condition: %+v", condition)
				}
				stored.Status.Conditions = slices.DeleteFunc(stored.Status.Conditions, func(c apis.Condition) bool { return c.Type == v1beta1.PlacementBackendReady })
			} else if condition != nil {
				t.Fatalf("unexpected backend condition: %+v", condition)
			}
			floorCondition := stored.Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			if tt.wantFloorHold {
				if floorCondition == nil || floorCondition.Status != corev1.ConditionFalse || floorCondition.Reason != "ReplicaFloorsMismatch" {
					t.Fatalf("replica floor condition: %+v", floorCondition)
				}
				stored.Status.Conditions = slices.DeleteFunc(stored.Status.Conditions, func(c apis.Condition) bool { return c.Type == v1beta1.PlacementReplicaFloorsReady })
			} else if floorCondition != nil {
				t.Fatalf("unexpected replica floor condition: %+v", floorCondition)
			}
			if !tt.wantVirtual {
				before.ResourceVersion = stored.ResourceVersion
				if diff := cmp.Diff(before, stored); diff != "" {
					t.Fatalf("service preservation (-want +got):\n%s", diff)
				}
			} else if got := stored.Status.GetCondition(apis.ConditionReady); got == nil || got.Reason != "VirtualDeployment" {
				t.Fatalf("virtual status: %+v", got)
			}
		})
	}
}

func TestPlacementBackendConditionWrites(t *testing.T) {
	for _, tt := range []struct {
		name                            string
		conflict, missing, fail, repeat bool
		want                            ctrl.Result
		wantErr                         bool
	}{
		{name: "hold condition written"},
		{name: "repeated hold avoids writes", repeat: true},
		{name: "conflict requeues", conflict: true, want: ctrl.Result{Requeue: true}},
		{name: "deleted member", missing: true},
		{name: "write error", fail: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := v1beta1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "uid"}}
			writes := 0
			cl := clientfake.NewClientBuilder().WithScheme(s).WithObjects(service).WithStatusSubresource(service).WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				writes++
				if tt.conflict {
					return apierrors.NewConflict(schema.GroupResource{Resource: "inferenceservices"}, service.Name, errors.New("changed"))
				}
				if tt.missing {
					return apierrors.NewNotFound(schema.GroupResource{Resource: "inferenceservices"}, service.Name)
				}
				if tt.fail {
					return errors.New("unavailable")
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			}}).Build()
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(service), service); err != nil {
				t.Fatal(err)
			}
			r := InferenceServiceReconciler{Client: cl}
			cause := errors.New("unsupported backend")
			got, err := r.holdPlacementBackend(t.Context(), service, cause)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error = %v: %s", err, diff)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			if tt.repeat {
				if _, err := r.holdPlacementBackend(t.Context(), service, cause); err != nil {
					t.Fatal(err)
				}
			}
			if diff := cmp.Diff(1, writes); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestClearPlacementBackendHold(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "unaffected service", true: "backend repaired"}[held], func(t *testing.T) {
			service := &v1beta1.InferenceService{}
			service.Status.Conditions = append(service.Status.Conditions, apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue})
			want := service.DeepCopy()
			if held {
				placementBackendConditions.Manage(&service.Status).SetCondition(apis.Condition{Type: v1beta1.PlacementBackendReady, Status: corev1.ConditionFalse})
			}
			clearPlacementBackendHold(service)
			if held {
				got := service.Status.GetCondition(v1beta1.PlacementBackendReady)
				if got == nil || got.Status != corev1.ConditionTrue || got.Reason != "OMENativeResolved" {
					t.Fatalf("recovery: %+v", got)
				}
				service.Status.Conditions = slices.DeleteFunc(service.Status.Conditions, func(c apis.Condition) bool { return c.Type == v1beta1.PlacementBackendReady })
			}
			if diff := cmp.Diff(want, service); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
