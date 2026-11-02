package inferenceservice

import (
	"context"
	"fmt"
	"strings"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// followerFixture is an engine the decoder follows at ratio 1, both
// OMENative and both at 8 Instances. The engine's count is written by an
// external scaler, so the projector keeps whatever the engine replica
// carries; the decoder's count is the coordinator's. With canary set the two
// share one canary group whose first step stages 25 percent, so 6 of 8
// decoders are held and 2 staged.
type followerFixture struct {
	client      client.Client
	reconciler  *InferenceServiceReconciler
	recorder    *record.FakeRecorder
	scaleWrites int
	key         types.NamespacedName
}

type followerOptions struct {
	canary bool
	// anchorByProjector gives the engine no autoscaler, so the projector
	// writes spec.engine.minReplicas on it every pass.
	anchorByProjector bool
	// decoderPatchConflicts makes every patch of the decoder replica fail
	// with a conflict, the way a write against a stale copy fails.
	decoderPatchConflicts bool
}

const (
	followerNamespace = "default"
	followerService   = "pair"
)

func newFollowerFixture(t *testing.T, opts followerOptions) *followerFixture {
	t.Helper()
	scheme := fakeclient.Scheme(t, monitoringv1.AddToScheme)
	runner := func(image string) *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: v1.Container{Name: constants.MainContainerName, Image: image}}
	}
	engineExt := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(8), MaxReplicas: 16,
		Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}}
	if opts.anchorByProjector {
		engineExt.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: followerService, Namespace: followerNamespace,
			UID: types.UID(followerService + "-uid"), Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "pair-runtime", Kind: ptr.To("ServingRuntime")},
			Engine:         &v1beta1.EngineSpec{Runner: runner("fake-serving:v1"), ComponentExtensionSpec: engineExt},
			Decoder: &v1beta1.DecoderSpec{Runner: runner("fake-serving:v1"),
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 16}},
			ScalingPolicy: &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional,
				Proportional: &v1beta1.ProportionalPolicy{Anchor: v1beta1.EngineComponent,
					Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse("1")}}},
		},
		Status: v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {},
		}},
	}
	if opts.canary {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 25},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		}}}
	}
	rt := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "pair-runtime", Namespace: followerNamespace},
		Spec: v1beta1.ServingRuntimeSpec{ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []v1.Container{{Name: constants.MainContainerName, Image: "fake-serving:v1"}},
		}},
	}
	replica := func(component v1beta1.ComponentType, autoscaler *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
		return &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Name: followerService + "-" + string(component), Namespace: followerNamespace, Generation: 1,
				UID: types.UID(followerService + "-" + string(component) + "-uid"),
				Annotations: map[string]string{
					constants.InferenceReplicaControllerWriteAnnotationKey:  constants.InferenceReplicaControllerWriteAnnotationVal,
					constants.InferenceReplicaParentGenerationAnnotationKey: "1",
				},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
			},
			Spec: v1beta1.InferenceReplicaSpec{
				ParentRef: &v1beta1.ParentReference{Name: followerService}, Component: component,
				Replicas: ptr.To(int32(8)), Autoscaler: autoscaler,
			},
			Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 1, Replicas: 8, ReadyReplicas: 8,
				ServingReplicas: 8, AvailableReplicas: 8, UpdatedReplicas: 8, UpdatedReadyReplicas: 8},
		}
	}
	engine := replica(v1beta1.EngineComponent, engineExt.Autoscaler.DeepCopy())
	decoder := replica(v1beta1.DecoderComponent, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})

	f := &followerFixture{key: types.NamespacedName{Name: followerService, Namespace: followerNamespace}}
	// The fake client has no scale subresource for a custom resource; model
	// it so a write through it lands on spec.replicas and is counted.
	funcs := interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, c client.Client, name string, obj client.Object, sub client.Object, opts ...client.SubResourceGetOption) error {
			if name != "scale" {
				return c.SubResource(name).Get(ctx, obj, sub, opts...)
			}
			live := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
				return err
			}
			scale := sub.(*autoscalingv1.Scale)
			scale.ResourceVersion = live.ResourceVersion
			scale.Spec.Replicas = ptr.Deref(live.Spec.Replicas, 0)
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if name != "scale" {
				return c.SubResource(name).Update(ctx, obj, opts...)
			}
			options := &client.SubResourceUpdateOptions{}
			options.ApplyOptions(opts)
			scale := options.SubResourceBody.(*autoscalingv1.Scale)
			live := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
				return err
			}
			live.Spec.Replicas = ptr.To(scale.Spec.Replicas)
			if err := c.Update(ctx, live); err != nil {
				return err
			}
			f.scaleWrites++
			return nil
		},
	}
	if opts.decoderPatchConflicts {
		funcs.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*v1beta1.InferenceReplica); ok && obj.GetName() == decoder.Name {
				return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}, obj.GetName(), fmt.Errorf("stale copy"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		}
	}
	f.client = fakeclient.New(t, fakeclient.WithScheme(scheme),
		fakeclient.WithObjects(isvc, rt, engine, decoder), fakeclient.WithStatusSubresource(isvc, engine, decoder),
		fakeclient.WithInterceptors(funcs))

	clientset := fake.NewClientset()
	if _, err := clientset.CoreV1().ConfigMaps("ome").Create(context.TODO(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data:       map[string]string{"deploy": `{"defaultDeploymentMode": "RawDeployment"}`},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.recorder = record.NewFakeRecorder(64)
	f.reconciler = &InferenceServiceReconciler{
		Client: f.client, APIReader: f.client, ClientConfig: &rest.Config{}, Clientset: clientset,
		Log: ctrl.Log.WithName("test"), Scheme: scheme, Recorder: f.recorder,
		RuntimeSelector:          runtimeselector.New(f.client),
		AcceleratorClassSelector: acceleratorclassselector.New(f.client),
	}
	return f
}

// pass runs one reconcile of the service.
func (f *followerFixture) pass(t *testing.T) ctrl.Result {
	t.Helper()
	result, err := f.reconciler.Reconcile(context.TODO(), ctrl.Request{NamespacedName: f.key})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

func (f *followerFixture) replica(t *testing.T, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := f.client.Get(context.TODO(), types.NamespacedName{Name: followerService + "-" + string(component), Namespace: followerNamespace}, ir); err != nil {
		t.Fatal(err)
	}
	return ir
}

// scaleAnchor is the external scaler's write of the engine's count.
func (f *followerFixture) scaleAnchor(t *testing.T, replicas int32) {
	t.Helper()
	ir := f.replica(t, v1beta1.EngineComponent)
	ir.Spec.Replicas = ptr.To(replicas)
	if err := f.client.Update(context.TODO(), ir); err != nil {
		t.Fatal(err)
	}
}

func (f *followerFixture) service(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	isvc := &v1beta1.InferenceService{}
	if err := f.client.Get(context.TODO(), f.key, isvc); err != nil {
		t.Fatal(err)
	}
	return isvc
}

func (f *followerFixture) scalingStatus(t *testing.T) *v1beta1.ScalingPolicyStatus {
	t.Helper()
	status := f.service(t).Status.ScalingPolicy
	if status == nil {
		t.Fatal("the coordinator published no status")
	}
	return status
}

func replicasAndPartition(ir *v1beta1.InferenceReplica) (int32, string) {
	partition := "none"
	if ir.Spec.Pacing != nil && ir.Spec.Pacing.Partition != nil {
		partition = fmt.Sprint(*ir.Spec.Pacing.Partition)
	}
	return ptr.Deref(ir.Spec.Replicas, -1), partition
}

func expectReplica(t *testing.T, ir *v1beta1.InferenceReplica, replicas int32, partition string) {
	t.Helper()
	gotReplicas, gotPartition := replicasAndPartition(ir)
	if gotReplicas != replicas || gotPartition != partition {
		t.Fatalf("%s carries %d Instances with partition %s; want %d with partition %s",
			ir.Spec.Component, gotReplicas, gotPartition, replicas, partition)
	}
}

func expectFollowerStatus(t *testing.T, status *v1beta1.ScalingPolicyStatus, anchor, target, current int32, reason string) {
	t.Helper()
	if status.AnchorDesired != anchor || len(status.Followers) != 1 ||
		status.Followers[0].Target != target || status.Followers[0].Current != current || status.Followers[0].Clamped {
		t.Fatalf("status = %+v; want anchor %d, decoder target %d current %d, not clamped", status, anchor, target, current)
	}
	condition := meta.FindStatusCondition(status.Conditions, "Ready")
	if condition == nil || condition.Reason != reason {
		t.Fatalf("Ready = %+v; want reason %s", condition, reason)
	}
	wantState := metav1.ConditionFalse
	if reason == "Applied" {
		wantState = metav1.ConditionTrue
	}
	if condition.Status != wantState {
		t.Fatalf("Ready = %+v; want status %s", condition, wantState)
	}
}

// The engine doubles from 8 to 16 under a 25 percent canary step. The decoder
// follows in the pass that sees the new count, and its replica never carries
// the new count beside the old step: 16 Instances and a partition of 12
// (16 less the 4 the step stages) land in one write.
func TestFollowerCountAndPartitionLandInOneWrite(t *testing.T) {
	f := newFollowerFixture(t, followerOptions{canary: true})
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 8, "6")
	expectFollowerStatus(t, f.scalingStatus(t), 8, 8, 8, "Applied")

	f.scaleAnchor(t, 16)
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.EngineComponent), 16, "12")
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 16, "12")
	expectFollowerStatus(t, f.scalingStatus(t), 16, 16, 16, "Applied")
	if f.scaleWrites != 0 {
		t.Fatalf("the coordinator wrote the follower's scale subresource %d times; the projection carries the count", f.scaleWrites)
	}
	select {
	case event := <-f.recorder.Events:
		if strings.Contains(event, "ScalingTargetClamped") {
			t.Fatalf("an unclamped target raised %s", event)
		}
	default:
	}
}

// A pass in which the anchor stands still writes nothing to the follower.
func TestFollowerWithAnUnchangedTargetIsNotRewritten(t *testing.T) {
	f := newFollowerFixture(t, followerOptions{canary: true})
	f.scaleAnchor(t, 16)
	f.pass(t)
	before := f.replica(t, v1beta1.DecoderComponent)
	expectReplica(t, before, 16, "12")
	f.pass(t)
	after := f.replica(t, v1beta1.DecoderComponent)
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("the decoder replica was rewritten (%s to %s) while its target stood at 16", before.ResourceVersion, after.ResourceVersion)
	}
	expectFollowerStatus(t, f.scalingStatus(t), 16, 16, 16, "Applied")
	if f.scaleWrites != 0 {
		t.Fatalf("scale subresource writes = %d; want none", f.scaleWrites)
	}
}

// Without a canary the follower's replica carries its count and no partition.
func TestFollowerUnderNoCanaryCarriesNoPartition(t *testing.T) {
	f := newFollowerFixture(t, followerOptions{})
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 8, "none")
	f.scaleAnchor(t, 16)
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 16, "none")
	expectFollowerStatus(t, f.scalingStatus(t), 16, 16, 16, "Applied")
	f.scaleAnchor(t, 4)
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 4, "none")
	expectFollowerStatus(t, f.scalingStatus(t), 4, 4, 4, "Applied")
	if f.scaleWrites != 0 {
		t.Fatalf("scale subresource writes = %d; want none", f.scaleWrites)
	}
}

// An anchor the projector itself scales (no autoscaler: the projector writes
// the declared minimum) moves in the pass that reads the edit. The coordinator
// read the anchor before that write, so the follower stands for that pass,
// the status says so, and the follower moves in the next pass, again with its
// count and its partition in one write.
func TestFollowerOfAProjectorScaledAnchorMovesOnTheNextPass(t *testing.T) {
	f := newFollowerFixture(t, followerOptions{canary: true, anchorByProjector: true})
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.EngineComponent), 8, "6")
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 8, "6")

	isvc := f.service(t)
	isvc.Spec.Engine.MinReplicas = ptr.To(16)
	if err := f.client.Update(context.TODO(), isvc); err != nil {
		t.Fatal(err)
	}
	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.EngineComponent), 16, "12")
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 8, "6")
	expectFollowerStatus(t, f.scalingStatus(t), 16, 16, 8, "FollowerPending")

	f.pass(t)
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 16, "12")
	expectFollowerStatus(t, f.scalingStatus(t), 16, 16, 16, "Applied")
	if f.scaleWrites != 0 {
		t.Fatalf("scale subresource writes = %d; want none", f.scaleWrites)
	}
}

// When the follower's projection cannot be written this pass, the status says
// which count the replica still carries and which it is meant to carry.
func TestFollowerWhoseProjectionFailsReportsThePendingCount(t *testing.T) {
	f := newFollowerFixture(t, followerOptions{canary: true, decoderPatchConflicts: true})
	f.scaleAnchor(t, 16)
	result := f.pass(t)
	if !result.Requeue {
		t.Fatalf("a conflicting projection must ask for another pass, got %+v", result)
	}
	expectReplica(t, f.replica(t, v1beta1.DecoderComponent), 8, "none")
	status := f.scalingStatus(t)
	expectFollowerStatus(t, status, 16, 16, 8, "FollowerPending")
	condition := meta.FindStatusCondition(status.Conditions, "Ready")
	for _, want := range []string{"decoder", "8", "16"} {
		if !strings.Contains(condition.Message, want) {
			t.Fatalf("message %q does not name %q", condition.Message, want)
		}
	}
	if f.scaleWrites != 0 {
		t.Fatalf("scale subresource writes = %d; want none", f.scaleWrites)
	}
}
