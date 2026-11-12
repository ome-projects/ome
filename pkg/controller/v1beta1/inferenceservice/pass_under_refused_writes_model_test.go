package inferenceservice

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/go-logr/logr"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// This file drives the production service pass (InferenceServiceReconciler
// .Reconcile: the component projections through irprojector, the
// coordination pass with its per-revision Services and traffic weights, the
// status flush and the route render) and the reference replica controller
// (inferencereplica.Reconciler.Reconcile, which runs the workload engine)
// against one fake apiserver whose writes a scenario refuses by kind, the
// way a manager restart leaves them when the manager serves its own
// admission webhooks: pod creates and InferenceReplica writes refused with
// the apiserver's own "failed calling webhook" words, deletes landing.
//
// The order of the service pass (controller.go, servicePass.run): the
// component reconcilers run in order and the first refused write ends the
// loop and becomes the pass's result; the pass skips the steps that build
// on this pass's projections and still runs reconcileCoordination, which
// writes the traffic weights from the pods that serve, and
// flushRequeueingPass, which writes the status and renders the routes. The
// replica controller does not wait for any of that: it drains an old pod
// once its replacement is listed, and it follows the replica's spec, which
// names the revision the operator asked for only once a projection has
// landed.
//
// A kubelet and an endpoints controller of the model's own turn pods Ready
// and list them, on two timings: the default scheduler's, one pod after
// another, and a gang scheduler's, every member of an Instance together
// after a longer wait.

// refusedKind names which writes a refusal window refuses.
type refusedKind uint8

const (
	refuseNothing refusedKind = iota
	// refusePodCreate: the pod mutating webhook cannot be called.
	refusePodCreate
	// refuseReplicaWrite: the InferenceReplica validating webhook cannot be
	// called; creates, updates and patches of the replica's main resource.
	refuseReplicaWrite
	// refuseServiceWrite: the apiserver times out on Service writes.
	refuseServiceWrite
	// refuseStatusWrite: the apiserver times out on every status write.
	refuseStatusWrite
	// refuseWebhookWrites: the manager's own webhooks are down: pod creates
	// and replica writes are refused together, deletes land.
	refuseWebhookWrites
	// refuseRouterWrite: only the router's InferenceReplica writes are
	// refused, the write the refused pass ends on after the engine's
	// and the decoder's had landed.
	refuseRouterWrite
)

func (k refusedKind) String() string {
	switch k {
	case refusePodCreate:
		return "pod-create"
	case refuseReplicaWrite:
		return "replica-write"
	case refuseServiceWrite:
		return "service-write"
	case refuseStatusWrite:
		return "status-write"
	case refuseWebhookWrites:
		return "webhooks-down"
	case refuseRouterWrite:
		return "router-write"
	}
	return "none"
}

// refusedShape is a reference shape: prefill and decode Instances, pods
// per Instance, routers beside them.
type refusedShape struct {
	prefill, decode, pods, routers int32
}

func (s refusedShape) String() string {
	out := fmt.Sprintf("%d:%d", s.prefill, s.decode)
	if s.pods > 1 {
		out += fmt.Sprintf("x%d", s.pods)
	}
	if s.routers > 0 {
		out += fmt.Sprintf("+r%d", s.routers)
	}
	return out
}

// refusedSettings is one setting of the service: the strategy per side at
// ten percent, a rolling-update group with or without a ratio rule, a
// partition on the prefill side, and the scheduler's timing.
type refusedSettings struct {
	prefill, decode v1beta1.UpdateStrategyType
	group           bool
	maintainRatio   bool
	partition       int32
	gangTiming      bool
}

func strategyWord(strategy v1beta1.UpdateStrategyType) string {
	switch strategy {
	case v1beta1.UpdateStrategySurgeThenDrain:
		return "surge"
	case v1beta1.UpdateStrategyRecreatePod:
		return "recreate"
	}
	return "unset"
}

func (s refusedSettings) String() string {
	parts := []string{strategyWord(s.prefill) + "/" + strategyWord(s.decode)}
	if s.group {
		parts = append(parts, "group")
	}
	if s.maintainRatio {
		parts = append(parts, "ratio")
	}
	if s.partition > 0 {
		parts = append(parts, fmt.Sprintf("partition=%d", s.partition))
	}
	if s.gangTiming {
		parts = append(parts, "gang-timing")
	}
	return strings.Join(parts, "+")
}

type refusedConfig struct {
	shape    refusedShape
	settings refusedSettings
	// thorough runs the full probe set on this configuration; the others
	// run the bounded one.
	thorough bool
}

func (c refusedConfig) String() string { return c.shape.String() + "/" + c.settings.String() }

const (
	refusedNamespace = "ns"
	refusedService   = "svc"
	refusedRuntime   = "rt"
	refusedImageV1   = "example.invalid/model:v1"
	refusedImageV2   = "example.invalid/model:v2"
	refusedImageV3   = "example.invalid/model:v3"
	refusedPort      = int32(8080)
	// refusedPodFinalizer stands for the kubelet: a deleted pod stays
	// Terminating for one tick before it is gone.
	refusedPodFinalizer = "model.ome.io/kubelet"
	refusedTickLength   = 5 * time.Second
)

// recordedEvents keeps the events of both controllers without blocking.
type recordedEvents struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordedEvents) Event(_ runtime.Object, eventtype, reason, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, eventtype+" "+reason+": "+message)
}

func (r *recordedEvents) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	r.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *recordedEvents) AnnotatedEventf(obj runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...any) {
	r.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

// refusals is the switch the hooks read: which kind of write is refused
// right now.
type refusals struct {
	mu   sync.Mutex
	kind refusedKind
	// quiet is set while the world's own hand writes (the operator, the
	// kubelet, the endpoints controller); those writes are never refused.
	quiet bool
}

// own runs write with the world's own hand.
func (r *refusals) own(write func()) {
	r.mu.Lock()
	r.quiet = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.quiet = false
		r.mu.Unlock()
	}()
	write()
}

func (r *refusals) set(kind refusedKind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kind = kind
}

func (r *refusals) current() refusedKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.kind
}

// webhookUnavailable is the apiserver failing closed on a write an
// admission webhook must see while the webhook cannot be called, in the
// dispatcher's own words; the engine's classifier reads the phrase.
func webhookUnavailable(webhook string) error {
	return apierrors.NewInternalError(fmt.Errorf(
		`failed calling webhook %q: failed to call webhook: Post "https://ome-webhook-server-service.ome.svc:443/validate?timeout=10s": no endpoints available for service "ome-webhook-server-service"`, webhook))
}

// apiserverTimeout is the apiserver giving up on a write with no answer.
func apiserverTimeout(what string) error {
	return apierrors.NewTimeoutError("the apiserver did not answer the "+what+" in time", 1)
}

// refuse answers whether a write of verb on obj is refused under the
// current switch, and with which error.
func (r *refusals) refuse(verb fakeclient.Verb, obj client.Object, status bool) error {
	r.mu.Lock()
	kind, quiet := r.kind, r.quiet
	r.mu.Unlock()
	if kind == refuseNothing || quiet {
		return nil
	}
	var err error
	switch obj.(type) {
	case *corev1.Pod:
		if !status && verb == fakeclient.Create && (kind == refusePodCreate || kind == refuseWebhookWrites) {
			err = webhookUnavailable("inferenceservice.ome-webhook-server.pod-mutator")
		}
	case *v1beta1.InferenceReplica:
		routerOnly := kind == refuseRouterWrite && strings.HasSuffix(obj.GetName(), "-"+string(v1beta1.RouterComponent))
		if !status && verb != fakeclient.Delete && (kind == refuseReplicaWrite || kind == refuseWebhookWrites || routerOnly) {
			err = webhookUnavailable("inferencereplica.ome-webhook-server.validator")
		}
	case *corev1.Service:
		if !status && kind == refuseServiceWrite {
			err = apiserverTimeout("Service write")
		}
	}
	if status && kind == refuseStatusWrite {
		err = apiserverTimeout("status write")
	}
	return err
}

// refusedWorld is one fake cluster with both controllers on it.
type refusedWorld struct {
	t       *testing.T
	cfg     refusedConfig
	ctx     context.Context
	scheme  *runtime.Scheme
	cli     client.WithWatch
	clock   *clocktesting.FakeClock
	events  *recordedEvents
	refuse  *refusals
	service *InferenceServiceReconciler
	replica *inferencereplica.Reconciler

	// tickNo counts the ticks since the world was made or restored.
	tickNo int
	// scheduledAt is the tick a pod was scheduled on, by pod name; the
	// kubelet turns its containers Ready a timing-dependent number of ticks
	// later.
	scheduledAt map[string]int
	// deletedAt is the tick a pod's deletion began, by pod name.
	deletedAt map[string]int
	// created counts pods created since the world was made, for UIDs.
	created int
	// knownPods is the pod set the informer last delivered, by name.
	knownPods map[string]expectationKey
	// images is the image each Component's spec names now.
	images map[v1beta1.ComponentType]string
	// counts is the Instance count each Component's spec names now.
	counts map[v1beta1.ComponentType]int32
	// partition is the prefill partition the spec names now.
	partition int32
	// trace is the per-tick log of the current path.
	trace []string
	// lastPass is the result of the last service pass.
	lastPassErr error

	// windowCloses is the tick the open refusal window closes on; zero
	// when none is open.
	windowCloses int
	// pushBackAt is the tick of the operator's push back, and rejected the
	// image it rejected; zero and empty when there was none.
	pushBackAt int
	rejected   string
	// resized is set once a probe or a random run changed a count, after
	// which a deleted old pod can be a scale-down and not a completion;
	// strategyEdited once a side's strategy changed mid-roll, after which
	// an attempt opened under the old strategy finishes under its budget.
	resized, strategyEdited bool
	// newPods is what the informer delivered this tick, each with the
	// pod's image.
	newPods map[string]string
	// everUp is, per Component, every Instance index that has served on
	// this path; an Instance that never served is a slot still being built.
	everUp map[v1beta1.ComponentType]map[string]bool
	// podImages remembers every pod's image by name while it exists.
	podImages map[string]string
	// previous is the reading of the previous tick.
	previous *tickReading
	// findings is the first example of every class this path found, and
	// classes counts the ticks of each class.
	findings map[string]refusedFinding
	classes  map[string]int
	// counters over the whole exploration of the configuration.
	ticks, probes int
	seen          map[string]bool
	path          string
}

func (w *refusedWorld) components() []v1beta1.ComponentType {
	out := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	if w.cfg.shape.routers > 0 {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}

func refusedRunner(image string) *v1beta1.RunnerSpec {
	return &v1beta1.RunnerSpec{Container: corev1.Container{
		Name:  constants.MainContainerName,
		Image: image,
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: refusedPort}},
	}}
}

func refusedLifecycle(strategy v1beta1.UpdateStrategyType) *v1beta1.LifecycleSpec {
	tenPercent := intstr.FromString("10%")
	ru := &v1beta1.RollingUpdate{}
	if strategy == v1beta1.UpdateStrategySurgeThenDrain {
		ru.MaxSurge = &tenPercent
	} else {
		ru.MaxUnavailable = &tenPercent
	}
	return &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: strategy, RollingUpdate: ru}}
}

func refusedExt(count int32, strategy v1beta1.UpdateStrategyType, partition int32) v1beta1.ComponentExtensionSpec {
	n := int(count)
	ext := v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: n, Lifecycle: refusedLifecycle(strategy)}
	if partition > 0 {
		ext.Lifecycle.UpdateStrategy.RollingUpdate.Partition = ptr.To(partition)
	}
	return ext
}

// serviceSpec renders the InferenceService spec from the world's current
// images, counts and partition.
func (w *refusedWorld) serviceSpec() v1beta1.InferenceServiceSpec {
	s := w.cfg.shape
	engine := &v1beta1.EngineSpec{
		Runner:                 refusedRunner(w.images[v1beta1.EngineComponent]),
		ComponentExtensionSpec: refusedExt(w.counts[v1beta1.EngineComponent], w.cfg.settings.prefill, w.partition),
	}
	decoder := &v1beta1.DecoderSpec{
		Runner:                 refusedRunner(w.images[v1beta1.DecoderComponent]),
		ComponentExtensionSpec: refusedExt(w.counts[v1beta1.DecoderComponent], w.cfg.settings.decode, 0),
	}
	if s.pods > 1 {
		workers := int(s.pods - 1)
		engine.Leader = &v1beta1.LeaderSpec{Runner: refusedRunner(w.images[v1beta1.EngineComponent])}
		engine.Worker = &v1beta1.WorkerSpec{Size: &workers, PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: w.images[v1beta1.EngineComponent]}}}}
		decoder.Leader = &v1beta1.LeaderSpec{Runner: refusedRunner(w.images[v1beta1.DecoderComponent])}
		decoder.Worker = &v1beta1.WorkerSpec{Size: &workers, PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: w.images[v1beta1.DecoderComponent]}}}}
	}
	spec := v1beta1.InferenceServiceSpec{
		DeploymentMode: ptr.To(constants.OMENative),
		Runtime:        &v1beta1.ServingRuntimeRef{Name: refusedRuntime, Kind: ptr.To("ServingRuntime")},
		Engine:         engine,
		Decoder:        decoder,
	}
	if s.routers > 0 {
		spec.Router = &v1beta1.RouterSpec{
			Runner:                 refusedRunner(w.images[v1beta1.RouterComponent]),
			ComponentExtensionSpec: refusedExt(w.counts[v1beta1.RouterComponent], v1beta1.UpdateStrategySurgeThenDrain, 0),
		}
	}
	if w.cfg.settings.group {
		tenPercent := intstr.FromString("10%")
		g := v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &tenPercent, MaxUnavailable: &tenPercent},
		}
		if w.cfg.settings.maintainRatio {
			g.MaintainRatio = &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(20))}
		}
		spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{g}}
	}
	return spec
}

func refusedServingRuntime() *v1beta1.ServingRuntime {
	return &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: refusedRuntime, Namespace: refusedNamespace},
		Spec: v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: refusedImageV1,
					Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: refusedPort}},
				}},
			},
		},
	}
}

// newRefusedWorld builds the fake cluster with the service at version 1
// and nothing else: the first ticks build the fleet.
var refusedLogOnce sync.Once

func newRefusedWorld(t *testing.T, cfg refusedConfig) *refusedWorld {
	t.Helper()
	refusedLogOnce.Do(func() { ctrl.SetLogger(logr.Discard()) })
	w := &refusedWorld{
		t: t, cfg: cfg, ctx: context.Background(),
		clock:       clocktesting.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		events:      &recordedEvents{},
		refuse:      &refusals{},
		scheduledAt: map[string]int{},
		deletedAt:   map[string]int{},
		knownPods:   map[string]expectationKey{},
		newPods:     map[string]string{},
		podImages:   map[string]string{},
		everUp:      map[v1beta1.ComponentType]map[string]bool{},
		findings:    map[string]refusedFinding{},
		classes:     map[string]int{},
		seen:        map[string]bool{},
		images: map[v1beta1.ComponentType]string{
			v1beta1.EngineComponent: refusedImageV1, v1beta1.DecoderComponent: refusedImageV1, v1beta1.RouterComponent: refusedImageV1,
		},
		counts: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: cfg.shape.prefill, v1beta1.DecoderComponent: cfg.shape.decode, v1beta1.RouterComponent: cfg.shape.routers,
		},
		partition: cfg.settings.partition,
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: refusedService, Namespace: refusedNamespace, UID: types.UID("svc-uid"), Generation: 1},
		Spec:       w.serviceSpec(),
	}
	w.build([]client.Object{isvc, refusedServingRuntime(), refusedNode()})
	return w
}

// refusedNode is the one Ready node every pod is placed on.
func refusedNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: types.UID("node-1-uid")},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

// build makes the client and both controllers over objs.
func (w *refusedWorld) build(objs []client.Object) {
	w.scheme = fakeclient.Scheme(w.t, discoveryv1.AddToScheme, gatewayapiv1.Install)
	w.cli = fakeclient.New(w.t,
		fakeclient.WithScheme(w.scheme),
		fakeclient.WithObjects(objs...),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}, &corev1.Pod{}),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor),
		fakeclient.WithIndex(&corev1.Pod{}, query.PodNodeNameField, query.PodNodeNameIndexExtractor),
		fakeclient.WithIndex(&corev1.Node{}, query.NodeUnschedulableField, query.NodeUnschedulableIndexExtractor),
		fakeclient.WithInterceptors(w.hooks()),
	)
	clientset := fake.NewClientset()
	if _, err := clientset.CoreV1().ConfigMaps("ome").Create(w.ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data:       map[string]string{},
	}, metav1.CreateOptions{}); err != nil {
		w.t.Fatalf("config map: %v", err)
	}
	w.service = &InferenceServiceReconciler{
		Client:                   w.cli,
		APIReader:                w.cli,
		ClientConfig:             &rest.Config{},
		Clientset:                clientset,
		Log:                      ctrl.Log.WithName("model"),
		Scheme:                   w.scheme,
		Recorder:                 w.events,
		RuntimeSelector:          runtimeselector.New(w.cli),
		AcceleratorClassSelector: acceleratorclassselector.New(w.cli),
		ConfigCacheTTL:           time.Hour,
	}
	w.replica = &inferencereplica.Reconciler{
		Client:                   w.cli,
		APIReader:                w.cli,
		Log:                      ctrl.Log.WithName("model-replica"),
		Recorder:                 w.events,
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workloadtypes.NewExpectationsWithClock(w.clock),
		ScaleDownRequeueInterval: 30 * time.Second,
		Clock:                    w.clock,
	}
}

// hooks are the apiserver's part of the model: UIDs and timestamps on
// create, the generation that moves on a spec change, the kubelet finalizer on
// pods, and the refusals.
func (w *refusedWorld) hooks() interceptor.Funcs {
	return interceptor.Funcs{
		Create:            w.onCreate,
		Update:            w.onUpdate,
		Patch:             w.onPatch,
		Delete:            w.onDelete,
		SubResourceUpdate: w.onStatusUpdate,
		SubResourcePatch:  w.onStatusPatch,
	}
}

// onCreate admits a create: the refusal switch first, then the UID, the
// creation time and the first generation the apiserver sets, and the
// kubelet finalizer on a pod.
func (w *refusedWorld) onCreate(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if err := w.refuse.refuse(fakeclient.Create, obj, false); err != nil {
		return err
	}
	if obj.GetUID() == "" {
		w.created++
		obj.SetUID(types.UID(fmt.Sprintf("%s-%d", obj.GetName(), w.created)))
	}
	if created := obj.GetCreationTimestamp(); created.IsZero() {
		obj.SetCreationTimestamp(metav1.NewTime(w.clock.Now()))
	}
	if obj.GetGeneration() == 0 {
		obj.SetGeneration(1)
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		pod.Finalizers = append(pod.Finalizers, refusedPodFinalizer)
		pod.Status.Phase = corev1.PodPending
	}
	return cl.Create(ctx, obj, opts...)
}

// onUpdate admits an update and moves the generation on a spec change.
func (w *refusedWorld) onUpdate(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
	if err := w.refuse.refuse(fakeclient.Update, obj, false); err != nil {
		return err
	}
	before := w.stored(ctx, cl, obj)
	if before != nil && refusedSpecChanged(before, obj) {
		obj.SetGeneration(before.GetGeneration() + 1)
	}
	return cl.Update(ctx, obj, opts...)
}

// onPatch admits a patch and moves the generation on a spec change.
func (w *refusedWorld) onPatch(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := w.refuse.refuse(fakeclient.Patch, obj, false); err != nil {
		return err
	}
	before := w.stored(ctx, cl, obj)
	if err := cl.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	if before != nil && refusedSpecChanged(before, obj) {
		obj.SetGeneration(before.GetGeneration() + 1)
		return cl.Update(ctx, obj)
	}
	return nil
}

// onDelete admits a delete and remembers the tick a pod's deletion began.
func (w *refusedWorld) onDelete(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	if err := w.refuse.refuse(fakeclient.Delete, obj, false); err != nil {
		return err
	}
	if _, ok := obj.(*corev1.Pod); ok {
		if _, seen := w.deletedAt[obj.GetName()]; !seen {
			w.deletedAt[obj.GetName()] = w.tickNo
		}
	}
	return cl.Delete(ctx, obj, opts...)
}

func (w *refusedWorld) onStatusUpdate(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if sub == "status" {
		if err := w.refuse.refuse(fakeclient.StatusUpdate, obj, true); err != nil {
			return err
		}
	}
	return cl.SubResource(sub).Update(ctx, obj, opts...)
}

func (w *refusedWorld) onStatusPatch(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if sub == "status" {
		if err := w.refuse.refuse(fakeclient.StatusPatch, obj, true); err != nil {
			return err
		}
	}
	return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
}

// stored reads the copy of obj the store holds, nil when there is none.
func (w *refusedWorld) stored(ctx context.Context, cl client.Reader, obj client.Object) client.Object {
	before, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), before); err != nil {
		return nil
	}
	return before
}

// refusedSpecChanged reports whether a write changes the spec of an
// InferenceService or an InferenceReplica, the change the apiserver counts
// as a new generation.
func refusedSpecChanged(before, after client.Object) bool {
	switch b := before.(type) {
	case *v1beta1.InferenceService:
		a, ok := after.(*v1beta1.InferenceService)
		return ok && !equalJSON(b.Spec, a.Spec)
	case *v1beta1.InferenceReplica:
		a, ok := after.(*v1beta1.InferenceReplica)
		return ok && !equalJSON(b.Spec, a.Spec)
	}
	return false
}

func equalJSON(a, b any) bool {
	return fmt.Sprintf("%+v", a) == fmt.Sprintf("%+v", b)
}

// writeSpec writes the service spec the world's images, counts and
// partition render; the operator's hand is never refused.
func (w *refusedWorld) writeSpec() {
	w.t.Helper()
	w.refuse.own(func() {
		isvc := &v1beta1.InferenceService{}
		if err := w.cli.Get(w.ctx, w.serviceKey(), isvc); err != nil {
			w.t.Fatalf("read service: %v", err)
		}
		isvc.Spec = w.serviceSpec()
		if err := w.cli.Update(w.ctx, isvc); err != nil {
			w.t.Fatalf("write service spec: %v", err)
		}
	})
}

func (w *refusedWorld) serviceKey() types.NamespacedName {
	return types.NamespacedName{Namespace: refusedNamespace, Name: refusedService}
}

func (w *refusedWorld) replicaKey(c v1beta1.ComponentType) types.NamespacedName {
	return types.NamespacedName{Namespace: refusedNamespace, Name: irprojector.InferenceReplicaName(refusedService, c)}
}

// push moves every rolling Component's image; the routers stay.
func (w *refusedWorld) push(image string) {
	w.images[v1beta1.EngineComponent] = image
	w.images[v1beta1.DecoderComponent] = image
	w.writeSpec()
	w.note("operator: push %s", image)
}

func (w *refusedWorld) note(format string, args ...any) {
	w.trace = append(w.trace, fmt.Sprintf("t=%d ", w.tickNo)+fmt.Sprintf(format, args...))
}

// servicePass runs one production pass of the service controller.
func (w *refusedWorld) servicePass() {
	_, err := w.service.Reconcile(w.ctx, ctrl.Request{NamespacedName: w.serviceKey()})
	w.lastPassErr = err
	if err != nil {
		w.note("service pass: %s", shortErr(err))
	}
}

func shortErr(err error) string {
	s := err.Error()
	if i := strings.Index(s, ": failed calling webhook"); i > 0 {
		s = s[:i] + ": failed calling webhook"
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

// replicaPasses runs the reference replica controller once per Component,
// and again while a pass asks to come back at once, up to a cap, the way
// the work queue runs an immediate requeue.
func (w *refusedWorld) replicaPasses() {
	for _, c := range w.components() {
		key := w.replicaKey(c)
		for i := 0; i < 4; i++ {
			res, err := w.replica.Reconcile(w.ctx, ctrl.Request{NamespacedName: key})
			w.informer()
			if err != nil {
				w.note("%s replica pass: %s", c, shortErr(err))
				break
			}
			if !res.Requeue || res.RequeueAfter > 0 { //nolint:staticcheck // the bare requeue is the work queue's immediate retry
				break
			}
		}
	}
}

// kubelet schedules Pending pods, turns their containers Ready on the
// configured timing, folds the serving gate into PodReady, and lets a
// deleted pod go after one tick.
func (w *refusedWorld) kubelet() {
	pods := &corev1.PodList{}
	if err := w.cli.List(w.ctx, pods, client.InNamespace(refusedNamespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	byInstance := map[string][]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		key := instanceKeyOf(pod)
		byInstance[key] = append(byInstance[key], pod)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		switch {
		case pod.DeletionTimestamp != nil:
			w.letPodGo(pod)
		case pod.Spec.NodeName == "" && !w.gangComplete(pod, byInstance):
		default:
			w.advancePod(pod)
		}
	}
}

func instanceKeyOf(pod *corev1.Pod) string {
	return pod.Labels[constants.OMEComponentLabel] + "/" + pod.Labels[query.LabelInstanceIdx]
}

// letPodGo removes the kubelet finalizer one tick after the deletion began.
func (w *refusedWorld) letPodGo(pod *corev1.Pod) {
	if w.tickNo <= w.deletedAt[pod.Name] {
		return
	}
	pod.Finalizers = nil
	if err := w.cli.Update(w.ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		w.t.Fatalf("let pod %s go: %v", pod.Name, err)
	}
	delete(w.deletedAt, pod.Name)
	delete(w.scheduledAt, pod.Name)
}

// advancePod places an unplaced pod, turns its containers Ready on the
// configured timing, and folds the readiness gates into PodReady; one
// status write carries what changed.
func (w *refusedWorld) advancePod(pod *corev1.Pod) {
	changed := false
	if pod.Spec.NodeName == "" {
		pod.Spec.NodeName = "node-1"
		if err := w.cli.Update(w.ctx, pod); err != nil {
			w.t.Fatalf("schedule pod %s: %v", pod.Name, err)
		}
		w.scheduledAt[pod.Name] = w.tickNo
		setPodCondition(pod, corev1.PodScheduled, corev1.ConditionTrue, w.clock.Now())
		pod.Status.Phase = corev1.PodPending
		changed = true
	}
	readyAfter := 1
	if w.cfg.settings.gangTiming {
		readyAfter = 2
	}
	if pod.Status.Phase == corev1.PodPending && w.tickNo-w.scheduledAt[pod.Name] >= readyAfter {
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = "10.0.0.1"
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, w.clock.Now())
		changed = true
	}
	ready := corev1.ConditionFalse
	if podCondition(pod, corev1.ContainersReady) == corev1.ConditionTrue && gatesSatisfied(pod) {
		ready = corev1.ConditionTrue
	}
	if podCondition(pod, corev1.PodReady) != ready {
		setPodCondition(pod, corev1.PodReady, ready, w.clock.Now())
		changed = true
	}
	if changed {
		if err := w.cli.Status().Update(w.ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			w.t.Fatalf("kubelet status of %s: %v", pod.Name, err)
		}
	}
}

// gangComplete reports whether every pod of the Instance exists, the
// condition under which a gang scheduler places any of them; under the
// default timing a pod is placed as it appears.
func (w *refusedWorld) gangComplete(pod *corev1.Pod, byInstance map[string][]*corev1.Pod) bool {
	if !w.cfg.settings.gangTiming || w.cfg.shape.pods <= 1 || pod.Labels[constants.OMEComponentLabel] == string(v1beta1.RouterComponent) {
		return true
	}
	members := byInstance[instanceKeyOf(pod)]
	live := 0
	for _, m := range members {
		if m.DeletionTimestamp == nil && m.Labels[query.LabelInstanceIncarnation] == pod.Labels[query.LabelInstanceIncarnation] {
			live++
		}
	}
	return int32(live) >= w.cfg.shape.pods
}

func gatesSatisfied(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.ReadinessGates {
		if podCondition(pod, gate.ConditionType) != corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func podCondition(pod *corev1.Pod, t corev1.PodConditionType) corev1.ConditionStatus {
	for _, c := range pod.Status.Conditions {
		if c.Type == t {
			return c.Status
		}
	}
	return corev1.ConditionUnknown
}

func setPodCondition(pod *corev1.Pod, t corev1.PodConditionType, status corev1.ConditionStatus, now time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			if pod.Status.Conditions[i].Status != status {
				pod.Status.Conditions[i].Status = status
				pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(now)
			}
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: t, Status: status, LastTransitionTime: metav1.NewTime(now)})
}

// endpoints lists, for every Service with a selector, the Running and
// Ready pods it selects, in one EndpointSlice per Service, the way the
// endpoints controller does; a Terminating pod leaves the slice.
func (w *refusedWorld) endpoints() {
	services := &corev1.ServiceList{}
	if err := w.cli.List(w.ctx, services, client.InNamespace(refusedNamespace)); err != nil {
		w.t.Fatalf("list services: %v", err)
	}
	pods := &corev1.PodList{}
	if err := w.cli.List(w.ctx, pods, client.InNamespace(refusedNamespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	for i := range services.Items {
		svc := &services.Items[i]
		if len(svc.Spec.Selector) == 0 {
			continue
		}
		selector := labels.SelectorFromSet(svc.Spec.Selector)
		var endpoints []discoveryv1.Endpoint
		for j := range pods.Items {
			pod := &pods.Items[j]
			if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || podCondition(pod, corev1.PodReady) != corev1.ConditionTrue || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			endpoints = append(endpoints, discoveryv1.Endpoint{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			})
		}
		sort.Slice(endpoints, func(a, b int) bool { return endpoints[a].TargetRef.Name < endpoints[b].TargetRef.Name })
		slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: svc.Name + "-slice", Namespace: refusedNamespace}}
		err := w.cli.Get(w.ctx, client.ObjectKeyFromObject(slice), slice)
		switch {
		case apierrors.IsNotFound(err):
			slice.Labels = map[string]string{discoveryv1.LabelServiceName: svc.Name}
			slice.AddressType = discoveryv1.AddressTypeIPv4
			slice.Endpoints = endpoints
			if err := w.cli.Create(w.ctx, slice); err != nil {
				w.t.Fatalf("create slice of %s: %v", svc.Name, err)
			}
		case err != nil:
			w.t.Fatalf("read slice of %s: %v", svc.Name, err)
		default:
			if endpointNames(slice.Endpoints) == endpointNames(endpoints) {
				continue
			}
			slice.Endpoints = endpoints
			if err := w.cli.Update(w.ctx, slice); err != nil {
				w.t.Fatalf("update slice of %s: %v", svc.Name, err)
			}
		}
	}
}

func endpointNames(eps []discoveryv1.Endpoint) string {
	names := make([]string, 0, len(eps))
	for _, ep := range eps {
		names = append(names, ep.TargetRef.Name)
	}
	return strings.Join(names, ",")
}

// tick is one step of the world: the service pass, the replica passes,
// the kubelet, the endpoints controller, and the clock moving on.
func (w *refusedWorld) tick() {
	w.tickNo++
	w.ticks++
	if w.windowCloses > 0 && w.tickNo > w.windowCloses {
		w.refuse.set(refuseNothing)
		w.windowCloses = 0
		w.note("apiserver: the refusal window closed")
	}
	w.servicePass()
	w.replicaPasses()
	w.refuse.own(func() {
		w.kubelet()
		w.informer()
		w.endpoints()
	})
	w.clock.Step(refusedTickLength)
	w.observe()
	w.trace = append(w.trace, fmt.Sprintf("t=%d %s", w.tickNo, w.previous.picture))
}

// expectationKey is the key the pod watch handler observes creates and
// deletes under: the name prefix, the Component and the Instance index.
type expectationKey struct {
	owner     string
	component workloadtypes.ComponentType
	instance  int32
}

// informer plays the pod watch handler: every pod that appeared since the
// last delivery is an observed create, every pod gone an observed delete.
func (w *refusedWorld) informer() {
	list := &corev1.PodList{}
	if err := w.cli.List(w.ctx, list, client.InNamespace(refusedNamespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	seen := map[string]bool{}
	for i := range list.Items {
		pod := &list.Items[i]
		seen[pod.Name] = true
		if _, known := w.knownPods[pod.Name]; known {
			continue
		}
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || pod.Labels[query.LabelManagedBy] != query.ManagedByOMENative {
			continue
		}
		key := expectationKey{owner: pod.Labels[constants.InferenceServicePodLabelKey], component: workloadtypes.ComponentType(pod.Labels[constants.OMEComponentLabel]), instance: idx}
		w.knownPods[pod.Name] = key
		w.replica.Expectations.ObservedCreate(refusedNamespace, key.owner, key.component, key.instance)
		image := ""
		if len(pod.Spec.Containers) > 0 {
			image = pod.Spec.Containers[0].Image
		}
		w.podImages[pod.Name] = image
		w.newPods[pod.Name] = image
	}
	for name, key := range w.knownPods {
		if seen[name] {
			continue
		}
		delete(w.knownPods, name)
		delete(w.podImages, name)
		w.replica.Expectations.ObservedDelete(refusedNamespace, key.owner, key.component, key.instance)
	}
}

// podView is one pod as the readings see it.
type podView struct {
	component v1beta1.ComponentType
	instance  string
	revision  string
	routed    bool
	serving   bool
	leaving   bool
	image     string
}

// pods reads every pod of the service.
func (w *refusedWorld) pods() []podView {
	list := &corev1.PodList{}
	if err := w.cli.List(w.ctx, list, client.InNamespace(refusedNamespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	out := make([]podView, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		image := ""
		if len(pod.Spec.Containers) > 0 {
			image = pod.Spec.Containers[0].Image
		}
		out = append(out, podView{
			component: v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel]),
			instance:  pod.Labels[query.LabelInstanceIdx],
			revision:  pod.Labels[query.LabelRevisionHash],
			routed:    query.RoutedRunner(pod),
			serving:   pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning && podCondition(pod, corev1.PodReady) == corev1.ConditionTrue,
			leaving:   pod.DeletionTimestamp != nil,
			image:     image,
		})
	}
	return out
}

// picture is one line per Component: pods per revision, serving and
// total, and the traffic the persisted status names.
func (w *refusedWorld) picture() string {
	isvc := &v1beta1.InferenceService{}
	if err := w.cli.Get(w.ctx, w.serviceKey(), isvc); err != nil {
		w.t.Fatalf("read service: %v", err)
	}
	pods := w.pods()
	var parts []string
	for _, c := range w.components() {
		serving := map[string]int{}
		total := map[string]int{}
		for _, p := range pods {
			if p.component != c || !p.routed {
				continue
			}
			total[p.revision]++
			if p.serving {
				serving[p.revision]++
			}
		}
		var revs []string
		for rev := range total {
			revs = append(revs, rev)
		}
		sort.Strings(revs)
		var cells []string
		for _, rev := range revs {
			cells = append(cells, fmt.Sprintf("%s=%d/%d", shortRev(rev), serving[rev], total[rev]))
		}
		var traffic []string
		for _, tt := range isvc.Status.Components[c].Traffic {
			traffic = append(traffic, fmt.Sprintf("%s:%d", shortRev(revisionHashOf(tt.RevisionName)), tt.Percent))
		}
		parts = append(parts, fmt.Sprintf("%s[%s traffic %s]", c, strings.Join(cells, " "), strings.Join(traffic, ",")))
	}
	return strings.Join(parts, " ")
}

func shortRev(rev string) string {
	if i := strings.LastIndex(rev, "-"); i >= 0 && i+1 < len(rev) {
		rev = rev[i+1:]
	}
	if len(rev) > 6 {
		rev = rev[:6]
	}
	return rev
}

// refusedSnapshot is the world at one instant: every object of the fake
// cluster and the world's own bookkeeping, enough to rebuild the client
// and both controllers and go on from there.
type refusedSnapshot struct {
	objects     []client.Object
	clockAt     time.Time
	tickNo      int
	created     int
	scheduledAt map[string]int
	deletedAt   map[string]int
	knownPods   map[string]expectationKey
	podImages   map[string]string
	images      map[v1beta1.ComponentType]string
	counts      map[v1beta1.ComponentType]int32
	partition   int32
	strategies  [2]v1beta1.UpdateStrategyType
}

// refusedListKinds are the kinds the two controllers and the model write.
func refusedListKinds() []client.ObjectList {
	return []client.ObjectList{
		&v1beta1.InferenceServiceList{}, &v1beta1.InferenceReplicaList{}, &v1beta1.ServingRuntimeList{},
		&corev1.PodList{}, &corev1.ServiceList{}, &corev1.ServiceAccountList{}, &corev1.NodeList{}, &corev1.ConfigMapList{},
		&discoveryv1.EndpointSliceList{}, &appsv1.ControllerRevisionList{}, &policyv1.PodDisruptionBudgetList{},
		&autoscalingv2.HorizontalPodAutoscalerList{}, &gatewayapiv1.HTTPRouteList{},
	}
}

func (w *refusedWorld) snapshot() *refusedSnapshot {
	w.t.Helper()
	snap := &refusedSnapshot{
		clockAt: w.clock.Now(), tickNo: w.tickNo, created: w.created,
		scheduledAt: copyIntMap(w.scheduledAt), deletedAt: copyIntMap(w.deletedAt),
		knownPods: map[string]expectationKey{}, images: map[v1beta1.ComponentType]string{}, counts: map[v1beta1.ComponentType]int32{},
		partition: w.partition, strategies: [2]v1beta1.UpdateStrategyType{w.cfg.settings.prefill, w.cfg.settings.decode},
	}
	for k, v := range w.knownPods {
		snap.knownPods[k] = v
	}
	snap.podImages = map[string]string{}
	for k, v := range w.podImages {
		snap.podImages[k] = v
	}
	for k, v := range w.images {
		snap.images[k] = v
	}
	for k, v := range w.counts {
		snap.counts[k] = v
	}
	for _, list := range refusedListKinds() {
		if err := w.cli.List(w.ctx, list); err != nil {
			w.t.Fatalf("snapshot list %T: %v", list, err)
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			w.t.Fatalf("snapshot extract %T: %v", list, err)
		}
		for _, item := range items {
			obj, ok := item.DeepCopyObject().(client.Object)
			if !ok {
				continue
			}
			obj.SetResourceVersion("")
			snap.objects = append(snap.objects, obj)
		}
	}
	return snap
}

func copyIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// restore rebuilds the world from snap: a fresh client over the objects,
// fresh controllers, the clock and the bookkeeping as they were.
func (w *refusedWorld) restore(snap *refusedSnapshot) {
	w.t.Helper()
	w.clock = clocktesting.NewFakeClock(snap.clockAt)
	w.tickNo = snap.tickNo
	w.created = snap.created
	w.scheduledAt = copyIntMap(snap.scheduledAt)
	w.deletedAt = copyIntMap(snap.deletedAt)
	w.knownPods = map[string]expectationKey{}
	for k, v := range snap.knownPods {
		w.knownPods[k] = v
	}
	w.podImages = map[string]string{}
	for k, v := range snap.podImages {
		w.podImages[k] = v
	}
	w.newPods = map[string]string{}
	w.everUp = map[v1beta1.ComponentType]map[string]bool{}
	w.windowCloses, w.pushBackAt, w.rejected, w.resized, w.strategyEdited = 0, 0, "", false, false
	w.previous = nil
	for k, v := range snap.images {
		w.images[k] = v
	}
	for k, v := range snap.counts {
		w.counts[k] = v
	}
	w.partition = snap.partition
	w.cfg.settings.prefill, w.cfg.settings.decode = snap.strategies[0], snap.strategies[1]
	w.refuse = &refusals{}
	w.events = &recordedEvents{}
	w.trace = nil
	w.lastPassErr = nil
	objs := make([]client.Object, 0, len(snap.objects))
	for _, obj := range snap.objects {
		objs = append(objs, obj.DeepCopyObject().(client.Object))
	}
	w.build(objs)
}

// Operator actions. Each writes the spec; the apiserver never refuses the
// operator's own hand.

// pushBack re-applies the image the roll started from on both sides.
func (w *refusedWorld) pushBack(image string) {
	if w.images[v1beta1.EngineComponent] == image {
		w.note("operator: push back to %s, which the spec already names", image)
		return
	}
	w.rejected = w.images[v1beta1.EngineComponent]
	w.pushBackAt = w.tickNo
	w.images[v1beta1.EngineComponent] = image
	w.images[v1beta1.DecoderComponent] = image
	w.writeSpec()
	w.note("operator: push back to %s", image)
}

// editPrefillStrategy flips the prefill side's strategy between surge and
// recreate.
func (w *refusedWorld) editPrefillStrategy() {
	if w.cfg.settings.prefill == v1beta1.UpdateStrategySurgeThenDrain {
		w.cfg.settings.prefill = v1beta1.UpdateStrategyRecreatePod
	} else {
		w.cfg.settings.prefill = v1beta1.UpdateStrategySurgeThenDrain
	}
	w.strategyEdited = true
	w.writeSpec()
	w.note("operator: prefill strategy now %s", w.cfg.settings.prefill)
}

// resizePrefill changes the prefill count by delta, never below one.
func (w *refusedWorld) resizePrefill(delta int32) {
	n := w.counts[v1beta1.EngineComponent] + delta
	if n < 1 {
		n = 1
	}
	w.counts[v1beta1.EngineComponent] = n
	w.resized = true
	w.writeSpec()
	w.note("operator: prefill resized to %d", n)
}

// setPartition sets the prefill partition, 0 clearing it.
func (w *refusedWorld) setPartition(n int32) {
	w.partition = n
	w.writeSpec()
	w.note("operator: prefill partition %d", n)
}

// refusedMoment names where in a roll a probe starts.
type refusedMoment uint8

const (
	atIdle refusedMoment = iota
	atSettling
	atRotating
	atDraining
	atLast
	momentCount
)

func (m refusedMoment) String() string {
	return [...]string{"idle", "settling", "rotating", "draining", "last"}[m]
}

// refusedAction is one operator action a probe makes as its window opens.
type refusedAction uint8

const (
	actNone refusedAction = iota
	actPushBack
	actPushNext
	actStrategyEdit
	actResizeUp
	actResizeDown
	actPartition
	actionCount
)

func (a refusedAction) String() string {
	return [...]string{"no action", "push back", "push a third revision", "edit the prefill strategy", "prefill +1", "prefill -1", "prefill partition 2"}[a]
}

// refusedProbe is one probe: at a moment, a refusal window of one kind and
// length, and an operator action as it opens.
type refusedProbe struct {
	moment refusedMoment
	kind   refusedKind
	window int
	action refusedAction
}

func (p refusedProbe) String() string {
	parts := []string{"at " + p.moment.String()}
	if p.kind != refuseNothing {
		parts = append(parts, fmt.Sprintf("refuse %s for %d ticks", p.kind, p.window))
	}
	if p.action != actNone {
		parts = append(parts, p.action.String())
	}
	return strings.Join(parts, " + ")
}

// apply makes the probe's action and opens its window.
func (w *refusedWorld) apply(p refusedProbe) {
	switch p.action {
	case actPushBack:
		w.pushBack(refusedImageV1)
	case actPushNext:
		w.push(refusedImageV3)
	case actStrategyEdit:
		w.editPrefillStrategy()
	case actResizeUp:
		w.resizePrefill(1)
	case actResizeDown:
		w.resizePrefill(-1)
	case actPartition:
		if w.partition > 0 {
			w.setPartition(0)
		} else {
			w.setPartition(2)
		}
	}
	if p.kind != refuseNothing {
		w.refuse.set(p.kind)
		w.windowCloses = w.tickNo + p.window
		w.note("apiserver: refusing %s writes for %d ticks", p.kind, p.window)
	}
}

// tickReading is what the readings see at the end of a tick.
type tickReading struct {
	tick int
	kind refusedKind
	// traffic is the weight per revision the persisted status names, and
	// serving the routed pods that serve per revision, per Component.
	traffic map[v1beta1.ComponentType]map[string]int32
	serving map[v1beta1.ComponentType]map[string]int32
	// up is the Instances with a serving routed pod, down the Instances
	// that served on this path and serve nothing now, and above the routed
	// pods beyond the Instance count, per Component.
	up, down, above map[v1beta1.ComponentType]int32
	// servingByImage is the routed pods that serve per image, over the
	// rolling Components.
	servingByImage map[string]int32
	settled        bool
	picture        string
}

// refusedFinding is the first example of a class on a configuration.
type refusedFinding struct {
	message, path, state string
}

func (w *refusedWorld) record(class, format string, args ...any) {
	w.classes[class]++
	if _, seen := w.findings[class]; !seen {
		w.findings[class] = refusedFinding{message: fmt.Sprintf(format, args...), path: w.path + " " + strings.Join(w.trace, "; "), state: w.picture()}
	}
}

func (w *refusedWorld) budgetOf(count int32) int32 {
	tenth := (count + 9) / 10
	if tenth < 1 {
		tenth = 1
	}
	return tenth
}

func (w *refusedWorld) strategyOf(c v1beta1.ComponentType) v1beta1.UpdateStrategyType {
	switch c {
	case v1beta1.EngineComponent:
		return w.cfg.settings.prefill
	case v1beta1.DecoderComponent:
		return w.cfg.settings.decode
	}
	return v1beta1.UpdateStrategySurgeThenDrain
}

// observe reads the world at the end of a tick and records every class the
// readings name.
func (w *refusedWorld) observe() {
	isvc := &v1beta1.InferenceService{}
	if err := w.cli.Get(w.ctx, w.serviceKey(), isvc); err != nil {
		w.t.Fatalf("read service: %v", err)
	}
	pods := w.pods()
	r := &tickReading{tick: w.tickNo, kind: w.refuse.current(),
		traffic: map[v1beta1.ComponentType]map[string]int32{}, serving: map[v1beta1.ComponentType]map[string]int32{},
		up: map[v1beta1.ComponentType]int32{}, down: map[v1beta1.ComponentType]int32{}, above: map[v1beta1.ComponentType]int32{}, servingByImage: map[string]int32{}}
	settled := true
	for _, c := range w.components() {
		r.traffic[c] = map[string]int32{}
		for _, tt := range isvc.Status.Components[c].Traffic {
			r.traffic[c][revisionHashOf(tt.RevisionName)] = tt.Percent
		}
		counts := w.countPods(c, pods, r)
		w.readInstances(c, counts, r)
		if !w.componentSettled(c, counts, r) {
			settled = false
		}
		w.readBudgets(c, r)
	}
	r.settled = settled
	r.picture = w.picture()
	w.seen[r.picture] = true
	w.readTraffic(r)
	w.readRejected(r)
	w.newPods = map[string]string{}
	w.previous = r
}

// podCounts is one Component's pods at the end of a tick.
type podCounts struct {
	routed, leaving, offSpecImage int32
	upByIndex                     map[string]bool
}

// countPods reads the Component's pods into r.serving and r.servingByImage
// and returns the counts the other readings use.
func (w *refusedWorld) countPods(c v1beta1.ComponentType, pods []podView, r *tickReading) podCounts {
	r.serving[c] = map[string]int32{}
	counts := podCounts{upByIndex: map[string]bool{}}
	for _, p := range pods {
		if p.component != c {
			continue
		}
		if p.leaving {
			counts.leaving++
			continue
		}
		if p.image != w.images[c] {
			counts.offSpecImage++
		}
		if !p.routed {
			continue
		}
		counts.routed++
		if !p.serving {
			continue
		}
		r.serving[c][p.revision]++
		counts.upByIndex[p.instance] = true
		if c != v1beta1.RouterComponent {
			r.servingByImage[p.image]++
		}
	}
	return counts
}

// readInstances records the Instances that serve, the Instances that
// served on this path and serve nothing now, and the routed pods beyond
// the count.
func (w *refusedWorld) readInstances(c v1beta1.ComponentType, counts podCounts, r *tickReading) {
	if w.everUp[c] == nil {
		w.everUp[c] = map[string]bool{}
	}
	for idx := range counts.upByIndex {
		w.everUp[c][idx] = true
	}
	count := w.counts[c]
	var known, served int32
	for idx := range w.everUp[c] {
		if instanceIndexBelow(idx, count) {
			known++
		}
	}
	for _, n := range r.serving[c] {
		served += n
	}
	if known > served {
		r.down[c] = known - served
	}
	r.up[c] = int32(len(counts.upByIndex))
	if counts.routed > count {
		r.above[c] = counts.routed - count
	}
}

// componentSettled reads a Component at rest: the replica promoted, every
// Instance serving, nothing leaving, and the pods and the weights on one
// revision, or on two with a partition keeping prefill Instances where
// they were. The engine holds the lowest-indexed Instances still off the
// target, up to the partition, so after a rollback the held ones can be
// fewer than the partition: the Instances that moved during the window.
func (w *refusedWorld) componentSettled(c v1beta1.ComponentType, counts podCounts, r *tickReading) bool {
	count := w.counts[c]
	held := int32(0)
	if c == v1beta1.EngineComponent && w.partition > 0 {
		held = min(w.partition, count)
	}
	offSpec := counts.offSpecImage / w.podsPerInstance(c)
	if offSpec > held || offSpec >= count {
		return false
	}
	ir := &v1beta1.InferenceReplica{}
	if err := w.cli.Get(w.ctx, w.replicaKey(c), ir); err != nil || !w.replicaSettled(ir, count, offSpec) {
		return false
	}
	oneRevision := len(r.serving[c]) == 1 && len(r.traffic[c]) == 1 && offSpec == 0
	twoRevisions := offSpec > 0 && len(r.serving[c]) == 2 && len(r.traffic[c]) == 2
	return counts.leaving == 0 && r.up[c] == count && (oneRevision || twoRevisions)
}

// revisionHashOf is the revision hash a per-revision Service name or a
// ControllerRevision name ends with; a bare hash is returned as it is.
func revisionHashOf(name string) string {
	if i := strings.LastIndex(name, "-rev-"); i >= 0 {
		return name[i+len("-rev-"):]
	}
	return name
}

// replicaSettled reads a replica at rest: every Instance Ready, and either
// all of them on the spec's revision or the held ones kept where they were.
func (w *refusedWorld) replicaSettled(ir *v1beta1.InferenceReplica, count, held int32) bool {
	st := ir.Status
	if st.UpdateRevision == "" || st.Replicas != count || st.ReadyReplicas != count {
		return false
	}
	if held == 0 {
		return st.UpdatedReadyReplicas == count && st.UpdateRevision == st.CurrentRevision
	}
	return st.UpdatedReadyReplicas == count-held
}

// podsPerInstance is how many pods an Instance of c runs.
func (w *refusedWorld) podsPerInstance(c v1beta1.ComponentType) int32 {
	if c == v1beta1.RouterComponent || w.cfg.shape.pods < 1 {
		return 1
	}
	return w.cfg.shape.pods
}

func instanceIndexBelow(idx string, count int32) bool {
	var n int32
	if _, err := fmt.Sscanf(idx, "%d", &n); err != nil {
		return true
	}
	return n < count
}

// readBudgets names a Component with more Instances down than its
// unavailability budget, or more routed pods above its count than its
// surge budget.
func (w *refusedWorld) readBudgets(c v1beta1.ComponentType, r *tickReading) {
	count := w.counts[c]
	strategy := w.strategyOf(c)
	var maxUnavailable, maxSurge int32
	if strategy == v1beta1.UpdateStrategySurgeThenDrain || (w.strategyEdited && c == v1beta1.EngineComponent) {
		maxSurge = w.budgetOf(count)
	}
	if strategy == v1beta1.UpdateStrategyRecreatePod || (w.strategyEdited && c == v1beta1.EngineComponent) {
		maxUnavailable = w.budgetOf(count)
	}
	if r.down[c] > maxUnavailable {
		w.record("unavailable-budget-exceeded/"+r.kind.String(), "%s has %d Instances down against a budget of %d", c, r.down[c], maxUnavailable)
	}
	if r.above[c] > maxSurge {
		w.record("surge-budget-exceeded/"+r.kind.String(), "%s has %d routed pods above its count of %d against a surge budget of %d", c, r.above[c], count, maxSurge)
	}
}

// readTraffic names a revision that carries weight with no serving pod
// while another revision serves, and a revision that serves with no
// weight, when the picture stands at the end of two ticks in a row: the
// pass of the second tick read the pods of the first and did not follow
// them.
func (w *refusedWorld) readTraffic(r *tickReading) {
	if w.previous == nil {
		return
	}
	// The class carries the refusal the picture began under: the pass of
	// the second tick may run after the window closed and still publish
	// what the first tick's failed pass left.
	kind := w.previous.kind
	if kind == refuseNothing {
		kind = r.kind
	}
	if kind == refuseStatusWrite {
		// The write that publishes the weights is the refused one; the
		// picture is the refusal itself, read as such.
		w.readPublicationRefused(r)
		return
	}
	for _, c := range w.components() {
		for rev, pct := range r.traffic[c] {
			if pct == 0 || r.serving[c][rev] > 0 || len(r.serving[c]) == 0 {
				continue
			}
			if w.previous.traffic[c][rev] > 0 && w.previous.serving[c][rev] == 0 && len(w.previous.serving[c]) > 0 {
				w.record("traffic-on-unserved-revision/"+kind.String(), "%s: %d%% of the traffic names revision %s, which has no serving pod, while %s serves", c, pct, shortRev(rev), servingList(r.serving[c]))
			}
		}
		if len(r.traffic[c]) == 0 {
			continue
		}
		for rev, n := range r.serving[c] {
			if n == 0 || r.traffic[c][rev] > 0 {
				continue
			}
			if w.previous.serving[c][rev] > 0 && w.previous.traffic[c][rev] == 0 && len(w.previous.traffic[c]) > 0 {
				w.record("serving-revision-without-weight/"+kind.String(), "%s: revision %s serves with %d pods and carries no weight; the weights name %s", c, shortRev(rev), n, trafficList(r.traffic[c]))
			}
		}
	}
}

// readPublicationRefused counts the ticks the published weights stand
// behind the pods while status writes are refused.
func (w *refusedWorld) readPublicationRefused(r *tickReading) {
	for _, c := range w.components() {
		for rev, pct := range r.traffic[c] {
			if pct > 0 && r.serving[c][rev] == 0 && len(r.serving[c]) > 0 {
				w.record("publication-refused", "%s: the weights still name %s while status writes are refused", c, shortRev(rev))
				return
			}
		}
	}
}

func servingList(m map[string]int32) string {
	var parts []string
	for rev, n := range m {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%s (%d)", shortRev(rev), n))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func trafficList(m map[string]int32) string {
	var parts []string
	for rev, pct := range m {
		parts = append(parts, fmt.Sprintf("%s:%d", shortRev(rev), pct))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// readRejected names, after the operator's push back, a new pod built
// toward the revision the operator rejected, and an old pod taken out of
// rotation by the move toward it: the pods serving the kept revision fall
// while a pod of the rejected one serves.
func (w *refusedWorld) readRejected(r *tickReading) {
	if w.pushBackAt == 0 || w.tickNo <= w.pushBackAt {
		return
	}
	for name, image := range w.newPods {
		if image == w.rejected && !strings.Contains(name, "-router-") {
			w.record("start-toward-rejected-revision/"+r.kind.String(), "pod %s was created on %s, %d ticks after the operator rolled back to %s", name, image, w.tickNo-w.pushBackAt, w.images[v1beta1.EngineComponent])
		}
	}
	if w.resized || w.previous == nil {
		return
	}
	kept := w.images[v1beta1.EngineComponent]
	if r.servingByImage[kept] < w.previous.servingByImage[kept] && r.servingByImage[w.rejected] > 0 {
		w.record("completion-onto-rejected-revision/"+r.kind.String(), "the pods serving %s, the revision the operator rolled back to, fell from %d to %d while %d serve the rejected %s, %d ticks after the rollback", kept, w.previous.servingByImage[kept], r.servingByImage[kept], r.servingByImage[w.rejected], w.rejected, w.tickNo-w.pushBackAt)
	}
}

// holdText is what the replicas say holds them: the hold on each replica
// and the open operations.
func (w *refusedWorld) holdText() string {
	var parts []string
	for _, c := range w.components() {
		ir := &v1beta1.InferenceReplica{}
		if err := w.cli.Get(w.ctx, w.replicaKey(c), ir); err != nil {
			parts = append(parts, fmt.Sprintf("%s: %v", c, err))
			continue
		}
		part := fmt.Sprintf("%s cur=%s upd=%s updatedReady=%d/%d", c, shortRev(ir.Status.CurrentRevision), shortRev(ir.Status.UpdateRevision), ir.Status.UpdatedReadyReplicas, ir.Status.Replicas)
		if ir.Status.RolloutHold != nil {
			part += fmt.Sprintf(" hold=%s: %s", ir.Status.RolloutHold.Gate, ir.Status.RolloutHold.Reason)
		}
		parts = append(parts, part)
	}
	if w.lastPassErr != nil {
		parts = append(parts, "service pass: "+shortErr(w.lastPassErr))
	}
	return strings.Join(parts, "; ")
}

// playOut ticks until the world settles with no window open, or the tick
// bound passes. Settled means every Component at its count on the image
// the spec names, nothing leaving, the replica promoted, one revision
// weighted at 100.
func (w *refusedWorld) playOut(maxTicks int) bool {
	settledTicks := 0
	for i := 0; i < maxTicks; i++ {
		w.tick()
		if w.windowCloses == 0 && w.previous.settled {
			settledTicks++
			if settledTicks >= 2 {
				return true
			}
		} else {
			settledTicks = 0
		}
	}
	return false
}

// classifyEnd records how a probe ended.
func (w *refusedWorld) classifyEnd(settled bool) {
	if settled {
		w.classes["complete"]++
		return
	}
	w.record("holds-forever/"+w.refuse.current().String(), "the roll did not finish within the tick bound: %s", w.holdText())
}

// reachIdle builds the fleet at version 1 and waits for it to settle.
func (w *refusedWorld) reachIdle() {
	w.t.Helper()
	w.path = "build"
	if !w.playOut(40) {
		w.t.Fatalf("the fleet never settled at version 1: %s", w.holdText())
	}
}

// captureMoments rolls version 1 to version 2 with nothing refused and
// keeps a snapshot at each moment of the roll.
func (w *refusedWorld) captureMoments(maxTicks int) map[refusedMoment]*refusedSnapshot {
	w.t.Helper()
	snaps := map[refusedMoment]*refusedSnapshot{atIdle: w.snapshot()}
	w.path = "reference roll"
	w.push(refusedImageV2)
	for i := 0; i < maxTicks; i++ {
		w.tick()
		w.keepMoments(snaps, w.prefillRoll(refusedImageV1, refusedImageV2))
		if w.previous.settled && w.windowCloses == 0 {
			break
		}
	}
	if !w.previous.settled {
		w.t.Fatalf("the reference roll never settled: %s", w.holdText())
	}
	for _, class := range []string{"traffic-on-unserved-revision/none", "serving-revision-without-weight/none", "unavailable-budget-exceeded/none", "surge-budget-exceeded/none"} {
		if f, found := w.findings[class]; found {
			w.t.Errorf("the reference roll with nothing refused shows %s: %s\n  state: %s", class, f.message, f.state)
		}
	}
	return snaps
}

// prefillRollView is where the prefill side's roll stands: whether a pod
// of the target exists and serves, whether an old pod is leaving, and how
// many Instances still run the old image.
type prefillRollView struct {
	newExists, newServes, oldLeaving bool
	oldInstances                     int32
}

func (w *refusedWorld) prefillRoll(old, target string) prefillRollView {
	var v prefillRollView
	oldSeen := map[string]bool{}
	for _, p := range w.pods() {
		if p.component != v1beta1.EngineComponent || !p.routed {
			continue
		}
		switch p.image {
		case target:
			v.newExists = true
			v.newServes = v.newServes || p.serving
		case old:
			if p.leaving {
				v.oldLeaving = true
			} else if !oldSeen[p.instance] {
				oldSeen[p.instance] = true
				v.oldInstances++
			}
		}
	}
	return v
}

// keepMoments takes the snapshot of each moment the first time the roll
// reaches it.
func (w *refusedWorld) keepMoments(snaps map[refusedMoment]*refusedSnapshot, v prefillRollView) {
	moved := v.oldInstances < w.cfg.shape.prefill
	reached := map[refusedMoment]bool{
		atSettling: v.newExists || moved,
		atRotating: v.newServes && moved,
		atDraining: snaps[atRotating] != nil && v.oldInstances >= 1 && v.oldInstances <= w.budgetOf(w.cfg.shape.prefill),
		atLast:     snaps[atDraining] != nil && v.oldInstances == 0 && !v.oldLeaving,
	}
	for _, m := range []refusedMoment{atSettling, atRotating, atDraining, atLast} {
		if snaps[m] == nil && reached[m] {
			snaps[m] = w.snapshot()
		}
	}
}

// OME_REFUSED_WRITES_HUNT=full runs every kind, window and action on the
// reference shapes and the bounded probes on a sample of the reference
// shapes; =reference runs the bounded probes on the reference shapes.
// OME_REFUSED_WRITES_HUNT_ONLY=<text> keeps the configurations whose name
// contains the text; OME_REFUSED_WRITES_HUNT_ACTIONS=<text,text> keeps the
// probes whose action's name contains one of the texts.
const (
	refusedHuntEnv    = "OME_REFUSED_WRITES_HUNT"
	refusedOnlyEnv    = "OME_REFUSED_WRITES_HUNT_ONLY"
	refusedActionsEnv = "OME_REFUSED_WRITES_HUNT_ACTIONS"
)

func refusedTickBound(full bool) int {
	if full {
		return 220
	}
	return 120
}

// refusedProbes lists the probes of a run: every moment, every refusal
// kind at every window length, and every push or strategy edit, in the
// full set; the refused kinds and the Service write, one window and the
// actions that change the spec in the bounded one. A resize or a partition
// change as the window opens is outside this model's readings: its budgets
// and its settled picture read the count and the partition the roll began
// with.
func refusedProbes(full bool) []refusedProbe {
	moments := []refusedMoment{atSettling, atRotating, atLast}
	kinds := []refusedKind{refuseReplicaWrite, refuseServiceWrite, refuseWebhookWrites}
	windows := []int{3}
	actions := []refusedAction{actNone, actPushBack, actStrategyEdit}
	if full {
		moments = []refusedMoment{atIdle, atSettling, atRotating, atDraining, atLast}
		kinds = []refusedKind{refusePodCreate, refuseReplicaWrite, refuseRouterWrite, refuseServiceWrite, refuseStatusWrite, refuseWebhookWrites}
		windows = []int{2, 6}
		actions = []refusedAction{actNone, actPushBack, actPushNext, actStrategyEdit}
	}
	var out []refusedProbe
	for _, m := range moments {
		for _, a := range actions {
			if m == atIdle && a == actPushBack || !actionWanted(a) {
				continue
			}
			if a != actNone {
				out = append(out, refusedProbe{moment: m, action: a})
			}
			for _, k := range kinds {
				for _, win := range windows {
					out = append(out, refusedProbe{moment: m, kind: k, window: win, action: a})
				}
			}
		}
	}
	return out
}

// actionWanted reports whether the action filter, if set, names a.
func actionWanted(a refusedAction) bool {
	filter := os.Getenv(refusedActionsEnv)
	if filter == "" {
		return true
	}
	for _, text := range strings.Split(filter, ",") {
		if strings.Contains(a.String(), strings.TrimSpace(text)) {
			return true
		}
	}
	return false
}

// refusedBoundedConfigs are the configurations the bounded run explores:
// the two small shapes and one reference shape under
// RecreatePod; a tick costs about a fifth of a second on these shapes.
func refusedBoundedConfigs() []refusedConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	return []refusedConfig{
		{shape: refusedShape{2, 2, 2, 2}, settings: refusedSettings{prefill: surge, decode: surge}},
		{shape: refusedShape{4, 4, 1, 2}, settings: refusedSettings{prefill: surge, decode: surge}},
		{shape: refusedShape{12, 8, 1, 0}, settings: refusedSettings{prefill: recreate, decode: recreate}},
	}
}

// refusedReferenceConfigs are the configurations the full run explores
// with the full probe set: the bounded ones and the reference shapes
// under their reference settings.
func refusedReferenceConfigs() []refusedConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	return append(refusedBoundedConfigs(),
		refusedConfig{shape: refusedShape{12, 4, 2, 2}, settings: refusedSettings{prefill: surge, decode: surge, group: true, maintainRatio: true, gangTiming: true}},
		refusedConfig{shape: refusedShape{16, 8, 1, 0}, settings: refusedSettings{prefill: recreate, decode: surge, group: true}},
		refusedConfig{shape: refusedShape{12, 8, 1, 0}, settings: refusedSettings{prefill: surge, decode: surge, partition: 2}},
	)
}

func refusedConfigs(full, reference bool) []refusedConfig {
	if !full && !reference {
		return refusedBoundedConfigs()
	}
	configs := refusedReferenceConfigs()
	if !full {
		return configs
	}
	for i := range configs {
		configs[i].thorough = true
	}
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	shapes := []refusedShape{{12, 8, 1, 0}, {15, 15, 1, 0}, {16, 8, 1, 2}, {20, 10, 1, 0}, {24, 8, 1, 0}, {30, 10, 1, 0}, {12, 4, 1, 2}, {12, 8, 2, 2}, {12, 4, 4, 0}, {12, 4, 8, 0}, {2, 2, 2, 2}, {4, 4, 1, 2}}
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	variants := []refusedSettings{{}, {group: true}, {group: true, maintainRatio: true}, {partition: 2}, {gangTiming: true}, {group: true, gangTiming: true}}
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	var all []refusedConfig
	for _, shape := range shapes {
		for _, pair := range pairs {
			for _, variant := range variants {
				settings := variant
				settings.prefill, settings.decode = pair[0], pair[1]
				cfg := refusedConfig{shape: shape, settings: settings}
				// A partition that holds every prefill Instance leaves the
				// prefill side no roll to probe.
				if settings.partition >= shape.prefill {
					continue
				}
				if !seen[cfg.String()] {
					seen[cfg.String()] = true
					all = append(all, cfg)
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if len(all) > refusedFullSample {
		all = all[:refusedFullSample]
	}
	return append(configs, all...)
}

const refusedFullSample = 36

// explore runs every probe of the mode from the moments of one reference
// roll, then random runs under a fixed seed.
func (w *refusedWorld) explore(full bool, seed int64, runs int) {
	w.t.Helper()
	maxTicks := refusedTickBound(full)
	w.reachIdle()
	snaps := w.captureMoments(maxTicks)
	var captured []string
	for m := atIdle; m < momentCount; m++ {
		if snaps[m] != nil {
			captured = append(captured, m.String())
		}
	}
	w.t.Logf("moments captured: %s", strings.Join(captured, ", "))
	for _, p := range refusedProbes(full) {
		snap := snaps[p.moment]
		if snap == nil {
			continue
		}
		w.restore(snap)
		w.path = p.String()
		w.probes++
		w.apply(p)
		if p.moment == atIdle && p.action != actPushNext {
			w.push(refusedImageV2)
		}
		w.classifyEnd(w.playOut(maxTicks))
	}
	w.randomRuns(snaps[atIdle], seed, runs, maxTicks, full)
}

// randomRuns plays the roll from idle with up to three probes at random
// ticks, under a fixed seed.
func (w *refusedWorld) randomRuns(idle *refusedSnapshot, seed int64, runs, maxTicks int, full bool) {
	w.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	probes := refusedProbes(full)
	if len(probes) == 0 {
		return
	}
	for run := 0; run < runs; run++ {
		w.restore(idle)
		w.path = fmt.Sprintf("random run %d", run+1)
		w.probes++
		w.push(refusedImageV2)
		left := 3
		settledTicks := 0
		for i := 0; i < maxTicks; i++ {
			if left > 0 && w.windowCloses == 0 && rng.Intn(6) == 0 {
				p := probes[rng.Intn(len(probes))]
				w.apply(p)
				left--
			}
			w.tick()
			if w.windowCloses == 0 && w.previous.settled {
				settledTicks++
				if settledTicks >= 2 {
					break
				}
			} else {
				settledTicks = 0
			}
		}
		w.classifyEnd(settledTicks >= 2)
	}
}

// refusedExpectedEnds are the ends that are not findings.
var refusedExpectedEnds = map[string]bool{"complete": true, "publication-refused": true}

// knownRefusedClasses are the classes the model shows at present, each
// with the picture behind it. A finding in a known class is reported and
// does not fail the run; a change of behaviour removes its entry and the lock test that
// holds the picture.
var knownRefusedClasses = map[string]string{
	"traffic-on-unserved-revision/service-write":      knownServiceWriteEndsThePass,
	"serving-revision-without-weight/service-write":   knownServiceWriteEndsThePass,
	"start-toward-rejected-revision/replica-write":    knownRollbackWaits,
	"start-toward-rejected-revision/webhooks-down":    knownRollbackWaits,
	"completion-onto-rejected-revision/replica-write": knownRollbackWaits,
	"completion-onto-rejected-revision/webhooks-down": knownRollbackWaits,
}

const knownServiceWriteEndsThePass = "under a refused Service write two pictures meet: the coordination pass ends at the first per-revision Service write the apiserver refuses, before its traffic step, which the coordination pass treats as intended behaviour (held by TestServicePassUnderRefusedWrites_ATrafficStepSkippedByARefusedServiceWriteLeavesTheWeightOnARevisionWithoutPods); and the roll goes on toward a revision nothing can route to, because the new revision's per-revision Services come from the service pass once a pod of it exists, the surge takes a replacement whose routing Service does not exist as one that needs no listing, and RecreatePod takes an old pod down once the replacement before it is Ready (held by TestServicePassUnderRefusedWrites_ARollGoesOnWhileTheNewRevisionHasNoService)"

const knownRollbackWaits = "the operator's rollback reaches the replica only through the projection, and the replica controller follows its spec until then: it finishes the open move onto the rejected revision and opens the next toward it (an open question: the controllers wait for the manager's own webhook, or the replica admits no start while its projection lags the parent's generation; held by TestServicePassUnderRefusedWrites_ARollbackRefusedAtTheReplicaLetsTheRollGoOnTowardTheRejectedRevision)"

type refusedResult struct {
	cfg      refusedConfig
	states   int
	ticks    int
	probes   int
	classes  map[string]int
	findings map[string]refusedFinding
	took     time.Duration
}

type refusedExample struct {
	cfg     refusedConfig
	finding refusedFinding
}

// refusedExplore runs the model over the configurations keep admits, one
// subtest per configuration in parallel, and reports every class, failing
// on the classes that are neither expected ends nor known. A thorough
// configuration runs the full probe set and eight random runs; the others
// run the bounded set and one.
func refusedExplore(t *testing.T, keep func(refusedConfig) bool) {
	t.Helper()
	full := os.Getenv(refusedHuntEnv) == "full"
	reference := os.Getenv(refusedHuntEnv) == "reference"
	only := os.Getenv(refusedOnlyEnv)
	start := time.Now()
	var configs []refusedConfig
	for _, cfg := range refusedConfigs(full, reference) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]refusedResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				results[i] = refusedResult{cfg: cfg}
				runs := 1
				if cfg.thorough {
					runs = 8
				}
				w := newRefusedWorld(t, cfg)
				w.explore(cfg.thorough, 1, runs)
				results[i] = refusedResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, probes: w.probes, classes: w.classes, findings: w.findings, took: time.Since(began)}
			})
		}
	})
	totals := map[string]int{}
	configurations := map[string]int{}
	examples := map[string][]refusedExample{}
	var states, ticks, probes int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		probes += res.probes
		var parts []string
		for class, c := range res.classes {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes, %s: %s", n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 4 {
				examples[class] = append(examples[class], refusedExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d probes, %s", len(configs), states, ticks, probes, time.Since(start).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d ticks in %d configurations", class, totals[class], configurations[class])
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		switch {
		case refusedExpectedEnds[class]:
		case knownRefusedClasses[class] != "":
			t.Logf("  known: %s", knownRefusedClasses[class])
		default:
			t.Errorf("%s: %d ticks (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// TestServicePassUnderRefusedWrites_EveryWindowLeavesTheRollAWayOut
// explores every configuration: a refusal window of each kind at each
// moment of a roll, with and without an operator action as it opens,
// leaves a roll that finishes on the revision the spec names, with the
// traffic following the pods that serve, no Instance started toward a
// revision the operator rolled back from, and the budgets kept.
func TestServicePassUnderRefusedWrites_EveryWindowLeavesTheRollAWayOut(t *testing.T) {
	refusedExplore(t, func(refusedConfig) bool { return true })
}

// lockPath builds the configuration, rolls it to the moment with nothing
// refused, applies the probe and plays it out for ticks ticks. It returns
// the world with its classes and trace.
func lockPath(t *testing.T, cfg refusedConfig, moment refusedMoment, probe refusedProbe, ticks int) *refusedWorld {
	t.Helper()
	w := newRefusedWorld(t, cfg)
	w.reachIdle()
	snaps := w.captureMoments(refusedTickBound(false))
	if snaps[moment] == nil {
		t.Fatalf("the reference roll never reached %s", moment)
	}
	w.restore(snaps[moment])
	w.path = probe.String()
	w.apply(probe)
	for i := 0; i < ticks; i++ {
		w.tick()
	}
	return w
}

// TestServicePassUnderRefusedWrites_ATrafficStepRunsPastARefusedRouterWrite
// holds the small shape: 2 prefill gangs of 2 pods, 2 decode
// gangs and 2 routers under SurgeThenDrain. The first replacement gang
// serves, the operator edits the prefill strategy to RecreatePod, and the
// router's InferenceReplica write is refused for six ticks while the
// engine's and the decoder's landed. Every pass ends with the router's
// error, the replica controller drains the old gang, and the weights
// follow the pods that serve on the next pass all the same: no tick ends
// twice in a row with weight on a revision no pod serves, or with a
// serving revision carrying none.
func TestServicePassUnderRefusedWrites_ATrafficStepRunsPastARefusedRouterWrite(t *testing.T) {
	cfg := refusedConfig{shape: refusedShape{2, 2, 2, 2}, settings: refusedSettings{prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain}}
	w := lockPath(t, cfg, atRotating, refusedProbe{moment: atRotating, kind: refuseRouterWrite, window: 6, action: actStrategyEdit}, 8)
	refusedPasses := 0
	for _, line := range w.trace {
		if strings.Contains(line, "failed to project InferenceReplica for router") {
			refusedPasses++
		}
	}
	if refusedPasses < 3 {
		t.Fatalf("the picture moved: the router's write was refused on %d passes, so the path stops exercising the pass under a refused write\n%s", refusedPasses, strings.Join(w.trace, "\n"))
	}
	for class, n := range w.classes {
		if strings.HasPrefix(class, "traffic-on-unserved-revision/") || strings.HasPrefix(class, "serving-revision-without-weight/") {
			t.Fatalf("%s on %d ticks: the weights did not follow the pods past the refused write: %s\n%s", class, n, w.findings[class].message, strings.Join(w.trace, "\n"))
		}
	}
	if !w.playOut(refusedTickBound(false)) {
		t.Fatalf("the roll did not finish once the write was allowed: %s", w.holdText())
	}
}

// TestServicePassUnderRefusedWrites_ATrafficStepSkippedByARefusedServiceWriteLeavesTheWeightOnARevisionWithoutPods
// holds the picture of the coordination pass under its own refused write:
// on the small shape, the first replacement serves when the operator
// pushes a third revision and every Service write starts to time out for
// six ticks. The pass ends at the third revision's Service create before
// its traffic step, the replica controller gives the open second-revision
// move up for one toward the third, and the weights keep naming the second
// revision with no pod behind it until the write is allowed. The scenario
// apiserver-refusals-leave-the-group-entry records the pass ending there as
// intended; when the traffic step runs past the refused Service write, this
// test fails, and the known class and this lock go with it.
func TestServicePassUnderRefusedWrites_ATrafficStepSkippedByARefusedServiceWriteLeavesTheWeightOnARevisionWithoutPods(t *testing.T) {
	cfg := refusedConfig{shape: refusedShape{2, 2, 2, 2}, settings: refusedSettings{prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain}}
	w := lockPath(t, cfg, atRotating, refusedProbe{moment: atRotating, kind: refuseServiceWrite, window: 6, action: actPushNext}, 8)
	if w.classes["traffic-on-unserved-revision/service-write"] == 0 {
		t.Fatalf("the picture moved: no tick ended twice in a row with weight on a revision no pod serves; if the traffic step now runs past a refused Service write, remove this lock\n%s", strings.Join(w.trace, "\n"))
	}
	if !w.playOut(refusedTickBound(false)) {
		t.Fatalf("the roll did not finish once the write was allowed: %s", w.holdText())
	}
}

// TestServicePassUnderRefusedWrites_ARollGoesOnWhileTheNewRevisionHasNoService
// holds the current picture of a roll under refused Service writes: on the
// small shape, a new pod exists for the first replacement, the
// operator edits the prefill strategy to RecreatePod, and every Service
// write times out for six ticks, so the new revision's per-revision
// Services cannot be created. The roll goes on under either strategy:
// RecreatePod takes an old pod down once the replacement before it is
// Ready, and the surge takes a replacement whose routing Service does not
// exist as one that needs no listing. The pods of the new revision serve,
// nothing can route to them, and the weights stay on the revision whose
// pods left. When the engine waits for the new revision's Service before
// it takes an old pod down, or the Services come with the revision's first
// pod, this test fails, and the known class and this lock go with it.
func TestServicePassUnderRefusedWrites_ARollGoesOnWhileTheNewRevisionHasNoService(t *testing.T) {
	cfg := refusedConfig{shape: refusedShape{2, 2, 2, 2}, settings: refusedSettings{prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain}}
	w := lockPath(t, cfg, atSettling, refusedProbe{moment: atSettling, kind: refuseServiceWrite, window: 6, action: actStrategyEdit}, 8)
	withoutWeight := w.classes["serving-revision-without-weight/service-write"]
	if withoutWeight == 0 {
		t.Fatalf("the picture moved: no tick ended with the new revision serving and carrying no weight; if the engine now waits for the Service, remove the known class and this lock\n%s", strings.Join(w.trace, "\n"))
	}
	if !w.playOut(refusedTickBound(false)) {
		t.Fatalf("the roll did not finish once the write was allowed: %s", w.holdText())
	}
}

// TestServicePassUnderRefusedWrites_ARollbackRefusedAtTheReplicaLetsTheRollGoOnTowardTheRejectedRevision
// holds the picture on the medium shape: 4 prefill, 4 decode and 2 routers
// under SurgeThenDrain. The first replacement serves, the operator rolls
// back to the revision the roll started from, and the InferenceReplica
// writes are refused for eight ticks. The rollback waits in the service's
// spec; the replica controller finishes the first Instance onto the
// rejected revision and starts the second toward it. When the replica
// controller admits no start toward a revision the service does not
// name, this test fails, and the known class and this lock go with it.
func TestServicePassUnderRefusedWrites_ARollbackRefusedAtTheReplicaLetsTheRollGoOnTowardTheRejectedRevision(t *testing.T) {
	cfg := refusedConfig{shape: refusedShape{4, 4, 1, 2}, settings: refusedSettings{prefill: v1beta1.UpdateStrategySurgeThenDrain, decode: v1beta1.UpdateStrategySurgeThenDrain}}
	w := lockPath(t, cfg, atRotating, refusedProbe{moment: atRotating, kind: refuseReplicaWrite, window: 8, action: actPushBack}, 10)
	started := w.classes["start-toward-rejected-revision/replica-write"]
	completed := w.classes["completion-onto-rejected-revision/replica-write"]
	if started == 0 || completed == 0 {
		t.Fatalf("the picture moved: starts toward the rejected revision %d, completions onto it %d; if the replica now waits for the rollback, remove the known class and this lock\n%s", started, completed, strings.Join(w.trace, "\n"))
	}
	if !w.playOut(refusedTickBound(false)) {
		t.Fatalf("the rollback did not finish once the write was allowed: %s", w.holdText())
	}
	for _, p := range w.pods() {
		if p.component != v1beta1.RouterComponent && p.image != refusedImageV1 {
			t.Fatalf("pod %s ended on %s after the rollback to %s", p.instance, p.image, refusedImageV1)
		}
	}
}
