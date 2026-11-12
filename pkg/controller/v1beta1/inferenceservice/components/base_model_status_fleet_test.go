package components

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateModelStatusGoldens = flag.Bool("update-model-status", false, "re-record testdata/model_status_allocations.json from this run")

const (
	modelStatusNamespace   = "fleet"
	modelStatusServiceName = "fleet"
	// modelStatusGangSize is the pods of one MultiNode group: a leader and
	// seven workers, the reference shape.
	modelStatusGangSize = 8
)

// modelStatusShape is one engine whose status the component reconciler
// writes at the end of its pass: the deployment mode, which decides what its
// pods carry, and how many pods of it the namespace holds.
type modelStatusShape struct {
	name      string
	mode      constants.DeploymentModeType
	instances int
}

// modelStatusShapes are the budget's shapes: every deployment mode at the
// largest reference replica of 4,000 pods and a quarter of it.
func modelStatusShapes() []modelStatusShape {
	var shapes []modelStatusShape
	for _, mode := range []constants.DeploymentModeType{constants.RawDeployment, constants.MultiNode, constants.OMENative} {
		for _, instances := range []int{1000, 4000} {
			shapes = append(shapes, modelStatusShape{name: fmt.Sprintf("%s/%d", mode, instances), mode: mode, instances: instances})
		}
	}
	return shapes
}

// replicas is the engine's replica count: one Instance per pod, or one per
// MultiNode group.
func (s modelStatusShape) replicas() int {
	if s.mode == constants.MultiNode {
		return s.instances / modelStatusGangSize
	}
	return s.instances
}

// modelState is what the model status writer leaves on the service: the
// transition status, the target model state and the copies it counted, -1
// when it wrote no count.
type modelState struct {
	transition v1beta1.TransitionStatus
	target     v1beta1.ModelState
	copies     int
}

func modelStateOf(s *v1beta1.InferenceServiceStatus) modelState {
	state := modelState{transition: s.ModelStatus.TransitionStatus, copies: -1}
	if s.ModelStatus.ModelRevisionStates != nil {
		state.target = s.ModelStatus.ModelRevisionStates.TargetModelState
	}
	if s.ModelStatus.ModelCopies != nil {
		state.copies = s.ModelStatus.ModelCopies.TotalCopies
	}
	return state
}

// wantModelState is the state the pods the list returns feed on a Ready
// service. RawDeployment pods and MultiNode leaders carry the app label the
// list asks for, so the service reads Loaded with the matched pods as its
// copies. An OMENative pod carries the ome.io labels alone, so the writer
// lists none and writes Pending, whatever the pods do.
func (s modelStatusShape) wantModelState(tb testing.TB) modelState {
	tb.Helper()
	switch s.mode {
	case constants.RawDeployment, constants.MultiNode:
		return modelState{transition: v1beta1.UpToDate, target: v1beta1.Loaded, copies: s.replicas()}
	case constants.OMENative:
		return modelState{transition: v1beta1.InProgress, target: v1beta1.Pending, copies: -1}
	}
	tb.Fatalf("no model state is expected for the %s mode", s.mode)
	return modelState{}
}

// namespacePods answers a List of pods the way the manager's informer cache
// answers it: it walks every pod of the namespace, keeps the ones the label
// selector matches and deep-copies those into the list. It records every pod
// list by its selector and counts the pods it copied. Every other call
// reaches the fake client it wraps.
type namespacePods struct {
	client.Client
	pods      []*corev1.Pod
	selectors []labels.Selector
	copied    int
}

func (c *namespacePods) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	podList, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	options := client.ListOptions{}
	options.ApplyOptions(opts)
	selector := labels.Everything()
	if options.LabelSelector != nil {
		selector = options.LabelSelector
	}
	c.selectors = append(c.selectors, selector)
	podList.Items = podList.Items[:0]
	for _, pod := range c.pods {
		if options.Namespace != "" && pod.Namespace != options.Namespace {
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		podList.Items = append(podList.Items, *pod.DeepCopy())
		c.copied++
	}
	return nil
}

// modelStatusFixture is one engine behind the stand-in cache: the component
// fields the reconciler hands its status writer, the service as the pass
// holds it, the engine's object metadata, and the counts of the pod lists,
// the pods copied and the writes the status writer makes.
type modelStatusFixture struct {
	fields     *BaseComponentFields
	isvc       *v1beta1.InferenceService
	objectMeta metav1.ObjectMeta
	cache      *namespacePods
	writes     *fakeclient.Calls
}

func newModelStatusFixture(tb testing.TB, shape modelStatusShape) *modelStatusFixture {
	tb.Helper()
	isvc := modelStatusServiceObject(shape)
	objectMeta := metav1.ObjectMeta{
		Name:      render.ComponentName(isvc, v1beta1.EngineComponent),
		Namespace: isvc.Namespace,
		Labels:    modelStatusComponentLabels(),
	}
	writes := fakeclient.CountingWrites()
	cache := &namespacePods{
		Client: componentClient(tb, writes),
		pods:   modelStatusFixturePods(shape, objectMeta.Name),
	}
	return &modelStatusFixture{
		fields: &BaseComponentFields{
			Piece:         render.Piece{DeploymentMode: shape.mode, Log: logr.Discard()},
			Client:        cache,
			StatusManager: status.NewStatusReconciler(),
		},
		isvc:       isvc,
		objectMeta: objectMeta,
		cache:      cache,
		writes:     writes,
	}
}

// modelStatusServiceObject is the service at rest: a model, a runtime, one
// engine in the shape's mode with no autoscaler, and a status that reads
// Ready, so the pods the list returns are read as Loaded.
func modelStatusServiceObject(shape modelStatusShape) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: modelStatusServiceName, Namespace: modelStatusNamespace,
			UID: types.UID(modelStatusServiceName + "-isvc-uid"), Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(shape.mode),
			Model:          &v1beta1.ModelRef{Name: "example-model"},
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "example-runtime"},
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(shape.replicas()),
					MaxReplicas: shape.replicas(),
					Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
				},
			},
		},
	}
	for _, conditionType := range []apis.ConditionType{apis.ConditionReady, v1beta1.EngineReady, v1beta1.IngressReady} {
		isvc.Status.Conditions = append(isvc.Status.Conditions, apis.Condition{Type: conditionType, Status: corev1.ConditionTrue})
	}
	return isvc
}

// modelStatusComponentLabels are the labels the render layer puts on the
// engine's object metadata, which every mode's pods inherit.
func modelStatusComponentLabels() map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: modelStatusServiceName,
		constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
		constants.ServingRuntimeLabelKey:      "example-runtime",
		constants.FTServingLabelKey:           "false",
	}
}

// modelStatusFixturePods are the engine's pods as the mode renders them,
// every one Running with its main container Ready, created one second apart
// so the newest-first sort of the list has work to do. RawDeployment pods
// carry the app label; of a MultiNode group only the leader carries it; an
// OMENative pod carries the ome.io labels of the workload instead.
func modelStatusFixturePods(shape modelStatusShape, componentName string) []*corev1.Pod {
	appLabel := constants.GetRawServiceLabel(componentName)
	workloadComponent := v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pods := make([]*corev1.Pod, 0, shape.instances)
	for i := 0; i < shape.instances; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         modelStatusNamespace,
				Labels:            modelStatusComponentLabels(),
				CreationTimestamp: metav1.NewTime(start.Add(time.Duration(i) * time.Second)),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}}},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: constants.MainContainerName, Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}},
			},
		}
		switch shape.mode {
		case constants.RawDeployment:
			pod.Name = fmt.Sprintf("%s-%d", componentName, i)
			pod.Labels[constants.RawDeploymentAppLabel] = appLabel
		case constants.MultiNode:
			group, member := i/modelStatusGangSize, i%modelStatusGangSize
			pod.Name = fmt.Sprintf("%s-%d-%d", componentName, group, member)
			if member == 0 {
				pod.Labels[constants.RawDeploymentAppLabel] = appLabel
			}
		case constants.OMENative:
			pod.Name = query.PodName(modelStatusServiceName, workloadComponent, int32(i), string(v1beta1.RunnerNameDefault), 0)
			pod.Labels[query.LabelManagedBy] = query.ManagedByOMENative
			pod.Labels[query.LabelInstanceIdx] = fmt.Sprint(i)
			pod.Labels[query.LabelRunner] = string(v1beta1.RunnerNameDefault)
			pod.Labels[query.LabelPodOrdinal] = "0"
			pod.Labels[query.LabelRevisionHash] = "0123456789ab"
		}
		pods = append(pods, pod)
	}
	return pods
}

// appLabelSelector is the selector the status writer lists the pods by.
func (f *modelStatusFixture) appLabelSelector() string {
	return constants.RawDeploymentAppLabel + "=" + constants.GetRawServiceLabel(f.objectMeta.Name)
}

// update runs the status writer once, as the engine reconciler does at the
// end of its pass.
func (f *modelStatusFixture) update(tb testing.TB) {
	tb.Helper()
	if err := UpdateComponentStatus(f.fields, f.isvc, v1beta1.EngineComponent, f.objectMeta, &f.isvc.Spec.Engine.ComponentExtensionSpec); err != nil {
		tb.Fatalf("UpdateComponentStatus: %v", err)
	}
}

// updateCounting runs the status writer once and returns the selectors of
// the pod lists it made, the pods those lists copied and the writes it made.
func (f *modelStatusFixture) updateCounting(tb testing.TB) (selectors []string, copied, writes int) {
	tb.Helper()
	listsBefore, copiedBefore, writesBefore := len(f.cache.selectors), f.cache.copied, f.writes.Count()
	f.update(tb)
	for _, selector := range f.cache.selectors[listsBefore:] {
		selectors = append(selectors, selector.String())
	}
	return selectors, f.cache.copied - copiedBefore, f.writes.Count() - writesBefore
}

// BenchmarkModelStatusPods is the cost of the component status writer's pod
// list per deployment mode at fleet size, with the pod lists it makes and
// the pods those lists copy beside the time.
func BenchmarkModelStatusPods(b *testing.B) {
	for _, shape := range modelStatusShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newModelStatusFixture(b, shape)
			b.ReportAllocs()
			lists, copied := len(f.cache.selectors), f.cache.copied
			b.ResetTimer()
			for b.Loop() {
				f.update(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(len(f.cache.selectors)-lists)/float64(b.N), "pod-lists/op")
			b.ReportMetric(float64(f.cache.copied-copied)/float64(b.N), "pods-copied/op")
		})
	}
}

// TestModelStatusPodsStayWithinBudget holds the component status writer, per
// deployment mode at 1,000 and 4,000 pods, to the recorded golden: its
// allocations, the pod lists it makes by the app label and the pods those
// lists copy. Beside the golden: every pod list asks for the app label and
// nothing else, an OMENative status writer lists no pods, a status writer
// writes nothing to the apiserver, and the model state it leaves is the one
// the mode's pods feed.
func TestModelStatusPodsStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "model_status_allocations.json"), *updateModelStatusGoldens)
	defer golden.Close()
	for _, shape := range modelStatusShapes() {
		f := newModelStatusFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.update(t) })
		selectors, copied, writes := f.updateCounting(t)
		golden.CheckCount(shape.name+"/app-label-pod-lists", int64(len(selectors)))
		golden.CheckCount(shape.name+"/pods-copied", int64(copied))
		for _, selector := range selectors {
			if selector != f.appLabelSelector() {
				t.Errorf("%s: a pod list asked for %q; the status writer lists by %q alone", shape.name, selector, f.appLabelSelector())
			}
		}
		if shape.mode == constants.OMENative && len(selectors) != 0 {
			t.Errorf("%s: the status writer listed the pods %d times; an OMENative pod never carries the app label, so the list is not made", shape.name, len(selectors))
		}
		if writes != 0 {
			t.Errorf("%s: the status writer wrote %d times; it writes the pass's copy of the service, never the apiserver", shape.name, writes)
		}
		if got, want := modelStateOf(&f.isvc.Status), shape.wantModelState(t); got != want {
			t.Errorf("%s: model state %+v, want %+v", shape.name, got, want)
		}
	}
}
