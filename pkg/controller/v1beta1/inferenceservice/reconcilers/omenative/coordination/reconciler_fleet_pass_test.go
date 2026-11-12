package coordination

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// The controller runs the coordination layer on every pass of every service
// with an OMENative Component, whether a roll is open or not: it lists each
// Component's pods from the manager's cache, keeps the per-revision
// Services, reads the endpoints behind the traffic split live, writes the
// split, and drives the group machine of a declared group. Its cost is what
// the manager pays for a service at rest, once per pass, for as long as the
// service exists. The shapes here are the reference ones: engine units of
// 1,000 and 4,000 single-pod Instances and of 160 gangs of eight, and
// prefill/decode units of 12:8 and 30:10 behind four routers, the two sides
// under one rollingUpdate group that keeps their ratio.

var updateCoordinationPassAllocations = flag.Bool("update-coordination-pass", false, "re-record testdata/coordination_pass_allocations.json from this run")

const (
	// passFleetDecodedRows is the row bound the production decoder carries
	// (omenativeStatus.maxDecodedInstances).
	passFleetDecodedRows = 20000
	// passFleetHash is the one revision every Instance of a service at rest runs.
	passFleetHash = "6f1ed002"
	// passFleetNodes is how many nodes the pods spread over.
	passFleetNodes = 500
	// passFleetEndpointsPerSlice is the endpoint controller's slice size.
	passFleetEndpointsPerSlice = 100
	passFleetNamespace         = "serving"
	passFleetServiceName       = "chat-model"
)

// passFleetMember is one Component of the service: its Instance count and
// the worker pods each Instance runs beside its leader (0 is a single pod).
type passFleetMember struct {
	component v1beta1.ComponentType
	instances int
	workers   int
}

func (m passFleetMember) podsPerInstance() int32 {
	return int32(1 + m.workers)
}

func (m passFleetMember) runners() []v1beta1.Runner {
	if m.workers == 0 {
		return []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	}
	return []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: int32(m.workers)}}
}

// passFleetShape is one reference shape: the Components of the service
// and whether the prefill and decode sides are declared under one
// rollingUpdate group, as the reference prefill/decode services declare them.
type passFleetShape struct {
	name    string
	members []passFleetMember
	grouped bool
}

func passFleetShapes() []passFleetShape {
	engine, decoder, router := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	return []passFleetShape{
		{name: "engine/1000x1", members: []passFleetMember{{engine, 1000, 0}}},
		{name: "engine/4000x1", members: []passFleetMember{{engine, 4000, 0}}},
		{name: "engine/160x8", members: []passFleetMember{{engine, 160, 7}}},
		{name: "prefill-decode/12:8", members: []passFleetMember{{engine, 12, 0}, {decoder, 8, 0}, {router, 4, 0}}, grouped: true},
		{name: "prefill-decode/30:10", members: []passFleetMember{{engine, 30, 0}, {decoder, 10, 0}, {router, 4, 0}}, grouped: true},
	}
}

// passFleet is one service at rest, ready for passes: the inputs a pass
// takes and the counting stand-ins behind them.
type passFleet struct {
	ctx   context.Context
	in    ReconcileInputs
	cache *passCache
	live  *passLive
	shape passFleetShape
	// held is a copy of every pod as the cache holds it, taken before the
	// first pass: a pass reads the cache's pods and writes nothing through
	// them, so they read the same after any number of passes.
	held []*corev1.Pod
}

// buildPassFleet stands the service up at rest: each Component's replica
// with its rows stored in the form the writer selects for them, its pods,
// its ControllerRevision and the endpoints of its routed Service; then it
// runs passes until a pass writes nothing, and checks the service rests.
func buildPassFleet(tb testing.TB, shape passFleetShape) *passFleet {
	tb.Helper()
	isvc := passFleetService(shape)
	cache := newPassCache()
	live := &passLive{replicas: map[types.NamespacedName]*v1beta1.InferenceReplica{}}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	ports := map[v1beta1.ComponentType][]corev1.ContainerPort{}
	for _, member := range shape.members {
		replica := passFleetReplica(tb, isvc, member)
		live.replicas[client.ObjectKeyFromObject(replica)] = replica
		cache.put(replica)
		cache.put(passFleetRevision(isvc, replica))
		pods := passFleetPods(isvc, replica, member)
		cache.putPods(query.OMENativePodIndexValue(isvc.Name, v1beta1convert.ComponentTypeToWorkload(member.component)), pods)
		live.slices = append(live.slices, passFleetEndpointSlices(isvc, member.component, pods)...)
		modes[member.component] = constants.OMENative
		ports[member.component] = []corev1.ContainerPort{{Name: servingPortName, ContainerPort: 8000, Protocol: corev1.ProtocolTCP}}
	}
	f := &passFleet{
		// The context carries a logger, as a pass's does: the root logger's
		// own bookkeeping before the manager sets it is not part of the cost.
		ctx:   logr.NewContext(context.Background(), logr.Discard()),
		cache: cache,
		live:  live,
		shape: shape,
		held:  cache.podCopies(),
		in: ReconcileInputs{
			ISVC:                         isvc,
			Policies:                     rollout.Policies{},
			Client:                       cache,
			Reader:                       irstatus.NewReader(live, irstatus.NewDecoder(passFleetDecodedRows)),
			Recorder:                     &record.FakeRecorder{},
			ComponentDeploymentModes:     modes,
			ComponentRunnerPorts:         ports,
			Now:                          irstatustest.TimePlus(24 * time.Hour).Time,
			DefaultRatioTolerancePercent: ptr.To(int32(20)),
		},
	}
	// The first pass creates the per-revision Services; the service is at
	// rest once a pass writes nothing, and the pass after that is read.
	for pass := 0; ; pass++ {
		f.resetCounts()
		f.pass(tb)
		if cache.writes == 0 {
			break
		}
		if pass == 4 {
			tb.Fatalf("%s: five passes and the service still writes", shape.name)
		}
	}
	f.checkAtRest(tb)
	f.resetCounts()
	return f
}

// passFleetService is the service: one OMENative Component per member at
// its Instance count, and for a prefill/decode shape an example group,
// a rollingUpdate over the two sides at maxSurge 10 percent that keeps
// their ratio within 20 percent; the routers roll on their own.
func passFleetService(shape passFleetShape) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: passFleetNamespace, Name: passFleetServiceName, UID: types.UID(passFleetServiceName + "-uid"), Generation: 1,
	}}
	for _, member := range shape.members {
		ext := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(member.instances)}
		switch member.component {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
		case v1beta1.RouterComponent:
			isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: ext}
		}
	}
	if shape.grouped {
		tenPercent := intstr.FromString("10%")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &tenPercent},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(20))},
		}}}
	}
	return isvc
}

// passFleetReplica is a member's replica as the replica controller
// publishes it at rest: every Instance Ready on the one revision with its
// readySince, the Ready condition True, the rows stored in the form the
// writer selects for them (the column form at fleet size).
func passFleetReplica(tb testing.TB, isvc *v1beta1.InferenceService, member passFleetMember) *v1beta1.InferenceReplica {
	tb.Helper()
	name := irprojector.InferenceReplicaName(isvc.Name, member.component)
	count, pods := int32(member.instances), member.podsPerInstance()
	revision := ControllerRevisionName(isvc.Name, member.component, passFleetHash)
	replica := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Namespace: isvc.Namespace, Name: name, UID: types.UID(name + "-uid"), Generation: 1, ResourceVersion: "1",
	}}
	replica.Spec.Component = member.component
	replica.Spec.Replicas = ptr.To(count)
	replica.Spec.Runners = member.runners()
	rows := make([]v1beta1.OMENativeInstanceStatus, member.instances)
	for i := range rows {
		readySince := irstatustest.TimePlus(time.Duration(i) * irstatustest.ReadySinceSpacing)
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   revision,
			PodCount:          pods,
			ServingPodCount:   pods,
			AvailablePodCount: pods,
			Admitted:          true,
			ReadySince:        &readySince,
		}
	}
	logical := v1beta1.InferenceReplicaStatus{
		ObservedGeneration:   1,
		CurrentRevision:      revision,
		UpdateRevision:       revision,
		Replicas:             count,
		ReadyReplicas:        count,
		ServingReplicas:      count,
		AvailableReplicas:    count,
		UpdatedReplicas:      count,
		UpdatedReadyReplicas: count,
		Conditions: []metav1.Condition{{
			Type: conditionReady, Status: metav1.ConditionTrue, Reason: "InstancesReady",
			LastTransitionTime: irstatustest.TimePlus(time.Duration(member.instances) * irstatustest.ReadySinceSpacing),
		}},
		InstanceStatuses: rows,
	}
	selection, err := irstatus.SelectCandidate(&logical, passFleetDecodedRows)
	if err != nil {
		tb.Fatal(err)
	}
	replica.Status = *selection.SelectedCandidate().Status
	return replica
}

// passFleetRevision is the member's ControllerRevision for the revision,
// named and labelled as the replica controller writes it and controlled by
// the replica.
func passFleetRevision(isvc *v1beta1.InferenceService, replica *v1beta1.InferenceReplica) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: isvc.Namespace,
			Name:      ControllerRevisionName(isvc.Name, replica.Spec.Component, passFleetHash),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(replica.Spec.Component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               passFleetHash,
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(replica, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))},
		},
		Revision: 1,
	}
}

// passFleetPods is the pods of one member as the manager's cache holds
// them: Running, Ready and serving, each on a node, with the labels the
// engine renders and the fields a served pod carries.
func passFleetPods(isvc *v1beta1.InferenceService, replica *v1beta1.InferenceReplica, member passFleetMember) []*corev1.Pod {
	component := v1beta1convert.ComponentTypeToWorkload(member.component)
	created := irstatustest.TimePlus(-time.Hour)
	owner := *metav1.NewControllerRef(replica, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))
	pods := make([]*corev1.Pod, 0, member.instances*int(member.podsPerInstance()))
	for index := 0; index < member.instances; index++ {
		for _, runner := range member.runners() {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				serial := len(pods)
				name := query.PodName(isvc.Name, component, int32(index), string(runner.Name), ordinal)
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: name, Namespace: isvc.Namespace, UID: types.UID(name + "-uid"), ResourceVersion: "1", CreationTimestamp: created,
						Labels: map[string]string{
							constants.InferenceServicePodLabelKey: isvc.Name,
							constants.OMEComponentLabel:           string(member.component),
							query.LabelInstanceIdx:                strconv.Itoa(index),
							query.LabelInstanceIncarnation:        "1",
							query.LabelRunner:                     string(runner.Name),
							query.LabelManagedBy:                  query.ManagedByOMENative,
							query.LabelPodOrdinal:                 strconv.Itoa(int(ordinal)),
							query.LabelRevisionHash:               passFleetHash,
						},
						OwnerReferences: []metav1.OwnerReference{owner},
					},
					Spec: corev1.PodSpec{
						NodeName: fmt.Sprintf("node-%d", serial%passFleetNodes),
						Containers: []corev1.Container{{
							Name:  constants.MainContainerName,
							Image: "registry.example.com/serving/engine:1.2.3",
							Ports: []corev1.ContainerPort{{Name: servingPortName, ContainerPort: 8000, Protocol: corev1.ProtocolTCP}},
						}},
					},
					Status: corev1.PodStatus{
						Phase:     corev1.PodRunning,
						PodIP:     fmt.Sprintf("10.%d.%d.%d", (serial/65536)%256, (serial/256)%256, serial%256),
						StartTime: &created,
						Conditions: []corev1.PodCondition{
							{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
							{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: created},
							{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
						},
						ContainerStatuses: []corev1.ContainerStatus{{
							Name:  constants.MainContainerName,
							Ready: true,
							State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: created}},
						}},
					},
				}
				if member.workers > 0 {
					pod.Labels[query.LabelPodGroup] = query.PodGroupName(isvc.Name, component, int32(index))
				}
				pods = append(pods, pod)
			}
		}
	}
	return pods
}

// passFleetEndpointSlices is the EndpointSlices the endpoints controller
// keeps for the routed Service of the member's one revision: every routed
// pod (a gang's leader, a single pod itself) a Ready endpoint, at the
// controller's slice size.
func passFleetEndpointSlices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, pods []*corev1.Pod) []*discoveryv1.EndpointSlice {
	service := PerRevisionServiceName(prefixFor(isvc, component), component, passFleetHash)
	routed := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if query.RoutedRunner(pod) {
			routed = append(routed, pod)
		}
	}
	var out []*discoveryv1.EndpointSlice
	for start := 0; start < len(routed); start += passFleetEndpointsPerSlice {
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-%d", service, start/passFleetEndpointsPerSlice), Namespace: isvc.Namespace,
				Labels: map[string]string{discoveryv1.LabelServiceName: service},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr.To(servingPortName), Port: ptr.To(int32(8000)), Protocol: ptr.To(corev1.ProtocolTCP)}},
		}
		for _, pod := range routed[start:min(start+passFleetEndpointsPerSlice, len(routed))] {
			slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
				Addresses:  []string{pod.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(true), Terminating: ptr.To(false)},
				NodeName:   ptr.To(pod.Spec.NodeName),
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			})
		}
		out = append(out, slice)
	}
	return out
}

// pass runs one coordination pass of the service, as the controller runs
// one per pass of the service.
func (f *passFleet) pass(tb testing.TB) {
	if _, err := Reconcile(f.ctx, f.in); err != nil {
		tb.Fatal(err)
	}
}

func (f *passFleet) resetCounts() {
	f.cache.reads, f.cache.writes, f.cache.podsListed, f.cache.podsCopied = 0, 0, 0, 0
	f.live.reads = 0
}

// checkAtRest holds the service where a service at rest stands: every
// Component sends all its traffic to its one revision and records it rolled
// out, no revision waits for its endpoints, a declared group is Idle, the
// pass wrote nothing, and the cache's pods read as they did before the
// first pass.
func (f *passFleet) checkAtRest(tb testing.TB) {
	tb.Helper()
	isvc := f.in.ISVC
	if f.cache.writes != 0 {
		tb.Fatalf("%s: a pass at rest wrote %d objects", f.shape.name, f.cache.writes)
	}
	for _, member := range f.shape.members {
		want := PerRevisionServiceName(prefixFor(isvc, member.component), member.component, passFleetHash)
		status := isvc.Status.Components[member.component]
		if len(status.Traffic) != 1 || status.Traffic[0].RevisionName != want || status.Traffic[0].Percent != 100 {
			tb.Fatalf("%s: %s should send all its traffic to %s, recorded %+v", f.shape.name, member.component, want, status.Traffic)
		}
		if status.LatestRolledoutRevision != want {
			tb.Fatalf("%s: %s should rest on %s, recorded %q", f.shape.name, member.component, want, status.LatestRolledoutRevision)
		}
	}
	if cond := isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady)); cond != nil && cond.Status != corev1.ConditionTrue {
		tb.Fatalf("%s: the split waits for endpoints: %s", f.shape.name, cond.Message)
	}
	if f.shape.grouped {
		groups := isvc.Status.RolloutCoordination
		if groups == nil || len(groups.Groups) != 1 || groups.Groups[0].Phase != v1beta1.CoordinationPhaseIdle {
			tb.Fatalf("%s: the group should rest Idle, recorded %+v", f.shape.name, groups)
		}
	}
	f.cache.checkPodsUnchanged(tb, f.held)
}

// passFleetCases is every shape at rest.
func passFleetCases(tb testing.TB) []*passFleet {
	var fleets []*passFleet
	for _, shape := range passFleetShapes() {
		fleets = append(fleets, buildPassFleet(tb, shape))
	}
	return fleets
}

// TestCoordinationPassAtRestStaysWithinBudget holds one coordination pass
// of a service at rest to the recorded golden, per shape: its allocations
// and bytes, the pods the cache handed it and how many of them it copied,
// its reads of the cache and of the live reader; a pass at rest writes
// nothing and leaves the cache's pods as they were.
func TestCoordinationPassAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "coordination_pass_allocations.json"), *updateCoordinationPassAllocations)
	defer golden.Close()
	for _, f := range passFleetCases(t) {
		golden.Check(f.shape.name+"/allocations", func() { f.pass(t) })
		golden.CheckBytes(f.shape.name+"/bytes", func() { f.pass(t) })
		f.resetCounts()
		f.pass(t)
		golden.CheckCount(f.shape.name+"/pods-listed", f.cache.podsListed)
		golden.CheckCount(f.shape.name+"/pods-copied", f.cache.podsCopied)
		golden.CheckCount(f.shape.name+"/cache-reads", f.cache.reads)
		golden.CheckCount(f.shape.name+"/live-reads", f.live.reads)
		f.checkAtRest(t)
	}
}

// BenchmarkCoordinationPassAtRest is the time of one coordination pass per
// shape, with the pods the cache copied for it and its reads beside it.
func BenchmarkCoordinationPassAtRest(b *testing.B) {
	for _, f := range passFleetCases(b) {
		b.Run(f.shape.name, func(b *testing.B) {
			f.resetCounts()
			b.ReportAllocs()
			for b.Loop() {
				f.pass(b)
			}
			passes := float64(b.N)
			b.ReportMetric(float64(f.cache.podsCopied)/passes, "pods-copied/op")
			b.ReportMetric(float64(f.cache.reads)/passes, "cache-reads/op")
			b.ReportMetric(float64(f.live.reads)/passes, "live-reads/op")
		})
	}
}

// passCache stands in for the manager's cached client: it serves each
// Component's pods through the OMENative pod index, the replicas, the
// ControllerRevisions and the per-revision Services, keeps what a pass
// creates or updates, and counts what it served. A read is a deep copy of
// the held object, as the informer cache answers it, unless the read asks
// for the cache's own objects (client.UnsafeDisableDeepCopy). The pass that
// stands the service up creates the Services through it; a pass at rest
// writes nothing. Every object lives in the one namespace of the fixture.
type passCache struct {
	client.Client
	objects map[passStoreKey]client.Object
	// pods is every Component's pods under the Component's index value.
	pods          map[string][]*corev1.Pod
	reads, writes int64
	// podsListed counts the pods the pod lists handed out; podsCopied the
	// ones handed out as deep copies.
	podsListed, podsCopied int64
}

// passStoreKey names one held object by its type, namespace and name.
type passStoreKey struct {
	kind, namespace, name string
}

func passStoreKeyOf(obj client.Object, namespace, name string) passStoreKey {
	return passStoreKey{kind: fmt.Sprintf("%T", obj), namespace: namespace, name: name}
}

func newPassCache() *passCache {
	return &passCache{objects: map[passStoreKey]client.Object{}, pods: map[string][]*corev1.Pod{}}
}

func (c *passCache) put(obj client.Object) {
	c.objects[passStoreKeyOf(obj, obj.GetNamespace(), obj.GetName())] = obj.DeepCopyObject().(client.Object)
}

func (c *passCache) putPods(indexValue string, pods []*corev1.Pod) {
	c.pods[indexValue] = append(c.pods[indexValue], pods...)
}

// podCopies is a deep copy of every held pod, Components in index order.
func (c *passCache) podCopies() []*corev1.Pod {
	var copies []*corev1.Pod
	for _, value := range slices.Sorted(maps.Keys(c.pods)) {
		for _, pod := range c.pods[value] {
			copies = append(copies, pod.DeepCopy())
		}
	}
	return copies
}

// checkPodsUnchanged holds every held pod equal to its copy in held.
func (c *passCache) checkPodsUnchanged(tb testing.TB, held []*corev1.Pod) {
	tb.Helper()
	i := 0
	for _, value := range slices.Sorted(maps.Keys(c.pods)) {
		for _, pod := range c.pods[value] {
			if i >= len(held) || !apiequality.Semantic.DeepEqual(pod, held[i]) {
				tb.Fatalf("the cache's pod %s changed under a pass", pod.Name)
			}
			i++
		}
	}
}

// passUncopied reads the deep-copy switch of a read's options.
func passUncopied(disableDeepCopy *bool) bool {
	return disableDeepCopy != nil && *disableDeepCopy
}

func (c *passCache) Get(_ context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.reads++
	stored, ok := c.objects[passStoreKeyOf(obj, key.Namespace, key.Name)]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, key.Name)
	}
	options := client.GetOptions{}
	options.ApplyOptions(opts)
	if passUncopied(options.UnsafeDisableDeepCopy) {
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored).Elem())
		return nil
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored.DeepCopyObject()).Elem())
	return nil
}

// List answers a Component's pod list through the index, and the Service
// lists of the namespace under a label selector, in name order.
func (c *passCache) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.reads++
	var options client.ListOptions
	options.ApplyOptions(opts)
	switch l := list.(type) {
	case *corev1.PodList:
		return c.listPods(l, options)
	case *corev1.ServiceList:
		l.Items = passStoredItems[corev1.Service](c, options)
		return nil
	default:
		return fmt.Errorf("the cache stand-in lists pods and Services only, not %T", list)
	}
}

// listPods answers the pod list of one Component through the OMENative pod
// index, as the manager's cache answers it: a copy of every pod, or the
// held pods themselves when the read asks for them.
func (c *passCache) listPods(l *corev1.PodList, options client.ListOptions) error {
	if options.FieldSelector == nil {
		return fmt.Errorf("the cache stand-in serves pod lists through the %s index only", query.OMENativePodIndexField)
	}
	value, ok := options.FieldSelector.RequiresExactMatch(query.OMENativePodIndexField)
	if !ok {
		return fmt.Errorf("the cache stand-in has no index for the field selector %q", options.FieldSelector)
	}
	held := c.pods[value]
	c.podsListed += int64(len(held))
	l.Items = make([]corev1.Pod, len(held))
	uncopied := passUncopied(options.UnsafeDisableDeepCopy)
	for i, pod := range held {
		if uncopied {
			l.Items[i] = *pod
			continue
		}
		l.Items[i] = *pod.DeepCopy()
		c.podsCopied++
	}
	return nil
}

// passStoredItems is the held objects of one type in the namespace the
// options name, matching their label selector, copied and in name order.
func passStoredItems[T any, PT interface {
	*T
	client.Object
}](c *passCache, options client.ListOptions) []T {
	var items []T
	for key, stored := range c.objects {
		obj, ok := stored.(PT)
		if !ok || key.namespace != options.Namespace {
			continue
		}
		if options.LabelSelector != nil && !options.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}
		items = append(items, *obj.DeepCopyObject().(PT))
	}
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(PT(&a).GetName(), PT(&b).GetName()) })
	return items
}

func (c *passCache) Create(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
	c.writes++
	if _, exists := c.objects[passStoreKeyOf(obj, obj.GetNamespace(), obj.GetName())]; exists {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, obj.GetName())
	}
	obj.SetResourceVersion("1")
	c.put(obj)
	return nil
}

func (c *passCache) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	c.writes++
	if _, exists := c.objects[passStoreKeyOf(obj, obj.GetNamespace(), obj.GetName())]; !exists {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, obj.GetName())
	}
	c.put(obj)
	return nil
}

func (c *passCache) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	c.writes++
	return fmt.Errorf("the cache stand-in takes no patch of %T %s", obj, obj.GetName())
}

func (c *passCache) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	c.writes++
	delete(c.objects, passStoreKeyOf(obj, obj.GetNamespace(), obj.GetName()))
	return nil
}

// passLive stands in for the live API reader and counts what a pass asks
// of it. It hands out a Component's replica by copy, because the decoder
// rewrites the object it reads, and the EndpointSlices of a routed Service
// themselves, in a fresh list; the decode of the apiserver's bytes is the
// client's own cost and is not in the count. It serves nothing else: a pass
// that reads another kind live is a finding, not a fixture gap.
type passLive struct {
	replicas map[types.NamespacedName]*v1beta1.InferenceReplica
	slices   []*discoveryv1.EndpointSlice
	reads    int64
}

func (r *passLive) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	replica, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return fmt.Errorf("the live stand-in serves InferenceReplica reads only, not %T", obj)
	}
	r.reads++
	stored, ok := r.replicas[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}, key.Name)
	}
	stored.DeepCopyInto(replica)
	return nil
}

func (r *passLive) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	out, ok := list.(*discoveryv1.EndpointSliceList)
	if !ok {
		return fmt.Errorf("the live stand-in serves EndpointSlice lists only, not %T", list)
	}
	var options client.ListOptions
	options.ApplyOptions(opts)
	r.reads++
	out.Items = out.Items[:0]
	for _, slice := range r.slices {
		if options.Namespace != "" && slice.Namespace != options.Namespace {
			continue
		}
		if options.LabelSelector != nil && !options.LabelSelector.Matches(labels.Set(slice.Labels)) {
			continue
		}
		out.Items = append(out.Items, *slice)
	}
	return nil
}
