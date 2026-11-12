package canary

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateDispatchAllocations = flag.Bool("update-dispatch", false, "re-record testdata/dispatch_allocations.json from this run")

// The controller runs one canary dispatch per canary group on every pass of
// the service, whether a roll is open or not. The dispatch reads every
// member's pods and its replica, and the service's own record, through the
// live API reader, so its cost is what the apiserver and the manager pay for
// a service with a canary group at rest, once per pass, for as long as the
// service exists. The shapes
// here are the reference ones: engine units of 1,000 and 4,000 single-pod
// Instances and of 160 gangs of eight, and prefill/decode units of 12:8 and
// 30:10 behind four routers.

// dispatchDecodedRows is the row bound the production decoder carries
// (omenativeStatus.maxDecodedInstances).
const dispatchDecodedRows = 20000

// The revision hashes of a unit: the one it rests on when settled, and the
// stable and target revisions of a roll that holds.
const (
	dispatchSettledHash = "6f1ed002"
	dispatchStableHash  = "2f32f6fe"
	dispatchTargetHash  = "9b1c0d2e"
)

// dispatchFleetState is where the unit rests: settled, with no run open and
// every Instance on one revision; or holding, with a run open and the split
// serving under the manual hold of its first step.
type dispatchFleetState string

const (
	dispatchSettled dispatchFleetState = "settled"
	dispatchHolding dispatchFleetState = "holding"
)

// dispatchFleetMember is one member of the unit: its Instance count and the
// worker pods each Instance runs beside its leader (0 is a single pod).
type dispatchFleetMember struct {
	component v1beta1.ComponentType
	instances int
	workers   int
}

// dispatchFleetShape is one reference shape of a canary unit: the group's
// members and the routers that serve beside the group.
type dispatchFleetShape struct {
	name    string
	members []dispatchFleetMember
	routers int
}

func dispatchFleetShapes() []dispatchFleetShape {
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	return []dispatchFleetShape{
		{name: "engine/1000x1", members: []dispatchFleetMember{{engine, 1000, 0}}},
		{name: "engine/4000x1", members: []dispatchFleetMember{{engine, 4000, 0}}},
		{name: "engine/160x8", members: []dispatchFleetMember{{engine, 160, 7}}},
		{name: "prefill-decode/12:8", members: []dispatchFleetMember{{engine, 12, 0}, {decoder, 8, 0}}, routers: 4},
		{name: "prefill-decode/30:10", members: []dispatchFleetMember{{engine, 30, 0}, {decoder, 10, 0}}, routers: 4},
	}
}

func (m dispatchFleetMember) podsPerInstance() int32 {
	return int32(1 + m.workers)
}

func (m dispatchFleetMember) runners() []v1beta1.Runner {
	if m.workers == 0 {
		return []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	}
	return []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: int32(m.workers)}}
}

// hashAt is the revision an Instance runs: settled, every Instance runs
// the one revision; holding, the upper half of the index space runs the
// target, as a partition at half the fleet leaves it.
func (m dispatchFleetMember) hashAt(state dispatchFleetState, index int) string {
	switch {
	case state == dispatchSettled:
		return dispatchSettledHash
	case index >= m.instances/2:
		return dispatchTargetHash
	}
	return dispatchStableHash
}

// dispatchFleet is one unit at rest, ready for passes: the deps a pass
// takes, the counting stand-ins behind them, and a copy of every pod as it
// was built, which a pass must leave as it found it.
type dispatchFleet struct {
	ctx        context.Context
	deps       DispatchDeps
	live       *liveStandIn
	cache      *cacheStandIn
	shape      dispatchFleetShape
	state      dispatchFleetState
	podsBefore [][]*corev1.Pod
}

// buildDispatchFleet stands the unit up at rest: the service, each
// member's replica with its rows stored in the column form a production
// replica at rest stores, its pods and its revisions; then it runs passes
// until a pass writes nothing, and checks the unit rests where the state
// says.
func buildDispatchFleet(tb testing.TB, shape dispatchFleetShape, state dispatchFleetState) *dispatchFleet {
	tb.Helper()
	const namespace = "serving"
	now := irstatustest.TimePlus(24 * time.Hour).Time
	isvc := dispatchFleetService(namespace, shape, state)
	live := &liveStandIn{replicas: map[types.NamespacedName]*v1beta1.InferenceReplica{}}
	cache := &cacheStandIn{objects: map[storeKey]client.Object{}}
	for _, member := range shape.members {
		replica := dispatchFleetReplica(tb, isvc, member, state)
		live.replicas[client.ObjectKeyFromObject(replica)] = replica
		live.podSets = append(live.podSets, dispatchFleetPods(isvc, member, state))
		cache.put(replica)
		for _, hash := range dispatchFleetHashes(state) {
			cache.put(dispatchFleetRevision(isvc, member.component, hash))
		}
	}
	// The informer holds the pods the apiserver holds: both stand-ins
	// serve the same objects.
	cache.podSets = live.podSets
	f := &dispatchFleet{
		// The context carries a logger, as a pass's does: the root logger's
		// own bookkeeping before the manager sets it is not part of the cost.
		ctx:   logr.NewContext(context.Background(), logr.Discard()),
		live:  live,
		cache: cache,
		shape: shape,
		state: state,
		deps: DispatchDeps{
			Client:               cache,
			Reader:               irstatus.NewReader(live, irstatus.NewDecoder(dispatchDecodedRows)),
			ISVC:                 isvc,
			Policies:             rollout.Policies{},
			Now:                  now,
			ComponentRunnerPorts: canaryRunnerPorts(),
			Group:                rollout.CanaryGroup(isvc, rollout.Policies{}),
			Requeue:              reconcileRequeue,
			ParkedRequeue:        failedRequeue,
			DefaultReadyTimeout:  cellReadyTimeout,
		},
	}
	for _, set := range live.podSets {
		before := make([]*corev1.Pod, len(set.pods))
		for i, pod := range set.pods {
			before[i] = pod.DeepCopy()
		}
		f.podsBefore = append(f.podsBefore, before)
	}
	// The first pass creates the per-revision Services; the unit is at
	// rest once a pass writes nothing, and the pass after that is read.
	for pass := 0; ; pass++ {
		f.resetCounts()
		f.pass(tb)
		if cache.writes == 0 {
			break
		}
		if pass == 4 {
			tb.Fatalf("%s/%s: five passes and the unit still writes", shape.name, state)
		}
	}
	// The controller hands the dispatch the record it read, and the
	// dispatch reads the live service once per pass to tell a stale copy
	// from a current one; at rest the live record is the one the pass holds.
	f.deps.Base = NewBase(isvc)
	live.service = isvc.DeepCopy()
	f.checkAtRest(tb)
	f.resetCounts()
	return f
}

// dispatchFleetHashes is the revisions a unit in state carries.
func dispatchFleetHashes(state dispatchFleetState) []string {
	if state == dispatchSettled {
		return []string{dispatchSettledHash}
	}
	return []string{dispatchStableHash, dispatchTargetHash}
}

// dispatchFleetService is the service of the unit: one canary group over
// the members with the manual two-step plan, and a run open toward the
// target revision when the unit holds.
func dispatchFleetService(namespace string, shape dispatchFleetShape, state dispatchFleetState) *v1beta1.InferenceService {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name, isvc.UID = namespace, "chat-model", "chat-model-uid"
	var components []v1beta1.ComponentType
	for _, member := range shape.members {
		components = append(components, member.component)
		ext := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(member.instances)}
		switch member.component {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
		}
	}
	if shape.routers > 0 {
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(shape.routers)}}
	}
	isvc.Spec.Rollout.Groups[0].Components = components
	if state == dispatchHolding {
		var targets []v1beta1.RolloutRunTarget
		for _, member := range shape.members {
			targets = append(targets, v1beta1.RolloutRunTarget{Component: member.component, Revision: dispatchTargetHash, StableRevision: dispatchStableHash})
		}
		isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID:           "run-1",
			OpenedAt:        irstatustest.TimePlus(23 * time.Hour),
			Plan:            v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: *isvc.Spec.Rollout.Groups[0].DeepCopy()}}},
			TargetRevisions: targets,
		}}
	}
	return isvc
}

// dispatchRevisionName is the full revision name a member's rows, status
// and ControllerRevisions carry for a hash.
func dispatchRevisionName(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash string) string {
	return coordination.ControllerRevisionName(isvc.Name, component, hash)
}

// dispatchFleetRevision is a member's ControllerRevision for a hash, named
// and labelled as the replica controller writes it and controlled by the
// member's replica.
func dispatchFleetRevision(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash string) *appsv1.ControllerRevision {
	replicaName := isvc.Name + "-" + string(component)
	replica := &metav1.ObjectMeta{Name: replicaName, UID: canaryIRUID(replicaName)}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: isvc.Namespace,
			Name:      dispatchRevisionName(isvc, component, hash),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(replica, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")),
			},
		},
		Revision: 1,
	}
}

// dispatchFleetReplica is a member's replica as the replica controller
// publishes it at rest: every Instance Ready with its readySince, the rows
// stored in the form the writer selects for them (the column form at
// fleet size), the status fresh at the replica's generation.
func dispatchFleetReplica(tb testing.TB, isvc *v1beta1.InferenceService, member dispatchFleetMember, state dispatchFleetState) *v1beta1.InferenceReplica {
	tb.Helper()
	name := isvc.Name + "-" + string(member.component)
	count := int32(member.instances)
	pods := member.podsPerInstance()
	replica := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Namespace: isvc.Namespace, Name: name, UID: canaryIRUID(name), Generation: 1, ResourceVersion: "1",
	}}
	replica.Spec.Component = member.component
	replica.Spec.Replicas = ptr.To(count)
	replica.Spec.Runners = member.runners()
	rows := make([]v1beta1.OMENativeInstanceStatus, member.instances)
	for i := range rows {
		readySince := irstatustest.TimePlus(time.Duration(i) * irstatustest.ReadySinceSpacing)
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   dispatchRevisionName(isvc, member.component, member.hashAt(state, i)),
			PodCount:          pods,
			ServingPodCount:   pods,
			AvailablePodCount: pods,
			Admitted:          true,
			ReadySince:        &readySince,
		}
	}
	hashes := dispatchFleetHashes(state)
	logical := v1beta1.InferenceReplicaStatus{
		ObservedGeneration: 1,
		CurrentRevision:    dispatchRevisionName(isvc, member.component, hashes[0]),
		UpdateRevision:     dispatchRevisionName(isvc, member.component, hashes[len(hashes)-1]),
		Replicas:           count,
		ReadyReplicas:      count,
		ServingReplicas:    count,
		AvailableReplicas:  count,
		LabelSelector:      constants.InferenceServicePodLabelKey + "=" + isvc.Name + "," + constants.OMEComponentLabel + "=" + string(member.component),
		InstanceStatuses:   rows,
	}
	selection, err := irstatus.SelectCandidate(&logical, dispatchDecodedRows)
	if err != nil {
		tb.Fatal(err)
	}
	replica.Status = *selection.SelectedCandidate().Status
	return replica
}

// dispatchFleetPods is the pods of one member as the apiserver serves them:
// Running and Ready, with the labels the readers select on, the serving
// container's status and the fields a served pod carries.
func dispatchFleetPods(isvc *v1beta1.InferenceService, member dispatchFleetMember, state dispatchFleetState) podSet {
	set := podSet{
		namespace: isvc.Namespace,
		labels: labels.Set{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(member.component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
	created := irstatustest.TimePlus(-time.Hour)
	for i := 0; i < member.instances; i++ {
		hash := member.hashAt(state, i)
		for _, runner := range member.runners() {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				name := fmt.Sprintf("%s-%s-%d-%s-%d", isvc.Name, member.component, i, runner.Name, ordinal)
				pod := canaryRunnerPod(isvc.Namespace, isvc.Name, string(member.component), hash, name, runner.Name, strconv.Itoa(int(ordinal)))
				pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(i)
				pod.UID = types.UID(name + "-uid")
				pod.ResourceVersion = "1"
				pod.CreationTimestamp = created
				pod.Spec.NodeName = fmt.Sprintf("node-%d", (i*int(member.podsPerInstance())+int(ordinal))%500)
				pod.Spec.Containers = []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "registry.example.com/serving/engine:1.2.3",
					Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP}},
				}}
				pod.Status.PodIP = fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256)
				pod.Status.StartTime = &created
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name:  constants.MainContainerName,
					Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: created}},
				}}
				set.pods = append(set.pods, pod)
			}
		}
	}
	if len(set.pods) > 0 {
		set.index = query.OMENativePodIndexExtractor(set.pods[0])[0]
	}
	return set
}

// pass runs one dispatch of the unit, as the controller runs one per pass.
func (f *dispatchFleet) pass(tb testing.TB) {
	if _, err := Dispatch(f.ctx, f.deps); err != nil {
		tb.Fatal(err)
	}
}

func (f *dispatchFleet) resetCounts() {
	f.live.gets, f.live.serviceGets, f.live.lists, f.live.podsListed = 0, 0, 0, 0
	f.cache.reads, f.cache.writes = 0, 0
	f.cache.podLists, f.cache.podsListed, f.cache.podsCopied = 0, 0, 0
}

// checkAtRest holds the unit where the state says: holding, the primary's
// phase is Paused under the manual hold of step 0 with the record open;
// settled, no record and every member recorded on the one revision. Every
// pod the stand-ins hold is as it was built: a pass reads the pods and
// writes nothing through them.
func (f *dispatchFleet) checkAtRest(tb testing.TB) {
	tb.Helper()
	isvc := f.deps.ISVC
	primary := primaryComponentOf(f.deps.Group)
	record := rollout.CanaryStatusFor(&isvc.Status, primary)
	phase := isvc.Status.Components[primary].RolloutPhase
	switch f.state {
	case dispatchHolding:
		if record == nil || record.CurrentStep != 0 || phase != v1beta1.RolloutPhasePaused {
			tb.Fatalf("%s/%s: the unit should hold at step 0 Paused, record %+v phase %q", f.shape.name, f.state, record, phase)
		}
	case dispatchSettled:
		if record != nil {
			tb.Fatalf("%s/%s: a settled unit carries no record, got %+v", f.shape.name, f.state, record)
		}
		for _, member := range f.shape.members {
			want := coordination.PerRevisionServiceName(isvc.Name, member.component, dispatchSettledHash)
			if got := isvc.Status.Components[member.component].LatestRolledoutRevision; got != want {
				tb.Fatalf("%s/%s: %s should rest on %s, recorded %q", f.shape.name, f.state, member.component, want, got)
			}
		}
	}
	if f.cache.writes != 0 {
		tb.Fatalf("%s/%s: a pass at rest wrote %d objects", f.shape.name, f.state, f.cache.writes)
	}
	if !f.deps.Base.Current(&f.live.service.Status) {
		tb.Fatalf("%s/%s: the record the pass holds should be the live one", f.shape.name, f.state)
	}
	for i, set := range f.live.podSets {
		for j, pod := range set.pods {
			if !reflect.DeepEqual(pod, f.podsBefore[i][j]) {
				tb.Fatalf("%s/%s: a pass changed pod %s", f.shape.name, f.state, pod.Name)
			}
		}
	}
}

// dispatchFleetCases is every shape in both states.
func dispatchFleetCases(tb testing.TB) []*dispatchFleet {
	var fleets []*dispatchFleet
	for _, shape := range dispatchFleetShapes() {
		for _, state := range []dispatchFleetState{dispatchSettled, dispatchHolding} {
			fleets = append(fleets, buildDispatchFleet(tb, shape, state))
		}
	}
	return fleets
}

// TestDispatchAtRestStaysWithinBudget holds one dispatch of a unit at rest
// to the recorded golden: its allocations and bytes, the live reads it
// makes (replica reads, the service's record, pod lists and the pods they
// hand out), its reads of the cache (the Services and ControllerRevisions,
// and the pod lists with the pods they hand out and copy), and the
// allocations and bytes of the pass with its live reads decoded from JSON,
// per shape and state; a pass at rest writes nothing.
func TestDispatchAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "dispatch_allocations.json"), *updateDispatchAllocations)
	defer golden.Close()
	for _, f := range dispatchFleetCases(t) {
		name := f.shape.name + "/" + string(f.state)
		golden.Check(name+"/allocations", func() { f.pass(t) })
		golden.CheckBytes(name+"/bytes", func() { f.pass(t) })
		f.resetCounts()
		f.pass(t)
		golden.CheckCount(name+"/live-gets", f.live.gets)
		golden.CheckCount(name+"/live-service-gets", f.live.serviceGets)
		golden.CheckCount(name+"/live-lists", f.live.lists)
		golden.CheckCount(name+"/pods-listed", f.live.podsListed)
		golden.CheckCount(name+"/cache-reads", f.cache.reads)
		golden.CheckCount(name+"/cache-pod-lists", f.cache.podLists)
		golden.CheckCount(name+"/cache-pods-listed", f.cache.podsListed)
		golden.CheckCount(name+"/cache-pods-copied", f.cache.podsCopied)
		// The pass with every live read decoded from the JSON the apiserver
		// serves is what the manager pays for its live reads; a pod list is
		// about fifty allocations and 4 to 6 KB per pod of it.
		f.live.encode(t)
		f.live.json = true
		allocations, bytes := allocbudget.Smallest(func() { f.pass(t) })
		f.live.json = false
		golden.CheckCount(name+"/live-json/allocations", allocations)
		golden.CheckCount(name+"/live-json/bytes", bytes)
		f.checkAtRest(t)
	}
}

// BenchmarkDispatchAtRest is the time of one dispatch per shape and state:
// under live-stored the live reads hand out the stored objects, so the time
// is the pass's own; under live-json they decode the JSON the apiserver
// would serve, which is what the manager pays for them.
func BenchmarkDispatchAtRest(b *testing.B) {
	for _, f := range dispatchFleetCases(b) {
		for _, form := range []string{"live-stored", "live-json"} {
			b.Run(f.shape.name+"/"+string(f.state)+"/"+form, func(b *testing.B) {
				f.live.json = form == "live-json"
				if f.live.json {
					f.live.encode(b)
				}
				f.resetCounts()
				b.ReportAllocs()
				for b.Loop() {
					f.pass(b)
				}
				passes := float64(b.N)
				b.ReportMetric(float64(f.live.gets)/passes, "live-gets/op")
				b.ReportMetric(float64(f.live.serviceGets)/passes, "live-service-gets/op")
				b.ReportMetric(float64(f.live.lists)/passes, "live-lists/op")
				b.ReportMetric(float64(f.live.podsListed)/passes, "pods-listed/op")
				b.ReportMetric(float64(f.cache.reads)/passes, "cache-reads/op")
				b.ReportMetric(float64(f.cache.podLists)/passes, "cache-pod-lists/op")
				b.ReportMetric(float64(f.cache.podsListed)/passes, "cache-pods-listed/op")
				b.ReportMetric(float64(f.cache.podsCopied)/passes, "cache-pods-copied/op")
				f.live.json = false
			})
		}
	}
}

// podSet is the pods of one member, with the labels the pod list selects
// them by and the value the pod index keys them under.
type podSet struct {
	namespace string
	labels    labels.Set
	index     string
	pods      []*corev1.Pod
	encoded   []byte
}

// selectedBy reports whether a list's options name this set: by the pod
// index's value for the member when the list names the index, else by the
// labels its pods carry.
func (s *podSet) selectedBy(options client.ListOptions) bool {
	if options.FieldSelector != nil {
		return options.FieldSelector.Matches(fields.Set{query.OMENativePodIndexField: s.index})
	}
	return options.LabelSelector != nil && options.LabelSelector.Matches(s.labels)
}

// liveStandIn stands in for the live API reader and counts what a pass
// asks of it. It hands out a member's pods themselves, in a fresh list,
// so a read costs the pass nothing and the count is the pass's own work;
// the replica it hands out by copy, because the decoder rewrites the
// object it reads, and the service by copy, the record the controller
// persisted. Under json every read decodes the JSON the apiserver would
// serve, which is what the manager pays for a live read. It serves nothing
// else: a pass that reads another kind live is a finding, not a fixture
// gap.
type liveStandIn struct {
	replicas        map[types.NamespacedName]*v1beta1.InferenceReplica
	service         *v1beta1.InferenceService
	podSets         []podSet
	json            bool
	encodedReplicas map[types.NamespacedName][]byte
	encodedService  []byte
	gets, lists     int64
	serviceGets     int64
	podsListed      int64
}

// encode serializes every stored object once, so a read under json pays
// the decode alone, as the client does.
func (r *liveStandIn) encode(tb testing.TB) {
	tb.Helper()
	if r.encodedReplicas != nil {
		return
	}
	r.encodedReplicas = map[types.NamespacedName][]byte{}
	for key, replica := range r.replicas {
		data, err := json.Marshal(replica)
		if err != nil {
			tb.Fatal(err)
		}
		r.encodedReplicas[key] = data
	}
	data, err := json.Marshal(r.service)
	if err != nil {
		tb.Fatal(err)
	}
	r.encodedService = data
	for i := range r.podSets {
		list := corev1.PodList{Items: make([]corev1.Pod, len(r.podSets[i].pods))}
		for j, pod := range r.podSets[i].pods {
			list.Items[j] = *pod
		}
		data, err := json.Marshal(&list)
		if err != nil {
			tb.Fatal(err)
		}
		r.podSets[i].encoded = data
	}
}

func (r *liveStandIn) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch obj := obj.(type) {
	case *v1beta1.InferenceReplica:
		return r.getReplica(key, obj)
	case *v1beta1.InferenceService:
		return r.getService(key, obj)
	}
	return fmt.Errorf("the live stand-in serves InferenceReplica and InferenceService reads only, not %T", obj)
}

func (r *liveStandIn) getReplica(key client.ObjectKey, replica *v1beta1.InferenceReplica) error {
	r.gets++
	stored, ok := r.replicas[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}, key.Name)
	}
	if r.json {
		return json.Unmarshal(r.encodedReplicas[key], replica)
	}
	stored.DeepCopyInto(replica)
	return nil
}

func (r *liveStandIn) getService(key client.ObjectKey, service *v1beta1.InferenceService) error {
	r.serviceGets++
	if r.service == nil || client.ObjectKeyFromObject(r.service) != key {
		return apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, key.Name)
	}
	if r.json {
		return json.Unmarshal(r.encodedService, service)
	}
	r.service.DeepCopyInto(service)
	return nil
}

func (r *liveStandIn) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return fmt.Errorf("the live stand-in serves pod lists only, not %T", list)
	}
	var options client.ListOptions
	options.ApplyOptions(opts)
	r.lists++
	for i := range r.podSets {
		set := &r.podSets[i]
		if set.namespace != options.Namespace || options.LabelSelector == nil || !options.LabelSelector.Matches(set.labels) {
			continue
		}
		r.podsListed += int64(len(set.pods))
		if r.json {
			return json.Unmarshal(set.encoded, pods)
		}
		pods.Items = make([]corev1.Pod, len(set.pods))
		for j, pod := range set.pods {
			pods.Items[j] = *pod
		}
		return nil
	}
	pods.Items = nil
	return nil
}

// cacheStandIn stands in for the manager's cached client: it serves the
// replicas, ControllerRevisions and per-revision Services a pass reads by
// copy, keeps what a pass creates or updates, and counts both. It serves a
// member's pods as the informer cache does, through the pod index or the
// label selector, a copy of each unless the list asks for the cache's own
// objects, and counts the pod lists, the pods they hand out and the pods
// they copy apart from the other reads. The pass that stands the unit up
// creates the Services through it; a pass at rest writes nothing. It takes
// no patch: the one patch the dispatch makes consumes an operator's
// annotation, which no pass at rest carries.
type cacheStandIn struct {
	client.Client
	objects       map[storeKey]client.Object
	podSets       []podSet
	reads, writes int64
	podLists      int64
	podsListed    int64
	podsCopied    int64
}

// storeKey names one stored object by its type, namespace and name.
type storeKey struct {
	kind, namespace, name string
}

func storeKeyOf(obj client.Object, namespace, name string) storeKey {
	return storeKey{kind: fmt.Sprintf("%T", obj), namespace: namespace, name: name}
}

func (c *cacheStandIn) put(obj client.Object) {
	c.objects[storeKeyOf(obj, obj.GetNamespace(), obj.GetName())] = obj.DeepCopyObject().(client.Object)
}

func (c *cacheStandIn) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	c.reads++
	stored, ok := c.objects[storeKeyOf(obj, key.Namespace, key.Name)]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, key.Name)
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored.DeepCopyObject()).Elem())
	return nil
}

// List answers the Service and ControllerRevision lists of one namespace
// under a label selector, in name order, and the pod lists of a member.
func (c *cacheStandIn) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var options client.ListOptions
	options.ApplyOptions(opts)
	if pods, ok := list.(*corev1.PodList); ok {
		return c.listPods(pods, options)
	}
	c.reads++
	switch l := list.(type) {
	case *corev1.ServiceList:
		l.Items = storedItems[corev1.Service](c, options)
	case *appsv1.ControllerRevisionList:
		l.Items = storedItems[appsv1.ControllerRevision](c, options)
	default:
		return fmt.Errorf("the cache stand-in lists Services, ControllerRevisions and Pods only, not %T", list)
	}
	return nil
}

// listPods answers a pod list as the manager's cache does: the member the
// pod index or the label selector names, a copy of each pod in a fresh
// list, or the stored pods themselves in a fresh list when the list asks
// for the cache's own objects.
func (c *cacheStandIn) listPods(pods *corev1.PodList, options client.ListOptions) error {
	c.podLists++
	for i := range c.podSets {
		set := &c.podSets[i]
		if set.namespace != options.Namespace || !set.selectedBy(options) {
			continue
		}
		c.podsListed += int64(len(set.pods))
		pods.Items = make([]corev1.Pod, len(set.pods))
		if options.UnsafeDisableDeepCopy != nil && *options.UnsafeDisableDeepCopy {
			for j, pod := range set.pods {
				pods.Items[j] = *pod
			}
			return nil
		}
		c.podsCopied += int64(len(set.pods))
		for j, pod := range set.pods {
			pods.Items[j] = *pod.DeepCopy()
		}
		return nil
	}
	pods.Items = nil
	return nil
}

// storedItems is the stored objects of one type in the namespace the
// options name, matching their label selector, copied and in name order.
func storedItems[T any, PT interface {
	*T
	client.Object
}](c *cacheStandIn, options client.ListOptions) []T {
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

func (c *cacheStandIn) Create(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
	c.writes++
	if _, exists := c.objects[storeKeyOf(obj, obj.GetNamespace(), obj.GetName())]; exists {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, obj.GetName())
	}
	obj.SetResourceVersion("1")
	c.put(obj)
	return nil
}

func (c *cacheStandIn) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	c.writes++
	if _, exists := c.objects[storeKeyOf(obj, obj.GetNamespace(), obj.GetName())]; !exists {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, obj.GetName())
	}
	c.put(obj)
	return nil
}

func (c *cacheStandIn) Patch(_ context.Context, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
	c.writes++
	return fmt.Errorf("the cache stand-in takes no patch of %T %s", obj, obj.GetName())
}

func (c *cacheStandIn) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	c.writes++
	delete(c.objects, storeKeyOf(obj, obj.GetNamespace(), obj.GetName()))
	return nil
}
