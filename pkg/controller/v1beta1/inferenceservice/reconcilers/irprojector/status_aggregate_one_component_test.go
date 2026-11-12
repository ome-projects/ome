package irprojector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// mirrorCase is one row of TestAggregateOneComponent: the service as the
// store holds it and as the pass is handed it, the replica in the store, the
// view the replica is read through, the failures the store answers with, and
// what the pass must leave on the live service, on its own copy of the
// service and in the count of status writes.
type mirrorCase struct {
	name string
	// component is the role whose replica is mirrored; empty is the engine.
	component v1beta1.ComponentType
	// stored edits the service in the store, which starts as baselineISVC.
	stored func(isvc *v1beta1.InferenceService)
	// noService leaves the service out of the store.
	noService bool
	// handed edits the copy the pass is given, which starts as a deep copy of
	// the stored service.
	handed func(isvc *v1beta1.InferenceService)
	// replica builds the replica in the store for a service of that name; nil
	// stores the reconciled two-Instance engine replica of liveIR.
	replica func(name string) *v1beta1.InferenceReplica
	// noReplica leaves the replica out of the store.
	noReplica bool
	// cache shapes the client the replica is read through, layered over the
	// store for a service of that name; nil reads the store as it is.
	cache func(name string) []fakeclient.Option
	// store shapes the store itself: the reader the service is read through
	// and the client the write reaches.
	store []fakeclient.Option
	// passes is how many times the pass runs, each over a fresh copy of the
	// handed service; zero is one. between runs before every pass but the
	// first.
	passes  int
	between func(s *mirrorPass)
	// wantErr is text the pass's error must carry, with %s for the service's
	// name; empty means the pass must succeed.
	wantErr string
	// wantWrites is how many status writes of the service the store saw,
	// refused ones included.
	wantWrites int
	check      func(s *mirrorPass)
}

// mirrorPass is what a row's check reads: the store, the copy of the service
// the last pass was handed, and the role.
type mirrorPass struct {
	*gomega.WithT
	t         *testing.T
	store     client.WithWatch
	name      string
	namespace string
	component v1beta1.ComponentType
	handed    *v1beta1.InferenceService
}

// live reads the service from the store.
func (s *mirrorPass) live() *v1beta1.InferenceService {
	s.t.Helper()
	got := &v1beta1.InferenceService{}
	s.Expect(s.store.Get(context.Background(), client.ObjectKey{Namespace: s.namespace, Name: s.name}, got)).To(gomega.Succeed())
	return got
}

// lifecycle is the role's lifecycle block on a service, nil when none.
func (s *mirrorPass) lifecycle(isvc *v1beta1.InferenceService) *v1beta1.LifecycleStatus {
	return isvc.Status.Components[s.component].Lifecycle
}

// replica reads the role's replica from the store.
func (s *mirrorPass) replica() *v1beta1.InferenceReplica {
	s.t.Helper()
	got := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: s.namespace, Name: InferenceReplicaName(s.name, s.component)}
	s.Expect(s.store.Get(context.Background(), key, got)).To(gomega.Succeed())
	return got
}

// publishReplicaStatus edits the replica's status in the store, as the
// replica controller does between two passes of the service.
func (s *mirrorPass) publishReplicaStatus(edit func(ir *v1beta1.InferenceReplica)) {
	s.t.Helper()
	ir := s.replica()
	edit(ir)
	s.Expect(s.store.Status().Update(context.Background(), ir)).To(gomega.Succeed())
}

// readinessSamples is how many readiness durations of one deploy kind the
// time-to-ready metric holds for the service.
func (s *mirrorPass) readinessSamples(kind string) uint64 {
	s.t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	s.Expect(err).NotTo(gomega.HaveOccurred())
	for _, family := range families {
		if family.GetName() != "ome_isvc_time_to_ready_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["namespace"] == s.namespace && labels["isvc"] == s.name && labels["deploy_kind"] == kind {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

// routeUp records the route as ready on a service, so the engine's
// condition alone decides whether the service reads Ready.
func routeUp(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{Type: v1beta1.IngressReady, Status: corev1.ConditionTrue, Reason: "IngressDisabled"})
}

// createdMinutesAgo dates the service's creation, the anchor of a first
// deploy's time to ready.
func createdMinutesAgo(minutes int) func(isvc *v1beta1.InferenceService) {
	return func(isvc *v1beta1.InferenceService) {
		isvc.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Duration(minutes) * time.Minute))
	}
}

// readinessLostMinutesAgo records the engine as not serving and the service
// as not Ready since that many minutes, the anchor of an update's time to
// ready.
func readinessLostMinutesAgo(minutes int) func(isvc *v1beta1.InferenceService) {
	return func(isvc *v1beta1.InferenceService) {
		isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Type: v1beta1.EngineReady, Status: corev1.ConditionFalse, Reason: "InsufficientAvailable"})
		lost := metav1.NewTime(time.Now().Add(-time.Duration(minutes) * time.Minute))
		for i := range isvc.Status.Conditions {
			if isvc.Status.Conditions[i].Type == knapis.ConditionReady {
				isvc.Status.Conditions[i].LastTransitionTime = knapis.VolatileTime{Inner: lost}
			}
		}
	}
}

// routeUpAndCreated is a service whose route is up and whose creation is dated,
// one write of a serving engine away from its first Ready.
func routeUpAndCreated(isvc *v1beta1.InferenceService) {
	routeUp(isvc)
	createdMinutesAgo(1)(isvc)
}

// routeUpAndReadinessLost is a service whose route is up and which lost its
// readiness two minutes ago, one write of a serving engine away from Ready again.
func routeUpAndReadinessLost(isvc *v1beta1.InferenceService) {
	routeUp(isvc)
	readinessLostMinutesAgo(2)(isvc)
}

// countsReadiness checks that the live service reads Ready and that the
// time-to-ready metric holds that many first-deploy and update samples for it.
func countsReadiness(creates, updates uint64) func(s *mirrorPass) {
	return func(s *mirrorPass) {
		s.Expect(s.live().Status.IsReady()).To(gomega.BeTrue())
		s.Expect(s.readinessSamples(obsmetrics.DeployKindCreate)).To(gomega.Equal(creates), "first-deploy samples")
		s.Expect(s.readinessSamples(obsmetrics.DeployKindUpdate)).To(gomega.Equal(updates), "update samples")
	}
}

// engineServing returns the engine replica of liveIR with its serving and
// ready counts set apart from its size.
func engineServing(replicas, ready, serving int32) func(name string) *v1beta1.InferenceReplica {
	return func(name string) *v1beta1.InferenceReplica {
		ir := liveIR(name, "prod", replicas)
		ir.Status.ReadyReplicas = ready
		ir.Status.ServingReplicas = serving
		ir.Status.AvailableReplicas = serving
		return ir
	}
}

// withLifecycle gives the service's engine a lifecycle block carrying that
// observed generation.
func withLifecycle(observedGeneration int64) func(isvc *v1beta1.InferenceService) {
	return func(isvc *v1beta1.InferenceService) {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{ObservedGeneration: observedGeneration}},
		}
	}
}

// unreconciledEngine returns the engine replica of liveIR with a spec its
// controller has not reconciled yet.
func unreconciledEngine(name string) *v1beta1.InferenceReplica {
	ir := liveIR(name, "prod", 2)
	ir.Generation = 2
	return ir
}

func runMirrorCase(t *testing.T, index int, row mirrorCase) {
	t.Helper()
	g := gomega.NewWithT(t)
	name, namespace := fmt.Sprintf("mirror-%02d", index), "prod"
	obsmetrics.DeleteISVCSeries(namespace, name)
	t.Cleanup(func() { obsmetrics.DeleteISVCSeries(namespace, name) })
	component := row.component
	if component == "" {
		component = v1beta1.EngineComponent
	}
	stored := baselineISVC(name, namespace)
	if row.stored != nil {
		row.stored(stored)
	}
	handed := stored.DeepCopy()
	if row.handed != nil {
		row.handed(handed)
	}
	var objects []client.Object
	if !row.noService {
		objects = append(objects, stored)
	}
	if !row.noReplica {
		ir := liveIR(name, namespace, 2)
		if row.replica != nil {
			ir = row.replica(name)
		}
		objects = append(objects, ir)
	}
	writes := fakeclient.CountingVerb(fakeclient.StatusUpdate).OnKind(&v1beta1.InferenceService{})
	options := []fakeclient.Option{
		fakeclient.WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}),
		fakeclient.WithObjects(objects...),
		writes,
	}
	store := fakeclient.New(t, append(options, row.store...)...)
	cache := store
	if row.cache != nil {
		cache = fakeclient.Wrap(t, store, row.cache(name)...)
	}
	s := &mirrorPass{WithT: g, t: t, store: store, name: name, namespace: namespace, component: component}
	var err error
	for pass := 0; pass < max(row.passes, 1); pass++ {
		if pass > 0 && row.between != nil {
			row.between(s)
		}
		s.handed = handed.DeepCopy()
		if err = aggregateOneComponent(context.Background(), cache, store, s.handed, component); err != nil {
			break
		}
	}
	if row.wantErr != "" {
		g.Expect(err).To(gomega.HaveOccurred())
		want := row.wantErr
		if strings.Contains(want, "%s") {
			want = fmt.Sprintf(want, name)
		}
		g.Expect(err.Error()).To(gomega.ContainSubstring(want))
	} else {
		g.Expect(err).NotTo(gomega.HaveOccurred())
	}
	g.Expect(writes.Count()).To(gomega.Equal(row.wantWrites), "status writes of the service")
	if row.check != nil {
		row.check(s)
	}
}

var (
	// conflictOnWrite is the refusal a service answers a write with when a
	// newer write landed first.
	conflictOnWrite = apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, "service", errors.New("a newer write landed"))
	// serviceGone is the refusal a deleted service answers with.
	serviceGone = apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, "service")
)

// mirrorCases are the rules of one pass of the status mirror over one role:
// what it reads, when it writes the live service, what it writes, how it
// answers a store that lags, refuses or lost the service, what it leaves on
// its own copy of the service, and when it counts a time to ready.
var mirrorCases = []mirrorCase{
	{
		name:      "a replica the store does not hold leaves the service and the pass's copy as they were",
		noReplica: true,
		check: func(s *mirrorPass) {
			s.Expect(s.live().Status.Components).To(gomega.BeNil())
			s.Expect(s.handed.Status.Components).To(gomega.BeNil())
		},
	},
	{
		name: "a replica read that fails for another reason is reported with the replica's name",
		cache: func(string) []fakeclient.Option {
			return []fakeclient.Option{fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("apiserver unavailable")).OnKind(&v1beta1.InferenceReplica{})}
		},
		wantErr: "get IR prod/%s-engine for component=engine: apiserver unavailable",
	},
	{
		name:       "the counters, revisions, hold and conditions of the replica land on the live service in one write",
		wantWrites: 1,
		check: func(s *mirrorPass) {
			live := s.live()
			s.Expect(s.lifecycle(live)).To(gomega.Equal(IRStatusToComponentStatus(s.replica(), nil)))
			s.Expect(s.lifecycle(live).ObservedGeneration).To(gomega.Equal(int64(1)), "the parent generation the replica reconciled")
			s.Expect(s.lifecycle(live).Conditions).To(gomega.HaveLen(1))
			s.Expect(s.lifecycle(live).CollisionCount).To(gomega.Equal(ptr.To(int32(0))))
			engine := live.Status.GetCondition(v1beta1.EngineReady)
			s.Expect(engine).NotTo(gomega.BeNil())
			s.Expect(engine.Status).To(gomega.Equal(corev1.ConditionTrue))
			s.Expect(engine.Reason).To(gomega.Equal("Ready"))
		},
	},
	{
		name:       "a replica with no serving Instance writes the role's condition False",
		replica:    engineServing(2, 0, 0),
		wantWrites: 1,
		check: func(s *mirrorPass) {
			engine := s.live().Status.GetCondition(v1beta1.EngineReady)
			s.Expect(engine.Status).To(gomega.Equal(corev1.ConditionFalse))
			s.Expect(engine.Reason).To(gomega.Equal("InsufficientAvailable"))
			s.Expect(engine.Message).To(gomega.Equal("0/2 Instances serving, need 2"))
		},
	},
	{
		name:      "a decoder's status lands under the decoder's key with the decoder's condition",
		component: v1beta1.DecoderComponent,
		replica:   func(name string) *v1beta1.InferenceReplica { return decoderIR(name, "prod", 8, 8) },
		stored: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(8)}}
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			live := s.live()
			s.Expect(live.Status.Components).To(gomega.HaveLen(1))
			s.Expect(s.lifecycle(live).ServingReplicas).To(gomega.Equal(int32(8)))
			s.Expect(live.Status.GetCondition(v1beta1.DecoderReady).Status).To(gomega.Equal(corev1.ConditionTrue))
			s.Expect(live.Status.GetCondition(v1beta1.EngineReady)).To(gomega.BeNil())
		},
	},
	{
		name: "a replica that surged above its desired count reads ready at its desired count",
		replica: func(name string) *v1beta1.InferenceReplica {
			ir := engineServing(10, 8, 8)(name)
			ir.Spec.Replicas = ptr.To(int32(8))
			return ir
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			engine := s.live().Status.GetCondition(v1beta1.EngineReady)
			s.Expect(engine.Status).To(gomega.Equal(corev1.ConditionTrue))
			s.Expect(engine.Reason).To(gomega.Equal("MinimumAvailable"))
			s.Expect(engine.Message).To(gomega.Equal("8/10 Instances serving (min 8)"))
		},
	},
	{
		name:       "a second pass over an unchanged replica writes nothing",
		passes:     2,
		wantWrites: 1,
	},
	{
		name:   "a counter that changed between two passes is written again",
		passes: 2,
		between: func(s *mirrorPass) {
			s.publishReplicaStatus(func(ir *v1beta1.InferenceReplica) { ir.Status.UpdatedReplicas = 1 })
		},
		wantWrites: 2,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).UpdatedReplicas).To(gomega.Equal(int32(1)))
		},
	},
	{
		name:       "the carried generation comes from the live service, not from the pass's copy",
		stored:     withLifecycle(7),
		handed:     func(isvc *v1beta1.InferenceService) { isvc.Status.Components = nil },
		replica:    unreconciledEngine,
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).ObservedGeneration).To(gomega.Equal(int64(7)))
			s.Expect(s.lifecycle(s.handed).ObservedGeneration).To(gomega.Equal(int64(7)))
		},
	},
	{
		name:   "a reconciled replica without a parent-generation record reports the live service's generation",
		stored: func(isvc *v1beta1.InferenceService) { isvc.Generation = 3 },
		handed: func(isvc *v1beta1.InferenceService) { isvc.Generation = 1 },
		replica: func(name string) *v1beta1.InferenceReplica {
			ir := liveIR(name, "prod", 2)
			delete(ir.Annotations, constants.InferenceReplicaParentGenerationAnnotationKey)
			return ir
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).ObservedGeneration).To(gomega.Equal(int64(3)))
			s.Expect(s.lifecycle(s.handed).ObservedGeneration).To(gomega.Equal(int64(3)))
		},
	},
	{
		name:   "a reconciled replica with a parent-generation record reports the record",
		stored: func(isvc *v1beta1.InferenceService) { isvc.Generation = 9 },
		replica: func(name string) *v1beta1.InferenceReplica {
			ir := liveIR(name, "prod", 2)
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "5"
			return ir
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).ObservedGeneration).To(gomega.Equal(int64(5)))
		},
	},
	{
		name: "the replica is read as the cache holds it",
		cache: func(name string) []fakeclient.Option {
			return []fakeclient.Option{fakeclient.StaleRead(engineServing(2, 0, 0)(name))}
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).ServingReplicas).To(gomega.Equal(int32(0)), "the lagging view's count, not the store's 2")
			s.Expect(s.live().Status.GetCondition(v1beta1.EngineReady).Status).To(gomega.Equal(corev1.ConditionFalse))
		},
	},
	{
		name:   "the service is read live, not as the cache holds it",
		stored: withLifecycle(7),
		cache: func(name string) []fakeclient.Option {
			stale := baselineISVC(name, "prod")
			withLifecycle(99)(stale)
			return []fakeclient.Option{fakeclient.StaleRead(stale)}
		},
		replica:    unreconciledEngine,
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live()).ObservedGeneration).To(gomega.Equal(int64(7)))
		},
	},
	{
		name:       "a conflict on the write is retried against a fresh read and lands",
		store:      []fakeclient.Option{fakeclient.FailingVerb(fakeclient.StatusUpdate, 1, conflictOnWrite).OnKind(&v1beta1.InferenceService{})},
		wantWrites: 2,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.live())).NotTo(gomega.BeNil())
			s.Expect(s.live().Status.GetCondition(v1beta1.EngineReady).Status).To(gomega.Equal(corev1.ConditionTrue))
		},
	},
	{
		name:      "a service gone before the live read is not an error, and the pass's copy still carries the replica's status",
		noService: true,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.handed)).To(gomega.Equal(IRStatusToComponentStatus(s.replica(), nil)))
			s.Expect(s.handed.Status.GetCondition(v1beta1.EngineReady).Status).To(gomega.Equal(corev1.ConditionTrue))
		},
	},
	{
		name:       "a write refused because the service is gone is not an error",
		store:      []fakeclient.Option{fakeclient.FailingVerb(fakeclient.StatusUpdate, fakeclient.EveryCall, serviceGone).OnKind(&v1beta1.InferenceService{})},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.live().Status.Components).To(gomega.BeNil())
		},
	},
	{
		name:    "another failure of the live read is reported",
		store:   []fakeclient.Option{fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("timeout")).OnKind(&v1beta1.InferenceService{})},
		wantErr: "persist component=engine status: re-read ISVC: timeout",
	},
	{
		name:       "another failure of the write is reported after one attempt",
		store:      []fakeclient.Option{fakeclient.FailingVerb(fakeclient.StatusUpdate, fakeclient.EveryCall, errors.New("forbidden")).OnKind(&v1beta1.InferenceService{})},
		wantErr:    "persist component=engine status: update ISVC status: forbidden",
		wantWrites: 1,
	},
	{
		name:       "the pass's copy mirrors the lifecycle block and the role's condition that were written",
		wantWrites: 1,
		check: func(s *mirrorPass) {
			live := s.live()
			s.Expect(s.lifecycle(s.handed)).To(gomega.Equal(s.lifecycle(live)))
			s.Expect(s.handed.Status.GetCondition(v1beta1.EngineReady).Status).To(gomega.Equal(live.Status.GetCondition(v1beta1.EngineReady).Status))
			s.Expect(s.handed.Status.GetCondition(v1beta1.EngineReady).Reason).To(gomega.Equal(live.Status.GetCondition(v1beta1.EngineReady).Reason))
		},
	},
	{
		name:       "a pass that writes nothing still brings its copy up to date",
		passes:     2,
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.lifecycle(s.handed)).To(gomega.Equal(s.lifecycle(s.live())))
			s.Expect(s.handed.Status.GetCondition(v1beta1.EngineReady).Status).To(gomega.Equal(corev1.ConditionTrue))
		},
	},
	{
		name:       "the write that first makes the service Ready is counted as a first deploy",
		stored:     routeUpAndCreated,
		wantWrites: 1,
		check:      countsReadiness(1, 0),
	},
	{
		name:   "a write while the service stays Ready counts nothing more",
		stored: routeUpAndCreated,
		passes: 2,
		between: func(s *mirrorPass) {
			s.publishReplicaStatus(func(ir *v1beta1.InferenceReplica) { ir.Status.UpdatedReplicas = 1 })
		},
		wantWrites: 2,
		check:      countsReadiness(1, 0),
	},
	{
		name:       "a service that lost readiness and serves again is counted as an update",
		stored:     routeUpAndReadinessLost,
		wantWrites: 1,
		check:      countsReadiness(0, 1),
	},
	{
		name:   "readiness before the write is read from the live service, not from the pass's copy",
		stored: routeUpAndCreated,
		handed: func(isvc *v1beta1.InferenceService) {
			isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Type: v1beta1.EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
		},
		wantWrites: 1,
		check: func(s *mirrorPass) {
			s.Expect(s.handed.Status.IsReady()).To(gomega.BeTrue(), "the handed copy already read Ready")
			s.Expect(s.readinessSamples(obsmetrics.DeployKindCreate)).To(gomega.Equal(uint64(1)), "the live service was not Ready before the write")
		},
	},
}

// TestAggregateOneComponent runs every row of mirrorCases.
func TestAggregateOneComponent(t *testing.T) {
	for i, row := range mirrorCases {
		t.Run(row.name, func(t *testing.T) { runMirrorCase(t, i, row) })
	}
}
