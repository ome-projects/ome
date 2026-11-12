package inferenceservice

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
)

// The live entry was written at one minute; the stale pass runs at two.
var (
	staleCopyLiveAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC))
	staleCopyPassAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC))
)

// A pass compares the group phase it computes with the copy of the service
// it read. When that copy trails the previous pass's write, the comparison
// sees that write's change once more, so the entry the pass carries to its
// final flush names the phase the live entry already holds with a transition
// time of now, beside a readiness condition with the time of now and, under a
// Sequential wait, a finish record taken again. The flush keeps the live
// instants for what the live entry already holds: the transition time of the
// same phase, the readiness condition's time while the condition is the same
// apart from its time, and the earlier of two finish records for the same
// member. A phase the live entry does not hold is a transition and takes the
// pass's time. The working copy shares the entries the flush writes, so it
// reads the kept instants too.
func TestFlushKeepsTheLivePhaseTimeOverAStalePass(t *testing.T) {
	f := newStaleCopyFlush(t)
	idle := groupShape{phase: v1beta1.CoordinationPhaseIdle, composite: "Idle"}
	awaiting := groupShape{phase: v1beta1.CoordinationPhaseIdle, composite: v1beta1.CompositePhaseSequentialAwaiting}
	surging := groupShape{phase: v1beta1.CoordinationPhaseSurging, composite: "engine.Surging"}

	t.Run("a phase the live entry already holds keeps the live transition time and readiness time", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		live, reconciler := f.live(f.entry(idle.at(staleCopyLiveAt)), f.readiness(v1.ConditionTrue, "Idle", staleCopyLiveAt))
		working, base := f.stalePass(live, f.entry(idle.at(staleCopyPassAt)), f.readiness(v1.ConditionTrue, "Idle", staleCopyPassAt))

		g.Expect(reconciler.FlushStatusThenConsume(context.TODO(), working, constants.OMENative, nil, base)).NotTo(gomega.HaveOccurred())
		got := f.persisted(live)
		g.Expect(f.groupOf(got).Phase).To(gomega.Equal(v1beta1.CoordinationPhaseIdle))
		g.Expect(f.groupOf(got).LastTransitionTime.Time).To(gomega.BeTemporally("==", staleCopyLiveAt.Time),
			"the live entry already held Idle; the time it entered Idle must not move under a pass that saw the change again")
		g.Expect(f.readyAt(got)).To(gomega.BeTemporally("==", staleCopyLiveAt.Time), "the readiness condition is the same condition; its time stays")
		g.Expect(f.groupOf(working).LastTransitionTime.Time).To(gomega.BeTemporally("==", staleCopyLiveAt.Time), "the working copy reads the kept time")
	})

	t.Run("a finish record for the same member keeps the instant it first read done", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		live, reconciler := f.live(f.entry(awaiting.at(staleCopyLiveAt).finishedAt(staleCopyLiveAt)), f.readiness(v1.ConditionTrue, "Idle", staleCopyLiveAt))
		working, base := f.stalePass(live, f.entry(awaiting.at(staleCopyPassAt).finishedAt(staleCopyPassAt)), f.readiness(v1.ConditionTrue, "Idle", staleCopyPassAt))

		g.Expect(reconciler.FlushStatusThenConsume(context.TODO(), working, constants.OMENative, nil, base)).NotTo(gomega.HaveOccurred())
		got := f.groupOf(f.persisted(live))
		g.Expect(got.MemberFinished).NotTo(gomega.BeNil())
		g.Expect(got.MemberFinished.At.Time).To(gomega.BeTemporally("==", staleCopyLiveAt.Time),
			"the decoder first read done at the live instant; a later pass that takes the record again must not move the wait's start")
		g.Expect(got.LastTransitionTime.Time).To(gomega.BeTemporally("==", staleCopyLiveAt.Time))
	})

	t.Run("a phase the live entry does not hold takes the pass's time", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		live, reconciler := f.live(f.entry(idle.at(staleCopyLiveAt)), f.readiness(v1.ConditionTrue, "Idle", staleCopyLiveAt))
		working, base := f.stalePass(live, f.entry(surging.at(staleCopyPassAt)), f.readiness(v1.ConditionFalse, "InProgress", staleCopyPassAt))

		g.Expect(reconciler.FlushStatusThenConsume(context.TODO(), working, constants.OMENative, nil, base)).NotTo(gomega.HaveOccurred())
		got := f.persisted(live)
		g.Expect(f.groupOf(got).Phase).To(gomega.Equal(v1beta1.CoordinationPhaseSurging))
		g.Expect(f.groupOf(got).LastTransitionTime.Time).To(gomega.BeTemporally("==", staleCopyPassAt.Time), "a transition takes the time of the pass that saw it")
		g.Expect(f.readyAt(got)).To(gomega.BeTemporally("==", staleCopyPassAt.Time))
	})
}

// groupShape is one group entry as a pass writes it: the phase, the composite
// phase, the transition time and, under a Sequential wait, the finish record
// of the decoder.
type groupShape struct {
	phase     v1beta1.CoordinationPhase
	composite string
	when      metav1.Time
	finished  *metav1.Time
}

func (s groupShape) at(when metav1.Time) groupShape {
	s.when = when
	return s
}

func (s groupShape) finishedAt(when metav1.Time) groupShape {
	s.finished = &when
	return s
}

// staleCopyFlush is the fixture of TestFlushKeepsTheLivePhaseTimeOverAStalePass:
// a live service with one Sequential group entry and its readiness condition,
// and the working copy a stale pass carries to its final flush.
type staleCopyFlush struct {
	g         *gomega.WithT
	scheme    *runtime.Scheme
	nn        types.NamespacedName
	readyType knapis.ConditionType
}

func newStaleCopyFlush(t *testing.T) *staleCopyFlush {
	g := gomega.NewGomegaWithT(t)
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	return &staleCopyFlush{
		g:         g,
		scheme:    scheme,
		nn:        types.NamespacedName{Name: "stale-copy-isvc", Namespace: "default"},
		readyType: knapis.ConditionType(v1beta1.RolloutCoordinationReady),
	}
}

func (f *staleCopyFlush) entry(s groupShape) *v1beta1.RolloutCoordinationStatus {
	when := s.when
	group := v1beta1.RolloutCoordinationGroupStatus{
		Name:               "0",
		Components:         []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:             v1beta1.CoordinationPolicySequential,
		Order:              []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Phase:              s.phase,
		CompositePhase:     s.composite,
		LastTransitionTime: &when,
	}
	if s.finished != nil {
		group.CurrentComponent = v1beta1.EngineComponent
		group.PreviousComponent = v1beta1.DecoderComponent
		group.MemberFinished = &v1beta1.RolloutCoordinationMemberFinish{Component: v1beta1.DecoderComponent, At: *s.finished}
	}
	return &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{group}}
}

func (f *staleCopyFlush) readiness(status v1.ConditionStatus, reason string, at metav1.Time) knapis.Conditions {
	return knapis.Conditions{{Type: f.readyType, Status: status, Reason: reason, LastTransitionTime: knapis.VolatileTime{Inner: at}}}
}

// live builds the live service and a reconciler over it.
func (f *staleCopyFlush) live(groups *v1beta1.RolloutCoordinationStatus, conditions knapis.Conditions) (client.Client, *InferenceServiceReconciler) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: f.nn.Name, Namespace: f.nn.Namespace},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "base-model", Kind: stringPtr("BaseModel")}, Engine: &v1beta1.EngineSpec{}},
		Status:     v1beta1.InferenceServiceStatus{RolloutCoordination: groups},
	}
	isvc.Status.SetConditions(conditions)
	live := ctrlclientfake.NewClientBuilder().WithScheme(f.scheme).WithObjects(isvc).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	return live, &InferenceServiceReconciler{Client: live, APIReader: live, Scheme: f.scheme, Log: ctrl.Log.WithName("test"), Recorder: record.NewFakeRecorder(10)}
}

func (f *staleCopyFlush) persisted(live client.Client) *v1beta1.InferenceService {
	got := &v1beta1.InferenceService{}
	f.g.Expect(live.Get(context.TODO(), f.nn, got)).NotTo(gomega.HaveOccurred())
	return got
}

func (f *staleCopyFlush) groupOf(isvc *v1beta1.InferenceService) v1beta1.RolloutCoordinationGroupStatus {
	f.g.Expect(isvc.Status.RolloutCoordination).NotTo(gomega.BeNil())
	f.g.Expect(isvc.Status.RolloutCoordination.Groups).To(gomega.HaveLen(1))
	return isvc.Status.RolloutCoordination.Groups[0]
}

func (f *staleCopyFlush) readyAt(isvc *v1beta1.InferenceService) time.Time {
	cond := isvc.Status.GetCondition(f.readyType)
	f.g.Expect(cond).NotTo(gomega.BeNil())
	return cond.LastTransitionTime.Inner.Time
}

// stalePass is the working copy a pass carries to its final flush after
// computing the given entry against a copy that showed another phase.
func (f *staleCopyFlush) stalePass(live client.Client, groups *v1beta1.RolloutCoordinationStatus, conditions knapis.Conditions) (*v1beta1.InferenceService, *canary.Base) {
	working := f.persisted(live)
	base := canary.NewBase(working)
	working.Status.RolloutCoordination = groups
	working.Status.SetConditions(conditions)
	return working, base
}
