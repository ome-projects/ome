package inferenceservice

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
)

// The copy the pass read shows the phase written at zero; the live object
// holds the phase written at one minute; the pass runs at two minutes.
var (
	boundaryCopyAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	boundaryLiveAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC))
	boundaryPassAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC))
)

// The status write at the run boundary runs before the pass's coordination
// step, so the group entry and the readiness condition it carries are the
// ones its cached copy held, and that copy can trail the previous pass's
// write by one phase. Three engines and one decoder finish their first roll:
// the pass that sees every Instance Ready writes Idle, and the next pass,
// started on a copy from before that write, closes the run and flushes at
// once. That write records the run and keeps the live entry: the phase, the
// time it was entered and the readiness condition stay what the previous
// pass computed, and the pass's own copy reads them from here on, so its
// coordination step compares against the live entry. The same holds for a
// run opened on a copy from before the operator paused the group.
func TestRunBoundaryFlushKeepsTheLiveGroupEntryOverAStalePass(t *testing.T) {
	for _, tc := range runBoundaryCases() {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			f := newRunBoundaryFlush(t, tc)
			working := f.staleCopy(tc)
			pass := &servicePass{
				reconciler:     f.reconciler,
				isvc:           working,
				log:            logr.Discard(),
				deploymentMode: constants.OMENative,
				base:           canary.NewBase(working),
				runOutcome:     &rolloutrun.Outcome{StateChanged: true},
			}

			next, err := pass.flushRunBoundary(context.TODO())
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(next).To(gomega.BeTrue())

			got := f.persisted()
			tc.runWritten(g, got.Status.Rollout)
			g.Expect(f.groupOf(got).Phase).To(gomega.Equal(tc.live.phase),
				"the live group entry outlives a boundary write from a copy that predates it")
			g.Expect(f.groupOf(got).LastTransitionTime.Time).To(gomega.BeTemporally("==", boundaryLiveAt.Time),
				"the time the group entered its live phase is kept")
			ready := got.Status.GetCondition(f.readyType)
			g.Expect(ready).NotTo(gomega.BeNil())
			g.Expect(ready.Status).To(gomega.Equal(tc.live.ready), "the live readiness condition is kept with the entry")
			g.Expect(ready.Reason).To(gomega.Equal(tc.live.reason))
			g.Expect(ready.LastTransitionTime.Inner.Time).To(gomega.BeTemporally("==", boundaryLiveAt.Time))
			g.Expect(f.groupOf(working).Phase).To(gomega.Equal(tc.live.phase),
				"the pass's copy reads the live entry from here on")
			g.Expect(working.Status.GetCondition(f.readyType).Reason).To(gomega.Equal(tc.live.reason))
		})
	}
}

// runBoundaryCase is one stale pass at a run boundary: the group entry and
// readiness condition the live object holds, the ones the pass's copy holds
// from before that write, the run each side records, and what the boundary
// write must record of the run.
type runBoundaryCase struct {
	name        string
	live        groupEntryShape
	copy        groupEntryShape
	liveRun     *v1beta1.RolloutStatus
	copyRun     *v1beta1.RolloutStatus
	annotations map[string]string
	runWritten  func(g *gomega.WithT, rollout *v1beta1.RolloutStatus)
}

// groupEntryShape is a group entry with the readiness condition it sets.
type groupEntryShape struct {
	phase  v1beta1.CoordinationPhase
	ready  v1.ConditionStatus
	reason string
	at     metav1.Time
}

func runBoundaryCases() []runBoundaryCase {
	openRun := func(opened metav1.Time) *v1beta1.RolloutStatus {
		return &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: "run-1", OpenedAt: opened, PinnedAt: opened,
			TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "b", StableRevision: "a"}},
		}}
	}
	closedRun := &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunCompleted, RunID: "run-1", OpenedAt: &boundaryCopyAt, ClosedAt: &boundaryPassAt,
	}}
	return []runBoundaryCase{
		{
			name:    "a run closed again on a copy from before the group read Idle keeps Idle",
			live:    groupEntryShape{phase: v1beta1.CoordinationPhaseIdle, ready: v1.ConditionTrue, reason: "Idle", at: boundaryLiveAt},
			copy:    groupEntryShape{phase: v1beta1.CoordinationPhaseSurging, ready: v1.ConditionFalse, reason: "InProgress", at: boundaryCopyAt},
			liveRun: openRun(boundaryCopyAt),
			copyRun: closedRun,
			runWritten: func(g *gomega.WithT, rollout *v1beta1.RolloutStatus) {
				g.Expect(rollout).NotTo(gomega.BeNil())
				g.Expect(rollout.ActiveRun).To(gomega.BeNil(), "the boundary write records the close")
				g.Expect(rollout.LastRun).NotTo(gomega.BeNil())
				g.Expect(rollout.LastRun.RunID).To(gomega.Equal("run-1"))
			},
		},
		{
			name:        "a run opened on a copy from before the operator paused keeps Paused",
			live:        groupEntryShape{phase: v1beta1.CoordinationPhasePaused, ready: v1.ConditionFalse, reason: "InProgress", at: boundaryLiveAt},
			copy:        groupEntryShape{phase: v1beta1.CoordinationPhaseIdle, ready: v1.ConditionTrue, reason: "Idle", at: boundaryCopyAt},
			copyRun:     openRun(boundaryPassAt),
			annotations: map[string]string{constants.PausedRolloutAnnotation: "true"},
			runWritten: func(g *gomega.WithT, rollout *v1beta1.RolloutStatus) {
				g.Expect(rollout).NotTo(gomega.BeNil())
				g.Expect(rollout.ActiveRun).NotTo(gomega.BeNil(), "the boundary write records the open")
				g.Expect(rollout.ActiveRun.RunID).To(gomega.Equal("run-1"))
			},
		},
	}
}

// runBoundaryFlush is the fixture of the boundary-write tests: a live
// service with one rolling group and a reconciler over it.
type runBoundaryFlush struct {
	g          *gomega.WithT
	nn         types.NamespacedName
	readyType  knapis.ConditionType
	live       client.Client
	reconciler *InferenceServiceReconciler
}

func newRunBoundaryFlush(t *testing.T, tc runBoundaryCase) *runBoundaryFlush {
	g := gomega.NewGomegaWithT(t)
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	f := &runBoundaryFlush{
		g:         g,
		nn:        types.NamespacedName{Name: "stale-boundary-isvc", Namespace: "default"},
		readyType: knapis.ConditionType(v1beta1.RolloutCoordinationReady),
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: f.nn.Name, Namespace: f.nn.Namespace, Annotations: tc.annotations},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "base-model", Kind: stringPtr("BaseModel")}, Engine: &v1beta1.EngineSpec{}},
		Status:     v1beta1.InferenceServiceStatus{RolloutCoordination: f.entry(tc.live), Rollout: tc.liveRun},
	}
	isvc.Status.SetConditions(f.readiness(tc.live))
	f.live = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	f.reconciler = &InferenceServiceReconciler{Client: f.live, APIReader: f.live, Scheme: scheme, Log: ctrl.Log.WithName("test"), Recorder: record.NewFakeRecorder(10)}
	return f
}

func (f *runBoundaryFlush) entry(s groupEntryShape) *v1beta1.RolloutCoordinationStatus {
	at := s.at
	return &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
		Name:               "0",
		Components:         []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:             v1beta1.CoordinationPolicyRollingUpdate,
		Phase:              s.phase,
		CompositePhase:     string(s.phase),
		LastTransitionTime: &at,
	}}}
}

func (f *runBoundaryFlush) readiness(s groupEntryShape) knapis.Conditions {
	return knapis.Conditions{{Type: f.readyType, Status: s.ready, Reason: s.reason, LastTransitionTime: knapis.VolatileTime{Inner: s.at}}}
}

// staleCopy is the service as the pass read it: the live object with the
// group entry and the readiness condition from before the live write, and
// the run as this pass's run layer left it.
func (f *runBoundaryFlush) staleCopy(tc runBoundaryCase) *v1beta1.InferenceService {
	working := f.persisted()
	working.Status.RolloutCoordination = f.entry(tc.copy)
	working.Status.SetConditions(f.readiness(tc.copy))
	working.Status.Rollout = tc.copyRun.DeepCopy()
	return working
}

func (f *runBoundaryFlush) persisted() *v1beta1.InferenceService {
	got := &v1beta1.InferenceService{}
	f.g.Expect(f.live.Get(context.TODO(), f.nn, got)).NotTo(gomega.HaveOccurred())
	return got
}

func (f *runBoundaryFlush) groupOf(isvc *v1beta1.InferenceService) v1beta1.RolloutCoordinationGroupStatus {
	f.g.Expect(isvc.Status.RolloutCoordination).NotTo(gomega.BeNil())
	f.g.Expect(isvc.Status.RolloutCoordination.Groups).To(gomega.HaveLen(1))
	return isvc.Status.RolloutCoordination.Groups[0]
}

// A boundary write against a live object with no group entry writes none:
// a service that never coordinated gains no entry from a copy, and the
// pass's copy drops the entry it carried.
func TestRunBoundaryFlushWritesNoEntryTheLiveObjectLacks(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	nn := types.NamespacedName{Name: "plain-isvc", Namespace: "default"}
	live := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: nn.Name, Namespace: nn.Namespace},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "base-model", Kind: stringPtr("BaseModel")}, Engine: &v1beta1.EngineSpec{}},
	}).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	reconciler := &InferenceServiceReconciler{Client: live, APIReader: live, Scheme: scheme, Log: ctrl.Log.WithName("test"), Recorder: record.NewFakeRecorder(10)}

	working := &v1beta1.InferenceService{}
	g.Expect(live.Get(context.TODO(), nn, working)).NotTo(gomega.HaveOccurred())
	working.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Phase: v1beta1.CoordinationPhaseIdle, LastTransitionTime: &boundaryCopyAt,
	}}}
	working.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{RunID: "run-1", OpenedAt: boundaryPassAt, PinnedAt: boundaryPassAt}}
	pass := &servicePass{
		reconciler:     reconciler,
		isvc:           working,
		log:            logr.Discard(),
		deploymentMode: constants.OMENative,
		base:           canary.NewBase(working),
		runOutcome:     &rolloutrun.Outcome{StateChanged: true},
	}

	next, err := pass.flushRunBoundary(context.TODO())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(next).To(gomega.BeTrue())
	got := &v1beta1.InferenceService{}
	g.Expect(live.Get(context.TODO(), nn, got)).NotTo(gomega.HaveOccurred())
	g.Expect(got.Status.RolloutCoordination).To(gomega.BeNil())
	g.Expect(got.Status.Rollout).NotTo(gomega.BeNil())
	g.Expect(got.Status.Rollout.ActiveRun).NotTo(gomega.BeNil())
	g.Expect(working.Status.RolloutCoordination).To(gomega.BeNil())
}
