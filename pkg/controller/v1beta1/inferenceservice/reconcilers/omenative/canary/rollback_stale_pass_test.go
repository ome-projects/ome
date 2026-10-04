package canary

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/rollout"
)

// passSim drives the controller's pass order over a P/D canary unit on the
// fake client: read the InferenceService (the current version, or the one
// before the last write, as an informer that has not yet seen the pass's own
// write serves it), run the run layer, project each member's partition onto
// its InferenceReplica, dispatch the executor, and flush the status the way
// the controller does. A spec write bumps the replica's generation; the
// replica reports it observed only when the test says so, the way the
// replica controller runs between passes.
type passSim struct {
	t          *testing.T
	c          client.Client
	ns, name   string
	now        time.Time
	history    []*v1beta1.InferenceService
	readAt     int
	specWrites map[v1beta1.ComponentType]int
	// members are the Components whose partition the pass projects.
	members []v1beta1.ComponentType
}

const (
	simEngineStable  = "engold"
	simEngineCanary  = "engnew"
	simDecoderStable = "decold"
	simDecoderCanary = "decnew"
)

// newPassSim is an [engine, decoder] canary group under a pairing-protocol
// swap: every revision re-minted, the run pinned toward the new pair, one
// ready pod per revision per member, and the first step's partition already
// projected on both replicas.
func newPassSim(t *testing.T, name string) *passSim {
	t.Helper()
	ns := "default"
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: simEngineCanary, StableRevision: simEngineStable},
		{Component: v1beta1.DecoderComponent, Revision: simDecoderCanary, StableRevision: simDecoderStable},
	}
	one := int32(1)
	replica := func(comp v1beta1.ComponentType, stable, canary string) *v1beta1.InferenceReplica {
		r := ir(ns, name, comp, canary)
		r.Spec.Component = comp
		r.Generation = 1
		r.Status.ObservedGeneration = 1
		r.Status.CurrentRevision = name + "-" + string(comp) + "-" + stable
		r.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: &one}
		return r
	}
	objs := []runtime.Object{isvc,
		replica(v1beta1.EngineComponent, simEngineStable, simEngineCanary),
		replica(v1beta1.DecoderComponent, simDecoderStable, simDecoderCanary),
		canaryPod(ns, name, "engine", simEngineStable, name+"-engine-0"),
		canaryPod(ns, name, "engine", simEngineCanary, name+"-engine-1"),
		canaryPod(ns, name, "decoder", simDecoderStable, name+"-decoder-0"),
		canaryPod(ns, name, "decoder", simDecoderCanary, name+"-decoder-1"),
		pairedRevision(ns, name, "engine", simEngineStable, 1, pdStableProtocol),
		pairedRevision(ns, name, "engine", simEngineCanary, 2, pdCanaryProtocol),
		pairedRevision(ns, name, "decoder", simDecoderStable, 1, pdStableProtocol),
		pairedRevision(ns, name, "decoder", simDecoderCanary, 2, pdCanaryProtocol),
	}
	return newPassSimWith(t, ns, name, []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, objs)
}

// newPassSimWith builds the simulation over seeded objects for the given
// group members.
func newPassSimWith(t *testing.T, ns, name string, members []v1beta1.ComponentType, objs []runtime.Object) *passSim {
	t.Helper()
	s := &passSim{t: t, ns: ns, name: name, now: time.Unix(1000, 0), specWrites: map[v1beta1.ComponentType]int{}, members: members}
	base := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	s.c = interceptor.NewClient(base, interceptor.Funcs{Update: s.bumpGeneration})
	s.record()
	return s
}

// bumpGeneration is the apiserver's rule for a replica spec write: a changed
// spec advances metadata.generation, and the write is counted per member.
func (s *passSim) bumpGeneration(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
	r, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return c.Update(ctx, obj, opts...)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(r), stored); err != nil {
		return err
	}
	if !equality.Semantic.DeepEqual(stored.Spec, r.Spec) {
		r.Generation = stored.Generation + 1
		s.specWrites[r.Spec.Component]++
	}
	return c.Update(ctx, obj, opts...)
}

func (s *passSim) key() types.NamespacedName {
	return types.NamespacedName{Namespace: s.ns, Name: s.name}
}

func (s *passSim) live() *v1beta1.InferenceService {
	s.t.Helper()
	got := &v1beta1.InferenceService{}
	if err := s.c.Get(context.Background(), s.key(), got); err != nil {
		s.t.Fatal(err)
	}
	return got
}

// record appends the persisted version a write produced.
func (s *passSim) record() {
	s.history = append(s.history, s.live())
}

// persisted is the version the apiserver holds now.
func (s *passSim) persisted() *v1beta1.InferenceService {
	return s.history[len(s.history)-1]
}

// read serves the pass its copy: the current version, or the one before the
// last write when the informer lags it. An informer never goes backwards.
func (s *passSim) read(stale bool) *v1beta1.InferenceService {
	idx := len(s.history) - 1
	if stale && idx > 0 {
		idx--
	}
	if idx < s.readAt {
		idx = s.readAt
	}
	s.readAt = idx
	return s.history[idx].DeepCopy()
}

func (s *passSim) replica(comp v1beta1.ComponentType) *v1beta1.InferenceReplica {
	s.t.Helper()
	got := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: s.ns, Name: irprojector.InferenceReplicaName(s.name, comp)}
	if err := s.c.Get(context.Background(), key, got); err != nil {
		s.t.Fatal(err)
	}
	return got
}

// catchUp is the replica controller observing its generation.
func (s *passSim) catchUp(comp v1beta1.ComponentType) {
	s.t.Helper()
	r := s.replica(comp)
	if r.Status.ObservedGeneration == r.Generation {
		return
	}
	r.Status.ObservedGeneration = r.Generation
	if err := s.c.Update(context.Background(), r); err != nil {
		s.t.Fatal(err)
	}
}

func (s *passSim) annotate(key, value string) {
	s.t.Helper()
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
	if err := s.c.Patch(context.Background(), s.persisted().DeepCopy(), client.RawPatch(types.MergePatchType, patch)); err != nil {
		s.t.Fatal(err)
	}
	s.record()
}

func componentExt(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	switch comp {
	case v1beta1.EngineComponent:
		return &isvc.Spec.Engine.ComponentExtensionSpec
	case v1beta1.DecoderComponent:
		return &isvc.Spec.Decoder.ComponentExtensionSpec
	}
	return nil
}

// project is the projector's partition write: the step partition the pass
// resolves from the record it read, written when it differs from the
// replica's.
func (s *passSim) project(working *v1beta1.InferenceService, comp v1beta1.ComponentType) {
	s.t.Helper()
	want := StepPartition(working, comp, componentExt(working, comp))
	if want == nil {
		return
	}
	r := s.replica(comp)
	if irprojector.IRPartition(r) == *want {
		return
	}
	if r.Spec.Pacing == nil {
		r.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	p := *want
	r.Spec.Pacing.Partition = &p
	if err := s.c.Update(context.Background(), r); err != nil {
		s.t.Fatal(err)
	}
}

// flush persists the pass's status as the controller's flush does: the
// informer has caught up with the last write by then, so a pass that decided
// nothing new skips the write; otherwise the status lands on the live
// resource version, the executor state guarded against a stale copy, the run
// pin against an older one. A write the guard reduced to the live state is
// no write.
func (s *passSim) flush(working *v1beta1.InferenceService, base *Base) {
	s.t.Helper()
	s.readAt = len(s.history) - 1
	if equality.Semantic.DeepEqual(s.persisted().Status, working.Status) {
		return
	}
	latest := s.live()
	before := latest.Status.DeepCopy()
	liveRollout := latest.Status.Rollout.DeepCopy()
	base.PreserveFresh(&working.Status, &latest.Status)
	latest.Status = working.Status
	rolloutrun.PreserveNewerRun(&latest.Status, liveRollout, false)
	if equality.Semantic.DeepEqual(*before, latest.Status) {
		base.Advance(&latest.Status)
		return
	}
	if err := s.c.Status().Update(context.Background(), latest); err != nil {
		s.t.Fatal(err)
	}
	base.Advance(&latest.Status)
	s.record()
}

// pass is one reconcile of the InferenceService.
func (s *passSim) pass(name string, stale bool) {
	s.t.Helper()
	ctx := context.Background()
	working := s.read(stale)
	base := NewBase(working)
	out, err := rolloutrun.Reconcile(ctx, rolloutrun.Inputs{Client: s.c, Reader: s.c, ISVC: working, Now: s.now, FeatureEnabled: true})
	if err != nil {
		s.t.Fatalf("%s: run layer: %v", name, err)
	}
	if out.Opened {
		for _, g := range rollout.CanaryGroups(working) {
			if err := BindRun(ctx, s.c, working, g, out.Adopted); err != nil {
				s.t.Fatalf("%s: bind run: %v", name, err)
			}
		}
	}
	if out.StateChanged {
		s.flush(working, base)
		if base.Stale() {
			return
		}
	}
	for _, comp := range s.members {
		s.project(working, comp)
	}
	res, err := Dispatch(ctx, DispatchDeps{
		Client: s.c, Reader: s.c, ISVC: working, Now: s.now, Requeue: reconcileRequeue,
		ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(working),
	})
	if err != nil {
		s.t.Fatalf("%s: dispatch: %v", name, err)
	}
	s.flush(working, base)
	if len(res.Consume) > 0 && !base.Stale() {
		if err := ConsumeAnnotations(ctx, s.c, working, res.Consume); err != nil {
			s.t.Fatal(err)
		}
		s.record()
	}
}

func (s *passSim) pinnedAtStable(comp v1beta1.ComponentType, stable string) bool {
	r := s.replica(comp)
	return r.Spec.Pacing != nil && r.Spec.Pacing.RollbackToRevision != nil &&
		*r.Spec.Pacing.RollbackToRevision == s.name+"-"+string(comp)+"-"+stable
}

// expectHold checks the persisted unit: rolling back, the rejected revision
// recorded, both replicas pinned at their stable revision.
func (s *passSim) expectHold(at string) {
	s.t.Helper()
	got := s.persisted()
	cs := rollout.CanaryStatusFor(&got.Status, v1beta1.EngineComponent)
	if cs == nil || cs.RolledBackRevisionHash != simEngineCanary || got.Status.Components[v1beta1.EngineComponent].RolloutPhase != v1beta1.RolloutPhaseRollingBack {
		s.t.Fatalf("%s: the unit must hold RollingBack with its rejection recorded, got %+v phase=%q",
			at, cs, got.Status.Components[v1beta1.EngineComponent].RolloutPhase)
	}
	if !s.pinnedAtStable(v1beta1.EngineComponent, simEngineStable) || !s.pinnedAtStable(v1beta1.DecoderComponent, simDecoderStable) {
		s.t.Fatalf("%s: both replicas must stay pinned at their stable revision", at)
	}
}

// A P/D unit rolling back holds RollingBack with its rejection recorded
// across passes that read a copy the informer has not caught up on, while
// one member's replica or the other still has the pass's own spec write to
// observe; each replica's spec moves once per decided change: the pin, then
// the partition released for the revert.
func TestPDRollbackHoldsAcrossStalePasses(t *testing.T) {
	s := newPassSim(t, "swap")
	s.pass("serve", false)
	if got := s.persisted().Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("the first split must serve before the rollback, got %q", got)
	}
	entered := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent).StepEnteredTime

	s.annotate(constants.RolloutRollbackAnnotation, "true")
	s.pass("roll back", false)
	s.expectHold("roll back")
	if s.specWrites[v1beta1.EngineComponent] != 1 || s.specWrites[v1beta1.DecoderComponent] != 1 {
		t.Fatalf("the rollback pass writes each replica once, the pin: %v", s.specWrites)
	}

	// The members observe the pin one at a time, and every pass reads the
	// version before the last write.
	members := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	for i := 0; i < 8; i++ {
		s.catchUp(members[i%2])
		s.pass(fmt.Sprintf("stale %d", i), true)
		s.expectHold(fmt.Sprintf("stale pass %d", i))
	}
	s.catchUp(v1beta1.EngineComponent)
	s.catchUp(v1beta1.DecoderComponent)
	s.pass("current", false)
	s.expectHold("current pass")

	if cs := rollout.CanaryStatusFor(&s.persisted().Status, v1beta1.EngineComponent); !cs.StepEnteredTime.Equal(entered) {
		t.Fatalf("the hold re-stamped the step clock: %v -> %v", entered, cs.StepEnteredTime)
	}
	// The pin, then the partition released for the revert: two writes per
	// replica, however many passes the hold spans.
	if s.specWrites[v1beta1.EngineComponent] != 2 || s.specWrites[v1beta1.DecoderComponent] != 2 {
		t.Fatalf("a replica's spec moves once per decided change, got %v writes", s.specWrites)
	}
	for _, comp := range members {
		if got := irprojector.IRPartition(s.replica(comp)); got != 0 {
			t.Fatalf("%s must release its partition for the revert, got %d", comp, got)
		}
	}
}
