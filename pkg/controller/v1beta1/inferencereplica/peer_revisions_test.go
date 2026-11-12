package inferencereplica

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/render"
)

// pdParent is a PD parent ISVC with one blueGreen group spanning engine and
// decoder, so peer env is declared and both Components are serving peers.
func pdParent() *v1beta1.InferenceService {
	parent := migrationParent(nil, true)
	parent.Spec.Engine = &v1beta1.EngineSpec{}
	parent.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}}}
	return parent
}

// projectedIR is baselineIR for component, stamped with the parent
// generation the projector writes, with status reporting the given
// current/update revisions as already observed for its generation.
func projectedIR(component v1beta1.ComponentType, parentGeneration, currentHash, updateHash string) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-"+string(component), "default", 1)
	ir.Spec.Component = component
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = parentGeneration
	ir.Generation = 3
	ir.Status.ObservedGeneration = 3
	if currentHash != "" {
		ir.Status.CurrentRevision = ir.Name + "-" + currentHash
	}
	if updateHash != "" {
		ir.Status.UpdateRevision = ir.Name + "-" + updateHash
	}
	return ir
}

func ownTargetCR(name string) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
}

// TestResolvePeerRevisions_PairsTargetWithTargetAndOthersWithCurrent pins
// the pairing rule: a pod rendered for the engine's roll target pairs with
// the decoder's roll target; a pod rendered for any other engine revision
// pairs with the decoder's last fully rolled revision.
func TestResolvePeerRevisions_PairsTargetWithTargetAndOthersWithCurrent(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	r, _ := newReconciler(t, engine, decoder, pdParent())

	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2"))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if hold != "" {
		t.Fatalf("fresh peer must not hold, got %q", hold)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "d2" {
		t.Errorf("target-revision engine pod must pair with the decoder target d2, got %q", got)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d1" {
		t.Errorf("old-revision engine pod must pair with the decoder's current d1, got %q", got)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, ""); got != "d2" {
		t.Errorf("empty pod hash pairs as target, got %q", got)
	}
	if got := peers.hashFor(v1beta1.RouterComponent, "e2"); got != "" {
		t.Errorf("undeclared peer must resolve to no revision, got %q", got)
	}
	var fn coordination.PeerRevisionFunc = peers.hashFor
	if fn == nil {
		t.Error("hashFor must satisfy coordination.PeerRevisionFunc")
	}
}

// TestResolvePeerRevisions_HoldsWhilePeerLags pins the two lag guards: a
// peer whose projection reflects another parent generation, and a peer whose
// status has not observed its own generation, both hold rendering.
func TestResolvePeerRevisions_HoldsWhilePeerLags(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "8", "e1", "e2")

	stale := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
	r, _ := newReconciler(t, engine, stale, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("projection lag must hold: hold=%q err=%v", hold, err)
	}

	unobserved := projectedIR(v1beta1.DecoderComponent, "8", "d1", "d1")
	unobserved.Status.ObservedGeneration = unobserved.Generation - 1
	r, _ = newReconciler(t, engine, unobserved, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("status lag must hold: hold=%q err=%v", hold, err)
	}

	noTarget := projectedIR(v1beta1.DecoderComponent, "8", "", "")
	r, _ = newReconciler(t, engine, noTarget, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("peer without a reported target must hold: hold=%q err=%v", hold, err)
	}
}

// TestResolvePeerRevisions_MissingPeerIRResolvesNothing pins that a declared
// peer with no IR (not IR-managed) neither holds nor yields a revision.
func TestResolvePeerRevisions_MissingPeerIRResolvesNothing(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	r, _ := newReconciler(t, engine, pdParent())
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("missing peer IR must not hold: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "" {
		t.Errorf("missing peer IR resolves to no revision, got %q", got)
	}
}

// Beside an encoder every role is an OMENative replica that one service pass
// projects, so a peer replica that does not exist yet holds a Component's
// first pods: rendered now, they would lack that peer's revision endpoint for
// life. A Component that already has Instances keeps rendering, and so does
// one whose missing peer is not OMENative.
func TestResolvePeerRevisions_MissingPeerHoldsFirstPodsBesideAnEncoder(t *testing.T) {
	epd := pdParent()
	epd.Spec.DeploymentMode = ptr.To(constants.OMENative)
	epd.Spec.Encoder = &v1beta1.EncoderSpec{}
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	r, _ := newReconciler(t, engine, decoder, epd)

	_, hold, err := r.resolvePeerRevisions(context.Background(), engine, epd, ownTargetCR("llama-engine-e2"))
	if err != nil || !strings.Contains(hold, "encoder IR is not projected yet") {
		t.Fatalf("first pods beside a missing encoder replica must hold: hold=%q err=%v", hold, err)
	}

	running := engine.DeepCopy()
	running.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	peers, hold, err := r.resolvePeerRevisions(context.Background(), running, epd, ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("a Component with Instances keeps rendering: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "d2" {
		t.Errorf("the decoder still pairs on its roll target d2, got %q", got)
	}

	raw := epd.DeepCopy()
	raw.Spec.Encoder.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, raw, ownTargetCR("llama-engine-e2")); err != nil || hold != "" {
		t.Fatalf("a missing peer that is not OMENative never holds: hold=%q err=%v", hold, err)
	}
}

// TestResolvePeerRevisions_RollbackPinsPeerTarget pins that a peer rolling
// back pairs on its rollback revision while that revision exists, and on
// its spec target once the rollback revision is gone.
func TestResolvePeerRevisions_RollbackPinsPeerTarget(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	decoder.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("llama-decoder-d1")}
	rollback := ownTargetCR("llama-decoder-d1")
	rollback.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(decoder, irGVK)}

	r, _ := newReconciler(t, engine, decoder, pdParent(), rollback)
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d1" {
		t.Errorf("rollback in flight pairs with the rollback revision d1, got %q", got)
	}

	r, _ = newReconciler(t, engine, decoder, pdParent())
	peers, hold, err = r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d2" {
		t.Errorf("swept rollback revision falls through to the spec target d2, got %q", got)
	}
}

// TestResolvePeerRevisions_ForeignRollbackRevisionPairsOnTheSpecTarget pins
// that a peer pinned to a rollback revision it does not control (another
// replica controls it, or nothing does) pairs on its spec target, since the
// peer's own reconciler ignores that revision.
func TestResolvePeerRevisions_ForeignRollbackRevisionPairsOnTheSpecTarget(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	decoder.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("pool-b-decoder-d1")}
	foreign := ownTargetCR("pool-b-decoder-d1")
	foreign.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(baselineIR("pool-b-decoder", "default", 1), irGVK)}

	r, _ := newReconciler(t, engine, decoder, pdParent(), foreign)
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d2" {
		t.Errorf("a rollback revision the peer does not control pairs on the peer's spec target d2, got %q", got)
	}

	ownerless := decoder.DeepCopy()
	ownerless.Spec.Pacing.RollbackToRevision = ptr.To("llama-decoder-d1")
	r, _ = newReconciler(t, engine, ownerless, pdParent(), ownTargetCR("llama-decoder-d1"))
	peers, hold, err = r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d2" {
		t.Errorf("a rollback revision with no owner references pairs on the peer's spec target d2, got %q", got)
	}
}

// TestEnsurePeerRevisionServices_CreatesPeerTargetServicesBeforePods pins
// that the engine pass creates the decoder target revision's Services —
// selected by that revision hash only — with no decoder pod in existence.
func TestEnsurePeerRevisionServices_CreatesPeerTargetServicesBeforePods(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	decoder.Spec.Runners[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}}
	parent := pdParent()
	r, c := newReconciler(t, engine, decoder, parent)

	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, parent, ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if err := r.ensurePeerRevisionServices(context.Background(), parent, peers); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	names := coordination.PerRevisionServiceNames(parent.Name, v1beta1.DecoderComponent, "d2")
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		svc := &corev1.Service{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: parent.Namespace, Name: name}, svc); err != nil {
			t.Fatalf("decoder target Service %s must exist before any decoder pod: %v", name, err)
		}
		if svc.Spec.Selector[query.LabelRevisionHash] != "d2" || svc.Spec.Selector[constants.OMEComponentLabel] != string(v1beta1.DecoderComponent) {
			t.Errorf("Service %s must select only decoders of revision d2, got %v", name, svc.Spec.Selector)
		}
	}
	// The engine's own endpoint for the rendered pod names exactly that Service.
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); coordination.PerRevisionServiceName(parent.Name, v1beta1.DecoderComponent, got) != names.RoutingName {
		t.Errorf("rendered engine pod pairs with %q, Service created for d2", got)
	}
	if err := r.ensurePeerRevisionServices(context.Background(), parent, nil); err != nil {
		t.Errorf("nil peers must be a no-op, got %v", err)
	}
}

// hashedIR is projectedIR carrying the template hash the projector writes
// beside the spec and a status that repeats it, the shape of every
// projected replica.
func hashedIR(component v1beta1.ComponentType, parentGeneration, currentHash, updateHash string) *v1beta1.InferenceReplica {
	ir := projectedIR(component, parentGeneration, currentHash, updateHash)
	ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey] = v1beta1convert.TemplateHash(ir, ir.Spec.Runners)
	ir.Status.ObservedTemplateHash = ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
	return ir
}

// TestResolvePeerRevisions_FreshnessFollowsTheTemplatesNotTheGeneration locks
// in what makes a peer's status stale: the pod templates its spec holds, not
// its generation. A decoder whose generation moved three times for writes
// outside its templates (an annotation value the revision leaves out, a
// count) with its status three generations behind is read as fresh and
// pairs on the target its status names. A decoder whose templates changed
// (a new image, a new hash) while its status still repeats the previous
// hash holds the engine's pass, even with its generation observed.
func TestResolvePeerRevisions_FreshnessFollowsTheTemplatesNotTheGeneration(t *testing.T) {
	engine := hashedIR(v1beta1.EngineComponent, "7", "e1", "e2")

	rewritten := hashedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	rewritten.Generation += 3
	r, _ := newReconciler(t, engine, rewritten, pdParent())
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("a peer whose generation moved with no template change must not hold: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "d2" {
		t.Errorf("the fresh peer pairs on its recorded target d2, got %q", got)
	}

	retemplated := hashedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
	retemplated.Spec.Runners[0].Template.Spec.Containers[0].Image = "sgl:2.0"
	retemplated.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey] = v1beta1convert.TemplateHash(retemplated, retemplated.Spec.Runners)
	if retemplated.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey] == retemplated.Status.ObservedTemplateHash {
		t.Fatal("a new image must move the template hash")
	}
	r, _ = newReconciler(t, engine, retemplated, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("a peer whose status predates its templates must hold: hold=%q err=%v", hold, err)
	}
}

// A writer that patches an annotation on the InferenceService twice a second
// must cost a roll nothing, with or without a rollout group. This file drives
// the real replica pass over a 4:4 prefill/decode roll under RecreatePod while
// such a writer runs and counts the passes each roll needs, what held them,
// how many status publications were refused and how many generations moved.
//
// The writer and the projector are stand-ins. A service annotation lands on
// every replica what the production render puts in a pod template for a key
// no Component declares (render.TemplateObjectMeta), and a replica whose
// spec did not change is not written. A second kind of tick is a spec write
// outside the pod templates (a count or a pacing field the projector
// writes again): the generation advances and the templates stand, which is the
// churn the peer freshness readers must tolerate. The kubelet and the
// endpoint controller are stand-ins too: a pod becomes ContainersReady one
// round after it appears, PodReady once the serving gate is open, and its
// EndpointSlice follows PodReady. The gates, the status writers, the peer
// freshness hold and the engine are production code.

// churnWriterKey is the annotation the competing writer sets on the service;
// the projector lists it among the keys the revision hash leaves out, and the
// render keeps it off every runner template.
const churnWriterKey = "editor.example.com/write-counter"

// churnWrite is what one tick of the competing writer changes.
type churnWrite int

const (
	// churnWriteServiceAnnotation patches an annotation on the service that
	// no Component declares; the replicas receive what the production render
	// puts in a pod template for it.
	churnWriteServiceAnnotation churnWrite = iota
	// churnWriteOutsideTemplates is a spec write that changes no pod template:
	// the generation advances and the templates stand.
	churnWriteOutsideTemplates
)

// serviceAnnotationOnTemplate is what the production render puts in a pod
// template for the writer's key at value: the template rule of
// render.TemplateObjectMeta over a component that declares nothing.
func serviceAnnotationOnTemplate(value string) map[string]string {
	return render.TemplateObjectMeta(metav1.ObjectMeta{Annotations: map[string]string{churnWriterKey: value}}, nil).Annotations
}

type churnShape struct {
	name       string
	grouped    bool
	sequential bool
	withRouter bool
	// pushOnly names the one Component the push edits; empty pushes the
	// engine and the decoder.
	pushOnly v1beta1.ComponentType
}

type churnRegime struct {
	name  string
	write churnWrite
	// period is the writer's period in model time; 0 means no writer.
	period time.Duration
	// longPass is the model duration of a pass that writes, drains, promotes
	// or asks the gates; shortPass that of a pass that only looks at a
	// starting pod, holds at the peer check or finds nothing to do.
	longPass, shortPass time.Duration
}

type churnOutcome struct {
	rounds, passes, held, refused, writes, generationMoves int
	heldByKey                                              map[string]int
	done                                                   bool
}

// churnModel owns the fake apiserver, the three stand-ins and the model
// clock, and runs passes of the real Reconciler one replica at a time.
type churnModel struct {
	t        *testing.T
	ctx      context.Context
	r        *Reconciler
	c        client.Client
	parent   *v1beta1.InferenceService
	keys     []client.ObjectKey
	pushed   map[client.ObjectKey]string // key -> the revision the push leaves behind
	pushOnly v1beta1.ComponentType

	regime    churnRegime
	clock     time.Duration
	nextWrite time.Duration
	writeSeq  int

	inPass              bool
	passKey             client.ObjectKey
	passInitialRead     bool
	passWrote           bool
	passListedSlices    bool
	midPassWriteDue     bool
	midPassWriteApplied bool
	trace               []string

	podSeenRounds map[string]int
	out           churnOutcome
}

// TestGroupedRollUnderServiceAnnotationChurn measures a 4:4 RecreatePod roll
// under a writer that rewrites a service annotation twice a second, with and
// without a rollout group, and under a spec write outside the pod templates
// at the same cadence. The service annotation reaches no pod template, so it
// moves no generation and the roll takes exactly the passes it takes without
// the writer. The spec write moves the generation and changes no template, so
// a peer's status stays current for the roll target it records and no pass
// may be held on it: with or without a group, the roll takes the same passes
// as without the writer, and a pass that spans a tick keeps its status
// publication, since the spec changed in nothing the pass read.
func TestGroupedRollUnderServiceAnnotationChurn(t *testing.T) {
	shapes := []churnShape{
		{name: "no-group", grouped: false},
		{name: "rollingUpdate-group", grouped: true},
		{name: "rollingUpdate-group-router-outside", grouped: true, withRouter: true},
		{name: "sequential-engine-then-decoder", grouped: true, sequential: true},
		{name: "sequential-decoder-alone-pushed", grouped: true, sequential: true, pushOnly: v1beta1.DecoderComponent},
	}
	regimes := []churnRegime{
		{name: "writer-off", period: 0, longPass: 250 * time.Millisecond, shortPass: 50 * time.Millisecond},
		{name: "service-annotation-500ms-long-pass-250ms", write: churnWriteServiceAnnotation, period: 500 * time.Millisecond, longPass: 250 * time.Millisecond, shortPass: 50 * time.Millisecond},
		{name: "service-annotation-500ms-long-pass-600ms", write: churnWriteServiceAnnotation, period: 500 * time.Millisecond, longPass: 600 * time.Millisecond, shortPass: 50 * time.Millisecond},
		{name: "spec-write-outside-templates-500ms-long-pass-250ms", write: churnWriteOutsideTemplates, period: 500 * time.Millisecond, longPass: 250 * time.Millisecond, shortPass: 50 * time.Millisecond},
		{name: "spec-write-outside-templates-500ms-long-pass-600ms", write: churnWriteOutsideTemplates, period: 500 * time.Millisecond, longPass: 600 * time.Millisecond, shortPass: 50 * time.Millisecond},
	}
	const maxRounds = 300

	results := map[string]churnOutcome{}
	for _, shape := range shapes {
		for _, regime := range regimes {
			name := shape.name + "/" + regime.name
			t.Run(name, func(t *testing.T) {
				m := newChurnModel(t, shape, regime)
				m.push()
				out := m.run(maxRounds)
				results[name] = out
				t.Logf("%s: done=%v rounds=%d passes=%d held-at-peer-check=%d refused-publications=%d writer-ticks=%d generation-moves=%d held-by-replica=%v",
					name, out.done, out.rounds, out.passes, out.held, out.refused, out.writes, out.generationMoves, out.heldByKey)
				t.Logf("%s: first passes: %s", name, strings.Join(m.trace, " "))
			})
		}
	}

	baseline := results["no-group/writer-off"]
	if !baseline.done {
		t.Fatalf("the ungrouped roll must finish without a writer; got %+v", baseline)
	}
	for _, regime := range regimes[1:] {
		got := results["no-group/"+regime.name]
		if !got.done || got.held != 0 || got.passes > baseline.passes*11/10 {
			t.Errorf("no group, %s: the writer must not hold a pass (held=%d) nor slow the roll (%d passes against %d) and the roll must finish (done=%v)",
				regime.name, got.held, got.passes, baseline.passes, got.done)
		}
	}
	// Neither tick changes a pod, so a grouped roll must take the same passes
	// with the writer as without it, as the ungrouped roll does.
	for _, shape := range shapes[1:] {
		own := results[shape.name+"/writer-off"]
		if !own.done {
			t.Fatalf("%s: the roll must finish without a writer; got %+v", shape.name, own)
		}
		for _, regime := range regimes[1:] {
			name := shape.name + "/" + regime.name
			got := results[name]
			if !got.done {
				t.Errorf("%s: the roll did not finish within %d rounds (passes=%d, held at the peer check=%d, refused publications=%d)",
					name, maxRounds, got.passes, got.held, got.refused)
				continue
			}
			if got.passes > own.passes*11/10 {
				t.Errorf("%s: the writer slowed the roll from %d to %d passes (held at the peer check=%d, refused publications=%d)",
					name, own.passes, got.passes, got.held, got.refused)
			}
		}
	}
	// A service annotation no Component declares lands nothing on a replica:
	// no generation moves and the roll takes exactly the passes it takes
	// without the writer.
	for _, shape := range shapes {
		own := results[shape.name+"/writer-off"]
		for _, regime := range regimes {
			if regime.period == 0 || regime.write != churnWriteServiceAnnotation {
				continue
			}
			got := results[shape.name+"/"+regime.name]
			if got.generationMoves != 0 || got.passes != own.passes {
				t.Errorf("%s/%s: the service annotation moved %d generation(s) and the roll took %d passes against %d without the writer",
					shape.name, regime.name, got.generationMoves, got.passes, own.passes)
			}
		}
	}
}

func newChurnModel(t *testing.T, shape churnShape, regime churnRegime) *churnModel {
	t.Helper()
	m := &churnModel{
		t:             t,
		ctx:           context.Background(),
		regime:        regime,
		pushed:        map[client.ObjectKey]string{},
		podSeenRounds: map[string]int{},
	}
	m.out.heldByKey = map[string]int{}

	parent := migrationParent(nil, true)
	parent.Spec.Engine = &v1beta1.EngineSpec{}
	parent.Generation = 1
	if shape.withRouter {
		parent.Spec.Router = &v1beta1.RouterSpec{}
	}
	if shape.grouped {
		parent.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		}}}
		if shape.sequential {
			parent.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
				{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
				{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			}}
		}
		pinRun(parent)
	}
	m.pushOnly = shape.pushOnly
	m.parent = parent

	objs := []client.Object{parent}
	components := []struct {
		component v1beta1.ComponentType
		replicas  int32
	}{{v1beta1.EngineComponent, 4}, {v1beta1.DecoderComponent, 4}}
	if shape.withRouter {
		components = append(components, struct {
			component v1beta1.ComponentType
			replicas  int32
		}{v1beta1.RouterComponent, 2})
	}
	for _, c := range components {
		ir := churnReplica(c.component, c.replicas)
		rev := targetRevisionNameFor(t, ir)
		objs = append(objs, ir)
		objs = append(objs, churnSeedReady(ir, rev)...)
		m.keys = append(m.keys, client.ObjectKeyFromObject(ir))
	}

	// A write to a pod or a revision marks the pass as being in its long
	// part; a status write on the replica itself is one quick step and does
	// not.
	wrote := func(obj client.Object) {
		if _, replica := obj.(*v1beta1.InferenceReplica); m.inPass && !replica {
			m.passWrote = true
		}
	}
	funcs := interceptor.Funcs{
		// A writer tick that falls inside a pass lands once the pass is in
		// its long part, after a pod or revision write or the publication's
		// slice list, at the next live re-read of the pass's own replica.
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1beta1.InferenceReplica); ok && m.inPass && key == m.passKey {
				if !m.passInitialRead {
					m.passInitialRead = true
				} else if m.midPassWriteDue && !m.midPassWriteApplied && (m.passWrote || m.passListedSlices) {
					m.midPassWriteApplied = true
					m.applyWrite(cl)
				}
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*discoveryv1.EndpointSliceList); ok && m.inPass {
				m.passListedSlices = true
			}
			return cl.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			wrote(obj)
			return cl.Create(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			wrote(obj)
			return cl.Delete(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			wrote(obj)
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			wrote(obj)
			return cl.Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			wrote(obj)
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			wrote(obj)
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		WithIndex(&v1beta1.InferenceReplica{}, irUIDIndexField, irUIDIndexExtractor).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithIndex(&corev1.Node{}, query.NodeUnschedulableField, query.NodeUnschedulableIndexExtractor).
		WithInterceptorFuncs(funcs).
		Build()
	m.c = c
	m.r = &Reconciler{
		Client:                   c,
		APIReader:                c,
		Log:                      m.holdCountingLogger(),
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workloadtypes.NewExpectations(),
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
	}
	return m
}

// holdCountingLogger counts the pass-wide hold the peer check writes at V(1).
func (m *churnModel) holdCountingLogger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		if strings.Contains(args, "Holding dispatch until peer revisions are resolvable") {
			m.out.held++
			m.out.heldByKey[m.passKey.Name]++
		}
	}, funcr.Options{Verbosity: 1})
}

// churnReplica is one single-pod Component of the parent "llama" rolling
// under RecreatePod one Instance at a time, projected for parent generation
// 1 with the writer's key recorded as excluded from the revision hash.
func churnReplica(component v1beta1.ComponentType, replicas int32) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-"+string(component), "default", replicas)
	ir.Spec.Component = component
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type:          v1beta1.UpdateStrategyRecreatePod,
			RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt32(1))},
		},
		// The readiness window the chart configures; without one every pass
		// writes a warning condition before the push's first record.
		InstanceReadyTimeout: &metav1.Duration{Duration: 3 * time.Minute},
	}
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "1"
	ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey] = churnWriterKey
	ir.Spec.Runners[0].Template.Annotations = serviceAnnotationOnTemplate("0")
	writeTemplateHash(ir)
	return ir
}

// writeTemplateHash writes the projector's hash of the pod templates, as
// with every projection.
func writeTemplateHash(ir *v1beta1.InferenceReplica) {
	ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey] = v1beta1convert.TemplateHash(ir, ir.Spec.Runners)
}

// churnSeedReady records every Instance Ready on rev with a serving pod and
// its endpoint, the settled state before the push.
func churnSeedReady(ir *v1beta1.InferenceReplica, rev string) []client.Object {
	ir.Status.CurrentRevision = rev
	ir.Status.UpdateRevision = rev
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.ObservedTemplateHash = ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
	var objs []client.Object
	for idx := int32(0); idx < *ir.Spec.Replicas; idx++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: idx, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: rev, PodCount: 1, ServingPodCount: 1,
		})
		pod := podForIR(ir, idx, string(v1beta1.RunnerNameDefault), 0, true, true)
		pod.Labels[query.LabelRevisionHash] = query.RevisionFromName(rev).Hash()
		objs = append(objs, pod, sliceForIRPod(ir, pod, true))
	}
	return objs
}

// push edits the engine's and the decoder's templates the way one service
// edit does: a new image on both, the parent generation advanced and
// recorded on every replica.
func (m *churnModel) push() {
	m.t.Helper()
	live := &v1beta1.InferenceService{}
	if err := m.c.Get(m.ctx, client.ObjectKeyFromObject(m.parent), live); err != nil {
		m.t.Fatalf("get parent: %v", err)
	}
	live.Generation = 2
	if err := m.c.Update(m.ctx, live); err != nil {
		m.t.Fatalf("update parent: %v", err)
	}
	for _, key := range m.keys {
		ir := &v1beta1.InferenceReplica{}
		if err := m.c.Get(m.ctx, key, ir); err != nil {
			m.t.Fatalf("get %s: %v", key.Name, err)
		}
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
		edited := ir.Spec.Component != v1beta1.RouterComponent
		if m.pushOnly != "" {
			edited = ir.Spec.Component == m.pushOnly
		}
		if edited {
			m.pushed[key] = ir.Status.UpdateRevision
			ir.Spec.Runners[0].Template.Spec.Containers[0].Image = "sgl:2.0"
		}
		writeTemplateHash(ir)
		ir.Generation++
		if err := m.c.Update(m.ctx, ir); err != nil {
			m.t.Fatalf("push %s: %v", key.Name, err)
		}
	}
	if m.regime.period > 0 {
		m.nextWrite = m.regime.period
	}
}

// applyWrite is one tick of the writer as the projector lands it. A service
// annotation lands what the production render keeps of it in the template,
// and a replica whose spec did not change is not written; a spec write
// outside the templates advances the generation with the templates standing.
func (m *churnModel) applyWrite(cl client.Client) {
	m.writeSeq++
	m.out.writes++
	for _, key := range m.keys {
		ir := &v1beta1.InferenceReplica{}
		if err := cl.Get(m.ctx, key, ir); err != nil {
			m.t.Fatalf("writer tick: get %s: %v", key.Name, err)
		}
		if m.regime.write == churnWriteServiceAnnotation && !m.landServiceAnnotation(ir) {
			continue
		}
		writeTemplateHash(ir)
		ir.Generation++
		m.out.generationMoves++
		if err := cl.Update(m.ctx, ir); err != nil {
			m.t.Fatalf("writer tick: update %s: %v", key.Name, err)
		}
	}
}

// landServiceAnnotation sets every runner template of ir to what the render
// keeps of the writer's current value and reports whether any template
// changed.
func (m *churnModel) landServiceAnnotation(ir *v1beta1.InferenceReplica) bool {
	value, kept := serviceAnnotationOnTemplate(strconv.Itoa(m.writeSeq))[churnWriterKey]
	changed := false
	for i := range ir.Spec.Runners {
		annotations := ir.Spec.Runners[i].Template.Annotations
		if had, ok := annotations[churnWriterKey]; ok == kept && had == value {
			continue
		}
		if annotations == nil {
			annotations = map[string]string{}
			ir.Spec.Runners[i].Template.Annotations = annotations
		}
		if kept {
			annotations[churnWriterKey] = value
		} else {
			delete(annotations, churnWriterKey)
		}
		changed = true
	}
	return changed
}

func (m *churnModel) run(maxRounds int) churnOutcome {
	m.t.Helper()
	for round := 1; round <= maxRounds; round++ {
		m.out.rounds = round
		for _, key := range m.keys {
			m.runPass(key)
		}
		m.kubeletAndEndpointsAct()
		if m.allDone() {
			m.out.done = true
			return m.out
		}
	}
	return m.out
}

// runPass runs one pass of the real Reconciler over key. The pass's model
// duration follows its predicted shape; a writer tick that falls inside it
// lands at the pass's first live re-read of its own replica, before any
// generation-checked status write.
func (m *churnModel) runPass(key client.ObjectKey) {
	m.t.Helper()
	held, long := m.predictPassShape(key)
	d := m.regime.shortPass
	if long && !held {
		d = m.regime.longPass
	}
	m.midPassWriteDue = m.regime.period > 0 && m.nextWrite > m.clock && m.nextWrite < m.clock+d
	m.midPassWriteApplied = false
	m.passInitialRead = false
	m.passWrote = false
	m.passListedSlices = false
	m.passKey = key
	before := m.replica(key)
	m.inPass = true
	m.r.Expectations = workloadtypes.NewExpectations() // the pod watch has confirmed the last pass's writes
	if _, err := m.r.Reconcile(m.ctx, ctrl.Request{NamespacedName: key}); err != nil {
		m.t.Fatalf("pass over %s: %v", key.Name, err)
	}
	m.inPass = false
	m.out.passes++
	heldBefore := m.out.held
	m.clock += d
	refused := false
	if m.midPassWriteApplied {
		m.nextWrite += m.regime.period
		after := m.replica(key)
		refused = after.Status.ObservedGeneration < after.Generation && after.Generation > before.Generation
		if refused {
			m.out.refused++
		}
	}
	if len(m.trace) < 40 {
		shape := "short"
		if long && !held {
			shape = "long"
		}
		m.trace = append(m.trace, fmt.Sprintf("%s:%s%s%s", strings.TrimPrefix(key.Name, "llama-"), shape,
			map[bool]string{true: ",held", false: ""}[m.out.held > heldBefore],
			map[bool]string{true: ",refused", false: ""}[refused]))
	}
	for m.regime.period > 0 && m.nextWrite <= m.clock {
		m.applyWrite(m.c)
		m.nextWrite += m.regime.period
	}
}

// predictPassShape reads the stored state the way the pass will: held when a
// serving peer's status predates the pod templates its spec holds or its
// projection records another parent generation (the peer check ends the pass
// there); long when the engine will write,
// drain, promote or ask a gate; short when every open operation only waits
// for its pod to become ContainersReady, or nothing is left to do.
func (m *churnModel) predictPassShape(key client.ObjectKey) (held bool, long bool) {
	ir := m.replica(key)
	if coordination.PeerEnvDeclared(m.parent) {
		own := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
		for _, peer := range coordination.ServingPeers(m.parent, ir.Spec.Component) {
			peerIR := m.replica(client.ObjectKey{Namespace: ir.Namespace, Name: "llama-" + string(peer)})
			if peerIR == nil {
				continue
			}
			if peerIR.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != own ||
				!coordination.StatusObservedTemplates(peerIR) || peerIR.Status.UpdateRevision == "" {
				return true, false
			}
		}
	}
	pods := m.podsOf(ir)
	open := 0
	waiting := 0
	offTarget := false
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		if row.Operation != nil {
			open++
			var current *corev1.Pod
			oldPresent := false
			for _, pod := range pods[row.Index] {
				if pod.Labels[query.LabelInstanceIncarnation] == strconv.FormatInt(int64(row.Incarnation), 10) {
					current = pod
				} else {
					oldPresent = true
				}
			}
			if current != nil && !oldPresent && !podreadiness.IsContainersReady(current) {
				waiting++
			}
			continue
		}
		if row.RunningRevision != ir.Status.UpdateRevision || row.Phase != v1beta1.OMENativeInstanceReady {
			offTarget = true
		}
	}
	if _, pushed := m.pushed[key]; pushed && ir.Status.UpdateRevision == m.pushed[key] {
		offTarget = true // the push is not recorded yet; the pass records it
	}
	if open > 0 {
		return false, waiting < open
	}
	return false, offTarget
}

func (m *churnModel) replica(key client.ObjectKey) *v1beta1.InferenceReplica {
	m.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := m.c.Get(m.ctx, key, ir); err != nil {
		m.t.Fatalf("get %s: %v", key.Name, err)
	}
	return ir
}

func (m *churnModel) podsOf(ir *v1beta1.InferenceReplica) map[int32][]*corev1.Pod {
	m.t.Helper()
	list := &corev1.PodList{}
	if err := m.c.List(m.ctx, list, client.InNamespace(ir.Namespace), client.MatchingLabels{
		constants.InferenceServicePodLabelKey: ir.NamePrefix(),
		constants.OMEComponentLabel:           string(ir.Spec.Component),
	}); err != nil {
		m.t.Fatalf("list pods of %s: %v", ir.Name, err)
	}
	out := map[int32][]*corev1.Pod{}
	for i := range list.Items {
		pod := &list.Items[i]
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			continue
		}
		out[int32(idx)] = append(out[int32(idx)], pod)
	}
	return out
}

// kubeletAndEndpointsAct is the kubelet and the endpoint controller between
// two rounds: a pod seen in an earlier round becomes ContainersReady, a pod
// whose serving gate is open becomes PodReady, and its EndpointSlice carries
// PodReady; the slice of a pod that is gone goes with it.
func (m *churnModel) kubeletAndEndpointsAct() {
	m.t.Helper()
	pods := &corev1.PodList{}
	if err := m.c.List(m.ctx, pods, client.InNamespace("default")); err != nil {
		m.t.Fatalf("list pods: %v", err)
	}
	live := map[string]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		live[pod.Name] = true
		if pod.DeletionTimestamp != nil {
			continue
		}
		changed := false
		if !podreadiness.IsContainersReady(pod) {
			if m.podSeenRounds[pod.Name] >= 1 {
				setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue)
				changed = true
			} else {
				m.podSeenRounds[pod.Name]++
			}
		}
		wantReady := podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)
		if podreadiness.IsPodReady(pod) != wantReady {
			status := corev1.ConditionFalse
			if wantReady {
				status = corev1.ConditionTrue
			}
			setPodCondition(pod, corev1.PodReady, status)
			changed = true
		}
		if changed {
			if err := m.c.Status().Update(m.ctx, pod); err != nil {
				m.t.Fatalf("kubelet: update pod %s: %v", pod.Name, err)
			}
		}
		m.ensureSlice(pod, wantReady)
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := m.c.List(m.ctx, slices, client.InNamespace("default")); err != nil {
		m.t.Fatalf("list slices: %v", err)
	}
	for name := range m.podSeenRounds {
		if !live[name] {
			delete(m.podSeenRounds, name)
		}
	}
	for i := range slices.Items {
		slice := &slices.Items[i]
		if len(slice.Endpoints) == 1 && slice.Endpoints[0].TargetRef != nil && !live[slice.Endpoints[0].TargetRef.Name] {
			if err := m.c.Delete(m.ctx, slice); err != nil && !apierrors.IsNotFound(err) {
				m.t.Fatalf("endpoints: delete slice %s: %v", slice.Name, err)
			}
		}
	}
}

func (m *churnModel) ensureSlice(pod *corev1.Pod, ready bool) {
	m.t.Helper()
	component := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
	ir := m.replica(client.ObjectKey{Namespace: pod.Namespace, Name: "llama-" + string(component)})
	want := sliceForIRPod(ir, pod, ready)
	got := &discoveryv1.EndpointSlice{}
	err := m.c.Get(m.ctx, client.ObjectKeyFromObject(want), got)
	switch {
	case apierrors.IsNotFound(err):
		if err := m.c.Create(m.ctx, want); err != nil {
			m.t.Fatalf("endpoints: create slice for %s: %v", pod.Name, err)
		}
	case err != nil:
		m.t.Fatalf("endpoints: get slice for %s: %v", pod.Name, err)
	case ptr.Deref(got.Endpoints[0].Conditions.Ready, false) != ready:
		got.Endpoints[0].Conditions.Ready = ptr.To(ready)
		if err := m.c.Update(m.ctx, got); err != nil {
			m.t.Fatalf("endpoints: update slice for %s: %v", pod.Name, err)
		}
	}
}

func setPodCondition(pod *corev1.Pod, kind corev1.PodConditionType, status corev1.ConditionStatus) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == kind {
			if pod.Status.Conditions[i].Status != status {
				pod.Status.Conditions[i].Status = status
				pod.Status.Conditions[i].LastTransitionTime = metav1.Now()
			}
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: kind, Status: status, LastTransitionTime: metav1.Now()})
}

// allDone reports whether every pushed Component runs the new revision on
// every Instance with nothing in flight and its revision pair equal.
func (m *churnModel) allDone() bool {
	for key, old := range m.pushed {
		ir := m.replica(key)
		if ir.Status.UpdateRevision == "" || ir.Status.UpdateRevision == old || ir.Status.CurrentRevision != ir.Status.UpdateRevision {
			return false
		}
		if len(ir.Status.InstanceStatuses) != int(*ir.Spec.Replicas) {
			return false
		}
		for i := range ir.Status.InstanceStatuses {
			row := &ir.Status.InstanceStatuses[i]
			if row.Operation != nil || row.Phase != v1beta1.OMENativeInstanceReady || row.RunningRevision != ir.Status.UpdateRevision {
				return false
			}
		}
	}
	return true
}

// String renders the outcome for a log line.
func (o churnOutcome) String() string {
	return fmt.Sprintf("done=%v rounds=%d passes=%d held=%d refused=%d writes=%d", o.done, o.rounds, o.passes, o.held, o.refused, o.writes)
}
