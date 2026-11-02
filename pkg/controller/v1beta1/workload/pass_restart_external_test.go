package workload_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	goruntime "runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the real dispatcher (workload.Reconcile with its
// restart pass, its escalation pass and the retry record) over the
// recovery harness's kubelet, scheduler, node and endpoint models, and
// explores what the engine does when pods break: a runner that exits once
// or on every run, an image that cannot be pulled, a pod whose node is
// lost or whose kubelet stops reporting, a gang short of a member, a pod
// deleted by hand, a pod that never finishes terminating, a cluster with
// no room, a push of a revision that crashes, a move of an Instance; each
// at every moment of a pod's life, with the clock advanced past every
// deadline and grace the operator configured.
//
// Every configuration is explored breadth-first to a bound with a hash of
// visited states, then played through by seeded runs that reach the
// depths the walk does not. At every state the explorer checks that a
// repair is never opened on a promoted set without a failure record
// naming its runner, that no pod of a revision whose retry record holds
// is built afterwards, that a gang under RecreateInstanceOnPodRestart is
// never rebuilt one member at a time, that the engine never errors, and
// that the pod churn stays bounded. At every state the engine has settled
// in, with no deadline or grace left to run, every Instance must either
// serve or name the reason it does not: a Failed row with its cause, a
// wait token on its operation, a hold on the Component, or an event that
// names it. Two pictures the model found are held by lock tests:
// TestRepair_AWaitDuringARebuildKeepsTheRunnersExitRecord holds that a
// wait landing on a repair keeps the runner's exit record, and
// TestRepair_AHeldRepairDoesNotStopTheForceDeleteOfALostNode holds that a
// held repair does not stop the force delete of another Instance's lost
// node; the explorer counts both pictures as known classes.
// OME_REPAIR_HUNT=full widens the sample and the bounds;
// OME_REPAIR_HUNT_ONLY=<text> keeps the configurations whose name carries
// the text, to replay one finding.
//
// The operator's values go in the way the controller loads them: written
// as a controllerconfig.LifecycleConfig, the shape of the
// inferenceservice-config ConfigMap, and parsed by its own loaders; the
// test never sets a duration or a count on the engine's input directly.

// repairKnobs is one set of the operator's lifecycle values: which
// deadline set, how many tries the retry schedule allows, whether its
// delays are the chart's or short ones, and whether force delete is on.
type repairKnobs struct {
	deadlines   string
	tries       int32
	shortDelays bool
	forceDelete bool
}

func (k repairKnobs) String() string {
	s := fmt.Sprintf("%s/tries=%d", k.deadlines, k.tries)
	if k.shortDelays {
		s += "/short-delays"
	}
	if k.forceDelete {
		s += "/force-delete"
	}
	return s
}

// lifecycleConfig is the ConfigMap block these knobs stand for. The chart
// set carries the values charts/ome-resources/values.yaml ships; the
// short set is a short-deadline configuration. Force delete uses the
// values the chart's commented block documents, or short ones under the
// short set.
func (k repairKnobs) lifecycleConfig() controllerconfig.LifecycleConfig {
	repairBatch := int32(10)
	cfg := controllerconfig.LifecycleConfig{
		RepairBatchSize: &repairBatch,
		Requeue:         &controllerconfig.RequeueConfig{Operation: "5s", Gate: "3s"},
		Audit:           &controllerconfig.AuditConfig{MaxInFlightMigrations: 3, MaxMigrationsPerWindow: 10, Window: "1h"},
	}
	switch k.deadlines {
	case "short":
		cfg.InstanceReadyTimeout, cfg.StuckPodGracePeriod = "3m", "20s"
		cfg.UnschedulableGracePeriod, cfg.AbandonedReplacementGracePeriod = "2m", "10s"
		if k.forceDelete {
			cfg.ForceDelete = &controllerconfig.ForceDeleteConfig{OverdueSlack: "90s", NodeUnreachableThreshold: "30s"}
		}
	default:
		cfg.InstanceReadyTimeout, cfg.StuckPodGracePeriod = "30m", "60s"
		cfg.UnschedulableGracePeriod, cfg.AbandonedReplacementGracePeriod = "15m", "30s"
		if k.forceDelete {
			cfg.ForceDelete = &controllerconfig.ForceDeleteConfig{OverdueSlack: "2m", NodeUnreachableThreshold: "5m"}
		}
	}
	retry := &controllerconfig.UpdateRetryConfig{MaxAttempts: k.tries, InitialDelay: "1m", MaxDelay: "30m", Multiplier: 2}
	if k.shortDelays {
		retry.InitialDelay, retry.MaxDelay = "20s", "1m"
	}
	cfg.UpdateRetry = retry
	return cfg
}

// repairSettings are the knobs as the engine's input carries them, read
// through the loaders the InferenceReplica controller uses.
type repairSettings struct {
	readyTimeout       time.Duration
	stuckGrace         time.Duration
	unschedulableGrace time.Duration
	abandonedGrace     time.Duration
	retry              *workloadtypes.RetryPolicy
	forceDelete        *workloadtypes.ForceDeletePolicy
	repairBatch        *int32
	requeue            workloadtypes.RequeueIntervals
	audit              *workloadtypes.MigrationAuditPolicy
}

func resolveRepairSettings(t *testing.T, cfg controllerconfig.LifecycleConfig) repairSettings {
	t.Helper()
	var s repairSettings
	var err error
	must := func(what string) {
		if err != nil {
			t.Fatalf("lifecycle config %s: %v", what, err)
		}
	}
	s.readyTimeout, err = cfg.ToInstanceReadyTimeout()
	must("instanceReadyTimeout")
	s.stuckGrace, err = cfg.ToGracePeriod()
	must("stuckPodGracePeriod")
	s.unschedulableGrace, err = cfg.ToUnschedulableGracePeriod()
	must("unschedulableGracePeriod")
	s.abandonedGrace, err = cfg.ToAbandonedReplacementGracePeriod()
	must("abandonedReplacementGracePeriod")
	s.retry, err = cfg.UpdateRetry.ToPolicy()
	must("updateRetry")
	s.forceDelete, err = cfg.ForceDelete.ToPolicy()
	must("forceDelete")
	s.repairBatch, err = cfg.ToRepairBatchSize()
	must("repairBatchSize")
	s.requeue, err = cfg.Requeue.ToIntervals()
	must("requeue")
	s.audit, err = cfg.Audit.ToPolicy()
	must("audit")
	return s
}

// repairShape is the Component's size: how many Instances, and how many
// pods each has (one, or a leader and workers).
type repairShape struct {
	instances int32
	pods      int32
}

func (s repairShape) String() string {
	if s.pods > 1 {
		return fmt.Sprintf("%d gangs of %d", s.instances, s.pods)
	}
	return fmt.Sprintf("%d single-pod", s.instances)
}

// The stories: each names the moves the breadth-first walk draws from.
// The seeded runs draw from all of them.
const (
	storyExit          = "runner-exits"
	storyWaiting       = "pod-waits-on-kubelet"
	storyLost          = "pod-lost"
	storyNode          = "node-lost"
	storySilent        = "kubelet-silent"
	storyRoom          = "no-room"
	storyPushCrashOnce = "push-crash-once"
	storyPushCrashLoop = "push-crash-loop"
	storyPushBadImage  = "push-bad-image"
	storyPushNoReady   = "push-never-ready"
	storyMove          = "move"
)

var repairStories = []string{storyExit, storyWaiting, storyLost, storyNode, storySilent, storyRoom,
	storyPushCrashOnce, storyPushCrashLoop, storyPushBadImage, storyPushNoReady, storyMove}

// repairConfig is one configuration under exploration.
type repairConfig struct {
	shape  repairShape
	policy v1beta1.InstanceRestartPolicy
	knobs  repairKnobs
	story  string
	// lateExit is how many of an Instance's runner deaths the kubelet
	// model first reports with the new run's start alone and the
	// termination one report later.
	lateExit int
	// partition is how many Instances the roll holds on the old
	// revision, as a canary step holds them.
	partition int32
}

func (c repairConfig) String() string {
	s := fmt.Sprintf("%s %s %s %s", c.shape, c.policy, c.knobs, c.story)
	if c.lateExit > 0 {
		s += fmt.Sprintf(" late-exit=%d", c.lateExit)
	}
	if c.partition > 0 {
		s += fmt.Sprintf(" partition=%d", c.partition)
	}
	return s
}

// repairMove is one thing the world does between two settles.
type repairMove struct {
	kind   string
	idx    int32
	runner string
	arg    string
	// after is how many passes the engine runs before the move lands, so
	// a fault reaches a pod at a moment of its life the settled state does
	// not show: a pod just created, running before Ready, just promoted.
	after int
}

func (m repairMove) String() string {
	s := m.kind
	if m.arg != "" {
		s += ":" + m.arg
	}
	switch m.kind {
	case "tick", "tick-all", "tick-1m", "node-back", "room-back", "no-room", "webhook-down", "webhook-back", "image-back", "recover-waiting", "node-pods-deleted":
	default:
		s += fmt.Sprintf("@%d", m.idx)
		if m.runner != "" {
			s += "/" + m.runner
		}
	}
	if m.after > 0 {
		s += fmt.Sprintf("+%dp", m.after)
	}
	return s
}

// repairFinding is one broken invariant, with the shortest path seen.
type repairFinding struct {
	class   string
	message string
	path    string
	picture string
}

// repairMemory is what a node remembers across passes for the checks that
// compare two moments: the pods a held revision had when it held, and
// the pods each gang incarnation was seen with.
type repairMemory struct {
	heldSeen     map[string]map[types.UID]bool
	heldInFlight map[string]map[int32]bool
	gangSeen     map[int32]map[int64]map[types.UID]bool
	eventsAt     int
}

func (m repairMemory) clone() repairMemory {
	out := repairMemory{heldSeen: map[string]map[types.UID]bool{}, heldInFlight: map[string]map[int32]bool{}, gangSeen: map[int32]map[int64]map[types.UID]bool{}, eventsAt: m.eventsAt}
	for rev, uids := range m.heldSeen {
		out.heldSeen[rev] = cloneMap(uids)
	}
	for rev, idxs := range m.heldInFlight {
		out.heldInFlight[rev] = cloneMap(idxs)
	}
	for idx, incs := range m.gangSeen {
		out.gangSeen[idx] = map[int64]map[types.UID]bool{}
		for inc, uids := range incs {
			out.gangSeen[idx][inc] = cloneMap(uids)
		}
	}
	return out
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// repairSnapshot is a harness at rest: its fields with the client, the
// clock and the recorder taken out, and every object the fake apiserver
// held. A node keeps a snapshot, not a harness, so a frontier of hundreds
// of states holds no fake clients.
type repairSnapshot struct {
	fields       recoveryHarness
	now          time.Time
	holdRecorded bool
	objs         []client.Object
}

// recoveryHarnessFields is how many fields recoveryHarness carries. The
// snapshot copies each map and slice by hand, so a field added to the
// harness must be added there too; this count is what makes the test
// say so instead of sharing state between branches.
const recoveryHarnessFields = 92

func copyHarnessCollections(dst, src *recoveryHarness) {
	dst.mutateErr = cloneMap(src.mutateErr)
	dst.terminatingUntil = cloneMap(src.terminatingUntil)
	dst.crashes = cloneMap(src.crashes)
	dst.lastCrash = cloneMap(src.lastCrash)
	dst.lastRunStart = cloneMap(src.lastRunStart)
	dst.servedPasses = cloneMap(src.servedPasses)
	dst.wedgedPods = cloneMap(src.wedgedPods)
	dst.evicted = cloneMap(src.evicted)
	dst.rejected = cloneMap(src.rejected)
	dst.deadNodes = cloneMap(src.deadNodes)
	dst.cordonedNodes = cloneMap(src.cordonedNodes)
	dst.readinessFails = cloneMap(src.readinessFails)
	dst.kubeletGone = cloneMap(src.kubeletGone)
	dst.imageNameOnNode = cloneMap(src.imageNameOnNode)
	dst.startedImage = cloneMap(src.startedImage)
	dst.crashedInstances = cloneMap(src.crashedInstances)
	dst.lateTerminations = cloneMap(src.lateTerminations)
	dst.killOnce = cloneMap(src.killOnce)
	dst.staysDown = cloneMap(src.staysDown)
	dst.onceCrashed = cloneMap(src.onceCrashed)
	dst.unplaceable = cloneMap(src.unplaceable)
	dst.exitsCleanOnStop = cloneMap(src.exitsCleanOnStop)
	dst.stillStopping = cloneMap(src.stillStopping)
	dst.blocks = append([]workloadtypes.RetryBlock(nil), src.blocks...)
	dst.heldWarnings = append([]string(nil), src.heldWarnings...)
	dst.failedWarnings = append([]string(nil), src.failedWarnings...)
	dst.events = append([]string(nil), src.events...)
	dst.nodeNames = append([]string(nil), src.nodeNames...)
	dst.holds = append([]workloadtypes.RolloutHold(nil), src.holds...)
	dst.migrations = append([]workloadtypes.MigrationRecord(nil), src.migrations...)
	dst.priorOpening = nil
	dst.verdict = nil
}

func snapshotHarness(h *recoveryHarness) *repairSnapshot {
	h.t.Helper()
	snap := &repairSnapshot{fields: *h, now: h.clk.Now(), holdRecorded: h.verdict != nil}
	snap.fields.c, snap.fields.clk, snap.fields.recorder = nil, nil, nil
	copyHarnessCollections(&snap.fields, h)
	list := func(l client.ObjectList, each func(client.Object)) {
		if err := h.c.List(h.ctx, l); err != nil {
			h.t.Fatalf("snapshot list %T: %v", l, err)
		}
		switch items := l.(type) {
		case *corev1.PodList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *corev1.NodeList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *corev1.ConfigMapList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *appsv1.ControllerRevisionList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *discoveryv1.EndpointSliceList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *v1beta1.InferenceReplicaList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		case *v1beta1.InferenceServiceList:
			for i := range items.Items {
				each(&items.Items[i])
			}
		}
	}
	keep := func(obj client.Object) {
		copied := obj.DeepCopyObject().(client.Object)
		// A Terminating pod whose kubelet stopped is shown without the
		// finalizer that keeps it in the fake apiserver; the stored object
		// has it, and a restore needs it back.
		if pod, ok := copied.(*corev1.Pod); ok && pod.DeletionTimestamp != nil && len(pod.Finalizers) == 0 {
			pod.Finalizers = []string{terminationFinalizer}
		}
		snap.objs = append(snap.objs, copied)
	}
	list(&v1beta1.InferenceServiceList{}, keep)
	list(&v1beta1.InferenceReplicaList{}, keep)
	list(&appsv1.ControllerRevisionList{}, keep)
	list(&corev1.NodeList{}, keep)
	list(&corev1.PodList{}, keep)
	list(&corev1.ConfigMapList{}, keep)
	list(&discoveryv1.EndpointSliceList{}, keep)
	return snap
}

func (w *repairWorld) restore(snap *repairSnapshot) *recoveryHarness {
	w.t.Helper()
	h := &recoveryHarness{}
	*h = snap.fields
	copyHarnessCollections(h, &snap.fields)
	h.clk = clocktesting.NewFakeClock(snap.now)
	h.recorder = record.NewFakeRecorder(8192)
	if snap.holdRecorded && len(h.holds) > 0 {
		h.verdict = &h.holds[len(h.holds)-1]
	}
	objs := make([]client.Object, 0, len(snap.objs))
	for _, obj := range snap.objs {
		objs = append(objs, obj.DeepCopyObject().(client.Object))
	}
	h.c = fake.NewClientBuilder().WithScheme(w.scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: h.interceptCreate, Delete: h.interceptDelete,
			Get: h.interceptGet, List: h.interceptList, Update: h.interceptUpdate,
		}).
		Build()
	return h
}

// repairNode is one explored state: the snapshot, what the node remembers,
// the moves that led here, the state's key, and what the settle that
// produced it showed.
type repairNode struct {
	snap   *repairSnapshot
	mem    repairMemory
	path   []repairMove
	key    string
	quiet  bool
	timers int
	found  []repairFinding
	image  string
}

// repairWorld builds harnesses for one configuration and runs the engine
// over them.
type repairWorld struct {
	t        *testing.T
	cfg      repairConfig
	settings repairSettings
	nodes    []string
	scheme   *runtime.Scheme
	// revImage names the image a revision renders, for the state key and
	// the pictures.
	revImage map[string]string
	passes   int
	// settlePasses bounds the passes one settle may take.
	settlePasses int
}

const (
	repairStep     = 5 * time.Second
	repairPodGrace = 10 * time.Second
)

func newRepairWorld(t *testing.T, cfg repairConfig) *repairWorld {
	t.Helper()
	w := &repairWorld{t: t, cfg: cfg, settings: resolveRepairSettings(t, cfg.knobs.lifecycleConfig()), revImage: map[string]string{}, scheme: makeScheme(t)}
	nodes := int(cfg.shape.instances)/2 + 2
	for i := 0; i < nodes; i++ {
		w.nodes = append(w.nodes, fmt.Sprintf("node-%c", 'a'+i))
	}
	w.settlePasses = 24 + 10*int(cfg.shape.instances)
	return w
}

// retarget points the harness at a revision rendered from image, keeping
// the gang's worker count, which setTarget renders at one.
func (w *repairWorld) retarget(h *recoveryHarness, rev *appsv1.ControllerRevision, image string) {
	h.setTarget(rev, image)
	if w.cfg.shape.pods > 1 {
		h.desired.Runners = workloadtypes.RunnersWithWorkerSize(h.desired.Runners, w.cfg.shape.pods-1)
	}
	w.revImage[rev.Name] = image
}

// root is the Component settled on its first revision with every Instance
// serving, the world's starting state.
func (w *repairWorld) root() *repairNode {
	w.t.Helper()
	h := newRecoveryHarnessFor(w.t, w.cfg.shape.pods > 1, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(8192)
	h.replicas = w.cfg.shape.instances
	policy := v1beta1convert.InstanceRestartPolicyToWorkload(w.cfg.policy)
	h.lifecycle.RestartPolicy = &policy
	maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
	rolling := &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable}
	if w.cfg.partition > 0 {
		partition := w.cfg.partition
		rolling.Partition = &partition
	}
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{Type: workloadtypes.UpdateStrategyRecreatePod, RollingUpdate: rolling}
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: w.settings.readyTimeout}
	h.retryPolicy = w.settings.retry
	h.stuckGrace = w.settings.stuckGrace
	h.unschedulableGrace = w.settings.unschedulableGrace
	h.retiredReplacementGrace = w.settings.abandonedGrace
	h.forceDelete = w.settings.forceDelete
	h.repairBatchSize = w.settings.repairBatch
	h.requeue = w.settings.requeue
	h.migrationAudit = w.settings.audit
	h.podGrace = repairPodGrace
	h.routed = true
	h.stepFloor, h.stepCap = repairStep, repairStep
	h.crashAfterServed = 1
	h.crashEverySet = true
	h.terminationLagsFor = w.cfg.lateExit
	h.useNodes(w.nodes...)
	w.revImage[h.revV1.Name] = goodImage
	w.revImage[h.revBad.Name] = badImage
	w.revImage[h.revFixed.Name] = fixedImage
	w.retarget(h, h.revV1, goodImage)
	node := &repairNode{mem: repairMemory{heldSeen: map[string]map[types.UID]bool{}, heldInFlight: map[string]map[int32]bool{}, gangSeen: map[int32]map[int64]map[types.UID]bool{}}, image: goodImage}
	if !h.run(40+10*int(w.cfg.shape.instances), func() bool { return w.settled(h) }) {
		h.dumpState("initial create")
		w.t.Fatalf("[%s] the initial create never settled on the first revision", w.cfg)
	}
	h.events, h.holds, h.heldWarnings, h.failedWarnings = nil, nil, nil, nil
	w.finish(node, h)
	return node
}

// settled reports whether every Instance is Ready on the target revision
// with no operation and its whole pod set serving.
func (w *repairWorld) settled(h *recoveryHarness) bool {
	rows := h.irStatuses()
	if int32(len(rows)) != w.cfg.shape.instances {
		return false
	}
	for _, s := range rows {
		if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.target.Name {
			return false
		}
		if !w.servingSet(h, s.Index) {
			return false
		}
	}
	return true
}

// servingSet reports whether the Instance's whole pod set is Ready and in
// the serving gate.
func (w *repairWorld) servingSet(h *recoveryHarness, idx int32) bool {
	pods := h.podsOf(idx)
	if int32(len(pods)) != w.cfg.shape.pods {
		return false
	}
	for _, pod := range pods {
		if !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
			return false
		}
	}
	return true
}

// pass runs one reconcile and moves the clock one step, as the harness's
// own step does.
func (w *repairWorld) pass(n *repairNode, h *recoveryHarness) {
	w.t.Helper()
	res, err := h.pass()
	w.passes++
	if err != nil {
		n.found = append(n.found, repairFinding{class: "error", message: fmt.Sprintf("Reconcile returned an error: %v", err)})
	}
	advance := h.stepFloor
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	if h.stepCap > 0 && advance > h.stepCap {
		advance = h.stepCap
	}
	h.clk.Step(advance)
	w.observe(n, h)
}

// observe runs the checks that compare two moments after every pass.
func (w *repairWorld) observe(n *repairNode, h *recoveryHarness) {
	rows := h.irStatuses()
	live := h.livePods()
	byIdx := map[int32][]*corev1.Pod{}
	for _, pod := range live {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			byIdx[idx] = append(byIdx[idx], pod)
		}
	}
	// A repair opened on a promoted set whose runner restarted must carry
	// the record that lets the rebuilt set's death read as a loop.
	for _, s := range rows {
		if s.Phase != workloadtypes.InstancePhaseRestarting || s.Operation == nil || !strings.Contains(s.Operation.Reason, "restarted after Ready") {
			continue
		}
		if s.LastFailure == nil || s.LastFailure.ContainerName == "" {
			class := "rebuild-without-record"
			if s.LastFailure != nil && waitReason(s.LastFailure.Reason) {
				// A wait that landed on the repair wrote its reading over
				// the exit record; the lock
				// TestRepair_AWaitDuringARebuildKeepsTheRunnersExitRecord
				// locks in the waits that must leave the record in place.
				class = "known-wait-replaced-exit-record"
			}
			n.found = append(n.found, repairFinding{class: class,
				message: fmt.Sprintf("Instance %d is being rebuilt for %q with no failure record naming its runner (record: %s, wait: %q)", s.Index, s.Operation.Reason, terminationText(s.LastFailure), s.Operation.Waiting)})
		}
	}
	// No pod of a revision whose retry record holds is built after the hold.
	for _, b := range h.blocks {
		if b.State != workloadtypes.RetryBlockHeld {
			continue
		}
		hash := query.RevisionFromName(b.TargetRevision).Hash()
		onRevision := map[types.UID]bool{}
		for _, pod := range live {
			if pod.Labels[query.LabelRevisionHash] == hash {
				onRevision[pod.UID] = true
			}
		}
		seen := n.mem.heldSeen[b.TargetRevision]
		if seen == nil {
			// An attempt admitted before the hold may still create its
			// pods; its Instance is remembered so they are not read as a
			// fresh start.
			n.mem.heldSeen[b.TargetRevision] = onRevision
			inFlight := map[int32]bool{}
			for _, s := range rows {
				if s.Operation != nil && (s.Operation.TargetRevision == b.TargetRevision || s.TargetRevision == b.TargetRevision) {
					inFlight[s.Index] = true
				}
			}
			n.mem.heldInFlight[b.TargetRevision] = inFlight
			continue
		}
		for _, pod := range live {
			idx, _ := query.InstanceIdxFromLabels(pod)
			if onRevision[pod.UID] && !seen[pod.UID] {
				switch {
				case n.mem.heldInFlight[b.TargetRevision][idx]:
					n.found = append(n.found, repairFinding{class: "attempt-in-flight-finished-after-held",
						message: fmt.Sprintf("pod %s of revision %s was created by an attempt admitted before that revision's retry record held (%d attempts)", pod.Name, w.short(b.TargetRevision), b.AttemptsStarted)})
				case repairOfPromotedSet(rows, idx, b.TargetRevision):
					// A row Ready on the held revision, whose set served
					// between crashes, is repaired at that revision when
					// it loses a pod; the class keeps that repair apart
					// from a pod built fresh under the hold.
					n.found = append(n.found, repairFinding{class: "repair-of-ready-row-under-held-record",
						message: fmt.Sprintf("pod %s of revision %s was rebuilt by the repair of a row that ran that revision after its retry record held (%d attempts)", pod.Name, w.short(b.TargetRevision), b.AttemptsStarted)})
				default:
					n.found = append(n.found, repairFinding{class: "rebuilt-after-held",
						message: fmt.Sprintf("pod %s of revision %s was created after that revision's retry record held (%d attempts)", pod.Name, w.short(b.TargetRevision), b.AttemptsStarted)})
				}
				seen[pod.UID] = true
			}
		}
	}
	// A gang under RecreateInstanceOnPodRestart moves as a whole: a member
	// never joins an incarnation its survivors already run while the gang
	// stands formed. A member re-created while the gang is still being
	// built, by the open create or repair, is reported in a class of its
	// own.
	phases := map[int32]workloadtypes.InstancePhase{}
	for _, s := range rows {
		phases[s.Index] = s.Phase
	}
	if w.cfg.shape.pods > 1 {
		for idx, pods := range byIdx {
			incs := n.mem.gangSeen[idx]
			if incs == nil {
				incs = map[int64]map[types.UID]bool{}
				n.mem.gangSeen[idx] = incs
			}
			for _, pod := range pods {
				inc, ok := query.InstanceIncarnationFromLabels(pod)
				if !ok {
					continue
				}
				uids := incs[inc]
				if uids == nil {
					uids = map[types.UID]bool{}
					incs[inc] = uids
				}
				if uids[pod.UID] {
					continue
				}
				survivor := false
				for _, other := range pods {
					if otherInc, ok := query.InstanceIncarnationFromLabels(other); ok && otherInc == inc && uids[other.UID] {
						survivor = true
					}
				}
				if survivor {
					class := "gang-member-alone"
					switch {
					case w.cfg.policy != v1beta1.InstanceRestartPolicyRecreateInstance:
						class = "gap-filled-under-none"
					case phases[idx] != workloadtypes.InstancePhaseReady:
						class = "gang-member-refilled-while-forming"
					}
					n.found = append(n.found, repairFinding{class: class,
						message: fmt.Sprintf("pod %s joined incarnation %d of Instance %d beside members that already ran it", pod.Name, inc, idx)})
				}
			}
			for _, pod := range pods {
				if inc, ok := query.InstanceIncarnationFromLabels(pod); ok {
					incs[inc][pod.UID] = true
				}
			}
		}
	}
}

// waitReason reports whether a failure record's reason is a wait's reading
// rather than a death of the set.
func waitReason(reason string) bool {
	switch reason {
	case workloadtypes.WaitingReasonUnschedulable, workloadtypes.RejectionReasonAdmissionUnavailable, workloadtypes.WaitingReasonNodeUnknown,
		workloadtypes.WaitingReasonCapacityProvisioning, workloadtypes.WaitingReasonPodGroupTerminating, workloadtypes.WaitingReasonSourceUnrouted:
		return true
	}
	return false
}

// repairOfPromotedSet reports whether the row of Instance idx runs rev and
// is under a repair, or stands Ready on it: a pod of rev created for it is
// the repair of a set that served, not a fresh start at the revision.
func repairOfPromotedSet(rows []workloadtypes.InstanceStatus, idx int32, rev string) bool {
	for _, s := range rows {
		if s.Index != idx || s.RunningRevision != rev {
			continue
		}
		if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationRestart {
			return true
		}
		return s.Phase == workloadtypes.InstancePhaseReady || s.Phase == workloadtypes.InstancePhaseRestarting
	}
	return false
}

// short is the image a revision renders, the way the pictures name it.
func (w *repairWorld) short(rev string) string {
	if rev == "" {
		return "-"
	}
	if image, ok := w.revImage[rev]; ok {
		return strings.TrimPrefix(image, "registry.test/serving:")
	}
	return query.RevisionFromName(rev).Hash()
}

func terminationText(t *workloadtypes.InstanceTermination) string {
	if t == nil {
		return "no record"
	}
	return fmt.Sprintf("%s/%s %s", t.PodName, t.ContainerName, t.Reason)
}

// key is the state as the engine's next decision reads it: the rows, the
// retry records, every pod's kubelet report, the faults standing in the
// world and which timers are still to run. Time itself is left out, so
// two paths that reach the same picture merge.
func (w *repairWorld) key(h *recoveryHarness) string {
	var b strings.Builder
	rows := h.irStatuses()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	for _, s := range rows {
		fmt.Fprintf(&b, "%d:%s:%s:%s:%d", s.Index, s.Phase, w.short(s.RunningRevision), w.short(s.TargetRevision), s.Incarnation)
		if s.Operation != nil {
			fmt.Fprintf(&b, ":%s/%s/%s/%d", s.Operation.Type, s.Operation.Step, s.Operation.Waiting, s.Operation.RetryCount)
		}
		if s.LastFailure != nil {
			fmt.Fprintf(&b, ":lf=%s/%t", s.LastFailure.Reason, s.LastFailure.ContainerName != "")
		}
		fmt.Fprintf(&b, ":rs=%t|", s.ReadySince != nil)
	}
	b.WriteString("#")
	for _, blk := range h.blocks {
		fmt.Fprintf(&b, "%s:%s:%d;", w.short(blk.TargetRevision), blk.State, blk.AttemptsStarted)
	}
	b.WriteString("#")
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("key: list pods: %v", err)
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	for i := range pods.Items {
		pod := &pods.Items[i]
		fmt.Fprintf(&b, "%s:%s:%s:%s:%s:%t:%t:%t:%t:%t:%t:%t;",
			strings.TrimPrefix(pod.Name, recoveryOwner+"-"), w.short(revisionNameOf(pod)), pod.Labels[query.LabelInstanceIncarnation], pod.Status.Phase,
			containerStateText(pod), podConditionTrue(pod, corev1.PodReady), podreadiness.IsServing(pod), podConditionTrue(pod, corev1.ContainersReady),
			pod.DeletionTimestamp != nil, h.kubeletStopped(pod), h.wedgeReasonFor(pod) != "" || h.evicted[wedgeKey(pod)], h.killOnce[pod.Name])
	}
	fmt.Fprintf(&b, "#dead=%d room=%t webhook=%t fault=%s imageback=%t target=%s", len(h.deadNodes), h.placementHeld, h.admissionDown, h.wedge, h.badImageHeals, w.short(h.target.Name))
	for _, m := range h.migrations {
		fmt.Fprintf(&b, " move=%s", m.Phase)
	}
	return b.String()
}

// revisionNameOf is the revision a pod's label names, through the names
// the world has seen.
func revisionNameOf(pod *corev1.Pod) string {
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		return ""
	}
	return recoveryOwner + "-" + string(workloadtypes.ComponentEngine) + "-" + hash
}

func containerStateText(pod *corev1.Pod) string {
	if len(pod.Status.ContainerStatuses) == 0 {
		return "-"
	}
	cs := pod.Status.ContainerStatuses[0]
	switch {
	case cs.State.Waiting != nil:
		return "W:" + cs.State.Waiting.Reason
	case cs.State.Terminated != nil:
		return fmt.Sprintf("T:%d", cs.State.Terminated.ExitCode)
	case cs.State.Running != nil:
		return fmt.Sprintf("R%d", cs.RestartCount)
	}
	return "-"
}

// repairTimer is one instant the engine or the kubelet model waits for.
type repairTimer struct {
	kind string
	at   time.Time
}

// timers lists every deadline and grace still to run: operation
// deadlines, the retry record's next attempt, a stopped repair's next
// try, the stuck-pod grace of a pod not serving, the unschedulable grace,
// a Terminating pod's own deadline and the force-delete slack past it, the
// node evidence threshold, the crash window after a promote, and the
// kubelet model's own termination grace.
func (w *repairWorld) timers(h *recoveryHarness) []repairTimer {
	now := h.clk.Now()
	var out []repairTimer
	// A timer due at this very instant is still pending: the kubelet and
	// the engine act on it at the start of the next pass.
	add := func(kind string, at time.Time) {
		if !at.IsZero() && !at.Before(now) {
			out = append(out, repairTimer{kind: kind, at: at})
		}
	}
	in := workloadtypes.ReconcileInput{UpdateRetryPolicy: h.retryPolicy, Clock: h.clk}
	rows := h.irStatuses()
	for i := range rows {
		s := rows[i]
		if s.Operation != nil && !s.Operation.Deadline.IsZero() {
			add("deadline", s.Operation.Deadline.Time)
		}
		if at, owed := workloadops.RepairRetryAt(in, &s); owed {
			add("repair-retry", at)
		}
		if s.Phase == workloadtypes.InstancePhaseReady && s.ReadySince != nil && w.settings.stuckGrace > 0 {
			add("crash-window", s.ReadySince.Add(w.settings.stuckGrace))
		}
	}
	for _, blk := range h.blocks {
		if blk.NextRetryAt != nil {
			add("retry", blk.NextRetryAt.Time)
		}
	}
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("timers: list pods: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			add("terminating", pod.DeletionTimestamp.Time)
			if w.settings.forceDelete != nil {
				add("force-delete", pod.DeletionTimestamp.Add(w.settings.forceDelete.OverdueSlack))
			}
			continue
		}
		if w.settings.stuckGrace > 0 && !(podConditionTrue(pod, corev1.PodReady) && podreadiness.IsServing(pod)) {
			add("stuck-grace", pod.CreationTimestamp.Add(w.settings.stuckGrace))
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.ContainersReady && cond.Status != corev1.ConditionTrue {
					add("unready-grace", cond.LastTransitionTime.Add(w.settings.stuckGrace))
				}
			}
		}
		if _, since, refused := workloadtypes.PodUnschedulable(pod); refused && w.settings.unschedulableGrace > 0 {
			add("unschedulable-grace", since.Add(w.settings.unschedulableGrace))
		}
	}
	for _, until := range h.terminatingUntil {
		add("termination-grace", until)
	}
	if w.settings.forceDelete != nil && len(h.deadNodes) > 0 {
		nodes := &corev1.NodeList{}
		if err := h.c.List(h.ctx, nodes); err != nil {
			w.t.Fatalf("timers: list nodes: %v", err)
		}
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if !h.deadNodes[node.Name] {
				continue
			}
			for _, cond := range node.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
					add("node-threshold", cond.LastTransitionTime.Add(w.settings.forceDelete.NodeUnreachableThreshold))
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at.Before(out[j].at) })
	return out
}

func (w *repairWorld) timerKinds(h *recoveryHarness) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range w.timers(h) {
		if !seen[t.kind] {
			seen[t.kind] = true
			out = append(out, t.kind)
		}
	}
	sort.Strings(out)
	return out
}

// settle runs passes until the state stops changing, then decides what
// the settled state says.
func (w *repairWorld) settle(n *repairNode, h *recoveryHarness) {
	prev := w.key(h)
	stable := 0
	before := len(h.livePods())
	n.quiet = false
	for i := 0; i < w.settlePasses; i++ {
		w.pass(n, h)
		k := w.key(h)
		if k == prev {
			stable++
			if stable >= 1 {
				n.quiet = true
				break
			}
			continue
		}
		stable = 0
		prev = k
	}
	if !n.quiet {
		n.found = append(n.found, repairFinding{class: "churn",
			message: fmt.Sprintf("the state kept changing for %d passes (%d live pods before, %d after)", w.settlePasses, before, len(h.livePods()))})
	}
}

// finish records a node's key, its pending timers, its picture, and the
// settled-state checks.
func (w *repairWorld) finish(n *repairNode, h *recoveryHarness) {
	kinds := w.timerKinds(h)
	n.key = w.key(h) + " timers=" + strings.Join(kinds, ",")
	n.timers = len(kinds)
	n.image = strings.TrimPrefix(w.revImage[h.target.Name], "registry.test/serving:")
	if n.quiet && n.timers == 0 {
		w.checkSettled(n, h)
	}
	if n.reportable() {
		picture := w.picture(h)
		for i := range n.found {
			n.found[i].picture = picture
		}
	}
	n.snap = snapshotHarness(h)
}

// reportable reports whether the node's settle showed a class the report
// prints with a picture: anything but a named or known wait.
func (n *repairNode) reportable() bool {
	for _, f := range n.found {
		switch {
		case strings.HasPrefix(f.class, "named-wait"), strings.HasPrefix(f.class, "known-"):
		case f.class == "gap-filled-under-none", f.class == "gang-member-refilled-while-forming", f.class == "repair-of-ready-row-under-held-record", f.class == "attempt-in-flight-finished-after-held":
		default:
			return true
		}
	}
	return false
}

// The event reasons that name a wait or a failure for an operator.
var repairNamedReasons = []workloadtypes.EventReason{
	workloadtypes.EventReasonRepairHeld, workloadtypes.EventReasonRepairRetriesExhausted, workloadtypes.EventReasonRepairWaitingOnWorkload,
	workloadtypes.EventReasonDrainOverdue, workloadtypes.EventReasonPodDeleteBlockedByFinalizer, workloadtypes.EventReasonMigrationParked,
	workloadtypes.EventReasonInstanceFailed, workloadtypes.EventReasonRetryHeld, workloadtypes.EventReasonRepairRevisionGone,
	workloadtypes.EventReasonMigrationDeferred, workloadtypes.EventReasonMigrationPolicyUnconfigured, workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured,
}

// named reports the reason the state gives an operator for an Instance
// that does not serve, or "" when nothing names it.
func (w *repairWorld) named(n *repairNode, h *recoveryHarness, s workloadtypes.InstanceStatus) string {
	if s.Phase == workloadtypes.InstancePhaseFailed && s.LastFailure != nil && s.LastFailure.Reason != "" {
		return "Failed:" + s.LastFailure.Reason
	}
	if s.Operation != nil && s.Operation.Waiting != "" {
		return "waiting:" + s.Operation.Waiting
	}
	if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationMigrate {
		for _, record := range h.migrations {
			if record.RequestUUID == s.Operation.RequestUUID {
				return "move:" + string(record.Phase)
			}
		}
		return "move"
	}
	if s.LastFailure != nil && strings.Contains(s.LastFailure.Message, "crash loop") {
		return "crash-loop-named"
	}
	rev := s.RunningRevision
	if rev == "" {
		rev = s.TargetRevision
	}
	if hold := h.verdict; hold != nil {
		namesRevision := (hold.Gate == workloadtypes.RolloutHoldGateRetryBlock || hold.Gate == workloadtypes.RolloutHoldGateHeld) &&
			(hold.Target == rev || hold.Target == s.TargetRevision || hold.Target == h.target.Name)
		// A budget hold is the roll's reason for every Instance it has
		// not moved yet: they wait their turn behind it.
		queued := hold.Gate == workloadtypes.RolloutHoldGateBudget && s.Operation == nil && s.RunningRevision != h.target.Name
		if holdNamesInstance(hold, s.Index) || slices.Contains(namedRebuilds(hold.Reason), s.Index) || namesRevision || queued {
			return "hold:" + string(hold.Gate)
		}
	}
	// A warning announced once for this row's episode stays named for the
	// episode, however long ago it was raised.
	for _, reason := range repairNamedReasons {
		if status.Announced(s, reason) {
			return "announced:" + string(reason)
		}
	}
	if b := h.findBlock(rev); b != nil && b.State == workloadtypes.RetryBlockHeld {
		return "retry-record-held"
	}
	podNames := map[string]bool{}
	for _, pod := range h.podsOf(s.Index) {
		podNames[pod.Name] = true
	}
	instanceKey := workloadtypes.InstanceKey(workloadtypes.ComponentEngine, s.Index)
	for _, e := range h.events[min(n.mem.eventsAt, len(h.events)):] {
		named := false
		for _, reason := range repairNamedReasons {
			if strings.Contains(e, " "+string(reason)+" ") {
				named = true
			}
		}
		if !named {
			continue
		}
		if strings.Contains(e, instanceKey) {
			return "event:" + strings.Fields(e)[1]
		}
		for name := range podNames {
			if strings.Contains(e, name) {
				return "event:" + strings.Fields(e)[1]
			}
		}
	}
	return ""
}

// checkSettled judges a state the engine has settled in with nothing left
// to wait for: every Instance serves, or names why it does not.
func (w *repairWorld) checkSettled(n *repairNode, h *recoveryHarness) {
	rows := h.irStatuses()
	for _, s := range rows {
		if s.Phase == workloadtypes.InstancePhaseReady && w.servingSet(h, s.Index) {
			continue
		}
		if reason := w.named(n, h, s); reason != "" {
			n.found = append(n.found, repairFinding{class: "named-wait:" + strings.SplitN(reason, ":", 2)[0], message: fmt.Sprintf("Instance %d does not serve and names %s", s.Index, reason)})
			continue
		}
		if known := w.knownWait(h, s.Index); known != "" {
			n.found = append(n.found, repairFinding{class: "known-wait:" + known, message: fmt.Sprintf("Instance %d does not serve: %s", s.Index, known)})
			continue
		}
		rev := s.RunningRevision
		if rev == "" {
			rev = s.TargetRevision
		}
		class := "rebuild-never-comes"
		if b := h.findBlock(rev); b != nil && b.State == workloadtypes.RetryBlockHeld {
			class = "no-way-out"
		}
		n.found = append(n.found, repairFinding{class: class,
			message: fmt.Sprintf("Instance %d (%s, %s, incarnation %d) does not serve, nothing names why, no deadline or grace is left to run", s.Index, s.Phase, w.short(s.RunningRevision), s.Incarnation)})
	}
	if int32(len(rows)) < w.cfg.shape.instances {
		n.found = append(n.found, repairFinding{class: "rebuild-never-comes", message: fmt.Sprintf("only %d of %d Instances have a row", len(rows), w.cfg.shape.instances)})
	}
}

// knownWait names a wait the engine holds by design and this model does
// not fail on: a pod of the Instance sits on a kubelet that stopped
// reporting, and either the Node under it still reads healthy or no
// force-delete policy lets the engine free a pod there. The engine reads
// such a pod as the kubelet last reported it and waits for its node; the
// study records what the operator sees meanwhile.
func (w *repairWorld) knownWait(h *recoveryHarness, idx int32) string {
	pods := h.podsOf(idx)
	all := &corev1.PodList{}
	if err := h.c.List(h.ctx, all, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("known wait: list pods: %v", err)
	}
	var own []*corev1.Pod
	for i := range all.Items {
		if i2, ok := query.InstanceIdxFromLabels(&all.Items[i]); ok && i2 == idx {
			own = append(own, &all.Items[i])
		}
	}
	if len(own) == 0 {
		return ""
	}
	silent, nodeEvidence := false, false
	for _, pod := range own {
		if !h.kubeletStopped(pod) {
			continue
		}
		silent = true
		if h.deadNodes[pod.Spec.NodeName] {
			nodeEvidence = true
		}
	}
	_ = pods
	switch {
	case !silent:
		return ""
	case !nodeEvidence:
		// The pod's kubelet stopped while its Node still reports Ready:
		// no reader of node health can tell this pod from a healthy one.
		return "kubelet-silent-node-reads-healthy"
	case w.settings.forceDelete == nil:
		// The Node reports the loss, and only the force-delete policy
		// lets the engine free a pod on it.
		return "node-lost-force-delete-off"
	case w.anotherAttemptOpen(h, idx):
		// The force delete of a serving Instance's lost node runs in the
		// create pass; with another Instance's attempt open, the model
		// reads this pod's wait as known. The lock
		// TestRepair_AHeldRepairDoesNotStopTheForceDeleteOfALostNode locks in
		// the held repair that must not stop that delete.
		return "lost-node-behind-another-attempt"
	}
	return ""
}

// anotherAttemptOpen reports whether an Instance other than idx carries an
// operation, or a move is in flight.
func (w *repairWorld) anotherAttemptOpen(h *recoveryHarness, idx int32) bool {
	for _, s := range h.irStatuses() {
		if s.Index != idx && s.Operation != nil {
			return true
		}
	}
	for _, record := range h.migrations {
		switch record.Phase {
		case workloadtypes.MigrationPhaseCompleted, workloadtypes.MigrationPhaseFailed:
		default:
			return true
		}
	}
	return false
}

// picture renders the state for a report.
func (w *repairWorld) picture(h *recoveryHarness) string {
	var parts []string
	for _, s := range h.irStatuses() {
		row := fmt.Sprintf("row %d %s on %s inc=%d", s.Index, s.Phase, w.short(s.RunningRevision), s.Incarnation)
		if s.Operation != nil {
			row += fmt.Sprintf(" op=%s/%s", s.Operation.Type, s.Operation.Step)
			if s.Operation.Waiting != "" {
				row += " waiting=" + s.Operation.Waiting
			}
			if s.Operation.Reason != "" {
				row += fmt.Sprintf(" reason=%q", s.Operation.Reason)
			}
		}
		if s.LastFailure != nil {
			row += fmt.Sprintf(" lastFailure=%s", terminationText(s.LastFailure))
		}
		parts = append(parts, row)
	}
	for _, b := range h.blocks {
		parts = append(parts, fmt.Sprintf("retry record %s %s attempts=%d reason=%q", w.short(b.TargetRevision), b.State, b.AttemptsStarted, b.Reason))
	}
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err == nil {
		sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
		for i := range pods.Items {
			pod := &pods.Items[i]
			text := fmt.Sprintf("pod %s %s inc=%s %s %s ready=%t serving=%t node=%s", strings.TrimPrefix(pod.Name, recoveryOwner+"-"), w.short(revisionNameOf(pod)), pod.Labels[query.LabelInstanceIncarnation],
				pod.Status.Phase, containerStateText(pod), podConditionTrue(pod, corev1.PodReady), podreadiness.IsServing(pod), pod.Spec.NodeName)
			if pod.DeletionTimestamp != nil {
				text += " terminating"
			}
			if h.kubeletStopped(pod) {
				text += " kubelet-stopped"
			}
			parts = append(parts, text)
		}
	}
	if h.verdict != nil {
		parts = append(parts, fmt.Sprintf("hold %s: %s", h.verdict.Gate, h.verdict.Reason))
	}
	events := h.events
	if len(events) > 6 {
		events = events[len(events)-6:]
	}
	for _, e := range events {
		parts = append(parts, "event "+e)
	}
	return strings.Join(parts, "\n      ")
}

// moves lists what the world may do next in the node's story.
func (w *repairWorld) moves(n *repairNode, h *recoveryHarness, all bool) []repairMove {
	var out []repairMove
	if n.timers > 0 {
		out = append(out, repairMove{kind: "tick"}, repairMove{kind: "tick-all"})
	}
	story := func(name string) bool { return all || w.cfg.story == name }
	targets := w.targets(h)
	delays := []int{0, 2}
	if all {
		delays = []int{0, 1, 2, 4}
	}
	for _, tgt := range targets {
		for _, after := range delays {
			if story(storyExit) || story(storyPushCrashOnce) || story(storyMove) || story(storyRoom) {
				out = append(out, repairMove{kind: "exit", idx: tgt.idx, runner: tgt.runner, after: after})
			}
			if story(storyWaiting) {
				out = append(out, repairMove{kind: "waiting", idx: tgt.idx, runner: tgt.runner, arg: "ImagePullBackOff", after: after})
				out = append(out, repairMove{kind: "waiting", idx: tgt.idx, runner: tgt.runner, arg: "CrashLoopBackOff", after: after})
			}
			if story(storyLost) || story(storyNode) || story(storySilent) || story(storyRoom) || story(storyMove) || story(storyPushBadImage) {
				out = append(out, repairMove{kind: "delete", idx: tgt.idx, runner: tgt.runner, after: after})
			}
			if story(storyLost) {
				out = append(out, repairMove{kind: "evict", idx: tgt.idx, after: after})
			}
			if story(storySilent) {
				out = append(out, repairMove{kind: "kubelet-silent", idx: tgt.idx, runner: tgt.runner, after: after})
			}
			if story(storyNode) && len(h.deadNodes) == 0 {
				out = append(out, repairMove{kind: "node-lost", idx: tgt.idx, runner: tgt.runner, after: after})
			}
		}
	}
	if story(storyWaiting) {
		out = append(out, repairMove{kind: "waiting-all", arg: "ImagePullBackOff"}, repairMove{kind: "recover-waiting"})
	}
	if story(storyExit) {
		out = append(out, repairMove{kind: "tick-1m"})
	}
	if story(storyNode) && len(h.deadNodes) > 0 {
		out = append(out, repairMove{kind: "node-pods-deleted"}, repairMove{kind: "node-back"})
	}
	if story(storySilent) && len(h.kubeletGone) > 0 {
		out = append(out, repairMove{kind: "kubelet-back"})
	}
	if story(storyRoom) {
		if h.placementHeld {
			out = append(out, repairMove{kind: "room-back"})
		} else {
			out = append(out, repairMove{kind: "no-room"})
		}
		if h.admissionDown {
			out = append(out, repairMove{kind: "webhook-back"})
		} else {
			out = append(out, repairMove{kind: "webhook-down"})
		}
	}
	pushes := map[string]string{storyPushCrashOnce: crashOnceAfterServingImage, storyPushCrashLoop: crashAfterServingImage, storyPushBadImage: badImage, storyPushNoReady: stuckImage}
	for name, image := range pushes {
		if story(name) && h.desired.PodSpec.Containers[0].Image == goodImage {
			out = append(out, repairMove{kind: "push", arg: strings.TrimPrefix(image, "registry.test/serving:")})
		}
	}
	if (story(storyPushCrashOnce) || story(storyPushCrashLoop) || story(storyPushBadImage) || story(storyPushNoReady)) && h.desired.PodSpec.Containers[0].Image != goodImage && h.desired.PodSpec.Containers[0].Image != fixedImage {
		out = append(out, repairMove{kind: "push", arg: "v2"})
	}
	if story(storyPushBadImage) && h.desired.PodSpec.Containers[0].Image == badImage && !h.badImageHeals {
		out = append(out, repairMove{kind: "image-back"})
	}
	if story(storyMove) && len(h.migrations) == 0 && w.movable(h, 0) {
		out = append(out, repairMove{kind: "move", idx: 0})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// repairTarget is one pod a fault may land on: an Instance and a runner.
type repairTarget struct {
	idx    int32
	runner string
}

// targets picks the pods the walk puts faults on: the lowest-indexed
// Instance that serves whole and the lowest-indexed one that does not,
// and in a gang the leader and the first worker of each.
func (w *repairWorld) targets(h *recoveryHarness) []repairTarget {
	rows := h.irStatuses()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	chosen := map[bool]int32{}
	for _, s := range rows {
		whole := s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && w.servingSet(h, s.Index)
		if _, ok := chosen[whole]; !ok {
			chosen[whole] = s.Index
		}
	}
	var out []repairTarget
	for _, whole := range []bool{true, false} {
		idx, ok := chosen[whole]
		if !ok {
			continue
		}
		if w.cfg.shape.pods > 1 {
			out = append(out, repairTarget{idx: idx, runner: workloadtypes.RunnerLeader}, repairTarget{idx: idx, runner: workloadtypes.RunnerWorker})
		} else {
			out = append(out, repairTarget{idx: idx})
		}
	}
	return out
}

// movable reports whether a move of Instance idx can be filed: a routed
// pod of it is placed on a node.
func (w *repairWorld) movable(h *recoveryHarness, idx int32) bool {
	for _, pod := range h.podsOf(idx) {
		if query.RoutedRunner(pod) && pod.Spec.NodeName != "" {
			return true
		}
	}
	return false
}

// podFor is the live pod of runner at idx a fault lands on, or nil.
func (w *repairWorld) podFor(h *recoveryHarness, idx int32, runner string) *corev1.Pod {
	for _, pod := range h.podsOf(idx) {
		if runner == "" || pod.Labels[query.LabelRunner] == runner {
			return pod
		}
	}
	return nil
}

// apply plays one move on a copy of the node and settles the engine.
func (w *repairWorld) apply(n *repairNode, mv repairMove) *repairNode {
	w.t.Helper()
	h := w.restore(n.snap)
	child := &repairNode{mem: n.mem.clone(), path: append(append([]repairMove(nil), n.path...), mv)}
	child.mem.eventsAt = len(h.events)
	for i := 0; i < mv.after; i++ {
		w.pass(child, h)
	}
	w.play(child, h, mv)
	w.settle(child, h)
	w.finish(child, h)
	return child
}

// play lands one move on the harness.
func (w *repairWorld) play(n *repairNode, h *recoveryHarness, mv repairMove) {
	w.t.Helper()
	pod := w.podFor(h, mv.idx, mv.runner)
	switch mv.kind {
	case "tick":
		if timers := w.timers(h); len(timers) > 0 {
			h.clk.Step(timers[0].at.Sub(h.clk.Now()) + time.Second)
		}
	case "tick-all":
		if timers := w.timers(h); len(timers) > 0 {
			h.clk.Step(timers[len(timers)-1].at.Sub(h.clk.Now()) + time.Second)
		}
	case "tick-1m":
		h.clk.Step(time.Minute)
	case "exit":
		if pod != nil && containerRunning(pod) {
			h.killRunnerOnce(pod)
		}
	case "waiting":
		if pod != nil {
			h.wedgePod(pod, mv.arg)
		}
	case "waiting-all":
		h.armWedge(mv.arg, false)
		h.wedgeLivePods(mv.arg)
	case "recover-waiting":
		h.clearWedge()
		h.wedgedPods = map[string]string{}
	case "delete":
		if pod != nil {
			if err := h.c.Delete(h.ctx, pod); err != nil {
				w.t.Fatalf("delete pod %s: %v", pod.Name, err)
			}
		}
	case "evict":
		if len(h.podsOf(mv.idx)) > 0 {
			h.evictPods(mv.idx)
		}
	case "kubelet-silent":
		if pod != nil {
			h.kubeletGone[pod.Name] = true
		}
	case "kubelet-back":
		h.kubeletGone = map[string]bool{}
	case "node-lost":
		if pod != nil && pod.Spec.NodeName != "" && len(h.deadNodes) == 0 && len(h.nodeNames) > 1 {
			h.failNode(pod.Spec.NodeName)
		}
	case "node-pods-deleted":
		for name := range h.deadNodes {
			for _, lost := range h.podsOnNode(name) {
				if lost.DeletionTimestamp == nil {
					if err := h.c.Delete(h.ctx, lost); err != nil {
						w.t.Fatalf("delete pod %s on the lost node: %v", lost.Name, err)
					}
				}
			}
		}
	case "node-back":
		for name := range h.deadNodes {
			h.recoverNode(name)
		}
	case "no-room":
		h.holdPlacement("0/4 nodes are available: insufficient accelerators")
	case "room-back":
		h.releasePlacement()
	case "webhook-down":
		h.admissionDown = true
	case "webhook-back":
		h.admissionDown = false
	case "push":
		image := "registry.test/serving:" + mv.arg
		w.retarget(h, h.ensureRevision(h.podSpec(image)), image)
	case "image-back":
		h.badImageHeals = true
	case "move":
		if w.movable(h, mv.idx) {
			h.requestMove(fmt.Sprintf("move-%d-%d", mv.idx, len(n.path)), mv.idx)
		}
	default:
		w.t.Fatalf("unknown move %s", mv.kind)
	}
}

func containerRunning(pod *corev1.Pod) bool {
	cs := priorContainerStatus(pod)
	return cs != nil && cs.State.Running != nil && pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning
}

// repairReport is what one configuration's exploration found.
type repairReport struct {
	states, depth, runs int
	counts              map[string]int
	findings            map[string]repairFinding
}

func newRepairReport() repairReport {
	return repairReport{counts: map[string]int{}, findings: map[string]repairFinding{}}
}

func (r *repairReport) take(n *repairNode) {
	for _, f := range n.found {
		r.counts[f.class]++
		if _, ok := r.findings[f.class]; !ok {
			f.path = repairPath(n.path)
			r.findings[f.class] = f
		}
	}
}

func repairPath(path []repairMove) string {
	parts := make([]string, 0, len(path))
	for _, mv := range path {
		parts = append(parts, mv.String())
	}
	if len(parts) == 0 {
		return "(start)"
	}
	return strings.Join(parts, " -> ")
}

// explore walks the configuration breadth-first to the bounds.
func (w *repairWorld) explore(maxStates, maxDepth int, budget time.Duration) repairReport {
	rep := newRepairReport()
	start := time.Now()
	root := w.root()
	rep.take(root)
	visited := map[string]bool{root.key: true}
	queue := []*repairNode{root}
	for len(queue) > 0 && rep.states < maxStates && time.Since(start) < budget {
		n := queue[0]
		queue = queue[1:]
		rep.states++
		if len(n.path) > rep.depth {
			rep.depth = len(n.path)
		}
		if len(n.path) >= maxDepth {
			continue
		}
		h := w.restore(n.snap)
		moves := w.moves(n, h, false)
		for _, mv := range moves {
			child := w.apply(n, mv)
			rep.take(child)
			if visited[child.key] {
				continue
			}
			visited[child.key] = true
			queue = append(queue, child)
		}
		n.snap = nil
	}
	return rep
}

// randomRuns plays whole runs from the start with a fixed seed, drawing
// from every story's moves, to reach the depths the walk does not.
func (w *repairWorld) randomRuns(rep *repairReport, seed int64, runs, maxDepth int, budget time.Duration) {
	rng := rand.New(rand.NewSource(seed))
	start := time.Now()
	for r := 0; r < runs && time.Since(start) < budget; r++ {
		n := w.root()
		rep.runs++
		for depth := 0; depth < maxDepth && time.Since(start) < budget; depth++ {
			h := w.restore(n.snap)
			moves := w.moves(n, h, true)
			if len(moves) == 0 {
				break
			}
			var progress, faults []repairMove
			for _, mv := range moves {
				switch mv.kind {
				case "tick", "tick-all", "tick-1m":
					progress = append(progress, mv)
				default:
					faults = append(faults, mv)
				}
			}
			var mv repairMove
			switch {
			case len(progress) == 0:
				mv = faults[rng.Intn(len(faults))]
			case len(faults) == 0 || rng.Intn(2) == 0:
				mv = progress[rng.Intn(len(progress))]
			default:
				mv = faults[rng.Intn(len(faults))]
			}
			n = w.apply(n, mv)
			rep.take(n)
			if len(n.path) > rep.depth {
				rep.depth = len(n.path)
			}
		}
	}
}

// repairConfigs lists the configurations to explore: the common shapes
// under the chart's values first, then a seeded sample over the whole
// space.
func repairConfigs(full bool, keep func(repairConfig) bool) []repairConfig {
	chart := repairKnobs{deadlines: "chart", tries: 3}
	short := repairKnobs{deadlines: "short", tries: 2, shortDelays: true, forceDelete: true}
	recreate, none := v1beta1.InstanceRestartPolicyRecreateInstance, v1beta1.InstanceRestartPolicyNone
	var out []repairConfig
	add := func(c repairConfig) {
		if keep(c) {
			out = append(out, c)
		}
	}
	for _, story := range repairStories {
		add(repairConfig{shape: repairShape{8, 1}, policy: recreate, knobs: chart, story: story})
		add(repairConfig{shape: repairShape{4, 1}, policy: none, knobs: chart, story: story})
		add(repairConfig{shape: repairShape{4, 2}, policy: recreate, knobs: short, story: story})
	}
	// The short-lived canary runner: one Instance of eight rolls, as a
	// canary step holds the other seven, and the pushed runner exits after
	// it serves; with the kubelet publishing the exit on time and late.
	for _, late := range []int{0, 1} {
		add(repairConfig{shape: repairShape{8, 1}, policy: recreate, knobs: chart, story: storyPushCrashOnce, lateExit: late, partition: 7})
	}
	add(repairConfig{shape: repairShape{8, 1}, policy: recreate, knobs: chart, story: storyExit, lateExit: 1})
	add(repairConfig{shape: repairShape{4, 4}, policy: recreate, knobs: chart, story: storyLost})
	add(repairConfig{shape: repairShape{4, 8}, policy: recreate, knobs: chart, story: storyNode})
	add(repairConfig{shape: repairShape{16, 1}, policy: recreate, knobs: chart, story: storyExit})
	if full {
		for _, story := range repairStories {
			add(repairConfig{shape: repairShape{16, 1}, policy: recreate, knobs: chart, story: story})
			add(repairConfig{shape: repairShape{8, 2}, policy: recreate, knobs: chart, story: story})
			add(repairConfig{shape: repairShape{4, 4}, policy: none, knobs: chart, story: story})
			add(repairConfig{shape: repairShape{8, 1}, policy: recreate, knobs: repairKnobs{deadlines: "chart", tries: 1}, story: story})
			add(repairConfig{shape: repairShape{8, 1}, policy: recreate, knobs: repairKnobs{deadlines: "chart", tries: 4, forceDelete: true}, story: story, lateExit: 1})
		}
		add(repairConfig{shape: repairShape{16, 8}, policy: recreate, knobs: chart, story: storyLost})
		add(repairConfig{shape: repairShape{16, 4}, policy: recreate, knobs: chart, story: storyExit})
	}
	shapes := []repairShape{{4, 1}, {8, 1}, {16, 1}, {4, 2}, {8, 2}, {16, 2}, {4, 4}, {8, 4}, {4, 8}, {8, 8}}
	rng := rand.New(rand.NewSource(20261019))
	sample := 6
	if full {
		sample = 30
	}
	for n := 0; n < sample; n++ {
		shape := shapes[rng.Intn(len(shapes))]
		policy := recreate
		if rng.Intn(3) == 0 {
			policy = none
		}
		knobs := repairKnobs{deadlines: []string{"chart", "short"}[rng.Intn(2)], tries: []int32{1, 2, 4}[rng.Intn(3)], shortDelays: rng.Intn(2) == 0, forceDelete: rng.Intn(2) == 0}
		late := 0
		if rng.Intn(3) == 0 {
			late = 1
		}
		var partition int32
		if rng.Intn(4) == 0 && shape.instances > 1 {
			partition = shape.instances - 1
		}
		add(repairConfig{shape: shape, policy: policy, knobs: knobs, story: repairStories[rng.Intn(len(repairStories))], lateExit: late, partition: partition})
	}
	return out
}

// repairExplore explores every configuration keep admits and reports each
// broken invariant with the shortest path to it. The classes that fail
// the test are a hold with no way out, a rebuild that never comes, a
// rebuild opened without a record, a pod built after the retry record held, a
// gang member rebuilt alone, unbounded churn and an engine error.
func repairExplore(t *testing.T, keep func(repairConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_REPAIR_HUNT") == "full"
	maxStates, maxDepth, runs, runDepth := 24, 4, 2, 14
	budget := 7 * time.Second
	if full {
		maxStates, maxDepth, runs, runDepth = 50, 5, 3, 20
		budget = 12 * time.Second
	}
	only := os.Getenv("OME_REPAIR_HUNT_ONLY")
	configs := repairConfigs(full, func(c repairConfig) bool {
		return keep(c) && (only == "" || strings.Contains(c.String(), only))
	})
	totals := map[string]int{}
	type example struct {
		cfg     repairConfig
		finding repairFinding
	}
	examples := map[string][]example{}
	var states, passes, allRuns int
	started := time.Now()
	for i, cfg := range configs {
		w := newRepairWorld(t, cfg)
		rep := w.explore(maxStates, maxDepth, budget*2/3)
		w.randomRuns(&rep, int64(i+1), runs, runDepth, budget/3)
		states += rep.states
		passes += w.passes
		allRuns += rep.runs
		var parts []string
		for class, c := range rep.counts {
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, depth %d, %d runs, %d passes: %s", i+1, len(configs), cfg, rep.states, rep.depth, rep.runs, w.passes, strings.Join(parts, " "))
		for class, n := range rep.counts {
			totals[class] += n
		}
		for class, f := range rep.findings {
			if len(examples[class]) < 8 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d seeded runs, %d engine passes, %s", len(configs), states, allRuns, passes, time.Since(started).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states across %d configurations", class, totals[class], len(examples[class]))
		if strings.HasPrefix(class, "named-wait") || strings.HasPrefix(class, "known-") {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state:\n      %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.picture)
		}
		switch class {
		case "no-way-out", "rebuild-never-comes", "rebuild-without-record", "rebuilt-after-held", "gang-member-alone", "churn", "error":
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// parseRepairMove reads a move back from the text the reports print, so a
// path can be replayed.
func parseRepairMove(text string) (repairMove, error) {
	var mv repairMove
	rest := text
	if i := strings.Index(rest, "+"); i >= 0 && strings.HasSuffix(rest, "p") {
		n, err := strconv.Atoi(strings.TrimSuffix(rest[i+1:], "p"))
		if err != nil {
			return mv, fmt.Errorf("move %q: bad pass count", text)
		}
		mv.after, rest = n, rest[:i]
	}
	if i := strings.Index(rest, "@"); i >= 0 {
		target := rest[i+1:]
		rest = rest[:i]
		if j := strings.Index(target, "/"); j >= 0 {
			mv.runner, target = target[j+1:], target[:j]
		}
		idx, err := strconv.Atoi(target)
		if err != nil {
			return mv, fmt.Errorf("move %q: bad instance", text)
		}
		mv.idx = int32(idx)
	}
	if i := strings.Index(rest, ":"); i >= 0 && !strings.HasPrefix(rest, "tick") {
		mv.kind, mv.arg = rest[:i], rest[i+1:]
	} else {
		mv.kind = rest
	}
	return mv, nil
}

// TestRepair_ReplayPath replays one path of a report under one
// configuration and logs the picture after every pass, for a reader who
// wants to watch a finding happen. It runs only when OME_REPAIR_HUNT_PATH
// names the path (moves joined by " -> ") and OME_REPAIR_HUNT_ONLY the
// configuration.
func TestRepair_ReplayPath(t *testing.T) {
	text := os.Getenv("OME_REPAIR_HUNT_PATH")
	only := os.Getenv("OME_REPAIR_HUNT_ONLY")
	if text == "" || only == "" {
		t.Skip("set OME_REPAIR_HUNT_PATH and OME_REPAIR_HUNT_ONLY to replay a path")
	}
	configs := repairConfigs(true, func(c repairConfig) bool { return strings.Contains(c.String(), only) })
	if len(configs) == 0 {
		t.Fatalf("no configuration carries %q", only)
	}
	w := newRepairWorld(t, configs[0])
	t.Logf("configuration: %s", configs[0])
	n := w.root()
	for _, part := range strings.Split(text, "->") {
		mv, err := parseRepairMove(strings.TrimSpace(part))
		if err != nil {
			t.Fatal(err)
		}
		h := w.restore(n.snap)
		child := &repairNode{mem: n.mem.clone(), path: append(append([]repairMove(nil), n.path...), mv)}
		child.mem.eventsAt = len(h.events)
		for i := 0; i < mv.after; i++ {
			w.pass(child, h)
			t.Logf("after %d pass(es) before %s, clock +%s:\n      %s", i+1, mv, h.clk.Now().Sub(n.snap.now).Round(time.Second), w.picture(h))
		}
		w.play(child, h, mv)
		t.Logf("move %s landed, clock +%s:\n      %s", mv, h.clk.Now().Sub(n.snap.now).Round(time.Second), w.picture(h))
		prev := w.key(h)
		for i := 0; i < w.settlePasses; i++ {
			w.pass(child, h)
			k := w.key(h)
			t.Logf("pass %d after %s, clock +%s, timers=%v:\n      %s", i+1, mv, h.clk.Now().Sub(n.snap.now).Round(time.Second), w.timerKinds(h), w.picture(h))
			if k == prev {
				child.quiet = true
				break
			}
			prev = k
		}
		w.finish(child, h)
		for _, f := range child.found {
			t.Logf("finding %s: %s", f.class, f.message)
		}
		n = child
	}
}

// repairLockWorld is the world the two locks below run in: eight single-pod
// Instances under RecreateInstanceOnPodRestart and the chart's values, with
// the force-delete policy on when a lock needs it.
func repairLockWorld(t *testing.T, forceDelete bool) *repairWorld {
	t.Helper()
	cfg := repairConfig{shape: repairShape{8, 1}, policy: v1beta1.InstanceRestartPolicyRecreateInstance,
		knobs: repairKnobs{deadlines: "chart", tries: 3, forceDelete: forceDelete}, story: storyRoom}
	return newRepairWorld(t, cfg)
}

// walk plays the moves on the node, one settle after another.
func (w *repairWorld) walk(n *repairNode, moves ...string) *repairNode {
	w.t.Helper()
	for _, text := range moves {
		mv, err := parseRepairMove(text)
		if err != nil {
			w.t.Fatal(err)
		}
		n = w.apply(n, mv)
	}
	return n
}

// eventsNaming counts the recorded events of reason that name Instance idx.
func eventsNaming(h *recoveryHarness, reason workloadtypes.EventReason, idx int32) int {
	key := workloadtypes.InstanceKey(workloadtypes.ComponentEngine, idx)
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, " "+string(reason)+" ") && strings.Contains(e, key) {
			n++
		}
	}
	return n
}

// TestRepair_AWaitDuringARebuildKeepsTheRunnersExitRecord: Instance 0's
// runner exits once after it served, and the engine rebuilds the Instance
// at once, recording the exit on the row. The rebuilt pod then waits: for
// room on the cluster, or for the apiserver's admission webhook. The wait
// must not replace the exit record: when the wait ends and the rebuilt
// pod's runner exits again inside the crash window, the engine reads the
// rebuilt set's death, counts it on the running revision's retry record
// and makes the next rebuild wait for the schedule.
func TestRepair_AWaitDuringARebuildKeepsTheRunnersExitRecord(t *testing.T) {
	// The rebuilt pod must be created inside the crash window after the
	// exit (the stuck-pod grace, 60 s under the chart's values): the
	// scheduler's wait holds a pod that already exists, so the tick to
	// the next timer is free; the webhook's wait holds the create itself,
	// and the tick would land it one second past the window, where the
	// next death is a steady-state break by design.
	for _, wait := range []struct {
		name  string
		moves []string
	}{
		{"no room on the cluster", []string{"exit@0", "no-room", "tick", "room-back"}},
		{"the admission webhook unreachable", []string{"exit@0", "webhook-down", "webhook-back"}},
	} {
		t.Run(wait.name, func(t *testing.T) {
			w := repairLockWorld(t, false)
			n := w.walk(w.root(), wait.moves...)
			h := w.restore(n.snap)
			s := h.instance(0)
			if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Incarnation != 2 || !w.servingSet(h, 0) {
				t.Fatalf("the rebuilt Instance 0 should serve at incarnation 2 once the wait ends; picture:\n%s", w.picture(h))
			}
			if s.LastFailure == nil || s.LastFailure.ContainerName == "" {
				t.Errorf("the rebuilt Instance 0 carries no failure record naming its runner after the wait (%s); the next death of its set cannot be read as the set's", terminationText(s.LastFailure))
			}
			restarts := eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 0)
			n = w.walk(n, "exit@0+1p")
			h = w.restore(n.snap)
			if b := h.findBlock(h.revV1.Name); b == nil {
				t.Errorf("the rebuilt set died inside the crash window and the running revision's retry record did not count it")
			}
			if again := eventsNaming(h, workloadtypes.EventReasonRestartTriggered, 0); again != restarts {
				t.Errorf("the engine rebuilt Instance 0 at once a second time (%d restarts after the second death, %d before) instead of waiting for the retry schedule; picture:\n%s", again, restarts, w.picture(h))
			}
		})
	}
}

// TestRepair_AHeldRepairDoesNotStopTheForceDeleteOfALostNode: the
// force-delete policy is on, Instance 0's rebuild waits on the admission
// webhook, and the node under Instance 1 stops reporting. The lost node's
// pod must be freed at the node threshold while Instance 0's repair still
// waits, and Instance 1 must serve again off the lost node once the
// webhook returns.
func TestRepair_AHeldRepairDoesNotStopTheForceDeleteOfALostNode(t *testing.T) {
	w := repairLockWorld(t, true)
	n := w.walk(w.root(), "webhook-down", "waiting:ImagePullBackOff@0", "tick-all", "node-lost@1", "tick-all", "tick-all")
	h := w.restore(n.snap)
	if s := h.instance(0); s == nil || s.Operation == nil || s.Operation.Waiting != workloadtypes.RejectionReasonAdmissionUnavailable {
		t.Fatalf("Instance 0's repair should be waiting on the admission webhook; picture:\n%s", w.picture(h))
	}
	if len(h.deadNodes) != 1 {
		t.Fatalf("one node should have stopped reporting, got %v", h.deadNodes)
	}
	if freed := eventsNaming(h, workloadtypes.EventReasonPodForceDeleted, 1); freed == 0 {
		t.Errorf("Instance 1's pod on the lost node was not force-deleted while Instance 0's repair waited; picture:\n%s", w.picture(h))
	}
	for _, pod := range h.podsOf(1) {
		if h.deadNodes[pod.Spec.NodeName] && h.kubeletStopped(pod) {
			t.Errorf("Instance 1's pod %s still sits on the lost node %s while Instance 0's repair waits", pod.Name, pod.Spec.NodeName)
		}
	}
	n = w.walk(n, "webhook-back", "tick-all", "tick-all")
	h = w.restore(n.snap)
	if !w.servingSet(h, 1) {
		t.Errorf("once the webhook returned, Instance 1 should serve again off the lost node; picture:\n%s", w.picture(h))
	}
	for _, pod := range h.podsOf(1) {
		if h.deadNodes[pod.Spec.NodeName] {
			t.Errorf("Instance 1's rebuilt pod %s sits on the lost node %s", pod.Name, pod.Spec.NodeName)
		}
	}
}

// TestRepair_HarnessSnapshotCopiesEveryField fails when the recovery
// harness grows a field the snapshot does not copy by hand.
func TestRepair_HarnessSnapshotCopiesEveryField(t *testing.T) {
	if n := reflect.TypeOf(recoveryHarness{}).NumField(); n != recoveryHarnessFields {
		t.Fatalf("recoveryHarness has %d fields, the snapshot copies %d: add the new map or slice fields to copyHarnessCollections and raise recoveryHarnessFields", n, recoveryHarnessFields)
	}
}

// TestRepair_PodFaultsEndServingOrNamed explores the faults that hit a pod
// where it runs: a runner that exits, a pod the kubelet cannot start, a
// pod deleted or evicted.
func TestRepair_PodFaultsEndServingOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool {
		switch c.story {
		case storyExit, storyWaiting, storyLost:
			return true
		}
		return false
	})
}

// TestRepair_NodeFaultsEndServingOrNamed explores the faults that come
// from under the pod: a node lost, a kubelet that stops reporting, a
// cluster with no room, and a move of an Instance.
func TestRepair_NodeFaultsEndServingOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool {
		switch c.story {
		case storyNode, storySilent, storyRoom, storyMove:
			return true
		}
		return false
	})
}

// TestRepair_PushesOfABrokenRevisionEndHeldOrNamed explores a push of a
// revision whose pods crash after serving, crash on every run, cannot pull
// their image or never become ready, with a corrective push after it.
func TestRepair_PushesOfABrokenRevisionEndHeldOrNamed(t *testing.T) {
	repairExplore(t, func(c repairConfig) bool {
		switch c.story {
		case storyPushCrashOnce, storyPushCrashLoop, storyPushBadImage, storyPushNoReady:
			return true
		}
		return false
	})
}

// podlessFleetRoll is one pass over a replica of gangs of one leader and
// seven workers, every gang Ready on version 1 with every pod of it gone,
// and version 2 pushed under RecreatePod one gang at a time. The fake
// client holds version 1's revision and no pod, so both pod reads of the
// pass find every Instance empty: every row is a repair candidate, and the
// restart selection asks each one whether its repair yields to the roll,
// which takes a row off the target that serves nothing. The group's gate
// is closed, as the other member of a group keeps it while its own batch
// is mid-step, but a row with no pod left is already outside the group's
// serving count, so its rebuild skips that gate and the per-Component
// budget alone sizes the batch: the pass rebuilds the first gang at
// version 2 and leaves the rest to the passes after it. One gang keeps
// what the pass creates, and the live pod read before each recreate, the
// same at every size. The rows are kept by an in-memory store, so what
// the pass wrote can be read after it.
type podlessFleetRoll struct {
	deps   workloadtypes.Deps
	input  workloadtypes.ReconcileInput
	plan   workloadtypes.ComponentPlan
	target *appsv1.ControllerRevision
	rows   *testAtomicMutationStore
	// pods is the size of a gang: what the pass creates for the one it
	// rebuilds.
	pods int
}

func newPodlessFleetRoll(t *testing.T, instances int) podlessFleetRoll {
	t.Helper()
	const running, pushed = "svc-engine-rev-a", "svc-engine-rev-b"
	const pods = 8
	input := fleetInput(instances, instances, pods, running)
	oneGang := intstr.FromInt32(1)
	input.DesiredSpec.Lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = &oneGang
	input.UpdateGate = func(string, workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
		return false, workloadtypes.RolloutHoldGateRatio, "the other member of the group is mid-step"
	}
	rows := installTestAtomicMutationStore(&input)
	input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, input.DesiredSpec, input.ObservedState)
	if err != nil {
		t.Fatal(err)
	}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: pushed, Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithObjects(revisionWithPodSpec(t, running, "ns", input.DesiredSpec.PodSpec)).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}
	return podlessFleetRoll{deps: deps, input: input, plan: plan, target: target, rows: rows, pods: pods}
}

// reconcile runs the whole pass: observe, plan and execute.
func (r podlessFleetRoll) reconcile(ctx context.Context) error {
	_, err := workload.Reconcile(ctx, r.deps, r.input, r.plan, r.target)
	return err
}

// repairsOpened is the number of rows the pass put under a repair.
func (r podlessFleetRoll) repairsOpened() int {
	n := 0
	for i := range r.rows.statuses {
		row := &r.rows.statuses[i]
		if row.Phase == workloadtypes.InstancePhaseRestarting || (row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationRestart) {
			n++
		}
	}
	return n
}

// podsCreated is the number of pods the pass created.
func (r podlessFleetRoll) podsCreated(ctx context.Context) (int, error) {
	pods := &corev1.PodList{}
	if err := r.deps.Client.List(ctx, pods); err != nil {
		return 0, err
	}
	return len(pods.Items), nil
}

// TestPassOverAPodlessFleetCostGrowsWithTheInstanceCount: the pass over
// 4,000 gangs that have lost every pod while version 2 is pushed costs
// about four times the pass over 1,000, not sixteen. Every row is a
// repair candidate the roll takes, and the restart selection asks the
// question once per row against one held set read once for the pass,
// instead of rebuilding that set, one row lookup per planned Instance,
// for every row it asks about; the one gang the roll rebuilds costs the
// same at either size. The sampling is
// TestBuildPlanCostGrowsWithTheInstanceCount's: each size is read as the
// fastest of its interleaved samples, with the heap collected between
// samples, so another process on the machine can only slow a sample
// down and the fastest is the one it disturbed least.
func TestPassOverAPodlessFleetCostGrowsWithTheInstanceCount(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-backed gate; skipped under -short")
	}
	const small, large = 1000, 4000
	// A cost that grows with the count gives a ratio near 4 between the two
	// sizes and one that grows with its square gives 16; the ceiling sits
	// between them, with room for a loaded machine.
	const ceiling = 10.0
	const samples = 9
	previousGCPercent := debug.SetGCPercent(1000)
	defer debug.SetGCPercent(previousGCPercent)
	ctx := logr.NewContext(context.Background(), logr.Discard())
	fastest := map[int]time.Duration{}
	for i := 0; i < samples; i++ {
		for _, n := range []int{small, large} {
			roll := newPodlessFleetRoll(t, n)
			goruntime.GC()
			start := time.Now()
			err := roll.reconcile(ctx)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			if opened := roll.repairsOpened(); opened != 0 {
				t.Fatalf("%d gangs: the pass opened %d repairs on rows the roll takes", n, opened)
			}
			created, err := roll.podsCreated(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if created != roll.pods {
				t.Fatalf("%d gangs: the pass created %d pods, not the %d of the one gang the roll rebuilds", n, created, roll.pods)
			}
			if i == 0 || elapsed < fastest[n] {
				fastest[n] = elapsed
			}
		}
	}
	ratio := float64(fastest[large]) / float64(fastest[small])
	t.Logf("pass over gangs with no pods left: %v at %d gangs, %v at %d, %.1fx (ceiling %.0fx)", fastest[small], small, fastest[large], large, ratio, ceiling)
	if ratio > ceiling {
		t.Errorf("the pass over %d gangs costs %.1fx the pass over %d, above the ceiling of %.0fx: the cost grows faster than the count", large, ratio, small, ceiling)
	}
}
