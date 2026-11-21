package workload_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the real engine through a manager restart at
// every step of an in-place roll. An in-place roll keeps each pod and
// patches its image: the engine writes the Instance's row Updating, turns
// the pod's serving gate off, waits for the routed Service to drop the
// pod, patches the pod, and once the kubelet runs the new image turns the
// gate back on and writes the row Ready. The budget (maxUnavailable) says
// how many Instances may be out of rotation at once. The manager keeps
// nothing of a roll in memory: a restarted manager must find the Instances
// already out from the rows and the pods.
//
// The world is one Component of single-pod Instances on the recovery
// harness, with no coordination gate wired, so the Instance engine's own
// budget is the only pacing. Every manager pass is workload.Reconcile:
// workload.Plan reads the selection (escalation.CurrentUnavailableInFlight
// over the rows, ops.InstanceNotServingOutsideRoll and
// ops.RolledInstanceNotServing over the pass's pod list), the update pass
// admits starts through escalation.BudgetDenies and runs ops.UpdateWithPods
// (the in-place phases of ops/update_inplace.go, the row writes of
// status/decision.go that open and end an in-place roll, and
// podreadiness.MarkPodNotServing). The kubelet and the EndpointSlice
// controller are steps of their own, so a pass can run before the kubelet
// has cleared Ready on a pod whose gate is off. The headless Service
// publishes not-ready addresses, as the rendered one does, so the status
// publication (status.CountAvailablePods) is read against the slices
// a real cluster holds.
//
// The plain roll from version 1 to version 2 is recorded pass by pass
// with the number of apiserver writes each pass made. Every probe restores
// the world before one pass and ends the manager there: the first k writes
// of the pass land and every later call fails, or the whole pass lands and
// the restart comes after it. The write the manager was sending when it
// died may be kept in flight and land after the next manager's first pass.
// The world then moves without a manager for the Lease wait (the kubelet
// alone, the kubelet and the endpoints, or both twice), a second restart
// may cut the new manager's first pass, and the roll is played to its end.
//
// The new manager's first pass may also read an older picture than the
// cluster holds. Its rows and its pods come from an informer cache, and a
// cache filled from a lagging apiserver can be behind what the manager
// before it acted on. The model keeps the rows and the pods every step of
// the recorded roll left, and a probe hands the first pass the rows of one
// step and the pods of another; every write still goes to the cluster as
// it is, and every read the engine makes live stays live.
//
// After every step, and after every write a pass lands, the model reads
// the pods and the rows: no more Instances than the budget have the gate
// off, are out of rotation, or carry an open roll; a gate is off only
// under an open roll; no pod is replaced; no repair opens; no Instance
// that finished rolls again; and the roll ends with every Instance on
// version 2. OME_RESTART_IN_PLACE_HUNT=full widens the shapes and the
// probes; OME_RESTART_IN_PLACE_HUNT_ONLY=<text> keeps the shapes whose
// name carries the text.
//
// The last two tests hand the older picture to a RecreatePod roll and to a
// SurgeThenDrain roll. Those two entries open a new attempt without
// comparing the live row with the pass's copy, and the tests hold what
// the engine does with that picture.

const (
	restartRollFullEnv = "OME_RESTART_IN_PLACE_HUNT"
	restartRollOnlyEnv = "OME_RESTART_IN_PLACE_HUNT_ONLY"
	// restartRollPassGap is the model clock between two manager passes.
	restartRollPassGap = 2 * time.Second
	// restartRollShortWait is the wait for a Lease the old manager released
	// as it stopped.
	restartRollShortWait = time.Second
	// restartRollTicksPerInstance bounds the passes a roll may take, per
	// Instance, before it is read as never ending.
	restartRollTicksPerInstance = 16
	// restartRollAllWrites is the probe whose restart comes after the
	// pass: every write of the pass lands.
	restartRollAllWrites = -1
	// restartRollNoSecond is the probe with one restart only.
	restartRollNoSecond = -2
)

// restartRollShape is one configuration of the roll.
type restartRollShape struct {
	instances int32
	budget    int32
	strategy  workloadtypes.UpdateStrategyType
	policy    workloadtypes.RestartPolicy
	// slowNode runs the kubelet and the endpoint controller after every
	// second pass, so a pass meets pods the node has not caught up on.
	slowNode bool
	// sparse keeps the probes that restart after a pass, with or without
	// the node catching up, and after each prefix of its writes.
	sparse bool
}

func (s restartRollShape) String() string {
	node := "prompt-node"
	if s.slowNode {
		node = "slow-node"
	}
	return fmt.Sprintf("%d-at-%d-%s-%s-%s", s.instances, s.budget, s.strategy, s.policy, node)
}

// restartRollGap is what the node does while no manager runs.
type restartRollGap uint8

const (
	restartGapNone restartRollGap = iota
	restartGapKubelet
	restartGapNode
	restartGapNodeTwice
)

var restartRollGaps = []restartRollGap{restartGapNone, restartGapKubelet, restartGapNode, restartGapNodeTwice}

var restartRollGapNames = [...]string{"nothing", "kubelet", "kubelet+endpoints", "kubelet+endpoints twice"}

// restartRollProbe is one restart: the pass the manager dies in or after,
// how many of its writes land, whether the next write stays in flight, the
// Lease wait and what the node does in it, and the writes of the new
// manager's first pass that land before a second restart.
type restartRollProbe struct {
	tick  int
	lands int
	late  bool
	wait  time.Duration
	gap   restartRollGap
	again int
	// rowsBack and podsBack are how many recorded steps behind the pass
	// the first pass of the new manager reads its rows and its pods; zero
	// reads them as they are.
	rowsBack int
	podsBack int
}

func (p restartRollProbe) String() string {
	lands := "every write lands"
	if p.lands != restartRollAllWrites {
		lands = fmt.Sprintf("%d write(s) land", p.lands)
	}
	if p.late {
		lands += ", the next in flight"
	}
	again := ""
	if p.again != restartRollNoSecond {
		again = fmt.Sprintf(", second restart after %d write(s) of the first pass", p.again)
		if p.again == restartRollAllWrites {
			again = ", second restart after the first pass"
		}
	}
	behind := ""
	if p.rowsBack != 0 || p.podsBack != 0 {
		behind = fmt.Sprintf(", first pass reads rows %d step(s) and pods %d step(s) behind", p.rowsBack, p.podsBack)
	}
	return fmt.Sprintf("restart at pass %d (%s), Lease wait %s, node does %s%s%s", p.tick, lands, p.wait, restartRollGapNames[p.gap], again, behind)
}

// restartRollTick is one pass of the recorded plain roll: the world
// before it and the writes it made.
type restartRollTick struct {
	before *restartRollSnapshot
	writes int
	// seen is the recorded step the pass left: an index into the world's
	// observations.
	seen int
}

// restartRollObservation is the rows and the pods one step left, as a
// cache that stopped there would serve them.
type restartRollObservation struct {
	rows []workloadtypes.InstanceStatus
	pods []corev1.Pod
}

type restartRollSnapshot struct {
	harness  *repairSnapshot
	finished map[int32]bool
}

// restartRollReading is what one step leaves: the Instances whose pod has
// the serving gate off, the Instances with no pod Ready and in rotation,
// and the Instances whose row carries an open roll.
type restartRollReading struct {
	gateOff  []int32
	out      []int32
	updating []int32
}

type restartRollWorld struct {
	t       *testing.T
	shape   restartRollShape
	h       *recoveryHarness
	rebuild *repairWorld
	// born is the pod each Instance had when the roll began.
	born map[int32]types.UID
	// finished lists the Instances a step has shown settled on version 2.
	finished map[int32]bool
	// findings keeps the first picture of every class a step broke.
	findings map[string]string
	// path names the steps since the last restore, for a picture.
	path   []string
	passes int
	digest uint64
	// passErr is the error of the last pass, nil when it returned none.
	passErr error
	// seen holds what every step of the recorded roll left, the push
	// first.
	seen []restartRollObservation
	// behind, when set, is what the next pass reads through its cache; the
	// pass takes it and clears it.
	behind *restartRollObservation
}

// newRestartRollWorld converges the shape on version 1 with the routed
// model on and pushes version 2, an image change alone. The budget is the
// number of Instances out at once, or under SurgeThenDrain the number of
// extra pods at once with no Instance out.
func newRestartRollWorld(t *testing.T, shape restartRollShape) *restartRollWorld {
	t.Helper()
	surge, unavailable := int32(0), shape.budget
	if shape.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
		surge, unavailable = shape.budget, 0
	}
	h := offTargetCrashHarness(t, false, shape.strategy, surge, unavailable, shape.policy, shape.instances)
	h.routed = true
	w := &restartRollWorld{
		t: t, shape: shape, h: h,
		rebuild:  &repairWorld{t: t, scheme: makeScheme(t)},
		born:     map[int32]types.UID{},
		finished: map[int32]bool{},
		findings: map[string]string{},
	}
	for i := 0; i < 2; i++ {
		w.kubelet()
		w.endpoints()
		w.pass(nil, nil)
	}
	for idx, pods := range w.podsByInstance() {
		if len(pods) == 1 {
			w.born[idx] = pods[0].UID
		}
	}
	if len(w.born) != int(shape.instances) {
		t.Fatalf("%s: %d Instances have one pod before the push, want %d", shape, len(w.born), shape.instances)
	}
	h.setTarget(h.revFixed, fixedImage)
	return w
}

func (w *restartRollWorld) kubelet() {
	w.h.kubelet()
	w.path = append(w.path, "kubelet")
}

// endpoints runs the endpoint controller and then lists every live pod of
// the headless Service as Ready, as a Service that publishes not-ready
// addresses does.
func (w *restartRollWorld) endpoints() {
	h := w.h
	h.endpointController()
	slice := &discoveryv1.EndpointSlice{}
	name := query.HeadlessServiceName(recoveryOwner, h.component) + "-endpoints"
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: recoveryNS, Name: name}, slice); err != nil {
		w.t.Fatalf("read the headless Service's slice: %v", err)
	}
	ready := true
	for i := range slice.Endpoints {
		if terminating := slice.Endpoints[i].Conditions.Terminating; terminating == nil || !*terminating {
			slice.Endpoints[i].Conditions.Ready = &ready
		}
	}
	if err := h.c.Update(h.ctx, slice); err != nil {
		w.t.Fatalf("publish not-ready addresses: %v", err)
	}
	w.path = append(w.path, "endpoints")
}

// pass runs one manager pass: a new process every time, with nothing kept
// from the pass before. cut, when set, is the manager dying inside the
// pass; sent, when set, receives the write it was sending when it died.
// It reports whether the manager died.
func (w *restartRollWorld) pass(cut *passCut, sent *func() error) bool {
	h := w.h
	live := h.c
	w.passErr = nil
	watched := &writeWatch{Client: live, landed: func() {
		held := h.c
		h.c = live
		w.check()
		h.c = held
	}}
	h.c = watched
	if cut != nil {
		dying := &cutClient{Client: watched, cut: cut}
		h.c = dying
		if sent != nil {
			h.c = &inFlightClient{cutClient: dying, sent: sent}
		}
	}
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	if h.recorder != nil {
		deps.Recorder = h.eventSink()
	}
	in := h.buildInput()
	// No coordination gate is wired, and an adapter that wires none hands
	// the pass no collector for the holds its operations observe.
	in.DrainHolds = nil
	if behind := w.behind; behind != nil {
		w.behind = nil
		if behind.rows != nil {
			in.ObservedState.InstanceStatuses = cloneTestInstanceStatuses(behind.rows)
		}
		if behind.pods != nil {
			deps.Client = &agedPodCache{Client: h.c, pods: behind.pods}
		}
	}
	plan, err := workload.BuildPlan(h.component, h.desired, in.ObservedState)
	if err != nil {
		w.t.Fatalf("BuildPlan: %v", err)
	}
	_, w.passErr = workload.Reconcile(h.ctx, deps, in, plan, h.target)
	h.c = live
	h.drainEvents()
	w.passes++
	if cut != nil && cut.hit {
		w.passErr = nil
		w.path = append(w.path, fmt.Sprintf("pass cut after %d write(s)", cut.writes-1))
		return true
	}
	w.path = append(w.path, "pass")
	h.publish(plan)
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	return false
}

// countedPass runs one pass to its end and returns how many writes it
// sent.
func (w *restartRollWorld) countedPass() int {
	count := &passCut{left: math.MaxInt32}
	w.pass(count, nil)
	return count.writes
}

// agedPodCache is an informer cache whose pods stopped at an earlier step:
// every list and get of a pod is answered from that step, everything else
// from the client it wraps.
type agedPodCache struct {
	client.Client
	pods []corev1.Pod
}

func (c *agedPodCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	out, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	out.Items = out.Items[:0]
	for i := range c.pods {
		pod := &c.pods[i]
		if options.Namespace != "" && pod.Namespace != options.Namespace {
			continue
		}
		if options.LabelSelector != nil && !options.LabelSelector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		out.Items = append(out.Items, *pod.DeepCopy())
	}
	return nil
}

func (c *agedPodCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	out, ok := obj.(*corev1.Pod)
	if !ok {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	for i := range c.pods {
		if c.pods[i].Namespace == key.Namespace && c.pods[i].Name == key.Name {
			c.pods[i].DeepCopyInto(out)
			return nil
		}
	}
	return apierrors.NewNotFound(corev1.Resource("pods"), key.Name)
}

// observe copies the rows and the pods as they are.
func (w *restartRollWorld) observe() restartRollObservation {
	list := &corev1.PodList{}
	if err := w.h.c.List(w.h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	return restartRollObservation{rows: w.h.irStatuses(), pods: list.Items}
}

// readBehind makes the next pass read its rows rowsBack recorded steps
// before step now and its pods podsBack steps before it. Zero leaves that
// half as the cluster holds it; a count past the push reads the push.
func (w *restartRollWorld) readBehind(now, rowsBack, podsBack int) {
	if rowsBack == 0 && podsBack == 0 {
		return
	}
	behind := &restartRollObservation{}
	if rowsBack != 0 {
		behind.rows = w.seen[max(now-rowsBack, 0)].rows
	}
	if podsBack != 0 {
		behind.pods = w.seen[max(now-podsBack, 0)].pods
	}
	w.behind = behind
}

// selection is the update selection the plan of a pass run now would
// read: what a new manager counts before it admits a start.
func (w *restartRollWorld) selection() *workload.UpdateSelection {
	h := w.h
	in := h.buildInput()
	in.ObservedState.IndexRows()
	in.RevisionBorn = h.target.CreationTimestamp.Time
	in.Pacing, in.PromoteWindow, in.PassWake = &workloadtypes.APIPacing{}, &workloadtypes.PromoteWindow{}, &workloadtypes.PassWake{}
	in.Swept, in.Held = &workloadtypes.SweptPods{}, &workloadtypes.HeldInstances{}
	plan, err := workload.BuildPlan(h.component, h.desired, in.ObservedState)
	if err != nil {
		w.t.Fatalf("BuildPlan: %v", err)
	}
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.NewObservedSnapshot(deps, in, plan.Component, in.ObservedState.InstanceStatuses)
	decision, err := workload.Plan(h.ctx, in, plan, h.target, snapshot)
	if err != nil {
		w.t.Fatalf("Plan: %v", err)
	}
	for _, action := range decision.Actions {
		if action.Update != nil {
			return action.Update
		}
	}
	return nil
}

// writeWatch tells the model when a write has landed, so it reads the
// cluster between two writes of one pass as well as after the pass.
type writeWatch struct {
	client.Client
	landed func()
}

func (c *writeWatch) wrote(err error) error {
	if err == nil {
		c.landed()
	}
	return err
}

func (c *writeWatch) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.wrote(c.Client.Update(ctx, obj, opts...))
}

func (c *writeWatch) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return c.wrote(c.Client.Patch(ctx, obj, patch, opts...))
}

func (c *writeWatch) Status() client.SubResourceWriter {
	return &writeWatchStatus{SubResourceWriter: c.Client.Status(), owner: c}
}

// writeWatchStatus is the status writer of writeWatch.
type writeWatchStatus struct {
	client.SubResourceWriter
	owner *writeWatch
}

func (s *writeWatchStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.owner.wrote(s.SubResourceWriter.Update(ctx, obj, opts...))
}

func (s *writeWatchStatus) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.owner.wrote(s.SubResourceWriter.Patch(ctx, obj, patch, opts...))
}

// inFlightClient is the apiserver as a dying manager sees it, for a
// manager whose first refused write was already on the wire: the write is
// kept as it was sent, to land later.
type inFlightClient struct {
	*cutClient
	sent *func() error
}

// sending reports whether the next write is the one the cut refuses first.
func (c *inFlightClient) sending() bool {
	return c.cut.left <= 0 && !c.cut.hit && *c.sent == nil
}

func (c *inFlightClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.sending() {
		wire := obj.DeepCopyObject().(client.Object)
		*c.sent = func() error { return c.Client.Update(ctx, wire, opts...) }
	}
	return c.cutClient.Update(ctx, obj, opts...)
}

func (c *inFlightClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.sending() {
		wire := obj.DeepCopyObject().(client.Object)
		*c.sent = func() error { return c.Client.Patch(ctx, wire, patch, opts...) }
	}
	return c.cutClient.Patch(ctx, obj, patch, opts...)
}

func (c *inFlightClient) Status() client.SubResourceWriter {
	return &inFlightStatusWriter{SubResourceWriter: c.cutClient.Status(), live: c.Client.Status(), owner: c}
}

// inFlightStatusWriter is the status writer of inFlightClient.
type inFlightStatusWriter struct {
	client.SubResourceWriter
	live  client.SubResourceWriter
	owner *inFlightClient
}

func (s *inFlightStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.owner.sending() {
		wire := obj.DeepCopyObject().(client.Object)
		*s.owner.sent = func() error { return s.live.Update(ctx, wire, opts...) }
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s *inFlightStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.owner.sending() {
		wire := obj.DeepCopyObject().(client.Object)
		*s.owner.sent = func() error { return s.live.Patch(ctx, wire, patch, opts...) }
	}
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func (w *restartRollWorld) podsByInstance() map[int32][]*corev1.Pod {
	list := &corev1.PodList{}
	if err := w.h.c.List(w.h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return query.BucketPodsByInstanceIdx(pods)
}

func (w *restartRollWorld) read() restartRollReading {
	return w.readOf(w.podsByInstance(), w.h.irStatuses())
}

// readOf is read over pods and rows already listed.
func (w *restartRollWorld) readOf(byInstance map[int32][]*corev1.Pod, rows []workloadtypes.InstanceStatus) restartRollReading {
	var r restartRollReading
	for idx := int32(0); idx < w.shape.instances; idx++ {
		gateOn, serving := false, false
		for _, pod := range byInstance[idx] {
			gateOn = gateOn || podreadiness.IsServing(pod)
			serving = serving || podreadiness.ReadyAndServing(pod)
		}
		if !gateOn {
			r.gateOff = append(r.gateOff, idx)
		}
		if !serving {
			r.out = append(r.out, idx)
		}
	}
	for _, row := range rows {
		if row.Phase == workloadtypes.InstancePhaseUpdating || row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationUpdate {
			r.updating = append(r.updating, row.Index)
		}
	}
	return r
}

// note records the first picture of a class.
func (w *restartRollWorld) note(class, format string, args ...any) {
	if _, seen := w.findings[class]; seen {
		return
	}
	w.findings[class] = fmt.Sprintf(format, args...) + "\n  steps: " + strings.Join(w.path, ", ") + "\n" + w.picture()
}

// picture is the rows and the pods, one line per Instance.
func (w *restartRollWorld) picture() string {
	rows := map[int32]workloadtypes.InstanceStatus{}
	for _, row := range w.h.irStatuses() {
		rows[row.Index] = row
	}
	byInstance := w.podsByInstance()
	var b strings.Builder
	for idx := int32(0); idx < w.shape.instances; idx++ {
		row := rows[idx]
		step := "-"
		if row.Operation != nil {
			step = string(row.Operation.Type) + "/" + row.Operation.Step
		}
		fmt.Fprintf(&b, "  Instance %d: row %s %s on %q", idx, row.Phase, step, row.RunningRevision)
		for _, pod := range byInstance[idx] {
			fmt.Fprintf(&b, "; pod image=%s gate=%v ready=%v", pod.Spec.Containers[0].Image, podreadiness.IsServing(pod), podreadiness.IsPodReady(pod))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// check reads the world after a step and records every class it breaks.
func (w *restartRollWorld) check() restartRollReading {
	pods, rows := w.podsByInstance(), w.h.irStatuses()
	r := w.readOf(pods, rows)
	budget := int(w.shape.budget)
	if len(r.gateOff) > budget {
		w.note("more-gates-off-than-the-budget", "Instances %v have the serving gate off at a budget of %d", r.gateOff, budget)
	}
	if len(r.out) > budget {
		w.note("more-instances-out-than-the-budget", "Instances %v are out of rotation at a budget of %d", r.out, budget)
	}
	if len(r.updating) > budget {
		w.note("more-rows-rolling-than-the-budget", "Instances %v carry an open roll at a budget of %d", r.updating, budget)
	}
	for _, idx := range r.gateOff {
		if !slices.Contains(r.updating, idx) {
			w.note("a-gate-is-off-under-no-open-roll", "the serving gate of Instance %d is off and its row carries no open roll", idx)
		}
	}
	if w.passErr != nil {
		w.note("a-pass-fails", "the pass returned %v", w.passErr)
	}
	w.checkPods(pods)
	w.checkRows(rows)
	hash := fnv.New64a()
	fmt.Fprintf(hash, "%d|%v|%v|%v", w.digest, r.gateOff, r.out, r.updating)
	w.digest = hash.Sum64()
	return r
}

// checkPods holds every Instance to the pod it began the roll with.
func (w *restartRollWorld) checkPods(byInstance map[int32][]*corev1.Pod) {
	for idx, pods := range byInstance {
		if len(pods) != 1 || pods[0].UID != w.born[idx] || pods[0].DeletionTimestamp != nil {
			w.note("a-pod-is-replaced", "Instance %d no longer has the one pod it began the roll with", idx)
		}
	}
}

// checkRows reads the rows for a repair and for a finished Instance
// rolling again.
func (w *restartRollWorld) checkRows(rows []workloadtypes.InstanceStatus) {
	for _, row := range rows {
		if row.Phase == workloadtypes.InstancePhaseRestarting || row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationRestart {
			w.note("a-repair-opens", "Instance %d is under a repair during the roll", row.Index)
		}
		settled := w.settled(row)
		if w.finished[row.Index] && !settled {
			w.note("a-finished-instance-rolls-again", "Instance %d had finished on version 2 and its row reads %s again", row.Index, row.Phase)
		}
		if settled {
			w.finished[row.Index] = true
		}
	}
}

// settled reports whether a row is Ready on version 2 with no operation.
func (w *restartRollWorld) settled(row workloadtypes.InstanceStatus) bool {
	return row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil && row.RunningRevision == w.h.target.Name
}

// complete reports whether every Instance serves version 2 with its row
// settled.
func (w *restartRollWorld) complete() bool {
	settled := int32(0)
	rows := w.h.irStatuses()
	for _, row := range rows {
		if w.settled(row) {
			settled++
		}
	}
	if settled != w.shape.instances {
		return false
	}
	byInstance := w.podsByInstance()
	if r := w.readOf(byInstance, rows); len(r.out) != 0 || len(r.updating) != 0 {
		return false
	}
	for _, pods := range byInstance {
		for _, pod := range pods {
			if pod.Spec.Containers[0].Image != fixedImage {
				return false
			}
		}
	}
	return true
}

func (w *restartRollWorld) snapshot() *restartRollSnapshot {
	return &restartRollSnapshot{harness: snapshotHarness(w.h), finished: cloneMap(w.finished)}
}

func (w *restartRollWorld) restore(snap *restartRollSnapshot) {
	w.h = w.rebuild.restore(snap.harness)
	w.finished = cloneMap(snap.finished)
	w.path = w.path[:0]
	w.digest = 0
	w.passErr = nil
}

// nodeRuns reports whether the node catches up after the pass of tick.
func (w *restartRollWorld) nodeRuns(tick int) bool {
	return !w.shape.slowNode || tick%2 == 1
}

// record plays the plain roll and returns its passes.
func (w *restartRollWorld) record() []restartRollTick {
	w.t.Helper()
	var ticks []restartRollTick
	bound := restartRollTicksPerInstance * int(w.shape.instances)
	w.seen = append(w.seen[:0], w.observe())
	for tick := 0; !w.complete(); tick++ {
		if tick >= bound {
			w.t.Fatalf("%s: the plain roll is not complete after %d passes\n%s", w.shape, bound, w.picture())
		}
		snap := w.snapshot()
		count := &passCut{left: math.MaxInt32}
		w.pass(count, nil)
		ticks = append(ticks, restartRollTick{before: snap, writes: count.writes, seen: len(w.seen)})
		w.seen = append(w.seen, w.observe())
		w.h.clk.Step(restartRollPassGap)
		w.check()
		if w.nodeRuns(tick) {
			w.kubelet()
			w.endpoints()
			w.seen = append(w.seen, w.observe())
			w.check()
		}
	}
	return ticks
}

// playOut runs the roll on from tick until it is complete.
func (w *restartRollWorld) playOut(tick int) {
	limit := restartRollTicksPerInstance * int(w.shape.instances)
	for bound := tick + limit; !w.complete(); tick++ {
		if tick >= bound {
			w.note("the-roll-never-ends", "the roll is not complete %d passes after the restart", limit)
			return
		}
		if w.nodeRuns(tick) {
			w.kubelet()
			w.endpoints()
			w.check()
		}
		w.pass(nil, nil)
		w.h.clk.Step(restartRollPassGap)
		w.check()
	}
}

// idle moves the world through the Lease wait with no manager.
func (w *restartRollWorld) idle(wait time.Duration, gap restartRollGap) {
	w.h.clk.Step(wait)
	rounds := 0
	switch gap {
	case restartGapKubelet:
		w.kubelet()
	case restartGapNode:
		rounds = 1
	case restartGapNodeTwice:
		rounds = 2
	}
	for i := 0; i < rounds; i++ {
		w.kubelet()
		w.endpoints()
	}
	w.check()
}

// cutAfter is the manager dying after lands writes of a pass, nil when
// every write lands.
func cutAfter(lands int) *passCut {
	if lands == restartRollAllWrites {
		return nil
	}
	return &passCut{left: lands}
}

// run plays one probe from the recorded roll to the roll's end.
func (w *restartRollWorld) run(ticks []restartRollTick, p restartRollProbe) {
	w.restore(ticks[p.tick].before)
	var sent func() error
	var keep *func() error
	if p.late {
		keep = &sent
	}
	w.pass(cutAfter(p.lands), keep)
	w.check()
	w.idle(p.wait, p.gap)
	if p.again != restartRollNoSecond {
		w.pass(cutAfter(p.again), nil)
		w.check()
		w.idle(p.wait, restartGapNone)
	}
	w.readBehind(ticks[p.tick].seen, p.rowsBack, p.podsBack)
	w.pass(nil, nil)
	w.h.clk.Step(restartRollPassGap)
	w.check()
	if sent != nil {
		landed := sent() == nil
		w.path = append(w.path, fmt.Sprintf("the write in flight arrives (landed=%v)", landed))
		w.check()
	}
	w.playOut(p.tick + 1)
}

// restartRollProbes lists the probes of one recorded roll. The bounded
// list restarts at every pass: after each prefix of its writes under every
// node gap, with the cut write in flight, after a Lease left to expire,
// and twice in a row. The full list crosses the long wait and the write in
// flight with every gap and cuts the second manager's first pass at every
// write of the recorded one.
func (w *restartRollWorld) restartRollProbes(ticks []restartRollTick, full bool) []restartRollProbe {
	if w.shape.sparse {
		return append(sparseRestartProbes(ticks), w.staleReadProbes(ticks, false)...)
	}
	long := 2 * w.h.stuckGrace
	var probes []restartRollProbe
	for tick, recorded := range ticks {
		for lands := restartRollAllWrites; lands < recorded.writes; lands++ {
			for _, gap := range restartRollGaps {
				probes = append(probes, restartRollProbe{tick: tick, lands: lands, wait: restartRollShortWait, gap: gap, again: restartRollNoSecond})
				if full {
					probes = append(probes, restartRollProbe{tick: tick, lands: lands, wait: long, gap: gap, again: restartRollNoSecond})
				}
				if lands != restartRollAllWrites && (full || gap == restartGapNone) {
					probes = append(probes, restartRollProbe{tick: tick, lands: lands, late: true, wait: restartRollShortWait, gap: gap, again: restartRollNoSecond})
				}
			}
			if full || lands == restartRollAllWrites {
				probes = append(probes, w.secondRestarts(tick, lands, recorded.writes, full)...)
			}
		}
		if !full {
			probes = append(probes, restartRollProbe{tick: tick, lands: restartRollAllWrites, wait: long, gap: restartGapNode, again: restartRollNoSecond})
		}
	}
	return append(probes, w.staleReadProbes(ticks, full)...)
}

// restartRollBacks are how far behind the bounded list lets a first pass
// read, in recorded steps: one, two and three writes back, most of one
// Instance's roll, a whole wave and two waves. Every list also reads all
// the way back to the push.
var restartRollBacks = []int{1, 2, 3, 4, 6, 8, 12, 16, 24}

// restartRollSparseBacks are the steps behind the sparse probe list reads:
// one write back and one whole wave. It reads back to the push as well.
var restartRollSparseBacks = []int{1, 12}

// restartRollCrossInstances is the largest shape whose full list reads
// every step back and crosses every number of steps behind for the rows
// with every one for the pods.
const restartRollCrossInstances = 6

// staleReadProbes restarts after every pass and hands the new manager's
// first pass an older picture: the rows and the pods the same number of
// steps behind, then each alone, for the steps of restartRollBacks and
// the push. The bounded sparse shape reads the steps of
// restartRollSparseBacks. For a shape of at most
// restartRollCrossInstances Instances the full list does so for every
// step back and crosses the two.
func (w *restartRollWorld) staleReadProbes(ticks []restartRollTick, full bool) []restartRollProbe {
	var probes []restartRollProbe
	backs := restartRollBacks
	if w.shape.sparse {
		backs = restartRollSparseBacks
	}
	behind := func(tick, rows, pods int) {
		probes = append(probes, restartRollProbe{tick: tick, lands: restartRollAllWrites, wait: restartRollShortWait, gap: restartGapNone, again: restartRollNoSecond, rowsBack: rows, podsBack: pods})
	}
	cross := full && w.shape.instances <= restartRollCrossInstances
	for tick, recorded := range ticks {
		for _, back := range stepsBack(recorded.seen, cross, backs) {
			behind(tick, back, back)
			behind(tick, back, 0)
			behind(tick, 0, back)
			for other := 1; cross && other <= recorded.seen; other++ {
				if other != back {
					behind(tick, back, other)
				}
			}
		}
	}
	return probes
}

// stepsBack lists how far behind a first pass reads after the pass that
// left step seen: every step when every is set, else the steps of backs
// within reach and the push itself.
func stepsBack(seen int, every bool, backs []int) []int {
	var out []int
	for back := 1; back <= seen; back++ {
		if every || back == seen || slices.Contains(backs, back) {
			out = append(out, back)
		}
	}
	return out
}

// sparseRestartProbes restarts after every pass, with and without the node
// catching up in the wait, and after each prefix of the pass's writes.
func sparseRestartProbes(ticks []restartRollTick) []restartRollProbe {
	var probes []restartRollProbe
	for tick, recorded := range ticks {
		probes = append(probes,
			restartRollProbe{tick: tick, lands: restartRollAllWrites, wait: restartRollShortWait, gap: restartGapNone, again: restartRollNoSecond},
			restartRollProbe{tick: tick, lands: restartRollAllWrites, wait: restartRollShortWait, gap: restartGapNode, again: restartRollNoSecond})
		for lands := 0; lands < recorded.writes; lands++ {
			probes = append(probes, restartRollProbe{tick: tick, lands: lands, wait: restartRollShortWait, gap: restartGapNone, again: restartRollNoSecond})
		}
	}
	return probes
}

// secondRestarts are the probes whose new manager dies in its first pass.
func (w *restartRollWorld) secondRestarts(tick, lands, writes int, full bool) []restartRollProbe {
	cuts := []int{restartRollAllWrites, 0, 1}
	if full {
		cuts = cuts[:1]
		for k := 0; k <= writes+1; k++ {
			cuts = append(cuts, k)
		}
	}
	probes := make([]restartRollProbe, 0, len(cuts))
	for _, again := range cuts {
		probes = append(probes, restartRollProbe{tick: tick, lands: lands, wait: restartRollShortWait, gap: restartGapNode, again: again})
	}
	return probes
}

// restartRollResult is one shape's exploration.
type restartRollResult struct {
	shape  restartRollShape
	ticks  int
	probes int
	passes int
	digest uint64
	// findings are the classes the probes whose passes all read the
	// cluster as it is broke; behind, the classes the probes whose first
	// pass reads an older picture broke.
	findings map[string]string
	behind   map[string]string
	took     time.Duration
}

// exploreRestartRoll records the shape's plain roll and plays every probe.
func exploreRestartRoll(t *testing.T, shape restartRollShape, full bool) restartRollResult {
	t.Helper()
	began := time.Now()
	w := newRestartRollWorld(t, shape)
	ticks := w.record()
	if len(w.findings) != 0 {
		t.Fatalf("%s: the plain roll breaks the model's invariants: %v", shape, w.findings)
	}
	result := restartRollResult{shape: shape, ticks: len(ticks), findings: map[string]string{}, behind: map[string]string{}}
	for _, probe := range w.restartRollProbes(ticks, full) {
		w.findings = map[string]string{}
		w.run(ticks, probe)
		result.probes++
		hash := fnv.New64a()
		fmt.Fprintf(hash, "%d|%d", result.digest, w.digest)
		result.digest = hash.Sum64()
		broke := result.findings
		if probe.rowsBack != 0 || probe.podsBack != 0 {
			broke = result.behind
		}
		for class, picture := range w.findings {
			if _, seen := broke[class]; !seen {
				broke[class] = probe.String() + ": " + picture
			}
		}
	}
	result.passes, result.took = w.passes, time.Since(began)
	return result
}

// restartRollShapes lists the configurations: six Instances two at a time
// under both in-place strategies and both restart policies, one of them
// again on a slow node, and twelve at two, the size of a larger decode
// side, on the sparse probe list. The full list runs both sizes on both
// nodes under every strategy and policy, on the whole probe list.
func restartRollShapes(full bool) []restartRollShape {
	var shapes []restartRollShape
	for _, strategy := range []workloadtypes.UpdateStrategyType{workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyInPlaceOnly} {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			shapes = append(shapes, restartRollShape{instances: 6, budget: 2, strategy: strategy, policy: policy})
			if full {
				shapes = append(shapes,
					restartRollShape{instances: 6, budget: 2, strategy: strategy, policy: policy, slowNode: true},
					restartRollShape{instances: 12, budget: 2, strategy: strategy, policy: policy},
					restartRollShape{instances: 12, budget: 2, strategy: strategy, policy: policy, slowNode: true})
			}
		}
	}
	if !full {
		shapes = append(shapes,
			restartRollShape{instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible, policy: workloadtypes.RestartPolicyRecreateInstance, slowNode: true},
			restartRollShape{instances: 12, budget: 2, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible, policy: workloadtypes.RestartPolicyRecreateInstance, sparse: true})
	}
	return shapes
}

// TestRestartMidInPlaceRoll_ARestartAtAnyStepKeepsTheRollWithinItsBudget
// restarts the manager at every write of every pass of the roll and holds
// every step after it to the budget: at most two Instances out of
// rotation, no pod replaced, no repair, no finished Instance rolled again,
// and the roll complete on version 2. It holds whether the new manager's
// first pass reads the cluster as it is or an older picture of the rows,
// of the pods, or of both.
func TestRestartMidInPlaceRoll_ARestartAtAnyStepKeepsTheRollWithinItsBudget(t *testing.T) {
	full := os.Getenv(restartRollFullEnv) == "full"
	only := os.Getenv(restartRollOnlyEnv)
	var mu sync.Mutex
	var results []restartRollResult
	began := time.Now()
	t.Run("shapes", func(t *testing.T) {
		for _, shape := range restartRollShapes(full) {
			if only != "" && !strings.Contains(shape.String(), only) {
				continue
			}
			t.Run(shape.String(), func(t *testing.T) {
				t.Parallel()
				result := exploreRestartRoll(t, shape, full)
				for class, picture := range result.findings {
					t.Errorf("%s: %s: %s", shape, class, picture)
				}
				for class, picture := range result.behind {
					t.Errorf("%s: on an older picture: %s: %s", shape, class, picture)
				}
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			})
		}
	})
	reportRestartRoll(t, results, full, time.Since(began))
}

// reportRestartRoll logs one line per shape and the totals.
func reportRestartRoll(t *testing.T, results []restartRollResult, full bool, took time.Duration) {
	t.Helper()
	sort.Slice(results, func(i, j int) bool { return results[i].shape.String() < results[j].shape.String() })
	probes, passes, findings, behind := 0, 0, 0, 0
	digest := fnv.New64a()
	for _, r := range results {
		probes, passes, findings, behind = probes+r.probes, passes+r.passes, findings+len(r.findings), behind+len(r.behind)
		fmt.Fprintf(digest, "%s|%d;", r.shape, r.digest)
		t.Logf("%s: %d passes in the plain roll, %d probes, %d engine passes, %d finding class(es) on live reads and %d on an older picture, digest %016x, %s",
			r.shape, r.ticks, r.probes, r.passes, len(r.findings), len(r.behind), r.digest, r.took.Round(time.Millisecond))
	}
	t.Logf("restart mid in-place roll (full=%v): %d shapes, %d probes, %d engine passes, %d finding class(es) on live reads and %d on an older picture, digest %016x, %s",
		full, len(results), probes, passes, findings, behind, digest.Sum64(), took.Round(time.Millisecond))
}

// restartRollStory is the shape the lock tests below tell their story on:
// six Instances, two at a time, in place, under the restart policy
// RecreateInstance.
var restartRollStory = restartRollShape{instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible, policy: workloadtypes.RestartPolicyRecreateInstance}

// requireOut holds a reading to the Instances named: their gates are off
// and their rows carry the open roll, and no other Instance is touched.
func requireOut(t *testing.T, w *restartRollWorld, r restartRollReading, want []int32, when string) {
	t.Helper()
	if !slices.Equal(r.gateOff, want) || !slices.Equal(r.updating, want) {
		t.Fatalf("%s: gates off on Instances %v and open rolls on %v, want both on %v\n%s", when, r.gateOff, r.updating, want, w.picture())
	}
}

// TestRestartMidInPlaceRoll_ANewManagerCountsEveryInstanceOutByItsRow
// reads, after every step of the plain roll, what a manager started at
// that moment would count against the budget. The count is the rows that
// carry an open roll, read from the replica's status. Every pod whose gate
// is off or whose Ready condition is cleared sits under one of those rows,
// so the plan lists no Instance down beside the roll and none on the
// target that does not serve: nothing is counted twice and nothing is
// missed, whether or not the kubelet has caught up.
func TestRestartMidInPlaceRoll_ANewManagerCountsEveryInstanceOutByItsRow(t *testing.T) {
	for _, shape := range []restartRollShape{restartRollStory, {instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategyInPlaceOnly, policy: workloadtypes.RestartPolicyNone}} {
		t.Run(shape.String(), func(t *testing.T) {
			w := newRestartRollWorld(t, shape)
			counted := 0
			for tick := 0; !w.complete(); tick++ {
				if tick >= restartRollTicksPerInstance*int(shape.instances) {
					t.Fatalf("the plain roll never completes\n%s", w.picture())
				}
				w.pass(nil, nil)
				w.h.clk.Step(restartRollPassGap)
				counted += requireCountedByRows(t, w, "after a pass")
				w.kubelet()
				counted += requireCountedByRows(t, w, "after the kubelet")
				w.endpoints()
				counted += requireCountedByRows(t, w, "after the endpoints")
			}
			if counted == 0 {
				t.Fatalf("no step of the roll had an Instance out; the test read nothing")
			}
		})
	}
}

// requireCountedByRows holds one moment to the rule above and returns how
// many Instances were out of rotation at it.
func requireCountedByRows(t *testing.T, w *restartRollWorld, when string) int {
	t.Helper()
	r := w.read()
	selection := w.selection()
	if selection == nil {
		if len(r.out) != 0 || len(r.updating) != 0 {
			t.Fatalf("%s: the plan rolls nothing while Instances %v are out and %v carry an open roll\n%s", when, r.out, r.updating, w.picture())
		}
		return 0
	}
	if int(selection.PriorUnavailInFlight) != len(r.updating) {
		t.Fatalf("%s: the plan counts %d Instance(s) in flight, the rows carry %d open roll(s) %v\n%s", when, selection.PriorUnavailInFlight, len(r.updating), r.updating, w.picture())
	}
	if len(selection.NotServingOutsideRoll) != 0 || len(selection.RolledNotServing) != 0 {
		t.Fatalf("%s: the plan counts Instances %v down beside the roll and %v on the target not serving; every Instance out is under an open row\n%s",
			when, selection.NotServingOutsideRoll, selection.RolledNotServing, w.picture())
	}
	for _, idx := range r.out {
		if !slices.Contains(r.updating, idx) {
			t.Fatalf("%s: Instance %d is out of rotation and its row carries no open roll\n%s", when, idx, w.picture())
		}
	}
	return len(r.out)
}

// TestRestartMidInPlaceRoll_ARestartBeforeTheKubeletClearsReadyStartsNoThird
// is the moment a cluster's whole-second times cannot show. Six pods roll
// two at a time. The manager turns the gates of pods 0 and 1 off and
// stops; the kubelet has not cleared Ready on either pod yet, so both
// still read Ready. The next manager counts both by their rows and starts
// no third Instance, on its first pass or its second.
func TestRestartMidInPlaceRoll_ARestartBeforeTheKubeletClearsReadyStartsNoThird(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the old manager's last pass")
	for idx, pods := range w.podsByInstance() {
		if idx > 1 {
			continue
		}
		if len(pods) != 1 || !podreadiness.IsPodReady(pods[0]) || podreadiness.IsServing(pods[0]) {
			t.Fatalf("Instance %d: want its pod still Ready with the gate off before the kubelet runs\n%s", idx, w.picture())
		}
	}
	if selection := w.selection(); selection == nil || selection.PriorUnavailInFlight != 2 || selection.UnavailBudget != 2 {
		t.Fatalf("the new manager's plan = %+v, want two Instances in flight against a budget of two", selection)
	}
	w.h.clk.Step(restartRollShortWait)
	for pass := 1; pass <= 2; pass++ {
		w.pass(nil, nil)
		requireOut(t, w, w.check(), []int32{0, 1}, fmt.Sprintf("after pass %d of the new manager", pass))
	}
	if len(w.findings) != 0 {
		t.Fatalf("findings: %v", w.findings)
	}
}

// TestRestartMidInPlaceRoll_AStartWhoseRowWriteIsRefusedTakesNoPodOut
// holds the order of a start's two writes: the row first, the gate after.
// The apiserver refuses the status write of Instance 0; the pass fails
// there, no pod is written, and no gate is off. A manager that dies
// between the two writes therefore always leaves the row behind, and a row
// is what the next manager counts.
func TestRestartMidInPlaceRoll_AStartWhoseRowWriteIsRefusedTakesNoPodOut(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	versions := map[string]string{}
	for _, pods := range w.podsByInstance() {
		for _, pod := range pods {
			versions[pod.Name] = pod.ResourceVersion
		}
	}
	w.h.mutateErr = map[int32]error{0: errors.New("the apiserver refuses the status write")}
	w.pass(nil, nil)
	if w.passErr == nil {
		t.Fatalf("the pass returned no error; want it to fail on the refused status write")
	}
	requireOut(t, w, w.read(), nil, "after the refused start")
	for _, pods := range w.podsByInstance() {
		for _, pod := range pods {
			if pod.ResourceVersion != versions[pod.Name] {
				t.Fatalf("pod %s was written by the pass whose row write was refused", pod.Name)
			}
		}
	}
	w.h.mutateErr = nil
	w.pass(nil, nil)
	requireOut(t, w, w.read(), []int32{0, 1}, "once the apiserver takes the write")
}

// TestRestartMidInPlaceRoll_AvailableCountsOnlyThePodsThatServeWhileTwoAreOut
// reads the counters the replica publishes while two of six pods are out.
// The headless Service lists every live pod as Ready, the two with the
// gate off included. Right after the gates go off the kubelet has not
// cleared Ready yet, so the published Available count still reads six
// while the Serving count reads four. Once the kubelet has cleared Ready
// both read four: a pod counts Available only while its own Ready
// condition is true. The budget reads neither counter: the two rows hold
// the two slots from the first write on.
func TestRestartMidInPlaceRoll_AvailableCountsOnlyThePodsThatServeWhileTwoAreOut(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the pass that turns the gates off")
	requirePublished(t, w, 6, 6, 4, "before the kubelet clears Ready")
	w.kubelet()
	w.endpoints()
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the next manager's first pass")
	requirePublished(t, w, 6, 4, 4, "once the kubelet has cleared Ready")
}

// requirePublished holds the counters the last pass published, summed
// over the rows: pods, Available pods and Serving pods.
func requirePublished(t *testing.T, w *restartRollWorld, wantPods, wantAvailable, wantServing int32, when string) {
	t.Helper()
	var pods, available, serving int32
	for _, row := range w.h.irStatuses() {
		pods, available, serving = pods+row.PodCount, available+row.AvailablePodCount, serving+row.ServingPodCount
	}
	if pods != wantPods || available != wantAvailable || serving != wantServing {
		t.Fatalf("%s the replica publishes %d pods, %d available, %d serving; want %d, %d and %d\n%s", when, pods, available, serving, wantPods, wantAvailable, wantServing, w.picture())
	}
}

// TestRestartMidInPlaceRoll_AWriteInFlightAtTheRestartStartsNothingOverTheBudget
// is the Lease taken while the old manager's last write is still on the
// wire. The old manager dies sending the row write that starts Instance 1.
// The new manager reads Instance 0 in flight and starts Instance 1 itself.
// The old write then reaches the apiserver, which refuses it: the replica
// has moved since the stopped manager read it. Two Instances are out, never
// three, and the roll completes.
func TestRestartMidInPlaceRoll_AWriteInFlightAtTheRestartStartsNothingOverTheBudget(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	var sent func() error
	if !w.pass(&passCut{left: 2}, &sent) || sent == nil {
		t.Fatalf("the old manager's pass was not cut with a write in flight")
	}
	requireOut(t, w, w.check(), []int32{0}, "after the two writes that landed")
	w.h.clk.Step(restartRollShortWait)
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the new manager's first pass")
	if err := sent(); !apierrors.IsConflict(err) {
		t.Fatalf("the write in flight returned %v; want the apiserver to refuse it with a conflict", err)
	}
	requireOut(t, w, w.check(), []int32{0, 1}, "after the write in flight arrived")
	w.playOut(1)
	if len(w.findings) != 0 {
		t.Fatalf("findings: %v", w.findings)
	}
}

// playUntil runs the plain roll, a pass and then the node, until done
// reports true.
func (w *restartRollWorld) playUntil(done func() bool, what string) {
	w.t.Helper()
	for tick := 0; !done(); tick++ {
		if tick >= restartRollTicksPerInstance*int(w.shape.instances) {
			w.t.Fatalf("the plain roll never reaches the moment %s\n%s", what, w.picture())
		}
		w.pass(nil, nil)
		w.h.clk.Step(restartRollPassGap)
		w.kubelet()
		w.endpoints()
	}
}

// firstTwoFinished reports whether Instances 0 and 1 are settled on
// version 2.
func (w *restartRollWorld) firstTwoFinished() bool {
	rows := w.h.irStatuses()
	return len(rows) > 1 && w.settled(rows[0]) && w.settled(rows[1])
}

// nextTwoOpen reports whether Instances 0 and 1 are settled on version 2
// and the rows of Instances 2 and 3, and no other, carry an open roll.
func (w *restartRollWorld) nextTwoOpen() bool {
	return w.firstTwoFinished() && slices.Equal(w.read().updating, []int32{2, 3})
}

// podsOf returns the one live pod of each Instance named.
func (w *restartRollWorld) podsOf(instances ...int32) map[int32]types.UID {
	w.t.Helper()
	byInstance := w.podsByInstance()
	pods := map[int32]types.UID{}
	for _, idx := range instances {
		var live []*corev1.Pod
		for _, pod := range byInstance[idx] {
			if pod.DeletionTimestamp == nil {
				live = append(live, pod)
			}
		}
		if len(live) != 1 {
			w.t.Fatalf("Instance %d has %d live pods, want one\n%s", idx, len(live), w.picture())
		}
		pods[idx] = live[0].UID
	}
	return pods
}

// gone lists the Instances of pods whose pod is no longer live.
func (w *restartRollWorld) gone(pods map[int32]types.UID) []int32 {
	live := map[types.UID]bool{}
	for _, list := range w.podsByInstance() {
		for _, pod := range list {
			if pod.DeletionTimestamp == nil {
				live[pod.UID] = true
			}
		}
	}
	var out []int32
	for idx, uid := range pods {
		if !live[uid] {
			out = append(out, idx)
		}
	}
	slices.Sort(out)
	return out
}

// restartRollSpread is the widest reading a run of steps showed: the most
// Instances out of rotation, with the gate off and with an open roll, and
// the most live pods.
type restartRollSpread struct {
	out, gateOff, updating []int32
	pods                   int
}

// see widens the spread by the world as it is.
func (s *restartRollSpread) see(w *restartRollWorld) {
	byInstance := w.podsByInstance()
	r := w.readOf(byInstance, w.h.irStatuses())
	if len(r.out) > len(s.out) {
		s.out = r.out
	}
	if len(r.gateOff) > len(s.gateOff) {
		s.gateOff = r.gateOff
	}
	if len(r.updating) > len(s.updating) {
		s.updating = r.updating
	}
	live := 0
	for _, pods := range byInstance {
		for _, pod := range pods {
			if pod.DeletionTimestamp == nil {
				live++
			}
		}
	}
	s.pods = max(s.pods, live)
}

// playRounds runs rounds of the node and one pass, and widens spread after
// every step.
func (w *restartRollWorld) playRounds(rounds int, spread *restartRollSpread) {
	for round := 0; round < rounds; round++ {
		w.h.clk.Step(restartRollPassGap)
		w.kubelet()
		w.endpoints()
		spread.see(w)
		w.pass(nil, nil)
		spread.see(w)
	}
}

const (
	// restartRollPassesBehind is how many passes in a row read the older
	// picture where a test keeps the manager's cache behind.
	restartRollPassesBehind = 3
	// restartRollRoundsAfter is how many rounds of the node and a pass the
	// tests of the other strategies play after the pass on the older
	// picture.
	restartRollRoundsAfter = 8
)

// requireFindings holds the classes the steps since the last restore
// broke, each by the first line of its picture, to the ones named.
func requireFindings(t *testing.T, w *restartRollWorld, want map[string]string, when string) {
	t.Helper()
	got := map[string]string{}
	for class, picture := range w.findings {
		got[class], _, _ = strings.Cut(picture, "\n")
	}
	if !maps.Equal(got, want) {
		t.Errorf("%s the model read %d class(es), want %d:\n got: %q\nwant: %q\n%v", when, len(got), len(want), got, want, w.findings)
	}
}

// requireRollEvents holds the roll's events to the starts and the
// completions named.
func requireRollEvents(t *testing.T, w *restartRollWorld, wantStarted, wantCompleted int, why string) {
	t.Helper()
	started, completed := w.h.eventCount(workloadtypes.EventReasonInPlaceUpdateStarted), w.h.eventCount(workloadtypes.EventReasonInPlaceUpdateCompleted)
	if started != wantStarted || completed != wantCompleted {
		t.Errorf("the roll recorded %d start(s) and %d completion(s), want %d and %d: %s", started, completed, wantStarted, wantCompleted, why)
	}
}

// requireWrites holds the writes a run of passes sent to the number named.
func requireWrites(t *testing.T, got, want int, why string) {
	t.Helper()
	if got != want {
		t.Errorf("the passes sent %d write(s), want %d: %s", got, want, why)
	}
}

// requireGone holds the Instances that lost the pod they had to the ones
// named.
func requireGone(t *testing.T, w *restartRollWorld, pods map[int32]types.UID, want []int32, why string) {
	t.Helper()
	if got := w.gone(pods); !slices.Equal(got, want) {
		t.Errorf("Instances %v lost the pod they had, want %v: %s\n%s", got, want, why, w.picture())
	}
}

// waitsOnAnOldCopy is the hold a pass reports when it leaves Instance idx
// as it is because its copy of the Instance's row is older than the row.
func (w *restartRollWorld) waitsOnAnOldCopy(idx int32) workloadtypes.RolloutHold {
	return workloadtypes.RolloutHold{
		Gate:   workloadtypes.RolloutHoldGateBudget,
		Reason: fmt.Sprintf("update to %s waits at Instance %d: the controller's copy of the Instance's status is older than the cluster's, and the controller reads the status again before it goes on", w.h.target.Name, idx),
		Target: w.h.target.Name,
	}
}

// requireHold requires the hold the last pass reported to be the one
// named.
func requireHold(t *testing.T, w *restartRollWorld, want workloadtypes.RolloutHold, when string) {
	t.Helper()
	if hold := w.h.hold; hold == nil || *hold != want {
		t.Errorf("%s the pass reported the hold %+v, want %+v", when, hold, want)
	}
}

// TestRestartMidInPlaceRoll_AFirstPassOnRowsAndPodsFromThePush is the
// restart whose new manager reads an older picture. Six pods roll two at a
// time. Pods 0 and 1 have finished on version 2 and pods 2 and 3 are out.
// The manager restarts, and the new manager's cache is filled from an
// apiserver that lags: three passes in a row read the rows and the pods as
// they were at the push, when all six served version 1. On that picture
// the budget is free, so each pass admits Instances 0 and 1. Both rows
// have moved since the picture was taken, so the pass opens neither. It
// writes nothing, starts no other Instance in their place, and reports why
// as the roll's hold. Pods 2 and 3 stay the only two out while the cache is
// behind, and the roll then ends with each Instance started and completed
// once.
func TestRestartMidInPlaceRoll_AFirstPassOnRowsAndPodsFromThePush(t *testing.T) {
	for _, shape := range []restartRollShape{restartRollStory, {instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategyInPlaceOnly, policy: workloadtypes.RestartPolicyNone}} {
		t.Run(shape.String(), func(t *testing.T) {
			w := newRestartRollWorld(t, shape)
			atThePush := w.observe()
			w.playUntil(func() bool { return slices.Equal(w.read().gateOff, []int32{2, 3}) }, "at which pods 2 and 3 are out")
			requireOut(t, w, w.check(), []int32{2, 3}, "before the restart")
			writes := 0
			for pass := 1; pass <= restartRollPassesBehind; pass++ {
				w.behind = &atThePush
				writes += w.countedPass()
				when := fmt.Sprintf("after pass %d on the picture from the push", pass)
				requireOut(t, w, w.check(), []int32{2, 3}, when)
				requireHold(t, w, w.waitsOnAnOldCopy(0), when)
			}
			requireFindings(t, w, nil, "over the passes on the picture from the push")
			requireWrites(t, writes, 0, "every start those passes admitted is refused and keeps its slot")
			w.playOut(0)
			requireFindings(t, w, nil, "to the end of the roll")
			requireRollEvents(t, w, 6, 6, "one of each per Instance")
		})
	}
}

// TestRestartMidInPlaceRoll_APassFromThePushWhileNoPodIsOut is the same
// older picture at the moment no pod is out: pods 0 and 1 have just
// finished on version 2, and the pass that starts pods 2 and 3 has not
// run. A pass that reads the rows and the pods from the push admits
// Instances 0 and 1 into the two slots of the budget. Both starts are
// refused, and each keeps its slot for the pass: the pass starts no other
// Instance in their place, writes nothing, and reports why as the roll's
// hold. The next pass reads the cluster as it is and starts Instances 2
// and 3.
func TestRestartMidInPlaceRoll_APassFromThePushWhileNoPodIsOut(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	atThePush := w.observe()
	w.playUntil(w.firstTwoFinished, "at which pods 0 and 1 have finished")
	requireOut(t, w, w.check(), nil, "once pods 0 and 1 have finished")
	w.behind = &atThePush
	writes := w.countedPass()
	requireOut(t, w, w.check(), nil, "after the pass on the picture from the push")
	requireFindings(t, w, nil, "over the pass on the picture from the push")
	requireWrites(t, writes, 0, "both starts the pass admitted are refused and keep their slots")
	requireHold(t, w, w.waitsOnAnOldCopy(0), "after the pass on the picture from the push")
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{2, 3}, "after the next pass, which reads the cluster as it is")
	w.playOut(0)
	requireFindings(t, w, nil, "to the end of the roll")
	requireRollEvents(t, w, 6, 6, "one of each per Instance")
}

// TestRestartMidInPlaceRoll_APassOneWriteBehindAPromote needs no restart.
// One pass promotes pods 0 and 1 onto version 2. The next pass starts
// before its cache shows that write and still reads both rows mid-roll. It
// finds the live rows finished and stops there: it opens neither Instance
// again, writes nothing, and reports why as the roll's hold. The roll
// records one start and one completion per Instance.
func TestRestartMidInPlaceRoll_APassOneWriteBehindAPromote(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	var beforeThePromote restartRollObservation
	for tick := 0; !w.firstTwoFinished(); tick++ {
		if tick >= restartRollTicksPerInstance*int(w.shape.instances) {
			t.Fatalf("Instances 0 and 1 never finish\n%s", w.picture())
		}
		w.kubelet()
		w.endpoints()
		beforeThePromote = w.observe()
		w.pass(nil, nil)
		w.h.clk.Step(restartRollPassGap)
	}
	w.check()
	w.behind = &restartRollObservation{rows: beforeThePromote.rows}
	writes := w.countedPass()
	w.check()
	requireFindings(t, w, nil, "after the pass one write behind the promote")
	requireWrites(t, writes, 0, "the pass finds both rows finished and leaves them")
	requireHold(t, w, w.waitsOnAnOldCopy(0), "after the pass one write behind the promote")
	w.playOut(0)
	requireFindings(t, w, nil, "to the end of the roll")
	requireRollEvents(t, w, 6, 6, "one of each per Instance")
}

// TestRestartMidInPlaceRoll_APassOneWriteBehindAStart needs no restart
// either. One pass starts Instances 0 and 1. The next pass starts before
// its cache shows those two writes and still reads both rows at rest. It
// finds both live rows rolling and goes on with them. It starts no third
// Instance and records no second start: the roll's events show one start
// and one completion per Instance.
func TestRestartMidInPlaceRoll_APassOneWriteBehindAStart(t *testing.T) {
	w := newRestartRollWorld(t, restartRollStory)
	beforeTheStart := w.observe()
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the pass that starts the first two")
	requireRollEvents(t, w, 2, 0, "one start for each of the two Instances out")
	w.behind = &restartRollObservation{rows: beforeTheStart.rows}
	w.pass(nil, nil)
	requireOut(t, w, w.check(), []int32{0, 1}, "after the pass one write behind the two starts")
	requireRollEvents(t, w, 2, 0, "the pass behind the two starts records neither again")
	w.playOut(0)
	requireFindings(t, w, nil, "to the end of the roll")
	requireRollEvents(t, w, 6, 6, "one of each per Instance")
}

// TestRestartMidInPlaceRoll_AFirstPassOnAnOlderPodList is the restart with
// the rows read as they are and only the pod list behind. Pods 0 and 1
// have finished on version 2 and pods 2 and 3 are out. The new manager's
// pod list still shows pods 0 and 1 with version 1's label: mid-roll with
// the gate off in one picture, serving at the push in the other. Beside
// rows that read finished on version 2, such a pod looks like a leftover
// of a revision the Instance has left, which the cleanup takes out of
// rotation and deletes. The cleanup reads each pod live first, finds it on
// version 2, and leaves it: no pod is deleted, pods 2 and 3 stay the only
// two out, and the roll ends.
func TestRestartMidInPlaceRoll_AFirstPassOnAnOlderPodList(t *testing.T) {
	for _, picture := range []string{"the pods at the push", "the pods mid-roll"} {
		t.Run(picture, func(t *testing.T) {
			w := newRestartRollWorld(t, restartRollStory)
			older := w.observe()
			w.pass(nil, nil)
			w.h.clk.Step(restartRollPassGap)
			w.kubelet()
			w.endpoints()
			if picture == "the pods mid-roll" {
				older = w.observe()
			}
			w.playUntil(func() bool { return slices.Equal(w.read().gateOff, []int32{2, 3}) }, "at which pods 2 and 3 are out")
			requireOut(t, w, w.check(), []int32{2, 3}, "before the restart")
			finished := w.podsOf(0, 1)
			w.behind = &restartRollObservation{pods: older.pods}
			w.pass(nil, nil)
			requireOut(t, w, w.check(), []int32{2, 3}, "after the first pass on the older pod list")
			w.playOut(0)
			requireFindings(t, w, nil, "to the end of the roll")
			requireGone(t, w, finished, nil, "the cleanup leaves the pods of the finished Instances")
			requireRollEvents(t, w, 6, 6, "one of each per Instance")
		})
	}
}

// TestRestartMidRecreateRoll_AFirstPassOnRowsAndPodsFromThePush hands the
// older picture to a RecreatePod roll, which deletes each pod and makes it
// again on version 2. Six pods roll two at a time. Pods 0 and 1 have
// finished and pods 2 and 3 are out when the new manager's first pass
// reads the rows and the pods from the push.
//
// The readings below hold what the engine does with that picture, past
// the budget: the recreate entry opens a new attempt on the live row
// without comparing it with the pass's copy. The pass opens a roll on
// Instance 0, whose row had finished. Three rows carry an open roll at a
// budget of 2, and the passes after it delete the pod Instance 0 finished
// with, while Instances 2 and 3 are out.
func TestRestartMidRecreateRoll_AFirstPassOnRowsAndPodsFromThePush(t *testing.T) {
	w := newRestartRollWorld(t, restartRollShape{instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategyRecreatePod, policy: workloadtypes.RestartPolicyRecreateInstance})
	atThePush := w.observe()
	w.playUntil(w.nextTwoOpen, "at which Instances 0 and 1 have finished and 2 and 3 are open")
	finished := w.podsOf(0, 1)
	w.behind = &atThePush
	w.pass(nil, nil)
	failed := fmt.Sprint(w.passErr)
	spread := &restartRollSpread{}
	spread.see(w)
	w.playRounds(restartRollRoundsAfter, spread)
	// The pass on the older picture fails on the pod of
	// Instance 0 after it has opened the roll on the finished row.
	if want := "pod identity changed"; !strings.Contains(failed, want) {
		t.Errorf("the pass on the picture from the push returned %q, want an error that carries %q", failed, want)
	}
	// Three rows carry an open roll at a budget of 2.
	if want := []int32{0, 2, 3}; !slices.Equal(spread.updating, want) {
		t.Errorf("the most open rolls at once are on Instances %v, want %v", spread.updating, want)
	}
	// Three Instances are out of rotation at a budget of 2.
	if want := []int32{0, 2, 3}; !slices.Equal(spread.out, want) {
		t.Errorf("the most Instances out of rotation at once are %v, want %v", spread.out, want)
	}
	// The pod Instance 0 finished the roll with is deleted.
	requireGone(t, w, finished, []int32{0}, "the passes after the older picture delete the pod of the finished Instance 0")
	if spread.pods != 6 {
		t.Errorf("the most live pods at once are %d, want 6: a recreate adds no pod", spread.pods)
	}
}

// TestRestartMidSurgeRoll_AFirstPassOnRowsAndPodsFromThePush hands the
// older picture to a SurgeThenDrain roll, which brings a new pod up beside
// each old one and then removes the old one. Six pods roll with two extra
// pods at a time. Pods 0 and 1 have finished and the new pods of 2 and 3
// are coming up when the new manager's first pass reads the rows and the
// pods from the push.
//
// The readings below hold what the engine does with that picture, past
// the budget: the surge entry opens a new attempt on the live row
// without comparing it with the pass's copy. The pass surges the finished
// Instances 0 and 1 again. Ten pods are live for six Instances at a surge
// budget of 2, and both finished pods are replaced. No Instance leaves
// rotation.
func TestRestartMidSurgeRoll_AFirstPassOnRowsAndPodsFromThePush(t *testing.T) {
	w := newRestartRollWorld(t, restartRollShape{instances: 6, budget: 2, strategy: workloadtypes.UpdateStrategySurgeThenDrain, policy: workloadtypes.RestartPolicyRecreateInstance})
	atThePush := w.observe()
	w.playUntil(w.nextTwoOpen, "at which Instances 0 and 1 have finished and 2 and 3 are open")
	finished := w.podsOf(0, 1)
	w.behind = &atThePush
	w.pass(nil, nil)
	spread := &restartRollSpread{}
	spread.see(w)
	w.playRounds(restartRollRoundsAfter, spread)
	// Four extra pods are live at a surge budget of 2.
	if spread.pods != 10 {
		t.Errorf("the most live pods at once are %d, want 10: the pass surges the finished Instances 0 and 1 again", spread.pods)
	}
	// Four rows carry an open roll at a budget of 2.
	if want := []int32{0, 1, 2, 3}; !slices.Equal(spread.updating, want) {
		t.Errorf("the most open rolls at once are on Instances %v, want %v", spread.updating, want)
	}
	// Both pods the finished Instances served with are replaced.
	requireGone(t, w, finished, []int32{0, 1}, "the second surge replaces the pods of the finished Instances 0 and 1")
	if len(spread.out) != 0 {
		t.Errorf("Instances %v left rotation, want none: a surge keeps the old pod serving until the new one does", spread.out)
	}
}
