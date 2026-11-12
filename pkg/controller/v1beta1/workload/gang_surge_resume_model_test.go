package workload_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine through a manager restart at
// every moment of a gang surge. A gang surge rolls a multi-pod Instance by
// building a whole new gang at a free index, taking the old gang out of
// rotation once the new one serves, deleting it, and promoting the new
// gang once the old gang's pods have left the cluster; its two rows (the
// old gang's, under an Update operation, and the new gang's, Creating
// under its claim) stay open until then. The manager keeps nothing of a
// surge in memory: a restarted manager must pick the pair up from its two
// rows and the pods.
//
// The world is the scaling model's (scale_beside_a_roll_model_test.go):
// one workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components, the endpoint model on
// for the gang side, the kubelet termination model holding a deleted pod
// for its grace. The plain roll from version 1 to version 2 is recorded
// tick by tick with the number of apiserver writes the gang side's pass
// made in each. Every probe restores the world before one of those ticks
// and runs that pass as a dying manager: the first k writes land, every
// later call fails, the pass's result reaches no queue, the Components
// after it make no pass. The world then moves without a manager for the
// Lease wait (pods start, serve and finish terminating), a move may land
// with the first pass of the new manager (a member of the new gang lost
// while nobody watched, a pod of another Instance lost, a scale, a push, a
// push back, a partition, a pause), and the roll is played to its end.
// A restart after the pass, between two passes, is the probe with every
// write landed. Both schedulers' timing are configurations: every member
// of a gang placed together, or the workers placed one tick after the
// leader.
//
// At every tick the model checks the scaling model's budgets and that no
// pod set of one name and revision was built twice without the world
// having lost it, no source names a second replacement index, no surge
// index is claimed by two sources, and no old gang is deleted before its
// new gang serves. At every settled end the roll must be complete, or
// held on a spent retry schedule, and rows still open once every pod is
// Ready on the target and none is leaving are a finding of their own.
// OME_GANG_SURGE_RESUME_HUNT=full widens the configurations, the gaps and
// the moves; OME_GANG_SURGE_RESUME_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	resumeHuntFullEnv = "OME_GANG_SURGE_RESUME_HUNT"
	resumeHuntOnlyEnv = "OME_GANG_SURGE_RESUME_HUNT_ONLY"
	// resumeSettleTicks is how many ticks a probe may take after the
	// restart before it is read as never settling.
	resumeSettleTicks = 160
	// resumeTickBudgetBounded and resumeTickBudgetFull bound the ticks one
	// configuration spends; the probes left when a budget is spent are
	// counted as skipped, so a run on a slower machine reads the same
	// first probes. A shape above resumeBudgetPods pods gets the budget
	// scaled down by its pod count, never below resumeTickBudgetLeast: a
	// tick costs about as much as the pods it moves, so every shape spends
	// about the same wall clock.
	resumeTickBudgetBounded = 6000
	resumeTickBudgetFull    = 40000
	resumeBudgetPods        = 16
	resumeTickBudgetLeast   = 4000
	// resumeWorkers is the most configurations explored at once in full
	// mode, to keep the suite's CPU use bounded.
	resumeWorkers = 16
	// resumeAllWrites is the probe whose restart comes after the pass:
	// every write of the pass lands.
	resumeAllWrites = -1
)

// passCut is the manager dying inside one pass: left is how many writes
// the pass may still make, hit records that a call was refused, writes
// counts the writes the pass asked for, landed or refused.
type passCut struct {
	left   int
	hit    bool
	writes int
}

// read admits a read until the cut has refused a call.
func (c *passCut) read() error {
	if c.hit {
		return context.Canceled
	}
	return nil
}

// write admits a write while the budget lasts and refuses every call
// after the first refusal.
func (c *passCut) write() error {
	c.writes++
	if c.hit || c.left <= 0 {
		c.hit = true
		return context.Canceled
	}
	c.left--
	return nil
}

// cutClient is the apiserver as a dying manager sees it: every call goes
// through until the cut refuses one; from then on every call fails.
type cutClient struct {
	client.Client
	cut *passCut
}

func (c *cutClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.cut.read(); err != nil {
		return err
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *cutClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.cut.read(); err != nil {
		return err
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *cutClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *cutClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *cutClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *cutClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *cutClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

func (c *cutClient) Status() client.SubResourceWriter {
	return &cutSubResourceWriter{SubResourceWriter: c.Client.Status(), cut: c.cut}
}

func (c *cutClient) SubResource(name string) client.SubResourceClient {
	return &cutSubResourceClient{SubResourceClient: c.Client.SubResource(name), cut: c.cut}
}

// cutSubResourceWriter is the status writer of a dying manager.
type cutSubResourceWriter struct {
	client.SubResourceWriter
	cut *passCut
}

func (c *cutSubResourceWriter) Create(ctx context.Context, obj, sub client.Object, opts ...client.SubResourceCreateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceWriter.Create(ctx, obj, sub, opts...)
}

func (c *cutSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceWriter.Update(ctx, obj, opts...)
}

func (c *cutSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// cutSubResourceClient is a sub-resource client of a dying manager.
type cutSubResourceClient struct {
	client.SubResourceClient
	cut *passCut
}

func (c *cutSubResourceClient) Get(ctx context.Context, obj, sub client.Object, opts ...client.SubResourceGetOption) error {
	if err := c.cut.read(); err != nil {
		return err
	}
	return c.SubResourceClient.Get(ctx, obj, sub, opts...)
}

func (c *cutSubResourceClient) Create(ctx context.Context, obj, sub client.Object, opts ...client.SubResourceCreateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceClient.Create(ctx, obj, sub, opts...)
}

func (c *cutSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceClient.Update(ctx, obj, opts...)
}

func (c *cutSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if err := c.cut.write(); err != nil {
		return err
	}
	return c.SubResourceClient.Patch(ctx, obj, patch, opts...)
}

// resumeMoveKind is what lands with the first pass of the new manager.
type resumeMoveKind uint8

const (
	resumeMoveNone              resumeMoveKind = iota
	resumeLoseReplacementMember                // a worker of the open pair's new gang died while no manager watched
	resumeLoseOtherPod                         // a pod of an Instance outside the pair died while no manager watched
	resumeScaleUp                              // the operator asks for one Instance more
	resumeScaleDown                            // the operator asks for one Instance fewer
	resumePushThird                            // the operator pushes a third version
	resumePushBack                             // the operator pushes version 1 back
	resumePartition                            // the operator sets a partition of one
	resumePause                                // the operator pauses for two ticks and resumes
)

var resumeMoveNames = [...]string{"none", "lose-replacement-member", "lose-other-pod", "scale-up", "scale-down", "push-v3", "push-back", "partition", "pause"}

func (k resumeMoveKind) String() string { return resumeMoveNames[k] }

// resumeConfig is one configuration the model explores: the scaling
// model's shape and settings, the scheduler's timing, the old gang's
// termination grace, and whether the push is to an image no pod can pull.
type resumeConfig struct {
	scaleConfig
	// staggered is the default scheduler's timing: the workers of a new
	// gang are placed one tick after its leader. Off, every member is
	// placed together, the gang scheduler's timing.
	staggered bool
	// grace is how long a deleted pod of the old gang stays before it
	// leaves; zero keeps the scaling model's one tick.
	grace time.Duration
	// held pushes an image no pod can pull: every attempt of the surge
	// fails until the retry schedule is spent and the roll holds.
	held bool
}

func (c resumeConfig) String() string {
	parts := []string{c.scaleConfig.String()}
	if c.staggered {
		parts = append(parts, "staggered")
	}
	if c.grace > 0 {
		parts = append(parts, "grace="+c.grace.String())
	}
	if c.held {
		parts = append(parts, "held")
	}
	return strings.Join(parts, "+")
}

// resumeProbe is one restart: the tick of the recorded roll it lands in,
// how many writes of that tick's pass land before the process dies
// (resumeAllWrites for a death after the pass), how many ticks the world
// moves with no manager, and the move that lands at the resume.
type resumeProbe struct {
	tick   int
	writes int
	gap    int
	move   resumeMoveKind
}

func (p resumeProbe) String() string {
	when := fmt.Sprintf("after %d write(s)", p.writes)
	if p.writes == resumeAllWrites {
		when = "after the pass"
	}
	return fmt.Sprintf("restart in tick %d %s, no manager for %d tick(s), then %s", p.tick, when, p.gap, p.move)
}

// rollTick is one tick of the recorded roll: the world before it, the
// writes the gang side's pass made in it, and the rows after it.
type rollTick struct {
	snap      *scaleSnapshot
	writes    int
	rowsAfter int
}

// resumeWorld is the scaling model's world with the gang side under
// study, the probe under play and the readings of this file.
type resumeWorld struct {
	*scaleWorld
	rcfg resumeConfig
	// side is the Component whose passes the restart cuts: the first side,
	// the one whose Instances are gangs under the configuration's strategy.
	side  *scaleSide
	probe resumeProbe
	// setUIDs keeps, per Instance index, pod name and revision, the pod
	// UIDs seen in this probe while the index's row stands; a second UID is
	// a set built twice unless the world lost it. rowsSeen names the
	// indexes whose row the last tick showed: an index whose row left
	// starts over, its names free for the next Instance built there.
	setUIDs  map[int32]map[string]map[types.UID]bool
	rowsSeen map[int32]bool
	// podsRecorded counts every distinct pod the readings recorded in this
	// probe, the ones of rows that left included.
	podsRecorded int
	// worldLost names the pods the probe's own move deleted, and
	// lostInstances the Instances they belonged to: a gang repairs as a
	// unit, so every member of such an Instance may be built again.
	worldLost     map[string]bool
	lostInstances map[int32]bool
	// replacementOf keeps, per source index, the surge index and revision
	// its open pair names; a second index for the same revision while the
	// claim stands is a second replacement, while a new revision is the
	// operator's push, and a claim the source dropped (the pair ended, or
	// reset for a new attempt) is closed.
	replacementOf map[int32]replacementClaim
	// lastClaim keeps the last claim each source named in this probe,
	// closed or not, for the tests that ask which index a pair took.
	lastClaim map[int32]replacementClaim
	// replacementLost says the probe's move took a member of the open
	// pair's new gang, so a rebuild of that gang is the world's doing.
	replacementLost bool
	// scaledDown says the probe's move asked for fewer Instances, so a
	// Delete operation is the operator's doing.
	scaledDown bool
	// basePods is how many pods the probe found when it started; the
	// pods it created are the ones seen since.
	basePods int
	// probes, skipped and rticks count the configuration's work.
	probes, skipped, rticks int
}

func newResumeWorld(t *testing.T, cfg resumeConfig) *resumeWorld {
	t.Helper()
	w := &resumeWorld{scaleWorld: newScaleWorld(t, cfg.scaleConfig), rcfg: cfg}
	w.side = w.prefill()
	for _, s := range w.sides {
		if cfg.grace > 0 {
			s.h.podGrace = cfg.grace
		}
	}
	w.side.h.routed = true
	w.moreChecks = w.observe
	w.tickBudget = resumeTickBudget(os.Getenv(resumeHuntFullEnv) == "full", cfg)
	w.resetReadings()
	return w
}

// resumeTickBudget is the tick budget of one configuration: the mode's,
// scaled down by the pod count of a shape above resumeBudgetPods pods.
func resumeTickBudget(full bool, cfg resumeConfig) int {
	budget := resumeTickBudgetBounded
	if full {
		budget = resumeTickBudgetFull
	}
	pods := int((cfg.shape.prefill + cfg.shape.decode) * cfg.shape.pods)
	if cfg.settings.router {
		pods += 2
	}
	if pods > resumeBudgetPods {
		budget = budget * resumeBudgetPods / pods
	}
	return max(budget, resumeTickBudgetLeast)
}

// resetReadings clears what the readings remember across a probe.
func (w *resumeWorld) resetReadings() {
	w.setUIDs = map[int32]map[string]map[types.UID]bool{}
	w.rowsSeen = map[int32]bool{}
	w.worldLost = map[string]bool{}
	w.lostInstances = map[int32]bool{}
	w.replacementOf = map[int32]replacementClaim{}
	w.lastClaim = map[int32]replacementClaim{}
	w.replacementLost = false
	w.podsRecorded = 0
	w.scaledDown = false
	w.basePods = 0
}

// replacementClaim is what a source's open pair named: the index of its
// replacement and the revision the pair is held to.
type replacementClaim struct {
	index    int32
	revision string
}

// podsSeen is how many distinct pods the readings have seen in this
// probe, by name and UID.
func (w *resumeWorld) podsSeen() int { return w.podsRecorded }

// podsCreated is how many pods the engine created since the probe began.
func (w *resumeWorld) podsCreated() int {
	return w.podsSeen() - w.basePods
}

// staggerHook is the default scheduler's timing: a worker the kubelet has
// not seen yet is refused by the scheduler model for one tick, and a
// worker it refused once is placed on the next. The leader is placed at
// once, so the gang's members come up one tick apart.
func (w *resumeWorld) staggerHook() {
	if !w.rcfg.staggered {
		return
	}
	for _, s := range w.sides {
		if s.h.workers == 0 {
			continue
		}
		if s.h.unplaceable == nil {
			s.h.unplaceable = map[string]bool{}
		}
		for _, pod := range w.pods(s) {
			if pod.Labels[query.LabelRunner] != "worker" || pod.DeletionTimestamp != nil {
				continue
			}
			_, _, refused := workloadtypes.PodUnschedulable(pod)
			switch {
			case pod.Status.Phase == "" && !s.h.unplaceable[pod.Name]:
				s.h.unplaceable[pod.Name] = true
			case refused && s.h.unplaceable[pod.Name]:
				delete(s.h.unplaceable, pod.Name)
			}
		}
	}
}

// tick runs the scaling model's tick behind the scheduler timing hook.
func (w *resumeWorld) tick() string {
	w.staggerHook()
	key := w.scaleWorld.tick()
	w.rticks++
	return key
}

// playOut ticks until the world has settled or the bound is reached, on
// this file's tick.
func (w *resumeWorld) playOut(maxTicks int) bool {
	w.t.Helper()
	settling := w.startSettling()
	for i := 0; i < maxTicks; i++ {
		if settling.observe(w.scaleWorld, w.tick()) {
			return true
		}
	}
	return false
}

// endTick is the clock, the state key and the checks every tick of this
// file ends with, the way the scaling model's tick ends.
func (w *resumeWorld) endTick() {
	w.clk.Step(scaleRollLeastStep)
	w.ticks++
	w.rticks++
	w.seen[w.key()] = struct{}{}
	w.check()
}

// restartTick is the tick the manager dies in: the service pass runs, the
// gang side's pass lands the probe's writes and dies (every write when
// the restart comes after the pass), the sides after it make no pass, and
// the world moves on.
func (w *resumeWorld) restartTick(writes int) {
	w.t.Helper()
	w.staggerHook()
	w.servicePass()
	cut := &passCut{left: writes}
	if writes == resumeAllWrites {
		cut.left = 1 << 30
	}
	w.side.h.cut = cut
	_, err := w.side.h.pass()
	w.side.h.cut = nil
	w.passes++
	if err != nil {
		w.note("error", "%s: Reconcile returned an error: %v", w.side.comp, err)
	}
	w.endTick()
}

// gapTick is a tick with no manager: the kubelets and the endpoint
// controller move, the clock moves, no pass runs.
func (w *resumeWorld) gapTick() {
	w.t.Helper()
	w.staggerHook()
	for _, s := range w.sides {
		s.h.kubelet()
		if s.h.routed {
			s.h.endpointController()
		}
	}
	w.endTick()
}

// openPair is the gang side's open pair as the rows read now: the source
// row and the index its replacement is built under.
func (w *resumeWorld) openPair() (source *workloadtypes.InstanceStatus, surgeIdx int32, ok bool) {
	rows := w.rows(w.side)
	for i := range rows {
		r := &rows[i]
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.SurgeIndex != nil {
			return r, *r.Operation.SurgeIndex, true
		}
	}
	return nil, 0, false
}

// losePod deletes one pod as the world's own hand and remembers it, so a
// rebuild of that pod reads as the world's doing.
func (w *resumeWorld) losePod(pod *corev1.Pod) {
	w.t.Helper()
	w.worldLost[pod.Name] = true
	if idx, ok := query.InstanceIdxFromLabels(pod); ok {
		w.lostInstances[idx] = true
		w.markFaulted(w.side, idx)
	}
	if err := w.side.h.c.Delete(w.ctx, pod); err != nil {
		w.t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
}

// applyMove lands the probe's move at the resume. A move the state cannot
// take (no pair open to lose a member of, no Instance outside the pair,
// one Instance to scale down) lands nothing and says so in the path.
func (w *resumeWorld) applyMove(kind resumeMoveKind) {
	w.t.Helper()
	s := w.side
	switch kind {
	case resumeMoveNone:
		return
	case resumeLoseReplacementMember:
		_, surgeIdx, ok := w.openPair()
		if !ok {
			w.path = append(w.path, "no pair open to lose a member of")
			return
		}
		pods := s.h.podsOf(surgeIdx)
		if len(pods) == 0 {
			w.path = append(w.path, "the new gang has no pod to lose")
			return
		}
		w.replacementLost = true
		w.losePod(pods[len(pods)-1])
	case resumeLoseOtherPod:
		source, surgeIdx, open := w.openPair()
		for _, r := range w.rows(s) {
			if open && (r.Index == source.Index || r.Index == surgeIdx) {
				continue
			}
			if pods := s.h.podsOf(r.Index); len(pods) > 0 {
				w.losePod(pods[0])
				return
			}
		}
		w.path = append(w.path, "no pod outside the pair to lose")
	case resumeScaleUp:
		w.setReplicas(s, s.h.replicas+1)
	case resumeScaleDown:
		if s.h.replicas <= 1 {
			w.path = append(w.path, "one Instance cannot scale down")
			return
		}
		w.scaledDown = true
		w.setReplicas(s, s.h.replicas-1)
	case resumePushThird:
		w.push(scaleRollThirdImage)
	case resumePushBack:
		w.push(goodImage)
	case resumePartition:
		w.setPartition(s, 1)
	case resumePause:
		w.setPaused(true)
		w.tick()
		w.tick()
		w.setPaused(false)
	}
}

// recordRoll plays the roll from idle and keeps, per tick, the world
// before it, the writes the gang side's pass made in it and the rows
// after it. The roll's own end is classified like any run.
func (w *resumeWorld) recordRoll(maxTicks int) []rollTick {
	w.t.Helper()
	if w.rcfg.held {
		w.pushHeld()
	} else {
		w.push(fixedImage)
	}
	var ticks []rollTick
	settling := w.startSettling()
	settled := false
	for i := 0; i < maxTicks && !settled; i++ {
		snap := w.snapshot()
		count := &passCut{left: 1 << 30}
		w.side.h.cut = count
		key := w.tick()
		w.side.h.cut = nil
		ticks = append(ticks, rollTick{snap: snap, writes: count.writes, rowsAfter: len(w.rows(w.side))})
		settled = settling.observe(w.scaleWorld, key)
	}
	w.path = append(w.path, "plain roll")
	w.classifyEnd(settled)
	return ticks
}

// play runs one probe from the recorded tick it names and classifies the
// end.
func (w *resumeWorld) play(ticks []rollTick, p resumeProbe, maxTicks int) {
	w.t.Helper()
	w.restore(ticks[p.tick].snap)
	w.side.h.recorder = record.NewFakeRecorder(8192)
	w.probe = p
	w.resetReadings()
	w.observe()
	w.basePods = w.podsSeen()
	w.path = append(w.path, p.String())
	w.probes++
	w.restartTick(p.writes)
	for i := 0; i < p.gap; i++ {
		w.gapTick()
	}
	w.applyMove(p.move)
	settled := w.playOut(maxTicks)
	w.readEnd(settled)
	w.classifyEnd(settled)
}

// observe runs after every tick's checks: the pod sets built, the pairs'
// claims and the order of the old gang's deletion against the new gang's
// readiness, on the gang side.
func (w *resumeWorld) observe() {
	s := w.side
	rows := w.rows(s)
	w.forgetLeftRows(rows)
	pods := w.pods(s)
	w.readSets(pods)
	for k, sources := range w.readClaims(rows, pods) {
		if len(sources) > 1 {
			w.note("surge-index-claimed-twice", "Instances %v name index %d as their replacement at once: %s", sources, k, w.picture())
		}
	}
	if w.scaledDown {
		return
	}
	for _, r := range rows {
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationDelete {
			w.note("instance-deleted-with-no-scale-down", "Instance %d is being deleted while the spec never asked for fewer Instances: %s", r.Index, w.picture())
		}
	}
}

// readSets records every pod by Instance index, name, revision and UID
// and reads a set built twice: a second UID for one name and revision
// with no loss in the world. A pod on its way out is not a build.
func (w *resumeWorld) readSets(pods []*corev1.Pod) {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		idx, _ := query.InstanceIdxFromLabels(pod)
		sets := w.setUIDs[idx]
		if sets == nil {
			sets = map[string]map[types.UID]bool{}
			w.setUIDs[idx] = sets
		}
		key := pod.Name + "@" + pod.Labels[query.LabelRevisionHash]
		uids := sets[key]
		if uids == nil {
			uids = map[types.UID]bool{}
			sets[key] = uids
		}
		if uids[pod.UID] {
			continue
		}
		if len(uids) > 0 && !w.lostInstances[idx] && pod.Spec.Containers[0].Image != badImage {
			w.note("set-built-twice", "%s was built again on the same revision with no loss in the world (UIDs %d): %s", pod.Name, len(uids)+1, w.picture())
		}
		uids[pod.UID] = true
		w.podsRecorded++
	}
}

// readClaims reads the pairs' claims: the sources naming each index, a
// source naming a second index for the same revision while its claim
// stands, and the order of each old gang's deletion. A claim a source no
// longer carries is closed.
func (w *resumeWorld) readClaims(rows []workloadtypes.InstanceStatus, pods []*corev1.Pod) map[int32][]int32 {
	claims := map[int32][]int32{}
	claiming := map[int32]bool{}
	for _, r := range rows {
		if r.Operation == nil || r.Operation.Type != workloadtypes.InstanceOperationUpdate || r.Operation.SurgeIndex == nil {
			continue
		}
		claiming[r.Index] = true
		k := *r.Operation.SurgeIndex
		claims[k] = append(claims[k], r.Index)
		claim := replacementClaim{index: k, revision: r.Operation.TargetRevision}
		if prev, seen := w.replacementOf[r.Index]; seen && prev.index != k && prev.revision == claim.revision && !w.replacementLost {
			w.note("second-replacement", "Instance %d named replacement index %d after %d toward the same revision with no loss of the first: %s", r.Index, k, prev.index, w.picture())
		}
		w.replacementOf[r.Index] = claim
		w.lastClaim[r.Index] = claim
		w.readDrainOrder(r.Index, k, pods)
	}
	for src := range w.replacementOf {
		if !claiming[src] {
			delete(w.replacementOf, src)
		}
	}
	return claims
}

// forgetLeftRows drops what the readings keep for an index whose row left
// the status since the last tick: the pod names of the index are free for
// the next Instance built there, and a claim the row held is closed.
func (w *resumeWorld) forgetLeftRows(rows []workloadtypes.InstanceStatus) {
	present := make(map[int32]bool, len(rows))
	for _, r := range rows {
		present[r.Index] = true
	}
	for idx := range w.rowsSeen {
		if present[idx] {
			continue
		}
		delete(w.rowsSeen, idx)
		delete(w.setUIDs, idx)
		delete(w.replacementOf, idx)
	}
	for idx := range present {
		w.rowsSeen[idx] = true
	}
}

// readDrainOrder holds the surge's order: an old gang's pod deleted by the
// engine while its new gang is short of a Ready member is a drain ahead
// of the replacement, unless the world took the member.
func (w *resumeWorld) readDrainOrder(sourceIdx, surgeIdx int32, pods []*corev1.Pod) {
	if w.replacementLost {
		return
	}
	deleting := false
	ready := 0
	for _, pod := range pods {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		switch idx {
		case sourceIdx:
			if pod.DeletionTimestamp != nil && !w.lostInstances[sourceIdx] {
				deleting = true
			}
		case surgeIdx:
			if podConditionTrue(pod, corev1.PodReady) && pod.DeletionTimestamp == nil {
				ready++
			}
		}
	}
	if deleting && int32(ready) < w.podsPerInstance(w.side) {
		w.note("old-gang-deleted-before-the-new-gang-served", "Instance %d's old gang is being deleted while its new gang at %d has %d of %d members Ready: %s",
			sourceIdx, surgeIdx, ready, w.podsPerInstance(w.side), w.picture())
	}
}

// podsConverged reports every pod of the side Ready on the target with
// none leaving, and every Instance the spec asks for with a pod set.
func (w *resumeWorld) podsConverged(s *scaleSide) bool {
	byIndex := w.livePodsByIndex(s)
	if int32(len(byIndex)) != s.h.replicas {
		return false
	}
	target := query.RevisionFromName(s.h.target.Name).Hash()
	for _, pod := range w.pods(s) {
		if pod.DeletionTimestamp != nil || !podConditionTrue(pod, corev1.PodReady) || pod.Labels[query.LabelRevisionHash] != target {
			return false
		}
	}
	return true
}

// readEnd runs at a settled end before the scaling model classifies it:
// rows still open once every pod of the side is Ready on the target and
// none is leaving is the expected reading.
func (w *resumeWorld) readEnd(settled bool) {
	if !settled || w.complete() || w.side.paused || w.side.partition > 0 {
		return
	}
	if w.podsConverged(w.side) {
		w.note("rows-open-after-the-pods-converged", "every pod of %s is Ready on the target and none is leaving, yet the rows read: %s", w.side.comp, w.picture())
	}
}

// resumeConfigs lists the configurations: the reference shape (two gangs of
// two beside two routers under SurgeThenDrain) under both schedulers'
// timing and a long termination grace, gangs of three, a double surge, a
// prefill/decode pair of gangs with a router, and gangs under RecreatePod;
// full mode adds the reference shapes, the held roll, a canary, a
// partition and a ratio rule.
func resumeConfigs(full bool) []resumeConfig {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	recreate := workloadtypes.UpdateStrategyRecreatePod
	engine := func(n, pods int32, settings scaleSettings) scaleConfig {
		settings.prefill = surge
		return scaleConfig{shape: scaleShape{n, 0, pods}, settings: settings}
	}
	pair := func(prefill, decode, pods int32, settings scaleSettings) scaleConfig {
		settings.prefill, settings.decode = surge, surge
		return scaleConfig{shape: scaleShape{prefill, decode, pods}, settings: settings}
	}
	configs := []resumeConfig{
		{scaleConfig: engine(2, 2, scaleSettings{router: true})},
		{scaleConfig: engine(2, 2, scaleSettings{router: true}), staggered: true},
		{scaleConfig: engine(2, 2, scaleSettings{router: true}), grace: 2 * time.Minute},
		{scaleConfig: engine(3, 3, scaleSettings{})},
		{scaleConfig: engine(4, 2, scaleSettings{budget: "50%"}), staggered: true},
		{scaleConfig: pair(2, 2, 2, scaleSettings{router: true})},
		{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 4}, settings: scaleSettings{prefill: recreate}}},
	}
	if !full {
		return configs
	}
	configs = append(configs,
		resumeConfig{scaleConfig: engine(2, 2, scaleSettings{router: true}), held: true},
		resumeConfig{scaleConfig: engine(3, 3, scaleSettings{}), staggered: true, grace: 2 * time.Minute},
		resumeConfig{scaleConfig: engine(4, 2, scaleSettings{budget: "50%"})},
		resumeConfig{scaleConfig: engine(8, 2, scaleSettings{budget: "25%"}), staggered: true},
		resumeConfig{scaleConfig: engine(4, 8, scaleSettings{})},
		resumeConfig{scaleConfig: engine(12, 2, scaleSettings{partition: 2})},
		resumeConfig{scaleConfig: pair(4, 4, 2, scaleSettings{canary: true})},
		resumeConfig{scaleConfig: pair(4, 4, 2, scaleSettings{router: true}), staggered: true},
		resumeConfig{scaleConfig: pair(12, 8, 2, scaleSettings{router: true})},
		resumeConfig{scaleConfig: pair(12, 4, 8, scaleSettings{})},
		resumeConfig{scaleConfig: pair(15, 15, 2, scaleSettings{maintainRatio: true, keepRatio: true})},
		resumeConfig{scaleConfig: pair(16, 8, 4, scaleSettings{router: true}), staggered: true},
		resumeConfig{scaleConfig: pair(20, 10, 2, scaleSettings{maintainRatio: true})},
		resumeConfig{scaleConfig: pair(24, 8, 2, scaleSettings{})},
		resumeConfig{scaleConfig: pair(30, 10, 2, scaleSettings{router: true})},
		resumeConfig{scaleConfig: scaleConfig{shape: scaleShape{4, 0, 4}, settings: scaleSettings{prefill: recreate}}, staggered: true},
	)
	return configs
}

// probeOrder lists the recorded ticks from both ends inward, so a tick
// budget that runs out still leaves the first surge and the roll's end
// probed.
func probeOrder(n int) []int {
	order := make([]int, 0, n)
	for lo, hi := 0, n-1; lo <= hi; lo, hi = lo+1, hi-1 {
		order = append(order, lo)
		if hi != lo {
			order = append(order, hi)
		}
	}
	return order
}

// probesOf lists the probes for one recorded tick: a death after every
// write of its pass and after the pass, each with the Lease wait of one
// tick and no move; in full mode every move lands behind each death, and
// a death with no Lease wait and with a wait of two ticks is tried too.
// In bounded mode the moves land behind the death after the pass.
func probesOf(tick int, rt rollTick, full bool) []resumeProbe {
	cuts := make([]int, 0, rt.writes+1)
	for k := 0; k < rt.writes; k++ {
		cuts = append(cuts, k)
	}
	cuts = append(cuts, resumeAllWrites)
	moves := []resumeMoveKind{resumeLoseReplacementMember, resumeLoseOtherPod, resumeScaleUp, resumePushThird}
	if full {
		moves = []resumeMoveKind{resumeLoseReplacementMember, resumeLoseOtherPod, resumeScaleUp, resumeScaleDown, resumePushThird, resumePushBack, resumePartition, resumePause}
	}
	var probes []resumeProbe
	for _, k := range cuts {
		probes = append(probes, resumeProbe{tick: tick, writes: k, gap: 1, move: resumeMoveNone})
		if full {
			probes = append(probes, resumeProbe{tick: tick, writes: k, gap: 0, move: resumeMoveNone},
				resumeProbe{tick: tick, writes: k, gap: 2, move: resumeMoveNone})
		}
		if full || k == resumeAllWrites {
			for _, mv := range moves {
				probes = append(probes, resumeProbe{tick: tick, writes: k, gap: 1, move: mv})
			}
		}
	}
	return probes
}

// explore runs one configuration: the roll recorded tick by tick, then
// every probe from every recorded tick, inside the tick budget.
func (w *resumeWorld) explore(full bool) {
	w.t.Helper()
	w.reachIdle()
	ticks := w.recordRoll(resumeSettleTicks)
	for _, ti := range probeOrder(len(ticks)) {
		for _, p := range probesOf(ti, ticks[ti], full) {
			if w.ticks >= w.budget(full) {
				w.skipped++
				continue
			}
			w.play(ticks, p, resumeSettleTicks)
		}
	}
}

// resumeExpectedClasses are the ends a probe may reach by design.
var resumeExpectedClasses = map[string]bool{"held": true, "ends-paused": true}

// knownResumeClasses are the findings of this model held by a lock test
// until the behaviour changes.
var knownResumeClasses = map[string]string{
	"scale-up-not-built-at-once":          "a scale-up is built within the three checks of the scale-up rule but on two paths. Held: while the push's revision has an attempt in progress or waits in its retry backoff, the retry record admits one attempt toward its revision at a time, so the scale-up's Instance is built when the next attempt opens (the scaling model reads a spent schedule as held-with-the-scale-incomplete). Resumed: when the restart cut a pair between the promote and the remove, the first passes go to the leftover row's removal, and a fresh Instance the plan placed on the index the next pair claims is built on the pass after, at the next free index: one pass past the window, and the roll ends complete",
	"set-built-twice":                     "a manager that dies after the first write of the pass that opens an in-place recreate (the row written as Update/Drain toward the pushed revision, before any pod is deleted), followed by a push back to the revision the pods run: the next manager restarts the drain toward the returned revision, as the recreate rule decides for any new target during the drain (the live pods read as rendered from the retired target), and rebuilds the Instance on the revision it already runs; held by TestGangSurgeResume_APushBackAfterTheRecreatesFirstWriteRebuildsTheRunningRevision",
	"instance-deleted-with-no-scale-down": "a manager that dies between the two writes that end a pair (the new gang promoted, the old gang's row not yet removed) leaves a row the next plan counts as a third Instance: the scale-down wave removes the podless old row under a Delete operation before the update pass reaches the pair, so the roll finishes without the surge's completion event and the status shows an Instance deleted that nobody scaled down (held by TestGangSurgeResume_ADeathBetweenThePromoteAndTheRemoveEndsThePairAsAScaleDown)",
}

// resumeKnownClasses are the scaling and move models' known classes and
// this file's.
func resumeKnownClasses() map[string]string {
	out := scaleKnownClasses()
	for class, text := range knownResumeClasses {
		out[class] = text
	}
	return out
}

type resumeResult struct {
	cfg                                    resumeConfig
	states, ticks, passes, probes, skipped int
	counts                                 map[string]int
	findings                               map[string]scaleFinding
	took                                   time.Duration
}

type resumeExample struct {
	cfg     resumeConfig
	finding scaleFinding
}

// resumeExplore runs the model over the configurations keep admits, a
// bounded number at a time in parallel, and reports every class, failing
// on the classes that are neither expected nor known.
func resumeExplore(t *testing.T, keep func(resumeConfig) bool) {
	t.Helper()
	full := os.Getenv(resumeHuntFullEnv) == "full"
	only := os.Getenv(resumeHuntOnlyEnv)
	workers := 8
	if full {
		workers = resumeWorkers
	}
	start := time.Now()
	var configs []resumeConfig
	for _, cfg := range resumeConfigs(full) {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			configs = append(configs, cfg)
		}
	}
	results := make([]resumeResult, len(configs))
	admission := make(chan struct{}, workers)
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				admission <- struct{}{}
				defer func() { <-admission }()
				began := time.Now()
				w := newResumeWorld(t, cfg)
				w.explore(full)
				results[i] = resumeResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, probes: w.probes, skipped: w.skipped,
					counts: w.counts, findings: w.findings, took: time.Since(began)}
			})
		}
	})
	known := resumeKnownClasses()
	totals := map[string]int{}
	examples := map[string][]resumeExample{}
	configurations := map[string]int{}
	var states, ticks, passes, probes, skipped int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		passes += res.passes
		probes += res.probes
		skipped += res.skipped
		var parts []string
		for class, c := range res.counts {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes (%d skipped past the tick budget), %s: %s", n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], resumeExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d probes (%d skipped past the tick budget), %s", len(configs), states, ticks, passes, probes, skipped, time.Since(start).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		if resumeExpectedClasses[class] {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		switch {
		case known[class] != "":
			t.Logf("  known: %s", known[class])
		default:
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// TestGangSurgeResume_EveryRestartLeavesTheRollFinished explores every
// configuration: a manager that dies after any write of any pass of a
// gang surge, with the world moving while no manager runs and a move
// landing with the first pass of the new one, leaves the roll finished
// with every pod set built once, or held on a spent retry schedule.
func TestGangSurgeResume_EveryRestartLeavesTheRollFinished(t *testing.T) {
	resumeExplore(t, func(resumeConfig) bool { return true })
}

// TestGangSurgeResume_TheRowsStayOpenWhileTheOldGangLeavesAndCloseAfter
// holds a picture that once read as a fault. Two gangs of two
// beside two routers under SurgeThenDrain; the manager dies in the first
// pass of the roll before its first write; the old gangs take two minutes
// to leave once deleted. While the second old gang's pods are being
// deleted, its row reads Updating under its Update operation and the new
// gang's row reads Creating under its claim, with every pod of the new
// gang Ready: the roll of that Instance is not finished. Once the old
// gang's pods have left, both Instances read Ready on version 2 with no
// operation within two ticks.
func TestGangSurgeResume_TheRowsStayOpenWhileTheOldGangLeavesAndCloseAfter(t *testing.T) {
	cfg := resumeConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain, router: true}}, grace: 2 * time.Minute}
	w := newResumeWorld(t, cfg)
	w.reachIdle()
	w.push(fixedImage)
	w.path = append(w.path, "restart in the first pass before its first write")
	w.restartTick(0)
	w.gapTick()
	openWhileLeaving := 0
	for i := 0; i < resumeSettleTicks && !w.complete(); i++ {
		w.tick()
		source, surgeIdx, ok := w.openPair()
		if !ok || w.leavingPods(source.Index) == 0 {
			continue
		}
		w.requirePairOpenWhileLeaving(t, source, surgeIdx)
		openWhileLeaving++
	}
	if openWhileLeaving < 2 {
		t.Fatalf("the pair should read open across at least two ticks while the old gang leaves under a two-minute grace; read it %d time(s): %s", openWhileLeaving, w.picture())
	}
	w.requireRollFinished(t, 4)
}

// leavingPods counts the pods of Instance idx on the gang side that are
// being deleted.
func (w *resumeWorld) leavingPods(idx int32) int {
	leaving := 0
	for _, pod := range w.pods(w.side) {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx && pod.DeletionTimestamp != nil {
			leaving++
		}
	}
	return leaving
}

// requirePairOpenWhileLeaving fails the test unless a pair whose old gang
// is leaving reads as the engine keeps it: the source Updating at its
// drain step, the replacement Creating under its claim, and every pod of
// the new gang Ready on the target.
func (w *resumeWorld) requirePairOpenWhileLeaving(t *testing.T, source *workloadtypes.InstanceStatus, surgeIdx int32) {
	t.Helper()
	replacement := w.side.h.instance(surgeIdx)
	if replacement == nil || replacement.Phase != workloadtypes.InstancePhaseCreating || replacement.Operation == nil ||
		source.Phase != workloadtypes.InstancePhaseUpdating || source.Operation.Step != workloadtypes.UpdateStepSurgeDrain {
		t.Fatalf("while the old gang of Instance %d leaves, the pair should read Updating/SurgeDrain and Creating under the claim; got %s", source.Index, w.picture())
	}
	target := query.RevisionFromName(w.side.h.target.Name).Hash()
	for _, pod := range w.side.h.podsOf(surgeIdx) {
		if !podConditionTrue(pod, corev1.PodReady) || pod.Labels[query.LabelRevisionHash] != target {
			t.Fatalf("the new gang at %d should be Ready on version 2 while the old gang leaves; got %s", surgeIdx, w.picture())
		}
	}
}

// requireRollFinished fails the test unless the roll is complete with
// every row of the gang side Ready on the target with no operation, pods
// pods standing, and no finding outside the expected classes.
func (w *resumeWorld) requireRollFinished(t *testing.T, pods int) {
	t.Helper()
	if !w.complete() {
		t.Fatalf("the roll should finish once the old gangs have left; got %s", w.picture())
	}
	for _, r := range w.rows(w.side) {
		if r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil || r.RunningRevision != w.side.h.target.Name {
			t.Fatalf("every row should read Ready on version 2 with no operation; got %s", w.picture())
		}
	}
	if got := len(w.pods(w.side)); got != pods {
		t.Fatalf("%d pods should stand; got %d: %s", pods, got, w.picture())
	}
	for class := range w.findings {
		if !resumeExpectedClasses[class] {
			t.Errorf("%s: %s", class, w.findings[class].message)
		}
	}
}

// TestGangSurgeResume_ADeathBetweenThePromoteAndTheRemoveEndsThePairAsAScaleDown
// holds the current picture of a manager that dies inside the pass that ends a
// pair, after its first write: the new gang is promoted Ready on version
// 2, the old gang's row still carries the surge operation, and the process
// dies. The next manager's plan counts the promoted gang and the old row
// as two Instances for a spec of two and removes the old row with its
// scale-down wave, under a Delete operation, before the update pass
// reaches the pair: the roll finishes with the right two Instances on
// version 2 and the second Instance's roll follows, but the cut pair's
// completion is never published and the status shows an Instance deleted
// that nobody scaled down. A fix flips the last two checks: the pair ends
// through its own removal, with its completion event and no Delete
// operation.
func TestGangSurgeResume_ADeathBetweenThePromoteAndTheRemoveEndsThePairAsAScaleDown(t *testing.T) {
	cfg := resumeConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain}}}
	w := newResumeWorld(t, cfg)
	w.reachIdle()
	ticks := w.recordRoll(resumeSettleTicks)
	ending := -1
	rows := int(w.rcfg.shape.prefill) + 1
	for i, rt := range ticks {
		if rt.rowsAfter < rows && rt.writes >= 2 {
			ending = i
			break
		}
		rows = rt.rowsAfter
	}
	if ending < 0 {
		t.Fatalf("no recorded tick removed a source row with two writes; the roll took %d ticks", len(ticks))
	}
	w.play(ticks, resumeProbe{tick: ending, writes: 1, gap: 1, move: resumeMoveNone}, resumeSettleTicks)
	if !w.complete() || len(w.rows(w.side)) != 2 {
		t.Fatalf("the roll should finish with two Instances after the death between the promote and the remove; got %s", w.picture())
	}
	if created, gang := w.podsCreated(), int(w.podsPerInstance(w.side)); created != gang {
		t.Errorf("after the death only the second Instance's new gang should be built (%d pods); %d pod(s) were", gang, created)
	}
	for class := range w.findings {
		if !resumeExpectedClasses[class] && knownResumeClasses[class] == "" {
			t.Errorf("%s: %s", class, w.findings[class].message)
		}
	}
	if _, found := w.findings["instance-deleted-with-no-scale-down"]; !found {
		t.Errorf("today the old row is removed by the scale-down wave under a Delete operation; the model read %v", classesOf(w.findings))
	}
	if n := w.completionEvents(0); n != 0 {
		t.Errorf("today the cut pair's completion is not published after the death; %d completion event(s) named Instance 0", n)
	}
}

// TestGangSurgeResume_APushBackAfterTheRecreatesFirstWriteRebuildsTheRunningRevision
// holds the recreate rule's outcome in the window a restart
// opens. Two gangs of four under RecreatePod, version 2 pushed; the
// manager dies after the first write of the pass that opens the first
// Instance's recreate (the row written as Update/Drain toward version 2),
// before any pod is deleted; the operator pushes version 1 back while no
// manager runs. The next manager restarts the drain toward version 1, as
// the recreate rule says for any new target during the drain, and rebuilds
// the gang on the revision it already runs: the four pods of the Instance
// are built again on version 1 and the roll ends with both Instances on
// version 1.
func TestGangSurgeResume_APushBackAfterTheRecreatesFirstWriteRebuildsTheRunningRevision(t *testing.T) {
	cfg := resumeConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 4}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod}}}
	w := newResumeWorld(t, cfg)
	w.reachIdle()
	ticks := w.recordRoll(resumeSettleTicks)
	if ticks[0].writes < 2 {
		t.Fatalf("the pass that opens the recreate should write the row before it deletes a pod; it made %d write(s)", ticks[0].writes)
	}
	w.play(ticks, resumeProbe{tick: 0, writes: 1, gap: 1, move: resumePushBack}, resumeSettleTicks)
	if !w.complete() || len(w.rows(w.side)) != 2 {
		t.Fatalf("the roll should end with both Instances on version 1; got %s", w.picture())
	}
	for _, r := range w.rows(w.side) {
		if r.RunningRevision != w.side.h.target.Name || r.Operation != nil {
			t.Fatalf("both Instances should run version 1 with no operation; got %s", w.picture())
		}
	}
	if _, found := w.findings["set-built-twice"]; !found {
		t.Errorf("today the gang is rebuilt on the revision it runs; the model read %v: %s", classesOf(w.findings), w.picture())
	}
	if created, gang := w.podsCreated(), int(w.podsPerInstance(w.side)); created != gang {
		t.Errorf("today the one Instance's gang is built again (%d pods); %d pod(s) were", gang, created)
	}
	for class := range w.findings {
		if !resumeExpectedClasses[class] && knownResumeClasses[class] == "" {
			t.Errorf("%s: %s", class, w.findings[class].message)
		}
	}
}

// completionEvents counts the gang surge completion events the probe
// recorded for the pair whose source is Instance sourceIdx.
func (w *resumeWorld) completionEvents(sourceIdx int32) int {
	key := workloadtypes.InstanceKey(w.side.comp, sourceIdx) + " gang surge complete"
	n := 0
	for _, event := range w.side.h.events {
		if strings.Contains(event, string(workloadtypes.EventReasonRecreateUpdateCompleted)) && strings.Contains(event, key) {
			n++
		}
	}
	return n
}

// TestGangSurgeResume_AScaleUpInThePassThatOpensAPairTakesTheNextIndex
// holds a scale-up that lands in the pass that opens a gang surge, restart
// or not. Two gangs of two under SurgeThenDrain; the first Instance's
// surge finishes and frees index 0 in one pass; the operator asks for
// three Instances before the next; that pass opens the second Instance's
// surge at index 0 and leaves the index to it. The new Instance is built
// on the next pass at index 3, the surge's replacement at index 0: one
// replacement index for the second Instance, four pods built for the four
// the roll and the scale-up need, no Instance deleted, three Instances on
// version 2 at the end.
func TestGangSurgeResume_AScaleUpInThePassThatOpensAPairTakesTheNextIndex(t *testing.T) {
	cfg := resumeConfig{scaleConfig: scaleConfig{shape: scaleShape{2, 0, 2}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategySurgeThenDrain}}}
	w := newResumeWorld(t, cfg)
	w.reachIdle()
	ticks := w.recordRoll(resumeSettleTicks)
	freed := -1
	rows := int(w.rcfg.shape.prefill) + 1
	for i, rt := range ticks {
		if rt.rowsAfter < rows {
			freed = i
			break
		}
		rows = rt.rowsAfter
	}
	if freed < 0 {
		t.Fatalf("no recorded tick freed the first source's index; the roll took %d ticks", len(ticks))
	}
	w.play(ticks, resumeProbe{tick: freed, writes: resumeAllWrites, gap: 0, move: resumeScaleUp}, resumeSettleTicks)
	if !w.complete() || w.side.h.replicas != 3 || len(w.rows(w.side)) != 3 {
		t.Fatalf("the roll should finish with three Instances; got %s", w.picture())
	}
	if created := w.podsCreated(); created != 4 {
		t.Errorf("four pods should be built for the four the roll and the scale-up need; got %d", created)
	}
	if claim, named := w.lastClaim[1]; !named || claim.index != 0 {
		t.Errorf("the second Instance should keep index 0 as its one replacement index; named=%t last=%d", named, claim.index)
	}
	for class := range w.findings {
		if !resumeExpectedClasses[class] {
			t.Errorf("%s: %s (classes read: %v)", class, w.findings[class].message, classesOf(w.findings))
		}
	}
}

// classesOf lists the classes of a findings map in order.
func classesOf(findings map[string]scaleFinding) []string {
	out := make([]string, 0, len(findings))
	for class := range findings {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}
