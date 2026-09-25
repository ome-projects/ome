// Internal-package tests for workload.Migrate's private helpers
// (validateFromNode, etc.) that aren't reachable from outside the
// workload/ops package. The end-to-end TestMigrate_* lifecycle tests
// (status.migrations-driven, single-pod + gang) live below.

package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type countingPodReader struct {
	client.Reader
	lists int
	err   error
}

func (r *countingPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.lists++
	if r.err != nil {
		return r.err
	}
	return r.Reader.List(ctx, list, opts...)
}

// vfnFixture builds the input + deps + plan a validateFromNode test
// reads — a single-pod engine workload selecting pods labelled with
// the canonical ISVC pod-label-key seed.
func vfnFixture(t *testing.T, pods ...*corev1.Pod) (workload.Deps, workload.ReconcileInput, workload.ComponentPlan) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	objs := make([]client.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	input := workload.ReconcileInput{
		Key: workload.Key{
			Namespace: "prod",
			Component: workload.ComponentEngine,
			OwnerName: "llama-70b",
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: "llama-70b",
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
	}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	return workload.Deps{Client: c}, input, plan
}

// vfnPod fabricates a pod under the validateFromNode selector with the
// given NodeName. Always Instance idx=0 — every validateFromNode test
// drives source index 0.
func vfnPod(nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b-engine-0-default-0",
			Namespace: "prod",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "llama-70b",
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
}

func TestValidateFromNode_MatchOK(t *testing.T) {
	deps, input, plan := vfnFixture(t, vfnPod("node5"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ || mismatch != "" {
		t.Errorf("expected pass; got mismatch=%q defer=%v", mismatch, defer_)
	}
}

func TestValidateFromNode_MismatchRejected(t *testing.T) {
	deps, input, plan := vfnFixture(t, vfnPod("node7"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("scheduled pod on different node must be a rejection, not a defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason")
	}
}

func TestValidateFromNodeUsesAuthoritativePodsAndFailsClosed(t *testing.T) {
	deps, input, plan := vfnFixture(t, vfnPod("stale-node"))
	liveDeps, _, _ := vfnFixture(t, vfnPod("live-node"))
	reader := &countingPodReader{Reader: liveDeps.Client}
	deps.APIReader = reader
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{{
		Index: 0, NodesOccupied: []string{"persisted-node"},
	}}

	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "live-node")
	if err != nil || defer_ || mismatch != "" {
		t.Fatalf("authoritative Pod node should pass: mismatch=%q defer=%v err=%v", mismatch, defer_, err)
	}
	if reader.lists != 1 {
		t.Fatalf("authoritative Pod lists: got %d want 1", reader.lists)
	}

	failure := errors.New("list failed")
	reader = &countingPodReader{Reader: liveDeps.Client, err: failure}
	deps.APIReader = reader
	mismatch, defer_, err = validateFromNode(context.Background(), deps, input, plan, 0, "live-node")
	if !errors.Is(err, failure) || defer_ || mismatch != "" {
		t.Fatalf("authoritative read failure must fail closed: mismatch=%q defer=%v err=%v", mismatch, defer_, err)
	}
	if reader.lists != 1 {
		t.Fatalf("failed authoritative Pod lists: got %d want 1", reader.lists)
	}
}

func TestValidateFromNode_UnscheduledDefers(t *testing.T) {
	// Pod exists but Spec.NodeName is empty. Defer rather than reject —
	// the next reconcile re-polls.
	deps, input, plan := vfnFixture(t, vfnPod(""))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if !defer_ {
		t.Errorf("unscheduled pod must defer; got mismatch=%q defer=%v", mismatch, defer_)
	}
	if mismatch != "" {
		t.Errorf("defer must not carry a rejection reason; got %q", mismatch)
	}
}

func TestValidateFromNode_NoLivePodsRejects(t *testing.T) {
	// Source instance has no pods at all — a rejection (the migration
	// requester can't legitimately request a migration of a
	// non-existent instance).
	deps, input, plan := vfnFixture(t)
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("no live pods must reject, not defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason for no-live-pods")
	}
}

// vfnTerminatingPod is vfnPod pinned Terminating (finalizer so the
// fake client stores the DeletionTimestamp) under a distinct name.
func vfnTerminatingPod(name, nodeName string) *corev1.Pod {
	pod := vfnPod(nodeName)
	pod.Name = name
	dt := metav1.NewTime(time.Now())
	pod.DeletionTimestamp = &dt
	pod.Finalizers = []string{"test.ome.io/block"}
	return pod
}

func TestValidateFromNode_TerminatingPodIgnored(t *testing.T) {
	// Recreate churn leaves the old pod Terminating on another node
	// beside the live replacement — only the live pod decides. Counting
	// the Terminating pod would reject with "span multiple nodes".
	deps, input, plan := vfnFixture(t, vfnPod("node5"), vfnTerminatingPod("llama-70b-engine-0-default-0-prev", "node7"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ || mismatch != "" {
		t.Errorf("Terminating pod must be ignored; got mismatch=%q defer=%v", mismatch, defer_)
	}
}

func TestValidateFromNode_AllTerminatingDefers(t *testing.T) {
	// Every pod Terminating is transient (replacements pending) —
	// defer, never a permanent rejection.
	deps, input, plan := vfnFixture(t, vfnTerminatingPod("llama-70b-engine-0-default-0", "node5"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if !defer_ {
		t.Errorf("all-Terminating source must defer; got mismatch=%q defer=%v", mismatch, defer_)
	}
	if mismatch != "" {
		t.Errorf("defer must not carry a rejection reason; got %q", mismatch)
	}
}

// vfnGangFixture is vfnFixture for a multi-node (gang) Instance: the
// plan carries an InstancePlan at idx=0 with leader + worker Runners so
// isMultiPodInstance(plan, 0) is true and validateFromNode takes the
// gang-aware branch.
func vfnGangFixture(t *testing.T, pods ...*corev1.Pod) (workload.Deps, workload.ReconcileInput, workload.ComponentPlan) {
	t.Helper()
	deps, input, plan := vfnFixture(t, pods...)
	plan.Instances = []workload.InstancePlan{{
		Index:       0,
		Incarnation: 1,
		Runners: []workload.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 1},
		},
	}}
	return deps, input, plan
}

// vfnGangPod fabricates a gang member pod (leader/worker) for Instance
// idx=0 on the given node. Pod listing is purely label-based on
// instance-index, so leader + worker share idx=0 and are returned
// together by LiveListPodsForInstance even when scheduled to different
// nodes.
func vfnGangPod(runner, nodeName string) *corev1.Pod {
	p := vfnPod(nodeName)
	p.Name = "llama-70b-engine-0-" + runner + "-0"
	p.Labels[query.LabelRunner] = runner
	return p
}

// TestValidateFromNode_GangSpanningNodesNotRejected pins that a
// multi-node gang Instance's pods span nodes BY DESIGN (leader + worker
// land on different nodes), so a migration whose FromNode hosts one of
// them must NOT be rejected. Rejecting any source whose pods span nodes
// would wrongly fail every gang migration that isn't co-located on a
// single node.
func TestValidateFromNode_GangSpanningNodesNotRejected(t *testing.T) {
	deps, input, plan := vfnGangFixture(t,
		vfnGangPod("leader", "node5"),
		vfnGangPod("worker", "node8"),
	)
	// FromNode = the leader's node; the gang spans node5 + node8.
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("gang with a scheduled pod on FromNode must not defer")
	}
	if mismatch != "" {
		t.Errorf("gang spanning nodes with a pod on FromNode must pass; got mismatch=%q", mismatch)
	}

	// FromNode = the worker's node also passes — either member's node is a
	// valid evacuation target for the whole-gang surge.
	mismatch, defer_, err = validateFromNode(context.Background(), deps, input, plan, 0, "node8")
	if err != nil {
		t.Fatalf("validateFromNode (worker node): %v", err)
	}
	if defer_ || mismatch != "" {
		t.Errorf("FromNode=worker node must pass; got mismatch=%q defer=%v", mismatch, defer_)
	}
}

// TestValidateFromNode_GangFromNodeAbsentRejected pins that a stale
// request — FromNode hosting none of the gang's pods (the gang has since
// moved off that node) — is still a rejection, so the surge's
// NotIn[FromNode] can't silently no-op against a node the gang no longer
// occupies.
func TestValidateFromNode_GangFromNodeAbsentRejected(t *testing.T) {
	deps, input, plan := vfnGangFixture(t,
		vfnGangPod("leader", "node5"),
		vfnGangPod("worker", "node8"),
	)
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node99")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("FromNode absent from a fully-scheduled gang must reject, not defer")
	}
	if mismatch == "" {
		t.Errorf("expected a rejection reason when FromNode hosts no gang pod")
	}
}

// TestValidateFromNode_GangUnscheduledDefers pins that the fresh-request
// defer guard survives in the gang branch: an unscheduled gang member
// defers (re-validated next reconcile) rather than rejecting.
func TestValidateFromNode_GangUnscheduledDefers(t *testing.T) {
	deps, input, plan := vfnGangFixture(t,
		vfnGangPod("leader", "node5"),
		vfnGangPod("worker", ""), // worker not scheduled yet
	)
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if !defer_ {
		t.Errorf("unscheduled gang member must defer; got mismatch=%q defer=%v", mismatch, defer_)
	}
	if mismatch != "" {
		t.Errorf("defer must not carry a rejection reason; got %q", mismatch)
	}
}

// TestSurgeRevisionAndSpec_ReturnsGangWorkerSpec pins that the migrate
// surge resolver returns the source revision's WORKER template (not just
// the leader) so a gang migration renders its worker pods from the right
// template. Before gang migrate support this returned only the leader
// spec; the worker spec is nil for single-pod revisions.
func TestSurgeRevisionAndSpec_ReturnsGangWorkerSpec(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)

	leader := &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "llama:v1"}}}
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}}}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		leader, worker, nil, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevisionWithWorker: %v", err)
	}
	// Point instance 0's running revision at the gang CR (update IR).
	ir.Status.InstanceStatuses[0].RunningRevision = cr.Name
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("update IR RunningRevision: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	gotCR, gotLeader, gotWorker, err := surgeRevisionAndSpec(context.Background(), legacyTestDeps(c), input, 0)
	if err != nil {
		t.Fatalf("surgeRevisionAndSpec: %v", err)
	}
	if gotCR == nil || gotLeader == nil {
		t.Fatalf("expected CR + leader spec, got cr=%v leader=%v", gotCR, gotLeader)
	}
	if gotWorker == nil {
		t.Fatalf("expected a worker spec for a gang revision, got nil")
	}
	if len(gotWorker.Containers) == 0 || gotWorker.Containers[0].Name != "worker" {
		t.Errorf("worker spec mismatch: %+v", gotWorker)
	}
}

// TestDrainServiceForPod_GangWorkerNotRoutable pins the worker-aware fix for
// gang migration finalization. The per-revision ROUTING Service selects the
// rank-0 leader only for multi-pod Components (workers run distributed-init
// peers and never serve customer traffic — see
// coordination.BuildPerRevisionRoutingService), so a gang worker pod is never
// a routing endpoint. drainServiceForPod must therefore return "" for a worker
// so the Migrate surge in-rotation gate (and the source drain gate) skip it
// instead of waiting forever on IsPodInRotation(worker) — otherwise every
// gang migration wedges at ledger phase=Started. Leader + single-pod "default"
// pods stay routable.
func TestDrainServiceForPod_GangWorkerNotRoutable(t *testing.T) {
	input := workload.ReconcileInput{Key: workload.Key{OwnerName: "llama-70b", Component: workload.ComponentEngine}}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}

	runnerPod := func(runner string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "llama-70b-engine-0-" + runner + "-0",
			Labels: map[string]string{
				query.LabelRunner:       runner,
				query.LabelRevisionHash: "abc123",
			},
		}}
	}

	if got := drainServiceForPod(input, plan, runnerPod("leader")); got == "" {
		t.Errorf("leader pod must be routable (non-empty routing Service), got empty")
	}
	if got := drainServiceForPod(input, plan, runnerPod("default")); got == "" {
		t.Errorf("single-pod default pod must be routable (non-empty routing Service), got empty")
	}
	if got := drainServiceForPod(input, plan, runnerPod("worker")); got != "" {
		t.Errorf("gang worker must be skipped so the surge rotation gate does not wedge; got routing Service %q", got)
	}
}

// End-to-end tests for the status.migrations-driven Migrate executor:
// the record is the single source of truth (dispatcher selects it, the
// executor resumes from SurgeInstance + Phase and advances the phase
// through MutateMigration). Single-pod and gang walks share one harness.
//
// The harness simulates the environment between Migrate passes:
// "kubelet" flips ContainersReady, an "endpoint controller" maintains
// per-revision EndpointSlices from pod serving state, and an
// "aggregator" refreshes the per-Instance pod counters on the IR —
// exactly the three external actors the real gates wait on.

// migFixture is the shared single-pod / gang migration harness.
type migFixture struct {
	c         client.Client
	isvc      *v1beta1.InferenceService
	component workload.ComponentType
	plan      workload.ComponentPlan
	gangSched bool
	// records is the in-memory status.migrations authority the
	// MutateMigration seam mutates; each pass mirrors it onto
	// ObservedState.Migrations exactly like the adapter does.
	records []workload.MigrationRecord
	// clk, when set, is wired into both Deps and ReconcileInput so
	// deadline comparisons are deterministic (expiry tests).
	clk clock.Clock
	// forceDelete, when set, arms the stuck-Terminating escalation on
	// the ReconcileInput (mirrors the adapter's config plumbing).
	forceDelete *workload.ForceDeletePolicy
	// stuckPodGrace, when set, arms the terminal-waiting evidence read
	// on the ReconcileInput (mirrors the adapter's config plumbing).
	stuckPodGrace time.Duration
	// migrationAudit is the capacity policy migration admission runs
	// against; the constructors set the stand-in for a chart-configured
	// deployment. A test pinning the unconfigured path clears it.
	migrationAudit *workload.MigrationAuditPolicy
	// conditions captures every Component condition the pass stamps, in
	// write order, when a test wires the capture.
	conditions *[]metav1.Condition
	// wake collects the wake-up a pass deposits for work it held.
	wake *workload.PassWake
	// recorder, when set, captures the events a pass emits.
	recorder *record.FakeRecorder
	// finalizeInstanceResources models per-Instance cleanup owned by the IR
	// adapter. When set, the fixture also wires the guarded batch status writer.
	finalizeInstanceResources func(context.Context, int32) (bool, error)
}

// deps builds the fixture's workload.Deps (clock and recorder included
// when set).
func (f *migFixture) deps() workload.Deps {
	d := legacyTestDeps(f.c)
	d.Clock = f.clk
	if f.recorder != nil {
		d.Recorder = f.recorder
	}
	return d
}

// fixtureMigrationAudit is the migration capacity policy the fixtures
// admit against, standing in for the operator config a deployed chart
// supplies. Capacity-specific expectations state their own numbers.
func fixtureMigrationAudit() *workload.MigrationAuditPolicy {
	return &workload.MigrationAuditPolicy{MaxInFlight: 3, MaxPerWindow: 10, Window: time.Hour}
}

// mkMigRecord is the shape the accept pass writes: Accepted, Manual,
// no surge index.
func mkMigRecord(uuid string, sourceIdx int32, fromNode string) workload.MigrationRecord {
	return workload.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workload.MigrationTriggerManual,
		Phase:          workload.MigrationPhaseAccepted,
		SourceInstance: sourceIdx,
		FromNode:       fromNode,
		Reason:         "maintenance",
		StartedAt:      metav1.Now(),
		Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
	}
}

func (f *migFixture) record(t *testing.T, uuid string) *workload.MigrationRecord {
	t.Helper()
	r := workload.FindMigrationRecord(f.records, uuid)
	if r == nil {
		t.Fatalf("record %s missing", uuid)
	}
	return r
}

// irKey is the fixture IR's NamespacedName.
func (f *migFixture) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: f.isvc.Namespace, Name: legacyIRName(f.isvc, f.component)}
}

func (f *migFixture) getIR(t *testing.T) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := f.c.Get(context.Background(), f.irKey(), ir); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	return ir
}

// migInstStatusToWorkload / migInstStatusFromWorkload are the fixture's
// full-fidelity InstanceStatus converters (counters included — the
// Available gate reads them; the lossy legacy helpers drop counters).
// Local because importing v1beta1convert from a workload/ops white-box
// test closes an import cycle through the workload root package.
func migInstStatusToWorkload(s v1beta1.OMENativeInstanceStatus) workload.InstanceStatus {
	out := workload.InstanceStatus{
		Index:             s.Index,
		Incarnation:       s.Incarnation,
		Phase:             workload.InstancePhase(s.Phase),
		RunningRevision:   s.RunningRevision,
		TargetRevision:    s.TargetRevision,
		ActiveOrdinal:     s.ActiveOrdinal,
		PodCount:          s.PodCount,
		ReadyPodCount:     s.ReadyPodCount,
		ServingPodCount:   s.ServingPodCount,
		AvailablePodCount: s.AvailablePodCount,
		ScheduledPodCount: s.ScheduledPodCount,
		Admitted:          s.Admitted,
		NodesOccupied:     append([]string(nil), s.NodesOccupied...),
		Conditions:        append([]metav1.Condition(nil), s.Conditions...),
		Operation:         legacyFromV1beta1Op(s.Operation),
	}
	if s.LastFailure != nil {
		out.LastFailure = &workload.InstanceTermination{
			PodName:       s.LastFailure.PodName,
			ContainerName: s.LastFailure.ContainerName,
			Reason:        s.LastFailure.Reason,
			Message:       s.LastFailure.Message,
			Time:          s.LastFailure.Time,
		}
		if s.LastFailure.ExitCode != nil {
			exitCode := *s.LastFailure.ExitCode
			out.LastFailure.ExitCode = &exitCode
		}
	}
	return out
}

func migInstStatusFromWorkload(w workload.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	out := v1beta1.OMENativeInstanceStatus{
		Index:             w.Index,
		Incarnation:       w.Incarnation,
		Phase:             v1beta1.OMENativeInstancePhase(w.Phase),
		RunningRevision:   w.RunningRevision,
		TargetRevision:    w.TargetRevision,
		ActiveOrdinal:     w.ActiveOrdinal,
		PodCount:          w.PodCount,
		ReadyPodCount:     w.ReadyPodCount,
		ServingPodCount:   w.ServingPodCount,
		AvailablePodCount: w.AvailablePodCount,
		ScheduledPodCount: w.ScheduledPodCount,
		Admitted:          w.Admitted,
		NodesOccupied:     append([]string(nil), w.NodesOccupied...),
		Conditions:        append([]metav1.Condition(nil), w.Conditions...),
		Operation:         legacyToV1beta1Op(w.Operation),
	}
	if w.LastFailure != nil {
		out.LastFailure = &v1beta1.InstanceTermination{
			PodName:       w.LastFailure.PodName,
			ContainerName: w.LastFailure.ContainerName,
			Reason:        w.LastFailure.Reason,
			Message:       w.LastFailure.Message,
			Time:          w.LastFailure.Time,
		}
		if w.LastFailure.ExitCode != nil {
			exitCode := *w.LastFailure.ExitCode
			out.LastFailure.ExitCode = &exitCode
		}
	}
	return out
}

// input builds a fresh full-fidelity ReconcileInput off the persisted
// IR — counters included (the Available gate reads them), unlike the
// lossy legacyTestInput conversion.
func (f *migFixture) input(t *testing.T) workload.ReconcileInput {
	t.Helper()
	in := legacyTestInput(f.isvc, f.c, f.component)
	ir := f.getIR(t)
	insts := make([]workload.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		insts = append(insts, migInstStatusToWorkload(s))
	}
	in.ObservedState.InstanceStatuses = insts
	in.MutateInstance = f.mutateInstance()
	in.RemoveInstance = legacyRemoveInstance(f.c, f.isvc, f.component)
	in.DesiredSpec.GangSchedulingAvailable = f.gangSched
	in.Clock = f.clk
	in.ForceDelete = f.forceDelete
	in.StuckPodGrace = f.stuckPodGrace
	in.MigrationAudit = f.migrationAudit
	if f.conditions != nil {
		in.WriteAggregateCondition = func(_ context.Context, cond metav1.Condition) error {
			*f.conditions = append(*f.conditions, cond)
			return nil
		}
	}
	if f.wake == nil {
		f.wake = &workload.PassWake{}
	}
	in.PassWake = f.wake
	if f.finalizeInstanceResources != nil {
		in.FinalizeInstanceResources = f.finalizeInstanceResources
		in.ApplyInstanceMutationsWithRetryBlock = f.applyInstanceMutationsWithRetryBlock
	}
	in.ObservedState.Migrations = append([]workload.MigrationRecord(nil), f.records...)
	in.MutateMigration = func(_ context.Context, uuid string, mutate func(*workload.MigrationRecord) bool) error {
		for i := range f.records {
			if f.records[i].RequestUUID == uuid {
				r := f.records[i]
				if mutate(&r) {
					f.records[i] = r
				}
				return nil
			}
		}
		return nil
	}
	return in
}

func (f *migFixture) applyInstanceMutationsWithRetryBlock(
	ctx context.Context,
	mutations []workload.InstanceMutation,
	_ string,
	mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition,
) error {
	if mutateRetryBlock != nil {
		return fmt.Errorf("migration fixture does not support RetryBlock mutations")
	}
	ir := &v1beta1.InferenceReplica{}
	if err := f.c.Get(ctx, f.irKey(), ir); err != nil {
		return err
	}
	slots := make(map[int32]workload.InstanceStatus, len(ir.Status.InstanceStatuses))
	for _, status := range ir.Status.InstanceStatuses {
		slots[status.Index] = migInstStatusToWorkload(status)
	}
	snapshot := workload.InstanceMutationSnapshot{
		OwnerUID:  f.isvc.UID,
		Instances: slots,
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workload.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*workload.InstanceStatus, *workload.InstanceStatus)
		previous *workload.InstanceStatus
		current  *workload.InstanceStatus
	}
	var committed []committedMutation
	changed := false
	for _, mutation := range mutations {
		status, found := slots[mutation.Index]
		if mutation.Remove {
			if !found || mutation.Precondition != nil && !mutation.Precondition(&status) {
				continue
			}
			previous := status
			delete(slots, mutation.Index)
			changed = true
			if mutation.OnCommit != nil {
				committed = append(committed, committedMutation{callback: mutation.OnCommit, previous: &previous})
			}
			continue
		}
		if !found {
			status = workload.InstanceStatus{Index: mutation.Index}
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) || mutation.Mutate == nil {
			continue
		}
		previous := status
		if !mutation.Mutate(&status) {
			continue
		}
		slots[mutation.Index] = status
		changed = true
		if mutation.OnCommit != nil {
			current := status
			committed = append(committed, committedMutation{callback: mutation.OnCommit, previous: &previous, current: &current})
		}
	}
	if changed {
		persisted := make([]v1beta1.OMENativeInstanceStatus, 0, len(slots))
		for _, original := range ir.Status.InstanceStatuses {
			if status, found := slots[original.Index]; found {
				persisted = append(persisted, migInstStatusFromWorkload(status))
				delete(slots, original.Index)
			}
		}
		for _, status := range slots {
			persisted = append(persisted, migInstStatusFromWorkload(status))
		}
		ir.Status.InstanceStatuses = persisted
		if err := f.c.Status().Update(ctx, ir); err != nil {
			return err
		}
	}
	for _, mutation := range committed {
		mutation.callback(mutation.previous, mutation.current)
	}
	return nil
}

// mutateInstance is a full-fidelity (v1beta1convert) round-trip writer
// so counter fields survive per-op writes.
func (f *migFixture) mutateInstance() func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		create := false
		if err := f.c.Get(ctx, f.irKey(), ir); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: f.irKey().Namespace, Name: f.irKey().Name}}
			create = true
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		slot := v1beta1.OMENativeInstanceStatus{Index: idx}
		if pos != -1 {
			slot = ir.Status.InstanceStatuses[pos]
		}
		w := migInstStatusToWorkload(slot)
		if !mutate(&w) {
			return nil
		}
		updated := migInstStatusFromWorkload(w)
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := f.c.Create(ctx, bare); err != nil {
				return err
			}
			bare.Status = ir.Status
			ir = bare
		}
		return f.c.Status().Update(ctx, ir)
	}
}

// pass runs one Migrate pass the way the dispatcher would: request
// reconstructed from the record, expectations reset (informer caught
// up).
func (f *migFixture) pass(t *testing.T, uuid string) (done, accepted bool) {
	t.Helper()
	done, accepted, err := f.passResult(t, uuid)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return done, accepted
}

func (f *migFixture) passResult(t *testing.T, uuid string) (done, accepted bool, err error) {
	t.Helper()
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(f.component),
		Instance:        rec.SourceInstance,
		FromNode:        rec.FromNode,
		HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
		Reason:          rec.Reason,
	}
	in := f.input(t)
	return Migrate(context.Background(), f.deps(), in, f.plan, rec.SourceInstance, uuid, req)
}

func (f *migFixture) listPods(t *testing.T) []*corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := f.c.List(context.Background(), list, client.InNamespace(f.isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out
}

func containersReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// react simulates the environment settling between passes: kubelet
// (ContainersReady on every pod), the endpoint controller (per-revision
// EndpointSlices reflect serving state), and the status aggregator
// (per-Instance pod counters on the IR).
func (f *migFixture) react(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	// Kubelet: every pod's containers come up.
	for _, pod := range f.listPods(t) {
		if containersReady(pod) {
			continue
		}
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
		if err := f.c.Status().Update(ctx, pod); err != nil {
			t.Fatalf("mark pod ready: %v", err)
		}
	}

	// Endpoint controller: one slice per routable per-revision Service;
	// endpoint Ready mirrors serving && containers-ready; no members →
	// slice deleted.
	byService := map[string][]*corev1.Pod{}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelRunner] == "worker" {
			continue
		}
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		svc := query.PerRevisionServiceName(f.isvc.Name, f.component, hash)
		byService[svc] = append(byService[svc], pod)
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := f.c.List(ctx, slices, client.InNamespace(f.isvc.Namespace)); err != nil {
		t.Fatalf("list slices: %v", err)
	}
	seen := map[string]bool{}
	for svc, pods := range byService {
		endpoints := make([]discoveryv1.Endpoint, 0, len(pods))
		for _, pod := range pods {
			ready := podreadiness.IsServing(pod) && containersReady(pod)
			r := ready
			endpoints = append(endpoints, discoveryv1.Endpoint{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: &r},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
			})
		}
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: svc + "-slice", Namespace: f.isvc.Namespace,
				Labels: map[string]string{discoveryv1.LabelServiceName: svc},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   endpoints,
		}
		seen[slice.Name] = true
		existing := &discoveryv1.EndpointSlice{}
		err := f.c.Get(ctx, types.NamespacedName{Namespace: slice.Namespace, Name: slice.Name}, existing)
		switch {
		case err == nil:
			existing.Endpoints = endpoints
			if uerr := f.c.Update(ctx, existing); uerr != nil {
				t.Fatalf("update slice: %v", uerr)
			}
		case apierrors.IsNotFound(err):
			if cerr := f.c.Create(ctx, slice); cerr != nil {
				t.Fatalf("create slice: %v", cerr)
			}
		default:
			t.Fatalf("get slice: %v", err)
		}
	}
	for i := range slices.Items {
		if !seen[slices.Items[i].Name] {
			if err := f.c.Delete(ctx, &slices.Items[i]); err != nil {
				t.Fatalf("delete slice: %v", err)
			}
		}
	}

	// Aggregator: refresh per-Instance pod counters on the IR.
	ir := f.getIR(t)
	pods := f.listPods(t)
	changed := false
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		var podCount, readyCount, availCount int32
		for _, pod := range pods {
			if pod.Labels[query.LabelInstanceIdx] != fmt.Sprintf("%d", s.Index) {
				continue
			}
			podCount++
			if containersReady(pod) {
				readyCount++
				if podreadiness.IsServing(pod) {
					availCount++
				}
			}
		}
		if s.PodCount != podCount || s.ReadyPodCount != readyCount || s.AvailablePodCount != availCount {
			s.PodCount, s.ReadyPodCount, s.AvailablePodCount = podCount, readyCount, availCount
			changed = true
		}
	}
	if changed {
		if err := f.c.Status().Update(context.Background(), ir); err != nil {
			t.Fatalf("update IR counters: %v", err)
		}
	}
}

// drive runs Migrate passes (react between them) until done.
func (f *migFixture) drive(t *testing.T, uuid string, maxPasses int) {
	t.Helper()
	for i := 0; i < maxPasses; i++ {
		done, accepted := f.pass(t, uuid)
		if done {
			return
		}
		if !accepted {
			t.Fatalf("pass %d: unexpected defer-without-ownership (record=%+v)", i, *f.record(t, uuid))
		}
		f.react(t)
	}
	t.Fatalf("migration did not complete in %d passes; record=%+v", maxPasses, *f.record(t, uuid))
}

// newSinglePodMigFixture: instance 0 Ready on node-a with a seeded
// RunningRevision and a serving pod.
func newSinglePodMigFixture(t *testing.T) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	sourcePod := legacyPodForInstance(isvc, 0, true, true)
	sourcePod.Spec.NodeName = "node-a"
	c := legacyNewFakeClient(t, isvc, ir, sourcePod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	})
	return &migFixture{
		c: c, isvc: isvc, component: workload.ComponentEngine,
		plan:           legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil),
		migrationAudit: fixtureMigrationAudit(),
	}
}

// seedMigrationCompletionTailWindow reconstructs the durable state after the
// source status was removed and the surge was promoted, while the migration
// record is still Draining. The caller controls whether authoritative source
// pods remain and may weaken the surge evidence for negative cases.
func seedMigrationCompletionTailWindow(
	t *testing.T,
	f *migFixture,
	deleteSourcePods bool,
	mutateSurge func(*v1beta1.OMENativeInstanceStatus),
) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	ir := f.getIR(t)
	if len(ir.Status.InstanceStatuses) != 1 || ir.Status.InstanceStatuses[0].Index != 0 {
		t.Fatalf("source fixture statuses = %+v, want only instance 0", ir.Status.InstanceStatuses)
	}
	source := ir.Status.InstanceStatuses[0]
	if source.RunningRevision == "" {
		t.Fatal("source fixture has no RunningRevision")
	}
	surge := v1beta1.OMENativeInstanceStatus{
		Index:           1,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: source.RunningRevision,
	}
	if mutateSurge != nil {
		mutateSurge(&surge)
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{surge}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed completion-tail status: %v", err)
	}
	if deleteSourcePods {
		deleted := 0
		for _, pod := range f.listPods(t) {
			if pod.Labels[query.LabelInstanceIdx] != "0" {
				continue
			}
			if err := f.c.Delete(context.Background(), pod); err != nil {
				t.Fatalf("delete source pod %s: %v", pod.Name, err)
			}
			deleted++
		}
		if deleted == 0 {
			t.Fatal("source fixture has no pod to delete")
		}
	}
	return source
}

// newMultiInstanceMigFixture: n single-pod instances (0..n-1), each
// Ready on node-a with a seeded RunningRevision and a serving pod —
// the shape a batch of migration requests lands on.
func newMultiInstanceMigFixture(t *testing.T, n int32) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc := legacyMinimalISVC("llama-70b", "prod", int(n))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, n)
	for i := int32(0); i < n; i++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
		})
	}
	ir := legacyInstanceIR(isvc, workload.ComponentEngine, statuses...)
	objs := []client.Object{isvc, ir}
	for i := int32(0); i < n; i++ {
		pod := legacyPodForInstance(isvc, i, true, true)
		pod.Spec.NodeName = "node-a"
		objs = append(objs, pod)
	}
	c := legacyNewFakeClient(t, objs...)
	for i := int32(0); i < n; i++ {
		legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, i, &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
		})
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	plan.Replicas = n
	plan.Instances = nil
	for i := int32(0); i < n; i++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index: i, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	return &migFixture{c: c, isvc: isvc, component: workload.ComponentEngine, plan: plan, migrationAudit: fixtureMigrationAudit()}
}

// newGangMigFixture: instance 0 is a 3-pod gang (leader + 2 workers)
// spanning nodes, RunningRevision carries leader + worker templates.
func newGangMigFixture(t *testing.T) *migFixture {
	t.Helper()
	return newGangMigFixtureWithWorkerSpec(t, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
	})
}

// newGangMigFixtureWithWorkerSpec is newGangMigFixture with the
// RunningRevision's WORKER template supplied by the caller — the
// worker-pinned-affinity rejection test needs a worker spec whose hard
// NodeAffinity collides with the migration overlay.
func newGangMigFixtureWithWorkerSpec(t *testing.T, workerSpec *corev1.PodSpec) *migFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	mkGangPod := func(runner string, ordinal int32, node string) *corev1.Pod {
		labels := legacyTestPodLabels(isvc.Name, workload.ComponentEngine, 0, runner, 1, ordinal)
		labels[query.LabelRevisionHash] = testRevisionHashLegacy
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      query.PodName(isvc.Name, workload.ComponentEngine, 0, runner, ordinal),
				Namespace: isvc.Namespace,
				Labels:    labels,
			},
			Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}},
		}
		now := metav1.Now()
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
		return pod
	}
	leader := mkGangPod("leader", 0, "node-a")
	worker0 := mkGangPod("worker", 0, "node-b")
	worker1 := mkGangPod("worker", 1, "node-c")
	c := legacyNewFakeClient(t, isvc, ir, leader, worker0, worker1)

	leaderSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "llama:v1"}}}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		leaderSpec, workerSpec, nil, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevisionWithWorker: %v", err)
	}
	freshIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}, freshIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	freshIR.Status.InstanceStatuses[0].RunningRevision = cr.Name
	if err := c.Status().Update(context.Background(), freshIR); err != nil {
		t.Fatalf("seed gang RunningRevision: %v", err)
	}

	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners: []workload.RunnerPlan{
				{Name: "leader", Size: 1},
				{Name: "worker", Size: 2},
			},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy:       workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
	}
	return &migFixture{
		c: c, isvc: isvc, component: workload.ComponentEngine,
		plan: plan, gangSched: true,
		migrationAudit: fixtureMigrationAudit(),
	}
}

// TestMigrate_EntryLifecycle_SinglePod walks the full Manual chain for
// a single-pod Instance: Accepted -> allocation (SurgeInstance +
// SurgePending recorded, pair stamped, ledger Started upserted with the
// real index) -> surge pod created -> SurgeReady -> Draining ->
// Completed (source pod deleted, source status removed, surge
// promoted, record terminal with CompletedAt).
func TestMigrate_EntryLifecycle_SinglePod(t *testing.T) {
	f := newSinglePodMigFixture(t)
	finalizations := 0
	f.finalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		finalizations++
		if index != 0 || findInstanceStatusOnIRForFixture(t, f, index) == nil {
			t.Fatalf("finalization ran without the owned source status: index=%d", index)
		}
		return true, nil
	}
	const uuid = "mig-single-1"
	rec0 := mkMigRecord(uuid, 0, "node-a")
	rec0.HintTargetNodes = []string{"node-hint-1", "node-hint-2"}
	f.records = []workload.MigrationRecord{rec0}

	// Pass 1: fresh record -> allocation + stamps + surge pod.
	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseSurgePending {
		t.Errorf("pass 1: record phase = %s, want SurgePending", rec.Phase)
	}
	if rec.SurgeInstance == nil || *rec.SurgeInstance != 1 {
		t.Fatalf("pass 1: record SurgeInstance = %v, want 1", rec.SurgeInstance)
	}
	if rec.AllocatedAt == nil {
		t.Errorf("pass 1: AllocatedAt must be stamped in the same write as SurgeInstance")
	}
	surgePodName := query.PodName(f.isvc.Name, f.component, 1, "default", 0)
	surgePod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: surgePodName}, surgePod); err != nil {
		t.Fatalf("surge pod must exist after pass 1: %v", err)
	}
	// The record's hints flow through the reconstructed request +
	// MigrationOverlay into the rendered surge (preferred affinity).
	if !surgePodHasHintTerm(surgePod, rec0.HintTargetNodes) {
		t.Errorf("surge pod must carry the record's HintTargetNodes as preferred node affinity; affinity=%+v", surgePod.Spec.Affinity)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if e := ledger.InFlightEntry(uuid); e == nil || e.SurgeInstance != 1 {
		t.Errorf("ledger Started row must carry the real surge index; got %+v", e)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source must be stamped Migrating; got %+v", src)
	}

	// Drive the rest; the record must pass through SurgeReady/Draining
	// before Completed (observed transitively via the terminal state).
	f.react(t)
	f.drive(t, uuid, 10)

	rec = f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("record must finish Completed with CompletedAt; got %+v", *rec)
	}
	// Source status removed; surge promoted Ready at the source's revision.
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("source InstanceStatus must be removed on completion; got %+v", src)
	}
	if finalizations != 1 {
		t.Errorf("source resource finalizations = %d, want 1", finalizations)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady || surge.RunningRevision == "" || surge.Operation != nil {
		t.Errorf("surge must be promoted Ready on the source revision with the op cleared; got %+v", surge)
	}
	// Source pod gone.
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: query.PodName(f.isvc.Name, f.component, 0, "default", 0)}, srcPod); !apierrors.IsNotFound(err) {
		t.Errorf("source pod must be deleted; get returned %v", err)
	}
	// Ledger terminal.
	ledger, _ = audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger must record the terminal Completed row")
	}
	// Terminal record is never picked again.
	if picked := workload.NextManualMigration(f.records); picked != nil {
		t.Errorf("completed record must not be re-selected; picked %q", picked.RequestUUID)
	}
}

func TestMigrate_FinalizationFailureRetainsSourceAndRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	finalizeErr := fmt.Errorf("PodGroup delete failed")
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return false, finalizeErr
	}
	const uuid = "mig-finalize-failure"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			if !strings.Contains(err.Error(), finalizeErr.Error()) {
				t.Fatalf("finalization error = %v", err)
			}
			if finalizations != 1 {
				t.Fatalf("finalizations = %d, want 1", finalizations)
			}
			if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
				t.Fatal("finalization failure removed the source status")
			}
			if record := f.record(t, uuid); record.Phase.Terminal() || record.CompletedAt != nil {
				t.Fatalf("finalization failure stamped a terminal migration record: %+v", *record)
			}
			return
		}
		if done || !accepted {
			t.Fatalf("pass %d completed or deferred before finalization failure: done=%v accepted=%v", pass, done, accepted)
		}
		f.react(t)
	}
	t.Fatal("migration never reached the finalization failure")
}

func TestMigrate_FinalizationWaitsForResourceAbsence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-finalize-pending"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if finalizations == 0 {
			if done || !accepted {
				t.Fatalf("pass %d completed or deferred before finalization: done=%v accepted=%v", pass, done, accepted)
			}
			f.react(t)
			continue
		}
		if done || !accepted {
			t.Fatalf("delete accepted completed migration: done=%v accepted=%v", done, accepted)
		}
		if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
			t.Fatal("delete accepted removed the source status")
		}
		if record := f.record(t, uuid); record.Phase.Terminal() || record.CompletedAt != nil {
			t.Fatalf("delete accepted stamped a terminal migration record: %+v", *record)
		}

		record := f.record(t, uuid)
		if record.SurgeInstance == nil {
			t.Fatal("promoted migration has no surge instance")
		}
		promoted := f.plan.Instances[0]
		promoted.Index = *record.SurgeInstance
		f.plan.Instances = []workload.InstancePlan{promoted}
		resourcesAbsent = true
		done, accepted, err = f.passResult(t, uuid)
		if err != nil || !done || !accepted {
			t.Fatalf("absence pass: done=%v accepted=%v err=%v", done, accepted, err)
		}
		if source := findInstanceStatusOnIRForFixture(t, f, 0); source != nil {
			t.Fatalf("absence pass retained source status: %+v", source)
		}
		if finalizations != 2 {
			t.Fatalf("finalizations=%d want 2", finalizations)
		}
		return
	}
	t.Fatal("migration never reached resource finalization")
}

func TestMigrate_GangPromotionTailCompletesFromRetainedSource(t *testing.T) {
	f := newGangMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-gang-promoted-tail"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgeIndex := driveGangMigrationToPendingFinalization(t, f, uuid)
	if finalizations != 1 {
		t.Fatalf("pending finalizations=%d want 1", finalizations)
	}

	// The promoted surge is the steady plan member while the source status
	// remains as the terminal-cleanup ownership token.
	promoted := f.plan.Instances[0]
	promoted.Index = surgeIndex
	f.plan.Instances = []workload.InstancePlan{promoted}
	resourcesAbsent = true

	done, accepted, err := f.passResult(t, uuid)
	defaultPod := &corev1.Pod{}
	defaultPodName := query.PodName(f.isvc.Name, f.component, surgeIndex, "default", 0)
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: defaultPodName}, defaultPod); !apierrors.IsNotFound(err) {
		t.Fatalf("completion tail synthesized a single-pod runner: get %s returned %v", defaultPodName, err)
	}
	if err != nil || !done || !accepted {
		t.Fatalf("completion pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 2 {
		t.Fatalf("finalizations=%d want 2", finalizations)
	}
	if source := findInstanceStatusOnIRForFixture(t, f, 0); source != nil {
		t.Fatalf("completion retained source status: %+v", source)
	}
	record := f.record(t, uuid)
	if record.Phase != workload.MigrationPhaseCompleted || record.CompletedAt == nil {
		t.Fatalf("completion record = %+v", *record)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("completion ledger: entries=%+v err=%v", ledger.Entries, err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid ||
		ledger.Entries[0].Phase != audit.PhaseCompleted || ledger.Entries[0].Outcome != "migrated" {
		t.Fatalf("completion ledger=%+v want one Completed migrated entry", ledger.Entries)
	}
	runnerCounts := map[string]int{}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == fmt.Sprintf("%d", surgeIndex) {
			runnerCounts[pod.Labels[query.LabelRunner]]++
		}
	}
	if runnerCounts["leader"] != 1 || runnerCounts["worker"] != 2 || len(runnerCounts) != 2 {
		t.Fatalf("promoted gang runners=%v want leader=1 worker=2", runnerCounts)
	}
}

func TestMigrate_GangPromotionTailRejectsDifferentRevision(t *testing.T) {
	f := newGangMigFixture(t)
	resourcesAbsent := false
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return resourcesAbsent, nil
	}
	const uuid = "mig-gang-promoted-tail-revision"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgeIndex := driveGangMigrationToPendingFinalization(t, f, uuid)

	ir := f.getIR(t)
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == surgeIndex {
			ir.Status.InstanceStatuses[i].RunningRevision = "different-revision"
		}
	}
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("change promoted revision: %v", err)
	}
	promoted := f.plan.Instances[0]
	promoted.Index = surgeIndex
	f.plan.Instances = []workload.InstancePlan{promoted}
	resourcesAbsent = true

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || done || !accepted {
		t.Fatalf("guarded pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 1 {
		t.Fatalf("revision mismatch ran finalization: calls=%d want 1", finalizations)
	}
	if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil {
		t.Fatal("revision mismatch removed the source status")
	}
	record := f.record(t, uuid)
	if record.Phase != workload.MigrationPhaseDraining || record.CompletedAt != nil {
		t.Fatalf("revision mismatch closed the record: %+v", *record)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("revision mismatch wrote a terminal ledger entry: %+v", ledger.Entries)
	}
}

func driveGangMigrationToPendingFinalization(t *testing.T, f *migFixture, uuid string) int32 {
	t.Helper()
	for pass := 0; pass < 12; pass++ {
		done, accepted, err := f.passResult(t, uuid)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done || !accepted {
			t.Fatalf("pass %d completed or deferred before finalization: done=%v accepted=%v", pass, done, accepted)
		}
		record := f.record(t, uuid)
		if record.SurgeInstance != nil && record.Phase == workload.MigrationPhaseDraining {
			surge := findInstanceStatusOnIRForFixture(t, f, *record.SurgeInstance)
			source := findInstanceStatusOnIRForFixture(t, f, record.SourceInstance)
			sourcePods := 0
			for _, pod := range f.listPods(t) {
				if pod.Labels[query.LabelInstanceIdx] == fmt.Sprintf("%d", record.SourceInstance) {
					sourcePods++
				}
			}
			if source != nil && sourcePods == 0 && surge != nil &&
				surge.Phase == v1beta1.OMENativeInstanceReady && surge.Operation == nil && surge.RunningRevision != "" {
				return *record.SurgeInstance
			}
		}
		f.react(t)
	}
	t.Fatal("gang migration never reached pending resource finalization")
	return -1
}

func TestMigrate_CompletionTailCrashRecoversBeforeDeadline(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-tail-recovery"
	source := seedMigrationCompletionTailWindow(t, f, true, nil)
	surgeIdx := int32(1)
	record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
	record.SurgeInstance = &surgeIdx
	record.Phase = workload.MigrationPhaseDraining
	f.records = []workload.MigrationRecord{record}

	finalizations := 0
	f.finalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		finalizations++
		if index != 0 {
			t.Fatalf("finalized instance %d, want source 0", index)
		}
		if status := findInstanceStatusOnIRForFixture(t, f, index); status != nil {
			t.Fatalf("residual resource finalization ran while source status was live: %+v", status)
		}
		return true, nil
	}

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || !done || !accepted {
		t.Fatalf("recovery pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if !clk.Now().Before(record.Deadline.Time) {
		t.Fatalf("test record expired before recovery: now=%v deadline=%v", clk.Now(), record.Deadline.Time)
	}
	if finalizations != 1 {
		t.Fatalf("residual resource finalizations = %d, want 1", finalizations)
	}
	completed := f.record(t, uuid)
	if completed.Phase != workload.MigrationPhaseCompleted || completed.CompletedAt == nil ||
		!strings.Contains(completed.Message, "migrated to instance=1") {
		t.Fatalf("recovered record = %+v, want Completed migration to instance 1", *completed)
	}
	if sourceStatus := findInstanceStatusOnIRForFixture(t, f, 0); sourceStatus != nil {
		t.Fatalf("recovery resurrected source status: %+v", sourceStatus)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, surgeIdx)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady ||
		surge.Operation != nil || surge.RunningRevision != source.RunningRevision {
		t.Fatalf("recovery changed promoted surge: %+v", surge)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load recovery ledger: %v", err)
	}
	if len(ledger.Entries) != 1 || ledger.Entries[0].RequestUUID != uuid ||
		ledger.Entries[0].Phase != audit.PhaseCompleted || ledger.Entries[0].Outcome != "migrated" {
		t.Fatalf("recovery ledger = %+v, want one Completed migrated entry", ledger.Entries)
	}
	if picked := workload.NextManualMigration(f.records); picked != nil {
		t.Fatalf("recovered record was selected again: %+v", *picked)
	}

	done, accepted, err = f.passResult(t, uuid)
	if err != nil || !done || !accepted || finalizations != 1 {
		t.Fatalf("terminal replay: done=%v accepted=%v err=%v finalizations=%d", done, accepted, err, finalizations)
	}
}

func TestMigrate_CompletionTailRecoveryRequiresCompleteEvidence(t *testing.T) {
	tests := []struct {
		name              string
		deleteSourcePods  bool
		mutateSurgeStatus func(*v1beta1.OMENativeInstanceStatus)
	}{
		{
			name:             "source pod remains",
			deleteSourcePods: false,
		},
		{
			name:             "surge is not Ready",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.Phase = v1beta1.OMENativeInstanceCreating
			},
		},
		{
			name:             "surge still owns an operation",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationType(workload.InstanceOperationMigrate)}
			},
		},
		{
			name:             "surge has no running revision",
			deleteSourcePods: true,
			mutateSurgeStatus: func(status *v1beta1.OMENativeInstanceStatus) {
				status.RunningRevision = ""
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			clk := f.withFakeClock()
			const uuid = "mig-tail-incomplete"
			seedMigrationCompletionTailWindow(t, f, test.deleteSourcePods, test.mutateSurgeStatus)
			surgeIdx := int32(1)
			record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
			record.SurgeInstance = &surgeIdx
			record.Phase = workload.MigrationPhaseDraining
			f.records = []workload.MigrationRecord{record}
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}

			done, accepted, err := f.passResult(t, uuid)
			if err != nil || done || !accepted {
				t.Fatalf("guarded pass: done=%v accepted=%v err=%v", done, accepted, err)
			}
			if finalizations != 0 {
				t.Fatalf("incomplete evidence triggered %d resource finalizations", finalizations)
			}
			got := f.record(t, uuid)
			if got.Phase != workload.MigrationPhaseDraining || got.CompletedAt != nil {
				t.Fatalf("incomplete evidence closed record: %+v", *got)
			}
			ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
			if err != nil {
				t.Fatalf("load ledger: %v", err)
			}
			if ledger.HasCompletedOrFailedRequest(uuid) {
				t.Fatalf("incomplete evidence wrote a terminal ledger row: %+v", ledger.Entries)
			}
		})
	}
}

func TestMigrate_CompletionTailRecoveryRechecksAuthoritativeAbsence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	const uuid = "mig-tail-status-drift"
	source := seedMigrationCompletionTailWindow(t, f, true, nil)
	surgeIdx := int32(1)
	record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
	record.SurgeInstance = &surgeIdx
	record.Phase = workload.MigrationPhaseDraining
	f.records = []workload.MigrationRecord{record}
	finalizations := 0
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalizations++
		return true, nil
	}

	input := f.input(t)
	liveIR := f.getIR(t)
	liveIR.Status.InstanceStatuses = append(liveIR.Status.InstanceStatuses, source)
	if err := f.c.Status().Update(context.Background(), liveIR); err != nil {
		t.Fatalf("restore live source status: %v", err)
	}
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      record.SourceInstance,
		FromNode:      record.FromNode,
		Reason:        record.Reason,
	}
	done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, record.SourceInstance, uuid, req)
	if err != nil || done || !accepted {
		t.Fatalf("drift pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if finalizations != 0 {
		t.Fatalf("live source status triggered %d residual resource finalizations", finalizations)
	}
	if status := findInstanceStatusOnIRForFixture(t, f, 0); status == nil {
		t.Fatal("authoritative source status was removed")
	}
	if got := f.record(t, uuid); got.Phase != workload.MigrationPhaseDraining || got.CompletedAt != nil {
		t.Fatalf("authoritative status drift closed record: %+v", *got)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if ledger.HasCompletedOrFailedRequest(uuid) {
		t.Fatalf("authoritative status drift wrote a terminal ledger row: %+v", ledger.Entries)
	}
}

// TestMigrate_EntryLifecycle_Gang walks the same chain for a 3-pod
// gang (leader + 2 workers). Gang-specific assertions: the fresh-stamp
// pass requeues BEFORE creating pods (PodGroup ordering under gang
// scheduling), the surge materializes all 3 pods with the worker
// template from the RunningRevision, and completion tears down all 3
// source pods.
func TestMigrate_EntryLifecycle_Gang(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-1"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-b")} // a worker's node — gang-aware validateFromNode accepts any member's node

	// Pass 1: allocation + stamps, then the gang PodGroup requeue —
	// NO surge pods yet (EnsurePodGroups must see the pinned index
	// before the gang's pods render).
	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 1: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseSurgePending || rec.SurgeInstance == nil || *rec.SurgeInstance != 1 {
		t.Fatalf("pass 1: record must be SurgePending with SurgeInstance=1; got %+v", *rec)
	}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "1" {
			t.Fatalf("gang fresh-stamp pass must requeue before creating surge pods; found %s", pod.Name)
		}
	}

	// Pass 2: surge gang pods render — 3 of them, worker template from
	// the RunningRevision's worker payload.
	done, accepted = f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("pass 2: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	var surgeLeaders, surgeWorkers int
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] != "1" {
			continue
		}
		switch pod.Labels[query.LabelRunner] {
		case "leader":
			surgeLeaders++
			if got := pod.Spec.Containers[0].Name; got != "leader" {
				t.Errorf("surge leader must render from the revision's leader template; container=%q", got)
			}
		case "worker":
			surgeWorkers++
			if got := pod.Spec.Containers[0].Name; got != "worker" {
				t.Errorf("surge worker must render from the revision's WORKER template; container=%q", got)
			}
		}
	}
	if surgeLeaders != 1 || surgeWorkers != 2 {
		t.Fatalf("surge gang shape: got %d leaders + %d workers, want 1 + 2", surgeLeaders, surgeWorkers)
	}

	f.react(t)
	f.drive(t, uuid, 12)

	rec = f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("gang record must finish Completed with CompletedAt; got %+v", *rec)
	}
	// All 3 source pods gone; the 3 surge pods remain; source status
	// removed; surge promoted.
	var sourceLeft, surgeLeft int
	for _, pod := range f.listPods(t) {
		switch pod.Labels[query.LabelInstanceIdx] {
		case "0":
			sourceLeft++
		case "1":
			surgeLeft++
		}
	}
	if sourceLeft != 0 {
		t.Errorf("all 3 gang source pods must be deleted; %d remain", sourceLeft)
	}
	if surgeLeft != 3 {
		t.Errorf("surge gang must survive completion; got %d pods", surgeLeft)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("gang source InstanceStatus must be removed; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("gang surge must be promoted Ready; got %+v", surge)
	}
}

// TestMigrate_ResumeAfterCrash_PreStamp pins the record-first crash
// anchor: SurgeInstance + SurgePending were committed but the process
// died before the pair stamps landed. The resume pass must NOT re-run
// the fresh-request guards (the source shows no steady-Ready violation
// here, but the point is the branch), must re-ensure both stamps at
// the recorded index, and the migration must run to completion.
func TestMigrate_ResumeAfterCrash_PreStamp(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-crash-prestamp"
	rec := mkMigRecord(uuid, 0, "node-a")
	idx := int32(1)
	rec.SurgeInstance = &idx
	rec.Phase = workload.MigrationPhaseSurgePending
	f.records = []workload.MigrationRecord{rec}

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating ||
		src.Operation == nil || src.Operation.RequestUUID != uuid {
		t.Fatalf("resume must re-ensure the source stamp; got %+v", src)
	}
	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Phase != v1beta1.OMENativeInstanceCreating ||
		surge.Operation == nil || surge.Operation.SurgeIndex == nil || *surge.Operation.SurgeIndex != 0 {
		t.Fatalf("resume must re-ensure the surge stamp (sibling pointer at source); got %+v", surge)
	}

	f.react(t)
	f.drive(t, uuid, 10)
	if got := f.record(t, uuid); got.Phase != workload.MigrationPhaseCompleted {
		t.Fatalf("crash-resume must complete; got %+v", *got)
	}
}

// TestMigrate_ResumeAfterCrash_Draining pins resume from the Draining
// phase: source pods already deleted, surge serving and in rotation,
// process died before the promote/remove/Completed tail. The resume
// pass must finish the tail without touching the (gone) source pods
// and without regressing the record's phase.
func TestMigrate_ResumeAfterCrash_Draining(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-crash-draining"

	// Walk the real machinery to the Draining state first (passes 1-3),
	// then simulate the crash by rebuilding fixture inputs from
	// persisted state only (records slice survives — it models the
	// re-read status.migrations).
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	for i := 0; i < 6; i++ {
		if f.record(t, uuid).Phase == workload.MigrationPhaseDraining {
			break
		}
		done, _ := f.pass(t, uuid)
		if done {
			t.Fatalf("reached terminal before Draining checkpoint")
		}
		f.react(t)
	}
	if f.record(t, uuid).Phase != workload.MigrationPhaseDraining {
		t.Fatalf("fixture never reached Draining; record=%+v", *f.record(t, uuid))
	}

	// Crash + resume: drive to completion from the persisted state.
	f.drive(t, uuid, 10)
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
		t.Fatalf("resume-from-Draining must complete; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src != nil {
		t.Errorf("source InstanceStatus must be removed; got %+v", src)
	}
}

// TestMigrate_Rejections_RecordFailed drives every fresh-request
// rejection path and asserts the record lands Phase=Failed with the
// rejection reason and CompletedAt, plus the terminal ledger row.
func TestMigrate_Rejections_RecordFailed(t *testing.T) {
	t.Run("from-node-mismatch", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-node"
		f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-WRONG")}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "does not match observed source node")
	})

	t.Run("source-missing", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-missing"
		f.records = []workload.MigrationRecord{mkMigRecord(uuid, 7, "node-a")}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "source InstanceStatus missing")
	})

	t.Run("capacity-in-flight-cap", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-reject-capacity"
		// Three other ALLOCATED non-terminal records exhaust the in-flight
		// cap; the 4th request is rejected. Three concurrent executions on
		// one IR can't occur under the serial dispatcher — records are
		// constructed directly to pin the cap.
		busy := func(u string, phase workload.MigrationPhase, surge int32) workload.MigrationRecord {
			at := metav1.Now()
			return workload.MigrationRecord{
				RequestUUID: u, Trigger: workload.MigrationTriggerManual, Phase: phase,
				SurgeInstance: &surge, AllocatedAt: &at, StartedAt: metav1.Now(),
			}
		}
		f.records = []workload.MigrationRecord{
			busy("busy-1", workload.MigrationPhaseSurgePending, 5),
			busy("busy-2", workload.MigrationPhaseDraining, 6),
			busy("busy-3", workload.MigrationPhaseSurgeReady, 7),
			mkMigRecord(uuid, 0, "node-a"),
		}
		done, accepted := f.pass(t, uuid)
		if !done || !accepted {
			t.Fatalf("rejection must be terminal: done=%v accepted=%v", done, accepted)
		}
		assertRecordFailed(t, f, uuid, "in-flight migration cap reached")
		// The busy records are untouched.
		for _, u := range []string{"busy-1", "busy-2", "busy-3"} {
			if r := f.record(t, u); r.Phase.Terminal() {
				t.Errorf("capacity rejection must not touch other records; %s became %s", u, r.Phase)
			}
		}
	})
}

// TestMigrate_QueuedBatch_NotCapacityPoisoned_SerialCompletion pins the
// queued-batch guard: five requests accepted in one burst sit queued
// (Accepted, no surge allocated). Capacity counts EXECUTION — allocated
// surges and AllocatedAt in the window — never queued intent, so the
// oldest record's fresh-path gate admits (counting the 4 queued siblings
// against the in-flight cap would terminally Fail the whole batch), and
// serial dispatch completes all five.
func TestMigrate_QueuedBatch_NotCapacityPoisoned_SerialCompletion(t *testing.T) {
	const n = 5
	f := newMultiInstanceMigFixture(t, n)
	uuids := make([]string, 0, n)
	for i := int32(0); i < n; i++ {
		rec := mkMigRecord(fmt.Sprintf("mig-batch-%d", i), i, "node-a")
		// Deterministic dispatch order: oldest first.
		rec.StartedAt = metav1.NewTime(time.Now().Add(time.Duration(i-n) * time.Minute))
		f.records = append(f.records, rec)
		uuids = append(uuids, rec.RequestUUID)
	}

	// The oldest queued record must not be capacity-rejected by its four
	// queued siblings.
	if picked := workload.NextManualMigration(f.records); picked == nil || picked.RequestUUID != uuids[0] {
		t.Fatalf("dispatcher must pick the oldest record first; picked %+v", picked)
	}
	done, accepted := f.pass(t, uuids[0])
	if done || !accepted {
		t.Fatalf("oldest queued record must start executing, not terminally fail: done=%v accepted=%v record=%+v",
			done, accepted, *f.record(t, uuids[0]))
	}

	// Serial completion of the whole batch, dispatcher order.
	f.react(t)
	for _, uuid := range uuids {
		f.drive(t, uuid, 12)
		rec := f.record(t, uuid)
		if rec.Phase != workload.MigrationPhaseCompleted || rec.CompletedAt == nil {
			t.Fatalf("batch record %s must complete; got %+v", uuid, *rec)
		}
	}
}

// TestMigrate_GangWorkerPinnedToFromNode_RejectedPreStamp pins the
// fresh-path overlay pre-check on BOTH gang templates: a WORKER spec
// whose hard NodeAffinity pins hostname=FromNode makes the surge
// unschedulable, so the request must fail terminally BEFORE the pair is
// stamped — no surge InstanceStatus, source untouched. (A pre-check that
// resolved only the leader spec would pass the gang, stamp the pair, then
// terminally fail on resume, orphaning the stamped pair until the
// deadline.)
func TestMigrate_GangWorkerPinnedToFromNode_RejectedPreStamp(t *testing.T) {
	f := newGangMigFixtureWithWorkerSpec(t, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"node-b"},
					}},
				}},
			},
		}},
	})
	const uuid = "mig-gang-worker-pinned"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-b")}

	done, accepted := f.pass(t, uuid)
	if !done || !accepted {
		t.Fatalf("worker-pinned gang must be rejected terminally on the fresh pass: done=%v accepted=%v record=%+v",
			done, accepted, *f.record(t, uuid))
	}
	assertRecordFailed(t, f, uuid, "NodeAffinity requires")
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s != nil {
		t.Errorf("rejection must land before the surge stamp; found surge status %+v", s)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Errorf("source must be untouched (Ready, no op); got %+v", src)
	}
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "1" {
			t.Errorf("no surge pod may exist after a pre-stamp rejection; found %s", pod.Name)
		}
	}
}

// surgePodHasHintTerm reports whether pod carries a preferred
// node-affinity term listing exactly the hint nodes.
func surgePodHasHintTerm(pod *corev1.Pod, hints []string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		for _, expr := range term.Preference.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn &&
				fmt.Sprintf("%v", expr.Values) == fmt.Sprintf("%v", hints) {
				return true
			}
		}
	}
	return false
}

// TestMigrate_DeferWithoutOwnership_LeavesRecordAccepted pins the
// mid-op defer contract: a source that is not steady-Ready (in-flight
// Update) defers (done=false, accepted=false) and the record stays
// Accepted for the next pass.
func TestMigrate_DeferWithoutOwnership_LeavesRecordAccepted(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-defer"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	ir := f.getIR(t)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Updating source: %v", err)
	}

	done, accepted := f.pass(t, uuid)
	if done || accepted {
		t.Fatalf("mid-Update source must defer without ownership: done=%v accepted=%v", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseAccepted || rec.SurgeInstance != nil {
		t.Errorf("deferred record must stay Accepted with no surge; got %+v", *rec)
	}
}

// countEventsWithReason counts drained events carrying reason.
func countEventsWithReason(events []string, reason workload.EventReason) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, string(reason)) {
			n++
		}
	}
	return n
}

// TestMigrate_UnconfiguredCapacityDefersAndResumes pins the unset path:
// with no operator capacity policy the pass has no bound to judge the
// request against, so it holds the request without taking ownership —
// the record stays Accepted, one Warning names the missing key, and the
// same record proceeds once the caps exist.
func TestMigrate_UnconfiguredCapacityDefersAndResumes(t *testing.T) {
	f := newSinglePodMigFixture(t)
	f.migrationAudit = nil
	f.recorder = record.NewFakeRecorder(16)
	var conditions []metav1.Condition
	f.conditions = &conditions
	const uuid = "mig-unconfigured"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	done, accepted := f.pass(t, uuid)
	if done || accepted {
		t.Fatalf("an unconfigured policy must defer without ownership: done=%v accepted=%v", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseAccepted || rec.SurgeInstance != nil || rec.CompletedAt != nil {
		t.Fatalf("held record must stay queued Accepted; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, "lifecycle.audit") {
		t.Errorf("held record Message = %q, want the missing configuration named", rec.Message)
	}
	events := migWedgeEvents(t, f)
	if got := countEventsWithReason(events, workload.EventReasonMigrationPolicyUnconfigured); got != 1 {
		t.Fatalf("held request must warn once; got %d in %v", got, events)
	}
	if got := countEventsWithReason(events, workload.EventReasonRateLimited); got != 0 {
		t.Errorf("a missing policy is not a cap breach; got %d RateLimited events", got)
	}
	if cond := lastConditionOfType(conditions, workload.ConditionMigrationPolicyUnconfigured); cond == nil ||
		cond.Status != metav1.ConditionTrue {
		t.Fatalf("the hold must raise the MigrationPolicyUnconfigured condition; got %+v", conditions)
	}
	// The hold owes the pass a wake-up: nothing in the cluster announces
	// the operator writing the missing key.
	if f.wake.Pending() == 0 && !f.wake.Bare() {
		t.Fatalf("a held record must leave the pass a wake-up")
	}

	// A second pass under the same unconfigured policy re-defers without
	// a second Warning: the hold is edge-triggered off the record.
	if done, accepted = f.pass(t, uuid); done || accepted {
		t.Fatalf("second pass must keep deferring: done=%v accepted=%v", done, accepted)
	}
	if got := countEventsWithReason(migWedgeEvents(t, f), workload.EventReasonMigrationPolicyUnconfigured); got != 0 {
		t.Errorf("the hold must warn once per hold, not once per pass; got %d", got)
	}

	// Configuring the caps admits the very same record.
	f.migrationAudit = fixtureMigrationAudit()
	if _, accepted = f.pass(t, uuid); !accepted {
		t.Fatalf("a configured policy must admit the held record")
	}
	rec = f.record(t, uuid)
	if rec.Phase == workload.MigrationPhaseFailed || rec.SurgeInstance == nil {
		t.Fatalf("resumed record must allocate a surge, not fail; got %+v", *rec)
	}
	if cond := lastConditionOfType(conditions, workload.ConditionMigrationPolicyUnconfigured); cond == nil ||
		cond.Status != metav1.ConditionFalse {
		t.Fatalf("a pass judging under configured caps must clear the condition; got %+v", conditions)
	}
}

// lastConditionOfType returns the most recently written condition of
// condType, or nil when the pass never wrote one.
func lastConditionOfType(conditions []metav1.Condition, condType workload.ConditionType) *metav1.Condition {
	for i := len(conditions) - 1; i >= 0; i-- {
		if conditions[i].Type == string(condType) {
			return &conditions[i]
		}
	}
	return nil
}

// assertRecordFailed asserts the record is terminal-Failed carrying the
// rejection reason + CompletedAt and the ledger holds a terminal row.
func assertRecordFailed(t *testing.T, f *migFixture, uuid, wantMsg string) {
	t.Helper()
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase = %s, want Failed", rec.Phase)
	}
	if rec.CompletedAt == nil {
		t.Errorf("Failed record must carry CompletedAt")
	}
	if !strings.Contains(rec.Message, wantMsg) {
		t.Errorf("record Message = %q, want substring %q", rec.Message, wantMsg)
	}
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("terminal Failed ledger row must exist for %s", uuid)
	}
}

// findInstanceStatusOnIRForFixture reads the persisted InstanceStatus
// for idx off the fixture's IR, or nil.
func findInstanceStatusOnIRForFixture(t *testing.T, f *migFixture, idx int32) *v1beta1.OMENativeInstanceStatus {
	t.Helper()
	ir := f.getIR(t)
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	return nil
}

// TestStampMigrationStatus_SourceSlotMissing_NotResurrected pins the
// fresh-empty-slot guard on the SOURCE side of the pair stamp: writing
// the Migrating stamp for an index whose InstanceStatus is gone must
// not append a phantom slot (the append path seeds Phase==""). The
// surge side legitimately seeds its slot and must keep doing so.
func TestStampMigrationStatus_SourceSlotMissing_NotResurrected(t *testing.T) {
	f := newSinglePodMigFixture(t)
	in := f.input(t)

	if err := patchInstanceStatusMigrating(context.Background(), in, 7, 8, "uuid-guard", time.Minute); err != nil {
		t.Fatalf("patchInstanceStatusMigrating: %v", err)
	}
	if s := findInstanceStatusOnIRForFixture(t, f, 7); s != nil {
		t.Fatalf("source stamp resurrected a removed slot: %+v", s)
	}

	if err := patchInstanceStatusMigrationSurge(context.Background(), in, 8, 7, "uuid-guard", time.Minute); err != nil {
		t.Fatalf("patchInstanceStatusMigrationSurge: %v", err)
	}
	s := findInstanceStatusOnIRForFixture(t, f, 8)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Fatalf("surge stamp must still seed its slot; got %+v", s)
	}
}

// TestMigrate_GangResume_SurgeSlotUnstamped_RequeuesBeforePods pins the
// PodGroup-before-pods ordering on RESUME: crash window after the
// record's SurgeInstance write but before the pair stamps. The resume
// pass re-ensures the stamps and must requeue — EnsurePodGroups ran
// this pass without the surge index in the plan, so creating the surge
// gang's pods in the same pass would beat their PodGroup and park the
// gang at the coscheduler.
func TestMigrate_GangResume_SurgeSlotUnstamped_RequeuesBeforePods(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-resume"
	rec := mkMigRecord(uuid, 0, "node-a")
	si := int32(1)
	rec.SurgeInstance = &si
	rec.Phase = workload.MigrationPhaseSurgePending
	f.records = []workload.MigrationRecord{rec}

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	if s := findInstanceStatusOnIRForFixture(t, f, 1); s == nil {
		t.Fatalf("resume pass must re-ensure the surge stamp")
	}
	surgePods, err := query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(surgePods) != 0 {
		t.Fatalf("surge pods created in the same pass as the re-ensured stamp: got %d, want 0 (requeue first)", len(surgePods))
	}

	// The next pass observes the stamped slot and creates the gang.
	done, accepted = f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("post-requeue pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}
	surgePods, err = query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("re-list surge pods: %v", err)
	}
	if len(surgePods) == 0 {
		t.Fatalf("surge gang must be created on the pass after the requeue")
	}
}

// TestMigrate_SourceStuckTerminating_EscalationRuns pins the exit path
// for the migration's primary use case: the drained source pod wedges
// Terminating (here finalizer-pinned) and the drive tail must run the
// stuck-teardown escalation instead of silently requeuing forever.
// Finalizer-pinned evidence is report-only, so the observable is the
// once-per-UID ForceDelete ledger marker.
func TestMigrate_SourceStuckTerminating_EscalationRuns(t *testing.T) {
	f := newSinglePodMigFixture(t)
	clk := f.withFakeClock()
	f.forceDelete = fdPolicy()
	const uuid = "mig-stuck-term"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute)),
	}
	driveToDrainingDrainIncomplete(t, f, uuid)
	f.react(t) // drain settles — the tail is one delete away

	srcPodName := query.PodName(f.isvc.Name, f.component, 0, "default", 0)
	srcPod := &corev1.Pod{}
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: f.isvc.Namespace, Name: srcPodName}, srcPod); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	srcPod.Finalizers = append(srcPod.Finalizers, "test.ome.io/block")
	if err := f.c.Update(context.Background(), srcPod); err != nil {
		t.Fatalf("pin source pod: %v", err)
	}
	if err := f.c.Delete(context.Background(), srcPod); err != nil {
		t.Fatalf("delete source pod: %v", err)
	}
	// Past the pod's own deletion deadline plus the policy's slack.
	clk.Step(10 * time.Minute)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("stuck pass: got done=%v accepted=%v, want in-flight", done, accepted)
	}

	found := false
	for _, e := range fdLedgerEntries(t, f.c, f.isvc) {
		if e.Reason == audit.ReasonForceDelete && e.Outcome == audit.OutcomeForceDeleteFinalizerReport {
			found = true
		}
	}
	if !found {
		t.Fatalf("Migrate's delete tail must run the stuck-Terminating escalation (no ForceDelete report ledger row)")
	}
}

// A migration is driven by its record, and the record's Deadline is the
// timeout authority for the pair. These tests pin what that ownership
// means at the seams: which apiserver rejections end the request early
// instead of idling to the Deadline, which readings and edits do not
// reach the pair at all, and what a close on the surge's own evidence
// leaves for a re-delivered edge to find.

// driveToStampedPair runs passes until both rows of the pair exist: the
// source pinned Migrating and the replacement marker at the surge index.
// Every rejection test needs that, because a rejection observed before
// the stamp would only reach the source's state.
func driveToStampedPair(t *testing.T, f *migFixture, uuid string, surgeIdx int32) {
	t.Helper()
	for i := 0; i < 4; i++ {
		if findInstanceStatusOnIRForFixture(t, f, surgeIdx) != nil {
			return
		}
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want the pair still being stamped", i+1, done, accepted)
		}
	}
	if findInstanceStatusOnIRForFixture(t, f, surgeIdx) == nil {
		t.Fatalf("the pair was never stamped; record=%+v", *f.record(t, uuid))
	}
}

// migPassWithRejectedCreates runs one Migrate pass whose pod creates the
// apiserver answers with reject, and reports the RetryBlock writes it
// asked for.
func migPassWithRejectedCreates(t *testing.T, f *migFixture, uuid string, reject error) (done bool, blocks []string) {
	t.Helper()
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      rec.SourceInstance,
		FromNode:      rec.FromNode,
		Reason:        rec.Reason,
	}
	in := f.input(t)
	in.Pacing = &workload.APIPacing{}
	recordBlocks := func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			blocks = append(blocks, rev)
		}
		return nil
	}
	in.MutateRetryBlock = recordBlocks
	deps := f.deps()
	deps.Client = rejectPodCreates(t, f.c, reject)

	// The surge create is not always the pass right after the stamp, so
	// drive until the rejection has been answered one way or the other.
	for i := 0; i < 5; i++ {
		var accepted bool
		var err error
		done, accepted, err = Migrate(context.Background(), deps, in, f.plan, rec.SourceInstance, uuid, req)
		if err != nil {
			t.Fatalf("Migrate over a rejected create: %v", err)
		}
		if !accepted {
			t.Fatalf("Migrate: accepted=false want the record handled")
		}
		if done {
			return done, blocks
		}
		legacyResetExpectations(t)
		in = f.input(t)
		in.Pacing = &workload.APIPacing{}
		in.MutateRetryBlock = recordBlocks
	}
	return done, blocks
}

// TestMigrateSurge_RejectionDispositions: the replacement's create is the
// one write a migration cannot retry its way past. A 422 and a
// terminating namespace are both permanent, so the request is closed on
// the first rejection instead of idling to its record Deadline — and
// neither blames the revision, because the replacement carries the
// request's placement overlay and the rejection may indict that instead.
// A 429 is the apiserver pacing us: nothing is written and the create is
// still polled to the Deadline like any other blocked create.
func TestMigrateSurge_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reject   error
		wantDone bool
	}{
		{name: "invalid pod spec", reject: siteInvalidError(), wantDone: true},
		{name: "namespace terminating", reject: siteNamespaceTerminatingError("engine-pod"), wantDone: true},
		{name: "throttled", reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			uuid := "mig-reject-" + t.Name()
			f.records = []workload.MigrationRecord{
				mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
			}
			done, blocks := migPassWithRejectedCreates(t, f, uuid, tc.reject)
			rec := f.record(t, uuid)

			// The pair stamp precedes the create, so the rejection is
			// observed by a row that already exists on both sides.
			if rec.SurgeInstance == nil {
				t.Fatalf("the replacement was never allocated; record=%+v", *rec)
			}
			if findInstanceStatusOnIRForFixture(t, f, *rec.SurgeInstance) == nil {
				t.Fatalf("the replacement row is missing; record=%+v", *rec)
			}

			if done != tc.wantDone {
				t.Fatalf("done: got %v want %v (record=%+v)", done, tc.wantDone, *rec)
			}
			if tc.wantDone {
				if rec.Phase != workload.MigrationPhaseFailed {
					t.Errorf("record phase: got %s want Failed", rec.Phase)
				}
				if rec.CompletedAt == nil {
					t.Errorf("record CompletedAt: got nil want the close timestamp")
				}
			} else {
				if rec.Phase.Terminal() {
					t.Errorf("record phase: got %s want the move still in flight; a 429 is pacing", rec.Phase)
				}
			}
			// Neither disposition charges the revision: an overlay-bearing
			// replacement says nothing about the pod template.
			if len(blocks) != 0 {
				t.Errorf("RetryBlock writes: got %v want none", blocks)
			}
		})
	}
}

// TestMigrate_DeadlineExceededIsNotEarlyFailureEvidence: the terminal
// waiting set every reader of this evidence shares does not contain
// DeadlineExceeded, so a surge parked in it is not closed early however
// long it sits. The pair keeps driving and the record Deadline stays the
// only bound on the move.
func TestMigrate_DeadlineExceededIsNotEarlyFailureEvidence(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-deadline-exceeded"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "DeadlineExceeded")
	clk.Step(wedgeGrace + time.Hour)
	migWedgeEvents(t, f)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("DeadlineExceeded closed the record early: %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Errorf("source: got %+v want the Migrate pin held", src)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("replacement: got %+v want it still coming up", surge)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("events: got %v want none; this reading is not evidence", events)
	}
}

// TestMigrate_RevisionRetargetIsDeclinedWhileTheRecordDrives: the update
// trigger declines outright for a migrate-owned status, on both rows —
// the source is mid-move and the replacement is a marker driven by its
// pair, never an independent update target. A migration surge and an
// update surge therefore never stack on one Instance; the new revision
// rolls once the record is terminal.
func TestMigrate_RevisionRetargetIsDeclinedWhileTheRecordDrives(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-revision-retarget"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, uuid, 1)

	// The operator publishes a new revision mid-move.
	nextSpec := legacyTargetSpecImage("llama:v2")
	nextCR := legacyEnsureTargetCR(t, f.c, f.isvc, nextSpec)
	in := f.input(t)
	in.ObservedState.UpdateRevision = nextCR.Name
	in.DesiredSpec.PodSpec = nextSpec

	for _, idx := range []int32{0, 1} {
		row := findInstanceStatusOnIRForFixture(t, f, idx)
		if row == nil || row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationMigrate {
			t.Fatalf("instance %d: got %+v want a Migrate-owned row", idx, row)
		}
		inst := workload.InstancePlan{
			Index:       idx,
			Incarnation: row.Incarnation,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}
		trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), f.deps(), in, f.plan, inst,
			nextCR, migPodsForInstance(t, f, idx))
		if err != nil {
			t.Fatalf("DetectUpdateTriggerWithPods (instance=%d): %v", idx, err)
		}
		if trigger {
			t.Errorf("instance %d: got trigger=true want the update declined while the record drives", idx)
		}
		if retryAfter != 0 {
			t.Errorf("instance %d: got retryAfter=%v want 0 (the record's own cadence drives the move)", idx, retryAfter)
		}
	}

	// The pair is untouched by the decline: no target revision is stamped
	// onto either row.
	for _, idx := range []int32{0, 1} {
		row := findInstanceStatusOnIRForFixture(t, f, idx)
		if row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationMigrate {
			t.Errorf("instance %d Operation: got %+v want the Migrate pin intact", idx, row.Operation)
		}
		if row.Operation != nil && row.Operation.TargetRevision == nextCR.Name {
			t.Errorf("instance %d: the new revision retargeted a pinned row", idx)
		}
	}
}

// TestMigrate_SecondRequestQueuesBehindTheInFlightPair: dispatch is
// serial and oldest-first, so a second request that arrives while a pair
// is in flight sits in status.migrations as a queued Accepted record. It
// allocates no surge and touches neither row, and a re-delivery of the
// uuid already in flight is the same record rather than a second one.
func TestMigrate_SecondRequestQueuesBehindTheInFlightPair(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const inFlight, queued = "mig-in-flight", "mig-queued"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(inFlight, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, inFlight, 1)
	before := *findInstanceStatusOnIRForFixture(t, f, 0)

	// The operator asks for the same Instance again, and the same uuid is
	// re-delivered alongside it.
	second := mkMigRecordWithDeadline(queued, 0, "node-a", time.Now().Add(4*time.Hour))
	second.StartedAt = metav1.NewTime(time.Now())
	f.records = append(f.records, second)

	if picked := workload.NextManualMigration(f.records); picked == nil || picked.RequestUUID != inFlight {
		t.Fatalf("dispatcher picked %+v, want the record already in flight", picked)
	}
	if done, accepted := f.pass(t, inFlight); done || !accepted {
		t.Fatalf("re-delivering the in-flight uuid: got done=%v accepted=%v, want the same move continuing", done, accepted)
	}

	if got := f.record(t, queued); got.Phase != workload.MigrationPhaseAccepted || got.SurgeInstance != nil {
		t.Errorf("queued record: got %+v want it still Accepted with no surge allocated", *got)
	}
	after := findInstanceStatusOnIRForFixture(t, f, 0)
	if after.Operation == nil || after.Operation.RequestUUID != before.Operation.RequestUUID {
		t.Errorf("source Operation: got %+v want the in-flight request still pinned", after.Operation)
	}
	var uuids []string
	for _, r := range f.records {
		uuids = append(uuids, r.RequestUUID)
	}
	if len(uuids) != 2 {
		t.Errorf("records: got %v want exactly the two requests (a re-delivery is deduped)", uuids)
	}
}

// TestMigrate_StatusConflictCostsAPassAndReDerives: the pair's status
// writes are read-modify-write, so a concurrent writer rejects one and
// the pass retries from a fresh read. The record survives that — it is
// not closed, the pin is not dropped — and the next pass re-evaluates the
// pair preconditions and gets where the rejected one was going.
func TestMigrate_StatusConflictCostsAPassAndReDerives(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-status-conflict"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", time.Now().Add(4*time.Hour)),
	}
	driveToStampedPair(t, f, uuid, 1)
	before := *findInstanceStatusOnIRForFixture(t, f, 0)

	// One pass whose every row write is rejected as stale.
	legacyResetExpectations(t)
	rec := f.record(t, uuid)
	in := f.input(t)
	in.MutateInstance = func(_ context.Context, idx int32, _ func(*workload.InstanceStatus) bool) error {
		return apierrors.NewConflict(v1beta1.Resource("inferencereplicas"), f.irKey().Name,
			errors.New("the object has been modified; please apply your changes to the latest version"))
	}
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      rec.SourceInstance,
		FromNode:      rec.FromNode,
		Reason:        rec.Reason,
	}
	_, _, _ = Migrate(context.Background(), f.deps(), in, f.plan, rec.SourceInstance, uuid, req)

	if got := f.record(t, uuid); got.Phase.Terminal() {
		t.Fatalf("a conflict closed the record: %+v", *got)
	}
	after := findInstanceStatusOnIRForFixture(t, f, 0)
	if after == nil || after.Operation == nil || after.Operation.RequestUUID != before.Operation.RequestUUID {
		t.Fatalf("source: got %+v want the pin intact across the rejected write", after)
	}

	// The next pass, against a client that accepts writes again, re-derives
	// the same work and the move keeps going.
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("recovery pass: got done=%v accepted=%v, want the move still in flight", done, accepted)
	}
	if got := f.record(t, uuid); got.Phase.Terminal() {
		t.Errorf("record: got %+v want the move still driving after the retry", *got)
	}
}

// wedgedSurgePastGrace drives a migration to a surge pod parked in a
// terminal waiting reason past the stuck-pod grace — the evidence the
// record consumes as early failure — and returns the fixture clock.
func wedgedSurgePastGrace(t *testing.T, f *migFixture, uuid string) *clocktesting.FakeClock {
	t.Helper()
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	surgePods := driveToSurgePods(t, f, uuid, 1)
	if findInstanceStatusOnIRForFixture(t, f, 1) == nil {
		t.Fatalf("setup: the replacement row must exist before the close")
	}
	wedgePod(t, f, surgePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)
	return clk
}

// TestMigrate_SourcePodLossAroundTheWedgeClose: the close on the surge's
// own evidence restores the source through the record, and that write is
// preconditioned on both rows carrying the expected identity. A source
// pod that disappears as the write lands therefore rejects it and leaves
// it to be re-derived — the pair is never half-restored. Past the close
// the source is unpinned and the loss belongs to the ordinary repair, so
// the migration reads nothing from it either.
func TestMigrate_SourcePodLossAroundTheWedgeClose(t *testing.T) {
	deleteSourcePods := func(t *testing.T, f *migFixture) {
		t.Helper()
		for _, pod := range migPodsForInstance(t, f, 0) {
			if err := f.c.Delete(context.Background(), pod); err != nil {
				t.Fatalf("delete the source pod: %v", err)
			}
		}
	}

	t.Run("lost as the restore lands, nothing is half-applied", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-source-lost-boundary"
		wedgedSurgePastGrace(t, f, uuid)
		deleteSourcePods(t, f)

		if done, accepted := f.pass(t, uuid); done || !accepted {
			t.Fatalf("boundary pass: got done=%v accepted=%v, want the write re-derived rather than applied", done, accepted)
		}
		if rec := f.record(t, uuid); rec.Phase.Terminal() {
			t.Fatalf("record: got %+v want it not closed over a pair it could not restore", *rec)
		}
		src := findInstanceStatusOnIRForFixture(t, f, 0)
		if src == nil || src.Operation == nil || src.Phase == v1beta1.OMENativeInstanceReady {
			t.Errorf("source: got %+v want the pin still held (a half-applied restore is the failure mode)", src)
		}
		if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation == nil {
			t.Errorf("replacement: got %+v want it still pinned alongside the source", surge)
		}
		assertNoRetryBlocks(t, f)
	})

	t.Run("lost after the close, the repair owns it", func(t *testing.T) {
		f := newSinglePodMigFixture(t)
		const uuid = "mig-source-lost-after"
		wedgedSurgePastGrace(t, f, uuid)

		if done, accepted := f.pass(t, uuid); !done || !accepted {
			t.Fatalf("close pass: got done=%v accepted=%v, want the record closed", done, accepted)
		}
		closed := *f.record(t, uuid)
		assertSourceRestored(t, f, 0, uuid, findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision)
		restored := *findInstanceStatusOnIRForFixture(t, f, 0)
		migWedgeEvents(t, f)

		deleteSourcePods(t, f)
		if done, accepted := f.pass(t, uuid); !done || !accepted {
			t.Fatalf("after pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
		}
		if got := f.record(t, uuid); got.Message != closed.Message || !got.CompletedAt.Equal(closed.CompletedAt) {
			t.Errorf("the re-delivered loss rewrote the terminal record: %+v -> %+v", closed, *got)
		}
		if after := findInstanceStatusOnIRForFixture(t, f, 0); !equalitySafeOperation(&restored, after) {
			t.Errorf("source: got %+v want it left Ready and unpinned (%+v)", after, restored)
		}
		if events := migWedgeEvents(t, f); len(events) != 0 {
			t.Errorf("events: got %v want none; the migration reads nothing off an unpinned row", events)
		}
	})
}

// TestMigrate_MigrationDeadlineAroundTheWedgeClose: the record Deadline
// is the backstop the surge's own evidence preempts. Elapsing as the
// close lands changes nothing — the record is closed Failed either way
// and carries one terminal write — and elapsing afterwards reopens
// nothing, because expiry declines a record that is already terminal.
func TestMigrate_MigrationDeadlineAroundTheWedgeClose(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-deadline-edges"
	clk := wedgedSurgePastGrace(t, f, uuid)

	// The boundary: the Deadline is already past on the observation the
	// terminal write is computed from.
	f.record(t, uuid).Deadline = metav1.NewTime(clk.Now().Add(-time.Second))
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("boundary pass: got done=%v accepted=%v, want the record closed", done, accepted)
	}
	closed := *f.record(t, uuid)
	if closed.Phase != workload.MigrationPhaseFailed || closed.CompletedAt == nil {
		t.Fatalf("record: got %+v want Failed with a close timestamp", closed)
	}
	source := findInstanceStatusOnIRForFixture(t, f, 0)
	if source == nil || source.Operation != nil {
		t.Fatalf("source: got %+v want the Migrate pin cleared", source)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge != nil && surge.Operation != nil &&
		surge.Operation.Type == v1beta1.InstanceOperationMigrate {
		t.Errorf("replacement: got %+v want the pin cleared so scale-down owns the index", surge.Operation)
	}
	assertNoRetryBlocks(t, f)
	restored := *source
	migWedgeEvents(t, f)

	// The after window: the same elapsed Deadline against a terminal
	// record.
	clk.Step(time.Hour)
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("after pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
	}
	got := f.record(t, uuid)
	if got.Message != closed.Message || !got.CompletedAt.Equal(closed.CompletedAt) {
		t.Errorf("expiry reopened a terminal record: %+v -> %+v", closed, *got)
	}
	if after := findInstanceStatusOnIRForFixture(t, f, 0); !equalitySafeOperation(&restored, after) {
		t.Errorf("source: got %+v want it left where the close put it (%+v)", after, restored)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("events: got %v want none; a terminal record announces nothing", events)
	}
}

func TestMigrate_DrainRejectsAuthoritativePairDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceReplica, int32, int32)
	}{
		{
			name: "source operation replaced",
			mutate: func(ir *v1beta1.InferenceReplica, sourceIdx, _ int32) {
				status := migrationStatusByIndex(t, ir, sourceIdx)
				status.Phase = v1beta1.OMENativeInstanceUpdating
				status.Operation.Type = v1beta1.InstanceOperationUpdate
				status.Operation.RequestUUID = "replacement-source"
			},
		},
		{
			name: "surge slot repurposed",
			mutate: func(ir *v1beta1.InferenceReplica, _, surgeIdx int32) {
				status := migrationStatusByIndex(t, ir, surgeIdx)
				status.Phase = v1beta1.OMENativeInstanceReady
				status.RunningRevision = "unrelated-revision"
				status.TargetRevision = ""
				status.Operation = nil
			},
		},
		{
			name: "surge slot removed",
			mutate: func(ir *v1beta1.InferenceReplica, _, surgeIdx int32) {
				removeMigrationStatusByIndex(ir, surgeIdx)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, uuid, surgeIdx := migrationGangReadyToDrain(t)
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}
			input := f.input(t)
			mutateMigrationIRStatus(t, f, func(ir *v1beta1.InferenceReplica) {
				test.mutate(ir, 0, surgeIdx)
			})
			wantEffects := snapshotMigrationEffects(t, f)

			done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, 0, uuid, migrationRequest(t, f, uuid))
			if err != nil || done || !accepted {
				t.Fatalf("guarded drain: done=%v accepted=%v err=%v", done, accepted, err)
			}
			assertMigrationEffectsUnchanged(t, f, wantEffects)
			if finalizations != 0 {
				t.Fatalf("pair drift triggered %d resource finalizations", finalizations)
			}
		})
	}
}

func TestMigrate_InitialPairStampRejectsAuthoritativeDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceReplica, int32, int32)
	}{
		{
			name: "source operation replaced",
			mutate: func(ir *v1beta1.InferenceReplica, sourceIdx, _ int32) {
				status := migrationStatusByIndex(t, ir, sourceIdx)
				status.Phase = v1beta1.OMENativeInstanceUpdating
				status.Operation = &v1beta1.InstanceOperation{
					Type:        v1beta1.InstanceOperationUpdate,
					RequestUUID: "replacement-source",
				}
			},
		},
		{
			name: "surge slot occupied",
			mutate: func(ir *v1beta1.InferenceReplica, _, surgeIdx int32) {
				ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
					Index:           surgeIdx,
					Incarnation:     1,
					Phase:           v1beta1.OMENativeInstanceReady,
					RunningRevision: "unrelated-revision",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}
			const uuid = "migration-initial-pair-fence"
			f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
			input := f.input(t)
			baseMutateMigration := input.MutateMigration
			var wantEffects string
			input.MutateMigration = func(ctx context.Context, requestUUID string, mutate func(*workload.MigrationRecord) bool) error {
				if err := baseMutateMigration(ctx, requestUUID, mutate); err != nil {
					return err
				}
				record := f.record(t, uuid)
				if record.SurgeInstance == nil {
					t.Fatal("allocation did not persist a surge index")
				}
				mutateMigrationIRStatus(t, f, func(ir *v1beta1.InferenceReplica) {
					test.mutate(ir, 0, *record.SurgeInstance)
				})
				wantEffects = snapshotMigrationEffects(t, f)
				return nil
			}

			done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, 0, uuid, migrationRequest(t, f, uuid))
			if err != nil || done || !accepted {
				t.Fatalf("guarded initial stamp: done=%v accepted=%v err=%v", done, accepted, err)
			}
			if wantEffects == "" {
				t.Fatal("test did not capture post-allocation effects")
			}
			assertMigrationEffectsUnchanged(t, f, wantEffects)
			if finalizations != 0 {
				t.Fatalf("initial pair drift triggered %d resource finalizations", finalizations)
			}
		})
	}
}

func TestMigrate_PromotionRejectsPairDriftAfterDrainClaim(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceReplica, int32, int32)
	}{
		{
			name: "source operation replaced",
			mutate: func(ir *v1beta1.InferenceReplica, sourceIdx, _ int32) {
				status := migrationStatusByIndex(t, ir, sourceIdx)
				status.Operation.Type = v1beta1.InstanceOperationUpdate
				status.Operation.RequestUUID = "replacement-source"
			},
		},
		{
			name: "surge slot repurposed",
			mutate: func(ir *v1beta1.InferenceReplica, _, surgeIdx int32) {
				status := migrationStatusByIndex(t, ir, surgeIdx)
				status.Phase = v1beta1.OMENativeInstanceReady
				status.RunningRevision = "unrelated-revision"
				status.TargetRevision = ""
				status.Operation = nil
			},
		},
		{
			name: "surge slot removed",
			mutate: func(ir *v1beta1.InferenceReplica, _, surgeIdx int32) {
				removeMigrationStatusByIndex(ir, surgeIdx)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, uuid, surgeIdx := migrationGangReadyToPromote(t)
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}
			input := f.input(t)
			baseApply := input.ApplyInstanceMutationsWithRetryBlock
			applyCalls := 0
			var wantEffects string
			input.ApplyInstanceMutationsWithRetryBlock = func(
				ctx context.Context,
				mutations []workload.InstanceMutation,
				targetRevision string,
				mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition,
			) error {
				applyCalls++
				err := baseApply(ctx, mutations, targetRevision, mutateRetryBlock)
				if err == nil && applyCalls == 1 {
					mutateMigrationIRStatus(t, f, func(ir *v1beta1.InferenceReplica) {
						test.mutate(ir, 0, surgeIdx)
					})
					wantEffects = snapshotMigrationEffects(t, f)
				}
				return err
			}

			done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, 0, uuid, migrationRequest(t, f, uuid))
			if err != nil || done || !accepted {
				t.Fatalf("guarded promotion: done=%v accepted=%v err=%v", done, accepted, err)
			}
			if applyCalls < 2 {
				t.Fatalf("promotion did not recheck the pair after drain claim: apply calls=%d", applyCalls)
			}
			if wantEffects == "" {
				t.Fatal("test did not inject pair drift")
			}
			assertMigrationEffectsUnchanged(t, f, wantEffects)
			if finalizations != 0 {
				t.Fatalf("promotion pair drift triggered %d resource finalizations", finalizations)
			}
		})
	}
}

func TestMigrate_CompletionRecoveryRejectsAuthoritativeTargetDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceReplica, int32)
	}{
		{
			name: "promoted target removed",
			mutate: func(ir *v1beta1.InferenceReplica, surgeIdx int32) {
				removeMigrationStatusByIndex(ir, surgeIdx)
			},
		},
		{
			name: "promoted target repurposed",
			mutate: func(ir *v1beta1.InferenceReplica, surgeIdx int32) {
				status := migrationStatusByIndex(t, ir, surgeIdx)
				status.RunningRevision = "unrelated-revision"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			clk := f.withFakeClock()
			const uuid = "migration-completion-pair-fence"
			seedMigrationCompletionTailWindow(t, f, true, nil)
			surgeIdx := int32(1)
			record := mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(30*time.Minute))
			record.SurgeInstance = &surgeIdx
			record.Phase = workload.MigrationPhaseDraining
			f.records = []workload.MigrationRecord{record}
			finalizations := 0
			f.finalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalizations++
				return true, nil
			}
			input := f.input(t)
			mutateMigrationIRStatus(t, f, func(ir *v1beta1.InferenceReplica) {
				test.mutate(ir, surgeIdx)
			})
			wantEffects := snapshotMigrationEffects(t, f)

			done, accepted, err := Migrate(context.Background(), f.deps(), input, f.plan, 0, uuid, migrationRequest(t, f, uuid))
			if err != nil || done || !accepted {
				t.Fatalf("guarded completion recovery: done=%v accepted=%v err=%v", done, accepted, err)
			}
			assertMigrationEffectsUnchanged(t, f, wantEffects)
			if finalizations != 0 {
				t.Fatalf("completion pair drift triggered %d resource finalizations", finalizations)
			}
		})
	}
}

func migrationGangReadyToDrain(t *testing.T) (*migFixture, string, int32) {
	t.Helper()
	f := newGangMigFixture(t)
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
	const uuid = "migration-pair-fence"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-b")}

	f.pass(t, uuid)
	f.pass(t, uuid)
	f.react(t)
	f.pass(t, uuid)
	f.react(t)

	record := f.record(t, uuid)
	if record.SurgeInstance == nil || record.Phase != workload.MigrationPhaseSurgePending {
		t.Fatalf("migration did not reach the pre-drain gate: %+v", *record)
	}
	return f, uuid, *record.SurgeInstance
}

func migrationGangReadyToPromote(t *testing.T) (*migFixture, string, int32) {
	t.Helper()
	f, uuid, surgeIdx := migrationGangReadyToDrain(t)
	f.pass(t, uuid)
	f.react(t)
	f.pass(t, uuid)
	f.react(t)

	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == "0" {
			t.Fatalf("source pod %s remained before promotion tail", pod.Name)
		}
	}
	if record := f.record(t, uuid); record.Phase != workload.MigrationPhaseDraining {
		t.Fatalf("migration did not reach the promotion tail: %+v", *record)
	}
	return f, uuid, surgeIdx
}

func migrationRequest(t *testing.T, f *migFixture, uuid string) *audit.MigrationRequest {
	t.Helper()
	record := f.record(t, uuid)
	return &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(f.component),
		Instance:        record.SourceInstance,
		FromNode:        record.FromNode,
		HintTargetNodes: append([]string(nil), record.HintTargetNodes...),
		Reason:          record.Reason,
	}
}

func mutateMigrationIRStatus(t *testing.T, f *migFixture, mutate func(*v1beta1.InferenceReplica)) {
	t.Helper()
	ir := f.getIR(t)
	mutate(ir)
	if err := f.c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("update migration status fixture: %v", err)
	}
}

func migrationStatusByIndex(t *testing.T, ir *v1beta1.InferenceReplica, index int32) *v1beta1.OMENativeInstanceStatus {
	t.Helper()
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == index {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	t.Fatalf("instance status %d missing", index)
	return nil
}

func removeMigrationStatusByIndex(ir *v1beta1.InferenceReplica, index int32) {
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index != index {
			continue
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:i], ir.Status.InstanceStatuses[i+1:]...)
		return
	}
}

func snapshotMigrationEffects(t *testing.T, f *migFixture) string {
	t.Helper()
	ir := f.getIR(t)
	pods := f.listPods(t)
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
	ledger, err := audit.LoadLedgerForOwner(context.Background(), f.c, f.isvc)
	if err != nil {
		t.Fatalf("load migration effects ledger: %v", err)
	}
	snapshot := struct {
		Status  v1beta1.InferenceReplicaStatus
		Pods    []*corev1.Pod
		Records []workload.MigrationRecord
		Ledger  *audit.Ledger
	}{
		Status:  ir.Status,
		Pods:    pods,
		Records: f.records,
		Ledger:  ledger,
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal migration effects: %v", err)
	}
	return string(data)
}

func assertMigrationEffectsUnchanged(t *testing.T, f *migFixture, want string) {
	t.Helper()
	if got := snapshotMigrationEffects(t, f); got != want {
		t.Fatal("migration pair drift changed status, pods, readiness, record, or ledger")
	}
}

func blockMigrationPodCreates(t *testing.T, f *migFixture, blocked *bool, allowedWhileBlocked int, blockErr error) *int {
	t.Helper()
	base, ok := f.c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", f.c)
	}
	attempts := 0
	f.c = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			pod, isPod := obj.(*corev1.Pod)
			if !isPod {
				return cl.Create(ctx, obj, opts...)
			}
			attempts++
			if !*blocked || allowedWhileBlocked > 0 {
				if *blocked {
					allowedWhileBlocked--
				}
				return cl.Create(ctx, obj, opts...)
			}
			if blockErr != nil {
				return blockErr
			}
			return apierrors.NewForbidden(corev1.Resource("pods"), pod.Name, errors.New("admission rejected test pod"))
		},
	})
	return &attempts
}

func TestMigrate_SurgePodCreateBlockUsesMigrationDeadline(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	f := newSinglePodMigFixture(t)
	f.clk = clk
	const uuid = "mig-create-block-deadline"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(time.Minute)),
	}
	blocked := true
	attempts := blockMigrationPodCreates(t, f, &blocked, 0, nil)

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || done || !accepted {
		t.Fatalf("blocked create pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if *attempts != 1 {
		t.Fatalf("pod create attempts = %d, want 1", *attempts)
	}
	if !workload.DefaultExpectations.Satisfied(f.isvc.Namespace, f.isvc.Name, f.component, 1) {
		t.Fatal("rejected pod create left an outstanding expectation")
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseSurgePending {
		t.Fatalf("record phase = %s, want SurgePending", rec.Phase)
	}

	clk.Step(2 * time.Minute)
	expired, err := ExpireMigrations(context.Background(), f.deps(), f.input(t), f.plan)
	if err != nil {
		t.Fatalf("ExpireMigrations: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired records = %d, want 1", expired)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase after deadline = %s, want Failed", rec.Phase)
	}
	if source := findInstanceStatusOnIRForFixture(t, f, 0); source == nil || workload.InstancePhase(source.Phase) != workload.InstancePhaseReady || source.Operation != nil {
		t.Fatalf("source not restored after expiry: %+v", source)
	}
}

func TestMigrate_SurgePodCreateBlockRetries(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-create-block-retry"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	blocked := true
	blockMigrationPodCreates(t, f, &blocked, 0, nil)

	done, accepted, err := f.passResult(t, uuid)
	if err != nil || done || !accepted {
		t.Fatalf("blocked create pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	blocked = false
	done, accepted, err = f.passResult(t, uuid)
	if err != nil || done || !accepted {
		t.Fatalf("retry create pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("surge pod count = %d, want 1", len(pods))
	}
}

func TestMigrate_GangPartialCreateBlockRetriesMissingMembers(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-create-block"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	blocked := true
	attempts := blockMigrationPodCreates(t, f, &blocked, 1, nil)

	if done, accepted, err := f.passResult(t, uuid); err != nil || done || !accepted {
		t.Fatalf("gang stamp pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if done, accepted, err := f.passResult(t, uuid); err != nil || done || !accepted {
		t.Fatalf("partial gang create pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if *attempts != 2 {
		t.Fatalf("pod create attempts = %d, want 2", *attempts)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list partial surge gang: %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("partial surge gang size = %d, want 1", len(pods))
	}

	blocked = false
	if done, accepted, err := f.passResult(t, uuid); err != nil || done || !accepted {
		t.Fatalf("gang retry pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	pods, err = query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list retried surge gang: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("retried surge gang size = %d, want 3", len(pods))
	}
}

func TestMigrate_GangCreateBlockBeforeFirstMember(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-gang-create-block-first"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	blocked := true
	attempts := blockMigrationPodCreates(t, f, &blocked, 0, nil)

	if done, accepted, err := f.passResult(t, uuid); err != nil || done || !accepted {
		t.Fatalf("gang stamp pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if done, accepted, err := f.passResult(t, uuid); err != nil || done || !accepted {
		t.Fatalf("blocked gang create pass: done=%v accepted=%v err=%v", done, accepted, err)
	}
	if *attempts != 1 {
		t.Fatalf("pod create attempts = %d, want 1", *attempts)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), f.c, f.isvc.Namespace, f.isvc.Name, f.component, 1)
	if err != nil {
		t.Fatalf("list blocked surge gang: %v", err)
	}
	if len(pods) != 0 {
		t.Fatalf("blocked surge gang size = %d, want 0", len(pods))
	}
}

func TestMigrate_SurgePodCreateBlockRequiresBoundedRetry(t *testing.T) {
	tests := []struct {
		name     string
		deadline bool
		blockErr error
	}{
		{name: "zero migration deadline", blockErr: apierrors.NewForbidden(corev1.Resource("pods"), "surge", errors.New("admission rejected test pod"))},
		{name: "context cancellation", deadline: true, blockErr: context.Canceled},
		{name: "context deadline", deadline: true, blockErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSinglePodMigFixture(t)
			const uuid = "mig-create-block-unbounded"
			rec := mkMigRecord(uuid, 0, "node-a")
			if !tt.deadline {
				rec.Deadline = metav1.Time{}
			}
			f.records = []workload.MigrationRecord{rec}
			blocked := true
			blockMigrationPodCreates(t, f, &blocked, 0, tt.blockErr)

			done, accepted, err := f.passResult(t, uuid)
			if err == nil || done || !accepted {
				t.Fatalf("create failure: done=%v accepted=%v err=%v", done, accepted, err)
			}
			if tt.blockErr == context.Canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
			if tt.blockErr == context.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want context.DeadlineExceeded", err)
			}
		})
	}
}

// Tests for the early close a migration takes when its surge pod is
// parked in a terminal kubelet waiting reason: the record — not the
// source row — is what fails, the source goes back to serving on the
// revision it never left, and the surge index is unpinned for the
// ordinary scale-down pipeline. Shares the entry-lifecycle harness
// (migFixture) with an injected fake clock and event recorder.

// wedgeGrace is the stuck-pod grace the wedge fixtures configure. Long
// enough that a freshly created pod is inside it, so the tests can step
// the clock across the boundary explicitly.
const wedgeGrace = 5 * time.Minute

// migWedgeEvents drains the fixture recorder.
func migWedgeEvents(t *testing.T, f *migFixture) []string {
	t.Helper()
	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// armWedgeFixture wires the fake clock, the recorder and the stuck-pod
// grace, then seeds one Manual record whose Deadline is far enough out
// that only the evidence can end the migration.
func armWedgeFixture(t *testing.T, f *migFixture, uuid string, sourceIdx int32, fromNode string) *clocktesting.FakeClock {
	t.Helper()
	clk := f.withFakeClock()
	f.recorder = record.NewFakeRecorder(16)
	f.stuckPodGrace = wedgeGrace
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, sourceIdx, fromNode, clk.Now().Add(4*time.Hour)),
	}
	return clk
}

// migPodsForInstance returns the fixture's live pods for one index.
func migPodsForInstance(t *testing.T, f *migFixture, idx int32) []*corev1.Pod {
	t.Helper()
	var out []*corev1.Pod
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] == strconv.Itoa(int(idx)) {
			out = append(out, pod)
		}
	}
	return out
}

// wedgePod parks a container of pod in reason, drops ContainersReady
// the way a kubelet does for a container that will not start, and
// stamps a creation time the fake clock can be advanced past — the
// kubelet exposes no per-state transition time, so creation is what the
// grace is measured from.
func wedgePod(t *testing.T, f *migFixture, pod *corev1.Pod, reason string) {
	t.Helper()
	ctx := context.Background()
	if pod.CreationTimestamp.IsZero() {
		pod.CreationTimestamp = metav1.NewTime(f.clk.Now())
		if err := f.c.Update(ctx, pod); err != nil {
			t.Fatalf("stamp pod %s creation time: %v", pod.Name, err)
		}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	kept := make([]corev1.PodCondition, 0, len(pod.Status.Conditions)+1)
	for _, c := range pod.Status.Conditions {
		if c.Type != corev1.ContainersReady {
			kept = append(kept, c)
		}
	}
	pod.Status.Conditions = append(kept, corev1.PodCondition{
		Type: corev1.ContainersReady, Status: corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(f.clk.Now()),
	})
	if err := f.c.Status().Update(ctx, pod); err != nil {
		t.Fatalf("wedge pod %s in %s: %v", pod.Name, reason, err)
	}
}

// driveToSurgePods runs passes until the surge index has live pods, and
// returns them. The gang shape needs one extra pass so its PodGroup is
// created before its members render.
func driveToSurgePods(t *testing.T, f *migFixture, uuid string, surgeIdx int32) []*corev1.Pod {
	t.Helper()
	for i := 0; i < 4; i++ {
		done, accepted := f.pass(t, uuid)
		if done || !accepted {
			t.Fatalf("pass %d: got done=%v accepted=%v, want in-flight", i+1, done, accepted)
		}
		if pods := migPodsForInstance(t, f, surgeIdx); len(pods) > 0 {
			return pods
		}
	}
	t.Fatalf("surge index %d never got pods; record=%+v", surgeIdx, *f.record(t, uuid))
	return nil
}

// assertSourceRestored pins the restore half of the close: the source is
// Ready again on the RunningRevision it never left, unpinned, and back
// in rotation with the drive's drain hold gone.
func assertSourceRestored(t *testing.T, f *migFixture, sourceIdx int32, uuid, wantRevision string) {
	t.Helper()
	src := findInstanceStatusOnIRForFixture(t, f, sourceIdx)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceReady || src.Operation != nil {
		t.Fatalf("source must be restored to Ready and unpinned; got %+v", src)
	}
	if src.RunningRevision != wantRevision {
		t.Errorf("source RunningRevision = %q, want the untouched %q", src.RunningRevision, wantRevision)
	}
	if src.LastFailure != nil {
		t.Errorf("a healthy source must not be stamped with a failure; got %+v", src.LastFailure)
	}
	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	for _, pod := range migPodsForInstance(t, f, sourceIdx) {
		if podreadiness.ContainsNotReadyKey(pod, drainKey) || !podreadiness.IsServing(pod) {
			t.Errorf("source pod %s must be back in rotation with the drain hold released; conditions=%+v",
				pod.Name, pod.Status.Conditions)
		}
	}
}

// assertNoRetryBlocks pins that the close charges no revision.
func assertNoRetryBlocks(t *testing.T, f *migFixture) {
	t.Helper()
	if blocks := f.getIR(t).Status.RetryBlocks; len(blocks) != 0 {
		t.Errorf("a wedged surge blames no revision; got RetryBlocks=%+v", blocks)
	}
}

// A surge pod that cannot pull its image can never take over, so the
// migration is closed on that evidence long before the record's
// Deadline: the record carries the pod and the reason, the source is
// restored to Ready on its untouched revision and back in rotation, the
// surge index is unpinned for the bounded scale-down pipeline, one
// Warning names the wedge and no revision is blamed.
func TestMigrate_SurgeWedgedInImagePull_FailsThroughRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-pull"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	if sourceRevision == "" {
		t.Fatal("setup: source has no RunningRevision")
	}
	wedgePod(t, f, surgePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f) // drop the accept event

	done, accepted := f.pass(t, uuid)
	if !done || !accepted {
		t.Fatalf("wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}

	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || rec.CompletedAt == nil {
		t.Fatalf("record must close Failed with CompletedAt; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, surgePods[0].Name) || !strings.Contains(rec.Message, "ImagePullBackOff") {
		t.Errorf("record Message must name the pod and the reason; got %q", rec.Message)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)

	surge := findInstanceStatusOnIRForFixture(t, f, 1)
	if surge == nil || surge.Operation != nil {
		t.Fatalf("surge must keep its slot with the pin cleared so scale-down takes it; got %+v", surge)
	}

	events := migWedgeEvents(t, f)
	if len(events) != 1 {
		t.Fatalf("want exactly one event for the close; got %v", events)
	}
	if !strings.Contains(events[0], string(workload.EventReasonMigrationSurgeWedged)) ||
		!strings.Contains(events[0], surgePods[0].Name) ||
		!strings.Contains(events[0], "ImagePullBackOff") {
		t.Errorf("the Warning must name the pod and the reason; got %q", events[0])
	}
}

// A crash-looping surge is only evidence once it has been crash-looping
// for the configured grace: inside it the migration keeps driving, past
// it the record closes. The grace is the operator's, so an unconfigured
// one leaves the record's Deadline as the only consumer.
func TestMigrate_SurgeCrashLoop_ClosesOnlyPastTheGrace(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-crash"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, surgePods[0], "CrashLoopBackOff")

	clk.Step(wedgeGrace - time.Minute)
	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("inside the grace: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("inside the grace the record must stay open; got %+v", *rec)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Operation == nil {
		t.Fatalf("inside the grace the source keeps its pin; got %+v", src)
	}

	clk.Step(2 * time.Minute)
	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("past the grace: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, "CrashLoopBackOff") {
		t.Fatalf("past the grace the record must close Failed naming the reason; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)
}

// The record is the single authority and the close is idempotent: a
// second pass over the same evidence re-reads a terminal record and
// writes nothing, and a record already terminal for another cause is
// not reopened by the evidence either.
func TestMigrate_SurgeWedgeCloseIsIdempotent(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-idem"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "ErrImagePull")
	clk.Step(wedgeGrace + time.Second)
	if done, _ := f.pass(t, uuid); !done {
		t.Fatal("first wedge pass must close the migration")
	}
	closed := *f.record(t, uuid)
	surgeBefore := findInstanceStatusOnIRForFixture(t, f, 1)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("second pass: got done=%v accepted=%v, want the terminal record handled", done, accepted)
	}
	after := f.record(t, uuid)
	if after.Message != closed.Message || !after.CompletedAt.Equal(closed.CompletedAt) {
		t.Errorf("second pass rewrote the terminal record: %+v -> %+v", closed, *after)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("second pass must be silent; got %v", events)
	}
	if got := findInstanceStatusOnIRForFixture(t, f, 1); !equalitySafeOperation(surgeBefore, got) {
		t.Errorf("second pass rewrote the surge slot: %+v -> %+v", surgeBefore, got)
	}
}

// A record already closed by another authority is left alone: the
// evidence pass reads the terminal phase and returns without touching
// the message, the pair, or the event stream.
func TestMigrate_SurgeWedgeDoesNotReopenTerminalRecord(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-terminal"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	wedgePod(t, f, surgePods[0], "InvalidImageName")
	clk.Step(wedgeGrace + time.Second)
	f.record(t, uuid).Phase = workload.MigrationPhaseFailed
	f.record(t, uuid).Message = "closed by an earlier authority"
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("terminal record: got done=%v accepted=%v, want it handled", done, accepted)
	}
	if msg := f.record(t, uuid).Message; msg != "closed by an earlier authority" {
		t.Errorf("a terminal record must not be reopened; message = %q", msg)
	}
	if src := findInstanceStatusOnIRForFixture(t, f, 0); src == nil || src.Operation == nil {
		t.Errorf("a terminal record drives no restore; source = %+v", src)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("a terminal record emits nothing; got %v", events)
	}
}

// The evidence read is scoped to the surge. A source pod wedged
// mid-migration is the row's own pod, which no escalation pass owns
// while the Migrate pin is held, so the record keeps driving and its
// Deadline stays the only bound.
func TestMigrate_WedgedSourcePodLeavesTheMigrationDriving(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-source"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")

	driveToSurgePods(t, f, uuid, 1)
	sourcePods := migPodsForInstance(t, f, 0)
	if len(sourcePods) != 1 {
		t.Fatalf("setup: want one source pod, got %d", len(sourcePods))
	}
	wedgePod(t, f, sourcePods[0], "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("wedged source: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase.Terminal() {
		t.Fatalf("a wedged source does not close the record; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Fatalf("a wedged source keeps its Migrate pin; got %+v", src)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("a wedged source announces nothing from the migration pass; got %v", events)
	}
}

// One wedged member is enough for a gang surge: the whole replacement
// can never be admitted, so the record closes on that member's evidence
// and the gang source goes back to serving intact.
func TestMigrate_GangSurgeMemberWedged_FailsThroughRecord(t *testing.T) {
	f := newGangMigFixture(t)
	const uuid = "mig-wedge-gang"
	clk := armWedgeFixture(t, f, uuid, 0, "node-b")

	surgePods := driveToSurgePods(t, f, uuid, 1)
	if len(surgePods) != 3 {
		t.Fatalf("setup: want a 3-pod surge gang, got %d", len(surgePods))
	}
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedged := surgePods[len(surgePods)-1]
	wedgePod(t, f, wedged, "ImagePullBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("gang wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed || !strings.Contains(rec.Message, wedged.Name) {
		t.Fatalf("record must close Failed naming the wedged member; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
	assertNoRetryBlocks(t, f)
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation != nil {
		t.Fatalf("the surge gang index must be unpinned for scale-down; got %+v", surge)
	}
}

// A surge that wedges once the drain is already under way still closes
// through the record, and the close is what puts the source back: the
// drive's drain hold comes off, the pod re-enters rotation, and the
// unpinned surge index leaves through the ordinary scale-down pipeline.
func TestMigrate_SurgeWedgedWhileDraining_RestoresRotation(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-draining"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	drainKey := podreadiness.Message{UserAgent: podreadiness.WriterMigrateSourceDrain, Key: uuid}
	srcPod := migPodsForInstance(t, f, 0)[0]
	if podreadiness.IsServing(srcPod) || !podreadiness.ContainsNotReadyKey(srcPod, drainKey) {
		t.Fatalf("setup: the drive must have drained the source pod; conditions=%+v", srcPod.Status.Conditions)
	}
	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("draining wedge pass: got done=%v accepted=%v, want the migration closed", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record must close Failed; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)

	// Once the endpoint controller settles, the un-drained source is
	// routable again in its per-revision Service.
	f.react(t)
	srcPod = migPodsForInstance(t, f, 0)[0]
	srcSvc := query.PerRevisionServiceName(f.isvc.Name, f.component, testRevisionHashLegacy)
	inRotation, rerr := drain.IsPodInRotation(context.Background(), f.c, f.isvc.Namespace, srcSvc, srcPod)
	if rerr != nil {
		t.Fatalf("check source rotation: %v", rerr)
	}
	if !inRotation {
		t.Fatal("the restored source must re-enter rotation")
	}

	// The unpinned surge index is torn down by the ordinary bounded
	// scale-down pipeline, not by anything the migration does.
	removed := false
	for i := 0; i < 8 && !removed; i++ {
		removed = f.deleteExtraBatch(t, 1)
		f.react(t)
	}
	if !removed {
		t.Fatal("the unpinned surge index must converge to gone through scale-down")
	}
	if pods := migPodsForInstance(t, f, 1); len(pods) != 0 {
		t.Errorf("scale-down must leave no surge pods; got %d", len(pods))
	}
}

// A source that is already drained and deleted has nothing to restore
// to rotation, so the evidence does not close the record from here: the
// pair stays as it is and the record's Deadline remains the backstop.
func TestMigrate_SurgeWedgedAfterSourceGone_LeavesTheDeadline(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-wedge-postdrain"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	for _, pod := range migPodsForInstance(t, f, 0) {
		if err := f.c.Delete(context.Background(), pod); err != nil {
			t.Fatalf("delete source pod %s: %v", pod.Name, err)
		}
	}
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); done || !accepted {
		t.Fatalf("post-drain wedge: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseDraining {
		t.Fatalf("the record must stay Draining for its Deadline; got %+v", *rec)
	}
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating || src.Operation == nil {
		t.Fatalf("a source with no pods is not restored; got %+v", src)
	}
	if src.LastFailure != nil {
		t.Errorf("a source with no pods is not stamped Failed either; got %+v", src.LastFailure)
	}
	if events := migWedgeEvents(t, f); len(events) != 0 {
		t.Errorf("nothing is announced when the evidence does not apply; got %v", events)
	}
}

// The close is re-derived from the record and the surge pods alone, so
// a crash part-way through the unwind resumes: the surge is already
// unpinned, which makes the pair unconfirmable, and the next pass still
// finishes the close instead of waiting out the Deadline.
func TestMigrate_SurgeWedgeCloseResumesAfterPartialUnwind(t *testing.T) {
	f := newSinglePodMigFixture(t)
	f.finalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
	const uuid = "mig-wedge-resume"
	clk := armWedgeFixture(t, f, uuid, 0, "node-a")
	driveToDrainingDrainIncomplete(t, f, uuid)

	sourceRevision := findInstanceStatusOnIRForFixture(t, f, 0).RunningRevision
	wedgePod(t, f, migPodsForInstance(t, f, 1)[0], "CrashLoopBackOff")
	clk.Step(wedgeGrace + time.Second)

	// The crash window: the surge pin is gone, the source restore and
	// the record write never landed.
	if err := status.ClearMigrationPin(context.Background(), f.input(t), 1, uuid); err != nil {
		t.Fatalf("simulate a crash after the surge unpin: %v", err)
	}
	if surge := findInstanceStatusOnIRForFixture(t, f, 1); surge == nil || surge.Operation != nil {
		t.Fatalf("crash window setup: the surge must be unpinned; got %+v", surge)
	}
	migWedgeEvents(t, f)

	if done, accepted := f.pass(t, uuid); !done || !accepted {
		t.Fatalf("resume pass: got done=%v accepted=%v, want the close finished", done, accepted)
	}
	if rec := f.record(t, uuid); rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("the resume pass must close the record; got %+v", *rec)
	}
	assertSourceRestored(t, f, 0, uuid, sourceRevision)
}

// equalitySafeOperation reports whether two status slots carry the same
// operation pin, tolerating either being absent.
func equalitySafeOperation(a, b *v1beta1.OMENativeInstanceStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return (a.Operation == nil) == (b.Operation == nil) && a.Phase == b.Phase
}

// quietMigrationSurge turns the migration's surge pod into one the
// kubelet has stopped reporting: phase Unknown, bound to a node the
// cluster never had, so node-death evidence reads it as gone.
func quietMigrationSurge(t *testing.T, f *migFixture, pod *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	live := &corev1.Pod{}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("get surge pod: %v", err)
	}
	live.Spec.NodeName = "node-gone"
	if err := f.c.Update(ctx, live); err != nil {
		t.Fatalf("bind surge pod: %v", err)
	}
	live.Status = corev1.PodStatus{Phase: corev1.PodUnknown}
	if err := f.c.Status().Update(ctx, live); err != nil {
		t.Fatalf("quiet surge pod: %v", err)
	}
}

// assertMigrationSourceUntouched pins that the source kept its pin and
// its rotation across a pass over a quiet surge pod.
func assertMigrationSourceUntouched(t *testing.T, f *migFixture, before *v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	src := findInstanceStatusOnIRForFixture(t, f, 0)
	if !equalitySafeOperation(src, before) {
		t.Errorf("source row moved: got %+v want %+v", src, before)
	}
	if src != nil && src.LastFailure != nil {
		t.Errorf("source LastFailure: got %+v want none (a quiet surge is not the source's failure)", src.LastFailure)
	}
	for _, pod := range migPodsForInstance(t, f, 0) {
		if pod.DeletionTimestamp != nil || !podreadiness.IsServing(pod) {
			t.Errorf("source pod %s must stay in rotation; deletion=%v conditions=%+v",
				pod.Name, pod.DeletionTimestamp, pod.Status.Conditions)
		}
	}
}

// TestMigrateSurge_UnknownTargetSweptOnProvenNodeDeath: the migration's
// surge pod goes quiet on a node the cluster no longer has. Its name is
// not recycled the way a dead pod's is; with a force-delete policy
// configured the sweep frees it on the pass that reads it and the same
// request rebuilds the surge at its ordinal, while the source keeps
// serving and the record stays in flight.
func TestMigrateSurge_UnknownTargetSweptOnProvenNodeDeath(t *testing.T) {
	f := newSinglePodMigFixture(t)
	f.forceDelete = &workload.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	const uuid = "mig-surge-unknown-swept"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgePods := driveToSurgePods(t, f, uuid, 1)
	quietMigrationSurge(t, f, surgePods[0])
	before := findInstanceStatusOnIRForFixture(t, f, 0)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("sweep pass: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if !sitePodGone(t, f.c, surgePods[0]) {
		t.Fatalf("quiet surge pod %s must be force-deleted once its node is provably gone", surgePods[0].Name)
	}
	if rec := f.record(t, uuid); rec.Phase == workload.MigrationPhaseFailed {
		t.Errorf("record: got Failed want the migration still driving; %+v", *rec)
	}
	assertMigrationSourceUntouched(t, f, before)

	done, accepted = f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("rebuild pass: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	rebuilt := migPodsForInstance(t, f, 1)
	if len(rebuilt) != 1 || rebuilt[0].Name != surgePods[0].Name || rebuilt[0].Status.Phase == corev1.PodUnknown {
		t.Fatalf("the same request must rebuild the surge at its ordinal; got %+v", rebuilt)
	}
	assertMigrationSourceUntouched(t, f, before)
}

// TestMigrateSurge_UnknownTargetHeldWithoutForceDeletePolicy: with no
// force-delete policy nothing can prove the node dead, so the quiet surge
// pod keeps its name: it is neither deleted nor created over, the record
// stays in flight for its Deadline, and the source is untouched.
func TestMigrateSurge_UnknownTargetHeldWithoutForceDeletePolicy(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-surge-unknown-held"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
	surgePods := driveToSurgePods(t, f, uuid, 1)
	quietMigrationSurge(t, f, surgePods[0])
	before := findInstanceStatusOnIRForFixture(t, f, 0)

	done, accepted := f.pass(t, uuid)
	if done || !accepted {
		t.Fatalf("held pass: got done=%v accepted=%v, want the migration still in flight", done, accepted)
	}
	if sitePodGone(t, f.c, surgePods[0]) {
		t.Fatalf("quiet surge pod %s must not be deleted without a force-delete policy", surgePods[0].Name)
	}
	held := migPodsForInstance(t, f, 1)
	if len(held) != 1 || held[0].Status.Phase != corev1.PodUnknown {
		t.Fatalf("the surge ordinal must still hold the quiet pod; got %+v", held)
	}
	if rec := f.record(t, uuid); rec.Phase == workload.MigrationPhaseFailed {
		t.Errorf("record: got Failed want the migration still driving; %+v", *rec)
	}
	assertMigrationSourceUntouched(t, f, before)
}
