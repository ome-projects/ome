// Internal-package tests for workload.Migrate's private helpers
// (validateFromNode, etc.) that aren't reachable from outside the
// workload/ops package. The end-to-end TestMigrate_* lifecycle tests
// (status.migrations-driven, single-pod + gang) live in
// migrate_entry_lifecycle_test.go alongside this file.
package ops

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
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
