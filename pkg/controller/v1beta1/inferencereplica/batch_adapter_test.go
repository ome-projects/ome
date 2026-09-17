package inferencereplica

// buildApplyInstanceMutations adapter contract: the batched sibling of
// buildMutateInstance — one fresh Get, every mutation applied, ONE
// Status().Update, retry-on-conflict as a whole, zero writes when no
// mutation reports a change, an owner-gone signal for guarded effects, and
// committed slots mirrored onto the caller's in-memory IR.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// newCountingStatusClient builds a fake client over objs that counts
// status-subresource Update calls, optionally failing the first
// failFirst of them with a Conflict.
func newCountingStatusClient(t *testing.T, failFirst int, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			assertCompactInferenceReplicaWrite(t, obj)
			*writes++
			if *writes <= failFirst {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
					obj.GetName(), fmt.Errorf("the object has been modified"))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

func assertCompactInferenceReplicaWrite(t *testing.T, obj client.Object) {
	t.Helper()
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return
	}
	for _, status := range ir.Status.InstanceStatuses {
		if status.ReadyPodCount != 0 || status.ScheduledPodCount != 0 || status.NodesOccupied != nil {
			t.Fatalf("status write for Instance %d persisted Pod-derived observations: %+v", status.Index, status)
		}
	}
	body, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal status write: %v", err)
	}
	for _, field := range []string{"readyPodCount", "scheduledPodCount", "nodesOccupied"} {
		if strings.Contains(string(body), `"`+field+`"`) {
			t.Fatalf("status write serialized compatibility field %q", field)
		}
	}
}

func newFailingStatusClient(t *testing.T, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			*writes++
			return fmt.Errorf("injected status write failure")
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

func newStatusOwnerGoneOnUpdateClient(t *testing.T, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			*writes++
			return apierrors.NewNotFound(
				schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
				obj.GetName())
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

func newWireNormalizingStatusClient(t *testing.T, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			*writes++
			wire := &v1beta1.InferenceReplica{}
			encoded, err := json.Marshal(obj)
			if err != nil {
				return fmt.Errorf("encode status update: %w", err)
			}
			if err := json.Unmarshal(encoded, wire); err != nil {
				return fmt.Errorf("decode status update: %w", err)
			}
			if err := c.SubResource(sub).Update(ctx, wire, opts...); err != nil {
				return err
			}
			persisted, ok := obj.(*v1beta1.InferenceReplica)
			if !ok {
				return fmt.Errorf("unexpected status object %T", obj)
			}
			wire.DeepCopyInto(persisted)
			return nil
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

func newCommitThenErrorStatusClient(t *testing.T, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			*writes++
			wire := &v1beta1.InferenceReplica{}
			encoded, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(encoded, wire); err != nil {
				return err
			}
			if err := c.SubResource(sub).Update(ctx, wire, opts...); err != nil {
				return err
			}
			return fmt.Errorf("injected response loss")
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

type staleReadingClient struct {
	client.Client
	reader client.Reader
}

func (c *staleReadingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}

type driftingInstanceReader struct {
	client.Reader
	reads int
	index int32
}

func (r *driftingInstanceReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	r.reads++
	if r.reads < 2 {
		return nil
	}
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok {
		return fmt.Errorf("unexpected object %T", obj)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == r.index {
			ir.Status.InstanceStatuses[i].Operation = &v1beta1.InstanceOperation{
				ID: "replacement-owner", Type: v1beta1.InstanceOperationUpdate,
			}
		}
	}
	return nil
}

type indexedPodCreateFailureClient struct {
	client.Client
	failIndex int32
	err       error
	attempted []int32
}

func (c *indexedPodCreateFailureClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		c.attempted = append(c.attempted, int32(idx))
		if int32(idx) == c.failIndex {
			return c.err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

// failedStamp is a canonical batchable mutation: flip the slot Failed.
func failedStamp(idx int32) workloadtypes.InstanceMutation {
	return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseFailed {
			return false
		}
		s.Phase = workload.InstancePhaseFailed
		return true
	}}
}

func creatingStamp(idx int32, now metav1.Time) workloadtypes.InstanceMutation {
	return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		s.Incarnation = 1
		s.Phase = workload.InstancePhaseCreating
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		}
		return true
	}}
}

func fullyPopulatedInstanceStatus(idx int32) v1beta1.OMENativeInstanceStatus {
	started := metav1.NewTime(time.Date(2026, time.February, 1, 10, 0, 0, 0, time.UTC))
	progress := metav1.NewTime(started.Add(2 * time.Minute))
	deadline := metav1.NewTime(started.Add(30 * time.Minute))
	transition := metav1.NewTime(started.Add(time.Minute))
	surge := idx + 100
	exitCode := int32(137)
	return v1beta1.OMENativeInstanceStatus{
		Index:             idx,
		Incarnation:       42,
		Phase:             v1beta1.OMENativeInstanceRestarting,
		RunningRevision:   "rev-running",
		TargetRevision:    "rev-target",
		PodCount:          8,
		ReadyPodCount:     7,
		ServingPodCount:   6,
		AvailablePodCount: 5,
		ScheduledPodCount: 8,
		Admitted:          true,
		NodesOccupied:     []string{"node-a", "node-b"},
		Conditions: []metav1.Condition{{
			Type:               "AllPodsReady",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: 17,
			LastTransitionTime: transition,
			Reason:             "PodPending",
			Message:            "one worker is still starting",
		}},
		Operation: &v1beta1.InstanceOperation{
			ID:              fmt.Sprintf("migrate-%d", idx),
			Type:            v1beta1.InstanceOperationMigrate,
			Step:            "WaitReady",
			StartedAt:       started,
			LastProgressAt:  progress,
			Deadline:        deadline,
			RetryCount:      3,
			TargetRevision:  "rev-target",
			Reason:          "node-recovery",
			SurgeIndex:      &surge,
			FromNode:        "node-old",
			HintTargetNodes: []string{"node-new-a", "node-new-b"},
			RequestUUID:     fmt.Sprintf("request-%d", idx),
		},
		ActiveOrdinal: 1,
		LastFailure: &v1beta1.InstanceTermination{
			PodName:       fmt.Sprintf("engine-%d-worker-6", idx),
			ContainerName: "main",
			Reason:        "OOMKilled",
			ExitCode:      &exitCode,
			Message:       "out of memory",
			Time:          transition,
		},
	}
}

func clearPodDerivedStatusForTest(status *v1beta1.OMENativeInstanceStatus) {
	status.ReadyPodCount = 0
	status.ScheduledPodCount = 0
	status.NodesOccupied = nil
}

func instanceStatusAt(t *testing.T, ir *v1beta1.InferenceReplica, idx int32) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	for _, status := range ir.Status.InstanceStatuses {
		if status.Index == idx {
			return status
		}
	}
	t.Fatalf("InstanceStatus index %d not found", idx)
	return v1beta1.OMENativeInstanceStatus{}
}

// TestBuildApplyInstanceMutations_OneWriteForBatch pins the headline
// contract: a batch touching several Instances lands in exactly ONE
// Status().Update, every mutation is persisted, and the committed slots
// are mirrored back onto the in-memory IR.
func TestBuildApplyInstanceMutations_OneWriteForBatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, Incarnation: 2, Admitted: true},
		{Index: 1, Phase: v1beta1.OMENativeInstanceRestarting, Incarnation: 2, Admitted: true},
	}
	c, writes := newCountingStatusClient(t, 0, ir)

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), failedStamp(1),
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(1),
		"a multi-instance batch must land in exactly ONE Status().Update")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(2))
	for i, s := range got.Status.InstanceStatuses {
		g.Expect(s.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceFailed), "instance %d", i)
		g.Expect(s.Incarnation).To(gomega.Equal(int64(2)), "instance %d untouched fields survive", i)
		g.Expect(s.Admitted).To(gomega.BeTrue(), "instance %d admission state survives", i)
	}
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses),
		"committed slots must be mirrored onto the in-memory IR")
}

func TestBuildApplyInstanceMutations_PreservesEveryRetainedField(t *testing.T) {
	g := gomega.NewWithT(t)
	original := fullyPopulatedInstanceStatus(7)
	ir := baselineIR("llama-engine", "prod", 8)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{original}
	c, writes := newCountingStatusClient(t, 0, ir)
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, before)).To(gomega.Succeed())

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{failedStamp(7)})).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1))

	want := *before.Status.InstanceStatuses[0].DeepCopy()
	want.Phase = v1beta1.OMENativeInstanceFailed
	clearPodDerivedStatusForTest(&want)
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal([]v1beta1.OMENativeInstanceStatus{want}),
		"the adapter must preserve retained counters, lifecycle state, conditions, ordinals, failure diagnostics, and every operation field")
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses),
		"the in-memory mirror must contain the same complete committed status")
}

func TestBuildApplyInstanceMutations_DuplicateIndexMutationsCompose(t *testing.T) {
	g := gomega.NewWithT(t)
	original := fullyPopulatedInstanceStatus(3)
	ir := baselineIR("llama-engine", "prod", 4)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{original}
	c, writes := newCountingStatusClient(t, 0, ir)
	secondObservedFirst := false

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		{Index: 3, Mutate: func(s *workload.InstanceStatus) bool {
			s.PodCount += 2
			s.Operation.RetryCount++
			return true
		}},
		{Index: 3, Mutate: func(s *workload.InstanceStatus) bool {
			secondObservedFirst = s.PodCount == original.PodCount+2 &&
				s.Operation != nil && s.Operation.RetryCount == original.Operation.RetryCount+1
			s.ReadyPodCount = s.PodCount
			s.Phase = workload.InstancePhaseReady
			return true
		}},
	})).To(gomega.Succeed())

	g.Expect(secondObservedFirst).To(gomega.BeTrue(), "duplicate-index callbacks must compose in batch order")
	g.Expect(*writes).To(gomega.Equal(1))
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1), "duplicate mutations must not append duplicate slots")
	status := got.Status.InstanceStatuses[0]
	g.Expect(status.PodCount).To(gomega.Equal(original.PodCount + 2))
	g.Expect(status.ReadyPodCount).To(gomega.BeZero())
	g.Expect(status.ScheduledPodCount).To(gomega.BeZero())
	g.Expect(status.NodesOccupied).To(gomega.BeNil())
	g.Expect(status.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady))
	g.Expect(status.Operation.RetryCount).To(gomega.Equal(original.Operation.RetryCount + 1))
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses))
}

// TestBuildApplyInstanceMutations_TwoThousandEntriesOneWrite guards the
// adapter's high-scale contract: appending a full 2,000-slot
// scale-up wave still performs one status update, not one full-IR rewrite per
// Instance. It also exercises the indexed slot lookup used by large batches.
func TestBuildApplyInstanceMutations_TwoThousandEntriesOneWrite(t *testing.T) {
	const replicas int32 = 2000

	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", replicas)
	c, writes := newCountingStatusClient(t, 0, ir)
	now := metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))

	mutations := make([]workloadtypes.InstanceMutation, 0, replicas)
	for idx := int32(0); idx < replicas; idx++ {
		mutations = append(mutations, creatingStamp(idx, now))
	}

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), mutations)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1),
		"2,000 mutations must coalesce into one Status().Update")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(int(replicas)))
	expectedDeadline := now.Add(30 * time.Minute)
	for idx, status := range got.Status.InstanceStatuses {
		op := status.Operation
		if status.Index != int32(idx) || status.Incarnation != 1 || status.Phase != v1beta1.OMENativeInstanceCreating ||
			op == nil || op.ID != fmt.Sprintf("create-%d-%d", idx, now.Unix()) ||
			op.Type != v1beta1.InstanceOperationCreate || op.Step != "CreatePods" ||
			!op.StartedAt.Time.Equal(now.Time) || !op.LastProgressAt.Time.Equal(now.Time) ||
			!op.Deadline.Time.Equal(expectedDeadline) {
			t.Fatalf("status[%d] is not a complete production-shaped Creating stamp: %+v", idx, status)
		}
	}
	g.Expect(ir.Status.InstanceStatuses).To(gomega.HaveLen(int(replicas)),
		"all committed slots must be mirrored onto the in-memory IR")
	for idx, status := range ir.Status.InstanceStatuses {
		if status.Index != int32(idx) || status.Phase != v1beta1.OMENativeInstanceCreating || status.Operation == nil {
			t.Fatalf("mirrored status[%d] = {index:%d phase:%q}, want {index:%d phase:%q}",
				idx, status.Index, status.Phase, idx, v1beta1.OMENativeInstanceCreating)
		}
	}
}

// TestBuildApplyInstanceMutations_ConflictRetriesWholeBatch pins the
// conflict contract: a conflicted batch retries AS A WHOLE — the second
// attempt re-reads and re-applies every mutation, and the final state
// carries all of them.
func TestBuildApplyInstanceMutations_ConflictRetriesWholeBatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting},
		{Index: 1, Phase: v1beta1.OMENativeInstanceRestarting},
	}
	c, writes := newCountingStatusClient(t, 1, ir)
	callbackCalls := map[int32]int{}
	trackedFailedStamp := func(idx int32) workloadtypes.InstanceMutation {
		return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
			callbackCalls[idx]++
			s.Phase = workload.InstancePhaseFailed
			return true
		}}
	}

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		trackedFailedStamp(0), trackedFailedStamp(1),
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(2),
		"first write conflicts, the whole batch retries once")
	g.Expect(callbackCalls).To(gomega.Equal(map[int32]int{0: 2, 1: 2}),
		"every mutation callback must be reapplied on the fresh object after a conflict")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	for i, s := range got.Status.InstanceStatuses {
		g.Expect(s.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"instance %d must carry its mutation after the batch retry", i)
	}
}

func TestBuildApplyInstanceMutations_OnCommitReportsSuccessfulConflictAttemptOnce(t *testing.T) {
	g := gomega.NewWithT(t)
	initial := fullyPopulatedInstanceStatus(0)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{initial}
	c, writes := newCountingStatusClient(t, 1, ir)
	storedBefore := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), storedBefore)).To(gomega.Succeed())
	wantPrevious := v1beta1convert.InstanceStatusToWorkload(instanceStatusAt(t, storedBefore, 0))
	commitCalls := 0
	var previous, current *workload.InstanceStatus
	mutationCalls := 0
	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workload.InstanceStatus) bool {
			mutationCalls++
			status.Phase = workload.InstancePhaseFailed
			status.Operation = nil
			return true
		},
		OnCommit: func(before, after *workload.InstanceStatus) {
			commitCalls++
			previous = before
			current = after
		},
	}})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(2))
	g.Expect(mutationCalls).To(gomega.Equal(2), "the mutation is replayed after conflict")
	g.Expect(commitCalls).To(gomega.Equal(1), "only the durable retry attempt is reported")
	g.Expect(previous).NotTo(gomega.BeNil())
	g.Expect(*previous).To(gomega.Equal(wantPrevious))
	g.Expect(current).NotTo(gomega.BeNil())
	g.Expect(current.Phase).To(gomega.Equal(workload.InstancePhaseFailed))
	g.Expect(current.Operation).To(gomega.BeNil())
}

func TestBuildApplyInstanceMutations_OnCommitUsesPersistedRepresentation(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, writes := newWireNormalizingStatusClient(t, ir)
	now := metav1.NewTime(time.Date(2026, time.August, 14, 12, 0, 0, 123456789, time.UTC))
	var committed *workload.InstanceStatus
	mutation := creatingStamp(0, now)
	mutation.OnCommit = func(_, current *workload.InstanceStatus) {
		committed = current
	}

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{mutation})).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1))
	g.Expect(committed).NotTo(gomega.BeNil())

	stored := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored)).To(gomega.Succeed())
	wantCommitted := v1beta1convert.InstanceStatusToWorkload(instanceStatusAt(t, stored, 0))
	g.Expect(*committed).To(gomega.Equal(wantCommitted),
		"OnCommit must expose the API-persisted value used by an exact rollback precondition")
	g.Expect(committed.Operation.StartedAt.Nanosecond()).To(gomega.Equal(0),
		"metav1.Time API serialization must be represented in the committed callback")
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(stored.Status.InstanceStatuses),
		"the in-memory mirror must use the API-persisted representation")

	wantRollbackValue := *committed
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Precondition: func(status *workload.InstanceStatus) bool {
			return reflect.DeepEqual(*status, wantRollbackValue)
		},
	}})).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(2))
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored)).To(gomega.Succeed())
	g.Expect(stored.Status.InstanceStatuses).To(gomega.BeEmpty(),
		"the exact rollback guard must survive an API timestamp round trip")
}

func TestBuildApplyInstanceMutations_ImmediateRollbackUsesAuthoritativeReader(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	live, writes := newCountingStatusClient(t, 0, ir)
	stale, _ := newCountingStatusClient(t, 0, ir.DeepCopy())
	writer := &staleReadingClient{Client: live, reader: stale}
	now := metav1.NewTime(time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC))
	var committed *workload.InstanceStatus
	mutation := creatingStamp(0, now)
	mutation.OnCommit = func(_, current *workload.InstanceStatus) {
		committed = current
	}

	apply := instanceOnlyMutationAdapter(
		buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(writer), live, ir),
	)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{mutation})).To(gomega.Succeed())
	g.Expect(committed).NotTo(gomega.BeNil())
	wantRollbackValue := *committed
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Precondition: func(status *workload.InstanceStatus) bool {
			return reflect.DeepEqual(*status, wantRollbackValue)
		},
	}})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(2),
		"an immediate rollback must read the status written earlier in the reconcile")
	stored := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), client.ObjectKeyFromObject(ir), stored)).To(gomega.Succeed())
	g.Expect(stored.Status.InstanceStatuses).To(gomega.BeEmpty())
}

func TestCreate_MidBatchFailureRollsBackAPINormalizedStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)
	c, writes := newWireNormalizingStatusClient(t, ir)
	clk := clocktesting.NewFakeClock(time.Date(2026, time.August, 14, 12, 0, 0, 123456789, time.UTC))
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Clock:                clk,
		Expectations:         workload.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
	}
	input := r.buildReconcileInput(context.Background(), ir, nil, nil, nil, 0, 0, coordination.GroupDefaults{})
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	plan, err := workload.BuildPlan(input.Key.Component, input.DesiredSpec, input.ObservedState)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	createErr := errors.New("pod create unavailable")
	podClient := &indexedPodCreateFailureClient{Client: c, failIndex: 1, err: createErr}
	_, err = workloadops.Create(context.Background(), workload.Deps{
		Client:       podClient,
		APIReader:    c,
		Expectations: r.Expectations,
		Clock:        clk,
	}, input, plan, nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(createErr.Error())))
	g.Expect(podClient.attempted).To(gomega.Equal([]int32{0, 1}))
	g.Expect(*writes).To(gomega.Equal(2),
		"the status wave and conditional rollback must each use one write")

	stored := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored)).To(gomega.Succeed())
	indices := make([]int32, 0, len(stored.Status.InstanceStatuses))
	for _, status := range stored.Status.InstanceStatuses {
		indices = append(indices, status.Index)
	}
	g.Expect(indices).To(gomega.ConsistOf(int32(0), int32(1)),
		"the unattempted instance must not retain a speculative Creating status")
}

func TestBuildApplyInstanceMutations_MixedChangesWriteOnceWithoutPhantomSlots(t *testing.T) {
	g := gomega.NewWithT(t)
	unchanged := fullyPopulatedInstanceStatus(0)
	unchanged.Phase = v1beta1.OMENativeInstanceFailed
	changed := fullyPopulatedInstanceStatus(1)
	ir := baselineIR("llama-engine", "prod", 3)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{unchanged, changed}
	c, writes := newCountingStatusClient(t, 0, ir)
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, before)).To(gomega.Succeed())
	ir.Status.InstanceStatuses = before.Status.DeepCopy().InstanceStatuses

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), // existing no-op
		failedStamp(1), // existing change
		// A declined mutation on a missing index must not append a slot.
		{Index: 7, Mutate: func(*workload.InstanceStatus) bool { return false }},
		// A changed mutation on a missing index must append exactly one slot.
		{Index: 2, Mutate: func(s *workload.InstanceStatus) bool {
			s.Incarnation = 1
			s.Phase = workload.InstancePhaseCreating
			return true
		}},
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(1), "a mixed batch with changes must still use one write")
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(3))
	wantUnchanged := instanceStatusAt(t, before, 0)
	clearPodDerivedStatusForTest(&wantUnchanged)
	g.Expect(instanceStatusAt(t, got, 0)).To(gomega.Equal(wantUnchanged),
		"an existing no-op slot keeps every retained field")
	wantChanged := instanceStatusAt(t, before, 1)
	wantChanged.Phase = v1beta1.OMENativeInstanceFailed
	clearPodDerivedStatusForTest(&wantChanged)
	g.Expect(instanceStatusAt(t, got, 1)).To(gomega.Equal(wantChanged))
	g.Expect(instanceStatusAt(t, got, 2)).To(gomega.Equal(v1beta1.OMENativeInstanceStatus{
		Index:       2,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
	}))
	for _, status := range got.Status.InstanceStatuses {
		g.Expect(status.Index).NotTo(gomega.Equal(int32(7)), "a declined missing-slot mutation must not append a phantom")
	}
	mirrored := ir.DeepCopy()
	irstatus.ClearPodDerivedObservations(mirrored.Status.InstanceStatuses)
	g.Expect(mirrored.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses),
		"the in-memory mirror may retain transient observations but must match every persisted field")
}

// TestBuildApplyInstanceMutations_NoChangeZeroWrites pins the no-op
// property: a batch whose mutations all report no change performs ZERO
// writes, and a probed-but-unchanged missing index is not appended as a
// phantom slot.
func TestBuildApplyInstanceMutations_NoChangeZeroWrites(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
	}
	c, writes := newCountingStatusClient(t, 0, ir)

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), // already Failed — reports no change
		{Index: 7, Mutate: func(s *workload.InstanceStatus) bool {
			// Missing-slot probe that declines: guard sentinel shape.
			return s.Phase != ""
		}},
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(0),
		"an all-no-op batch must perform ZERO status writes")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1),
		"a declined missing-index mutation must not append a phantom slot")
}

func TestBuildApplyInstanceMutations_NotFoundAbortsStaleReconcile(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, writes := newCountingStatusClient(t, 0) // IR never stored

	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)
	err := apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0),
	})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(0))
}

func TestBuildReconcileInput_AtomicInstanceAndRetryBlockMutationOneWrite(t *testing.T) {
	g := gomega.NewWithT(t)
	targetRevision := "rev-target"
	now := metav1.NewTime(time.Date(2026, time.March, 1, 10, 0, 0, 0, time.UTC))
	nextRetry := metav1.NewTime(now.Add(5 * time.Minute))
	firstFailure := metav1.NewTime(now.Add(-time.Hour))
	lastFailure := metav1.NewTime(now.Add(-5 * time.Minute))
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.UpdateRevision = targetRevision
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:           0,
		Phase:           v1beta1.OMENativeInstanceFailed,
		RunningRevision: "rev-old",
		Admitted:        true,
	}}
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{{
		TargetRevision:  targetRevision,
		State:           v1beta1.RetryBlockBackoff,
		AttemptsStarted: 2,
		NextRetryAt:     &nextRetry,
		FirstFailureAt:  &firstFailure,
		LastFailureAt:   &lastFailure,
		Reason:          "worker exited",
	}}
	c, writes := newCountingStatusClient(t, 0, ir)
	storedBefore := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), storedBefore)).To(gomega.Succeed())
	wantBlock := *storedBefore.Status.RetryBlocks[0].DeepCopy()
	wantBlock.State = v1beta1.RetryBlockRetryInProgress
	r := &Reconciler{Client: c, APIReader: c, InstanceStatusTarget: irstatus.EncodingDenseV1}
	input := r.buildReconcileInput(context.Background(), ir, nil, nil, nil, 0, 0, coordination.GroupDefaults{})
	g.Expect(input.ApplyInstanceMutationsWithRetryBlock).NotTo(gomega.BeNil(),
		"the production IR input must expose the atomic status capability")

	g.Expect(input.ApplyInstanceMutationsWithRetryBlock(
		context.Background(),
		[]workloadtypes.InstanceMutation{creatingStamp(0, now)},
		targetRevision,
		func(block *workload.RetryBlock) workload.RetryBlockDisposition {
			g.Expect(block.State).To(gomega.Equal(workload.RetryBlockBackoff))
			block.State = workload.RetryBlockRetryInProgress
			return workload.RetryBlockPersist
		},
	)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1),
		"the Creating stamp and retry authorization transition must share one status write")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	instance := instanceStatusAt(t, got, 0)
	g.Expect(instance.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
	g.Expect(instance.RunningRevision).To(gomega.Equal("rev-old"))
	g.Expect(instance.Admitted).To(gomega.BeTrue())
	g.Expect(instance.Operation).NotTo(gomega.BeNil())
	g.Expect(got.Status.RetryBlocks).To(gomega.Equal([]v1beta1.RetryBlock{wantBlock}),
		"the retry transition must preserve every untouched block field")
	mirrored := instanceStatusAt(t, ir, 0)
	g.Expect(mirrored.Phase).To(gomega.Equal(instance.Phase))
	g.Expect(mirrored.RunningRevision).To(gomega.Equal(instance.RunningRevision))
	g.Expect(mirrored.Admitted).To(gomega.Equal(instance.Admitted))
	g.Expect(mirrored.Operation).NotTo(gomega.BeNil())
	g.Expect(mirrored.Operation.StartedAt.Time.Equal(instance.Operation.StartedAt.Time)).To(gomega.BeTrue())
	g.Expect(mirrored.Operation.Deadline.Time.Equal(instance.Operation.Deadline.Time)).To(gomega.BeTrue())
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(got.Status.RetryBlocks))
}

func TestBuildApplyInstanceMutationsWithRetryBlock_AllNoOpZeroWrites(t *testing.T) {
	g := gomega.NewWithT(t)
	targetRevision := "rev-target"
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceFailed,
	}}
	c, writes := newCountingStatusClient(t, 0, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)

	g.Expect(apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{failedStamp(0)},
		targetRevision,
		func(block *workload.RetryBlock) workload.RetryBlockDisposition {
			g.Expect(block).To(gomega.Equal(&workload.RetryBlock{TargetRevision: targetRevision}),
				"a missing block must be presented as a revision-scoped zero value")
			return workload.RetryBlockUnchanged
		},
	)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0))

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal(ir.Status.InstanceStatuses))
	g.Expect(got.Status.RetryBlocks).To(gomega.BeEmpty(),
		"an unchanged missing block must not create a phantom entry")

	g.Expect(apply(context.Background(), nil, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0), "an empty optional mutation set must remain a zero-write no-op")
}

func TestBuildApplyInstanceMutationsWithRetryBlock_WriteFailureIsAtomic(t *testing.T) {
	g := gomega.NewWithT(t)
	targetRevision := "rev-target"
	ir := baselineIR("llama-engine", "prod", 1)
	originalInstance := fullyPopulatedInstanceStatus(0)
	originalBlock := v1beta1.RetryBlock{
		TargetRevision:  targetRevision,
		State:           v1beta1.RetryBlockBackoff,
		AttemptsStarted: 1,
		Reason:          "transient failure",
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{originalInstance}
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{originalBlock}
	c, writes := newFailingStatusClient(t, ir)
	storedBefore := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), storedBefore)).To(gomega.Succeed())
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)

	err := apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{failedStamp(0)},
		targetRevision,
		func(block *workload.RetryBlock) workload.RetryBlockDisposition {
			block.State = workload.RetryBlockRetryInProgress
			return workload.RetryBlockPersist
		},
	)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("injected status write failure")))
	g.Expect(*writes).To(gomega.Equal(1))

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal(storedBefore.Status.InstanceStatuses),
		"a failed combined write must persist no InstanceStatus change")
	g.Expect(got.Status.RetryBlocks).To(gomega.Equal(storedBefore.Status.RetryBlocks),
		"a failed combined write must persist no RetryBlock change")
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal([]v1beta1.OMENativeInstanceStatus{originalInstance}),
		"failed writes must not advance the in-memory instance mirror")
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal([]v1beta1.RetryBlock{originalBlock}),
		"failed writes must not advance the in-memory retry-block mirror")
}

func TestBuildApplyInstanceMutationsWithRetryBlock_ConflictRetriesBothMutations(t *testing.T) {
	g := gomega.NewWithT(t)
	targetRevision := "rev-target"
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceRestarting,
	}}
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{{
		TargetRevision:  targetRevision,
		State:           v1beta1.RetryBlockBackoff,
		AttemptsStarted: 3,
	}}
	c, writes := newCountingStatusClient(t, 1, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
	instanceCalls := 0
	retryBlockCalls := 0

	g.Expect(apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{{Index: 0, Mutate: func(status *workload.InstanceStatus) bool {
			instanceCalls++
			status.Phase = workload.InstancePhaseUpdating
			status.TargetRevision = targetRevision
			return true
		}}},
		targetRevision,
		func(block *workload.RetryBlock) workload.RetryBlockDisposition {
			retryBlockCalls++
			block.State = workload.RetryBlockRetryInProgress
			return workload.RetryBlockPersist
		},
	)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(2))
	g.Expect(instanceCalls).To(gomega.Equal(2), "the instance mutation must be reapplied to the fresh retry snapshot")
	g.Expect(retryBlockCalls).To(gomega.Equal(2), "the RetryBlock mutation must be reapplied to the fresh retry snapshot")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(instanceStatusAt(t, got, 0).Phase).To(gomega.Equal(v1beta1.OMENativeInstanceUpdating))
	g.Expect(instanceStatusAt(t, got, 0).TargetRevision).To(gomega.Equal(targetRevision))
	g.Expect(got.Status.RetryBlocks).To(gomega.HaveLen(1))
	g.Expect(got.Status.RetryBlocks[0].State).To(gomega.Equal(v1beta1.RetryBlockRetryInProgress))
	g.Expect(got.Status.RetryBlocks[0].AttemptsStarted).To(gomega.Equal(int32(3)))
}

func TestBuildApplyInstanceMutationsWithRetryBlock_MixedRestoreAndRemoveOneWrite(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)
	broken := fullyPopulatedInstanceStatus(0)
	broken.Phase = v1beta1.OMENativeInstanceFailed
	broken.Operation = nil
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		broken,
		fullyPopulatedInstanceStatus(1),
		fullyPopulatedInstanceStatus(2),
	}
	c, writes := newCountingStatusClient(t, 0, ir)
	storedBefore := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), storedBefore)).To(gomega.Succeed())
	restored := workload.InstanceStatus{
		Index:             0,
		Incarnation:       9,
		Phase:             workload.InstancePhaseReady,
		RunningRevision:   "rev-restored",
		PodCount:          8,
		ReadyPodCount:     8,
		ServingPodCount:   8,
		AvailablePodCount: 8,
		ScheduledPodCount: 8,
		Admitted:          true,
		NodesOccupied:     []string{"node-restored"},
	}
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		{Index: 0, Mutate: func(status *workload.InstanceStatus) bool {
			*status = restored
			return true
		}},
		{Index: 1, Remove: true},
		{Index: 99, Remove: true}, // Removing an absent slot is a no-op.
	}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1), "restore and removal must share one status write")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	wantRestored := v1beta1convert.InstanceStatusFromWorkload(restored)
	clearPodDerivedStatusForTest(&wantRestored)
	wantUntouched := storedBefore.Status.InstanceStatuses[2]
	clearPodDerivedStatusForTest(&wantUntouched)
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal([]v1beta1.OMENativeInstanceStatus{wantRestored, wantUntouched}))
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses),
		"a committed removal must replace the complete in-memory slice")
}

func TestBuildApplyInstanceMutationsWithRetryBlock_RemovalConflictRetriesWholeBatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
	}
	c, writes := newCountingStatusClient(t, 1, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
	restoreCalls := 0

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		{Index: 0, Mutate: func(status *workload.InstanceStatus) bool {
			restoreCalls++
			status.Phase = workload.InstancePhaseReady
			return true
		}},
		{Index: 1, Remove: true},
	}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(2))
	g.Expect(restoreCalls).To(gomega.Equal(2), "the complete mixed batch must be reapplied after conflict")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal([]v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceReady,
	}}))
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses))
}

func TestBuildApplyInstanceMutationsWithRetryBlock_PreconditionRejectsStaleRemoval(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:           0,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "revision-concurrent",
	}}
	c, writes := newCountingStatusClient(t, 0, ir)
	commitCalls := 0
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Precondition: func(status *workload.InstanceStatus) bool {
			return status.Phase == workload.InstancePhaseCreating
		},
		OnCommit: func(_, _ *workload.InstanceStatus) {
			commitCalls++
		},
	}}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0), "a rejected conditional removal must not write status")
	g.Expect(commitCalls).To(gomega.Equal(0), "a rejected mutation must not report a commit")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal(ir.Status.InstanceStatuses))
}

func TestBuildApplyInstanceMutationsWithRetryBlock_OwnerGoneSentinel(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		g := gomega.NewWithT(t)
		ir := baselineIR("llama-engine", "prod", 1)
		c, writes := newCountingStatusClient(t, 0)

		atomicApply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
		err := atomicApply(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
		g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
		g.Expect(*writes).To(gomega.Equal(0))

		instanceOnlyApply := buildApplyInstanceMutations(testStatusWriter(c), ir)
		err = instanceOnlyApply(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)})
		g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
		g.Expect(*writes).To(gomega.Equal(0))
	})

	t.Run("status update", func(t *testing.T) {
		g := gomega.NewWithT(t)
		ir := baselineIR("llama-engine", "prod", 1)
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
			Index: 0,
			Phase: v1beta1.OMENativeInstanceRestarting,
		}}
		original := ir.Status.DeepCopy()
		c, writes := newStatusOwnerGoneOnUpdateClient(t, ir)

		atomicApply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
		err := atomicApply(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
		g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
		g.Expect(*writes).To(gomega.Equal(1))
		g.Expect(ir.Status).To(gomega.Equal(*original), "an uncommitted write must not advance the in-memory mirror")

		instanceOnlyApply := buildApplyInstanceMutations(testStatusWriter(c), ir)
		err = instanceOnlyApply(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)})
		g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
		g.Expect(*writes).To(gomega.Equal(2))
	})
}

func TestBuildApplyInstanceMutationsWithRetryBlock_RejectsAmbiguousMutation(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, writes := newCountingStatusClient(t, 0, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Mutate: func(*workload.InstanceStatus) bool { return true },
	}}, "", nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("sets both Remove and Mutate")))

	err = apply(context.Background(), []workloadtypes.InstanceMutation{{Index: 0}}, "", nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("sets neither Remove nor Mutate")))

	err = apply(context.Background(), []workloadtypes.InstanceMutation{
		{
			Index:    0,
			Mutate:   func(*workload.InstanceStatus) bool { return true },
			OnCommit: func(*workload.InstanceStatus, *workload.InstanceStatus) {},
		},
		{Index: 0, Mutate: func(*workload.InstanceStatus) bool { return true }},
	}, "", nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("uses OnCommit but the index appears 2 times")))
	g.Expect(*writes).To(gomega.Equal(0), "invalid mutation shapes must fail before any status write")
}

func TestBuildApplyInstanceMutations_BatchPreconditionRejectsEverything(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady},
	}
	c, writes := newCountingStatusClient(t, 0, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
	guardCalls := 0
	err := apply(context.Background(), []workloadtypes.InstanceMutation{
		{
			Index: 0,
			BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
				guardCalls++
				return snapshot.OwnerUID == ir.UID &&
					snapshot.Instances[1].Incarnation == 99
			},
			Mutate: func(status *workload.InstanceStatus) bool {
				status.Phase = workload.InstancePhaseDeleting
				return true
			},
		},
		failedStamp(1),
	}, "", nil)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusMutationPrecondition)).To(gomega.BeTrue())
	g.Expect(guardCalls).To(gomega.Equal(1))
	g.Expect(*writes).To(gomega.Equal(0))

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal(ir.Status.InstanceStatuses))
}

func TestBuildApplyInstanceMutations_BatchPreconditionIncludesOwnerIdentity(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Generation = 17
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady,
	}}
	c, writes := newCountingStatusClient(t, 0, ir)
	var observed workloadtypes.InstanceMutationSnapshot

	err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		BatchPrecondition: func(snapshot workloadtypes.InstanceMutationSnapshot) bool {
			observed = snapshot
			return true
		},
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			status.Phase = workloadtypes.InstancePhaseDeleting
			return true
		},
	}}, "", nil)

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(observed.OwnerUID).To(gomega.Equal(ir.UID))
	g.Expect(observed.OwnerGeneration).To(gomega.Equal(ir.Generation))
	g.Expect(*writes).To(gomega.Equal(1))
}

func TestBuildApplyInstanceMutations_BatchPreconditionRecheckedAfterConflict(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	c, writes := newCountingStatusClient(t, 1, ir)
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)
	guardCalls := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			guardCalls++
			return snapshot.OwnerUID == ir.UID
		},
		Mutate: func(status *workload.InstanceStatus) bool {
			status.Phase = workload.InstancePhaseDeleting
			return true
		},
	}}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(2))
	g.Expect(guardCalls).To(gomega.Equal(2))
}

func TestBuildApplyInstanceMutations_GuardedRemovalRejectsOwnershipDriftAfterConflict(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 4, Phase: v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{ID: "gang-source", Type: v1beta1.InstanceOperationUpdate, Step: workloadops.UpdateStepSurgeDrain},
	}}
	c, writes := newCountingStatusClient(t, 1, ir)
	reads := &driftingInstanceReader{Reader: c, index: 0}
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), reads, ir)
	metricBefore := make(map[string]float64)
	for _, result := range []string{obsmetrics.ResultSuccess, obsmetrics.ResultConflict, obsmetrics.ResultNotFound, obsmetrics.ResultError} {
		metricBefore[result] = irStatusUpdateMetric(t, result)
	}
	commits := 0
	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			status, found := snapshot.Instances[0]
			return found && snapshot.OwnerUID == ir.UID && status.Incarnation == 4 &&
				status.Operation != nil && status.Operation.ID == "gang-source" &&
				status.Operation.Type == workload.InstanceOperationUpdate
		},
		OnCommit: func(*workload.InstanceStatus, *workload.InstanceStatus) { commits++ },
	}}, "", nil)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusMutationPrecondition)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(1), "the retry must stop before a second status write")
	g.Expect(reads.reads).To(gomega.Equal(3), "conflict confirmation plus the retry must use authoritative snapshots")
	g.Expect(commits).To(gomega.Equal(0))
	for result, before := range metricBefore {
		g.Expect(irStatusUpdateMetric(t, result)).To(gomega.Equal(before),
			"a rejected replan must not report a terminal status-write outcome")
	}

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1), "ownership drift must retain the incumbent status")
}

func TestBuildApplyInstanceMutations_AmbiguousCommitConfirmedAuthoritatively(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Incarnation: 4, Phase: v1beta1.OMENativeInstanceReady}}
	c, writes := newCommitThenErrorStatusClient(t, ir)
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir)
	now := metav1.NewTime(time.Date(2026, 4, 5, 6, 7, 8, 987654321, time.UTC))
	commits := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workload.InstanceStatus) bool {
			status.Phase = workload.InstancePhaseDeleting
			status.Operation = &workload.InstanceOperation{
				ID: "delete-0", Type: workload.InstanceOperationDelete, Step: "Drain", StartedAt: now,
			}
			return true
		},
		Postcondition: func(status *workload.InstanceStatus) bool {
			return status != nil && status.Phase == workload.InstancePhaseDeleting && status.Operation != nil && status.Operation.ID == "delete-0"
		},
		OnCommit: func(_, current *workload.InstanceStatus) {
			commits++
			g.Expect(current).NotTo(gomega.BeNil())
			g.Expect(current.Operation.StartedAt.Nanosecond()).To(gomega.Equal(0), "callback must receive the wire-normalized value")
		},
	}}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1))
	g.Expect(commits).To(gomega.Equal(1))
	g.Expect(ir.Status.InstanceStatuses[0].Operation.StartedAt.Nanosecond()).To(gomega.Equal(0))
}

func TestBuildApplyInstanceMutations_AmbiguousRemovalForgetsOnlyAfterConfirmedAbsence(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 4, Phase: v1beta1.OMENativeInstanceDeleting,
		Operation: &v1beta1.InstanceOperation{ID: "delete-0", Type: v1beta1.InstanceOperationDelete},
	}}
	c, writes := newCommitThenErrorStatusClient(t, ir)
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir)
	commits := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			status, found := snapshot.Instances[0]
			return found && snapshot.OwnerUID == ir.UID && status.Incarnation == 4 &&
				status.Operation != nil && status.Operation.ID == "delete-0"
		},
		OnCommit: func(previous, current *workload.InstanceStatus) {
			commits++
			g.Expect(previous).NotTo(gomega.BeNil())
			g.Expect(current).To(gomega.BeNil())
		},
	}}, "", nil)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(1))
	g.Expect(commits).To(gomega.Equal(1))
	g.Expect(ir.Status.InstanceStatuses).To(gomega.BeEmpty())
}

func TestBuildApplyInstanceMutations_AmbiguousCombinedCommitConfirmed(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating}}
	c, writes := newCommitThenErrorStatusClient(t, ir)
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir)
	commits := 0

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workload.InstanceStatus) bool {
			status.Phase = workload.InstancePhaseReady
			return true
		},
		Postcondition: func(status *workload.InstanceStatus) bool {
			return status != nil && status.Phase == workload.InstancePhaseReady
		},
		OnCommit: func(*workload.InstanceStatus, *workload.InstanceStatus) { commits++ },
	}}, "revision-b", func(block *workload.RetryBlock) workload.RetryBlockDisposition {
		block.State = workload.RetryBlockHeld
		block.AttemptsStarted = 2
		return workload.RetryBlockPersist
	})

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*writes).To(gomega.Equal(1))
	g.Expect(commits).To(gomega.Equal(1))
	g.Expect(ir.Status.RetryBlocks).To(gomega.ConsistOf(v1beta1.RetryBlock{
		TargetRevision:  "revision-b",
		State:           v1beta1.RetryBlockHeld,
		AttemptsStarted: 2,
	}))
}

func TestBuildApplyInstanceMutations_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	c, writes := newCountingStatusClient(t, 0, replacement)
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, original)

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workload.InstanceStatus) bool {
			status.Phase = workload.InstancePhaseDeleting
			return true
		},
	}}, "", nil)

	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.BeZero())
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(replacement), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady))
}

func TestBuildMutateInstance_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	r, c := newReconciler(t, replacement)
	called := false

	mutate := buildMutateInstance(r.statusWriter(), r.Client, original)
	err := mutate(context.Background(), 0, func(*workload.InstanceStatus) bool {
		called = true
		return true
	})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(called).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(replacement), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady))
}

func TestBuildRemoveInstance_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	r, c := newReconciler(t, replacement)

	remove := buildRemoveInstance(r.statusWriter(), r.Client, original, workload.NewExpectations())
	removed, err := remove(context.Background(), 0)
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(removed).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(replacement), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.Equal(replacement.Status.InstanceStatuses))
}

func TestBuildWriteAggregateCondition_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	r, c := newReconciler(t, replacement)

	write := buildWriteAggregateCondition(r.statusWriter(), r.Client, original)
	err := write(context.Background(), metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready"})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(replacement), got)).To(gomega.Succeed())
	g.Expect(got.Status.Conditions).To(gomega.BeEmpty())
}

func TestBuildApplyInstanceMutations_RecordsOneTerminalStatusOutcome(t *testing.T) {
	t.Run("conflict then success records success once", func(t *testing.T) {
		ir := baselineIR("metric-success", "prod", 1)
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		c, writes := newCountingStatusClient(t, 1, ir)
		beforeSuccess := irStatusUpdateMetric(t, obsmetrics.ResultSuccess)
		beforeConflict := irStatusUpdateMetric(t, obsmetrics.ResultConflict)

		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if *writes != 2 {
			t.Fatalf("status attempts = %d, want 2", *writes)
		}
		if got := irStatusUpdateMetric(t, obsmetrics.ResultSuccess) - beforeSuccess; got != 1 {
			t.Fatalf("success metric delta = %g, want 1", got)
		}
		if got := irStatusUpdateMetric(t, obsmetrics.ResultConflict) - beforeConflict; got != 0 {
			t.Fatalf("terminal conflict metric delta = %g, want 0", got)
		}
	})

	t.Run("terminal error records error once", func(t *testing.T) {
		ir := baselineIR("metric-error", "prod", 1)
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		c, _ := newFailingStatusClient(t, ir)
		before := irStatusUpdateMetric(t, obsmetrics.ResultError)
		if err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil); err == nil {
			t.Fatal("expected status failure")
		}
		if got := irStatusUpdateMetric(t, obsmetrics.ResultError) - before; got != 1 {
			t.Fatalf("error metric delta = %g, want 1", got)
		}
	})

	t.Run("owner disappearance records notfound once", func(t *testing.T) {
		ir := baselineIR("metric-notfound", "prod", 1)
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		c, _ := newStatusOwnerGoneOnUpdateClient(t, ir)
		before := irStatusUpdateMetric(t, obsmetrics.ResultNotFound)
		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
		if !errors.Is(err, workloadtypes.ErrStatusOwnerGone) {
			t.Fatalf("owner-gone error = %v", err)
		}
		if got := irStatusUpdateMetric(t, obsmetrics.ResultNotFound) - before; got != 1 {
			t.Fatalf("notfound metric delta = %g, want 1", got)
		}
	})

	t.Run("no-op records no outcome", func(t *testing.T) {
		ir := baselineIR("metric-noop", "prod", 1)
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		c, writes := newCountingStatusClient(t, 0, ir)
		before := map[string]float64{}
		for _, result := range []string{obsmetrics.ResultSuccess, obsmetrics.ResultConflict, obsmetrics.ResultNotFound, obsmetrics.ResultError} {
			before[result] = irStatusUpdateMetric(t, result)
		}
		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir)(context.Background(), []workloadtypes.InstanceMutation{{
			Index: 0, Mutate: func(*workload.InstanceStatus) bool { return false },
		}}, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if *writes != 0 {
			t.Fatalf("no-op status writes = %d, want 0", *writes)
		}
		for result, value := range before {
			if got := irStatusUpdateMetric(t, result); got != value {
				t.Fatalf("%s metric changed on no-op: got %g want %g", result, got, value)
			}
		}
	})
}

func irStatusUpdateMetric(t *testing.T, result string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "ome_isvc_status_update_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricLabelsMatch(metric, map[string]string{"controller": obsmetrics.ControllerIR, "result": result}) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func metricLabelsMatch(metric *dto.Metric, expected map[string]string) bool {
	if len(metric.GetLabel()) != len(expected) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if expected[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}
