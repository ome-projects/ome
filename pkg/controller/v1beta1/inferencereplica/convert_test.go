package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// buildRemoveInstance must Forget the SAME expectations bucket the
// workload ops populate: Key.OwnerName is the parent ISVC name
// (buildKey), not the IR name. Keyed on the IR name the Forget deletes
// nothing, so a reused index inherits stale counters until the TTL.
func TestBuildRemoveInstance_ForgetsParentKeyedExpectations(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()

	exp := workloadtypes.NewExpectations()
	key := buildKey(ir)
	exp.ExpectCreates(key.Namespace, key.OwnerName, key.Component, 0, 1)
	g.Expect(exp.Satisfied(key.Namespace, key.OwnerName, key.Component, 0)).To(gomega.BeFalse(),
		"an in-flight create must block the bucket before removal")

	removed, err := buildRemoveInstance(testStatusWriter(c), c, ir, exp)(context.Background(), 0)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(removed).To(gomega.BeTrue())

	g.Expect(exp.Satisfied(key.Namespace, key.OwnerName, key.Component, 0)).To(gomega.BeTrue(),
		"Forget must clear the bucket ExpectCreates populated (OwnerName = parent ISVC name)")
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
	return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workloadtypes.InstanceStatus) bool {
		if s.Phase == workloadtypes.InstancePhaseFailed {
			return false
		}
		s.Phase = workloadtypes.InstancePhaseFailed
		return true
	}}
}

func creatingStamp(idx int32, now metav1.Time) workloadtypes.InstanceMutation {
	return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workloadtypes.InstanceStatus) bool {
		s.Incarnation = 1
		s.Phase = workloadtypes.InstancePhaseCreating
		s.Operation = &workloadtypes.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           workloadtypes.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		}
		return true
	}}
}

func clearPodDerivedStatusForTest(status *v1beta1.OMENativeInstanceStatus) {
	status.ReadyPodCount = 0
	status.ScheduledPodCount = 0
	status.NodesOccupied = nil
}

// buildApplyInstanceMutations adapter contract: the batched sibling of
// buildMutateInstance — one fresh Get, every mutation applied, ONE
// Status().Update, retry-on-conflict as a whole, zero writes when no
// mutation reports a change, an owner-gone signal for guarded effects, and
// committed slots mirrored onto the caller's in-memory IR.

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
		{Index: 3, Mutate: func(s *workloadtypes.InstanceStatus) bool {
			s.PodCount += 2
			s.Operation.RetryCount++
			return true
		}},
		{Index: 3, Mutate: func(s *workloadtypes.InstanceStatus) bool {
			secondObservedFirst = s.PodCount == original.PodCount+2 &&
				s.Operation != nil && s.Operation.RetryCount == original.Operation.RetryCount+1
			s.ReadyPodCount = s.PodCount
			s.Phase = workloadtypes.InstancePhaseReady
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
		return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workloadtypes.InstanceStatus) bool {
			callbackCalls[idx]++
			s.Phase = workloadtypes.InstancePhaseFailed
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
	var previous, current *workloadtypes.InstanceStatus
	mutationCalls := 0
	apply := buildApplyInstanceMutations(testStatusWriter(c), ir)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			mutationCalls++
			status.Phase = workloadtypes.InstancePhaseFailed
			status.Operation = nil
			return true
		},
		OnCommit: func(before, after *workloadtypes.InstanceStatus) {
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
	g.Expect(current.Phase).To(gomega.Equal(workloadtypes.InstancePhaseFailed))
	g.Expect(current.Operation).To(gomega.BeNil())
}

func TestBuildApplyInstanceMutations_OnCommitUsesPersistedRepresentation(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, writes := newWireNormalizingStatusClient(t, ir)
	now := metav1.NewTime(time.Date(2026, time.August, 14, 12, 0, 0, 123456789, time.UTC))
	var committed *workloadtypes.InstanceStatus
	mutation := creatingStamp(0, now)
	mutation.OnCommit = func(_, current *workloadtypes.InstanceStatus) {
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
		Precondition: func(status *workloadtypes.InstanceStatus) bool {
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
	var committed *workloadtypes.InstanceStatus
	mutation := creatingStamp(0, now)
	mutation.OnCommit = func(_, current *workloadtypes.InstanceStatus) {
		committed = current
	}

	apply := instanceOnlyMutationAdapter(
		buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(writer), live, ir, nil),
	)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{mutation})).To(gomega.Succeed())
	g.Expect(committed).NotTo(gomega.BeNil())
	wantRollbackValue := *committed
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Precondition: func(status *workloadtypes.InstanceStatus) bool {
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
		Expectations:         workloadtypes.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
	}
	input := r.buildReconcileInput(context.Background(), ir, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	plan, err := workload.BuildPlan(input.Key.Component, input.DesiredSpec, input.ObservedState)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	createErr := errors.New("pod create unavailable")
	podClient := &indexedPodCreateFailureClient{Client: c, failIndex: 1, err: createErr}
	_, err = workloadops.Create(context.Background(), workloadtypes.Deps{
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
		{Index: 7, Mutate: func(*workloadtypes.InstanceStatus) bool { return false }},
		// A changed mutation on a missing index must append exactly one slot.
		{Index: 2, Mutate: func(s *workloadtypes.InstanceStatus) bool {
			s.Incarnation = 1
			s.Phase = workloadtypes.InstancePhaseCreating
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
		{Index: 7, Mutate: func(s *workloadtypes.InstanceStatus) bool {
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
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
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
	input := r.buildReconcileInput(context.Background(), ir, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.ApplyInstanceMutationsWithRetryBlock).NotTo(gomega.BeNil(),
		"the production IR input must expose the atomic status capability")

	g.Expect(input.ApplyInstanceMutationsWithRetryBlock(
		context.Background(),
		[]workloadtypes.InstanceMutation{creatingStamp(0, now)},
		targetRevision,
		func(block *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
			g.Expect(block.State).To(gomega.Equal(workloadtypes.RetryBlockBackoff))
			block.State = workloadtypes.RetryBlockRetryInProgress
			return workloadtypes.RetryBlockPersist
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)

	g.Expect(apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{failedStamp(0)},
		targetRevision,
		func(block *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
			g.Expect(block).To(gomega.Equal(&workloadtypes.RetryBlock{TargetRevision: targetRevision}),
				"a missing block must be presented as a revision-scoped zero value")
			return workloadtypes.RetryBlockUnchanged
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)

	err := apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{failedStamp(0)},
		targetRevision,
		func(block *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
			block.State = workloadtypes.RetryBlockRetryInProgress
			return workloadtypes.RetryBlockPersist
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
	instanceCalls := 0
	retryBlockCalls := 0

	g.Expect(apply(
		context.Background(),
		[]workloadtypes.InstanceMutation{{Index: 0, Mutate: func(status *workloadtypes.InstanceStatus) bool {
			instanceCalls++
			status.Phase = workloadtypes.InstancePhaseUpdating
			status.TargetRevision = targetRevision
			return true
		}}},
		targetRevision,
		func(block *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
			retryBlockCalls++
			block.State = workloadtypes.RetryBlockRetryInProgress
			return workloadtypes.RetryBlockPersist
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
	restored := workloadtypes.InstanceStatus{
		Index:             0,
		Incarnation:       9,
		Phase:             workloadtypes.InstancePhaseReady,
		RunningRevision:   "rev-restored",
		PodCount:          8,
		ReadyPodCount:     8,
		ServingPodCount:   8,
		AvailablePodCount: 8,
		ScheduledPodCount: 8,
		Admitted:          true,
		NodesOccupied:     []string{"node-restored"},
	}
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		{Index: 0, Mutate: func(status *workloadtypes.InstanceStatus) bool {
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
	restoreCalls := 0

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		{Index: 0, Mutate: func(status *workloadtypes.InstanceStatus) bool {
			restoreCalls++
			status.Phase = workloadtypes.InstancePhaseReady
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)

	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Precondition: func(status *workloadtypes.InstanceStatus) bool {
			return status.Phase == workloadtypes.InstancePhaseCreating
		},
		OnCommit: func(_, _ *workloadtypes.InstanceStatus) {
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

		atomicApply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
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

		atomicApply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		Mutate: func(*workloadtypes.InstanceStatus) bool { return true },
	}}, "", nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("sets both Remove and Mutate")))

	err = apply(context.Background(), []workloadtypes.InstanceMutation{{Index: 0}}, "", nil)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("sets neither Remove nor Mutate")))

	err = apply(context.Background(), []workloadtypes.InstanceMutation{
		{
			Index:    0,
			Mutate:   func(*workloadtypes.InstanceStatus) bool { return true },
			OnCommit: func(*workloadtypes.InstanceStatus, *workloadtypes.InstanceStatus) {},
		},
		{Index: 0, Mutate: func(*workloadtypes.InstanceStatus) bool { return true }},
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
	guardCalls := 0
	err := apply(context.Background(), []workloadtypes.InstanceMutation{
		{
			Index: 0,
			BatchPrecondition: func(snapshot workloadtypes.InstanceMutationSnapshot) bool {
				guardCalls++
				return snapshot.OwnerUID == ir.UID &&
					snapshot.Instances[1].Incarnation == 99
			},
			Mutate: func(status *workloadtypes.InstanceStatus) bool {
				status.Phase = workloadtypes.InstancePhaseDeleting
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

	err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)(context.Background(), []workloadtypes.InstanceMutation{{
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
	apply := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)
	guardCalls := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		BatchPrecondition: func(snapshot workloadtypes.InstanceMutationSnapshot) bool {
			guardCalls++
			return snapshot.OwnerUID == ir.UID
		},
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			status.Phase = workloadtypes.InstancePhaseDeleting
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
		Operation: &v1beta1.InstanceOperation{ID: "gang-source", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurgeDrain},
	}}
	c, writes := newCountingStatusClient(t, 1, ir)
	reads := &driftingInstanceReader{Reader: c, index: 0}
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), reads, ir, nil)
	metricBefore := make(map[string]float64)
	for _, result := range []string{obsmetrics.ResultSuccess, obsmetrics.ResultConflict, obsmetrics.ResultNotFound, obsmetrics.ResultError} {
		metricBefore[result] = irStatusUpdateMetric(t, result)
	}
	commits := 0
	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		BatchPrecondition: func(snapshot workloadtypes.InstanceMutationSnapshot) bool {
			status, found := snapshot.Instances[0]
			return found && snapshot.OwnerUID == ir.UID && status.Incarnation == 4 &&
				status.Operation != nil && status.Operation.ID == "gang-source" &&
				status.Operation.Type == workloadtypes.InstanceOperationUpdate
		},
		OnCommit: func(*workloadtypes.InstanceStatus, *workloadtypes.InstanceStatus) { commits++ },
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
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir, nil)
	now := metav1.NewTime(time.Date(2026, 4, 5, 6, 7, 8, 987654321, time.UTC))
	commits := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			status.Phase = workloadtypes.InstancePhaseDeleting
			status.Operation = &workloadtypes.InstanceOperation{
				ID: "delete-0", Type: workloadtypes.InstanceOperationDelete, Step: "Drain", StartedAt: now,
			}
			return true
		},
		Postcondition: func(status *workloadtypes.InstanceStatus) bool {
			return status != nil && status.Phase == workloadtypes.InstancePhaseDeleting && status.Operation != nil && status.Operation.ID == "delete-0"
		},
		OnCommit: func(_, current *workloadtypes.InstanceStatus) {
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
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir, nil)
	commits := 0
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index:  0,
		Remove: true,
		BatchPrecondition: func(snapshot workloadtypes.InstanceMutationSnapshot) bool {
			status, found := snapshot.Instances[0]
			return found && snapshot.OwnerUID == ir.UID && status.Incarnation == 4 &&
				status.Operation != nil && status.Operation.ID == "delete-0"
		},
		OnCommit: func(previous, current *workloadtypes.InstanceStatus) {
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
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, ir, nil)
	commits := 0

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			status.Phase = workloadtypes.InstancePhaseReady
			return true
		},
		Postcondition: func(status *workloadtypes.InstanceStatus) bool {
			return status != nil && status.Phase == workloadtypes.InstancePhaseReady
		},
		OnCommit: func(*workloadtypes.InstanceStatus, *workloadtypes.InstanceStatus) { commits++ },
	}}, "revision-b", func(block *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		block.State = workloadtypes.RetryBlockHeld
		block.AttemptsStarted = 2
		return workloadtypes.RetryBlockPersist
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
	apply := buildApplyInstanceMutationsWithRetryBlockFromReader(testStatusWriter(c), c, original, nil)

	err := apply(context.Background(), []workloadtypes.InstanceMutation{{
		Index: 0,
		Mutate: func(status *workloadtypes.InstanceStatus) bool {
			status.Phase = workloadtypes.InstancePhaseDeleting
			return true
		},
	}}, "", nil)

	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
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
	err := mutate(context.Background(), 0, func(*workloadtypes.InstanceStatus) bool {
		called = true
		return true
	})
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
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

	remove := buildRemoveInstance(r.statusWriter(), r.Client, original, workloadtypes.NewExpectations())
	removed, err := remove(context.Background(), 0)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
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
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())

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

		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
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
		if err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil); err == nil {
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
		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)(context.Background(), []workloadtypes.InstanceMutation{failedStamp(0)}, "", nil)
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
		err := buildApplyInstanceMutationsWithRetryBlock(testStatusWriter(c), ir, nil)(context.Background(), []workloadtypes.InstanceMutation{{
			Index: 0, Mutate: func(*workloadtypes.InstanceStatus) bool { return false },
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

// Every conflict-retry closure must re-read its base through the
// authoritative reader. Re-reading the informer cache cannot converge: a 409
// means the apiserver already holds a ResourceVersion the cache has not
// observed, so each attempt resubmits the same stale base until the backoff
// is spent.
func TestRetryClosuresReReadThroughLiveReader(t *testing.T) {
	scheme := testScheme(t)
	ctx := context.Background()

	newFixture := func(t *testing.T) (*Reconciler, *countingReader, *v1beta1.InferenceReplica) {
		t.Helper()
		ir := baselineIR("llama-engine", "default", 1)
		cached := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		live := &countingReader{Reader: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()}
		return &Reconciler{
			Client:               cached,
			APIReader:            live,
			Log:                  ctrl.Log.WithName("test"),
			Expectations:         workloadtypes.NewExpectations(),
			InstanceStatusTarget: irstatus.EncodingDenseV1,
		}, live, ir
	}

	t.Run("MutateInstance", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildMutateInstance(r.statusWriter(), r.APIReader, ir)(ctx, 0, func(s *workloadtypes.InstanceStatus) bool {
			s.Phase = workloadtypes.InstancePhaseReady
			return true
		})
		if err != nil {
			t.Fatalf("mutate instance: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("PromoteCurrentRevision", func(t *testing.T) {
		r, live, ir := newFixture(t)
		if err := buildPromoteCurrentRevision(r.statusWriter(), r.APIReader, ir)(ctx, "llama-engine-abc123"); err != nil {
			t.Fatalf("promote: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("WriteAggregateCondition", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildWriteAggregateCondition(r.statusWriter(), r.APIReader, ir)(ctx, metav1.Condition{
			Type:    InferenceReplicaConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  ReasonAllInstancesReady,
			Message: "ready",
		})
		if err != nil {
			t.Fatalf("write aggregate condition: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("MutateRetryBlock", func(t *testing.T) {
		r, live, ir := newFixture(t)
		err := buildMutateRetryBlock(r.statusWriter(), r.APIReader, ir, nil)(ctx, "llama-engine-abc123",
			func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
				return workloadtypes.RetryBlockRemove
			})
		if err != nil {
			t.Fatalf("mutate retry block: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})

	t.Run("RemoveInstance", func(t *testing.T) {
		r, live, ir := newFixture(t)
		if _, err := buildRemoveInstance(r.statusWriter(), r.APIReader, ir, r.Expectations)(ctx, 0); err != nil {
			t.Fatalf("remove instance: %v", err)
		}
		if live.gets == 0 {
			t.Error("re-read must go through the live reader, not the cache")
		}
	})
}

// pinRun pins the ISVC's current spec.rollout as its active run: the update
// gates fail closed for grouped Components without one, so every gate fixture
// models the production state where the run layer has already pinned.
func pinRun(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	if isvc.Spec.Rollout == nil {
		return isvc
	}
	groups := make([]v1beta1.RolloutRunGroup, 0, len(isvc.Spec.Rollout.Groups))
	for i := range isvc.Spec.Rollout.Groups {
		groups = append(groups, v1beta1.RolloutRunGroup{
			Source: v1beta1.RolloutPlanSourceInline,
			Group:  *isvc.Spec.Rollout.Groups[i].DeepCopy(),
		})
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    "test",
		OpenedAt: metav1.Now(),
		Plan:     v1beta1.RolloutRunPlan{Groups: groups},
	}}
	return isvc
}

// mkSequentialParent returns an ISVC named "llama" (the parent every
// baselineIR points at) declaring a Sequential rollout over [decoder,
// engine] in that order. In the v2 rollout API Sequential is spelled as a run
// of single-Component blueGreen groups, in list order; the controller's
// collapseSequential folds them back into one Sequential group whose Order
// is the list order (decoder→engine). Generation is 2; callers stamp
// each IR's parent-generation annotation and status to model in-flight
// vs converged (stamp ≠ 2 ⇒ projection lag, the moment-of-bump signal
// observeSequentialComponentsForGate keys on).
func mkSequentialParent() *v1beta1.InferenceService {
	return pinRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 2},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	})
}

// mkIR creates an InferenceReplica for the given component of parent "llama".
// Callers stamp status fields to set up test scenarios.
func mkIR(c v1beta1.ComponentType, replicas int32) *v1beta1.InferenceReplica {
	return baselineIR("llama-"+string(c), "default", replicas)
}

// setParentGenStamp models the projector's parent-generation annotation
// on a fake IR — the stamp the Sequential gate's projection-lag signal
// reads.
func setParentGenStamp(ir *v1beta1.InferenceReplica, gen int64) {
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(gen, 10)
}

func setObservedGen(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.ObservedGeneration = gen
	isvc.Status.Components[c] = cs
}

// TestBuildReconcileInput_WiresSequentialGate pins that the IR-managed
// path wires ReconcileInput.UpdateGate: with the gate nil the dispatcher
// skips CheckSequential entirely and both Components of a Sequential
// group roll concurrently (the engine starts before the decoder
// finishes). The gate must be wired AND deny the engine while the
// decoder (first in Order) is in flight.
func TestBuildReconcileInput_WiresSequentialGate(t *testing.T) {
	g := gomega.NewWithT(t)
	// Moment-of-bump: both Components' parent-generation stamps lag
	// isvc.Generation=2 (the projector hasn't re-applied them yet) ⇒
	// both in flight. The active selector picks decoder (first in
	// Order); the engine must wait.
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Generation = 1
	decoderIR.Status.ObservedGeneration = 1
	setParentGenStamp(decoderIR, 1)
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Generation = 1
	engineIR.Status.ObservedGeneration = 1
	setParentGenStamp(engineIR, 1)
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil(),
		"UpdateGate must be wired on the IR path when the parent declares coordination")

	allowed, gate, reason := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be denied while decoder (first in Sequential Order) is in flight")
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
	g.Expect(gate).To(gomega.Equal(workloadtypes.RolloutHoldGateSequential),
		"a Sequential denial must report gate=Sequential so the RolloutHold surface names the right layer")
}

// TestBuildReconcileInput_SequentialReleasesActiveComponent proves the
// gate is not a blanket block: once the decoder has converged, the engine
// becomes the active Sequential Component and is allowed to roll.
func TestBuildReconcileInput_SequentialReleasesActiveComponent(t *testing.T) {
	g := gomega.NewWithT(t)
	// Decoder converged: its parent-generation stamp matches parent
	// Generation=2 and its status has caught up to its own IR generation
	// ⇒ not in flight. Engine's projected spec just changed (IR
	// generation 2, status still at 1) ⇒ it is the active Sequential
	// Component. Seed BOTH IRs so the gate reads their fresh status
	// (the group is not idle — engine is genuinely in flight — so this exercises
	// the active-component release path, not the "Sequential group idle" bypass).
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Generation = 1
	decoderIR.Status.ObservedGeneration = 1
	setParentGenStamp(decoderIR, 2)
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Generation = 2
	engineIR.Status.ObservedGeneration = 1
	setParentGenStamp(engineIR, 2)
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, _ := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine is the active Sequential Component once decoder converged; it must be allowed")
}

// TestBuildReconcileInput_NilParentLeavesGateNil pins that without a
// resolvable parent there is no RolloutCoordination block to enforce, so
// the gate stays nil and the dispatcher's "always allowed" fallback
// applies (matching the documented EventTarget fallback behavior).
func TestBuildReconcileInput_NilParentLeavesGateNil(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	input := r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).To(gomega.BeNil(),
		"no parent ⇒ no coordination to enforce ⇒ gate stays nil")
}

// TestBuildReconcileInput_WiresMigrationWhenParentSet pins the migration
// wiring: the IR-managed path always wires MutateMigration (the executor's
// status.migrations write-back seam) and mirrors the persisted records
// onto ObservedState.Migrations (the dispatcher's work source); with a
// resolvable parent it points the migration audit ledger at the parent
// ISVC via LedgerOwner, while the IR still owns the pods.
func TestBuildReconcileInput_WiresMigrationWhenParentSet(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)
	engineIR.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID: "u-1", Trigger: v1beta1.MigrationTriggerManual,
		Phase: v1beta1.MigrationPhaseAccepted, SourceInstance: 0,
	}}

	parent := mkSequentialParent() // any non-nil ISVC named "llama"
	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})

	g.Expect(input.MutateMigration).NotTo(gomega.BeNil(),
		"MutateMigration must be wired on the IR path")
	g.Expect(input.AppendMigration).NotTo(gomega.BeNil(),
		"AppendMigration must be wired on the IR path (the disposition's Auto mirror)")
	g.Expect(input.ObservedState.Migrations).To(gomega.HaveLen(1),
		"status.migrations must mirror onto ObservedState.Migrations")
	g.Expect(input.ObservedState.Migrations[0].RequestUUID).To(gomega.Equal("u-1"))
	g.Expect(input.LedgerOwner).NotTo(gomega.BeNil(),
		"the migration ledger must be owned by the parent ISVC, not the IR")
	g.Expect(input.LedgerOwner.GetName()).To(gomega.Equal(parent.Name))
	g.Expect(input.LedgerOwnerGVK).To(gomega.Equal(isvcGVK))

	// Nil parent ⇒ ledger falls back to the IR (workload-side owner
	// resolution); the migration seam stays wired.
	nilInput := r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(nilInput.MutateMigration).NotTo(gomega.BeNil())
	g.Expect(nilInput.AppendMigration).NotTo(gomega.BeNil())
	g.Expect(nilInput.LedgerOwner).To(gomega.BeNil())
}

// mkRatioParent returns an ISVC named "llama" declaring a RatioBalanced
// (MaintainRatio) rollingUpdate group over [engine, decoder] anchored at a
// symmetric 4:4 original ratio. The per-Component projected Status reflects
// whatever the caller stamps — modeling the lagged irprojector rollup the
// gate reads for PEER Components.
func mkRatioParent(tol int32, engServing, decServing int32) *v1beta1.InferenceService {
	return pinRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: engServing}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: decServing}},
			},
		},
	})
}

// peerIR returns a sibling IR (e.g. the decoder) with the given fresh
// serving count stamped on its status — the authoritative count the
// gate should read for a PEER Component, not the parent's lagged
// projection.
func peerIR(component v1beta1.ComponentType, replicas, serving int32) *v1beta1.InferenceReplica {
	engineIR := baselineIR("llama-"+string(component), "default", replicas)
	engineIR.Spec.Component = component
	engineIR.Status.Replicas = replicas
	engineIR.Status.ServingReplicas = serving
	return engineIR
}

// TestBuildReconcileInput_GateReadsFreshPeerStatus is the regression guard
// for the cross-Component coordination stale-status race. The gate the IR
// path wires reads the GATED Component's counts from the IR's own fresh
// status, but it must ALSO read every PEER Component's counts from that
// peer's fresh IR status — not the parent ISVC's lagged projection.
//
// Scenario (RatioBalanced 4:4, tol 25% → band [0.75, 1.25]): the engine is
// at full serving (4/4) and asks to surge one new-revision pod. The decoder
// is actually mid-roll with one pod already out of rotation (fresh decoder
// IR: serving 3/4), but the parent ISVC's projected status still reports the
// decoder fully serving (4/4) because the irprojector rollup lags.
//
//   - Reading the STALE projected decoder (4/4): engine surge projects
//     5/4 = 1.25, inside the strict band → engine ALLOWED to run ahead.
//   - Reading the FRESH decoder (3/4): engine surge projects 5/3 = 1.667,
//     past the band; the surge tiebreaker also refuses (the baseline 4/3 is
//     already out of band) → engine correctly DENIED until the decoder
//     catches up.
//
// A gate reading the stale projection would let the engine outrun the
// decoder past the RatioBalanced tolerance; the gate therefore overlays
// each peer's fresh IR status onto its view, so the engine is held.
func TestBuildReconcileInput_GateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is genuinely behind: fresh IR serving 3/4 (one pod out).
	decoder := peerIR(v1beta1.DecoderComponent, 4, 3)

	// Engine IR being reconciled: at full serving 4/4, about to surge. Seed it
	// into the client too, so the gate reads the engine's OWN fresh status (not
	// nil) and the denial is genuinely driven by the decoder's fresh 3/4.
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is STALE: it still reports the decoder fully
	// serving (4/4), which is the lagged irprojector rollup.
	parent := mkRatioParent(25, 4, 4)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, gate, reason := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be held while the decoder is genuinely behind (fresh decoder serving 3/4 → "+
			"engine surge 5/3 = 1.667 out of band); got allowed — the gate read the stale parent "+
			"projection (decoder 4/4) instead of the decoder's fresh IR status: "+reason)
	g.Expect(gate).To(gomega.Equal(workloadtypes.RolloutHoldGateRatio),
		"a RatioBalanced denial must report gate=Ratio so the RolloutHold surface names the right layer")
}

// TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced is the GREEN
// companion: when the decoder's fresh IR status shows it back in balance
// (4/4), the same engine surge projects 5/4 = 1.25 (in band) and must be
// ALLOWED. This pins that the peer-freshness overlay does not turn into a
// blanket block — it tracks the peer's true position both ways.
func TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is caught up: fresh IR serving 4/4.
	decoder := peerIR(v1beta1.DecoderComponent, 4, 4)
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is irrelevant here (stamp it stale-low to prove the
	// gate prefers the fresh peer IR): decoder projected at 3/4.
	parent := mkRatioParent(25, 4, 3)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine surge must be allowed once the decoder's FRESH status is balanced (4/4 → engine 5/4 = "+
			"1.25 in band), even though the parent projection lags at 3/4: "+reason)
}

// TestBuildReconcileInput_RatioRecoveryStartsFromAuthoritativeZero verifies
// that authoritative positive desired state at zero serving admits one
// recovery surge and serializes a second same-Component start in the wake-up.
func TestBuildReconcileInput_RatioRecoveryStartsFromAuthoritativeZero(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := peerIR(v1beta1.EngineComponent, 4, 0)
	decoderIR := peerIR(v1beta1.DecoderComponent, 4, 0)
	for i := int32(0); i < 4; i++ {
		engineIR.Status.InstanceStatuses = append(engineIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i,
			Phase: v1beta1.OMENativeInstanceFailed,
		})
		decoderIR.Status.InstanceStatuses = append(decoderIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i,
			Phase: v1beta1.OMENativeInstanceFailed,
		})
	}
	r, _ := newReconciler(t, engineIR, decoderIR)

	parent := mkRatioParent(25, 4, 1)
	maxSurge := intstr.FromInt32(4)
	parent.Spec.Rollout.Groups[0].RollingUpdate.MaxSurge = &maxSurge
	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"positive authoritative shape at 0:0 serving must admit one recovery bootstrap: "+reason)

	allowed, _, reason = input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 1, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"same-wakeup recovery must serialize per Component even when MaxSurge permits more: "+reason)
}

// TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus is the
// Sequential analogue of the RatioBalanced peer-freshness guard. The
// decoder is first in Order; the engine must not start until the decoder
// finishes. Here the parent ISVC's projected status reports the decoder
// fully CONVERGED (UpdateRevision == CurrentRevision, ObservedGeneration
// caught up) because the irprojector rollup lags, but the decoder's OWN
// fresh IR status shows it still mid-rollout (UpdateRevision !=
// CurrentRevision).
//
//   - Reading the STALE projection: the decoder looks done, no Component
//     is in flight, the gate reports "Sequential group idle" → the engine
//     starts EARLY (before the decoder finishes).
//   - Reading the decoder's FRESH IR status: the decoder is in flight and
//     is the active Sequential Component → the engine is correctly DENIED.
func TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Fresh decoder IR: revision skew (v2 target, v1 current) ⇒ still
	// rolling, even though the parent projection will say it's done.
	decoder := baselineIR("llama-decoder", "default", 1)
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.ObservedGeneration = 2
	decoder.Status.CurrentRevision = "llama-decoder-v1"
	decoder.Status.UpdateRevision = "llama-decoder-v2"
	r, _ := newReconciler(t, decoder)

	engineIR := baselineIR("llama-engine", "default", 1) // Component=engine

	parent := mkSequentialParent() // Generation=2, Order=[decoder, engine]
	// STALE projection: both Components look converged (decoder "done").
	setObservedGen(parent, v1beta1.DecoderComponent, 2)
	setObservedGen(parent, v1beta1.EngineComponent, 2)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must wait while the decoder is genuinely still rolling (fresh decoder IR has "+
			"UpdateRevision != CurrentRevision); got allowed — the gate read the stale parent "+
			"projection (decoder converged) instead of the decoder's fresh IR status: "+reason)
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
}

// TestBuildReconcileInput_ThreadsGangSchedulingAvailable pins that
// buildReconcileInput threads the controller's GangSchedulingAvailable flag
// into DesiredSpec, because EnsurePodGroups gates per-Instance PodGroup
// creation on it. Without it, IR-managed multi-node pods render the gang
// reference but no PodGroup object is ever created, so the gang stays
// Pending forever ("PodGroup not found").
func TestBuildReconcileInput_ThreadsGangSchedulingAvailable(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	r.GangSchedulingAvailable = true
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{}).DesiredSpec.GangSchedulingAvailable).To(gomega.BeTrue(),
		"DesiredSpec.GangSchedulingAvailable must follow the controller flag (true) so EnsurePodGroups runs")

	r.GangSchedulingAvailable = false
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{}).DesiredSpec.GangSchedulingAvailable).To(gomega.BeFalse(),
		"flag false ⇒ DesiredSpec false ⇒ EnsurePodGroups skips (CRD absent / degradation surface)")
}

func TestBuildReconcileInput_ParentPauseAnnotationIsAuthoritative(t *testing.T) {
	tests := []struct {
		name        string
		irPaused    bool
		irPauseMode v1beta1.PauseMode
		parent      *v1beta1.InferenceService
		wantPaused  bool
		wantFreeze  bool
	}{
		{
			name:       "readable parent pause overrides stale false IR projection",
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"}}},
			wantPaused: true,
		},
		{
			name:       "readable parent removal overrides stale true IR projection",
			irPaused:   true,
			parent:     &v1beta1.InferenceService{},
			wantPaused: false,
		},
		{
			name:       "unreadable parent falls back to projected IR value",
			irPaused:   true,
			wantPaused: true,
		},
		{
			name:       "readable parent freeze value sets both pause depths",
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: constants.PausedRolloutFreezeValue}}},
			wantPaused: true,
			wantFreeze: true,
		},
		{
			name:        "readable parent plain pause overrides stale freeze IR projection",
			irPaused:    true,
			irPauseMode: v1beta1.PauseModeFreeze,
			parent:      &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"}}},
			wantPaused:  true,
			wantFreeze:  false,
		},
		{
			name:        "unreadable parent falls back to projected freeze",
			irPaused:    true,
			irPauseMode: v1beta1.PauseModeFreeze,
			wantPaused:  true,
			wantFreeze:  true,
		},
		{
			name:       "unknown parent annotation value is not paused",
			irPaused:   true,
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "True"}}},
			wantPaused: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t)
			engineIR := baselineIR("llama-engine", "default", 1)
			engineIR.Spec.Paused = tc.irPaused
			engineIR.Spec.PauseMode = tc.irPauseMode
			desired := r.buildReconcileInput(context.Background(), engineIR, tc.parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{}).DesiredSpec
			if desired.Paused != tc.wantPaused {
				t.Fatalf("DesiredSpec.Paused: got %v want %v", desired.Paused, tc.wantPaused)
			}
			if desired.PauseFreeze != tc.wantFreeze {
				t.Fatalf("DesiredSpec.PauseFreeze: got %v want %v", desired.PauseFreeze, tc.wantFreeze)
			}
		})
	}
}

// TestBuildReconcileInput_GateUsesFreshIRStatus pins the IR-path
// gate-staleness guard: the gate must read the GATED Component's
// counts from the IR's OWN fresh status, not the parent ISVC's lagged
// projection. Here the IR's fresh status shows the engine already one pod
// down (serving 3/4); a RatioBalanced drain (RecreatePod → -1) must be
// DENIED — the tiebreaker bounds in-flight to one pod — even though the
// parent's projected status still reports a full 4/4 (which, if the gate
// read it, would let the tiebreaker fire again and over-drain).
func TestBuildReconcileInput_GateUsesFreshIRStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := baselineIR("llama-engine", "default", 4) // engine, parent llama
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 3 // FRESH: one engine pod already out of rotation
	// Peer decoder is balanced at 4/4 (fresh); only the engine is down. Seed
	// both so the gate reads the engine's OWN fresh 3/4 (not nil) — the drain
	// denial must be driven by that, not by the engine being absent.
	decoderIR := peerIR(v1beta1.DecoderComponent, 4, 4)
	r, _ := newReconciler(t, engineIR, decoderIR)

	tol := int32(25)
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					// RatioBalanced pacing is expressed as MaintainRatio with a
					// 25% tolerance.
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				// STALE projection: still reports engine fully serving (4/4).
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}
	pinRun(parent)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _, reason := input.UpdateGate(workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"with the IR's fresh status (engine serving 3/4 = one pod already out), the "+
			"RatioBalanced tiebreaker must refuse a second drain; got allowed — the gate "+
			"read the stale parent projection (4/4) instead of the IR's fresh status: "+reason)
}

const (
	promoteTargetRev = "llama-engine-aaaaaaaa"
	promotePriorRev  = "llama-engine-bbbbbbbb"
)

// getFreshIR re-reads the persisted IR so a test can assert the callback's
// Status().Update landed (or did not).
func getFreshIR(t *testing.T, r *Reconciler, ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	return fresh
}

// TestPromoteCurrentRevision_PartitionedRolloutDoesNotPromote pins the
// staged-rollout contract: an IR converged to a NON-zero partition (some
// Instances Ready on the target revision, the rest Ready-and-held on the
// prior one) must NOT promote CurrentRevision. The promotion gate is
// status.RolloutComplete, which is the partition-0 predicate
// (ReachedDesiredShape(...,0,replicas)); any held Instance makes it false,
// so partition>0 never triggers a promotion. Staged is surfaced downstream
// via the unchanged CurrentRevision != UpdateRevision, not by promoting.
func TestPromoteCurrentRevision_PartitionedRolloutDoesNotPromote(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.CurrentRevision = promotePriorRev
	ir.Status.UpdateRevision = promoteTargetRev
	// Staged shape at partition 1: Instance 0 rolled to the target, Instance
	// 1 intentionally held Ready on the prior revision.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promotePriorRev},
	}

	r, _ := newReconciler(t, ir)
	promote := buildPromoteCurrentRevision(r.statusWriter(), r.Client, ir)
	g.Expect(promote(context.Background(), promoteTargetRev)).To(gomega.Succeed())

	// In-memory snapshot untouched.
	g.Expect(ir.Status.CurrentRevision).To(gomega.Equal(promotePriorRev),
		"a partitioned (staged) rollout must NOT promote CurrentRevision")
	// Persisted status untouched — no write happened.
	fresh := getFreshIR(t, r, ir)
	g.Expect(fresh.Status.CurrentRevision).To(gomega.Equal(promotePriorRev),
		"no Status().Update should have promoted CurrentRevision for a staged rollout")
}

// TestPromoteCurrentRevision_FullConvergePromotes is the positive control:
// every Instance Ready on the target revision (partition 0) → the callback
// promotes CurrentRevision to the target on both the in-memory snapshot and
// the persisted status.
func TestPromoteCurrentRevision_FullConvergePromotes(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.CurrentRevision = promotePriorRev
	ir.Status.UpdateRevision = promoteTargetRev
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev},
	}

	r, _ := newReconciler(t, ir)
	promote := buildPromoteCurrentRevision(r.statusWriter(), r.Client, ir)
	g.Expect(promote(context.Background(), promoteTargetRev)).To(gomega.Succeed())

	g.Expect(ir.Status.CurrentRevision).To(gomega.Equal(promoteTargetRev),
		"full convergence must promote the in-memory CurrentRevision")
	fresh := getFreshIR(t, r, ir)
	g.Expect(fresh.Status.CurrentRevision).To(gomega.Equal(promoteTargetRev),
		"full convergence must persist the promoted CurrentRevision")
}

// TestPromoteCurrentRevision_AlreadyEqualNoWrite pins the no-op-write
// discipline: a converged IR already on the target performs ZERO writes
// (ResourceVersion unchanged), so a steady-state reconcile stays silent.
func TestPromoteCurrentRevision_AlreadyEqualNoWrite(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.CurrentRevision = promoteTargetRev
	ir.Status.UpdateRevision = promoteTargetRev
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev},
	}

	r, _ := newReconciler(t, ir)
	before := getFreshIR(t, r, ir).ResourceVersion

	promote := buildPromoteCurrentRevision(r.statusWriter(), r.Client, ir)
	g.Expect(promote(context.Background(), promoteTargetRev)).To(gomega.Succeed())

	after := getFreshIR(t, r, ir).ResourceVersion
	g.Expect(after).To(gomega.Equal(before),
		"an already-converged IR must perform no Status().Update (ResourceVersion unchanged)")
}

// TestPromoteCurrentRevision_UsesAuthoritativeStatusAfterLifecycleWrite keeps
// a stale Ready cache view from racing a lifecycle status commit.
func TestPromoteCurrentRevision_UsesAuthoritativeStatusAfterLifecycleWrite(t *testing.T) {
	g := gomega.NewWithT(t)
	stale := baselineIR("llama-engine", "prod", 1)
	stale.Status.CurrentRevision = promotePriorRev
	stale.Status.UpdateRevision = promoteTargetRev
	stale.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev,
	}}
	liveObject := stale.DeepCopy()
	liveObject.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceDeleting

	live, writes := newCountingStatusClient(t, 0, liveObject)
	staleReader, _ := newCountingStatusClient(t, 0, stale.DeepCopy())
	writer := &staleReadingClient{Client: live, reader: staleReader}

	promote := buildPromoteCurrentRevision(testStatusWriter(writer), live, stale)
	g.Expect(promote(context.Background(), promoteTargetRev)).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0),
		"a stale Ready cache view must not promote across a committed lifecycle transition")

	persisted := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, persisted)).To(gomega.Succeed())
	g.Expect(persisted.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))
	g.Expect(stale.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))
}

func TestPromoteCurrentRevision_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	stale := baselineIR("llama-engine", "prod", 1)
	stale.Status.CurrentRevision = promotePriorRev

	replacement := stale.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	replacement.Status.CurrentRevision = promotePriorRev
	replacement.Status.UpdateRevision = promoteTargetRev
	replacement.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev,
	}}
	live, writes := newCountingStatusClient(t, 0, replacement)

	promote := buildPromoteCurrentRevision(testStatusWriter(live), live, stale)
	err := promote(context.Background(), promoteTargetRev)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(0))

	persisted := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, persisted)).To(gomega.Succeed())
	g.Expect(persisted.UID).To(gomega.Equal(replacement.UID))
	g.Expect(persisted.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))
}

func TestPromoteCurrentRevision_GenerationChangeAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	stale := baselineIR("llama-engine", "prod", 1)
	stale.Status.CurrentRevision = promotePriorRev
	stale.Status.UpdateRevision = promoteTargetRev
	stale.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: promoteTargetRev,
	}}

	liveObject := stale.DeepCopy()
	liveObject.Generation++
	live, writes := newCountingStatusClient(t, 0, liveObject)

	promote := buildPromoteCurrentRevision(testStatusWriter(live), live, stale)
	err := promote(context.Background(), promoteTargetRev)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusMutationPrecondition)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(0))
	g.Expect(stale.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))

	persisted := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, persisted)).To(gomega.Succeed())
	g.Expect(persisted.Generation).To(gomega.Equal(liveObject.Generation))
	g.Expect(persisted.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))
}

// TestBuildMutateRetryBlock_PersistCreatesBlock pins the Persist path:
// the closure creates the status entry for a previously-absent target
// revision, the write lands on the apiserver (proven by a re-read from
// the fake client, not the in-memory IR), and the committed slice is
// mirrored back onto the caller's in-memory IR so later ops in the same
// pass observe the post-write state.
func TestBuildMutateRetryBlock_PersistCreatesBlock(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	r, c := newReconciler(t, ir)

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir, nil)
	g.Expect(mutate(context.Background(), "rev-a", func(b *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		g.Expect(b.TargetRevision).To(gomega.Equal("rev-a"),
			"an absent block must be handed to the callback as a zero block with TargetRevision set")
		b.State = workloadtypes.RetryBlockBackoff
		b.AttemptsStarted = 1
		b.NextRetryAt = mt(10, 1)
		b.FirstFailureAt = mt(10, 0)
		b.LastFailureAt = mt(10, 0)
		b.Reason = "ImagePullBackOff"
		return workloadtypes.RetryBlockPersist
	})).To(gomega.Succeed())

	// Persistence proof: re-read from the fake client.
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.HaveLen(1))
	b := got.Status.RetryBlocks[0]
	g.Expect(b.TargetRevision).To(gomega.Equal("rev-a"))
	g.Expect(b.State).To(gomega.Equal(v1beta1.RetryBlockBackoff))
	g.Expect(b.AttemptsStarted).To(gomega.Equal(int32(1)))
	g.Expect(b.NextRetryAt).To(gomega.Equal(mt(10, 1)))
	g.Expect(b.FirstFailureAt).To(gomega.Equal(mt(10, 0)))
	g.Expect(b.LastFailureAt).To(gomega.Equal(mt(10, 0)))
	g.Expect(b.Reason).To(gomega.Equal("ImagePullBackOff"))

	// In-memory mirror: the caller's IR snapshot observes the same state.
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(got.Status.RetryBlocks),
		"the committed RetryBlocks must be mirrored back onto the in-memory IR")
}

// TestBuildMutateRetryBlock_RemoveDeletes pins the success-prune path:
// Remove deletes the entry from the persisted status and the mirror.
// Removing an absent block is a clean no-op with zero writes.
func TestBuildMutateRetryBlock_RemoveDeletes(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-a", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1, LastFailureAt: mt(10, 0)},
		{TargetRevision: "rev-b", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, LastFailureAt: mt(11, 0)},
	}
	r, c := newReconciler(t, ir)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir, nil)
	g.Expect(mutate(context.Background(), "rev-a", func(_ *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		return workloadtypes.RetryBlockRemove
	})).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.HaveLen(1))
	g.Expect(got.Status.RetryBlocks[0].TargetRevision).To(gomega.Equal("rev-b"),
		"only the removed revision's block may disappear")
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(got.Status.RetryBlocks))

	// Remove of an absent block: no write (ResourceVersion stable).
	rvBefore := got.ResourceVersion
	g.Expect(mutate(context.Background(), "rev-gone", func(_ *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		return workloadtypes.RetryBlockRemove
	})).To(gomega.Succeed())
	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.ResourceVersion).To(gomega.Equal(rvBefore),
		"removing an absent block must perform zero writes")
}

// TestBuildMutateRetryBlock_UnchangedWritesNothing pins the no-op
// short-circuit: a callback returning Unchanged must not touch the
// apiserver (ResourceVersion stable).
func TestBuildMutateRetryBlock_UnchangedWritesNothing(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-a", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1},
	}
	r, c := newReconciler(t, ir)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir, nil)
	g.Expect(mutate(context.Background(), "rev-a", func(b *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		// Even a callback that scribbles on the block writes nothing when
		// it reports Unchanged.
		b.Reason = "scratch"
		return workloadtypes.RetryBlockUnchanged
	})).To(gomega.Succeed())

	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.ResourceVersion).To(gomega.Equal(before.ResourceVersion),
		"Unchanged must perform zero writes")
	g.Expect(after.Status.RetryBlocks[0].Reason).To(gomega.BeEmpty())
}

func TestBuildMutateRetryBlock_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.RetryBlocks = []v1beta1.RetryBlock{{
		TargetRevision: "rev-a", State: v1beta1.RetryBlockHeld, AttemptsStarted: 2,
	}}
	r, c := newReconciler(t, replacement)
	called := false

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, original, nil)
	err := mutate(context.Background(), "rev-a", func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		called = true
		return workloadtypes.RetryBlockRemove
	})
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(called).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.Equal(replacement.Status.RetryBlocks))
}

// TestBuildMutateRetryBlock_RetentionPrunesOldest pins the retention
// rule: historical blocks (TargetRevision != Status.UpdateRevision) are
// capped at the configured retryBlockHistoryLimit, pruned oldest-first by
// LastFailureAt with nil LastFailureAt sorting oldest — while the block
// for the CURRENT UpdateRevision is NEVER pruned, even when it is the
// oldest block in the list.
func TestBuildMutateRetryBlock_RetentionPrunesOldest(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// The current-target block is the OLDEST (nil LastFailureAt) — the
	// strongest form of the never-pruned guarantee.
	ir.Status.UpdateRevision = "rev-current"
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-current", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1},
		{TargetRevision: "rev-old-nil", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3},
		{TargetRevision: "rev-old-1", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, LastFailureAt: mt(9, 0)},
		{TargetRevision: "rev-old-2", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, LastFailureAt: mt(10, 0)},
	}
	r, c := newReconciler(t, ir)

	// Persisting a 4th historical block pushes the historical count to 4
	// (> the configured cap of 3): the oldest historical — rev-old-nil,
	// nil LastFailureAt — must be pruned; rev-current must survive
	// despite being older than everything else.
	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir, ptr.To(int32(3)))
	g.Expect(mutate(context.Background(), "rev-old-3", func(b *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		b.State = workloadtypes.RetryBlockHeld
		b.AttemptsStarted = 3
		b.LastFailureAt = mt(11, 0)
		return workloadtypes.RetryBlockPersist
	})).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	var revs []string
	for _, b := range got.Status.RetryBlocks {
		revs = append(revs, b.TargetRevision)
	}
	g.Expect(revs).To(gomega.ConsistOf("rev-current", "rev-old-1", "rev-old-2", "rev-old-3"),
		"nil-LastFailureAt historical block prunes first; the UpdateRevision block survives even as the oldest")
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(got.Status.RetryBlocks),
		"the pruned slice must be mirrored back onto the in-memory IR")
}

// TestBuildMutateRetryBlock_UnconfiguredRetentionKeepsHistory pins the
// unset path: with no operator-configured history limit every
// historical block survives, rather than being pruned to a depth the
// binary picked.
func TestBuildMutateRetryBlock_UnconfiguredRetentionKeepsHistory(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.UpdateRevision = "rev-current"
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "rev-current", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1},
		{TargetRevision: "rev-old-nil", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3},
		{TargetRevision: "rev-old-1", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, LastFailureAt: mt(9, 0)},
		{TargetRevision: "rev-old-2", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, LastFailureAt: mt(10, 0)},
	}
	r, c := newReconciler(t, ir)

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir, nil)
	g.Expect(mutate(context.Background(), "rev-old-3", func(b *workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		b.State = workloadtypes.RetryBlockHeld
		b.AttemptsStarted = 3
		b.LastFailureAt = mt(11, 0)
		return workloadtypes.RetryBlockPersist
	})).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	var revs []string
	for _, b := range got.Status.RetryBlocks {
		revs = append(revs, b.TargetRevision)
	}
	g.Expect(revs).To(gomega.ConsistOf("rev-current", "rev-old-nil", "rev-old-1", "rev-old-2", "rev-old-3"),
		"no configured history limit must prune nothing")
}

// TestRetryBlocksFromIR_RoundTrip pins the observed-state mirror:
// observedFromIR converts IR.Status.RetryBlocks field-for-field onto the
// workload shape, and the v1beta1 -> workload -> v1beta1 round-trip is
// the identity (so the RMW closure cannot lose fields in conversion).
func TestRetryBlocksFromIR_RoundTrip(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	in := []v1beta1.RetryBlock{
		{
			TargetRevision:  "rev-a",
			State:           v1beta1.RetryBlockBackoff,
			AttemptsStarted: 2,
			NextRetryAt:     mt(10, 4),
			FirstFailureAt:  mt(10, 0),
			LastFailureAt:   mt(10, 2),
			Reason:          "CrashLoopBackOff",
		},
		{TargetRevision: "rev-b", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3},
	}
	ir.Status.RetryBlocks = in

	observed := observedFromIR(ir)
	g.Expect(observed.RetryBlocks).To(gomega.HaveLen(2))
	w := observed.RetryBlocks[0]
	g.Expect(w.TargetRevision).To(gomega.Equal("rev-a"))
	g.Expect(w.State).To(gomega.Equal(workloadtypes.RetryBlockBackoff))
	g.Expect(w.AttemptsStarted).To(gomega.Equal(int32(2)))
	g.Expect(w.NextRetryAt).To(gomega.Equal(mt(10, 4)))
	g.Expect(w.FirstFailureAt).To(gomega.Equal(mt(10, 0)))
	g.Expect(w.LastFailureAt).To(gomega.Equal(mt(10, 2)))
	g.Expect(w.Reason).To(gomega.Equal("CrashLoopBackOff"))

	// Pointer safety: the mirror must not alias the IR's timestamps.
	g.Expect(w.NextRetryAt).NotTo(gomega.BeIdenticalTo(in[0].NextRetryAt))

	// Round-trip identity.
	for i := range observed.RetryBlocks {
		g.Expect(retryBlockFromWorkload(observed.RetryBlocks[i])).To(gomega.Equal(in[i]))
	}
}
