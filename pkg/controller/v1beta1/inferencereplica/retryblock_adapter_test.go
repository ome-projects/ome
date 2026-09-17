package inferencereplica

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// mt returns a whole-second, local-zone metav1.Time so values compare
// DeepEqual across the fake client's serialization round-trip (metav1
// truncates sub-second precision and unmarshals into the local zone).
func mt(hour, min int) *metav1.Time {
	t := metav1.NewTime(time.Date(2026, 7, 22, hour, min, 0, 0, time.UTC).Local())
	return &t
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

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir)
	g.Expect(mutate(context.Background(), "rev-a", func(b *workload.RetryBlock) workload.RetryBlockDisposition {
		g.Expect(b.TargetRevision).To(gomega.Equal("rev-a"),
			"an absent block must be handed to the callback as a zero block with TargetRevision set")
		b.State = workload.RetryBlockBackoff
		b.AttemptsStarted = 1
		b.NextRetryAt = mt(10, 1)
		b.FirstFailureAt = mt(10, 0)
		b.LastFailureAt = mt(10, 0)
		b.Reason = "ImagePullBackOff"
		return workload.RetryBlockPersist
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

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir)
	g.Expect(mutate(context.Background(), "rev-a", func(_ *workload.RetryBlock) workload.RetryBlockDisposition {
		return workload.RetryBlockRemove
	})).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.HaveLen(1))
	g.Expect(got.Status.RetryBlocks[0].TargetRevision).To(gomega.Equal("rev-b"),
		"only the removed revision's block may disappear")
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(got.Status.RetryBlocks))

	// Remove of an absent block: no write (ResourceVersion stable).
	rvBefore := got.ResourceVersion
	g.Expect(mutate(context.Background(), "rev-gone", func(_ *workload.RetryBlock) workload.RetryBlockDisposition {
		return workload.RetryBlockRemove
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

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir)
	g.Expect(mutate(context.Background(), "rev-a", func(b *workload.RetryBlock) workload.RetryBlockDisposition {
		// Even a callback that scribbles on the block writes nothing when
		// it reports Unchanged.
		b.Reason = "scratch"
		return workload.RetryBlockUnchanged
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

	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, original)
	err := mutate(context.Background(), "rev-a", func(*workload.RetryBlock) workload.RetryBlockDisposition {
		called = true
		return workload.RetryBlockRemove
	})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(called).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.Equal(replacement.Status.RetryBlocks))
}

// TestBuildMutateRetryBlock_RetentionPrunesOldest pins the retention
// rule: historical blocks (TargetRevision != Status.UpdateRevision) are
// capped at maxHistoricalRetryBlocks, pruned oldest-first by
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
	// (> cap 3): the oldest historical — rev-old-nil, nil LastFailureAt —
	// must be pruned; rev-current must survive despite being older than
	// everything else.
	mutate := buildMutateRetryBlock(r.statusWriter(), r.Client, ir)
	g.Expect(mutate(context.Background(), "rev-old-3", func(b *workload.RetryBlock) workload.RetryBlockDisposition {
		b.State = workload.RetryBlockHeld
		b.AttemptsStarted = 3
		b.LastFailureAt = mt(11, 0)
		return workload.RetryBlockPersist
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
	g.Expect(w.State).To(gomega.Equal(workload.RetryBlockBackoff))
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

// TestAggregateStatus_PreservesRetryBlocks pins the status aggregator
// against silently dropping the persisted retry authority: an IR whose
// status carries a RetryBlock must still carry it after a full
// aggregateAndWriteStatus pass (which re-reads, recomputes counters and
// conditions, and issues a Status().Update). A status rebuild that
// reconstructs Status from scratch instead of mutating the re-read
// object would fail here.
func TestAggregateStatus_PreservesRetryBlocks(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
	}
	block := v1beta1.RetryBlock{
		TargetRevision:  "rev-broken",
		State:           v1beta1.RetryBlockHeld,
		AttemptsStarted: 3,
		FirstFailureAt:  mt(9, 0),
		LastFailureAt:   mt(10, 0),
		Reason:          "ImagePullBackOff",
	}
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{block}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	r, c := newReconciler(t, ir, pod0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	// The first pass over a fresh IR stamps counters/conditions, so a
	// real Status().Update lands — this is the write that would drop the
	// block if the aggregator rebuilt status instead of mutating it.
	g.Expect(writeStatus(r, ir, plan, nil, false, nil)).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.RetryBlocks).To(gomega.Equal([]v1beta1.RetryBlock{block}),
		"aggregateAndWriteStatus must never drop or rewrite persisted RetryBlocks")
}
