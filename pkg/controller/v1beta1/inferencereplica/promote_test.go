package inferencereplica

import (
	"context"
	"errors"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

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
// workload.RolloutComplete, which is the partition-0 predicate
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
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
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
	g.Expect(errors.Is(err, workload.ErrStatusMutationPrecondition)).To(gomega.BeTrue())
	g.Expect(*writes).To(gomega.Equal(0))
	g.Expect(stale.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))

	persisted := &v1beta1.InferenceReplica{}
	g.Expect(live.Get(context.Background(), types.NamespacedName{Name: stale.Name, Namespace: stale.Namespace}, persisted)).To(gomega.Succeed())
	g.Expect(persisted.Generation).To(gomega.Equal(liveObject.Generation))
	g.Expect(persisted.Status.CurrentRevision).To(gomega.Equal(promotePriorRev))
}
