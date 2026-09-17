package inferencereplica

// Release-mailbox coverage for the ome.io/release-held-revision
// annotation: a Held block matched by full revision name or bare hash
// is removed and the annotation is consumed; non-Held or unknown
// revisions consume as an explanatory no-op; unmatched blocks are
// never touched.

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// newReleaseFixture builds a fake-client Reconciler (with recorder)
// around the given IR and re-reads it so the ResourceVersion is
// stamped for the annotation-consume Update.
func newReleaseFixture(t *testing.T, ir *v1beta1.InferenceReplica) (*Reconciler, client.Client, *record.FakeRecorder, *v1beta1.InferenceReplica) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	rec := record.NewFakeRecorder(16)
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Log:                  logf.Log.WithName("test"),
		Recorder:             rec,
		Expectations:         workload.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
	}
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	return r, c, rec, fresh
}

// releaseIR is baselineIR plus the release annotation and two blocks:
// a Held one for rev "llama-engine-aaaaaaaa" and a Backoff one for
// "llama-engine-bbbbbbbb" (the untouched bystander).
func releaseIR(annotationValue string) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-engine", "default", 1)
	ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = annotationValue
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{
		{TargetRevision: "llama-engine-aaaaaaaa", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, Reason: "ImagePullBackOff"},
		{TargetRevision: "llama-engine-bbbbbbbb", State: v1beta1.RetryBlockBackoff, AttemptsStarted: 1},
	}
	return ir
}

// assertAnnotationConsumed checks the annotation is gone from both the
// apiserver copy and the caller's in-memory IR.
func assertAnnotationConsumed(t *testing.T, g *gomega.WithT, c client.Client, ir *v1beta1.InferenceReplica) {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Annotations).NotTo(gomega.HaveKey(constants.ReleaseHeldRevisionAnnotationKey),
		"the annotation must be consumed on the apiserver copy")
	g.Expect(ir.Annotations).NotTo(gomega.HaveKey(constants.ReleaseHeldRevisionAnnotationKey),
		"the consumed annotation must be mirrored off the in-memory IR")
}

// TestConsumeReleaseHeld_FullNameReleasesHeldBlock pins the release
// path: a Held block named by full revision name is removed (status
// write + in-memory mirror), the bystander block survives untouched,
// the annotation is consumed, and a RetryBlockReleased event fires.
func TestConsumeReleaseHeld_FullNameReleasesHeldBlock(t *testing.T) {
	g := gomega.NewWithT(t)
	r, c, rec, ir := newReleaseFixture(t, releaseIR("llama-engine-aaaaaaaa"))

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.RetryBlocks).To(gomega.HaveLen(1))
	g.Expect(fresh.Status.RetryBlocks[0].TargetRevision).To(gomega.Equal("llama-engine-bbbbbbbb"),
		"only the matched Held block may be removed")
	g.Expect(fresh.Status.RetryBlocks[0].State).To(gomega.Equal(v1beta1.RetryBlockBackoff),
		"the bystander block must keep its state")
	g.Expect(ir.Status.RetryBlocks).To(gomega.Equal(fresh.Status.RetryBlocks),
		"the committed slice must be mirrored onto the in-memory IR")

	assertAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonRetryBlockReleased))).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "llama-engine-aaaaaaaa")).NotTo(gomega.BeEmpty(),
		"the event must name the released revision")
	g.Expect(eventsContaining(events, "operator request")).NotTo(gomega.BeEmpty(),
		"the event must say the release was operator-requested")
}

// TestConsumeReleaseHeld_BareHashMatches pins the bare-hash form: the
// annotation value "aaaaaaaa" resolves to the Held block whose revision
// name ends in that hash.
func TestConsumeReleaseHeld_BareHashMatches(t *testing.T) {
	g := gomega.NewWithT(t)
	r, c, rec, ir := newReleaseFixture(t, releaseIR("aaaaaaaa"))

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.RetryBlocks).To(gomega.HaveLen(1))
	g.Expect(fresh.Status.RetryBlocks[0].TargetRevision).To(gomega.Equal("llama-engine-bbbbbbbb"))

	assertAnnotationConsumed(t, g, c, ir)
	g.Expect(eventsContaining(drainEvents(rec), string(workload.EventReasonRetryBlockReleased))).To(gomega.HaveLen(1))
}

// TestConsumeReleaseHeld_NonHeldMatch_NoOpConsumed pins the non-Held
// branch: a Backoff block matched by the request is left exactly as it
// was, the annotation is still consumed, and the skip event explains
// why nothing changed.
func TestConsumeReleaseHeld_NonHeldMatch_NoOpConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	r, c, rec, ir := newReleaseFixture(t, releaseIR("llama-engine-bbbbbbbb"))

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.RetryBlocks).To(gomega.HaveLen(2),
		"a non-Held match must not remove anything")

	assertAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonRetryBlockReleaseSkipped))).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "not Held")).NotTo(gomega.BeEmpty())
	g.Expect(eventsContaining(events, string(workload.EventReasonRetryBlockReleased)+" ")).To(gomega.BeEmpty(),
		"no release event on the no-op branch")
}

// TestConsumeReleaseHeld_UnknownRevision_NoOpConsumed pins the
// no-match branch: an annotation naming a revision with no block
// consumes as a no-op with an explanatory event; all blocks survive.
func TestConsumeReleaseHeld_UnknownRevision_NoOpConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	r, c, rec, ir := newReleaseFixture(t, releaseIR("llama-engine-cccccccc"))

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, ir, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.RetryBlocks).To(gomega.HaveLen(2))

	assertAnnotationConsumed(t, g, c, ir)

	events := drainEvents(rec)
	g.Expect(eventsContaining(events, string(workload.EventReasonRetryBlockReleaseSkipped))).To(gomega.HaveLen(1))
	g.Expect(eventsContaining(events, "no retryBlock exists")).NotTo(gomega.BeEmpty())
}

// TestConsumeReleaseHeld_NoAnnotation_NoOp pins the empty-mailbox fast
// path: no annotation means no writes and no events.
func TestConsumeReleaseHeld_NoAnnotation_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := releaseIR("x")
	delete(ir.Annotations, constants.ReleaseHeldRevisionAnnotationKey)
	r, c, rec, fresh0 := newReleaseFixture(t, ir)
	rv := fresh0.ResourceVersion

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, fresh0, nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.ResourceVersion).To(gomega.Equal(rv), "empty mailbox must perform zero writes")
	g.Expect(fresh.Status.RetryBlocks).To(gomega.HaveLen(2))
	g.Expect(drainEvents(rec)).To(gomega.BeEmpty())
}

// TestConsumeReleaseHeld_ParentIsEventTarget pins the event routing:
// with a resolvable parent, the release event lands on the ISVC (the
// user-facing stream), matching the rest of the IR event surface.
func TestConsumeReleaseHeld_ParentIsEventTarget(t *testing.T) {
	g := gomega.NewWithT(t)
	r, c, rec, ir := newReleaseFixture(t, releaseIR("llama-engine-aaaaaaaa"))
	parent := migrationParent(nil, false)

	requeue, err := r.consumeReleaseHeldRequest(context.Background(), r.Log, ir, parent)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	assertAnnotationConsumed(t, g, c, ir)
	g.Expect(eventsContaining(drainEvents(rec), string(workload.EventReasonRetryBlockReleased))).To(gomega.HaveLen(1))
}
