package inferencereplica

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// withRetentionConfig wires a fake clientset serving an
// inferenceservice-config ConfigMap whose lifecycle block carries the
// given revisionHistoryLimit, exercising the config-default resolution
// path of the retention sweep.
func withRetentionConfig(r *Reconciler, limit int32) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"lifecycle": fmt.Sprintf(`{"revisionHistoryLimit":%d}`, limit),
		},
	}
	r.Clientset = kubefake.NewSimpleClientset(cm)
}

// TestReconcile_RetainsControllerRevisions is the end-to-end retention
// wire-in test for the CONFIG-DEFAULT path: the retention cap comes from
// lifecycle.revisionHistoryLimit in the operator ConfigMap (no per-IR
// spec limit). It seeds far more than the cap in non-live
// ControllerRevisions plus one live revision (pinned via the IR's
// CurrentRevision), drives a reconcile, and asserts:
//
//   - the live revision survives even though it is the OLDEST seeded CR
//     (proving the live-name union, not just recency, protects it);
//   - non-live revisions beyond the configured cap are deleted;
//   - the newest cap-many non-live CRs remain, plus the 1 live one.
//
// This exercises the real reconcile defer path (sweepRevisions →
// resolveRevisionRetentionLimit → revision.RetainControllerRevisions +
// revision.CollectLiveRevisionNames) against the fake client, so it
// covers the call-site wiring the unit tests in workload/revision/
// cannot reach. The per-Instance RunningRevision union is covered
// separately by TestSweepRevisions_HonorsRunningRevisionUnion below
// (kept off the reconcile path so the workload dispatcher doesn't try
// to load the pinned CR's payload).
func TestReconcile_RetainsControllerRevisions(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)

	// The live revision is the OLDEST (.Revision=1) so retention by
	// recency alone would delete it — only the live-name union saves it.
	liveCR := seedControllerRevision(ir, "live", 1)

	// Pin it live via the Component-level CurrentRevision. No
	// InstanceStatuses are set so the workload dispatcher stays on the
	// Create path and never tries to load the pinned CR's payload (which
	// the seed leaves empty); the retention assertion is unaffected.
	ir.Status.CurrentRevision = liveCR.Name

	// Seed testRevisionRetention+5 non-live CRs with strictly higher
	// (newer) revision numbers than the live one. Retention must keep the
	// newest testRevisionRetention of these and drop the oldest 5.
	const extra = 5
	nonLive := make([]*appsv1.ControllerRevision, 0, testRevisionRetention+extra)
	for i := 0; i < testRevisionRetention+extra; i++ {
		// .Revision starts at 2 (above the live CR's 1) and increases.
		cr := seedControllerRevision(ir, fmt.Sprintf("nonlive%02d", i), int64(i+2))
		nonLive = append(nonLive, cr)
	}

	objs := []client.Object{ir, liveCR}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)
	withRetentionConfig(r, testRevisionRetention)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"reconcile should not fail; retention is best-effort and non-fatal")

	survivors := listRevisionNames(t, c, ir.Namespace)

	// The live (oldest) CR must survive purely on the live-name union.
	g.Expect(survivors).To(gomega.HaveKey(liveCR.Name),
		"live revision (oldest) must never be swept even past the retention bound")

	// The oldest `extra` non-live CRs (lowest .Revision) must be gone.
	for i := 0; i < extra; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"oldest non-live revision %s should have been swept", nonLive[i].Name)
	}

	// The newest testRevisionRetention non-live CRs must remain.
	for i := extra; i < len(nonLive); i++ {
		g.Expect(survivors).To(gomega.HaveKey(nonLive[i].Name),
			"newest non-live revision %s must be retained", nonLive[i].Name)
	}

	// The reconcile may itself ensure one fresh CR for the live PodSpec.
	// Floor the count: at least the live + the retained non-live window.
	// Upper-bound it so a regression that disables the sweep (leaving all
	// testRevisionRetention+extra non-live CRs) fails loudly.
	g.Expect(len(survivors)).To(gomega.BeNumerically("<", 1+testRevisionRetention+extra),
		"retention must have deleted at least the oldest non-live revisions")
	g.Expect(len(survivors)).To(gomega.BeNumerically(">=", 1+testRevisionRetention),
		"live revision plus the newest retention window must all survive")
}

// TestReconcile_SpecLimitOverridesConfigDefault is the end-to-end test
// for the ANNOTATION path: Spec.RevisionHistoryLimit (projected from
// the parent ISVC's ome.io/revision-history-limit annotation) wins over
// the operator-level config default. With a spec limit of 1 and a
// config default of testRevisionRetention, at most ONE seeded non-live
// revision survives a reconcile — and the live revision still survives.
func TestReconcile_SpecLimitOverridesConfigDefault(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.RevisionHistoryLimit = ptr.To(int32(1))

	liveCR := seedControllerRevision(ir, "live", 1)
	ir.Status.CurrentRevision = liveCR.Name

	const seeded = 4
	nonLive := make([]*appsv1.ControllerRevision, 0, seeded)
	for i := 0; i < seeded; i++ {
		nonLive = append(nonLive, seedControllerRevision(ir, fmt.Sprintf("nonlive%02d", i), int64(i+2)))
	}

	objs := []client.Object{ir, liveCR}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)
	// The config default is present AND higher — the per-IR spec limit
	// must still win.
	withRetentionConfig(r, testRevisionRetention)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	survivors := listRevisionNames(t, c, ir.Namespace)
	g.Expect(survivors).To(gomega.HaveKey(liveCR.Name),
		"live revision must survive regardless of the limit")
	// All seeded non-live CRs except the newest must be gone. The
	// reconcile may mint one fresh CR for the live PodSpec, but that one
	// is live (UpdateRevision) — it never consumes the non-live budget.
	for i := 0; i < seeded-1; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"with limit 1, older non-live revision %s must be swept", nonLive[i].Name)
	}
	g.Expect(survivors).To(gomega.HaveKey(nonLive[seeded-1].Name),
		"the single newest non-live revision must be retained")
}

// TestSweepRevisions_UnconfiguredPrunesNothing pins the graceful-
// degradation contract: with no per-IR spec limit and no operator
// config (nil clientset), the sweep is a no-op — nothing is deleted,
// because the binary carries no baked-in retention default.
func TestSweepRevisions_UnconfiguredPrunesNothing(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)

	const seeded = 15
	nonLive := make([]*appsv1.ControllerRevision, 0, seeded)
	for i := 0; i < seeded; i++ {
		nonLive = append(nonLive, seedControllerRevision(ir, fmt.Sprintf("nl%02d", i), int64(i+1)))
	}
	objs := []client.Object{ir}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)
	// No withRetentionConfig, no Spec.RevisionHistoryLimit.

	r.sweepRevisions(context.Background(), ir)

	survivors := listRevisionNames(t, c, ir.Namespace)
	for _, cr := range nonLive {
		g.Expect(survivors).To(gomega.HaveKey(cr.Name),
			"unconfigured retention must not delete %s", cr.Name)
	}
}

// TestSweepRevisions_NonFatalOnNilRecorder pins the best-effort
// contract at the helper boundary: sweepRevisions must not panic when
// the Recorder is nil (the reconciler tolerates a nil Recorder) and must
// not return an error channel that could fail the reconcile — it returns
// nothing. Seeds no CRs so the sweep is a clean no-op.
func TestSweepRevisions_NonFatalOnNilRecorder(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.RevisionHistoryLimit = ptr.To(int32(testRevisionRetention))
	r, _ := newReconciler(t, ir)
	r.Recorder = nil // explicit: the helper must tolerate this

	g.Expect(func() {
		r.sweepRevisions(context.Background(), ir)
	}).NotTo(gomega.Panic(),
		"sweepRevisions must be a safe no-op with no history and a nil Recorder")
}

// TestSweepRevisions_HonorsRunningRevisionUnion pins the migration-
// safety case at the helper boundary: a revision pinned ONLY by a
// per-Instance RunningRevision (not CurrentRevision / UpdateRevision)
// must survive retention even when it is the oldest CR and the bound is
// exceeded. This proves sweepRevisions threads the per-Instance union
// from ir.Status.InstanceStatuses into CollectLiveRevisionNames — the
// guard that stops an in-flight migration's source CR from being swept
// out from under it. Calls sweepRevisions directly so the workload
// dispatcher (which would try to load the pinned CR's payload) stays out
// of the picture. The limit rides the per-IR spec path here; the config
// path is covered by TestReconcile_RetainsControllerRevisions.
func TestSweepRevisions_HonorsRunningRevisionUnion(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.RevisionHistoryLimit = ptr.To(int32(testRevisionRetention))

	// migrationSource is the OLDEST CR, pinned ONLY via a per-Instance
	// RunningRevision — recency would delete it; the union must save it.
	migrationSource := seedControllerRevision(ir, "migsrc", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, RunningRevision: migrationSource.Name},
	}

	const extra = 3
	nonLive := make([]*appsv1.ControllerRevision, 0, testRevisionRetention+extra)
	for i := 0; i < testRevisionRetention+extra; i++ {
		nonLive = append(nonLive, seedControllerRevision(ir, fmt.Sprintf("nl%02d", i), int64(i+2)))
	}

	objs := []client.Object{ir, migrationSource}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}
	r, c := newReconciler(t, objs...)

	r.sweepRevisions(context.Background(), ir)

	survivors := listRevisionNames(t, c, ir.Namespace)
	g.Expect(survivors).To(gomega.HaveKey(migrationSource.Name),
		"a revision pinned only by InstanceStatus.RunningRevision must survive retention")
	// Oldest `extra` non-live trimmed; newest testRevisionRetention kept.
	for i := 0; i < extra; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"oldest non-live revision %s should have been swept", nonLive[i].Name)
	}
	for i := extra; i < len(nonLive); i++ {
		g.Expect(survivors).To(gomega.HaveKey(nonLive[i].Name),
			"newest non-live revision %s must be retained", nonLive[i].Name)
	}
}
