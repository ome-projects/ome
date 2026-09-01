package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy"
)

func newTestReporter(t *testing.T, objs ...client.Object) (*Reporter, *metrics.Metrics, *record.FakeRecorder, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	m := metrics.New(prometheus.NewRegistry())
	recorder := record.NewFakeRecorder(50)
	return &Reporter{
		Client:    fakeClient,
		Recorder:  recorder,
		Metrics:   m,
		Log:       logr.Discard(),
		Namespace: "ome",
	}, m, recorder, fakeClient
}

func recommendationsCM(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "alfred-recommendations"},
		Data:       data,
	}
}

func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func hasEvent(events []string, substr string) bool {
	for _, e := range events {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func TestReportCycleOutcomes(t *testing.T) {
	r, m, recorder, cl := newTestReporter(t, recommendationsCM(map[string]string{"node.old": "kept"}))
	cfg := config.Default() // recommend-only

	admitted := cand("prod/a", "node1")
	rejected := cand("prod/b", "node3")
	lws := cand("prod/lws", "node1")
	lws.Executable = false
	lws.AdvisoryReason = policy.AdvisoryLWSMigrationUnsupported

	decisions := []Decision{
		{Candidate: admitted, Admitted: true, Target: "node2"},
		{Candidate: rejected, Reason: RejectCooldown},
	}
	r.ReportCycle(context.Background(), []policy.Candidate{admitted, rejected, lws}, decisions, cfg, testNow)

	get := func(vec *prometheus.CounterVec, labels ...string) float64 {
		return promtestutil.ToFloat64(vec.WithLabelValues(labels...))
	}
	if got := get(m.RecommendationsProduced, "defragmentation", "prod/a", "engine", policy.ReasonFragmentation, "true"); got != 1 {
		t.Fatalf("produced{prod/a} = %v", got)
	}
	if got := get(m.RecommendationsProduced, "defragmentation", "prod/lws", "engine", policy.ReasonFragmentation, "false"); got != 1 {
		t.Fatalf("produced{advisory} = %v", got)
	}
	if got := get(m.RecommendationsAccepted, "defragmentation", "prod/a", "engine"); got != 1 {
		t.Fatalf("accepted = %v", got)
	}
	if got := get(m.RecommendationsRejected, "defragmentation", "prod/b", "engine", RejectCooldown); got != 1 {
		t.Fatalf("rejected = %v", got)
	}
	if got := get(m.LWSRecommendations, "prod/lws", "MigrateToOMENative"); got != 1 {
		t.Fatalf("lws counter = %v", got)
	}

	events := drainEvents(recorder)
	for _, want := range []string{
		"RecommendationWithheld", "recommend-only: not dispatched",
		"RecommendationRejected", RejectCooldown,
		"FragmentationRecommendationProduced", policy.AdvisoryLWSMigrationUnsupported,
	} {
		if !hasEvent(events, want) {
			t.Fatalf("events missing %q: %v", want, events)
		}
	}

	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	doc := cm.Data[recommendationsKey]
	for _, want := range []string{
		`"outcome":"` + OutcomeWithheld + `"`,
		`"outcome":"` + OutcomeRejected + `"`,
		`"outcome":"` + OutcomeAdvisory + `"`,
		`"rejectReason":"` + RejectCooldown + `"`,
		`"mode":"recommend-only"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("record missing %s: %s", want, doc)
		}
	}
	if cm.Data["node.old"] != "kept" {
		t.Fatal("sibling ConfigMap keys must be preserved")
	}
}

func TestRawAdvisoryUsesDedicatedEventReason(t *testing.T) {
	r, _, recorder, _ := newTestReporter(t)
	cfg := config.Default()
	*cfg.RecommendationsConfigMapEnabled = false

	raw := cand("prod/raw", "node1")
	raw.Executable = false
	raw.AdvisoryReason = "RawDeploymentMigrationUnsupported"
	raw.HintTargetNodes = nil
	raw.FootprintGPUs = 0
	r.ReportCycle(context.Background(), []policy.Candidate{raw}, nil, cfg, testNow)

	events := drainEvents(recorder)
	if !hasEvent(events, "RawDeploymentMigrationUnsupported") {
		t.Fatalf("Raw advisory must use its dedicated Event reason: %v", events)
	}
	if hasEvent(events, "FragmentationRecommendationProduced") {
		t.Fatalf("Raw advisory must not use the generic fragmentation Event reason: %v", events)
	}
}

// TestCrossPolicyDecisionsKeyedSeparately: node-health and defrag can both
// target the same instance in one pass; each candidate must be reported with
// its own decision, not the other policy's.
func TestCrossPolicyDecisionsKeyedSeparately(t *testing.T) {
	r, m, _, cl := newTestReporter(t, recommendationsCM(nil))
	cfg := config.Default()

	defragCand := cand("prod/a", "node1")
	healthCand := health("prod/a", "node1")
	decisions := []Decision{
		{Candidate: healthCand, Admitted: true, Target: "node2"},
		{Candidate: defragCand, Reason: RejectWorkloadBusy},
	}
	r.ReportCycle(context.Background(), []policy.Candidate{defragCand, healthCand}, decisions, cfg, testNow)

	if got := promtestutil.ToFloat64(m.RecommendationsAccepted.WithLabelValues("nodehealth", "prod/a", "engine")); got != 1 {
		t.Fatalf("health admission miscounted: %v", got)
	}
	if got := promtestutil.ToFloat64(m.RecommendationsRejected.WithLabelValues("defragmentation", "prod/a", "engine", RejectWorkloadBusy)); got != 1 {
		t.Fatalf("defrag rejection miscounted: %v", got)
	}
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	doc := cm.Data[recommendationsKey]
	for _, want := range []string{
		`"policy":"nodehealth","reason":"NodeUnhealthy","outcome":"withheld"`,
		`"policy":"defragmentation","reason":"Fragmentation","outcome":"rejected"`,
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("record missing %s: %s", want, doc)
		}
	}
}

func TestReportCycleWithholdsAdmissionUntilDispatcherExists(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		wantReason string
		wantNote   string
	}{
		{
			name:       "recommend only",
			mode:       config.ModeRecommendOnly,
			wantReason: "RecommendOnly",
			wantNote:   "recommend-only: not dispatched",
		},
		{
			name:       "execute without dispatcher",
			mode:       config.ModeExecute,
			wantReason: "DispatcherUnavailable",
			wantNote:   "dispatcher unavailable: not dispatched",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, m, recorder, cl := newTestReporter(t, recommendationsCM(nil))
			cfg := config.Default()
			cfg.Mode = test.mode

			c := cand("prod/a", "node1")
			r.ReportCycle(context.Background(), []policy.Candidate{c},
				[]Decision{{Candidate: c, Admitted: true, Target: "node2"}}, cfg, testNow)

			if got := promtestutil.ToFloat64(m.RecommendationsAccepted.WithLabelValues(
				"defragmentation", "prod/a", "engine")); got != 1 {
				t.Fatalf("accepted admission counter = %v, want 1", got)
			}
			events := drainEvents(recorder)
			if !hasEvent(events, "RecommendationWithheld") || !hasEvent(events, test.wantNote) {
				t.Fatalf("withheld event missing fixed note %q: %v", test.wantNote, events)
			}
			for _, forbidden := range []string{"RecommendationAdmitted", "will dispatch"} {
				if hasEvent(events, forbidden) {
					t.Fatalf("event falsely claims dispatch via %q: %v", forbidden, events)
				}
			}

			var cm corev1.ConfigMap
			if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
				t.Fatal(err)
			}
			var record struct {
				Recommendations []struct {
					Outcome        string `json:"outcome"`
					WithholdReason string `json:"withholdReason"`
				} `json:"recommendations"`
			}
			if err := json.Unmarshal([]byte(cm.Data[recommendationsKey]), &record); err != nil {
				t.Fatalf("decode recommendations: %v", err)
			}
			if len(record.Recommendations) != 1 {
				t.Fatalf("recommendations = %+v, want one", record.Recommendations)
			}
			got := record.Recommendations[0]
			if got.Outcome != "withheld" || got.WithholdReason != test.wantReason {
				t.Fatalf("withheld record = %+v, want outcome=%q reason=%q", got, "withheld", test.wantReason)
			}
		})
	}
}

func TestCooldownOverrideAudited(t *testing.T) {
	r, m, recorder, _ := newTestReporter(t, recommendationsCM(nil))
	c := health("prod/a", "node1")
	r.ReportCycle(context.Background(), []policy.Candidate{c},
		[]Decision{{Candidate: c, Admitted: true, Target: "node2", CooldownOverridden: true}},
		config.Default(), testNow)

	if got := promtestutil.ToFloat64(m.CooldownOverrides.WithLabelValues("nodehealth")); got != 1 {
		t.Fatalf("override counter = %v", got)
	}
	if events := drainEvents(recorder); !hasEvent(events, "CooldownOverriddenForEvacuation") {
		t.Fatalf("override event missing: %v", events)
	}
}

func TestConfigMapIsUpdateOnly(t *testing.T) {
	r, _, _, cl := newTestReporter(t) // no pre-created ConfigMap
	c := cand("prod/a", "node1")
	r.ReportCycle(context.Background(), []policy.Candidate{c},
		[]Decision{{Candidate: c, Admitted: true, Target: "node2"}}, config.Default(), testNow)

	var cm corev1.ConfigMap
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	if err == nil {
		t.Fatal("the reporter must never create the ConfigMap (the chart pre-creates it)")
	}
}

// TestConfigMapConflictRetried: a stale read must not drop the record — a
// lost node-signal write would be deduped away and never rewritten.
func TestConfigMapConflictRetried(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	conflicts := 1
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(recommendationsCM(nil)).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if conflicts > 0 {
					conflicts--
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("stale"))
				}
				return c.Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Reporter{
		Client:    cl,
		Recorder:  record.NewFakeRecorder(10),
		Metrics:   metrics.New(prometheus.NewRegistry()),
		Log:       logr.Discard(),
		Namespace: "ome",
	}

	c := cand("prod/a", "node1")
	r.ReportCycle(context.Background(), []policy.Candidate{c},
		[]Decision{{Candidate: c, Admitted: true, Target: "node2"}}, config.Default(), testNow)

	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cm.Data[recommendationsKey], `"outcome":"`+OutcomeWithheld+`"`) {
		t.Fatalf("record lost to the conflict: %v", cm.Data)
	}
	if conflicts != 0 {
		t.Fatal("the interceptor conflict was never exercised")
	}
}

// TestNodeSignalRetriesAfterFailedWrite: a failed durable write must clear
// the dedup fingerprint so the next pass retries — otherwise the event and
// metric fire once and the node.<name> record is suppressed forever.
func TestNodeSignalRetriesAfterFailedWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	failing := true
	updates := 0
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(recommendationsCM(nil)).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				if failing {
					return apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("rbac denied"))
				}
				return c.Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Reporter{
		Client:    cl,
		Recorder:  record.NewFakeRecorder(10),
		Metrics:   metrics.New(prometheus.NewRegistry()),
		Log:       logr.Discard(),
		Namespace: "ome",
	}
	cfg := config.Default()

	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow)
	afterFirst := updates
	if afterFirst == 0 {
		t.Fatal("the first signal must attempt the durable write")
	}

	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow.Add(time.Minute))
	if updates <= afterFirst {
		t.Fatal("a failed durable write must be retried on the next signal, not deduped away")
	}

	failing = false
	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow.Add(2*time.Minute))
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cm.Data["node.node7"], "GpuUnhealthy") {
		t.Fatalf("durable record still missing after recovery: %v", cm.Data)
	}

	settled := updates
	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow.Add(3*time.Minute))
	if updates != settled {
		t.Fatal("once the write landed the signal must dedup again")
	}
}

func TestOMENativeTransitionEventOnce(t *testing.T) {
	r, _, recorder, _ := newTestReporter(t)

	r.ReportOMENativeState(false)
	r.ReportOMENativeState(false)
	if events := drainEvents(recorder); len(events) != 1 || !hasEvent(events, "OMENativeUnavailable") {
		t.Fatalf("transition into degraded must emit exactly once: %v", events)
	}

	r.ReportOMENativeState(true)
	if events := drainEvents(recorder); len(events) != 0 {
		t.Fatalf("recovery is a log line, not an event: %v", events)
	}

	r.ReportOMENativeState(false)
	if events := drainEvents(recorder); len(events) != 1 {
		t.Fatalf("a fresh degradation must emit again: %v", events)
	}
}

func TestNodeSignalDedup(t *testing.T) {
	r, m, recorder, cl := newTestReporter(t, recommendationsCM(nil))
	cfg := config.Default()

	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow)
	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow.Add(5*time.Minute))
	if got := promtestutil.ToFloat64(m.NodeHealthSignals.WithLabelValues("node7", "GpuUnhealthy")); got != 1 {
		t.Fatalf("flapping condition must not spam signals: %v", got)
	}
	if events := drainEvents(recorder); len(events) != 1 || !hasEvent(events, "GpuUnhealthyEvacuating") {
		t.Fatalf("node signal events: %v", events)
	}

	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cm.Data["node.node7"], "GpuUnhealthy") {
		t.Fatalf("durable node record missing: %v", cm.Data)
	}

	r.ClearNodeSignal("node7")
	r.NodeSignal(context.Background(), "node7", "GpuUnhealthy", []string{"prod/a"}, cfg, testNow.Add(time.Hour))
	if got := promtestutil.ToFloat64(m.NodeHealthSignals.WithLabelValues("node7", "GpuUnhealthy")); got != 2 {
		t.Fatalf("a fresh incident after clearing must signal again: %v", got)
	}
}
