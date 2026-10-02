package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

type capturedEvent struct {
	object                     runtime.Object
	eventType, reason, message string
}
type capturingRecorder struct{ events []capturedEvent }

func (r *capturingRecorder) Event(o runtime.Object, typ, reason, message string) {
	r.events = append(r.events, capturedEvent{o, typ, reason, message})
}
func (r *capturingRecorder) Eventf(o runtime.Object, typ, reason, format string, args ...interface{}) {
	r.Event(o, typ, reason, fmt.Sprintf(format, args...))
}
func (r *capturingRecorder) AnnotatedEventf(o runtime.Object, _ map[string]string, typ, reason, format string, args ...interface{}) {
	r.Eventf(o, typ, reason, format, args...)
}
func (r *capturingRecorder) count(reason string) int {
	n := 0
	for _, e := range r.events {
		if e.reason == reason {
			n++
		}
	}
	return n
}
func (r *capturingRecorder) event(reason string) (capturedEvent, bool) {
	for _, e := range r.events {
		if e.reason == reason {
			return e, true
		}
	}
	return capturedEvent{}, false
}

type nodeConditionView struct {
	Type               corev1.NodeConditionType
	Status             corev1.ConditionStatus
	LastTransitionTime time.Time
}
type nodeRecordView struct {
	NodeUID                                      types.UID                           `json:"nodeUID"`
	State                                        snapshot.NodeHealthState            `json:"state"`
	Conditions                                   []nodeConditionView                 `json:"conditions"`
	SuspectUntil                                 *time.Time                          `json:"suspectUntil"`
	Maintenance                                  snapshot.NodeMaintenanceObservation `json:"maintenance"`
	Workloads                                    []string                            `json:"workloads"`
	OMEGPUOccupantsPresent                       bool                                `json:"omeGpuOccupantsPresent"`
	ObservedAt                                   time.Time                           `json:"observedAt"`
	SignaledAt, DrainedAt                        *time.Time
	MaintenanceRequestedAt, MaintenanceDrainedAt *time.Time
}

func remediationCandidate(node string, health snapshot.NodeHealthObservation, workloads ...string) policy.Candidate {
	return policy.Candidate{Policy: "nodehealth", Reason: policy.ReasonRemediationSignal, FromNode: node, Remediation: &policy.NodeRemediation{
		Node: node, NodeUID: types.UID(node + "-uid"), ObservedAt: testNow, Health: health, Workloads: workloads, OMEGPUOccupantsPresent: len(workloads) > 0,
	}}
}
func nodeRecord(t *testing.T, cl client.Client, node string) (nodeRecordView, bool) {
	t.Helper()
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm); err != nil {
		t.Fatal(err)
	}
	raw, ok := cm.Data["node."+node]
	if !ok {
		return nodeRecordView{}, false
	}
	var v nodeRecordView
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatal(err)
	}
	return v, true
}
func unhealthyObservation() snapshot.NodeHealthObservation {
	return snapshot.NodeHealthObservation{State: snapshot.NodeHealthUnhealthy, Conditions: []snapshot.NodeConditionObservation{{Type: "GpuUnhealthy", Status: corev1.ConditionTrue, LastTransitionTime: testNow.Add(-time.Hour)}}}
}
func reportMarker(r *Reporter, c policy.Candidate, observed time.Time) {
	c.Remediation.ObservedAt = observed
	r.ReportCycle(context.Background(), []policy.Candidate{c}, nil, config.Default(), testNow.Add(time.Hour))
}

func TestReporterNodeCachedObservationCannotDrain(t *testing.T) {
	r, m, _, cl := newTestReporter(t, recommendationsCM(nil))
	rec := &capturingRecorder{}
	r.Recorder = rec
	c := remediationCandidate("node7", unhealthyObservation())
	reportMarker(r, c, testNow)
	if rec.count("NodeRepairNeeded") != 1 {
		t.Fatalf("opening repair signal missing: %+v", rec.events)
	}
	for range 2 {
		reportMarker(r, c, testNow)
	}
	v, ok := nodeRecord(t, cl, "node7")
	if !ok || v.DrainedAt != nil || !v.ObservedAt.Equal(testNow) || rec.count("NodeDrainedForRepair") != 0 {
		t.Fatalf("cached snapshot advanced lifecycle: %+v events=%+v", v, rec.events)
	}
	reportMarker(r, c, testNow.Add(time.Minute))
	v, _ = nodeRecord(t, cl, "node7")
	if v.DrainedAt == nil || !v.DrainedAt.Equal(testNow.Add(time.Minute)) || rec.count("NodeDrainedForRepair") != 1 {
		t.Fatalf("new observation failed to drain: %+v", v)
	}
	if promtestutil.ToFloat64(m.NodeHealthSignals.WithLabelValues("node7", "NodeRepairNeeded")) != 1 || promtestutil.CollectAndCount(m.RecommendationsProduced) != 0 {
		t.Fatal("node lifecycle must signal once without creating workload recommendations")
	}
	if e, _ := rec.event("NodeRepairNeeded"); e.object.(*corev1.Node).UID != "node7-uid" {
		t.Fatal("node event missing incarnation UID")
	}
}

func TestReporterNodeInvalidObservationCannotAdvance(t *testing.T) {
	for _, at := range []time.Time{{}, testNow.Add(-time.Minute), testNow.Add(2 * time.Hour)} {
		t.Run(at.String(), func(t *testing.T) {
			r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
			rec := &capturingRecorder{}
			r.Recorder = rec
			c := remediationCandidate("node7", unhealthyObservation(), "prod/a")
			reportMarker(r, c, testNow)
			c.Remediation.Workloads = nil
			c.Remediation.OMEGPUOccupantsPresent = false
			reportMarker(r, c, at)
			v, _ := nodeRecord(t, cl, "node7")
			if !v.ObservedAt.Equal(testNow) || !v.OMEGPUOccupantsPresent || v.DrainedAt != nil {
				t.Fatalf("invalid observation advanced record: %+v", v)
			}
		})
	}
}

func TestReporterNodeMaintenanceAndRepairIndependent(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(map[string]string{"state.json": "durable-dispatch", "unrelated": "keep"}))
	rec := &capturingRecorder{}
	r.Recorder = rec
	c := remediationCandidate("node7", snapshot.NodeHealthObservation{State: snapshot.NodeHealthClear}, "prod/a")
	c.Remediation.Maintenance = snapshot.NodeMaintenanceObservation{Requested: true, Triggers: []string{"operator"}}
	reportMarker(r, c, testNow)
	if rec.count("NodeMaintenanceRequested") != 1 || rec.count("NodeRepairNeeded") != 0 {
		t.Fatalf("healthy maintenance mislabeled: %+v", rec.events)
	}
	c.Remediation.Health = unhealthyObservation()
	reportMarker(r, c, testNow.Add(time.Minute))
	if rec.count("NodeRepairNeeded") != 1 || rec.count("NodeMaintenanceRequested") != 1 {
		t.Fatalf("dual reason suppressed or duplicated: %+v", rec.events)
	}
	c.Remediation.Workloads = nil // unidentified occupant still present
	reportMarker(r, c, testNow.Add(2*time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if v.DrainedAt != nil || v.MaintenanceDrainedAt != nil {
		t.Fatal("unidentified GPU occupant marked drained")
	}
	c.Remediation.OMEGPUOccupantsPresent = false
	reportMarker(r, c, testNow.Add(3*time.Minute))
	v, _ = nodeRecord(t, cl, "node7")
	if v.DrainedAt == nil || v.MaintenanceDrainedAt == nil || rec.count("NodeDrainedForRepair") != 1 || rec.count("NodeDrainedForMaintenance") != 1 {
		t.Fatalf("independent drain transitions missing: %+v events=%+v", v, rec.events)
	}
	c.Remediation.OMEGPUOccupantsPresent = true
	reportMarker(r, c, testNow.Add(4*time.Minute))
	v, _ = nodeRecord(t, cl, "node7")
	if v.DrainedAt != nil || v.MaintenanceDrainedAt != nil {
		t.Fatal("reoccupation retained drained readiness")
	}
	c.Remediation.Maintenance = snapshot.NodeMaintenanceObservation{}
	reportMarker(r, c, testNow.Add(5*time.Minute))
	v, _ = nodeRecord(t, cl, "node7")
	if v.MaintenanceRequestedAt != nil || v.SignaledAt == nil {
		t.Fatalf("maintenance clear damaged repair episode: %+v", v)
	}
	var cm corev1.ConfigMap
	_ = cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	if cm.Data["state.json"] != "durable-dispatch" || cm.Data["unrelated"] != "keep" {
		t.Fatal("reporter replaced unrelated durable keys")
	}
	for _, e := range rec.events {
		if strings.Contains(e.message, "power off") || strings.Contains(e.message, "ready for repair") {
			t.Fatal("event claimed whole-node repair safety")
		}
	}
}

func TestReporterNodeUnknownSignalsOnlyAndSuspectDoesNotOpen(t *testing.T) {
	for _, state := range []snapshot.NodeHealthState{snapshot.NodeHealthUnknown, snapshot.NodeHealthSuspect} {
		t.Run(string(state), func(t *testing.T) {
			r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
			rec := &capturingRecorder{}
			r.Recorder = rec
			c := remediationCandidate("node7", snapshot.NodeHealthObservation{State: state})
			reportMarker(r, c, testNow)
			reportMarker(r, c, testNow.Add(time.Minute))
			want := 0
			if state == snapshot.NodeHealthUnknown {
				want = 1
			}
			v, _ := nodeRecord(t, cl, "node7")
			if rec.count("NodeRepairNeeded") != want || rec.count("NodeDrainedForRepair") != 0 || v.DrainedAt != nil {
				t.Fatalf("uncertain health advanced episode: %+v %+v", v, rec.events)
			}
		})
	}
}

func TestReporterNodeRestartAndUIDReplacement(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	first := &capturingRecorder{}
	r.Recorder = first
	c := remediationCandidate("node7", unhealthyObservation())
	reportMarker(r, c, testNow)
	restart := func() (*Reporter, *capturingRecorder) {
		rec := &capturingRecorder{}
		return &Reporter{Client: cl, Recorder: rec, Metrics: metrics.New(prometheus.NewRegistry()), Log: logr.Discard(), Namespace: "ome"}, rec
	}
	r, rec := restart()
	reportMarker(r, c, testNow)
	if len(rec.events) != 0 {
		t.Fatalf("restart replayed/advanced same evidence: %+v", rec.events)
	}
	reportMarker(r, c, testNow.Add(time.Minute))
	if rec.count("NodeDrainedForRepair") != 1 || rec.count("NodeRepairNeeded") != 0 {
		t.Fatalf("valid durable episode not continued: %+v", rec.events)
	}
	r, rec = restart()
	c.Remediation.NodeUID = "replacement-uid"
	reportMarker(r, c, testNow.Add(2*time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if rec.count("NodeRepairNeeded") != 1 || v.DrainedAt != nil || v.NodeUID != "replacement-uid" {
		t.Fatalf("new node inherited old episode: %+v events=%+v", v, rec.events)
	}
}

func TestReporterNodeRestartRejectsChangedHealthEvidence(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	r.Recorder = &capturingRecorder{}
	c := remediationCandidate("node7", unhealthyObservation())
	reportMarker(r, c, testNow)
	reportMarker(r, c, testNow.Add(time.Minute))
	c.Remediation.Health.Conditions[0].LastTransitionTime = testNow.Add(2 * time.Minute)
	rec := &capturingRecorder{}
	r = &Reporter{Client: cl, Recorder: rec, Metrics: metrics.New(prometheus.NewRegistry()), Log: logr.Discard(), Namespace: "ome"}
	reportMarker(r, c, testNow.Add(3*time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if rec.count("NodeRepairNeeded") != 1 || v.DrainedAt != nil || v.SignaledAt == nil || !v.SignaledAt.Equal(testNow.Add(3*time.Minute)) {
		t.Fatalf("changed evidence inherited stale readiness: %+v events=%+v", v, rec.events)
	}
}

func TestReporterNodeMaintenanceRestartRequiresNewDrainEvidence(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	r.Recorder = &capturingRecorder{}
	c := remediationCandidate("node7", snapshot.NodeHealthObservation{State: snapshot.NodeHealthClear})
	c.Remediation.Maintenance = snapshot.NodeMaintenanceObservation{Requested: true, Triggers: []string{"operator"}}
	reportMarker(r, c, testNow)
	reportMarker(r, c, testNow.Add(time.Minute))
	rec := &capturingRecorder{}
	r = &Reporter{Client: cl, Recorder: rec, Metrics: metrics.New(prometheus.NewRegistry()), Log: logr.Discard(), Namespace: "ome"}
	reportMarker(r, c, testNow.Add(2*time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if rec.count("NodeMaintenanceRequested") != 1 || v.MaintenanceDrainedAt != nil {
		t.Fatalf("maintenance restart inherited unproven episode: %+v events=%+v", v, rec.events)
	}
	reportMarker(r, c, testNow.Add(3*time.Minute))
	v, _ = nodeRecord(t, cl, "node7")
	if rec.count("NodeDrainedForMaintenance") != 1 || v.MaintenanceDrainedAt == nil {
		t.Fatalf("new maintenance drain interval missing: %+v", v)
	}
}

func TestReporterNodeMaintenanceMetricDoesNotCountHealthDamage(t *testing.T) {
	r, _, _, _ := newTestReporter(t, recommendationsCM(nil))
	registry := prometheus.NewRegistry()
	m := metrics.New(registry)
	r.Metrics = m
	c := remediationCandidate("node7", snapshot.NodeHealthObservation{State: snapshot.NodeHealthClear})
	c.Remediation.Maintenance = snapshot.NodeMaintenanceObservation{Requested: true, Triggers: []string{"operator"}}
	reportMarker(r, c, testNow)
	if promtestutil.CollectAndCount(m.NodeHealthSignals) != 0 {
		t.Fatal("healthy maintenance counted as health damage")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "alfred_nodemaintenance_signals_total" {
			found = len(f.Metric) == 1 && f.Metric[0].GetCounter().GetValue() == 1
		}
	}
	if !found {
		t.Fatal("maintenance request did not increment its separate lifecycle counter")
	}
}

func TestReporterNodeEmptySetRequiresNewSuccessfulObservation(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	c := remediationCandidate("node7", unhealthyObservation())
	reportMarker(r, c, testNow)
	for _, at := range []time.Time{{}, testNow.Add(-time.Minute), testNow, testNow.Add(2 * time.Hour)} {
		r.ReportCycle(context.Background(), nil, nil, config.Default(), testNow.Add(time.Hour), at)
		v, ok := nodeRecord(t, cl, "node7")
		if !ok || !v.ObservedAt.Equal(testNow) {
			t.Fatalf("stale/invalid empty set cleared episode: %v %+v", at, v)
		}
	}
	r.ReportCycle(context.Background(), nil, nil, config.Default(), testNow.Add(time.Hour), testNow.Add(time.Minute))
	if _, ok := nodeRecord(t, cl, "node7"); ok {
		t.Fatal("fresh complete empty set did not clear episode")
	}
}

func TestReporterNodePersistenceFailureDoesNotReplayEvents(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(map[string]string{"state.json": "preserve"}))
	rec := &capturingRecorder{}
	r.Recorder = rec
	fail, conflict := true, false
	r.Client = interceptor.NewClient(cl.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if fail {
			return errors.New("temporary write failure")
		}
		if conflict {
			conflict = false
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("changed"))
		}
		return c.Update(ctx, obj, opts...)
	}})
	c := remediationCandidate("node7", unhealthyObservation(), "prod/a")
	reportMarker(r, c, testNow)
	reportMarker(r, c, testNow)
	if rec.count("NodeRepairNeeded") != 1 {
		t.Fatalf("persistence failure replayed signal: %+v", rec.events)
	}
	fail = false
	conflict = true
	reportMarker(r, c, testNow)
	v, ok := nodeRecord(t, cl, "node7")
	if !ok || !v.ObservedAt.Equal(testNow) || rec.count("NodeRepairNeeded") != 1 || conflict {
		t.Fatalf("retry lost phase or duplicated signal: %+v", v)
	}
}

func TestReporterNodeSeedFailurePreservesRecordsAndNormalCycle(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	c := remediationCandidate("node7", unhealthyObservation(), "prod/a")
	reportMarker(r, c, testNow)
	rec := &capturingRecorder{}
	readsFail := true
	reader := interceptor.NewClient(cl.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if readsFail {
			readsFail = false
			return errors.New("seed unavailable")
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	r = &Reporter{Client: cl, DirectReader: reader, Recorder: rec, Metrics: metrics.New(prometheus.NewRegistry()), Log: logr.Discard(), Namespace: "ome"}
	reportMarker(r, c, testNow.Add(time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if len(rec.events) != 0 || !v.ObservedAt.Equal(testNow) {
		t.Fatalf("failed seed advanced lifecycle: %+v events=%+v", v, rec.events)
	}
	var cm corev1.ConfigMap
	_ = cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	var cycle cycleRecord
	if json.Unmarshal([]byte(cm.Data[recommendationsKey]), &cycle) != nil || !cycle.Timestamp.Equal(testNow.Add(time.Hour)) {
		t.Fatal("seed failure suppressed normal cycle record")
	}
	reportMarker(r, c, testNow.Add(time.Minute))
	if len(rec.events) != 0 {
		t.Fatalf("successful seed replayed signal: %+v", rec.events)
	}
}

func TestReporterNodeRecordRetainsPublicConditionJSON(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	reportMarker(r, remediationCandidate("node7", unhealthyObservation()), testNow)
	var cm corev1.ConfigMap
	_ = cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	var doc struct {
		Conditions []map[string]json.RawMessage `json:"conditions"`
	}
	if err := json.Unmarshal([]byte(cm.Data["node.node7"]), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Conditions) != 1 || string(doc.Conditions[0]["type"]) != `"GpuUnhealthy"` || string(doc.Conditions[0]["status"]) != `"True"` || len(doc.Conditions[0]["lastTransitionTime"]) == 0 {
		t.Fatalf("condition record schema changed: %+v", doc)
	}
}

func TestReporterNodeMaximumNameSurvivesRestartAndClear(t *testing.T) {
	node := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	c := remediationCandidate(node, unhealthyObservation(), "prod/a")
	reportMarker(r, c, testNow)
	var cm corev1.ConfigMap
	_ = cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	count := 0
	for key := range cm.Data {
		if strings.HasPrefix(key, "node-hash.") {
			count++
			if len(key) > 253 {
				t.Fatal("invalid long-node ConfigMap key")
			}
		}
	}
	if count != 1 {
		t.Fatal("missing durable long-node record")
	}
	rec := &capturingRecorder{}
	r = &Reporter{Client: cl, Recorder: rec, Metrics: metrics.New(prometheus.NewRegistry()), Log: logr.Discard(), Namespace: "ome"}
	reportMarker(r, c, testNow.Add(time.Minute))
	if len(rec.events) != 0 {
		t.Fatalf("long-node restart replayed signal: %+v", rec.events)
	}
	r.ReportCycle(context.Background(), nil, nil, config.Default(), testNow.Add(time.Hour), testNow.Add(2*time.Minute))
	_ = cl.Get(context.Background(), client.ObjectKey{Namespace: "ome", Name: "alfred-recommendations"}, &cm)
	for key := range cm.Data {
		if strings.HasPrefix(key, "node-hash.") {
			t.Fatal("fresh clearing left long-node record")
		}
	}
}

func TestReporterNodeMarkerTimestampMustMatchCompleteSnapshot(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	rec := &capturingRecorder{}
	r.Recorder = rec
	c := remediationCandidate("node7", unhealthyObservation())
	r.ReportCycle(context.Background(), []policy.Candidate{c}, nil, config.Default(), testNow.Add(time.Hour), testNow.Add(time.Minute))
	if _, ok := nodeRecord(t, cl, "node7"); ok || len(rec.events) != 0 {
		t.Fatal("mismatched marker advanced lifecycle")
	}
}

func TestReporterNodeDisabledPersistenceDoesNotResurrectOldEpisode(t *testing.T) {
	r, _, _, cl := newTestReporter(t, recommendationsCM(nil))
	rec := &capturingRecorder{}
	r.Recorder = rec
	c := remediationCandidate("node7", unhealthyObservation(), "prod/a")
	reportMarker(r, c, testNow)
	cfg := config.Default()
	*cfg.RecommendationsConfigMapEnabled = false
	r.ReportCycle(context.Background(), nil, nil, cfg, testNow.Add(time.Hour), testNow.Add(time.Minute))
	reportMarker(r, c, testNow.Add(2*time.Minute))
	v, _ := nodeRecord(t, cl, "node7")
	if rec.count("NodeRepairNeeded") != 2 || v.SignaledAt == nil || !v.SignaledAt.Equal(testNow.Add(2*time.Minute)) {
		t.Fatalf("enabled persistence revived cleared phase: %+v", v)
	}
}
