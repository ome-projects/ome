package obsmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// counterFor fetches the sample matching labels from a CounterVec by name
// from the controller-runtime registry. Returns 0 when the metric exists
// but the label combo has no sample yet, and -1 only when Gather errors.
func counterFor(metricName string, labels prometheus.Labels) float64 {
	g, err := metrics.Registry.Gather()
	if err != nil {
		return -1
	}
	for _, mf := range g {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if v, want := labels[l.GetName()]; want && v != l.GetValue() {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

// histogramFor fetches the (sampleCount, sampleSum) of the histogram
// sample matching labels. Returns (0,0) when no sample yet.
func histogramFor(metricName string, labels prometheus.Labels) (uint64, float64) {
	g, err := metrics.Registry.Gather()
	if err != nil {
		return 0, 0
	}
	for _, mf := range g {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for _, l := range m.GetLabel() {
				if v, want := labels[l.GetName()]; want && v != l.GetValue() {
					matched = false
					break
				}
			}
			if !matched || m.Histogram == nil {
				continue
			}
			return m.Histogram.GetSampleCount(), m.Histogram.GetSampleSum()
		}
	}
	return 0, 0
}

func histogramBucketFor(metricName string, labels prometheus.Labels, upperBound float64) uint64 {
	gathered, err := metrics.Registry.Gather()
	if err != nil {
		return 0
	}
	for _, family := range gathered {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.Histogram == nil || !labelsMatch(metric.GetLabel(), labels) {
				continue
			}
			for _, bucket := range metric.Histogram.GetBucket() {
				if bucket.GetUpperBound() == upperBound {
					return bucket.GetCumulativeCount()
				}
			}
		}
	}
	return 0
}

func gaugeFor(metricName string, labels prometheus.Labels) (float64, bool) {
	gathered, err := metrics.Registry.Gather()
	if err != nil {
		return 0, false
	}
	for _, family := range gathered {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.Gauge == nil || !labelsMatch(metric.GetLabel(), labels) {
				continue
			}
			return metric.Gauge.GetValue(), true
		}
	}
	return 0, false
}

func labelsMatch(pairs []*dto.LabelPair, labels prometheus.Labels) bool {
	if len(pairs) != len(labels) {
		return false
	}
	for _, pair := range pairs {
		if value, ok := labels[pair.GetName()]; !ok || value != pair.GetValue() {
			return false
		}
	}
	return true
}

func TestRecordStatusUpdate_IncrementsPerResult(t *testing.T) {
	for _, result := range []string{ResultSuccess, ResultConflict, ResultNotFound, ResultError} {
		labels := prometheus.Labels{"controller": ControllerISVC, "result": result}
		start := counterFor("ome_isvc_status_update_total", labels)
		RecordStatusUpdate(ControllerISVC, result)
		RecordStatusUpdate(ControllerISVC, result)
		if end := counterFor("ome_isvc_status_update_total", labels); end-start != 2 {
			t.Errorf("result %q: got delta %v want 2", result, end-start)
		}
	}
}

func TestRecordStatusUpdate_ControllerLabelIsolated(t *testing.T) {
	ir := prometheus.Labels{"controller": ControllerIR, "result": ResultConflict}
	start := counterFor("ome_isvc_status_update_total", ir)
	RecordStatusUpdate(ControllerIR, ResultConflict)
	if end := counterFor("ome_isvc_status_update_total", ir); end-start != 1 {
		t.Errorf("IR conflict: got delta %v want 1", end-start)
	}
	// The IR increment must not bleed into the ISVC series.
	isvc := prometheus.Labels{"controller": ControllerISVC, "result": ResultConflict}
	before := counterFor("ome_isvc_status_update_total", isvc)
	RecordStatusUpdate(ControllerIR, ResultConflict)
	if after := counterFor("ome_isvc_status_update_total", isvc); after != before {
		t.Errorf("IR write must not touch ISVC series; got %v want %v", after, before)
	}
}

func TestRecordStatusUpdate_EmptyArgsNoOp(t *testing.T) {
	RecordStatusUpdate("", ResultSuccess)
	RecordStatusUpdate(ControllerISVC, "")
	// Empty-label combos never land a sample.
	if v := counterFor("ome_isvc_status_update_total", prometheus.Labels{"controller": "", "result": ResultSuccess}); v != 0 {
		t.Errorf("empty controller must short-circuit; got %v", v)
	}
}

func TestRecordRolloutPhaseDuration_Observes(t *testing.T) {
	labels := prometheus.Labels{"phase": "Surging"}
	startCount, startSum := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	RecordRolloutPhaseDuration("Surging", 2.5)
	RecordRolloutPhaseDuration("Surging", 1.5)
	cnt, sum := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	if cnt-startCount != 2 {
		t.Errorf("count delta: got %v want 2", cnt-startCount)
	}
	if d := sum - startSum; d < 3.99 || d > 4.01 {
		t.Errorf("sum delta: got %v want ~4.0", d)
	}
}

func TestRecordRolloutPhaseDuration_NonPositiveDropped(t *testing.T) {
	labels := prometheus.Labels{"phase": "Draining"}
	startCount, _ := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels)
	RecordRolloutPhaseDuration("Draining", 0)
	RecordRolloutPhaseDuration("Draining", -1)
	RecordRolloutPhaseDuration("", 5)
	if cnt, _ := histogramFor("ome_omenative_rollout_phase_duration_seconds", labels); cnt != startCount {
		t.Errorf("non-positive/empty must be dropped; count moved from %v to %v", startCount, cnt)
	}
}

func TestRecordPodCreateToReady_Observes(t *testing.T) {
	labels := prometheus.Labels{"namespace": "prod", "isvc": "llama", "component": "engine"}
	startCount, startSum := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	RecordPodCreateToReady("prod", "llama", "engine", 10)
	cnt, sum := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	if cnt-startCount != 1 {
		t.Errorf("count delta: got %v want 1", cnt-startCount)
	}
	if d := sum - startSum; d < 9.99 || d > 10.01 {
		t.Errorf("sum delta: got %v want ~10.0", d)
	}
}

func TestRecordPodCreateToReady_NonPositiveDropped(t *testing.T) {
	labels := prometheus.Labels{"namespace": "prod", "isvc": "llama", "component": "decoder"}
	startCount, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels)
	RecordPodCreateToReady("prod", "llama", "decoder", 0)
	RecordPodCreateToReady("prod", "llama", "decoder", -3)
	RecordPodCreateToReady("", "llama", "decoder", 5)
	RecordPodCreateToReady("prod", "", "decoder", 5)
	RecordPodCreateToReady("prod", "llama", "", 5)
	if cnt, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", labels); cnt != startCount {
		t.Errorf("non-positive/empty must be dropped; count moved from %v to %v", startCount, cnt)
	}
}

func TestRecordPodCreateToReady_ISVCLabelIsolated(t *testing.T) {
	first := prometheus.Labels{"namespace": "prod", "isvc": "llama-a", "component": "engine"}
	second := prometheus.Labels{"namespace": "prod", "isvc": "llama-b", "component": "engine"}
	firstStart, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", first)
	secondStart, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", second)

	RecordPodCreateToReady("prod", "llama-a", "engine", 10)

	firstEnd, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", first)
	secondEnd, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", second)
	if firstEnd-firstStart != 1 {
		t.Errorf("first ISVC count delta: got %v want 1", firstEnd-firstStart)
	}
	if secondEnd != secondStart {
		t.Errorf("first ISVC observation changed second ISVC count from %v to %v", secondStart, secondEnd)
	}
}

func TestRecordScaleDownBatchPods(t *testing.T) {
	engine := prometheus.Labels{"component": "scale-down-batch-engine"}
	decoder := prometheus.Labels{"component": "scale-down-batch-decoder"}
	engineStartCount, engineStartSum := histogramFor("ome_omenative_scale_down_batch_pods", engine)
	engineStart128 := histogramBucketFor("ome_omenative_scale_down_batch_pods", engine, 128)
	decoderStartCount, _ := histogramFor("ome_omenative_scale_down_batch_pods", decoder)

	RecordScaleDownBatchPods("scale-down-batch-engine", 96)
	RecordScaleDownBatchPods("scale-down-batch-engine", 8)
	RecordScaleDownBatchPods("scale-down-batch-decoder", 4)
	RecordScaleDownBatchPods("scale-down-batch-engine", 0)
	RecordScaleDownBatchPods("scale-down-batch-engine", -1)
	RecordScaleDownBatchPods("", 10)

	engineCount, engineSum := histogramFor("ome_omenative_scale_down_batch_pods", engine)
	if engineCount-engineStartCount != 2 {
		t.Fatalf("engine sample count delta: got %d, want 2", engineCount-engineStartCount)
	}
	if delta := engineSum - engineStartSum; delta != 104 {
		t.Errorf("engine sample sum delta: got %v, want 104", delta)
	}
	if delta := histogramBucketFor("ome_omenative_scale_down_batch_pods", engine, 128) - engineStart128; delta != 2 {
		t.Errorf("engine <=128 bucket delta: got %d, want 2", delta)
	}
	decoderCount, _ := histogramFor("ome_omenative_scale_down_batch_pods", decoder)
	if decoderCount-decoderStartCount != 1 {
		t.Errorf("decoder sample count delta: got %d, want 1", decoderCount-decoderStartCount)
	}
}

func TestScaleDownGaugesSetResetAndCleanup(t *testing.T) {
	labels := prometheus.Labels{
		"namespace": "metrics-cleanup",
		"isvc":      "scale-down-gauges",
		"component": "engine",
	}
	DeleteScaleDownSeries("metrics-cleanup", "scale-down-gauges", "engine")
	if _, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); found {
		t.Fatal("active gauge exists before it is set")
	}
	if _, found := gaugeFor("ome_omenative_scale_down_deferred_instances", labels); found {
		t.Fatal("deferred gauge exists before it is set")
	}

	SetScaleDownActivePods("metrics-cleanup", "scale-down-gauges", "engine", 96)
	SetScaleDownDeferredInstances("metrics-cleanup", "scale-down-gauges", "engine", 1987)
	if got, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); !found || got != 96 {
		t.Errorf("active gauge: got (%v, %t), want (96, true)", got, found)
	}
	if got, found := gaugeFor("ome_omenative_scale_down_deferred_instances", labels); !found || got != 1987 {
		t.Errorf("deferred gauge: got (%v, %t), want (1987, true)", got, found)
	}
	DeleteScaleDownSeries("", "scale-down-gauges", "engine")
	if got, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); !found || got != 96 {
		t.Errorf("incomplete cleanup identity changed active gauge: got (%v, %t), want (96, true)", got, found)
	}

	SetScaleDownActivePods("metrics-cleanup", "scale-down-gauges", "engine", 0)
	SetScaleDownDeferredInstances("metrics-cleanup", "scale-down-gauges", "engine", 0)
	if got, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); !found || got != 0 {
		t.Errorf("reset active gauge: got (%v, %t), want (0, true)", got, found)
	}
	if got, found := gaugeFor("ome_omenative_scale_down_deferred_instances", labels); !found || got != 0 {
		t.Errorf("reset deferred gauge: got (%v, %t), want (0, true)", got, found)
	}

	DeleteScaleDownSeries("metrics-cleanup", "scale-down-gauges", "engine")
	DeleteScaleDownSeries("metrics-cleanup", "scale-down-gauges", "engine")
	if _, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); found {
		t.Error("active gauge survived series cleanup")
	}
	if _, found := gaugeFor("ome_omenative_scale_down_deferred_instances", labels); found {
		t.Error("deferred gauge survived series cleanup")
	}
}

func TestScaleDownGaugesRejectInvalidSamples(t *testing.T) {
	labels := prometheus.Labels{
		"namespace": "metrics-invalid",
		"isvc":      "scale-down-gauges",
		"component": "engine",
	}
	DeleteScaleDownSeries("metrics-invalid", "scale-down-gauges", "engine")
	SetScaleDownActivePods("metrics-invalid", "scale-down-gauges", "engine", -1)
	SetScaleDownDeferredInstances("metrics-invalid", "scale-down-gauges", "engine", -1)
	SetScaleDownActivePods("", "scale-down-gauges", "engine", 10)
	SetScaleDownDeferredInstances("metrics-invalid", "", "engine", 10)
	if _, found := gaugeFor("ome_omenative_scale_down_active_pods", labels); found {
		t.Error("invalid active sample created a series")
	}
	if _, found := gaugeFor("ome_omenative_scale_down_deferred_instances", labels); found {
		t.Error("invalid deferred sample created a series")
	}
}

func TestRecordScaleDownInstanceDuration(t *testing.T) {
	labels := prometheus.Labels{"component": "scale-down-duration-engine"}
	startCount, startSum := histogramFor("ome_omenative_scale_down_instance_duration_seconds", labels)
	start16 := histogramBucketFor("ome_omenative_scale_down_instance_duration_seconds", labels, 16)
	RecordScaleDownInstanceDuration("scale-down-duration-engine", 12.5)
	RecordScaleDownInstanceDuration("scale-down-duration-engine", 0)
	RecordScaleDownInstanceDuration("scale-down-duration-engine", -1)
	RecordScaleDownInstanceDuration("", 7)
	count, sum := histogramFor("ome_omenative_scale_down_instance_duration_seconds", labels)
	if count-startCount != 2 {
		t.Errorf("sample count delta: got %d, want 2", count-startCount)
	}
	if delta := sum - startSum; delta != 12.5 {
		t.Errorf("sample sum delta: got %v, want 12.5", delta)
	}
	if delta := histogramBucketFor("ome_omenative_scale_down_instance_duration_seconds", labels, 16) - start16; delta != 2 {
		t.Errorf("<=16s bucket delta: got %d, want 2", delta)
	}
}

func TestRecordScaleDownOversizedBatch(t *testing.T) {
	labels := prometheus.Labels{"component": "scale-down-oversized-engine"}
	start := counterFor("ome_omenative_scale_down_oversized_batch_total", labels)
	RecordScaleDownOversizedBatch("scale-down-oversized-engine")
	RecordScaleDownOversizedBatch("")
	if delta := counterFor("ome_omenative_scale_down_oversized_batch_total", labels) - start; delta != 1 {
		t.Errorf("counter delta: got %v, want 1", delta)
	}
}

func TestRecordISVCTimeToReady_Observes(t *testing.T) {
	labels := prometheus.Labels{"namespace": "prod", "isvc": "llama", "deploy_kind": DeployKindCreate}
	startCount, startSum := histogramFor("ome_isvc_time_to_ready_seconds", labels)
	RecordISVCTimeToReady("prod", "llama", DeployKindCreate, 120)
	cnt, sum := histogramFor("ome_isvc_time_to_ready_seconds", labels)
	if cnt-startCount != 1 {
		t.Errorf("count delta: got %v want 1", cnt-startCount)
	}
	if d := sum - startSum; d < 119.99 || d > 120.01 {
		t.Errorf("sum delta: got %v want ~120.0", d)
	}
}

func TestRecordISVCTimeToReady_DeployKindIsolated(t *testing.T) {
	create := prometheus.Labels{"namespace": "prod", "isvc": "mistral", "deploy_kind": DeployKindCreate}
	update := prometheus.Labels{"namespace": "prod", "isvc": "mistral", "deploy_kind": DeployKindUpdate}
	createStart, _ := histogramFor("ome_isvc_time_to_ready_seconds", create)
	updateStart, _ := histogramFor("ome_isvc_time_to_ready_seconds", update)

	RecordISVCTimeToReady("prod", "mistral", DeployKindUpdate, 30)

	createEnd, _ := histogramFor("ome_isvc_time_to_ready_seconds", create)
	updateEnd, _ := histogramFor("ome_isvc_time_to_ready_seconds", update)
	if updateEnd-updateStart != 1 {
		t.Errorf("update count delta: got %v want 1", updateEnd-updateStart)
	}
	if createEnd != createStart {
		t.Errorf("update observation changed create count from %v to %v", createStart, createEnd)
	}
}

func TestRecordISVCTimeToReady_InvalidSamplesDropped(t *testing.T) {
	labels := prometheus.Labels{"namespace": "prod", "isvc": "phi", "deploy_kind": DeployKindCreate}
	startCount, _ := histogramFor("ome_isvc_time_to_ready_seconds", labels)
	RecordISVCTimeToReady("prod", "phi", DeployKindCreate, 0)
	RecordISVCTimeToReady("prod", "phi", DeployKindCreate, -3)
	RecordISVCTimeToReady("", "phi", DeployKindCreate, 5)
	RecordISVCTimeToReady("prod", "", DeployKindCreate, 5)
	// An unrecognized deploy kind must not land a series: dashboards select
	// on the two stable values and a typo would silently split the SLI.
	RecordISVCTimeToReady("prod", "phi", "", 5)
	RecordISVCTimeToReady("prod", "phi", "recreate", 5)
	if cnt, _ := histogramFor("ome_isvc_time_to_ready_seconds", labels); cnt != startCount {
		t.Errorf("invalid samples must be dropped; count moved from %v to %v", startCount, cnt)
	}
	if cnt, _ := histogramFor("ome_isvc_time_to_ready_seconds",
		prometheus.Labels{"namespace": "prod", "isvc": "phi", "deploy_kind": "recreate"}); cnt != 0 {
		t.Errorf("unrecognized deploy_kind landed a series with count %v", cnt)
	}
}

// Readiness spans minutes for a cold model load, so the histogram must retain
// resolution well past DefBuckets' 10s ceiling or every real observation
// collapses into +Inf and the quantiles become unrecoverable.
func TestReadinessHistogramsResolveMinutes(t *testing.T) {
	labels := prometheus.Labels{"namespace": "prod", "isvc": "gemma", "deploy_kind": DeployKindCreate}
	const tenMinutes = 600
	start := histogramBucketFor("ome_isvc_time_to_ready_seconds", labels, 1024)
	RecordISVCTimeToReady("prod", "gemma", DeployKindCreate, tenMinutes)
	if got := histogramBucketFor("ome_isvc_time_to_ready_seconds", labels, 1024); got-start != 1 {
		t.Errorf("a %ds observation did not land in the le=1024 bucket (delta %v)", tenMinutes, got-start)
	}
}

func TestDeleteISVCSeries(t *testing.T) {
	ttr := prometheus.Labels{"namespace": "prod", "isvc": "doomed", "deploy_kind": DeployKindCreate}
	pcr := prometheus.Labels{"namespace": "prod", "isvc": "doomed", "component": "engine"}
	RecordISVCTimeToReady("prod", "doomed", DeployKindCreate, 42)
	RecordPodCreateToReady("prod", "doomed", "engine", 7)
	if cnt, _ := histogramFor("ome_isvc_time_to_ready_seconds", ttr); cnt == 0 {
		t.Fatal("precondition: time-to-ready series should exist before delete")
	}
	if cnt, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", pcr); cnt == 0 {
		t.Fatal("precondition: pod-create-to-ready series should exist before delete")
	}

	// Empty identities are a no-op rather than a wildcard delete.
	DeleteISVCSeries("", "doomed")
	DeleteISVCSeries("prod", "")
	if cnt, _ := histogramFor("ome_isvc_time_to_ready_seconds", ttr); cnt == 0 {
		t.Error("empty identity must not delete series")
	}

	DeleteISVCSeries("prod", "doomed")
	if cnt, _ := histogramFor("ome_isvc_time_to_ready_seconds", ttr); cnt != 0 {
		t.Errorf("time-to-ready series survived delete with count %v", cnt)
	}
	if cnt, _ := histogramFor("ome_omenative_pod_create_to_ready_seconds", pcr); cnt != 0 {
		t.Errorf("pod-create-to-ready series survived delete with count %v", cnt)
	}
}

func TestIRStatusBytesGaugeTracksOneEncodingAndCleansUp(t *testing.T) {
	dense := prometheus.Labels{"namespace": "team-a", "name": "chat-engine", "component": "engine", "encoding": IRStatusEncodingDenseV1}
	columnar := prometheus.Labels{"namespace": "team-a", "name": "chat-engine", "component": "engine", "encoding": IRStatusEncodingColumnarV2}

	SetIRStatusBytes("team-a", "chat-engine", "engine", IRStatusEncodingDenseV1, 4096)
	if got, found := gaugeFor("ome_omenative_ir_status_bytes", dense); !found || got != 4096 {
		t.Fatalf("dense gauge = %v (found %v), want 4096", got, found)
	}

	// A switch of representation replaces the series rather than leaving a
	// stale size under the other encoding.
	SetIRStatusBytes("team-a", "chat-engine", "engine", IRStatusEncodingColumnarV2, 512)
	if got, found := gaugeFor("ome_omenative_ir_status_bytes", columnar); !found || got != 512 {
		t.Fatalf("columnar gauge = %v (found %v), want 512", got, found)
	}
	if _, found := gaugeFor("ome_omenative_ir_status_bytes", dense); found {
		t.Fatal("dense series must be dropped when the IR is written as ColumnarV2")
	}

	// Invalid samples are dropped without touching the live series.
	SetIRStatusBytes("team-a", "chat-engine", "engine", "compressed", 1)
	SetIRStatusBytes("team-a", "chat-engine", "engine", IRStatusEncodingColumnarV2, -1)
	SetIRStatusBytes("", "chat-engine", "engine", IRStatusEncodingColumnarV2, 7)
	if got, found := gaugeFor("ome_omenative_ir_status_bytes", columnar); !found || got != 512 {
		t.Fatalf("columnar gauge after invalid samples = %v (found %v), want 512", got, found)
	}

	DeleteIRStatusSeries("team-a", "chat-engine", "engine")
	if _, found := gaugeFor("ome_omenative_ir_status_bytes", columnar); found {
		t.Fatal("series must be removed when the IR is torn down")
	}
}

func TestRecordIRStatusWriteFixedVocabulary(t *testing.T) {
	for _, result := range []string{IRStatusWriteAttempt, IRStatusWriteCommitted, IRStatusWriteConfirmed, IRStatusWriteConflict, IRStatusWriteRejected, IRStatusWriteError} {
		labels := prometheus.Labels{"encoding": IRStatusEncodingDenseV1, "result": result}
		start := counterFor("ome_omenative_ir_status_writes_total", labels)
		RecordIRStatusWrite(IRStatusEncodingDenseV1, result)
		if end := counterFor("ome_omenative_ir_status_writes_total", labels); end-start != 1 {
			t.Errorf("result %q: got delta %v want 1", result, end-start)
		}
	}
	for _, sample := range [][2]string{{"", IRStatusWriteAttempt}, {IRStatusEncodingDenseV1, ""}, {"DenseV1", IRStatusWriteAttempt}, {IRStatusEncodingColumnarV2, "success"}} {
		labels := prometheus.Labels{"encoding": sample[0], "result": sample[1]}
		start := counterFor("ome_omenative_ir_status_writes_total", labels)
		RecordIRStatusWrite(sample[0], sample[1])
		if end := counterFor("ome_omenative_ir_status_writes_total", labels); end != start {
			t.Errorf("sample %v: unknown label values must be dropped", sample)
		}
	}
}

func TestRecordIRStatusCodecError(t *testing.T) {
	labels := prometheus.Labels{"reason": "cardinality_limit"}
	start := counterFor("ome_omenative_ir_status_codec_errors_total", labels)
	RecordIRStatusCodecError("cardinality_limit")
	RecordIRStatusCodecError("")
	if end := counterFor("ome_omenative_ir_status_codec_errors_total", labels); end-start != 1 {
		t.Fatalf("got delta %v want 1", end-start)
	}
}

func TestRecordIRStatusConversion(t *testing.T) {
	labels := prometheus.Labels{"from": IRStatusEncodingColumnarV2, "to": IRStatusEncodingDenseV1}
	start := counterFor("ome_omenative_ir_status_conversions_total", labels)
	RecordIRStatusConversion(IRStatusEncodingColumnarV2, IRStatusEncodingDenseV1)
	RecordIRStatusConversion(IRStatusEncodingDenseV1, IRStatusEncodingDenseV1)
	RecordIRStatusConversion("ColumnarV2", IRStatusEncodingDenseV1)
	if end := counterFor("ome_omenative_ir_status_conversions_total", labels); end-start != 1 {
		t.Fatalf("got delta %v want 1", end-start)
	}
}
