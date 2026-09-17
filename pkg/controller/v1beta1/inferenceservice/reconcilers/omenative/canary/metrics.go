package canary

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// Every per-ISVC vector carries both a "namespace" and an "isvc" label so
// same-named ISVCs across namespaces do not collide, and so the series can be
// dropped wholesale on ISVC teardown (see DeleteForISVC) to bound cardinality.
var (
	canaryPhaseTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_phase_total",
		Help: "Count of canary reconcile passes by resulting RolloutPhase.",
	}, []string{"namespace", "isvc", "phase"})

	canaryStepTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_step_total",
		Help: "Count of canary step advances per (namespace, isvc, component).",
	}, []string{"namespace", "isvc", "component"})

	canaryCurrentStep = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_current_step",
		Help: "Zero-based index of the active canary step per (namespace, isvc, component); 0 when no canary is in progress.",
	}, []string{"namespace", "isvc", "component"})

	canaryTrafficWeight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_traffic_weight",
		Help: "External traffic weight currently programmed for the canary revision per (namespace, isvc, component).",
	}, []string{"namespace", "isvc", "component"})

	canaryCompleteTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_complete_total",
		Help: "Count of canary rollouts that reached a terminal state (Stable after promote or rollback).",
	}, []string{"namespace", "isvc"})

	// Metric-gated canary promotion observability.
	canaryAnalysisEvaluations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_analysis_evaluations_total",
		Help: "Count of canary analysis samples by outcome (pass|fail|inconclusive) per (namespace, isvc, component).",
	}, []string{"namespace", "isvc", "component", "result"})

	canaryAnalysisMetricValue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_analysis_metric_value",
		Help: "Last observed value of each canary analysis metric per (namespace, isvc, component, metric).",
	}, []string{"namespace", "isvc", "component", "metric"})

	canaryAnalysisFailedChecks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ome_canary_analysis_failed_checks",
		Help: "Failing analysis samples accrued in the current step per (namespace, isvc, component); resets on advance.",
	}, []string{"namespace", "isvc", "component"})

	canaryRollbackTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_canary_rollback_total",
		Help: "Count of canary rollbacks by cause (analysis|manual) per (namespace, isvc, component).",
	}, []string{"namespace", "isvc", "component", "cause"})

	// Sampler saturation observability. MaxConcurrency is a fleet-wide ceiling, so
	// at hundreds of concurrent canaries queries queue behind the semaphore and the
	// effective sample interval degrades — which can spuriously trip the analysis
	// stall timeout. These expose when that is happening so operators know to raise
	// MaxConcurrency (inferenceservice-config) for the fleet size.
	canarySamplerInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ome_canary_sampler_inflight",
		Help: "Background analysis queries currently executing (holding a concurrency slot), fleet-wide.",
	})

	canarySamplerQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ome_canary_sampler_queue_depth",
		Help: "Background analysis queries blocked waiting for a concurrency slot, fleet-wide; sustained >0 means MaxConcurrency is the bottleneck.",
	})

	canarySamplerStarvedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ome_canary_sampler_starved_total",
		Help: "Count of background analysis queries that had to wait for a concurrency slot before running.",
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		canaryPhaseTotal, canaryStepTotal, canaryCurrentStep, canaryTrafficWeight, canaryCompleteTotal,
		canaryAnalysisEvaluations, canaryAnalysisMetricValue, canaryAnalysisFailedChecks, canaryRollbackTotal,
		canarySamplerInflight, canarySamplerQueueDepth, canarySamplerStarvedTotal,
	)
}

// recordAnalysisSample emits the per-sample analysis metrics: the evaluation
// counter by outcome, the current failed-check gauge, and a value gauge per
// numeric metric result. Called once per actual sample — the Interval throttle in
// evaluateAnalysisStep guarantees that is at most once per Interval, so the
// counter reflects real samples, not reconcile frequency.
func recordAnalysisSample(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, res analysis.Result, failedChecks int32) {
	canaryAnalysisEvaluations.WithLabelValues(isvc.Namespace, isvc.Name, string(c), outcomeLabel(res.Outcome)).Inc()
	canaryAnalysisFailedChecks.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Set(float64(failedChecks))
	for _, m := range res.Metrics {
		if v, err := strconv.ParseFloat(m.Value, 64); err == nil {
			canaryAnalysisMetricValue.WithLabelValues(isvc.Namespace, isvc.Name, string(c), m.Name).Set(v)
		}
	}
}

// recordRollback emits the rollback counter with its cause (analysis|manual).
func recordRollback(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, cause string) {
	canaryRollbackTotal.WithLabelValues(isvc.Namespace, isvc.Name, string(c), cause).Inc()
}

// DeleteForISVC drops every canary series carrying the given
// (namespace, isvc) label pair. Called on terminal ISVC delete so the
// per-ISVC vectors do not leak unbounded series after teardown.
func DeleteForISVC(namespace, isvc string) {
	if isvc == "" {
		return
	}
	match := prometheus.Labels{"namespace": namespace, "isvc": isvc}
	canaryPhaseTotal.DeletePartialMatch(match)
	canaryStepTotal.DeletePartialMatch(match)
	canaryCurrentStep.DeletePartialMatch(match)
	canaryTrafficWeight.DeletePartialMatch(match)
	canaryCompleteTotal.DeletePartialMatch(match)
	canaryAnalysisEvaluations.DeletePartialMatch(match)
	canaryAnalysisMetricValue.DeletePartialMatch(match)
	canaryAnalysisFailedChecks.DeletePartialMatch(match)
	canaryRollbackTotal.DeletePartialMatch(match)
}

func outcomeLabel(o analysis.Outcome) string {
	switch o {
	case analysis.Pass:
		return "pass"
	case analysis.Fail:
		return "fail"
	default:
		return "inconclusive"
	}
}

// recordDispatch emits canary metrics + Events for one reconcile pass. The
// counters increment only on edges (a step advance, a completion), the gauges
// are set idempotently, so re-reconciles of an unchanged step do not inflate.
func recordDispatch(rec record.EventRecorder, isvc *v1beta1.InferenceService, c v1beta1.ComponentType, res *Result) {
	if res == nil || !res.Active {
		return
	}
	// A completed canary keeps its status at the done sentinel (CurrentStep ==
	// len(steps)), so the step/weight gauges + step Event read live status here.
	cs := rollout.CanaryStatusFor(&isvc.Status, c)
	phase := isvc.Status.Components[c].RolloutPhase
	canaryPhaseTotal.WithLabelValues(isvc.Namespace, isvc.Name, string(phase)).Inc()
	if cs != nil {
		canaryCurrentStep.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Set(float64(cs.CurrentStep))
		canaryTrafficWeight.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Set(float64(cs.ObservedTrafficWeight))
		if res.Stepped {
			canaryStepTotal.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Inc()
			if rec != nil {
				// Report only the new step index — the new step's traffic weight is
				// programmed on the next reconcile, so pairing it with the
				// currently-programmed (previous step's) weight would mislead.
				rec.Eventf(isvc, corev1.EventTypeNormal, EventReasonCanaryStepAdvanced,
					"canary advanced to step %d", cs.CurrentStep)
			}
		}
	}
	if res.Complete {
		canaryCompleteTotal.WithLabelValues(isvc.Namespace, isvc.Name).Inc()
		// Reset the in-progress gauges: the canary is done, no active step/split.
		canaryCurrentStep.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Set(0)
		canaryTrafficWeight.WithLabelValues(isvc.Namespace, isvc.Name, string(c)).Set(0)
		if rec != nil {
			rec.Eventf(isvc, corev1.EventTypeNormal, EventReasonCanaryCompleted,
				"canary rollout completed; %s is now stable at 100%%", c)
		}
	}
}
