// Package obsmetrics holds cross-controller scale-operability Prometheus
// collectors shared by the ISVC reconciler, the IR reconciler, the
// OMENative coordination layer, and the workload op dispatcher. It is a
// leaf package (only depends on prometheus + controller-runtime metrics)
// so every controller can wire it without import cycles. Collectors are
// registered with the controller-runtime registry at package init so the
// operator's /metrics endpoint surfaces them with no extra wiring.
//
// Cardinality is deliberately controlled: metrics avoid pod-level labels.
// The pod startup histogram and active scale-down gauges carry the stable ISVC
// identity so operators can compare workloads and locate deferred work. This
// assumes tens to low hundreds of ISVCs per cluster; pod identities are
// excluded because fleets may contain thousands of pods and pod names churn
// across rollouts.
package obsmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Status-update result label values for RecordStatusUpdate. Stable
// strings — dashboards and alerts key off them, so they must not change
// silently if the call-site classification is refactored.
const (
	// ResultSuccess is recorded when the status write committed.
	ResultSuccess = "success"
	// ResultConflict is recorded when RetryOnConflict exhausted its
	// budget on a 409 — the known OMENative status-churn hotspot.
	ResultConflict = "conflict"
	// ResultNotFound is recorded when the object vanished mid-flush
	// (raced a delete); the write is dropped cleanly, not an error.
	ResultNotFound = "notfound"
	// ResultError is recorded for any other terminal write failure.
	ResultError = "error"
)

// Controller label values for RecordStatusUpdate.
const (
	// ControllerISVC tags the parent InferenceService status flush.
	ControllerISVC = "isvc"
	// ControllerIR tags the per-Component InferenceReplica status flush.
	ControllerIR = "ir"
)

// deploy_kind label values for RecordISVCTimeToReady. Stable strings —
// dashboards and alerts key off them.
const (
	// DeployKindCreate is recorded when the ISVC reached Ready for the
	// first time, so the measurement covers an initial deployment.
	DeployKindCreate = "create"
	// DeployKindUpdate is recorded when the ISVC had previously been Ready,
	// so the measurement covers a rollout of an already-serving service.
	DeployKindUpdate = "update"
)

// Encoding label values for the InferenceReplica status write-boundary
// metrics. Fixed vocabulary: a representation name never appears as a label
// in any other spelling.
const (
	// IRStatusEncodingDenseV1 tags the dense instanceStatuses list.
	IRStatusEncodingDenseV1 = "dense_v1"
	// IRStatusEncodingColumnarV2 tags the instanceStatusColumns payload.
	IRStatusEncodingColumnarV2 = "columnar_v2"
)

// Result label values for RecordIRStatusWrite. Every attempt records
// IRStatusWriteAttempt and then exactly one terminal result.
const (
	// IRStatusWriteAttempt is recorded before each status update request.
	IRStatusWriteAttempt = "attempt"
	// IRStatusWriteCommitted is recorded when the update succeeded.
	IRStatusWriteCommitted = "committed"
	// IRStatusWriteConfirmed is recorded when the update returned an error
	// but a live read proved the intended state was committed.
	IRStatusWriteConfirmed = "confirmed"
	// IRStatusWriteConflict is recorded for a 409; the caller re-reads and
	// retries from the fresh object.
	IRStatusWriteConflict = "conflict"
	// IRStatusWriteRejected is recorded when the API server refused the
	// request as too large.
	IRStatusWriteRejected = "rejected"
	// IRStatusWriteError is recorded for every other failure.
	IRStatusWriteError = "error"
)

var (
	// Histogram buckets affect exported measurement resolution only; they do
	// not tune controller behavior. Pod-cost buckets cover large fleet changes,
	// while duration buckets retain resolution from one second through
	// multi-hour drains.
	scaleDownBatchPodBuckets = prometheus.ExponentialBuckets(1, 2, 12)
	scaleDownDurationBuckets = prometheus.ExponentialBuckets(1, 2, 15)

	// Model-serving readiness spans seconds (a warm pod restarting) to tens of
	// minutes (a cold multi-hundred-gigabyte weight load), so these buckets run
	// from one second to a little over four hours. Upstream's DefBuckets tops
	// out at ten seconds, which puts every real observation in +Inf and makes
	// the quantiles unrecoverable.
	readinessDurationBuckets = prometheus.ExponentialBuckets(1, 2, 15)

	statusUpdateTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_isvc_status_update_total",
			Help: "Count of status-update attempts by controller and terminal result (success|conflict|notfound|error). Surfaces the OMENative 409 conflict hotspot.",
		},
		[]string{"controller", "result"},
	)

	// rolloutPhaseDurationSeconds records how long an OMENative
	// coordination group dwelt in a phase before transitioning out of
	// it. Labeled by the phase being left (NOT per-isvc) to keep
	// cardinality bounded by the small coordination-phase enum.
	rolloutPhaseDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ome_omenative_rollout_phase_duration_seconds",
			Help: "Wall-clock seconds a coordination group spent in a phase before transitioning out, labeled by the phase being left.",
			// A group dwells in Waiting or Draining for minutes, so the
			// bucket ceiling has to clear that. Bucket layout is a dashboard
			// concern, not a behavioral one (it never changes a deployment
			// outcome, timeout, or limit).
			Buckets: readinessDurationBuckets,
		},
		[]string{"phase"},
	)

	// podCreateToReadySeconds records pod-create-to-ready latency for an
	// Instance: the gap between the earliest pod's creation timestamp and
	// the moment the Instance was first promoted to Ready.
	podCreateToReadySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "ome_omenative_pod_create_to_ready_seconds",
			Help: "Wall-clock seconds from the earliest pod's creation to the Instance first reaching Ready, labeled by namespace, ISVC, and component.",
			// Readiness buckets (see above): a serving pod pulling an image
			// and localizing weights routinely takes minutes.
			Buckets: readinessDurationBuckets,
		},
		[]string{"namespace", "isvc", "component"},
	)

	// isvcTimeToReadySeconds records end-to-end deployment latency: the gap
	// between the ISVC last losing readiness (or being created, for a first
	// deploy) and the aggregate Ready condition next going True. Unlike
	// podCreateToReadySeconds it spans admission, runtime selection, and
	// model localization, because it is anchored on the ISVC's own condition
	// rather than on a pod that does not exist until those steps finish.
	//
	// deployKind separates a first deploy from a rollout of an existing
	// service: they have different expected durations and mixing them makes
	// the quantiles bimodal.
	isvcTimeToReadySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ome_isvc_time_to_ready_seconds",
			Help:    "Wall-clock seconds from an InferenceService losing readiness (or being created) to its aggregate Ready condition next going True, labeled by namespace, ISVC, and deploy kind (create|update).",
			Buckets: readinessDurationBuckets,
		},
		[]string{"namespace", "isvc", "deploy_kind"},
	)

	scaleDownBatchPods = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ome_omenative_scale_down_batch_pods",
			Help:    "Pod-equivalent cost selected in one OMENative scale-down wave, labeled by component.",
			Buckets: scaleDownBatchPodBuckets,
		},
		[]string{"component"},
	)

	scaleDownActivePods = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_scale_down_active_pods",
			Help: "Current Pod-equivalent cost selected for OMENative scale-down, labeled by namespace, ISVC, and component.",
		},
		[]string{"namespace", "isvc", "component"},
	)

	scaleDownDeferredInstances = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_scale_down_deferred_instances",
			Help: "Eligible OMENative scale-down Instances outside the active prefix, labeled by namespace, ISVC, and component.",
		},
		[]string{"namespace", "isvc", "component"},
	)

	scaleDownInstanceDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ome_omenative_scale_down_instance_duration_seconds",
			Help:    "Wall-clock seconds from OMENative Delete admission to confirmed Instance status removal, labeled by component.",
			Buckets: scaleDownDurationBuckets,
		},
		[]string{"component"},
	)

	scaleDownOversizedBatchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_scale_down_oversized_batch_total",
			Help: "Count of OMENative scale-down waves that admit one indivisible Instance above the configured Pod budget, labeled by component.",
		},
		[]string{"component"},
	)

	// irStatusBytes is the serialized size of the status carried by the most
	// recent InferenceReplica status write attempt. It is per IR so an object
	// approaching the API request limit can be located before a write is
	// rejected; the encoding label separates the two representations.
	irStatusBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ome_omenative_ir_status_bytes",
			Help: "Serialized bytes of the InferenceReplica status in the most recent status write attempt, labeled by namespace, IR name, component, and encoding (dense_v1|columnar_v2).",
		},
		[]string{"namespace", "name", "component", "encoding"},
	)

	irStatusWritesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ir_status_writes_total",
			Help: "Count of InferenceReplica status write attempts and their terminal results (attempt|committed|confirmed|conflict|rejected|error), labeled by the written encoding.",
		},
		[]string{"encoding", "result"},
	)

	irStatusCodecErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ir_status_codec_errors_total",
			Help: "Count of InferenceReplica per-Instance status codec failures by fixed-catalog reason.",
		},
		[]string{"reason"},
	)

	irStatusConversionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ome_omenative_ir_status_conversions_total",
			Help: "Count of committed representation-only InferenceReplica status writes, labeled by the source and target encoding.",
		},
		[]string{"from", "to"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		statusUpdateTotal,
		rolloutPhaseDurationSeconds,
		podCreateToReadySeconds,
		isvcTimeToReadySeconds,
		scaleDownBatchPods,
		scaleDownActivePods,
		scaleDownDeferredInstances,
		scaleDownInstanceDurationSeconds,
		scaleDownOversizedBatchTotal,
		irStatusBytes,
		irStatusWritesTotal,
		irStatusCodecErrorsTotal,
		irStatusConversionsTotal,
	)
}

// SetIRStatusBytes publishes the serialized status size of one InferenceReplica
// status write attempt. An IR carries one representation at a time, so the
// series for the other encoding is dropped rather than left at its last
// value. Incomplete identity, an unknown encoding, and negative sizes are
// dropped.
func SetIRStatusBytes(namespace, name, component, encoding string, bytes int) {
	if !validScaleDownIdentity(namespace, name, component) || bytes < 0 {
		return
	}
	var other string
	switch encoding {
	case IRStatusEncodingDenseV1:
		other = IRStatusEncodingColumnarV2
	case IRStatusEncodingColumnarV2:
		other = IRStatusEncodingDenseV1
	default:
		return
	}
	irStatusBytes.DeleteLabelValues(namespace, name, component, other)
	irStatusBytes.WithLabelValues(namespace, name, component, encoding).Set(float64(bytes))
}

// DeleteIRStatusSeries removes the per-IR status size series when the IR
// disappears, for both encodings.
func DeleteIRStatusSeries(namespace, name, component string) {
	if !validScaleDownIdentity(namespace, name, component) {
		return
	}
	irStatusBytes.DeleteLabelValues(namespace, name, component, IRStatusEncodingDenseV1)
	irStatusBytes.DeleteLabelValues(namespace, name, component, IRStatusEncodingColumnarV2)
}

// RecordIRStatusWrite counts one status write attempt or terminal result.
// Unknown encodings and results are dropped so the label vocabulary stays
// fixed.
func RecordIRStatusWrite(encoding, result string) {
	if !validIRStatusEncoding(encoding) {
		return
	}
	switch result {
	case IRStatusWriteAttempt, IRStatusWriteCommitted, IRStatusWriteConfirmed,
		IRStatusWriteConflict, IRStatusWriteRejected, IRStatusWriteError:
	default:
		return
	}
	irStatusWritesTotal.WithLabelValues(encoding, result).Inc()
}

// RecordIRStatusCodecError counts one codec failure by its fixed-catalog
// reason. Empty reasons are dropped.
func RecordIRStatusCodecError(reason string) {
	if reason == "" {
		return
	}
	irStatusCodecErrorsTotal.WithLabelValues(reason).Inc()
}

// RecordIRStatusConversion counts one committed representation-only write
// from one encoding to another. Unknown or equal encodings are dropped.
func RecordIRStatusConversion(from, to string) {
	if !validIRStatusEncoding(from) || !validIRStatusEncoding(to) || from == to {
		return
	}
	irStatusConversionsTotal.WithLabelValues(from, to).Inc()
}

func validIRStatusEncoding(encoding string) bool {
	return encoding == IRStatusEncodingDenseV1 || encoding == IRStatusEncodingColumnarV2
}

// RecordStatusUpdate increments the status-update result counter for one
// terminal status-write outcome. Empty args are a no-op so a misclassified
// call site never lands a blank-label series.
func RecordStatusUpdate(controller, result string) {
	if controller == "" || result == "" {
		return
	}
	statusUpdateTotal.WithLabelValues(controller, result).Inc()
}

// RecordRolloutPhaseDuration observes how long a coordination group spent
// in the phase it is leaving. Non-positive durations and empty phase
// labels are dropped so a clock skew or unobserved-prior-phase never
// pollutes the histogram.
func RecordRolloutPhaseDuration(phase string, seconds float64) {
	if phase == "" || seconds <= 0 {
		return
	}
	rolloutPhaseDurationSeconds.WithLabelValues(phase).Observe(seconds)
}

// RecordPodCreateToReady observes pod-create-to-ready latency for an
// Instance promotion. Non-positive durations and empty identity labels are
// dropped.
func RecordPodCreateToReady(namespace, isvc, component string, seconds float64) {
	if namespace == "" || isvc == "" || component == "" || seconds <= 0 {
		return
	}
	podCreateToReadySeconds.WithLabelValues(namespace, isvc, component).Observe(seconds)
}

// RecordISVCTimeToReady observes end-to-end deployment latency for one
// not-Ready-to-Ready transition of an InferenceService. Callers must invoke
// it only on the edge, and only after the status write that carried the
// transition has committed, so a level-triggered reconcile loop cannot
// re-observe the same deployment. Non-positive durations, empty identity
// labels, and unrecognized deploy kinds are dropped.
func RecordISVCTimeToReady(namespace, isvc, deployKind string, seconds float64) {
	if namespace == "" || isvc == "" || seconds <= 0 {
		return
	}
	if deployKind != DeployKindCreate && deployKind != DeployKindUpdate {
		return
	}
	isvcTimeToReadySeconds.WithLabelValues(namespace, isvc, deployKind).Observe(seconds)
}

// DeleteISVCSeries removes the per-ISVC readiness series when their owning
// ISVC disappears. Both vectors carry the ISVC identity, so without this they
// would retain a series per ISVC ever deployed for the lifetime of the
// process. Partial matching drops every deploy-kind and component variant
// under the identity in one call.
func DeleteISVCSeries(namespace, isvc string) {
	if namespace == "" || isvc == "" {
		return
	}
	id := prometheus.Labels{"namespace": namespace, "isvc": isvc}
	isvcTimeToReadySeconds.DeletePartialMatch(id)
	podCreateToReadySeconds.DeletePartialMatch(id)
}

// RecordScaleDownBatchPods observes the Pod-equivalent cost admitted into one
// scale-down wave. A wave always has positive cost; invalid observations and
// empty component labels are dropped.
func RecordScaleDownBatchPods(component string, pods int32) {
	if component == "" || pods <= 0 {
		return
	}
	scaleDownBatchPods.WithLabelValues(component).Observe(float64(pods))
}

// SetScaleDownActivePods publishes the current active scale-down wave cost.
// Zero resets a converged IR's series; negative values and incomplete identity
// labels are dropped.
func SetScaleDownActivePods(namespace, isvc, component string, pods int32) {
	if !validScaleDownIdentity(namespace, isvc, component) || pods < 0 {
		return
	}
	scaleDownActivePods.WithLabelValues(namespace, isvc, component).Set(float64(pods))
}

// SetScaleDownDeferredInstances publishes the eligible candidate count outside
// the active scale-down prefix. Zero resets a converged IR's series; negative
// values and incomplete identity labels are dropped.
func SetScaleDownDeferredInstances(namespace, isvc, component string, instances int) {
	if !validScaleDownIdentity(namespace, isvc, component) || instances < 0 {
		return
	}
	scaleDownDeferredInstances.WithLabelValues(namespace, isvc, component).Set(float64(instances))
}

// DeleteScaleDownSeries removes per-IR gauges when their owning IR disappears.
// The scale-down histograms and counters are labeled by component alone, so
// their cardinality is bounded and they remain; the ISVC-labeled readiness
// histograms are cleaned up by DeleteISVCSeries instead.
func DeleteScaleDownSeries(namespace, isvc, component string) {
	if !validScaleDownIdentity(namespace, isvc, component) {
		return
	}
	scaleDownActivePods.DeleteLabelValues(namespace, isvc, component)
	scaleDownDeferredInstances.DeleteLabelValues(namespace, isvc, component)
}

// RecordScaleDownInstanceDuration observes a completed Instance's time from
// durable Delete admission to confirmed status removal. Zero is valid for
// podless work completed within the timestamp precision; negative durations
// and empty component labels are dropped.
func RecordScaleDownInstanceDuration(component string, seconds float64) {
	if component == "" || seconds < 0 {
		return
	}
	scaleDownInstanceDurationSeconds.WithLabelValues(component).Observe(seconds)
}

// RecordScaleDownOversizedBatch increments once when an indivisible Instance
// is admitted above the configured Pod budget.
func RecordScaleDownOversizedBatch(component string) {
	if component == "" {
		return
	}
	scaleDownOversizedBatchTotal.WithLabelValues(component).Inc()
}

func validScaleDownIdentity(namespace, isvc, component string) bool {
	return namespace != "" && isvc != "" && component != ""
}
