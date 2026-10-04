package sliceprovision

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

var metricsLog = logf.Log.WithName("TPUSliceMetrics")

// sliceLabels are bounded by the configured slice types and topologies.
var sliceLabels = []string{"slice_type", "topology"}

// The states the slice gauge reports. Each provisioned slice is in exactly
// one.
const (
	stateTerminating = "terminating"
	stateOrphaned    = "orphaned"
	stateReady       = "ready"
	statePending     = "pending"
)

var (
	slicesCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_created_total",
		Help: "TPU slices the controller created for its workloads' pods, by slice type and topology.",
	}, sliceLabels)

	slicesReleased = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_released_total",
		Help: "TPU slices the controller provisioned that the API server has removed, whoever deleted them, by slice type and topology. A slice the provider holds behind a finalizer counts once the provider lets it go. Counted from the leader's watch, so a slice removed while no leader watches is not counted.",
	}, sliceLabels)

	sliceCreateFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_create_failures_total",
		Help: "TPU slice creates by the controller that failed, by slice type, topology and reason. The reason is the API server's status reason, such as Forbidden or Invalid; OwnershipConflict when another owner's slice has the name; or Unknown, as for a network error. The owner's reconcile retries the create, and each failed attempt counts.",
	}, []string{"slice_type", "topology", "reason"})

	sliceReleasesDeferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_release_deferred_total",
		Help: "Releases of TPU slices the controller provisioned that it kept back because pods of another workload hold chips on the slice's hosts, by slice type and topology. The owner's reconcile retries, and each deferred attempt counts.",
	}, sliceLabels)

	slicesLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_lost_total",
		Help: "TPU slices the controller provisioned that were deleted, or moved off the nodes its pods are bound to, from outside while its pods ran on them, by slice type and topology. Counted once the controller deletes the pods so that their Instance is rebuilt.",
	}, sliceLabels)
	sliceProvisionSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ome_tpu_slice_provision_duration_seconds",
		Help: "Seconds from the controller creating a TPU slice to its watch first seeing the slice in a ready state, by slice type and topology. Only slices the current leader created are timed, and a slice released before it is ready is not observed.",
		// OME's readiness buckets: one second to a little over four hours.
		Buckets: prometheus.ExponentialBuckets(1, 2, 15),
	}, sliceLabels)

	sliceStates = &stateCollector{desc: prometheus.NewDesc(
		"ome_tpu_slices",
		"TPU slices the controller provisioned that exist now, by slice type, topology and state. A slice being deleted is terminating. Otherwise it is orphaned when the owner it was provisioned for no longer exists, ready when its state is a configured ready state, and pending in any other state, including a failed one. Reported by the leader only.",
		[]string{"slice_type", "topology", "state"}, nil,
	)}
)

func init() {
	ctrlmetrics.Registry.MustRegister(slicesCreated, slicesReleased, sliceCreateFailures, sliceReleasesDeferred, slicesLost, sliceProvisionSeconds, sliceStates)
}

// InitSeries exports the created, released and provision duration series of
// each slice type and topology cfg can provision, at zero. A series that
// appears with its first slice is already past zero when first scraped, so
// rate() and increase() would miss that slice. The create failure series are
// left out: a failure's reason is known only when a create fails. The caller
// calls it once at startup, on every replica, so that a replica's series
// exist before it leads.
func InitSeries(cfg *controllerconfig.TPUSliceProvisioningConfig) {
	for _, a := range cfg.Accelerators {
		for _, topology := range a.Topologies {
			slicesCreated.WithLabelValues(a.SliceType, topology)
			slicesReleased.WithLabelValues(a.SliceType, topology)
			sliceReleasesDeferred.WithLabelValues(a.SliceType, topology)
			slicesLost.WithLabelValues(a.SliceType, topology)
			sliceProvisionSeconds.WithLabelValues(a.SliceType, topology)
		}
	}
}

// provisionStarts holds when this process created each slice its watch has
// not yet seen ready, by UID.
var provisionStarts = struct {
	sync.Mutex
	since map[types.UID]time.Time
}{since: map[types.UID]time.Time{}}

// provisionClock times provisioning. Tests replace it.
var provisionClock clock.PassiveClock = clock.RealClock{}

// provisionStarted starts timing s, which this process just created.
func provisionStarted(s gke.Slice) {
	if s.UID == "" {
		return
	}
	provisionStarts.Lock()
	defer provisionStarts.Unlock()
	provisionStarts.since[s.UID] = provisionClock.Now()
}

// ObserveReady times obj's provisioning when it is a slice this process
// created and readyStates make it ready for the first time. The caller calls
// it for each slice its watch sees created or updated.
func ObserveReady(obj client.Object, readyStates []string) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	provisionStarts.Lock()
	defer provisionStarts.Unlock()
	since, ok := provisionStarts.since[u.GetUID()]
	if !ok {
		return
	}
	s, err := gke.Parse(u)
	if err != nil || !s.Ready(readyStates) {
		return
	}
	delete(provisionStarts.since, u.GetUID())
	sliceProvisionSeconds.WithLabelValues(s.Type, s.Topology).Observe(provisionClock.Since(since).Seconds())
}

// createFailureReason is the reason a failed create counts under: the API
// server's status reason, OwnershipConflict, or Unknown.
func createFailureReason(err error) string {
	if errors.Is(err, gke.ErrOwnershipConflict) {
		return "OwnershipConflict"
	}
	if reason := apierrors.ReasonForError(err); reason != metav1.StatusReasonUnknown {
		return string(reason)
	}
	return "Unknown"
}

// ObserveDeleted counts obj as released when it is a slice provisioned for an
// owner, and stops timing its provisioning. The caller calls it once for each
// slice the API server removes.
func ObserveDeleted(obj client.Object) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	provisionStarts.Lock()
	delete(provisionStarts.since, u.GetUID())
	provisionStarts.Unlock()
	if u.GetLabels()[LabelOwnerUID] == "" {
		return
	}
	s, err := gke.Parse(u)
	if err != nil {
		return
	}
	slicesReleased.WithLabelValues(s.Type, s.Topology).Inc()
}

// StateSource is where the slice gauge reads the slices provisioned for one
// kind of owner. A scrape runs its reads, so they must not wait for an
// informer to sync.
type StateSource struct {
	// Slices reads slices.
	Slices client.Reader
	// Config is the configuration the slices were provisioned under.
	Config *controllerconfig.TPUSliceProvisioningConfig
	// OwnerKind is the kind of owner whose slices are reported.
	OwnerKind string
	// OwnerExists reports whether the owner with uid exists.
	OwnerExists func(ctx context.Context, uid types.UID) (bool, error)
}

// ReportStates reports src's slices on the slice gauge until stop is called.
// A later call replaces src; stopping a replaced source changes nothing.
func ReportStates(src StateSource) (stop func()) {
	s := &src
	sliceStates.source.Store(s)
	return func() { sliceStates.source.CompareAndSwap(s, nil) }
}

// stateCollector reports its source's slices when scraped, and nothing
// without a source.
type stateCollector struct {
	desc   *prometheus.Desc
	source atomic.Pointer[StateSource]
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	src := c.source.Load()
	if src == nil {
		return
	}
	counts, err := src.count(context.Background())
	if err != nil {
		metricsLog.Error(err, "Not reporting TPU slices by state in this scrape")
		return
	}
	for k, n := range counts {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), k.sliceType, k.topology, k.state)
	}
}

type stateKey struct {
	sliceType, topology, state string
}

// count counts src's slices by type, topology and state. It looks up each
// owner once.
func (src *StateSource) count(ctx context.Context) (map[stateKey]int, error) {
	provisioned, err := gke.NewClient(src.Slices, nil).List(ctx,
		labels.SelectorFromSet(labels.Set{src.Config.Slice.OwnerKindLabel: src.OwnerKind}))
	if err != nil {
		return nil, err
	}
	exists := map[string]bool{}
	counts := map[stateKey]int{}
	for _, s := range provisioned {
		uid := s.Labels[LabelOwnerUID]
		if uid == "" {
			continue
		}
		state := stateTerminating
		if !s.Terminating {
			live, seen := exists[uid]
			if !seen {
				if live, err = src.OwnerExists(ctx, types.UID(uid)); err != nil {
					return nil, err
				}
				exists[uid] = live
			}
			switch {
			case !live:
				state = stateOrphaned
			case s.Ready(src.Config.Slice.ReadyStates):
				state = stateReady
			default:
				state = statePending
			}
		}
		counts[stateKey{sliceType: s.Type, topology: s.Topology, state: state}]++
	}
	return counts, nil
}
