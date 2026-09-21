package modelagent

import (
	"net/http"
	"runtime"
	"time"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/hfutil/hub"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a struct that contains all metrics for the model-agent
type Metrics struct {
	// Counter metrics
	modelDownloadsSuccessTotal *prometheus.CounterVec
	modelDownloadsFailedTotal  *prometheus.CounterVec
	modelVerificationsTotal    *prometheus.CounterVec
	mdChecksumsFailedTotal     *prometheus.CounterVec
	rateLimitCounter           *prometheus.CounterVec

	// Histogram metrics
	modelDownloadDuration         *prometheus.HistogramVec
	modelVerificationDuration     prometheus.Histogram
	modelDownloadBytesTransferred *prometheus.CounterVec
	rateLimitWaitDuration         *prometheus.HistogramVec

	// Go runtime metrics
	goGoroutines      prometheus.Gauge
	goThreads         prometheus.Gauge
	goHeapObjects     prometheus.Gauge
	goGCDuration      prometheus.Histogram
	goMemoryAlloc     prometheus.Gauge
	goMemoryHeapAlloc prometheus.Gauge
	goMemoryHeapSys   prometheus.Gauge
	goMemoryStackSys  prometheus.Gauge
	goGCCount         prometheus.Counter
}

// NewMetrics creates a new Metrics struct with initialized Prometheus metrics
func NewMetrics(registerer prometheus.Registerer, modelsRootDir string) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	// There is no eviction: a full models root turns into failed downloads that
	// only a human can clear. Publishing free space is what makes that visible
	// before it happens, and what the console reads to warn before staging.
	modelsRootFreeBytes := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "model_agent_models_root_free_bytes",
		Help: "Free bytes on the filesystem holding the models root directory",
	})
	updateModelsRootFreeBytes := func() {
		available, err := hub.AvailableDiskSpace(modelsRootDir)
		if err != nil {
			// An unreadable models root must not take the agent down; 0 reads
			// as "unknown or full", which is the safe direction for an alert.
			modelsRootFreeBytes.Set(0)
			return
		}
		modelsRootFreeBytes.Set(float64(available))
	}
	// Publish once synchronously so the gauge is meaningful immediately after
	// startup rather than after the first tick.
	updateModelsRootFreeBytes()

	// Manual Go metrics for more detailed tracking
	goGoroutines := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_goroutines_current",
		Help: "Current number of goroutines",
	})

	goThreads := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_threads_current",
		Help: "Current number of OS threads",
	})

	goHeapObjects := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_heap_objects_current",
		Help: "Current number of heap objects",
	})

	goGCDuration := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:    "go_gc_pause_duration_seconds_custom",
		Help:    "Custom: GC pause duration in seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15), // From 100us to ~3s
	})

	// Memory metrics
	goMemoryAlloc := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_memory_alloc_bytes",
		Help: "Currently allocated memory in bytes",
	})

	goMemoryHeapAlloc := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_memory_heap_alloc_bytes",
		Help: "Heap memory allocated in bytes",
	})

	goMemoryHeapSys := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_memory_heap_sys_bytes",
		Help: "Heap memory obtained from system in bytes",
	})

	goMemoryStackSys := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "go_memory_stack_sys_bytes",
		Help: "Stack memory obtained from system in bytes",
	})

	goGCCount := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "go_gc_count_total",
		Help: "Total number of garbage collections",
	})

	// Start a goroutine to periodically update Go runtime metrics
	go func() {
		memStats := &runtime.MemStats{}
		var lastGC uint32

		for {
			runtime.ReadMemStats(memStats)

			// Update metrics
			goGoroutines.Set(float64(runtime.NumGoroutine()))
			goThreads.Set(float64(runtime.NumCPU()))
			goHeapObjects.Set(float64(memStats.HeapObjects))

			// Memory metrics
			goMemoryAlloc.Set(float64(memStats.Alloc))
			goMemoryHeapAlloc.Set(float64(memStats.HeapAlloc))
			goMemoryHeapSys.Set(float64(memStats.HeapSys))
			goMemoryStackSys.Set(float64(memStats.StackSys))

			// GC metrics - only record if a new GC has occurred
			if memStats.NumGC > lastGC {
				// Count how many new GCs have happened
				newGCs := memStats.NumGC - lastGC
				goGCCount.Add(float64(newGCs))
				lastGC = memStats.NumGC

				// Record the most recent GC pause time
				if newGCs > 0 {
					// Calculate index of the most recent GC pause
					pauseIndex := int(memStats.NumGC % 256)
					if pauseIndex == 0 {
						pauseIndex = 255
					} else {
						pauseIndex--
					}
					goGCDuration.Observe(float64(memStats.PauseNs[pauseIndex]) / 1e9)
				}
			}

			updateModelsRootFreeBytes()

			time.Sleep(15 * time.Second)
		}
	}()

	return &Metrics{
		modelDownloadsSuccessTotal: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_downloads_success_total",
				Help: "The total number of successful model downloads",
			},
			[]string{"model_type", "namespace", "name"},
		),
		modelDownloadsFailedTotal: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_downloads_failed_total",
				Help: "The total number of failed model downloads",
			},
			[]string{"model_type", "namespace", "name"},
		),
		modelVerificationsTotal: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_verifications_total",
				Help: "The total number of model verification attempts",
			},
			[]string{"model_type", "namespace", "name", "result"},
		),
		mdChecksumsFailedTotal: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_md5_checksum_failed_total",
				Help: "The total number of MD5 checksum failures",
			},
			[]string{"model_type", "namespace", "name"},
		),
		rateLimitCounter: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_rate_limit_total",
				Help: "The total number of rate limit (429) responses encountered",
			},
			[]string{"model_type", "namespace", "name"},
		),
		modelDownloadDuration: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "model_agent_download_duration_seconds",
				Help:    "The duration of model downloads in seconds",
				Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // From 0.1s to ~1.7m
			},
			[]string{"model_type", "namespace", "name"},
		),
		modelVerificationDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "model_agent_verification_duration_seconds",
			Help:    "The duration of model verifications in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // From 0.1s to ~1.7m
		}),
		modelDownloadBytesTransferred: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "model_agent_download_bytes_total",
				Help: "The total bytes transferred while downloading models",
			},
			[]string{"model_type", "namespace", "name"},
		),
		rateLimitWaitDuration: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "model_agent_rate_limit_wait_seconds",
				Help:    "The duration waited due to rate limits in seconds",
				Buckets: prometheus.ExponentialBuckets(1, 2, 10), // From 1s to ~17m
			},
			[]string{"model_type", "namespace", "name"},
		),
		// Store Go runtime metrics
		goGoroutines:      goGoroutines,
		goThreads:         goThreads,
		goHeapObjects:     goHeapObjects,
		goGCDuration:      goGCDuration,
		goMemoryAlloc:     goMemoryAlloc,
		goMemoryHeapAlloc: goMemoryHeapAlloc,
		goMemoryHeapSys:   goMemoryHeapSys,
		goMemoryStackSys:  goMemoryStackSys,
		goGCCount:         goGCCount,
	}
}

// RecordSuccessfulDownload records a successful model download
func (m *Metrics) RecordSuccessfulDownload(modelType, namespace, name string) {
	m.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name).Inc()
}

// RecordFailedDownload records a failed model download
func (m *Metrics) RecordFailedDownload(modelType, namespace, name, errorType string) {
	m.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name).Inc()
}

// RecordVerification records a model verification
func (m *Metrics) RecordVerification(modelType, namespace, name string, success bool) {
	result := "success"
	if !success {
		result = "failure"
		m.mdChecksumsFailedTotal.WithLabelValues(modelType, namespace, name).Inc()
	}
	m.modelVerificationsTotal.WithLabelValues(modelType, namespace, name, result).Inc()
}

// ObserveDownloadDuration records the duration of a model download
func (m *Metrics) ObserveDownloadDuration(modelType, namespace, name string, duration time.Duration) {
	m.modelDownloadDuration.WithLabelValues(modelType, namespace, name).Observe(duration.Seconds())
}

// ObserveVerificationDuration records the duration of a model verification
func (m *Metrics) ObserveVerificationDuration(duration time.Duration) {
	m.modelVerificationDuration.Observe(duration.Seconds())
}

// RecordBytesTransferred records the number of bytes transferred during a download
func (m *Metrics) RecordBytesTransferred(modelType, namespace, name string, bytes int64) {
	m.modelDownloadBytesTransferred.WithLabelValues(modelType, namespace, name).Add(float64(bytes))
}

// RecordGCDuration records the duration of a garbage collection cycle
func (m *Metrics) RecordGCDuration(duration time.Duration) {
	m.goGCDuration.Observe(duration.Seconds())
}

// RecordRateLimit records a rate limit event
func (m *Metrics) RecordRateLimit(modelType, namespace, name string, waitDuration time.Duration) {
	m.rateLimitCounter.WithLabelValues(modelType, namespace, name).Inc()
	m.rateLimitWaitDuration.WithLabelValues(modelType, namespace, name).Observe(waitDuration.Seconds())
}

// RegisterMetricsHandler registers the metrics HTTP handler
func RegisterMetricsHandler(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
}

// GetModelTypeNamespaceAndName extracts the model type, namespace, and name from a gopher task
func GetModelTypeNamespaceAndName(task *GopherTask) (string, string, string) {
	if task.BaseModel != nil {
		return constants.BaseModel, task.BaseModel.Namespace, task.BaseModel.Name
	} else if task.ClusterBaseModel != nil {
		return constants.ClusterBaseModel, "", task.ClusterBaseModel.Name
	}
	return "unknown", "unknown", "unknown"
}
