package workloadcluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// DefaultHealthInterval is the fallback re-probe cadence used when
// Reconciler.HealthInterval is unset — how often a WorkloadCluster is re-probed
// for reachability when nothing else triggers a reconcile. The operative value
// is supplied by the manager flag / chart; this is the graceful-degradation
// default.
const DefaultHealthInterval = time.Minute

// DefaultConnectionGracePeriod is the fallback window during which a cluster
// that was previously reachable tolerates transient probe failures before it is
// flipped to Ready=False and disconnected. A single failed /version probe (a
// momentary blip) must not tear down a live, working connection. The operative
// value is supplied by the manager flag / chart; this is the
// graceful-degradation default. A negative ConnectionGracePeriod disables
// grace (flip to Ready=False on the first failure); zero means "use this
// default".
const DefaultConnectionGracePeriod = 2 * time.Minute

const kubeConfigSecretRefIndex = "spec.clusterSource.kubeConfig.secretRef"

// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconciler reconciles a WorkloadCluster: it resolves the cluster's
// kubeconfig, probes reachability, and writes the Ready condition. It does NOT
// read capacity — that is a placer concern.
type Reconciler struct {
	client.Client
	Log logr.Logger
	// Probe reports whether the cluster described by these kubeconfig bytes is
	// reachable. Defaults to probeViaServerVersion; overridden in tests.
	Probe func(ctx context.Context, kubeconfig []byte) error
	// HealthInterval is the re-probe cadence; defaults to DefaultHealthInterval.
	HealthInterval time.Duration
	// Manager holds the live per-cluster clients; the reconciler connects a
	// cluster when it becomes Reachable and disconnects it on removal. Optional
	// (nil-safe) so the health-only path keeps working without transport.
	Manager *Manager
	// ExecPolicy is the exec credential policy applied during validation.
	ExecPolicy ExecCredentialPolicy
	// SecretReader reads kubeconfig Secrets WITHOUT caching them. The Secret watch
	// is metadata-only, so payloads never enter the informer cache; reading the
	// bytes through the cached client would defeat that by starting a second,
	// full-payload Secret informer over every namespace. SetupWithManager defaults
	// it to mgr.GetAPIReader(); nil falls back to the reconciler's own client.
	SecretReader client.Reader
	// ConnectionGracePeriod is how long a previously reachable cluster tolerates
	// transient probe failures before it is flipped to Ready=False and
	// disconnected; zero defaults to DefaultConnectionGracePeriod. A negative
	// value disables grace.
	ConnectionGracePeriod time.Duration
	// ProbeTimeout bounds the default reachability probe's single request. It is
	// the same budget as a remote per-call timeout, so the control-plane wiring
	// feeds it the configured per-call value. Zero falls back to the health
	// interval, which is always positive: the probe must never outlast the
	// cadence that schedules the next one, and this controller runs one worker.
	ProbeTimeout time.Duration

	// mu guards health. Each record is read and written only by its own
	// cluster's pass, and controller-runtime runs one pass per key at a time.
	mu sync.Mutex
	// health holds one record per registered cluster; a removed or replaced
	// registration drops its record.
	health map[string]*clusterHealth

	// cfg is the resolved timing configuration assembled at SetupWithManager
	// from functional Options (seeded by the HealthInterval/ConnectionGracePeriod
	// struct fields). Nil until Setup runs — the helper methods fall back to the
	// struct fields + Default* consts so a Reconciler built directly (unit tests)
	// keeps working without Setup.
	cfg *controllerConfig
}

// clusterHealth is what the controller remembers about one cluster between
// passes. The failure clock runs from the first failed reachability probe
// until a probe succeeds, and bounds the grace window. The failure count grows
// with every pass that ends disconnected and resets when the cluster connects;
// it indexes the reconnect backoff.
type clusterHealth struct {
	firstFailure time.Time
	failures     uint
}

// startFailureClock begins the failure run at now unless one is running.
func (h *clusterHealth) startFailureClock(now time.Time) {
	if h.firstFailure.IsZero() {
		h.firstFailure = now
	}
}

func (h *clusterHealth) stopFailureClock() {
	h.firstFailure = time.Time{}
}

// countFailure adds a pass that ended disconnected and returns the run length.
func (h *clusterHealth) countFailure() uint {
	h.failures++
	return h.failures
}

func (h *clusterHealth) resetFailures() {
	h.failures = 0
}

// Ready condition reasons.
const (
	reasonReachable                 = "Reachable"
	reasonConnectionFailed          = "ConnectionFailed"
	reasonProbeFailedRetrying       = "ProbeFailedRetrying"
	reasonSecretNotFound            = "SecretNotFound"
	reasonBadKubeConfig             = "BadKubeConfig"
	reasonClusterProfileUnsupported = "ClusterProfileUnsupported"
)

// probeOutcome is what one pass learned about a cluster: the Ready condition
// it earns and, when it is reachable, the kubeconfig bytes to connect with.
type probeOutcome struct {
	status     metav1.ConditionStatus
	reason     string
	message    string
	kubeconfig []byte
}

func notReady(reason, message string) probeOutcome {
	return probeOutcome{status: metav1.ConditionFalse, reason: reason, message: message}
}

func (o probeOutcome) reachable() bool {
	return o.status == metav1.ConditionTrue
}

// connectionFailed reports a reachability failure, the one kind the grace
// window and the reconnect backoff apply to. The other failures are
// configuration problems and are acted on at once.
func (o probeOutcome) connectionFailed() bool {
	return o.status != metav1.ConditionTrue && o.reason == reasonConnectionFailed
}

// heldWithinGrace is the outcome reported while a live connection is kept
// through a transient probe failure: Ready stays True and the message names
// the failure.
func (o probeOutcome) heldWithinGrace() probeOutcome {
	return probeOutcome{
		status:  metav1.ConditionTrue,
		reason:  reasonProbeFailedRetrying,
		message: fmt.Sprintf("transient probe failure, holding connection within grace period: %s", o.message),
	}
}

// readyCondition is the Ready condition this outcome earns on the cluster's
// current generation.
func (o probeOutcome) readyCondition(wc *v1beta1.WorkloadCluster) metav1.Condition {
	return metav1.Condition{
		Type:               v1beta1.WorkloadClusterReady,
		Status:             o.status,
		Reason:             o.reason,
		Message:            o.message,
		ObservedGeneration: wc.Generation,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wc := &v1beta1.WorkloadCluster{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		if apierrors.IsNotFound(err) {
			r.forgetConnection(req.Name)
			if r.Manager != nil {
				r.Manager.MarkConnectionDecided(req.Name, "")
			}
			// The member is gone: drop its series so it stops counting toward
			// the fleet size reported by ome_workload_cluster_ready.
			deleteForCluster(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if r.Manager != nil {
		if _, connected := r.Manager.ClientFor(wc.Name); connected {
			if _, matches := r.Manager.ClientForUID(wc.Name, wc.UID); !matches {
				r.forgetConnection(wc.Name)
			}
		}
	}

	outcome := r.assess(ctx, wc)
	if r.Manager != nil {
		// Every path below settles whether this registration is connected, so a
		// missing connection after this pass is a decision, not a pending one.
		defer r.Manager.MarkConnectionDecided(wc.Name, wc.UID)
	}
	health := r.healthOf(wc.Name)

	// A transient probe failure must not tear down a live connection: while the
	// cluster still holds a client and its failure run is inside the grace
	// window, Ready stays True and the client stays connected. Re-probing sooner
	// than the steady cadence confirms a real outage once grace expires.
	if outcome.connectionFailed() && r.withinGrace(wc.Name, health) {
		apimeta.SetStatusCondition(&wc.Status.Conditions, outcome.heldWithinGrace().readyCondition(wc))
		return r.finish(ctx, wc, r.graceRequeue())
	}

	if outcome.connectionFailed() {
		health.startFailureClock(time.Now())
	} else {
		health.stopFailureClock()
	}
	apimeta.SetStatusCondition(&wc.Status.Conditions, outcome.readyCondition(wc))
	connFailed := outcome.connectionFailed()
	if r.Manager != nil {
		if outcome.reachable() {
			if err := r.Manager.ConnectFor(r.connectCtx(), wc.Name, wc.UID, outcome.kubeconfig); err != nil {
				health.startFailureClock(time.Now())
				connFailed = true
				apimeta.SetStatusCondition(&wc.Status.Conditions, notReady(reasonConnectionFailed, err.Error()).readyCondition(wc))
			}
		} else {
			r.Manager.Disconnect(wc.Name)
		}
	}
	// A down cluster is re-probed on the reconnect backoff (0 to about 5m20s)
	// rather than at every health interval; the count resets once it connects.
	requeue := r.healthInterval()
	if connFailed {
		requeue = r.backoff().retryAfter(health.countFailure())
	} else {
		health.resetFailures()
	}
	return r.finish(ctx, wc, requeue)
}

// finish writes the status and returns the steady requeue.
func (r *Reconciler) finish(ctx context.Context, wc *v1beta1.WorkloadCluster, requeue time.Duration) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, wc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		// wc was read through the cache, so a pass that runs before the cache
		// caught up writes at a stale resourceVersion and loses the race. The
		// next pass recomputes the same status from fresh state, so requeue
		// rather than reporting a failure that did not happen.
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("workloadcluster %s: update status: %w", wc.Name, err)
	}
	// Publish only after the status write landed, so the gauges track the
	// condition that was actually persisted.
	recordWorkloadCluster(wc)
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// connectCtx returns the long-lived context that remote clients (and their
// future cache informers) must derive from, so they outlive the request-scoped
// reconcile ctx and are torn down only by Manager.Disconnect / Stop. Falls back
// to context.Background() when no base context was wired (e.g. unit tests that
// build the Reconciler directly).
func (r *Reconciler) connectCtx() context.Context {
	if r.Manager != nil {
		if base := r.Manager.BaseContext(); base != nil {
			return base
		}
	}
	return context.Background()
}

func (r *Reconciler) gracePeriod() time.Duration {
	if r.cfg != nil {
		return r.cfg.connectionGracePeriod
	}
	if r.ConnectionGracePeriod != 0 {
		return r.ConnectionGracePeriod
	}
	return DefaultConnectionGracePeriod
}

// graceRequeue is the re-probe cadence used while holding a connection within
// the grace window. It is the shorter of the health interval and the grace
// period so the outage is confirmed promptly without busy-looping.
func (r *Reconciler) graceRequeue() time.Duration {
	g := r.gracePeriod()
	h := r.healthInterval()
	if g > 0 && g < h {
		return g
	}
	return h
}

// withinGrace reports whether a probe failure is still inside the grace
// window: the cluster holds a live client and its failure run, which this call
// starts if none is running, is shorter than the grace period. A cluster with
// no connection (or no Manager) has nothing to hold, so it is never within grace.
func (r *Reconciler) withinGrace(name string, health *clusterHealth) bool {
	if r.Manager == nil {
		return false
	}
	if _, connected := r.Manager.ClientFor(name); !connected {
		return false
	}
	g := r.gracePeriod()
	if g <= 0 {
		return false
	}
	now := time.Now()
	health.startFailureClock(now)
	return now.Sub(health.firstFailure) < g
}

// healthOf returns the cluster's record, creating it on first use.
func (r *Reconciler) healthOf(name string) *clusterHealth {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.health == nil {
		r.health = map[string]*clusterHealth{}
	}
	h, ok := r.health[name]
	if !ok {
		h = &clusterHealth{}
		r.health[name] = h
	}
	return h
}

// forgetConnection drops the cluster's client and its health record, so the
// next pass starts as it would for a new registration.
func (r *Reconciler) forgetConnection(name string) {
	if r.Manager != nil {
		r.Manager.Disconnect(name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.health, name)
}

// backoff is the reconnect schedule Setup resolved, or the default for a
// Reconciler built directly (unit tests).
func (r *Reconciler) backoff() reconnectBackoff {
	if r.cfg != nil {
		return r.cfg.reconnect
	}
	return defaultReconnectBackoff()
}

// assess resolves the cluster's kubeconfig and probes reachability. The Probe
// is injected so this is unit-testable without a real cluster.
func (r *Reconciler) assess(ctx context.Context, wc *v1beta1.WorkloadCluster) probeOutcome {
	src := wc.Spec.ClusterSource
	if src.KubeConfig == nil {
		return notReady(reasonClusterProfileUnsupported, "clusterProfileRef is not yet supported; use kubeConfig")
	}

	key := src.KubeConfig.Key
	if key == "" {
		key = "kubeconfig"
	}
	secret := &corev1.Secret{}
	nn := types.NamespacedName{Name: src.KubeConfig.SecretRef.Name, Namespace: src.KubeConfig.SecretRef.Namespace}
	if err := r.secretReader().Get(ctx, nn, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return notReady(reasonSecretNotFound, fmt.Sprintf("kubeconfig Secret %s/%s not found", nn.Namespace, nn.Name))
		}
		// Local-apiserver read failures are transient transport failures, not a
		// missing configuration. Reuse the connection grace path so one timeout
		// does not discard an otherwise healthy remote client.
		return notReady(reasonConnectionFailed, fmt.Sprintf("reading kubeconfig Secret %s/%s: %v", nn.Namespace, nn.Name, err))
	}
	raw, ok := secret.Data[key]
	if !ok || len(raw) == 0 {
		return notReady(reasonBadKubeConfig, fmt.Sprintf("Secret %s/%s has no %q key", nn.Namespace, nn.Name, key))
	}
	// Validate the kubeconfig (parse + security: reject exec/token-file/insecure)
	// BEFORE probing. This keeps "malformed/unsafe kubeconfig" (BadKubeConfig)
	// distinct from "cluster unreachable" (ConnectionFailed), and ensures the
	// security validation runs regardless of which probe is in use.
	if _, err := RESTConfigFromKubeConfig(raw, r.ExecPolicy); err != nil {
		return notReady(reasonBadKubeConfig, err.Error())
	}
	if err := r.Probe(ctx, raw); err != nil {
		return notReady(reasonConnectionFailed, err.Error())
	}
	return probeOutcome{status: metav1.ConditionTrue, reason: reasonReachable, kubeconfig: raw}
}

func (r *Reconciler) healthInterval() time.Duration {
	if r.cfg != nil {
		return r.cfg.healthInterval
	}
	if r.HealthInterval > 0 {
		return r.HealthInterval
	}
	return DefaultHealthInterval
}

// secretReader returns the uncached reader for kubeconfig Secrets, falling back
// to the reconciler's client when none was wired (unit tests build it directly).
func (r *Reconciler) secretReader() client.Reader {
	if r.SecretReader != nil {
		return r.SecretReader
	}
	return r.Client
}

// probeTimeout is the client-level bound for one reachability probe. It comes
// from config (workloadCluster.perCallTimeout); when that is unset the health
// interval bounds it instead, so an unreachable endpoint can never hold the
// single reconcile worker past the next scheduled probe.
func (r *Reconciler) probeTimeout() time.Duration {
	if r.cfg != nil {
		if r.cfg.probeTimeout > 0 {
			return r.cfg.probeTimeout
		}
		if r.cfg.healthInterval > 0 {
			return r.cfg.healthInterval
		}
	}
	return r.ProbeTimeout
}

// SetupWithManager wires the reconciler: watch WorkloadClusters, and re-enqueue
// (debounced by the events-batch period) when a referenced kubeconfig Secret
// changes. Functional Options shrink the timing knobs (health interval,
// worker-lost timeout, reconnect backoff, events-batch period) for tests.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...Option) error {
	if r.Probe == nil {
		r.Probe = func(ctx context.Context, raw []byte) error {
			return probeViaServerVersion(ctx, raw, r.ExecPolicy, r.probeTimeout())
		}
	}
	if r.SecretReader == nil {
		r.SecretReader = mgr.GetAPIReader()
	}
	cfg := r.resolveConfig(opts...)
	r.cfg = &cfg
	// Propagate the resolved reconnect backoff to the transport so reconnect
	// attempts use the configured establish/retry schedule.
	if r.Manager != nil {
		r.Manager.SetReconnectBackoff(cfg.reconnect)
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1beta1.WorkloadCluster{}, kubeConfigSecretRefIndex, kubeConfigSecretRefValues); err != nil {
		return fmt.Errorf("index WorkloadClusters by kubeconfig Secret: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.WorkloadCluster{}).
		// Metadata-only: this controller needs Secret CHANGE events, never cached
		// Secret payloads. A full informer would hold every Secret in the cluster
		// in memory and widen the blast radius of a compromised manager pod.
		Watches(&corev1.Secret{}, r.secretEventHandler(cfg.eventsBatchPeriod), builder.OnlyMetadata).
		Complete(r)
}

func kubeConfigSecretRefValues(obj client.Object) []string {
	wc := obj.(*v1beta1.WorkloadCluster)
	kc := wc.Spec.ClusterSource.KubeConfig
	if kc == nil || kc.SecretRef.Name == "" || kc.SecretRef.Namespace == "" {
		return nil
	}
	return []string{types.NamespacedName{Namespace: kc.SecretRef.Namespace, Name: kc.SecretRef.Name}.String()}
}

// secretEventHandler re-enqueues the WorkloadClusters referencing a changed
// Secret, debounced by batchPeriod via AddAfter so a kubeconfig rotation that
// touches several Secret keys back-to-back folds into one reconcile (and thus
// one remote-client rebuild) instead of a burst.
func (r *Reconciler) secretEventHandler(batchPeriod time.Duration) handler.EventHandler {
	enqueue := func(ctx context.Context, obj client.Object, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
		for _, req := range r.workloadClustersForSecret(ctx, obj) {
			if batchPeriod > 0 {
				q.AddAfter(req, batchPeriod)
			} else {
				q.Add(req)
			}
		}
	}
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			enqueue(ctx, e.Object, q)
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			enqueue(ctx, e.ObjectNew, q)
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			enqueue(ctx, e.Object, q)
		},
	}
}

// workloadClustersForSecret maps a Secret change to the WorkloadClusters whose
// kubeConfig.secretRef points at it.
func (r *Reconciler) workloadClustersForSecret(ctx context.Context, obj client.Object) []ctrl.Request {
	list := &v1beta1.WorkloadClusterList{}
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}.String()
	if err := r.List(ctx, list, client.MatchingFields{kubeConfigSecretRefIndex: key}); err != nil {
		r.Log.Error(err, "list WorkloadClusters for kubeconfig Secret", "secret", key)
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Name: list.Items[i].Name}})
	}
	return reqs
}
