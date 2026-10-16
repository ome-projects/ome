package modeleviction

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/artifactinventory"
	"sigs.k8s.io/ome/pkg/modelusage"
)

// SetupWithManager wires indexes and watches before the manager starts. The
// manager must cache all namespaces and all serving objects, without selectors.
// Node, ConfigMap and ReplicationJob reads bypass the reference cache.
func SetupWithManager(ctx context.Context, mgr ctrl.Manager, capacity CapacityReader, options Options) error {
	if _, err := options.validate(); err != nil {
		return err
	}
	sources := []client.Object{
		&v1beta1.InferenceService{}, &corev1.Pod{}, &appsv1.Deployment{},
		&appsv1.ReplicaSet{}, &appsv1.StatefulSet{}, &v1beta1.InferenceReplica{},
	}
	queryOptions := modelusage.SourceOptions{}
	// An installed LWS API must participate in protection. Restart this controller
	// after installing that optional CRD so its watch and index are registered.
	_, err := mgr.GetRESTMapper().RESTMapping(lws.SchemeGroupVersion.WithKind("LeaderWorkerSet").GroupKind(), lws.SchemeGroupVersion.Version)
	switch {
	case err == nil:
		queryOptions.LeaderWorkerSets = true
		sources = append(sources, &lws.LeaderWorkerSet{})
	case !meta.IsNoMatchError(err):
		return fmt.Errorf("discover LeaderWorkerSet API: %w", err)
	}
	var informers []cache.Informer
	hasSynced := func() bool {
		if len(informers) == 0 {
			return false
		}
		for _, informer := range informers {
			if !informer.HasSynced() {
				return false
			}
		}
		return true
	}
	query := modelusage.NewReferenceQuery(mgr.GetCache(), hasSynced, queryOptions)
	if err := query.RegisterIndexes(ctx, mgr.GetFieldIndexer()); err != nil {
		return err
	}
	// Give uncached reads a per-request deadline without putting a short HTTP
	// timeout on the long-running informer watches.
	apiConfig := rest.CopyConfig(mgr.GetConfig())
	apiConfig.Timeout = options.RequestTimeout
	apiReader, err := client.New(apiConfig, client.Options{Scheme: mgr.GetScheme(), Mapper: mgr.GetRESTMapper()})
	if err != nil {
		return err
	}
	evaluator := modelusage.NewEvaluator(apiReader, mgr.GetClient(), query, time.Now)
	controller, err := NewController(apiReader, artifactinventory.NewBuilder(apiReader, options.ModelNamespace), evaluator, capacity, options)
	if err != nil {
		return err
	}
	worker := newWorker(controller.RunOnce, evaluator, options, mgr.GetCache().WaitForCacheSync)

	// BaseModel updates provide the final-claim-release event. Workload removals
	// remain necessary for models whose serving users do not yet write claims.
	for _, source := range append(sources, &v1beta1.BaseModel{}) {
		informer, err := mgr.GetCache().GetInformer(ctx, source, cache.BlockUntilSynced(false))
		if err != nil {
			return fmt.Errorf("watch %T: %w", source, err)
		}
		if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObject, newObject any) {
				oldModel, oldOK := oldObject.(client.Object)
				newModel, newOK := newObject.(client.Object)
				if oldOK && newOK {
					worker.removed(oldModel, newModel)
				}
			},
			DeleteFunc: worker.deleted,
		}); err != nil {
			return fmt.Errorf("handle %T events: %w", source, err)
		}
		informers = append(informers, informer)
	}
	if err := mgr.AddReadyzCheck("references", func(*http.Request) error {
		if !hasSynced() {
			return fmt.Errorf("reference cache is not synchronized")
		}
		return nil
	}); err != nil {
		return err
	}
	return mgr.Add(worker)
}

// worker owns scheduling and event retries. It holds no independent model-to-
// workload graph; each request queries the existing informer indexes afresh.
type worker struct {
	runRound    func(context.Context) (*Report, error)
	usage       UsageEvaluator
	options     Options
	waitForSync func(context.Context) bool
	clock       clock.Clock
	queue       workqueue.TypedRateLimitingInterface[modelusage.Request]
	active      atomic.Bool
}

func newWorker(runRound func(context.Context) (*Report, error), usage UsageEvaluator, options Options, waitForSync func(context.Context) bool) *worker {
	return &worker{
		runRound: runRound, usage: usage, options: options, waitForSync: waitForSync, clock: clock.RealClock{},
		queue: workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[modelusage.Request](time.Second, time.Minute)),
	}
}

func (w *worker) NeedLeaderElection() bool { return true }

func (w *worker) Start(ctx context.Context) error {
	defer w.queue.ShutDown()
	if !w.waitForSync(ctx) {
		return nil
	}
	w.active.Store(true)
	defer w.active.Store(false)
	historyStopped := make(chan struct{})
	go func() {
		defer close(historyStopped)
		for w.processHistory(ctx) {
		}
	}()
	defer func() { w.queue.ShutDown(); <-historyStopped }()

	for ctx.Err() == nil {
		report, err := w.runRound(ctx)
		if ctx.Err() != nil {
			break
		}
		logger := log.FromContext(ctx)
		if err != nil {
			logger.Error(err, "Dry-run planning round failed", "report", report)
		} else {
			logger.Info("Dry-run planning round completed", "report", report)
		}
		// Start the next interval after completion. Long rounds never overlap and
		// missed ticks do not trigger a burst of catch-up rounds.
		timer := w.clock.NewTimer(w.options.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C():
		}
	}
	return nil
}

func (w *worker) deleted(object any) {
	if tombstone, ok := object.(toolscache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	if model, ok := object.(client.Object); ok {
		w.removed(model, nil)
	}
}

func (w *worker) removed(oldObject, newObject client.Object) {
	if !w.active.Load() {
		return
	}
	requests, err := modelusage.RemovedReferences(oldObject, newObject, w.clock.Now())
	if err != nil {
		ctrl.Log.Error(err, "Read removed model references")
		return
	}
	for _, request := range requests {
		w.queue.Add(request)
	}
}

func (w *worker) processHistory(ctx context.Context) bool {
	request, shutdown := w.queue.Get()
	if shutdown {
		return false
	}
	defer w.queue.Done(request)
	requestCtx, cancel := context.WithTimeout(ctx, w.options.RequestTimeout)
	_, err := w.usage.Reconcile(requestCtx, request, types.UID(""), false)
	cancel()
	if ctx.Err() != nil {
		w.queue.Forget(request)
		return false
	}
	if err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "Update model usage history", "model", request.Model)
		// Keep the original observation time on retries, not the retry time.
		w.queue.AddRateLimited(request)
	} else {
		w.queue.Forget(request)
	}
	return true
}
