package config

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ReloadObserver is notified of every reload attempt's outcome; the metrics
// package implements it (alfred_policy_reload_total).
type ReloadObserver interface {
	ObserveConfigReload(outcome string)
}

// Watcher hot-reloads the Store from the alfred-config ConfigMap through the
// manager's informer cache. It runs on every replica (config is needed
// before and regardless of leadership) and never restarts Alfred: a change
// takes effect on the next loop pass that calls Store.Get, and a loop that
// arms timers from the config is woken through Store.Changed.
type Watcher struct {
	Cache     ctrlcache.Cache
	Namespace string
	Name      string
	// Key is the ConfigMap key holding the YAML document (config.yaml).
	Key      string
	Store    *Store
	Log      logr.Logger
	Recorder record.EventRecorder
	Observer ReloadObserver

	// lastApplied dedups informer resyncs: reprocessing an unchanged
	// document must not inflate the reload metric. lastMissing tracks the
	// distinct "key absent" state (not representable as a document value,
	// including the empty string) so a persistently broken ConfigMap is
	// reported once, not once per resync.
	lastApplied string
	lastMissing bool
	seeded      bool
}

var _ manager.Runnable = &Watcher{}
var _ manager.LeaderElectionRunnable = &Watcher{}

// NeedLeaderElection returns false: every replica loads config.
func (w *Watcher) NeedLeaderElection() bool { return false }

// Start registers the event handler and blocks until the context ends.
func (w *Watcher) Start(ctx context.Context) error {
	informer, err := w.Cache.GetInformer(ctx, &corev1.ConfigMap{})
	if err != nil {
		return fmt.Errorf("get configmap informer: %w", err)
	}
	registration, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { w.observe(obj) },
		UpdateFunc: func(_, newObj interface{}) {
			w.observe(newObj)
		},
		DeleteFunc: func(obj interface{}) { w.observeDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("register configmap handler: %w", err)
	}
	defer func() {
		_ = informer.RemoveEventHandler(registration)
	}()
	<-ctx.Done()
	return nil
}

func (w *Watcher) observe(obj interface{}) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm.Namespace != w.Namespace || cm.Name != w.Name {
		return
	}
	w.Apply(cm)
}

// observeDelete reacts only to the watched ConfigMap disappearing — the cache
// holds every ConfigMap in the namespace, so unrelated deletions (e.g.
// alfred-recommendations) must stay silent. Deletion keeps last-known-good;
// the log is loud so the operator knows edits will not land. The informer may
// deliver a DeletedFinalStateUnknown tombstone instead of the object.
func (w *Watcher) observeDelete(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm.Namespace != w.Namespace || cm.Name != w.Name {
		return
	}
	w.Log.Info("alfred-config ConfigMap deleted; keeping last-known-good configuration",
		"namespace", w.Namespace, "name", w.Name)
}

// Apply processes one ConfigMap state. Exposed for tests; the informer path
// funnels here.
func (w *Watcher) Apply(cm *corev1.ConfigMap) {
	raw, ok := cm.Data[w.Key]
	if !ok {
		if w.seeded && w.lastMissing {
			return
		}
		w.seeded, w.lastMissing = true, true
		w.reloadFailed(cm, fmt.Errorf("ConfigMap %s/%s has no %q key", cm.Namespace, cm.Name, w.Key))
		return
	}
	w.lastMissing = false
	if w.seeded && raw == w.lastApplied {
		return
	}
	outcome, err := w.Store.Update([]byte(raw))
	if w.Observer != nil {
		w.Observer.ObserveConfigReload(outcome)
	}
	if err != nil {
		// The document is remembered even on failure so a broken edit
		// is reported once, not on every resync.
		w.seeded, w.lastApplied = true, raw
		if w.Recorder != nil {
			w.Recorder.Eventf(cm, corev1.EventTypeWarning, "PolicyReloadFailed",
				"alfred-config rejected, keeping last-known-good: %v", err)
		}
		w.Log.Error(err, "config reload failed; keeping last-known-good")
		return
	}
	w.seeded, w.lastApplied = true, raw
	w.Log.Info("config reloaded", "mode", w.Store.Get().Mode)
}

func (w *Watcher) reloadFailed(cm *corev1.ConfigMap, err error) {
	if w.Observer != nil {
		w.Observer.ObserveConfigReload(OutcomeFailure)
	}
	if w.Recorder != nil {
		w.Recorder.Eventf(cm, corev1.EventTypeWarning, "PolicyReloadFailed", "%v", err)
	}
	w.Log.Error(err, "config reload failed; keeping last-known-good")
}
