package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// EarlyTicker requests supplemental fresh decision passes when enabled health
// or configured maintenance observations change. The signal feeds a
// non-reentrant loop without moving its regular cadence. It runs on every replica — the
// channel is only consumed by the leader's decision loop, and a non-leader's
// signals are cheap.
type EarlyTicker struct {
	Cache ctrlcache.Cache
	Store *config.Store
	Log   logr.Logger

	// C carries the tick signal; buffered with capacity 1 so a signal
	// landing mid-pass waits there instead of being lost, and a storm of
	// changes collapses into one advancement.
	C chan struct{}
}

var _ manager.Runnable = &EarlyTicker{}
var _ manager.LeaderElectionRunnable = &EarlyTicker{}

// NeedLeaderElection returns false — see the type comment.
func (e *EarlyTicker) NeedLeaderElection() bool { return false }

// Start registers the node handler and blocks until the context ends.
func (e *EarlyTicker) Start(ctx context.Context) error {
	informer, err := e.Cache.GetInformer(ctx, &corev1.Node{})
	if err != nil {
		return fmt.Errorf("get node informer: %w", err)
	}
	registration, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) { e.observe(oldObj, newObj) },
	})
	if err != nil {
		return fmt.Errorf("register node handler: %w", err)
	}
	defer func() {
		_ = informer.RemoveEventHandler(registration)
	}()
	<-ctx.Done()
	return nil
}

// observe signals when a node's configured observations changed and the trigger is
// enabled. Enablement is checked at signal time against the live config, so
// a reload takes effect without re-registering the handler.
func (e *EarlyTicker) observe(oldObj, newObj interface{}) {
	oldNode, okOld := oldObj.(*corev1.Node)
	newNode, okNew := newObj.(*corev1.Node)
	if !okOld || !okNew {
		return
	}
	cfg := e.Store.Get()
	changed := earlyTickEnabled(cfg, config.EarlyTickNodeConditionChange) && nodeConditionsChanged(oldNode, newNode)
	if !changed && earlyTickEnabled(cfg, config.EarlyTickNodeMaintenanceChange) {
		triggers := cfg.Policies.NodeHealth.Maintenance.Triggers
		oldMaintenance := snapshot.ObserveNodeMaintenance(oldNode, triggers)
		newMaintenance := snapshot.ObserveNodeMaintenance(newNode, triggers)
		changed = !slices.Equal(oldMaintenance.Triggers, newMaintenance.Triggers)
	}
	if !changed {
		return
	}
	select {
	case e.C <- struct{}{}:
		e.Log.V(1).Info("node observation changed; requesting fresh decision", "node", newNode.Name)
	default:
		// A signal is already pending; the next pass covers this change.
	}
}

func earlyTickEnabled(cfg *config.Config, event string) bool {
	for _, trigger := range cfg.EarlyTickOn {
		if trigger == event {
			return true
		}
	}
	return false
}

// nodeConditionsChanged compares statuses and recovery transition times by
// type. Heartbeat, reason, and message updates do not trigger fresh passes.
func nodeConditionsChanged(oldNode, newNode *corev1.Node) bool {
	if len(oldNode.Status.Conditions) != len(newNode.Status.Conditions) {
		return true
	}
	previous := make(map[corev1.NodeConditionType]corev1.NodeCondition, len(oldNode.Status.Conditions))
	for _, cond := range oldNode.Status.Conditions {
		previous[cond.Type] = cond
	}
	for _, cond := range newNode.Status.Conditions {
		if old, ok := previous[cond.Type]; !ok || old.Status != cond.Status || !old.LastTransitionTime.Equal(&cond.LastTransitionTime) {
			return true
		}
	}
	return false
}
