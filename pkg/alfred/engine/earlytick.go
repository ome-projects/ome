package engine

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/alfred/config"
)

// EarlyTicker turns node-condition changes into supplemental decision passes
// (earlyTickOn: [NodeConditionChange]): health evacuation starts within seconds
// while defragmentation stays lazily periodic. The signal requests a fresh pass
// from the non-reentrant loop without moving its regular cadence. It runs on
// every replica — the channel is only consumed by the leader's decision loop,
// and a non-leader's signals are cheap.
type EarlyTicker struct {
	Cache ctrlcache.Cache
	Store *config.Store
	Log   logr.Logger

	// C carries the tick signal; buffered with capacity 1 so a signal
	// landing mid-pass waits there instead of being lost, and a storm of
	// changes collapses into one supplemental pass.
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

// observe signals when a node's conditions changed and the trigger is
// enabled. Enablement is checked at signal time against the live config, so
// a reload takes effect without re-registering the handler.
func (e *EarlyTicker) observe(oldObj, newObj interface{}) {
	oldNode, okOld := oldObj.(*corev1.Node)
	newNode, okNew := newObj.(*corev1.Node)
	if !okOld || !okNew {
		return
	}
	if !earlyTickEnabled(e.Store.Get()) {
		return
	}
	if !nodeConditionsChanged(oldNode, newNode) {
		return
	}
	select {
	case e.C <- struct{}{}:
		e.Log.V(1).Info("node condition change; requesting supplemental decision pass", "node", newNode.Name)
	default:
		// A signal is already pending; the next pass covers this change.
	}
}

func earlyTickEnabled(cfg *config.Config) bool {
	for _, trigger := range cfg.EarlyTickOn {
		if trigger == config.EarlyTickNodeConditionChange {
			return true
		}
	}
	return false
}

// nodeConditionsChanged compares condition statuses by type: a status flip
// (or a condition appearing/disappearing) is a change; heartbeat-only
// updates are not.
func nodeConditionsChanged(oldNode, newNode *corev1.Node) bool {
	if len(oldNode.Status.Conditions) != len(newNode.Status.Conditions) {
		return true
	}
	previous := make(map[corev1.NodeConditionType]corev1.ConditionStatus, len(oldNode.Status.Conditions))
	for _, cond := range oldNode.Status.Conditions {
		previous[cond.Type] = cond.Status
	}
	for _, cond := range newNode.Status.Conditions {
		if status, ok := previous[cond.Type]; !ok || status != cond.Status {
			return true
		}
	}
	return false
}
