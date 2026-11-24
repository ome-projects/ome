package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

const (
	// CordonedForMaintenanceAnnotation names the maintenance rule that made
	// Alfred cordon a node. Alfred never removes it or the cordon.
	CordonedForMaintenanceAnnotation = "alfred.ome.io/cordoned-for-maintenance"
	eventNodeCordonedForMaintenance  = "NodeCordonedForMaintenance"
)

// MaintenanceCordoner cordons nodes while a maintenance rule marked
// cordon: true requests maintenance on them. Schedulers, including a TPU slice
// scheduler that skips cordoned partitions, then stop placing new work there
// while Alfred moves the existing work off. It acts only in execute mode,
// re-reads each node before writing, and never uncordons: the maintenance
// tooling that owns the node decides when it returns to service.
type MaintenanceCordoner struct {
	// Client writes the cordon. Reader serves the read before the write and
	// should bypass the informer cache that fed the snapshot; Client serves it
	// when Reader is nil.
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	Metrics  *metrics.Metrics
	Log      logr.Logger
	Now      func() time.Time

	mu     sync.Mutex
	failed error
}

// Reconcile cordons every node in snap that a cordon rule currently puts under
// maintenance and that is not cordoned yet. Fenced reports the cordons that
// failed until the next call.
func (c *MaintenanceCordoner) Reconcile(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config) {
	if c == nil {
		return
	}
	err := c.reconcile(ctx, snap, cfg)
	c.mu.Lock()
	c.failed = err
	c.mu.Unlock()
}

// Fenced returns nil when the latest Reconcile cordoned every node it had to,
// and otherwise its failures, each naming the node and rule. A replacement may
// land on any schedulable node, so migration dispatch waits for the fence.
func (c *MaintenanceCordoner) Fenced() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

func (c *MaintenanceCordoner) reconcile(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config) error {
	if snap == nil || cfg == nil || cfg.Mode != config.ModeExecute {
		return nil
	}
	triggers := cfg.Policies.NodeHealth.Maintenance.Triggers
	rules := cordonRules(triggers)
	if len(rules) == 0 {
		return nil
	}
	names := make([]string, 0, len(snap.Nodes))
	for name := range snap.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	var failed []error
	for _, name := range names {
		node := snap.Nodes[name]
		rule := ""
		if node != nil && !node.Cordoned {
			rule = cordonRule(node.Maintenance, rules)
		}
		if rule == "" {
			continue
		}
		if err := c.cordon(ctx, name, node.UID, triggers, rules); err != nil {
			err = fmt.Errorf("cordon node %s for maintenance rule %q: %w", name, rule, err)
			if apierrors.IsConflict(err) {
				// The node changed after the read; the next pass reads it again.
				c.Log.Info("maintenance cordon conflicted; retrying next pass", "node", name, "rule", rule)
			} else {
				c.Log.Error(err, "maintenance cordon failed", "node", name, "rule", rule)
			}
			failed = append(failed, err)
		}
	}
	return errors.Join(failed...)
}

func (c *MaintenanceCordoner) cordon(ctx context.Context, name string, uid types.UID, triggers []config.MaintenanceTrigger, rules map[string]bool) error {
	reader := c.Reader
	if reader == nil {
		reader = c.Client
	}
	var live corev1.Node
	if err := reader.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read node: %w", err)
	}
	// Act only on the node the snapshot saw, and only while the rule still
	// matches it now.
	if live.UID != uid || live.Spec.Unschedulable || !live.DeletionTimestamp.IsZero() {
		return nil
	}
	rule := cordonRule(snapshot.ObserveNodeMaintenance(&live, triggers, c.now()), rules)
	if rule == "" {
		return nil
	}
	patched := live.DeepCopy()
	patched.Spec.Unschedulable = true
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[CordonedForMaintenanceAnnotation] = rule
	if err := c.Client.Patch(ctx, patched, client.MergeFromWithOptions(&live, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("patch node: %w", err)
	}
	c.Log.Info("cordoned node for maintenance", "node", name, "rule", rule)
	if c.Metrics != nil {
		c.Metrics.NodeMaintenanceSignals.WithLabelValues(name, eventNodeCordonedForMaintenance).Inc()
	}
	if c.Recorder != nil {
		c.Recorder.Eventf(&live, corev1.EventTypeNormal, eventNodeCordonedForMaintenance,
			"node %s cordoned because maintenance rule %q requests maintenance; Alfred does not uncordon it", name, rule)
	}
	return nil
}

// cordonRule returns the first matched maintenance rule that asks for a
// cordon, or "" when none does.
func cordonRule(m snapshot.NodeMaintenanceObservation, cordonRules map[string]bool) string {
	if !m.Requested {
		return ""
	}
	for _, name := range m.Triggers {
		if cordonRules[name] {
			return name
		}
	}
	return ""
}

func (c *MaintenanceCordoner) now() time.Time {
	if c.Now == nil {
		return time.Now()
	}
	return c.Now()
}
