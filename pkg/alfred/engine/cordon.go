package engine

import (
	"context"
	"sort"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
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
	Client   client.Client
	Recorder record.EventRecorder
	Metrics  *metrics.Metrics
	Log      logr.Logger
	Now      func() time.Time
}

// Reconcile cordons every node in snap that a cordon rule currently puts under
// maintenance and that is not cordoned yet.
func (c *MaintenanceCordoner) Reconcile(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config) {
	if c == nil || snap == nil || cfg == nil || cfg.Mode != config.ModeExecute {
		return
	}
	triggers := cfg.Policies.NodeHealth.Maintenance.Triggers
	cordonRules := map[string]bool{}
	for _, t := range triggers {
		if t.Cordon {
			cordonRules[t.Name] = true
		}
	}
	if len(cordonRules) == 0 {
		return
	}
	names := make([]string, 0, len(snap.Nodes))
	for name := range snap.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		node := snap.Nodes[name]
		if node == nil || node.Cordoned || cordonRule(node.Maintenance, cordonRules) == "" {
			continue
		}
		c.cordon(ctx, name, node.UID, triggers, cordonRules)
	}
}

func (c *MaintenanceCordoner) cordon(ctx context.Context, name string, uid types.UID, triggers []config.MaintenanceTrigger, cordonRules map[string]bool) {
	var live corev1.Node
	if err := c.Client.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
		c.Log.Error(err, "read node before maintenance cordon", "node", name)
		return
	}
	// Act only on the node the snapshot saw, and only while the rule still
	// matches it now.
	if live.UID != uid || live.Spec.Unschedulable || !live.DeletionTimestamp.IsZero() {
		return
	}
	rule := cordonRule(snapshot.ObserveNodeMaintenance(&live, triggers, c.now()), cordonRules)
	if rule == "" {
		return
	}
	patched := live.DeepCopy()
	patched.Spec.Unschedulable = true
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[CordonedForMaintenanceAnnotation] = rule
	if err := c.Client.Patch(ctx, patched, client.MergeFromWithOptions(&live, client.MergeFromWithOptimisticLock{})); err != nil {
		c.Log.Error(err, "maintenance cordon failed", "node", name, "rule", rule)
		return
	}
	c.Log.Info("cordoned node for maintenance", "node", name, "rule", rule)
	if c.Metrics != nil {
		c.Metrics.NodeMaintenanceSignals.WithLabelValues(name, eventNodeCordonedForMaintenance).Inc()
	}
	if c.Recorder != nil {
		c.Recorder.Eventf(&live, corev1.EventTypeNormal, eventNodeCordonedForMaintenance,
			"node %s cordoned because maintenance rule %q requests maintenance; Alfred does not uncordon it", name, rule)
	}
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
