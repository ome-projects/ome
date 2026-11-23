package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy/tpudefrag"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

const (
	eventNodeFenced        = "NodeFencedForDefragmentation"
	eventNodeUnfenced      = "NodeUncordonedAfterDefragmentation"
	eventNodeFenceReleased = "NodeKeptCordonedAfterDefragmentation"
)

// DefragmentationFencer is the only writer of the TPU defragmentation fence.
// Before each decision pass it ends drains that must stop, uncordons
// partitions that have emptied, completes a fence that is missing hosts, and,
// in execute mode inside a maintenance window with a migration API
// configured, fences the drains the policy proposes. It re-reads every node before writing and writes with an
// optimistic lock. It uncordons only nodes that carry its annotation.
type DefragmentationFencer struct {
	// Client writes. Reader serves the read before each write and should
	// bypass the informer cache; Client serves it when Reader is nil.
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	Metrics  *metrics.Metrics
	Log      logr.Logger
	Now      func() time.Time

	mu sync.Mutex
	// backoff holds, per TPU pod, when drains may open there again after a
	// slice was placed inside a fence. It is not persisted: a restart forgets
	// it, and the next fence that is not honored sets it again.
	backoff map[string]time.Time
}

// Reconcile applies one pass of fence changes for snap.
func (f *DefragmentationFencer) Reconcile(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config) {
	if f == nil || snap == nil || cfg == nil {
		return
	}
	now := f.now()
	plan := tpudefrag.Build(snap, cfg, now, f.inBackoff(now))
	fenced := 0
	for _, node := range snap.Nodes {
		if node != nil && node.DefragmentationFence != "" {
			fenced++
		}
	}
	for _, d := range plan.Open {
		if d.End != "" {
			removed, err := f.unfenceAll(ctx, snap, cfg, d.ID, d.Fenced, d.End)
			fenced -= removed
			if err != nil {
				f.Log.Error(err, "ending TPU defragmentation drain", "drain", d.ID, "outcome", d.End)
				continue
			}
			f.Log.Info("ended TPU defragmentation drain", "drain", d.ID, "pod", d.Pod, "outcome", d.End)
			f.countDrain(d.End)
			if d.End == tpudefrag.OutcomeFenceNotHonored {
				f.setBackoff(d.Pod, now.Add(cfg.Policies.TPUDefragmentation.FenceFailureBackoff()))
			}
			continue
		}
		if len(d.Empty) > 0 {
			release := map[string]bool{}
			for _, pid := range d.Empty {
				release[pid] = true
			}
			var hosts []string
			for _, name := range d.Fenced {
				if release[partitionOf(snap, cfg, d, name)] {
					hosts = append(hosts, name)
				}
			}
			removed, err := f.unfenceAll(ctx, snap, cfg, d.ID, hosts, tpudefrag.OutcomeCompleted)
			fenced -= removed
			if err != nil {
				f.Log.Error(err, "releasing emptied TPU partitions", "drain", d.ID, "partitions", d.Empty)
				continue
			}
			f.Log.Info("released emptied TPU partitions", "drain", d.ID, "partitions", d.Empty)
			if len(d.Empty) == len(d.Partitions) {
				f.Log.Info("TPU defragmentation drain completed", "drain", d.ID, "pod", d.Pod)
				f.countDrain(tpudefrag.OutcomeCompleted)
				continue
			}
		}
		if len(d.Missing) > 0 && f.mayFence(snap, cfg, now) {
			added, err := f.fenceAll(ctx, snap, cfg, &d.Drain, d.Missing, now)
			fenced += added
			if err != nil {
				f.abort(ctx, snap, cfg, d.ID, append(append([]string(nil), d.Fenced...), d.Missing...), err)
				fenced -= len(d.Fenced) + added
			}
		}
	}
	if f.mayFence(snap, cfg, now) {
		for _, d := range plan.Intents {
			d.ID = drainID(d, now)
			added, err := f.fenceAll(ctx, snap, cfg, d, d.Hosts, now)
			fenced += added
			if err != nil {
				f.abort(ctx, snap, cfg, d.ID, d.Hosts, err)
				fenced -= added
				continue
			}
			f.Log.Info("fenced TPU defragmentation drain", "drain", d.ID, "pod", d.Pod, "unit", d.Unit,
				"partitions", d.Partitions, "moves", len(d.Moves), "deadline", d.Deadline)
		}
	}
	if f.Metrics != nil {
		if fenced < 0 {
			fenced = 0
		}
		f.Metrics.TPUFencedHosts.Set(float64(fenced))
	}
}

// mayFence reports whether the fencer may cordon hosts now. Without a
// migration API no move can be submitted, so a fence would only hold
// capacity.
func (f *DefragmentationFencer) mayFence(snap *snapshot.ClusterSnapshot, cfg *config.Config, now time.Time) bool {
	return cfg.Mode == config.ModeExecute && tpudefrag.Enabled(cfg) && cfg.MaintenanceWindowOpen(now) &&
		snap.OMENativeExecutor.Available
}

// abort removes a drain's fence after a cordon failed.
func (f *DefragmentationFencer) abort(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config, id string, hosts []string, cause error) {
	f.Log.Error(cause, "fencing TPU defragmentation drain failed; removing its fence", "drain", id)
	if _, err := f.unfenceAll(ctx, snap, cfg, id, hosts, tpudefrag.OutcomeCordonFailed); err != nil {
		f.Log.Error(err, "removing the fence of a failed drain; retrying next pass", "drain", id)
		return
	}
	f.countDrain(tpudefrag.OutcomeCordonFailed)
}

// fenceAll cordons hosts for drain d, writing each host's fence. It returns
// how many hosts it fenced and stops at the first failure.
func (f *DefragmentationFencer) fenceAll(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config, d *tpudefrag.Drain, hosts []string, now time.Time) (int, error) {
	busy := map[string]bool{}
	for _, name := range d.Busy {
		busy[name] = true
	}
	added := 0
	for _, name := range hosts {
		node := snap.Nodes[name]
		if node == nil {
			return added, fmt.Errorf("node %s left the snapshot", name)
		}
		fence := tpudefrag.Fence{
			Drain:       d.ID,
			Unit:        d.Unit,
			Partition:   node.Labels[cfg.TPUSlicePartitions.Label(cfg.TPUSlicePartitions.IDLabel, d.Unit)],
			Deadline:    d.Deadline,
			FencedAt:    now,
			FreeAtFence: !busy[name] && !tpudefrag.Occupied(node, cfg),
		}
		wrote, err := f.fence(ctx, node, fence)
		if err != nil {
			return added, fmt.Errorf("fence node %s: %w", name, err)
		}
		if wrote {
			added++
		}
	}
	return added, nil
}

// fence cordons one node and writes its fence. It returns false without error
// when the node already carries this drain's fence.
func (f *DefragmentationFencer) fence(ctx context.Context, node *snapshot.Node, fence tpudefrag.Fence) (bool, error) {
	live, err := f.read(ctx, node.Name)
	if err != nil {
		return false, err
	}
	if live == nil || live.UID != node.UID || !live.DeletionTimestamp.IsZero() {
		return false, fmt.Errorf("node changed since the snapshot")
	}
	if current := live.Annotations[snapshot.DefragmentationFenceAnnotation]; current != "" {
		if existing, ok := tpudefrag.DecodeFence(current); ok && existing.Drain == fence.Drain && live.Spec.Unschedulable {
			return false, nil
		}
		return false, fmt.Errorf("node carries another fence")
	}
	if live.Spec.Unschedulable {
		return false, fmt.Errorf("node was cordoned by someone else")
	}
	value, err := fence.Encode()
	if err != nil {
		return false, err
	}
	patched := live.DeepCopy()
	patched.Spec.Unschedulable = true
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[snapshot.DefragmentationFenceAnnotation] = value
	if err := f.Client.Patch(ctx, patched, client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, fmt.Errorf("patch node: %w", err)
	}
	f.event(live, corev1.EventTypeNormal, eventNodeFenced,
		"node %s cordoned for TPU defragmentation drain %s (partition %s %s); Alfred uncordons it when the partition empties or the drain ends",
		live.Name, fence.Drain, fence.Unit, fence.Partition)
	return true, nil
}

// unfenceAll removes drain id's fence from hosts. It returns how many fences
// it removed and the joined errors.
func (f *DefragmentationFencer) unfenceAll(ctx context.Context, snap *snapshot.ClusterSnapshot, cfg *config.Config, id string, hosts []string, outcome string) (int, error) {
	removed := 0
	var errs []error
	for _, name := range hosts {
		wrote, err := f.unfence(ctx, snap.Nodes[name], name, cfg, id, outcome)
		if err != nil {
			errs = append(errs, fmt.Errorf("node %s: %w", name, err))
			continue
		}
		if wrote {
			removed++
		}
	}
	return removed, errors.Join(errs...)
}

// unfence removes drain id's fence from one node. A node that is unhealthy,
// in the snapshot or on the live read, or under maintenance stays cordoned
// for the node-health policy and the maintenance tooling; a node whose maintenance rule cordons also receives
// that rule's maintenance annotation, as the maintenance cordoner skips nodes
// that are already cordoned. A node carrying the hold-cordon annotation stays
// cordoned for the operator. Every other node is uncordoned.
func (f *DefragmentationFencer) unfence(ctx context.Context, observed *snapshot.Node, name string, cfg *config.Config, id, outcome string) (bool, error) {
	live, err := f.read(ctx, name)
	if err != nil || live == nil {
		return false, err
	}
	current, ok := tpudefrag.DecodeFence(live.Annotations[snapshot.DefragmentationFenceAnnotation])
	if !ok || current.Drain != id {
		return false, nil
	}
	patched := live.DeepCopy()
	delete(patched.Annotations, snapshot.DefragmentationFenceAnnotation)
	triggers := cfg.Policies.NodeHealth.Maintenance.Triggers
	maintenance := snapshot.ObserveNodeMaintenance(live, triggers, f.now())
	unhealthy := (observed != nil && observed.Health.Quarantined()) ||
		snapshot.ObserveNodeHealth(live, cfg.Policies.NodeHealth.TriggerConditions, f.now(), cfg.NodeSuspicionWindow()).Quarantined()
	keep := ""
	switch {
	case unhealthy || maintenance.Requested:
		keep = "the node is unhealthy or under maintenance"
		if rule := cordonRule(maintenance, cordonRules(triggers)); rule != "" {
			patched.Annotations[CordonedForMaintenanceAnnotation] = rule
		}
	case live.Annotations[snapshot.HoldCordonAnnotation] == "true":
		keep = "the node carries " + snapshot.HoldCordonAnnotation
	default:
		patched.Spec.Unschedulable = false
	}
	if err := f.Client.Patch(ctx, patched, client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, fmt.Errorf("patch node: %w", err)
	}
	if keep == "" {
		f.event(live, corev1.EventTypeNormal, eventNodeUnfenced,
			"node %s uncordoned: TPU defragmentation drain %s ended (%s)", name, id, outcome)
	} else {
		f.event(live, corev1.EventTypeNormal, eventNodeFenceReleased,
			"node %s left cordoned after TPU defragmentation drain %s ended (%s): %s", name, id, outcome, keep)
	}
	return true, nil
}

func (f *DefragmentationFencer) read(ctx context.Context, name string) (*corev1.Node, error) {
	reader := f.Reader
	if reader == nil {
		reader = f.Client
	}
	var live corev1.Node
	if err := reader.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read node: %w", err)
	}
	return &live, nil
}

func (f *DefragmentationFencer) event(node *corev1.Node, eventType, reason, format string, args ...interface{}) {
	if f.Recorder != nil {
		f.Recorder.Eventf(node, eventType, reason, format, args...)
	}
}

func (f *DefragmentationFencer) countDrain(outcome string) {
	if f.Metrics != nil {
		f.Metrics.TPUDrains.WithLabelValues(outcome).Inc()
	}
}

func (f *DefragmentationFencer) inBackoff(now time.Time) func(string) bool {
	f.mu.Lock()
	until := make(map[string]time.Time, len(f.backoff))
	for pod, t := range f.backoff {
		until[pod] = t
	}
	f.mu.Unlock()
	return func(pod string) bool { return now.Before(until[pod]) }
}

func (f *DefragmentationFencer) setBackoff(pod string, until time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.backoff == nil {
		f.backoff = map[string]time.Time{}
	}
	f.backoff[pod] = until
}

func (f *DefragmentationFencer) now() time.Time {
	if f.Now == nil {
		return time.Now()
	}
	return f.Now()
}

// partitionOf names the drain partition a fenced host belongs to.
func partitionOf(snap *snapshot.ClusterSnapshot, cfg *config.Config, d *tpudefrag.OpenDrain, name string) string {
	node := snap.Nodes[name]
	if node == nil {
		return ""
	}
	if fence, ok := tpudefrag.DecodeFence(node.DefragmentationFence); ok && fence.Drain == d.ID {
		return fence.Partition
	}
	return node.Labels[cfg.TPUSlicePartitions.Label(cfg.TPUSlicePartitions.IDLabel, d.Unit)]
}

// drainID names a new drain by its start time and partitions.
func drainID(d *tpudefrag.Drain, now time.Time) string {
	sum := sha256.Sum256([]byte(d.Pod + "/" + d.Unit + "/" + strings.Join(d.Partitions, ",")))
	return now.UTC().Format("20060102t150405z") + "-" + hex.EncodeToString(sum[:4])
}

// cordonRules returns the maintenance rules that ask for a cordon.
func cordonRules(triggers []config.MaintenanceTrigger) map[string]bool {
	rules := map[string]bool{}
	for _, t := range triggers {
		if t.Cordon {
			rules[t.Name] = true
		}
	}
	return rules
}
