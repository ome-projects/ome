// Package testutil provides the shared synthetic ClusterSnapshot harness
// used by Alfred's policy, arbiter, and chaos tests. Building snapshots
// directly — no fake clients, no informers — is what keeps every policy a
// table-testable pure function of the snapshot (OEP-0008).
package testutil

import (
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ReferenceTime is the fixed "now" synthetic snapshots are built against, so
// age-based logic is deterministic in tests.
var ReferenceTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// SnapshotBuilder assembles a snapshot.ClusterSnapshot fluently.
type SnapshotBuilder struct {
	s          *snapshot.ClusterSnapshot
	podCounter int
}

// NewSnapshot returns a builder for an empty cluster at ReferenceTime.
func NewSnapshot() *SnapshotBuilder {
	return &SnapshotBuilder{
		s: &snapshot.ClusterSnapshot{
			Timestamp:         ReferenceTime,
			Nodes:             map[string]*snapshot.Node{},
			Workloads:         map[types.NamespacedName]*snapshot.Workload{},
			Models:            map[snapshot.ModelKey]*snapshot.ModelAvailability{},
			OMENativeExecutor: snapshot.OMENativeExecutorState{Available: true},
		},
	}
}

// NodeOption mutates a node under construction.
type NodeOption func(*snapshot.Node)

// NodeUnhealthy marks the node unhealthy with the given trigger conditions
// (GpuUnhealthy when none given).
func NodeUnhealthy(conditions ...string) NodeOption {
	return func(n *snapshot.Node) {
		n.Unhealthy = true
		if len(conditions) == 0 {
			conditions = []string{"GpuUnhealthy"}
		}
		n.UnhealthyConditions = append(n.UnhealthyConditions, conditions...)
	}
}

// NodeCordoned marks the node cordoned.
func NodeCordoned() NodeOption { return func(n *snapshot.Node) { n.Cordoned = true } }

// NodePreemptible marks the node spot/preemptible.
func NodePreemptible() NodeOption { return func(n *snapshot.Node) { n.Preemptible = true } }

// NodeScaleDownDisabled sets the CA scale-down-disabled flag.
func NodeScaleDownDisabled() NodeOption { return func(n *snapshot.Node) { n.ScaleDownDisabled = true } }

// NodeScaleDownMarked sets the CA to-be-deleted flag.
func NodeScaleDownMarked() NodeOption { return func(n *snapshot.Node) { n.ScaleDownMarked = true } }

// NodeSuspect puts the node inside a suspicion window.
func NodeSuspect() NodeOption { return func(n *snapshot.Node) { n.Suspect = true } }

// NodeLabels merges labels onto the node.
func NodeLabels(labels map[string]string) NodeOption {
	return func(n *snapshot.Node) {
		for k, v := range labels {
			n.Labels[k] = v
		}
	}
}

// WithNode adds a GPU node of the given hardware pool and size. Each
// node may be defined once: redefining a name would silently discard the
// occupancy other builder calls already accumulated on it (AllocatedGPUs,
// OMEPods, OtherOccupants) while workload pods keep referencing the node —
// an inconsistent snapshot. Use ConfigureNode to mutate an existing node.
func (b *SnapshotBuilder) WithNode(name, pool string, totalGPUs int64, opts ...NodeOption) *SnapshotBuilder {
	if _, exists := b.s.Nodes[name]; exists {
		panic(fmt.Sprintf("testutil: node %q already defined; use ConfigureNode to modify it", name))
	}
	n := &snapshot.Node{
		Name:        name,
		Labels:      map[string]string{},
		GPUPool:     pool,
		GPUResource: constants.NvidiaGPUResourceType,
		TotalGPUs:   totalGPUs,
	}
	for _, opt := range opts {
		opt(n)
	}
	b.s.Nodes[name] = n
	return b
}

// WithOMENative sets structured executor availability.
func (b *SnapshotBuilder) WithOMENative(available bool) *SnapshotBuilder {
	b.s.OMENativeExecutor.Available = available
	return b
}

// WithOMENativeExecutor replaces the structured OMENative executor state.
func (b *SnapshotBuilder) WithOMENativeExecutor(state snapshot.OMENativeExecutorState) *SnapshotBuilder {
	b.s.OMENativeExecutor = state
	return b
}

// WithOtherOccupant places a non-OME GPU pod (a notebook, a batch job) on a
// node: it counts against capacity but is never a candidate.
func (b *SnapshotBuilder) WithOtherOccupant(node string, gpus int64) *SnapshotBuilder {
	n := b.mustNode(node)
	pod := snapshot.PodInfo{
		Namespace: "other",
		Name:      b.nextPodName("occupant"),
		Node:      node,
		GPUs:      gpus,
		Ready:     true,
	}
	n.OtherOccupants = append(n.OtherOccupants, pod)
	n.AllocatedGPUs += gpus
	return b
}

// WithInstance adds one single-pod Instance of a workload's component on a
// node, creating the workload and component as needed. The workload key is
// "namespace/name".
func (b *SnapshotBuilder) WithInstance(workload string, ctype v1beta1.ComponentType, mode constants.DeploymentModeType, node string, gpus int64) *SnapshotBuilder {
	return b.WithMultiPodInstance(workload, ctype, mode, gpus, node)
}

// WithMultiPodInstance adds one Instance whose pods (gpusPerPod each) sit on
// the given nodes — an atomic multi-pod group when len(nodes) > 1.
func (b *SnapshotBuilder) WithMultiPodInstance(workload string, ctype v1beta1.ComponentType, mode constants.DeploymentModeType, gpusPerPod int64, nodes ...string) *SnapshotBuilder {
	w := b.ensureWorkload(workload)
	component, ok := w.Components[ctype]
	if !ok {
		component = &snapshot.Component{
			Type:             ctype,
			DeploymentMode:   mode,
			StatusFresh:      mode == constants.OMENative,
			ObservationValid: true,
		}
		if mode == constants.OMENative {
			component.IR = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
		}
		w.Components[ctype] = component
	} else if component.DeploymentMode != mode {
		// A silent rewrite would re-label instances added under the
		// earlier mode; the test would then assert against a snapshot
		// it did not intend to build.
		panic(fmt.Sprintf("testutil: component %s of %q already defined with mode %q; cannot redefine as %q",
			ctype, workload, component.DeploymentMode, mode))
	}

	inst := &snapshot.Instance{
		Index:            int32(len(component.Instances)),
		Incarnation:      1,
		Phase:            v1beta1.OMENativeInstanceReady,
		Admitted:         true,
		DesiredPods:      int32(len(nodes)),
		StatusPods:       int32(len(nodes)),
		ObservedPods:     int32(len(nodes)),
		ServingPods:      int32(len(nodes)),
		AvailablePods:    int32(len(nodes)),
		ObservationValid: true,
		NodesSet:         map[string]int{},
	}
	if mode == constants.OMENative {
		inst.RunningRevision = "revision-1"
		inst.TargetRevision = "revision-1"
	}
	for i, nodeName := range nodes {
		n := b.mustNode(nodeName)
		runner := v1beta1.RunnerNameDefault
		ordinal := int32(0)
		if len(nodes) > 1 {
			if i == 0 {
				runner = v1beta1.RunnerNameLeader
			} else {
				runner = v1beta1.RunnerNameWorker
				ordinal = int32(i - 1)
			}
		}
		pod := snapshot.PodInfo{
			Namespace:            w.NamespacedName.Namespace,
			Name:                 b.nextPodName(w.NamespacedName.Name + "-" + string(ctype)),
			Node:                 nodeName,
			GPUs:                 gpusPerPod,
			Ready:                true,
			ISVC:                 w.NamespacedName,
			Component:            ctype,
			ManagedBy:            "OMENative",
			InstanceIndex:        inst.Index,
			InstanceIndexPresent: true,
			InstanceIndexValid:   true,
			Incarnation:          inst.Incarnation,
			IncarnationPresent:   true,
			IncarnationValid:     true,
			Runner:               runner,
			RunnerPresent:        true,
			RunnerValid:          true,
			PodOrdinal:           ordinal,
			PodOrdinalPresent:    true,
			PodOrdinalValid:      true,
		}
		start := ReferenceTime.Add(-24 * time.Hour)
		pod.StartTime = &start
		inst.Pods = append(inst.Pods, pod)
		inst.NodesSet[nodeName]++
		inst.TotalGPUs += gpusPerPod
		inst.ReadyPods++
		if gpusPerPod > 0 {
			n.AllocatedGPUs += gpusPerPod
			n.OMEPods = append(n.OMEPods, pod)
		}
	}
	component.Instances = append(component.Instances, inst)
	if mode == constants.OMENative {
		replicas := int32(len(component.Instances))
		component.IR.Spec.Replicas = &replicas
		component.IR.Status = v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   component.IR.Generation,
			Replicas:             replicas,
			ReadyReplicas:        replicas,
			ServingReplicas:      replicas,
			AvailableReplicas:    replicas,
			UpdatedReplicas:      replicas,
			UpdatedReadyReplicas: replicas,
			CurrentRevision:      "revision-1",
			UpdateRevision:       "revision-1",
		}
	}
	return b
}

// WithInvalidOMENativeObservation marks one synthetic OMENative component and
// all of its Instances structurally invalid with a deliberate advisory reason.
func (b *SnapshotBuilder) WithInvalidOMENativeObservation(workload string, ctype v1beta1.ComponentType, reason string) *SnapshotBuilder {
	w := b.ensureWorkload(workload)
	component, ok := w.Components[ctype]
	if !ok || component.DeploymentMode != constants.OMENative {
		panic(fmt.Sprintf("testutil: OMENative component %s of %q must exist before marking its observation invalid", ctype, workload))
	}
	component.ObservationValid = false
	component.ObservationReason = reason
	for _, instance := range component.Instances {
		instance.ObservationValid = false
		instance.ObservationReason = reason
	}
	return b
}

// WithPendingPod adds a real pending GPU pod that has waited for age.
func (b *SnapshotBuilder) WithPendingPod(gpus int64, age time.Duration, pool string) *SnapshotBuilder {
	return b.WithPendingPodIn("pending", gpus, age, pool)
}

// WithPendingPodIn adds a pending GPU pod in a specific namespace — tenant
// scoping in policy tests needs control over where pending demand lives.
func (b *SnapshotBuilder) WithPendingPodIn(namespace string, gpus int64, age time.Duration, pool string) *SnapshotBuilder {
	b.s.PendingPods = append(b.s.PendingPods, snapshot.PendingPod{
		Namespace:    namespace,
		Name:         b.nextPodName("pending"),
		GPUsNeeded:   gpus,
		GPUPool:      pool,
		PendingSince: ReferenceTime.Add(-age),
	})
	return b
}

// WithModel registers model availability and points a workload at it.
func (b *SnapshotBuilder) WithModel(workload string, avail *snapshot.ModelAvailability) *SnapshotBuilder {
	w := b.ensureWorkload(workload)
	w.ModelKey = avail.Key
	b.s.Models[avail.Key] = avail
	return b
}

// ConfigureWorkload applies arbitrary mutations to a workload (movable,
// priority, cooldown state, active migrations, ...), creating it if needed.
func (b *SnapshotBuilder) ConfigureWorkload(workload string, fn func(*snapshot.Workload)) *SnapshotBuilder {
	fn(b.ensureWorkload(workload))
	return b
}

// ConfigureNode applies arbitrary mutations to an existing node.
func (b *SnapshotBuilder) ConfigureNode(node string, fn func(*snapshot.Node)) *SnapshotBuilder {
	fn(b.mustNode(node))
	return b
}

// Build finalizes derived fields (free GPUs, ordering) and returns the
// snapshot. The builder must not be reused afterwards.
func (b *SnapshotBuilder) Build() *snapshot.ClusterSnapshot {
	for _, n := range b.s.Nodes {
		n.FreeGPUs = n.TotalGPUs - n.AllocatedGPUs
		if n.FreeGPUs < 0 {
			n.FreeGPUs = 0
		}
	}
	sort.Slice(b.s.PendingPods, func(i, j int) bool {
		if b.s.PendingPods[i].Namespace != b.s.PendingPods[j].Namespace {
			return b.s.PendingPods[i].Namespace < b.s.PendingPods[j].Namespace
		}
		return b.s.PendingPods[i].Name < b.s.PendingPods[j].Name
	})
	return b.s
}

func (b *SnapshotBuilder) ensureWorkload(key string) *snapshot.Workload {
	parts := strings.Split(key, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		panic(fmt.Sprintf("testutil: workload key %q must be namespace/name", key))
	}
	name := types.NamespacedName{Namespace: parts[0], Name: parts[1]}
	if w, ok := b.s.Workloads[name]; ok {
		return w
	}
	w := &snapshot.Workload{
		NamespacedName:      name,
		Components:          map[v1beta1.ComponentType]*snapshot.Component{},
		Movable:             true,
		Priority:            0.5,
		MigrationStateValid: true,
	}
	b.s.Workloads[name] = w
	return w
}

func (b *SnapshotBuilder) mustNode(name string) *snapshot.Node {
	n, ok := b.s.Nodes[name]
	if !ok {
		panic(fmt.Sprintf("testutil: node %q not defined; call WithNode first", name))
	}
	return n
}

func (b *SnapshotBuilder) nextPodName(prefix string) string {
	b.podCounter++
	return fmt.Sprintf("%s-%d", prefix, b.podCounter)
}
