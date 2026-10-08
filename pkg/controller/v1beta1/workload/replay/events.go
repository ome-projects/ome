package replay

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// eventApplier turns one timeline event into the cluster or spec change
// the table says it is, and returns the detail the trace records.
type eventApplier func(ctx context.Context, d *driver, ev TimelineEvent) (string, error)

// eventAppliers is the set of table events the driver can stage. An event
// the table declares but this map does not carry fails to load, so a
// scenario can never silently skip an observation.
var eventAppliers map[string]eventApplier

func init() {
	eventAppliers = map[string]eventApplier{
		"pod.scheduled":            applyPodScheduled,
		"pod.containersReady":      applyContainersReady,
		"pod.containersNotReady":   applyContainersNotReady,
		"pod.ready":                applyPodReady,
		"pod.servingOn":            applyServing(corev1.ConditionTrue),
		"pod.servingOff":           applyServing(corev1.ConditionFalse),
		"pod.terminating":          applyPodTerminating,
		"pod.stuckTerminating":     applyStuckTerminating,
		"pod.deleted":              applyPodDeleted,
		"pod.uidRotated":           applyUIDRotated,
		"pod.phaseTerminal":        applyPhaseTerminal,
		"pod.admissionRejected":    applyAdmissionRejected,
		"pod.evicted":              applyEvicted,
		"pod.nodeLost":             applyNodeLost,
		"pod.waiting":              applyPodWaiting,
		"pod.unschedulable":        applyUnschedulable,
		"pod.schedulingGated":      applySchedulingGated,
		"pod.admitted":             applyAdmitted,
		"pod.containerRestart":     applyContainerRestart,
		"endpoint.rotationIn":      applyRotation(true),
		"endpoint.rotationOut":     applyRotation(false),
		"api.quotaDenied":          applyRejection(quotaDenied, "create"),
		"api.invalid":              applyRejection(invalidPodSpec, "create"),
		"api.notFound":             applyRejection(notFound, "delete"),
		"api.alreadyExists":        applyRejection(alreadyExists, "create"),
		"api.throttled":            applyThrottled,
		"api.namespaceTerminating": applyRejection(namespaceTerminating, "create"),
		"api.conflict":             applyConflict,
		"spec.revision":            applySpecRevision,
		"spec.annotationOnly":      applyAnnotationOnly,
		"spec.strategy":            applySpecStrategy,
		"spec.replicasUp":          applyReplicas,
		"spec.replicasDown":        applyReplicas,
		"spec.pauseTrue":           applyPause(true, false),
		"spec.pauseFreeze":         applyPause(true, true),
		"spec.unpause":             applyPause(false, false),
		"spec.teardown":            applyTeardown,
		"spec.gangWidth":           applyGangWidth,
		"spec.partition":           applyPartition,
		"spec.rollbackTarget":      applyRollbackTarget,
		"spec.policyTuning":        applyPolicyTuning,
		"spec.migrateRequest":      applyMigrateRequest,
		"config.changed":           applyConfigChanged,
		"gang.podGroup":            applyPodGroup,
		"timer.operationDeadline":  applyOperationDeadline,
		"timer.stuckPodGrace":      applyStuckPodGrace,
		"timer.retryAt":            applyRetryAt,
		"timer.repairRetry":        applyRepairRetry,
		"timer.migrationDeadline":  applyMigrationDeadline,
		"timer.forceDelete":        applyForceDelete,
		"timer.teardownDeadline":   applyTeardownDeadline,
		"ctrl.crash":               applyCtrlCrash,
		"ctrl.staleRead":           applyStaleRead,
	}
}

// apply stages one event and records it.
func (d *driver) apply(ctx context.Context, ev TimelineEvent) error {
	if ev.ID == ClockAdvance {
		detail, err := d.advance("clock.advance", ev.Args.Duration)
		if err != nil {
			return err
		}
		d.trace.applied(ev, detail)
		return nil
	}
	applier, ok := eventAppliers[ev.ID]
	if !ok {
		return fmt.Errorf("replay: no applier for event %s", ev.ID)
	}
	detail, err := applier(ctx, d, ev)
	if err != nil {
		return err
	}
	d.trace.applied(ev, detail)
	return nil
}

// advance moves the injected clock forward.
func (d *driver) advance(field, value string) (string, error) {
	delta, err := ParseDuration(field, value)
	if err != nil {
		return "", err
	}
	if delta < 0 {
		return "", fmt.Errorf("replay: %s: the clock does not run backwards", field)
	}
	d.clock.Step(delta)
	return "by=" + delta.String(), nil
}

func (d *driver) podName(ref PodRef) string {
	return query.PodName(d.opts.OwnerName, d.opts.Component, ref.Index, ref.RunnerName(), ref.Ordinal)
}

func requirePod(ev TimelineEvent) (PodRef, error) {
	if ev.Args.Pod == nil {
		return PodRef{}, fmt.Errorf("replay: event %s needs a pod reference", ev.Key())
	}
	return *ev.Args.Pod, nil
}

func applyPodScheduled(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	node := ev.Args.Message
	if node == "" {
		node = fmt.Sprintf("node-%d", ref.Index)
	}
	name := d.podName(ref)
	return "pod=" + name + " node=" + node, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Spec.NodeName = node
		setPodCondition(pod, corev1.PodScheduled, corev1.ConditionTrue, d.clock.Now())
	})
}

func applyContainersReady(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodRunning
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, d.clock.Now())
		setContainerStatusesReady(pod)
	})
}

// applyContainersNotReady is a running pod's readiness regressing: the
// kubelet drops ContainersReady, folds that into Ready, and reports the
// containers not ready while they keep running. A pod that was never
// ContainersReady has nothing to regress from and fails the run.
func applyContainersNotReady(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if cond := findCondition(pod, corev1.ContainersReady); cond == nil || cond.Status != corev1.ConditionTrue {
		return "", fmt.Errorf("replay: pod.containersNotReady: pod %s is not ContainersReady, so its readiness cannot regress", name)
	}
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		now := d.clock.Now()
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
		for i := range pod.Status.ContainerStatuses {
			pod.Status.ContainerStatuses[i].Ready = false
		}
	})
}

// findCondition returns the pod condition of the named type, or nil.
func findCondition(pod *corev1.Pod, condType corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// applyPodReady is the kubelet folding every readiness gate into the
// pod's own Ready condition, which is the bar every promote reads.
func applyPodReady(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodRunning
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, d.clock.Now())
		setPodCondition(pod, corev1.PodReady, corev1.ConditionTrue, d.clock.Now())
		setContainerStatusesReady(pod)
	})
}

func applyServing(status corev1.ConditionStatus) eventApplier {
	return func(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
		ref, err := requirePod(ev)
		if err != nil {
			return "", err
		}
		name := d.podName(ref)
		return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
			setPodCondition(pod, query.ServingConditionType, status, d.clock.Now())
		})
	}
}

func applyPodTerminating(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: name}}
	if err := d.staging(func() error { return d.cli.Delete(ctx, pod) }); err != nil {
		return "", fmt.Errorf("replay: terminate pod %s: %w", name, err)
	}
	return "pod=" + name, nil
}

// applyStuckTerminating is the kubelet behind an already-deleted pod
// going away: the pod keeps its deletion timestamp, loses the finalizer
// that was standing in for a kubelet's acknowledgement, and never
// disappears on its own again. That is the object the force-delete
// escalation exists for — a Terminating pod still carrying a finalizer is
// classified foreign-finalizers and only ever reported.
//
// It is an observation about a deletion already in flight, so a pod nobody
// has deleted has nothing to be stuck at and fails the run.
func applyStuckTerminating(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	hold := d.terminating[name]
	if hold == nil {
		return "", fmt.Errorf("replay: pod.stuckTerminating: pod %s is not Terminating, so no deletion is wedged on it", name)
	}
	if hold.kubeletStuck {
		return "", fmt.Errorf("replay: pod.stuckTerminating: pod %s is already held by a dead kubelet", name)
	}
	hold.kubeletStuck = true
	return fmt.Sprintf("pod=%s deletionDeadline=%s", name, d.norm.at(hold.at)), nil
}

func applyPodDeleted(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.removePod(ctx, name)
}

// applyUIDRotated is a pod of the same name reappearing under a new UID:
// the object the engine observed is gone, and an identical one — same
// labels and template, fresh, unscheduled, Pending — stands where it was.
// The watch sees a delete and an add, so the expectations balance; a
// delete the engine pinned to the old UID matches nothing.
func applyUIDRotated(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	previous := d.norm.uid(string(pod.UID))
	successor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			Labels:          pod.Labels,
			Annotations:     pod.Annotations,
			OwnerReferences: pod.OwnerReferences,
		},
		Spec: *pod.Spec.DeepCopy(),
	}
	successor.Spec.NodeName = ""
	if err := d.removePod(ctx, name); err != nil {
		return "", err
	}
	if err := d.staging(func() error { return d.cli.Create(ctx, successor) }); err != nil {
		return "", fmt.Errorf("replay: recreate pod %s under a new uid: %w", name, err)
	}
	if err := d.patchPod(ctx, name, func(pod *corev1.Pod) { pod.Status.Phase = corev1.PodPending }); err != nil {
		return "", err
	}
	return fmt.Sprintf("pod=%s uid=%s→%s", name, previous, d.norm.uid(string(successor.UID))), nil
}

func applyPhaseTerminal(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	phase := corev1.PodPhase(ev.Variant)
	if phase == "" {
		return "", fmt.Errorf("replay: pod.phaseTerminal needs a variant")
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = phase
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, d.clock.Now())
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, d.clock.Now())
		exitCode := int32(1)
		if phase == corev1.PodSucceeded {
			exitCode = 0
		}
		statuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
		for _, c := range pod.Spec.Containers {
			statuses = append(statuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   exitCode,
					Reason:     string(phase),
					FinishedAt: metav1.NewTime(d.clock.Now()),
				}},
			})
		}
		pod.Status.ContainerStatuses = statuses
	})
}

// nodeNotReadyReason is the reason the node lifecycle controller writes on
// a pod's Ready condition when the node under it stops reporting.
const nodeNotReadyReason = "NodeNotReady"

// applyNodeLost is the node under a bound pod dying, in the three forms
// the force-delete evidence tells apart: the Node object gone, the
// unreachable taint with its TimeAdded, and NodeReady False with its
// transition time. The last two also take the pod out of Ready, as the
// node lifecycle controller does for every pod on such a node; a kubelet
// that stopped reporting leaves the container statuses as they were.
func applyNodeLost(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("replay: pod.nodeLost: pod %s is not bound, so no node of its can be lost", name)
	}
	nodeName := pod.Spec.NodeName
	now := metav1.NewTime(d.clock.Now())
	switch ev.Variant {
	case "NodeGone":
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
		if err := d.staging(func() error { return client.IgnoreNotFound(d.cli.Delete(ctx, node)) }); err != nil {
			return "", fmt.Errorf("replay: delete node %s: %w", nodeName, err)
		}
		return "pod=" + name + " node=" + nodeName + " evidence=gone", nil
	case "UnreachableTaint":
		err = d.writeNode(ctx, nodeName, func(node *corev1.Node) {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
				Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute, TimeAdded: &now,
			})
			setNodeReady(node, corev1.ConditionUnknown, now)
		})
	case "NodeNotReady":
		err = d.writeNode(ctx, nodeName, func(node *corev1.Node) { setNodeReady(node, corev1.ConditionFalse, now) })
	default:
		return "", fmt.Errorf("replay: pod.nodeLost needs a variant: NodeGone, UnreachableTaint or NodeNotReady")
	}
	if err != nil {
		return "", err
	}
	if err := d.patchPod(ctx, name, func(pod *corev1.Pod) {
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now.Time)
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodReady {
				pod.Status.Conditions[i].Reason = nodeNotReadyReason
			}
		}
	}); err != nil {
		return "", err
	}
	return "pod=" + name + " node=" + nodeName + " evidence=" + ev.Variant + " since=" + d.norm.at(now.Time), nil
}

// writeNode creates the Node if the cluster holds none and applies the
// mutation. The fake cluster holds no Node until a scenario names one.
func (d *driver) writeNode(ctx context.Context, name string, mutate func(*corev1.Node)) error {
	return d.staging(func() error {
		node := &corev1.Node{}
		err := d.cli.Get(ctx, client.ObjectKey{Name: name}, node)
		switch {
		case apierrors.IsNotFound(err):
			node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
			mutate(node)
			return d.cli.Create(ctx, node)
		case err != nil:
			return fmt.Errorf("replay: get node %s: %w", name, err)
		}
		mutate(node)
		return d.cli.Update(ctx, node)
	})
}

// setNodeReady writes the NodeReady condition with its transition time,
// which is what the node-death evidence ages.
func setNodeReady(node *corev1.Node, status corev1.ConditionStatus, at metav1.Time) {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady {
			node.Status.Conditions[i].Status = status
			node.Status.Conditions[i].LastTransitionTime = at
			return
		}
	}
	node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
		Type: corev1.NodeReady, Status: status, LastTransitionTime: at,
	})
}

// evictedReason is the pod-level reason the kubelet writes when it evicts
// a pod under node pressure.
const evictedReason = "Evicted"

// applyEvicted is the kubelet evicting a running pod under node pressure:
// the pod goes Failed with the Evicted reason and the kubelet's message,
// its containers killed. An eviction through the Eviction API is a
// graceful delete and is staged as pod.terminating instead.
func applyEvicted(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("replay: pod.evicted: pod %s is not bound to a node, and only a node evicts under pressure", name)
	}
	return "pod=" + name + " reason=" + evictedReason, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		now := metav1.NewTime(d.clock.Now())
		pod.Status.Phase = corev1.PodFailed
		pod.Status.Reason = evictedReason
		pod.Status.Message = ev.Args.Message
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now.Time)
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now.Time)
		for i := range pod.Status.ContainerStatuses {
			cs := &pod.Status.ContainerStatuses[i]
			cs.Ready = false
			cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   137,
				Reason:     "Error",
				FinishedAt: now,
			}}
		}
	})
}

// outOfResourceReasonPrefix is how the kubelet names a refusal for a
// resource the node ran out of, OutOfcpu or OutOfnvidia.com/gpu, which
// is the family the engine's admission-rejection predicate recognizes.
const outOfResourceReasonPrefix = "OutOf"

// applyAdmissionRejected is the kubelet refusing a scheduled pod: the pod
// goes Failed with the refusal as its pod-level reason and no container
// ever runs. UnexpectedAdmissionError is the reason itself; OutOfResource
// stands for the kubelet's OutOf<resource> family and names the resource
// through `resource:`, as in OutOfcpu. Only a bound pod can be refused by
// its node, so an unscheduled one fails the run.
func applyAdmissionRejected(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	var reason string
	switch ev.Variant {
	case "UnexpectedAdmissionError":
		if ev.Args.Resource != "" {
			return "", fmt.Errorf("replay: pod.admissionRejected[UnexpectedAdmissionError] names no resource")
		}
		reason = query.ReasonUnexpectedAdmissionError
	case "OutOfResource":
		if ev.Args.Resource == "" {
			return "", fmt.Errorf("replay: pod.admissionRejected[OutOfResource] needs the resource the node ran out of, as in cpu or nvidia.com/gpu")
		}
		reason = outOfResourceReasonPrefix + ev.Args.Resource
	default:
		return "", fmt.Errorf("replay: pod.admissionRejected needs a variant: UnexpectedAdmissionError or OutOfResource")
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("replay: pod.admissionRejected: pod %s is not bound to a node, and only a node refuses admission", name)
	}
	return "pod=" + name + " reason=" + reason, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		now := d.clock.Now()
		pod.Status.Phase = corev1.PodFailed
		pod.Status.Reason = reason
		pod.Status.Message = ev.Args.Message
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
		pod.Status.ContainerStatuses = nil
	})
}

func applyPodWaiting(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	if ev.Variant == "" {
		return "", fmt.Errorf("replay: pod.waiting needs a variant naming the reason")
	}
	container := ev.Args.Container
	if container == "" {
		container = d.containerName()
	}
	name := d.podName(ref)
	return "pod=" + name + " reason=" + ev.Variant, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
		// A kubelet reports ContainersReady=False with a pod's first status,
		// so a pod that never served has held it since its creation; only a
		// pod that served transitions now. PodReady follows ContainersReady
		// down in the same status, whatever the gate says.
		notReadySince := d.clock.Now()
		if !hasPodCondition(pod, corev1.ContainersReady) && !pod.CreationTimestamp.IsZero() {
			notReadySince = pod.CreationTimestamp.Time
		}
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, notReadySince)
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, notReadySince)
		waiting := corev1.ContainerStatus{
			Name:  container,
			Image: podImage(pod, container),
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  ev.Variant,
				Message: ev.Args.Message,
			}},
		}
		// The kubelet keeps a container's restart count and last termination
		// through a waiting episode, so a runner that restarted before it
		// parked still reads as restarted.
		for i := range pod.Status.ContainerStatuses {
			if cs := &pod.Status.ContainerStatuses[i]; cs.Name == container {
				waiting.RestartCount = cs.RestartCount
				waiting.LastTerminationState = cs.LastTerminationState
			}
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{waiting}
	})
}

// hasPodCondition reports whether the pod carries a condition of the
// named type, whatever its status.
func hasPodCondition(pod *corev1.Pod, condType corev1.PodConditionType) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			return true
		}
	}
	return false
}

// applyContainerRestart is the kubelet restarting a container in place:
// the pod keeps its UID and stays Running, and the only evidence is on the
// container status — a bumped restart count, the previous run's
// termination, and a current run that began after it. The restart trigger
// reads that evidence off the runner container and dates it against the
// Instance's ReadySince.
func applyContainerRestart(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	container := ev.Args.Container
	if container == "" {
		container = d.containerName()
	}
	reason := ev.Args.Message
	if reason == "" {
		reason = "Error"
	}
	name := d.podName(ref)
	found := false
	if err := d.patchPod(ctx, name, func(pod *corev1.Pod) {
		now := metav1.NewTime(d.clock.Now())
		for i := range pod.Status.ContainerStatuses {
			cs := &pod.Status.ContainerStatuses[i]
			if cs.Name != container {
				continue
			}
			found = true
			cs.RestartCount++
			cs.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   1,
				Reason:     reason,
				FinishedAt: now,
			}}
			cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}
			cs.Ready = true
		}
	}); err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("replay: pod.containerRestart: pod %s reports no container %q to restart", name, container)
	}
	return "pod=" + name + " container=" + container + " reason=" + reason, nil
}

func applyUnschedulable(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodScheduled {
				pod.Status.Conditions[i].Status = corev1.ConditionFalse
				pod.Status.Conditions[i].Reason = corev1.PodReasonUnschedulable
				pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(d.clock.Now())
				return
			}
		}
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             corev1.PodReasonUnschedulable,
			LastTransitionTime: metav1.NewTime(d.clock.Now()),
		})
	})
}

// podAdmissionGate is the scheduling gate the driver places on a pod a
// queue admission holds. The engine reads only that a gate is present,
// never which one.
const podAdmissionGate = "replay.workload.ome.io/queue-admission"

// applySchedulingGated is a queue admission holding a pod: it carries a
// scheduling gate, stays Pending and reports PodScheduled=False with the
// scheduler's SchedulingGated reason. A real cluster stamps the gate on
// admission; the driver writes it onto the created pod after the fact,
// which is the same object the next pass reads.
func applySchedulingGated(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod.Spec.NodeName != "" {
		return "", fmt.Errorf("replay: pod.schedulingGated: pod %s is already bound to %s, and a bound pod is past admission", name, pod.Spec.NodeName)
	}
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		if !types.PodAdmissionGated(pod) {
			pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: podAdmissionGate})
		}
		pod.Status.Phase = corev1.PodPending
		setPodCondition(pod, corev1.PodScheduled, corev1.ConditionFalse, d.clock.Now())
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodScheduled {
				pod.Status.Conditions[i].Reason = corev1.PodReasonSchedulingGated
			}
		}
	})
}

// applyAdmitted is the queue admitting a gated pod: every gate comes off
// and the PodScheduled report with it, leaving the pod to the scheduler,
// whose verdict arrives as pod.scheduled or pod.unschedulable. A pod with
// no gate has nothing to be admitted from and fails the run.
func applyAdmitted(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod, err := d.getPod(ctx, name)
	if err != nil {
		return "", err
	}
	if !types.PodAdmissionGated(pod) {
		return "", fmt.Errorf("replay: pod.admitted: pod %s carries no scheduling gate", name)
	}
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Spec.SchedulingGates = nil
		kept := pod.Status.Conditions[:0]
		for _, cond := range pod.Status.Conditions {
			if cond.Type != corev1.PodScheduled {
				kept = append(kept, cond)
			}
		}
		pod.Status.Conditions = kept
	})
}

// getPod reads one stored pod by name.
func (d *driver) getPod(ctx context.Context, name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := d.cli.Get(ctx, client.ObjectKey{Namespace: d.opts.Namespace, Name: name}, pod); err != nil {
		return nil, fmt.Errorf("replay: get pod %s: %w", name, err)
	}
	return pod, nil
}

func applyRotation(ready bool) eventApplier {
	return func(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
		ref, err := requirePod(ev)
		if err != nil {
			return "", err
		}
		name := d.podName(ref)
		return "pod=" + name, d.setRotation(ctx, name, ready)
	}
}

// applyRejection arms an apiserver refusal for the writes a scenario
// names. count defaults to one write; the verb defaults per refusal, to
// the write the refusal is the natural answer to.
func applyRejection(build func(resource, name string) error, defaultVerb string) eventApplier {
	return func(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
		return d.armRejection(ev, defaultVerb, build)
	}
}

// armRejection records one refusal owed to the engine's next matching
// writes.
func (d *driver) armRejection(ev TimelineEvent, defaultVerb string, build func(resource, name string) error) (string, error) {
	verb := ev.Args.Verb
	if verb == "" {
		verb = defaultVerb
	}
	resource := ev.Args.Resource
	if resource == "" {
		resource = "pods"
	}
	count := ev.Args.Count
	if count == 0 {
		count = 1
	}
	d.rejections = append(d.rejections, &pendingRejection{
		verb:      verb,
		resource:  resource,
		remaining: count,
		err:       build(resource, d.opts.OwnerName),
		label:     ev.Key(),
	})
	return fmt.Sprintf("verb=%s resource=%s count=%d", verb, resource, count), nil
}

// applyThrottled arms the apiserver shedding a write: 429 TooManyRequests
// or 503 ServiceUnavailable, with the Retry-After the scenario names, if
// any. A shed write is the natural answer to a create, which is also the
// write the engine classifies and paces by.
func applyThrottled(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	retryAfter, err := ParseDuration("api.throttled.retryAfter", ev.Args.RetryAfter)
	if err != nil {
		return "", err
	}
	seconds := int(retryAfter / time.Second)
	var build func(resource, name string) error
	switch ev.Variant {
	case "TooManyRequests":
		build = func(string, string) error {
			return apierrors.NewTooManyRequests("the server is shedding load", seconds)
		}
	case "ServiceUnavailable":
		build = func(string, string) error {
			status := apierrors.NewServiceUnavailable("the server is currently unable to handle the request")
			if seconds > 0 {
				status.ErrStatus.Details = &metav1.StatusDetails{RetryAfterSeconds: int32(seconds)}
			}
			return status
		}
	default:
		return "", fmt.Errorf("replay: api.throttled needs a variant: TooManyRequests or ServiceUnavailable")
	}
	detail, err := d.armRejection(ev, "create", build)
	if err != nil {
		return "", err
	}
	if retryAfter > 0 {
		detail += " retryAfter=" + retryAfter.String()
	}
	return detail, nil
}

func quotaDenied(resource, name string) error {
	return apierrors.NewForbidden(
		schema.GroupResource{Resource: resource}, name,
		errors.New("exceeded quota: compute-resources, requested: pods=1, used: pods=4, limited: pods=4"),
	)
}

// notFound is the apiserver answering a write whose object is already
// gone. The driver removes the object as it answers, so the refusal is
// never a lie about the cluster: the next read finds nothing either.
func notFound(resource, name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Resource: resource}, name)
}

// alreadyExists is the apiserver answering a create whose name is already
// taken: the race the engine loses to its own earlier pass, whose create
// landed before the watch delivered it. The driver lands the object as it
// answers, so the name really is taken.
func alreadyExists(resource, name string) error {
	return apierrors.NewAlreadyExists(schema.GroupResource{Resource: resource}, name)
}

// conflict is the apiserver refusing a write made against a stale
// resourceVersion.
func conflict(resource, name string) error {
	return apierrors.NewConflict(schema.GroupResource{Resource: resource}, name,
		errors.New("the object has been modified; please apply your changes to the latest version and try again"))
}

// applyConflict arms a 409 on one write of the next pass. On the owner
// status — the default, and the resource the row store stands in for — it
// answers the write-th status write of the pass, count attempts in a row,
// behind the adapter's own conflict retry. On any other resource it is an
// ordinary refusal of the engine's next matching writes.
func applyConflict(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	resource := ev.Args.Resource
	if resource == "" {
		resource = statusOwnerResource
	}
	if resource != statusOwnerResource {
		return d.armRejection(ev, "update", conflict)
	}
	if ev.Args.Verb != "" && ev.Args.Verb != "update" {
		return "", fmt.Errorf("replay: api.conflict: the owner status is written by update, not %s", ev.Args.Verb)
	}
	if ev.Args.Write < 1 {
		return "", fmt.Errorf("replay: api.conflict needs write, the ordinal of the status write of the pass it answers")
	}
	count := ev.Args.Count
	if count == 0 {
		count = 1
	}
	d.store.ArmConflict(ev.Args.Write, count)
	return fmt.Sprintf("resource=%s write=%d count=%d", resource, ev.Args.Write, count), nil
}

// namespaceTerminating is the apiserver refusing a create because the
// namespace is being deleted: a Forbidden carrying the NamespaceTerminating
// cause, which is what the engine's classifier reads.
func namespaceTerminating(resource, name string) error {
	status := apierrors.NewForbidden(
		schema.GroupResource{Resource: resource}, name,
		errors.New("unable to create new content in namespace because it is being terminated"),
	)
	status.ErrStatus.Details.Causes = append(status.ErrStatus.Details.Causes, metav1.StatusCause{
		Type:    corev1.NamespaceTerminatingCause,
		Message: "namespace is being terminated",
		Field:   "metadata.namespace",
	})
	return status
}

func invalidPodSpec(resource, name string) error {
	return apierrors.NewInvalid(
		schema.GroupKind{Kind: "Pod"}, name,
		field.ErrorList{field.Invalid(
			field.NewPath("spec", "containers").Index(0).Child("resources", "limits"),
			"-1", "must be greater than or equal to 0",
		)},
	)
}

// applySpecRevision edits the pod template, which mints a new target
// revision. An image rewrite is the in-place-eligible edit; an environment
// rewrite is the one an in-place strategy is not allowed to converge on,
// and the two are spelled apart so a scenario says which kind of edit it
// is making.
func applySpecRevision(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Image == "" && ev.Args.Env == nil && ev.Args.Annotations == nil {
		return "", fmt.Errorf("replay: spec.revision needs a template edit: the new image, the new env, the new annotations, or several")
	}
	detail := ""
	if ev.Args.Image != "" {
		d.spec.Image = ev.Args.Image
		detail += "image=" + ev.Args.Image + " "
	}
	if ev.Args.Env != nil {
		d.spec.Env = ev.Args.Env
		detail += "env=" + renderEnv(ev.Args.Env) + " "
	}
	if ev.Args.Annotations != nil {
		d.spec.PodAnnotations = ev.Args.Annotations
		detail += "annotations=" + renderMap(ev.Args.Annotations) + " "
	}
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("%sgeneration=%d", detail, d.owner.Generation), nil
}

// applyAnnotationOnly rewrites the pod template's labels and annotations
// with an edit the revision hasher does not read as intent, so the target
// revision does not move. The hasher itself is the judge: an edit that
// mints a new revision is a template change and fails the run, because
// the scenario vocabulary spells that edit spec.revision.
func applyAnnotationOnly(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Labels == nil && ev.Args.Annotations == nil {
		return "", fmt.Errorf("replay: spec.annotationOnly needs the template's new labels, annotations, or both")
	}
	before, _, err := revision.HashWithWorker(d.podTemplate(d.spec.Image), nil, d.podMeta(), d.collisionCount, parentUID)
	if err != nil {
		return "", fmt.Errorf("replay: hash the template before the edit: %w", err)
	}
	labels, annotations := d.spec.PodLabels, d.spec.PodAnnotations
	if ev.Args.Labels != nil {
		d.spec.PodLabels = ev.Args.Labels
	}
	if ev.Args.Annotations != nil {
		d.spec.PodAnnotations = ev.Args.Annotations
	}
	after, _, err := revision.HashWithWorker(d.podTemplate(d.spec.Image), nil, d.podMeta(), d.collisionCount, parentUID)
	if err != nil {
		return "", fmt.Errorf("replay: hash the template after the edit: %w", err)
	}
	if after != before {
		d.spec.PodLabels, d.spec.PodAnnotations = labels, annotations
		return "", fmt.Errorf("replay: spec.annotationOnly: the edit moves the revision hash %s→%s, so it is a template change; stage it as spec.revision", before, after)
	}
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	detail := ""
	if ev.Args.Labels != nil {
		detail += "labels=" + renderMap(ev.Args.Labels) + " "
	}
	if ev.Args.Annotations != nil {
		detail += "annotations=" + renderMap(ev.Args.Annotations) + " "
	}
	return fmt.Sprintf("%sgeneration=%d", detail, d.owner.Generation), nil
}

// renderMap spells a metadata edit in key order.
func renderMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+":"+m[k])
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// renderEnv spells an environment edit in key order, so a trace line does
// not depend on map iteration.
func renderEnv(env map[string]string) string {
	vars := sortedEnv(env)
	parts := make([]string, 0, len(vars))
	for _, v := range vars {
		parts = append(parts, v.Name+":"+v.Value)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// applySpecStrategy rewrites lifecycle.updateStrategy. The strategy is not
// part of the revision payload, so the edit retargets nothing: what it
// tests is which machine owns a row that already has an attempt in flight.
func applySpecStrategy(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Strategy == "" {
		return "", fmt.Errorf("replay: spec.strategy needs the new strategy")
	}
	d.spec.Strategy = ev.Args.Strategy
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("strategy=%s generation=%d", ev.Args.Strategy, d.owner.Generation), nil
}

func applyReplicas(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.To == nil {
		return "", fmt.Errorf("replay: %s needs the new replica count", ev.Key())
	}
	d.spec.Replicas = *ev.Args.To
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("replicas=%d generation=%d", d.spec.Replicas, d.owner.Generation), nil
}

// applyTeardown begins owner deletion. From here the planned index set
// reads as empty, so every Instance is a scale-down extra and nothing but
// the scale-down pipeline runs. The clock's reading is the owner's
// deletion instant, which its teardown deadline is measured from.
func applyTeardown(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant != "" && ev.Variant != "deleted" {
		return "", fmt.Errorf("replay: spec.teardown[%s]: the driver models the owner's own deletion, not an owner replaced under a new UID", ev.Variant)
	}
	// The deletion observed again moves nothing; the instant is the first.
	if !d.teardown {
		d.teardown = true
		d.teardownAt = d.clock.Now()
	}
	return "teardown=true", nil
}

// applyTeardownDeadline advances the clock onto the deleted owner's
// teardown deadline: its deletion instant plus config.teardownDeadline.
// The adapter checks that deadline ahead of the teardown dispatch, so the
// pass that observes it elapsed releases the owner before anything else
// runs.
func applyTeardownDeadline(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if !d.teardown {
		return "", fmt.Errorf("replay: timer.teardownDeadline: the owner is not Terminating, so no teardown deadline runs")
	}
	deadline, err := ParseDuration("config.teardownDeadline", d.cfg.TeardownDeadline)
	if err != nil {
		return "", err
	}
	if deadline <= 0 {
		return "", fmt.Errorf("replay: timer.teardownDeadline: config.teardownDeadline is not set, so teardown holds strictly and no deadline elapses")
	}
	return d.advanceOnto("timer.teardownDeadline", d.teardownAt.Add(deadline), ev)
}

// applyGangWidth changes the per-Instance worker count without touching
// the pod template, so the Instance's expected pod set moves at an
// unchanged revision hash.
func applyGangWidth(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.To == nil {
		return "", fmt.Errorf("replay: %s needs the new worker count", ev.Key())
	}
	if *ev.Args.To < 1 {
		return "", fmt.Errorf("replay: %s: a gang keeps at least one worker beside its leader", ev.Key())
	}
	d.spec.Workers = *ev.Args.To
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("workers=%d generation=%d", d.spec.Workers, d.owner.Generation), nil
}

// applyPartition rewrites rollingUpdate.partition, the canary hold that
// keeps the Instances below it on their revision. The partition is not
// part of the revision payload, so the edit retargets nothing; what it
// changes is which rows the next update pass may select.
func applyPartition(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.To == nil {
		return "", fmt.Errorf("replay: spec.partition needs the new partition as to")
	}
	if *ev.Args.To < 0 {
		return "", fmt.Errorf("replay: spec.partition: a partition is a count of held Instances and cannot be negative")
	}
	partition := *ev.Args.To
	d.spec.Partition = &partition
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("partition=%d generation=%d", partition, d.owner.Generation), nil
}

// applyRollbackTarget pins the roll to a stored revision, named by the
// image whose template minted it, or releases the pin. The pinned
// revision overrides the rendered template and becomes the roll target;
// what the owner reports as its update revision stays the spec target.
func applyRollbackTarget(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	switch ev.Variant {
	case "set":
		if ev.Args.Image == "" {
			return "", fmt.Errorf("replay: spec.rollbackTarget[set] needs the image of the stored revision to roll back to")
		}
		d.rollbackImage = ev.Args.Image
	case "clear":
		if ev.Args.Image != "" {
			return "", fmt.Errorf("replay: spec.rollbackTarget[clear] releases the pin and takes no image")
		}
		d.rollbackImage = ""
	default:
		return "", fmt.Errorf("replay: spec.rollbackTarget needs a variant: set or clear")
	}
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("rollbackTo=%s generation=%d", orNil(d.rollbackImage), d.owner.Generation), nil
}

// applyPolicyTuning rewrites one lifecycle knob outside the pod template,
// spelled as the owner spec spells it. No lifecycle knob feeds the
// revision payload, so the edit retargets nothing; what it changes is the
// next decision that reads the knob.
func applyPolicyTuning(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant == "" {
		return "", fmt.Errorf("replay: spec.policyTuning needs a variant naming the knob")
	}
	value := ev.Args.Value
	if value == "" {
		return "", fmt.Errorf("replay: spec.policyTuning[%s] needs the new setting as value", ev.Variant)
	}
	switch ev.Variant {
	case "restartPolicy":
		d.spec.RestartPolicy = value
	case "migrationPolicy":
		d.spec.MigrationMode = value
	case "instanceReadyTimeout":
		if _, err := ParseDuration("spec.policyTuning[instanceReadyTimeout].value", value); err != nil {
			return "", err
		}
		d.spec.InstanceReadyTimeout = value
	case "minReadySeconds":
		seconds, err := strconv.ParseInt(value, 10, 32)
		if err != nil || seconds < 0 {
			return "", fmt.Errorf("replay: spec.policyTuning[minReadySeconds]: value %q is not a non-negative number of seconds", value)
		}
		d.spec.MinReadySeconds = int32(seconds)
	case "markNotReady":
		flag, err := strconv.ParseBool(value)
		if err != nil {
			return "", fmt.Errorf("replay: spec.policyTuning[markNotReady]: value %q is not a boolean", value)
		}
		d.spec.MarkNotReady = &flag
	default:
		return "", fmt.Errorf("replay: spec.policyTuning[%s] is not a knob the driver rewrites", ev.Variant)
	}
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s generation=%d", ev.Variant, value, d.owner.Generation), nil
}

// configKnob is one variant of config.changed: the ConfigState fields the
// knob owns, copied from the event's config onto the driver's. The
// adapter re-reads its ConfigMap through a short-lived cache on every
// pass, so an edit between two ticks is read by the very next pass.
type configKnob struct {
	copy   func(dst, src *ConfigState)
	render func(c *ConfigState) string
}

var configKnobs = map[string]configKnob{
	"updateRetry": {
		copy:   func(dst, src *ConfigState) { dst.UpdateRetry = src.UpdateRetry },
		render: func(c *ConfigState) string { return renderRetry(c.UpdateRetry) },
	},
	"stuckPodGrace": {
		copy:   func(dst, src *ConfigState) { dst.StuckPodGrace = src.StuckPodGrace },
		render: func(c *ConfigState) string { return orNil(c.StuckPodGrace) },
	},
	"forceDelete": {
		copy: func(dst, src *ConfigState) { dst.ForceDelete = src.ForceDelete },
		render: func(c *ConfigState) string {
			if c.ForceDelete == nil {
				return "nil"
			}
			return "{overdueSlack=" + orNil(c.ForceDelete.OverdueSlack) + " nodeUnreachableThreshold=" + orNil(c.ForceDelete.NodeUnreachableThreshold) + "}"
		},
	},
	"teardownDeadline": {
		copy:   func(dst, src *ConfigState) { dst.TeardownDeadline = src.TeardownDeadline },
		render: func(c *ConfigState) string { return orNil(c.TeardownDeadline) },
	},
	"scaleDownInterval": {
		copy:   func(dst, src *ConfigState) { dst.ScaleDownRequeueInterval = src.ScaleDownRequeueInterval },
		render: func(c *ConfigState) string { return orNil(c.ScaleDownRequeueInterval) },
	},
	"autoMigrateBudget": {
		copy:   func(dst, src *ConfigState) { dst.AutoMigrateBudget = src.AutoMigrateBudget },
		render: func(c *ConfigState) string { return fmt.Sprint(c.AutoMigrateBudget) },
	},
	"audit": {
		copy: func(dst, src *ConfigState) { dst.MigrationAudit = src.MigrationAudit },
		render: func(c *ConfigState) string {
			if c.MigrationAudit == nil {
				return "nil"
			}
			return fmt.Sprintf("{maxInFlight=%d maxPerWindow=%d window=%s}", c.MigrationAudit.MaxInFlight, c.MigrationAudit.MaxPerWindow, orNil(c.MigrationAudit.Window))
		},
	},
	"requeue": {
		copy: func(dst, src *ConfigState) {
			dst.RequeueOperation, dst.RequeueGate = src.RequeueOperation, src.RequeueGate
		},
		render: func(c *ConfigState) string {
			return "{operation=" + orNil(c.RequeueOperation) + " gate=" + orNil(c.RequeueGate) + "}"
		},
	},
}

func renderRetry(spec *RetrySpec) string {
	if spec == nil {
		return "nil"
	}
	return fmt.Sprintf("{maxAttempts=%d initialDelay=%s maxDelay=%s multiplier=%g}",
		spec.MaxAttempts, orNil(spec.InitialDelay), orNil(spec.MaxDelay), spec.Multiplier)
}

// applyConfigChanged rewrites one operator-configuration knob between
// passes. The event's config is the knob's whole new value: a group left
// empty clears it, and a field outside the knob fails the run, because
// the variant says which knob changed. The two knobs the driver has no
// input for are refused by name rather than silently accepted.
func applyConfigChanged(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	switch ev.Variant {
	case "":
		return "", fmt.Errorf("replay: config.changed needs a variant naming the knob")
	case "gangScheduleTimeout", "retryBlockHistoryLimit":
		return "", fmt.Errorf("replay: config.changed[%s]: the driver carries no %s input, so the knob has nothing to change", ev.Variant, ev.Variant)
	}
	knob, ok := configKnobs[ev.Variant]
	if !ok {
		return "", fmt.Errorf("replay: config.changed[%s] is not a knob the driver rewrites", ev.Variant)
	}
	next := ConfigState{}
	if ev.Args.Config != nil {
		next = *ev.Args.Config
	}
	// Everything the event names must belong to the knob: a copy of only
	// the knob's fields has to reproduce the whole argument.
	var onlyKnob ConfigState
	knob.copy(&onlyKnob, &next)
	if !reflect.DeepEqual(onlyKnob, next) {
		return "", fmt.Errorf("replay: config.changed[%s]: config names a field outside the %s knob", ev.Variant, ev.Variant)
	}
	knob.copy(&d.cfg, &next)
	return ev.Variant + "=" + knob.render(&d.cfg), nil
}

// applyCtrlCrash is the controller process restarting. The cluster and the
// owner status are durable and stay as they are; what the restarted engine
// loses is the expectations cache, the one thing it keeps in memory between
// passes, so its first pass reads every pending create and delete as
// satisfied and re-derives the row's step from status and the pods alone.
func applyCtrlCrash(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant != "" {
		return "", fmt.Errorf("replay: ctrl.crash takes no variant")
	}
	d.expectations = types.NewExpectationsWithClock(d.clock)
	return "expectations=dropped", nil
}

// applyMigrateRequest is the operator's migration mailbox, consumed: the
// adapter answers a request annotation by appending an Accepted record to
// the owner's status, and that record is the only thing the engine's
// migrate pass reads.
func applyMigrateRequest(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Instance == nil {
		return "", fmt.Errorf("replay: spec.migrateRequest needs the Instance index to migrate")
	}
	d.migrationRequests++
	uuid := ev.Args.UUID
	if uuid == "" {
		uuid = fmt.Sprintf("migration-%d", d.migrationRequests)
	}
	for i := range d.migrations {
		if d.migrations[i].RequestUUID == uuid {
			return "", fmt.Errorf("replay: spec.migrateRequest: uuid %s is already recorded", uuid)
		}
	}
	timeout, err := ParseDuration("spec.instanceReadyTimeout", d.spec.InstanceReadyTimeout)
	if err != nil {
		return "", err
	}
	now := metav1.NewTime(d.clock.Now())
	d.migrations = append(d.migrations, types.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        types.MigrationTriggerManual,
		SourceInstance: *ev.Args.Instance,
		FromNode:       ev.Args.FromNode,
		Reason:         ev.Args.Reason,
		Phase:          types.MigrationPhaseAccepted,
		StartedAt:      now,
		Deadline:       types.DeadlineAt(now, timeout),
	})
	return fmt.Sprintf("uuid=%s instance=%d fromNode=%s", d.norm.uid(uuid), *ev.Args.Instance, orNil(ev.Args.FromNode)), nil
}

func applyPause(paused, freeze bool) eventApplier {
	return func(ctx context.Context, d *driver, _ TimelineEvent) (string, error) {
		d.spec.Paused = paused
		d.spec.PauseFreeze = freeze
		if err := d.bumpGeneration(ctx); err != nil {
			return "", err
		}
		return fmt.Sprintf("paused=%t freeze=%t generation=%d", paused, freeze, d.owner.Generation), nil
	}
}

// applyOperationDeadline advances the clock onto the earliest in-flight
// operation deadline, plus an optional slack, so the escalation pass sees
// it elapsed. It is the table's timer.operationDeadline expressed the only
// way a deterministic harness can express a timer: by moving the clock.
func applyOperationDeadline(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	var earliest time.Time
	for _, row := range d.store.Rows() {
		op := row.Operation
		if op == nil || op.Deadline.IsZero() {
			continue
		}
		if earliest.IsZero() || op.Deadline.Time.Before(earliest) {
			earliest = op.Deadline.Time
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.operationDeadline: no in-flight operation carries a deadline")
	}
	return d.advanceOnto("timer.operationDeadline", earliest, ev)
}

// applyStuckPodGrace advances the clock onto the earliest instant at which
// a pod wedged in a terminal waiting reason has held it for the configured
// stuck-pod grace. The deadline is the start of the pod's own waiting
// episode (evidence.WaitingEpisodeStart) plus the configured grace, so the
// scenario names neither.
func applyStuckPodGrace(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	grace, err := ParseDuration("config.stuckPodGrace", d.cfg.StuckPodGrace)
	if err != nil {
		return "", err
	}
	if grace <= 0 {
		return "", fmt.Errorf("replay: timer.stuckPodGrace: config.stuckPodGrace is not set, so no grace elapses")
	}
	pods, err := query.ListOMENativePodsByName(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return "", fmt.Errorf("replay: timer.stuckPodGrace: list pods: %w", err)
	}
	var earliest time.Time
	for _, pod := range pods {
		since := evidence.WaitingEpisodeStart(pod)
		if !waitingTerminally(pod) || since.IsZero() {
			continue
		}
		due := since.Add(grace)
		if earliest.IsZero() || due.Before(earliest) {
			earliest = due
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.stuckPodGrace: no pod holds a terminal waiting reason, so no grace is running")
	}
	return d.advanceOnto("timer.stuckPodGrace", earliest, ev)
}

// applyRetryAt advances the clock onto the earliest RetryBlock retry
// instant the owner carries.
func applyRetryAt(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	var earliest time.Time
	for _, block := range d.store.RetryBlocks() {
		if block.NextRetryAt == nil || block.NextRetryAt.IsZero() {
			continue
		}
		if earliest.IsZero() || block.NextRetryAt.Time.Before(earliest) {
			earliest = block.NextRetryAt.Time
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.retryAt: no RetryBlock carries a nextRetryAt")
	}
	return d.advanceOnto("timer.retryAt", earliest, ev)
}

// applyRepairRetry advances the clock onto the earliest instant a repair
// parked at Failed may re-arm: the recorded failure plus the configured
// ladder's delay for its next attempt.
func applyRepairRetry(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	policy, err := retryPolicyOf("config.updateRetry", d.cfg.UpdateRetry)
	if err != nil {
		return "", err
	}
	if policy == nil {
		return "", fmt.Errorf("replay: timer.repairRetry: config.updateRetry is not set, so no retry ladder runs")
	}
	input := types.ReconcileInput{UpdateRetryPolicy: policy, Clock: d.clock}
	var earliest time.Time
	for _, row := range d.store.Rows() {
		row := row
		at, owed := workloadops.RepairRetryAt(input, &row)
		if !owed {
			continue
		}
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.repairRetry: no repair parked at Failed owes a re-arm")
	}
	return d.advanceOnto("timer.repairRetry", earliest, ev)
}

// applyMigrationDeadline advances the clock onto the earliest deadline a
// non-terminal migration record carries. The record's Deadline is the
// migration timeout authority — distinct from the Instance operation
// deadline stamped on the same rows, which the migrate pin makes the
// escalation skip.
func applyMigrationDeadline(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	var earliest time.Time
	for _, record := range d.migrations {
		if record.Phase.Terminal() || record.Deadline.IsZero() {
			continue
		}
		if earliest.IsZero() || record.Deadline.Time.Before(earliest) {
			earliest = record.Deadline.Time
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.migrationDeadline: no in-flight migration record carries a deadline")
	}
	return d.advanceOnto("timer.migrationDeadline", earliest, ev)
}

// applyForceDelete advances the clock onto the earliest instant at which
// one half of the force-delete predicate holds, read off the same fields
// the engine classifies on so the scenario restates neither: the
// overdueSlack variant is a Terminating pod past its own deletion deadline
// plus config.forceDelete.overdueSlack; the nodeUnreachable variant is a
// bound pod's node unreachable, by taint or by NodeReady, for
// config.forceDelete.nodeUnreachableThreshold.
func applyForceDelete(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if d.cfg.ForceDelete == nil {
		return "", fmt.Errorf("replay: timer.forceDelete: config.forceDelete is not set, so no force-delete clock runs")
	}
	pods, err := query.ListOMENativePodsByName(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return "", fmt.Errorf("replay: timer.forceDelete: list pods: %w", err)
	}
	switch ev.Variant {
	case "nodeUnreachable":
		threshold, err := ParseDuration("config.forceDelete.nodeUnreachableThreshold", d.cfg.ForceDelete.NodeUnreachableThreshold)
		if err != nil {
			return "", err
		}
		earliest, err := d.earliestNodeEvidence(ctx, pods, threshold)
		if err != nil {
			return "", err
		}
		if earliest.IsZero() {
			return "", fmt.Errorf("replay: timer.forceDelete[nodeUnreachable]: no bound pod sits on a node with unreachable evidence, so no threshold is running")
		}
		return d.advanceOnto("timer.forceDelete[nodeUnreachable]", earliest, ev)
	case "", "overdueSlack":
		slack, err := ParseDuration("config.forceDelete.overdueSlack", d.cfg.ForceDelete.OverdueSlack)
		if err != nil {
			return "", err
		}
		var earliest time.Time
		for _, pod := range pods {
			if pod.DeletionTimestamp == nil {
				continue
			}
			due := pod.DeletionTimestamp.Add(slack)
			if earliest.IsZero() || due.Before(earliest) {
				earliest = due
			}
		}
		if earliest.IsZero() {
			return "", fmt.Errorf("replay: timer.forceDelete: no pod is Terminating, so no overdue clock is running")
		}
		return d.advanceOnto("timer.forceDelete", earliest, ev)
	}
	return "", fmt.Errorf("replay: timer.forceDelete[%s] is not a variant the driver resolves", ev.Variant)
}

// earliestNodeEvidence resolves, over the nodes the pods are bound to, the
// earliest instant the node-death evidence becomes actionable: the
// unreachable taint's TimeAdded or the NodeReady transition plus the
// threshold, exactly the instants the evidence ages from. A node the
// cluster does not hold is gone already and runs no clock.
func (d *driver) earliestNodeEvidence(ctx context.Context, pods []*corev1.Pod, threshold time.Duration) (time.Time, error) {
	var earliest time.Time
	seen := map[string]bool{}
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" || seen[nodeName] {
			continue
		}
		seen[nodeName] = true
		node := &corev1.Node{}
		if err := d.cli.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return time.Time{}, fmt.Errorf("replay: get node %s: %w", nodeName, err)
		}
		consider := func(at time.Time) {
			due := at.Add(threshold)
			if earliest.IsZero() || due.Before(earliest) {
				earliest = due
			}
		}
		for _, taint := range node.Spec.Taints {
			if taint.Key == corev1.TaintNodeUnreachable && taint.TimeAdded != nil {
				consider(taint.TimeAdded.Time)
			}
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
				consider(cond.LastTransitionTime.Time)
			}
		}
	}
	return earliest, nil
}

// waitingTerminally reports whether any of a pod's containers is parked in
// a waiting reason it cannot get itself out of — the set the stuck-pod
// grace runs against.
func waitingTerminally(pod *corev1.Pod) bool {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && types.IsTerminalWaitingReason(cs.State.Waiting.Reason) {
				return true
			}
		}
	}
	return false
}

// advanceOnto moves the clock onto a deadline the engine's own state
// carries, plus the slack the scenario names. Every timer event works this
// way: a deterministic harness can only express a timer as a clock move,
// and how far past a deadline an observation lands is a property of the
// scenario rather than a default the harness is entitled to invent — so the
// duration never has to be spelled out in the timeline, and a config change
// moves the deadline without rewriting the scenario.
func (d *driver) advanceOnto(event string, deadline time.Time, ev TimelineEvent) (string, error) {
	if ev.Args.Slack == "" {
		return "", fmt.Errorf("replay: %s needs a slack: how far past the deadline the pass observes it is the scenario's choice", event)
	}
	slack, err := ParseDuration(event+".slack", ev.Args.Slack)
	if err != nil {
		return "", err
	}
	target := deadline.Add(slack)
	if target.Before(d.clock.Now()) {
		return "already-elapsed", nil
	}
	d.clock.SetTime(target)
	return "to=" + d.norm.at(target), nil
}

func podImage(pod *corev1.Pod, container string) string {
	for _, c := range pod.Spec.Containers {
		if c.Name == container {
			return c.Image
		}
	}
	return ""
}
