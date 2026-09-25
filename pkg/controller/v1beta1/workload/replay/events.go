package replay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
		"pod.scheduled":           applyPodScheduled,
		"pod.containersReady":     applyContainersReady,
		"pod.ready":               applyPodReady,
		"pod.servingOn":           applyServing(corev1.ConditionTrue),
		"pod.servingOff":          applyServing(corev1.ConditionFalse),
		"pod.terminating":         applyPodTerminating,
		"pod.stuckTerminating":    applyStuckTerminating,
		"pod.deleted":             applyPodDeleted,
		"pod.phaseTerminal":       applyPhaseTerminal,
		"pod.waiting":             applyPodWaiting,
		"pod.unschedulable":       applyUnschedulable,
		"pod.containerRestart":    applyContainerRestart,
		"endpoint.rotationIn":     applyRotation(true),
		"endpoint.rotationOut":    applyRotation(false),
		"api.quotaDenied":         applyRejection(quotaDenied),
		"api.invalid":             applyRejection(invalidPodSpec),
		"spec.revision":           applySpecRevision,
		"spec.strategy":           applySpecStrategy,
		"spec.replicasUp":         applyReplicas,
		"spec.replicasDown":       applyReplicas,
		"spec.pauseTrue":          applyPause(true, false),
		"spec.pauseFreeze":        applyPause(true, true),
		"spec.unpause":            applyPause(false, false),
		"spec.teardown":           applyTeardown,
		"spec.gangWidth":          applyGangWidth,
		"spec.rollbackTarget":     applyRollbackTarget,
		"spec.migrateRequest":     applyMigrateRequest,
		"gang.podGroup":           applyPodGroup,
		"timer.operationDeadline": applyOperationDeadline,
		"timer.stuckPodGrace":     applyStuckPodGrace,
		"timer.retryAt":           applyRetryAt,
		"timer.migrationDeadline": applyMigrationDeadline,
		"timer.forceDelete":       applyForceDelete,
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
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, d.clock.Now())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  container,
			Image: podImage(pod, container),
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  ev.Variant,
				Message: ev.Args.Message,
			}},
		}}
	})
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
// names. count defaults to one write.
func applyRejection(build func(resource, name string) error) eventApplier {
	return func(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
		verb := ev.Args.Verb
		if verb == "" {
			verb = "create"
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
}

func quotaDenied(resource, name string) error {
	return apierrors.NewForbidden(
		schema.GroupResource{Resource: resource}, name,
		errors.New("exceeded quota: compute-resources, requested: pods=1, used: pods=4, limited: pods=4"),
	)
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
	if ev.Args.Image == "" && ev.Args.Env == nil {
		return "", fmt.Errorf("replay: spec.revision needs a template edit: the new image, the new env, or both")
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
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("%sgeneration=%d", detail, d.owner.Generation), nil
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
// the scale-down pipeline runs.
func applyTeardown(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant != "" && ev.Variant != "deleted" {
		return "", fmt.Errorf("replay: spec.teardown[%s]: the driver models the owner's own deletion, not an owner replaced under a new UID", ev.Variant)
	}
	d.teardown = true
	return "teardown=true", nil
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
// stuck-pod grace. The deadline is the pod's own creationTimestamp plus the
// configured grace, so the scenario names neither.
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
		if !waitingTerminally(pod) || pod.CreationTimestamp.IsZero() {
			continue
		}
		due := pod.CreationTimestamp.Time.Add(grace)
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

// applyForceDelete advances the clock onto the earliest instant at which a
// Terminating pod is past its own deletion deadline plus the configured
// overdue slack. That is the first half of the force-delete predicate, read
// off the same pod field the engine classifies on, so the scenario restates
// neither the slack nor the deletion instant.
//
// The node half is evidence rather than a clock the driver can move: the
// fake cluster holds no Node objects, so a scheduled pod's node reads as
// gone the moment the overdue clock lands, and there is no unreachable
// interval to age.
func applyForceDelete(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant == "nodeUnreachable" {
		return "", fmt.Errorf("replay: timer.forceDelete[nodeUnreachable]: the driver seeds no Node objects, so a pod's node reads as gone rather than as unreachable for a measurable time")
	}
	if d.cfg.ForceDelete == nil {
		return "", fmt.Errorf("replay: timer.forceDelete: config.forceDelete is not set, so no force-delete clock runs")
	}
	slack, err := ParseDuration("config.forceDelete.overdueSlack", d.cfg.ForceDelete.OverdueSlack)
	if err != nil {
		return "", err
	}
	pods, err := query.ListOMENativePodsByName(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return "", fmt.Errorf("replay: timer.forceDelete: list pods: %w", err)
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
