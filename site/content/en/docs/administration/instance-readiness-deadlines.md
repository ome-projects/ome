---
title: "Instance Readiness Deadlines"
linkTitle: "Instance Readiness Deadlines"
weight: 8
description: >
  How long OMENative waits for a new Instance to become Ready — the instanceReadyTimeout deadline, the stuck-pod and unschedulable escalations, and the three ways an expired attempt is disposed.
---

When an [OMENative](/ome/docs/concepts/omenative) component creates, updates, or restarts an Instance, the controller records an **operation** on the owning InferenceReplica (`status.instanceStatuses[].operation`) and stamps a **deadline** on it: the operation's start time plus the component's effective `instanceReadyTimeout`. If the Instance has not become Ready when the deadline elapses, the attempt is failed and classified — the revision is held, the rebuild is relocated to another node, or the Instance is left `Failed` for an operator.

This page answers "my new Instance is stuck — how long will OME wait, and what will it do?". It covers three windows:

| Window | Bounds | Chart default |
|--------|--------|---------------|
| `instanceReadyTimeout` | The whole attempt, from operation start to Instance Ready. | `30m` |
| `stuckPodGracePeriod` | How long a pod may sit in a terminal kubelet waiting state (`CrashLoopBackOff`, `ImagePullBackOff`, ...) before the Instance is failed **without** waiting out the full readiness window. | `60s` |
| `unschedulableGracePeriod` | How long a pod may carry `PodScheduled=False` with reason `Unschedulable` before the Instance is failed as environment-caused. | `15m` |

A related but distinct mechanism is the same-target retry backoff (`lifecycle.updateRetry`): these deadlines bound **how long one attempt may run**; the retry backoff bounds **how many times a failed target revision is re-attempted**. See [Release a Held Revision](/ome/docs/tasks/release-a-held-revision) for the retry side.

## The readiness deadline: `instanceReadyTimeout`

The effective timeout for a component resolves in two steps:

1. **Per-component**: `spec.<component>.lifecycle.instanceReadyTimeout` on the InferenceService (`engine`, `decoder`, or `router`) always wins when set to a positive duration.
2. **Operator-wide**: otherwise the `lifecycle.instanceReadyTimeout` value in the `inferenceservice-config` ConfigMap applies (Helm value `ome.controller.lifecycle.instanceReadyTimeout`, chart default `30m`).

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
spec:
  engine:
    lifecycle:
      instanceReadyTimeout: 45m   # this component only; wins over the ConfigMap
```

The OME binary has **no built-in window**. The deadline is stamped when the operation opens, so a configuration change applies to operations that open (or resume from an external hold) afterwards — it does not move deadlines already in flight.

The live deadline is visible on the InferenceReplica:

```bash
kubectl get inferencereplica chat-engine -n prod \
  -o jsonpath='{.status.instanceStatuses}' | jq '.[] | {index, phase, operation}'
```

`operation.deadline` is the hard timeout for the attempt; a **null** deadline means either the clock is parked on an external wait (below) or no timeout is configured at all.

### When no timeout is configured

If neither level supplies a value, the backstop simply does not exist: operations open with no deadline, and an Instance that never becomes Ready waits for an operator instead of being failed against a fabricated window. The controller reports this rather than treating it as an error:

- The InferenceReplica carries the condition `InstanceReadyTimeoutUnconfigured=True` (mirrored into the InferenceService's `status.components.<component>.lifecycle.conditions`) with the message `no instanceReadyTimeout is set on the Component or in lifecycle.instanceReadyTimeout; operations open with no readiness deadline`.
- A Warning event with reason `InstanceReadyTimeoutUnconfigured` fires once per episode (edge-triggered off the condition, not repeated every reconcile).

Once either level supplies a window, the condition flips to `False` with reason `InstanceReadyTimeoutConfigured` and a message naming the effective deadline.

### External waits park the clock

A wait caused by something outside the workload's control does not count against the timeout. While one of these holds stands, the controller zeroes `operation.deadline` (the clock parks), and when the hold releases it restarts the clock at the **full** window — the deadline measures from admission, not from operation start:

- **Admission scheduling gates** — a pod still carrying a scheduling gate (for example, queued by Kueue).
- **`waiting: QuotaExceeded`** — the apiserver refused the operation's pod creates for lack of ResourceQuota.
- **`waiting: Unschedulable`** — the scheduler reports it cannot place one of the operation's pods (see the [unschedulable escalation](#escalating-an-unschedulable-pod-unschedulablegraceperiod) below, which is the only thing that ends this wait).
- **`waiting: PodGroupTerminating`** — a gang's PodGroup name is still held by a terminating object.

The `operation.waiting` field names the external blocker; the blocking authority's own message lands in Events, not in status. A [paused rollout](/ome/docs/tasks/pause-and-resume-a-rollout) is likewise never expired: a paused operation is skipped by both escalation paths for the length of the pause.

## Fast escalation: stuck pods (`stuckPodGracePeriod`)

Waiting 30 minutes for a pod that has already told you its image does not exist is pointless. When a live pod of an in-flight attempt has a container (regular or init) parked in a **terminal kubelet waiting state** —

`CrashLoopBackOff`, `ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`, `CreateContainerConfigError`, `CreateContainerError`, `RunContainerError`

— for longer than `stuckPodGracePeriod`, the Instance is failed immediately instead of waiting out the readiness deadline. The window is measured from the pod's creation time (the kubelet exposes no per-state transition time), so the grace is what separates a genuinely stuck image pull from the brief `ContainerCreating` / `PodInitializing` window every pod passes through.

The chart default is `60s`. Removing the field disables this fast path for the affected passes — the `instanceReadyTimeout` backstop still fires.

## Escalating an unschedulable pod (`unschedulableGracePeriod`)

A pod the scheduler cannot place is queued on cluster capacity, not failing: the hold is recorded as `operation.waiting: Unschedulable` and the readiness clock parks. Only `unschedulableGracePeriod` ends that wait. When the pod has carried `PodScheduled=False` with reason `Unschedulable` for longer than the grace — measured from that condition's `lastTransitionTime` — the Instance is failed as **environment-caused**: the failure record carries the scheduler's own message (which constraint had no placement), and the target revision is never blamed, because an unplaceable pod proves nothing about the pod template.

The chart default is `15m`. Removing the field disables the escalation entirely: an unplaceable pod parks the clock and waits for an operator instead.

## What happens when the deadline expires

An expired or stuck attempt is classified into exactly one of three outcomes. Whichever way it goes, the Instance transitions to `Phase=Failed`, its diagnostics survive in `status.instanceStatuses[].lastFailure`, and a Warning event with reason `InstanceFailed` is emitted against the InferenceService.

1. **Workload-caused → the revision is held.** A live pod shows a failure that deterministically travels with the pod template — `ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`, `CreateContainerConfigError`, or an `InvalidPodSpec` apiserver rejection — so retrying or relocating would reproduce it. The controller records a retry block against the target revision in `status.retryBlocks`, clears the operation, and stamps `Phase=Failed`. Recovery is publishing a corrected revision (or an explicit [operator release](/ome/docs/tasks/release-a-held-revision)); the same revision is not re-attempted on its own beyond the `lifecycle.updateRetry` budget.

2. **Relocatable → the rebuild is steered off the node.** No workload-caused evidence (a bare timeout, a crash loop, a `RunContainerError` — all ambiguous between the revision and the node), the component's effective migration policy allows automatic relocation (`spec.<component>.lifecycle.migrationPolicy.mode` unset or `Auto`; `Never` disables it), the `lifecycle.autoMigrate` relocation budget is configured and not exhausted, and the attempt's own pods occupy exactly one node. The controller records a **relocation directive** naming that suspect node, then fails the attempt; the normal rebuild renders with a node-affinity exclusion for the recorded nodes, so the next attempt lands elsewhere. Each directive emits a Normal event `AutoMigrationTriggered` (`attempt <n>/<maxAttempts>, rebuild steered off node <node>`), appears in `status.migrations` as a born-terminal `Relocated` record, and counts against `autoMigrate.maxAttempts` (chart default `3`) per Instance. Filling the last budget slot emits one Warning event `AutoMigrationCapReached`; further expiries dispose terminally without relocation. An Instance that reaches Ready resets its budget.

3. **Terminal → wait for an operator or the next trigger.** Everything else: the operation is cleared and the Instance is stamped `Phase=Failed` with reason `DeadlineExceeded` (message `DeadlineExceeded: <type>/<step> exceeded InstanceReadyTimeout`) or the more specific evidence when a pod supplied any — a pod that ran but never passed its readiness probes, or one whose readiness gate never folded. No revision is blamed. The owning operation reconciler may attempt again only when its own safety checks pass.

Removing the `autoMigrate` block from the configuration disables branch 2 entirely — every non-workload-caused expiry then disposes terminally.

Two boundaries of this machinery are worth knowing:

- **Serving Instances are never escalated.** An Instance whose pods are all Ready and in the traffic rotation at the desired count is exempt from every path, whatever stale evidence says — failing a serving workload would be a status lie.
- **Migrations and deletions have their own authorities.** A `Migrate` operation is bounded by its record in `status.migrations` (the same `instanceReadyTimeout` window, enforced by the migration expiry pass), and a `Delete` by the teardown machinery — neither is expired by the generic deadline described here. Multi-pod gang **update** surges also route their expiry through the gang abandon path (surge teardown, then the same retry-block accounting) rather than the three-way disposition above.

## Configuration reference

All four settings live in the `lifecycle` block of the `inferenceservice-config` ConfigMap, rendered by the `ome-resources` chart from `ome.controller.lifecycle`:

```yaml
ome:
  controller:
    lifecycle:
      instanceReadyTimeout: 30m
      stuckPodGracePeriod: 60s
      unschedulableGracePeriod: 15m
      autoMigrate:
        maxAttempts: 3
```

| Setting | Chart default | When absent |
|---------|---------------|-------------|
| `instanceReadyTimeout` | `30m` | No deadline for components that set none of their own; `InstanceReadyTimeoutUnconfigured` condition + one Warning event. |
| `stuckPodGracePeriod` | `60s` | Fast escalation disabled; the readiness deadline still fires. |
| `unschedulableGracePeriod` | `15m` | An unplaceable pod parks the clock indefinitely and waits for an operator. |
| `autoMigrate.maxAttempts` | `3` | The relocation branch is disabled; non-workload-caused expiries dispose terminally. |

The OME binary carries **no built-in value for any of them** — absent configuration means the corresponding behavior does not exist, never a silent fallback. An invalid duration is logged and treated as unconfigured for that pass. Only the per-component `spec.<component>.lifecycle.instanceReadyTimeout` overrides a ConfigMap value; the two grace periods and the relocation budget are operator-level only.

The component's effective `instanceReadyTimeout` is also the input from which multi-pod Instances derive their PodGroup schedule timeout — see [Gang Scheduling](/ome/docs/concepts/gang_scheduling/#bounding-the-gang-schedule-timeout).

## Watching a stuck Instance

The operation row tells you which state the wait is in:

```bash
kubectl get inferencereplica chat-engine -n prod \
  -o jsonpath='{.status.instanceStatuses}' \
  | jq '.[] | {index, phase, waiting: .operation.waiting, deadline: .operation.deadline, lastFailure}'
```

- `deadline` in the future — the attempt is inside its window.
- `deadline: null` with a `waiting` token — the clock is parked on an external blocker (`QuotaExceeded`, `Unschedulable`, `PodGroupTerminating`).
- `deadline: null`, no `waiting`, and `InstanceReadyTimeoutUnconfigured=True` in conditions — no timeout is configured; the Instance will wait forever without operator action.
- `phase: Failed` — read `lastFailure.reason` (`DeadlineExceeded`, a kubelet reason like `ImagePullBackOff`, or `Unschedulable`) and `lastFailure.message` for the surviving diagnostics.

The escalations are mirrored as events on the InferenceService:

```bash
kubectl get events -n prod --field-selector reason=InstanceFailed
kubectl get events -n prod --field-selector reason=AutoMigrationTriggered
kubectl get events -n prod --field-selector reason=InstanceReadyTimeoutUnconfigured
```

## Related resources

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative) — what an Instance is and how a component resolves to OMENative
- [Gang Scheduling](/ome/docs/concepts/gang_scheduling) — the PodGroup schedule timeout derived from `instanceReadyTimeout`
- [Release a Held Revision](/ome/docs/tasks/release-a-held-revision) — inspecting and releasing the retry blocks the workload-caused disposition records
- [Request an Instance Migration](/ome/docs/tasks/request-an-instance-migration) — the operator-driven migration path, which shares the relocation machinery
