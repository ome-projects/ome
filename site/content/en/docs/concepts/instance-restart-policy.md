---
title: "OMENative Instance Restart Policy"
linkTitle: "Instance Restart Policy"
weight: 31
description: >
  How OMENative recovers a crashed pod or a lost gang member — lifecycle.restartPolicy with None versus RecreateInstanceOnPodRestart, what triggers a whole-Instance recreate, and how the drain-and-rebuild runs.
---

An [OMENative](/ome/docs/concepts/omenative) component's pods are grouped into **Instances** — a single pod, or a leader pod plus its worker pods (a gang). When a managed pod fails while the component is running, `lifecycle.restartPolicy` decides whether OMENative repairs the failed pod alone or drains and recreates every pod in the Instance together:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: deepseek-r1-native
spec:
  model:
    name: deepseek-r1
  engine:
    minReplicas: 2
    lifecycle:
      restartPolicy: RecreateInstanceOnPodRestart
    leader:
      runner:
        name: ome-container
        resources:
          limits:
            nvidia.com/gpu: "8"
    worker:
      size: 1
      runner:
        name: ome-container
        resources:
          limits:
            nvidia.com/gpu: "8"
```

This field is **not** the pod-level `restartPolicy`. The standard `restartPolicy` (a `PodSpec` field, also inlined into the engine/decoder/router spec) tells the **kubelet** whether to restart a container in place inside its pod; `lifecycle.restartPolicy` tells the **OMENative controller** whether a pod failure should recycle the whole Instance. That name collision is why the OMENative field nests under `lifecycle:`. Like the rest of the `lifecycle` block, it is read only for components that resolve to the OMENative deployment mode.

## The two policies

| `restartPolicy` | Recovery unit | Default for |
|-----------------|---------------|-------------|
| `None` | The failed pod alone — the rest of the Instance is never touched | Single-pod Instances |
| `RecreateInstanceOnPodRestart` | The whole Instance — every pod is drained, deleted, and recreated together | Multi-pod (leader/worker) Instances |

### `None`

Recovery is per pod. A crashed container is restarted in place by the kubelet under the pod's own `restartPolicy` (which defaults to `Always`), and OMENative does not react to it. A pod that is gone entirely — terminally `Failed`, evicted, or deleted — is recreated individually at the Instance's current incarnation, without draining or recreating the pods around it.

### `RecreateInstanceOnPodRestart`

Any pod failure recycles the Instance as a unit: the surviving pods are withdrawn from traffic, every pod is deleted, and the full set is recreated together at a bumped **incarnation**. This is the default for leader/worker Instances because a gang's pods form one process group: a member that dies and comes back — even via the kubelet's in-place container restart — is no longer part of the distributed process group its peers formed. Only the whole-Instance restart drains the survivors, so the replacement set is scheduled together into a consistent topology domain instead of the replacement chasing a placement the survivors still pin.

## Where the value comes from

For each component, the first layer that sets `lifecycle.restartPolicy` wins:

1. **The InferenceService component** — `spec.engine.lifecycle.restartPolicy` (likewise `decoder`, `router`).
2. **The ServingRuntime** — the same block on the runtime's `engineConfig` / `decoderConfig` / `routerConfig`, merged with your component spec (your fields take precedence).
3. **Fixed fallback by shape** — multi-pod Instances run as `RecreateInstanceOnPodRestart`, single-pod Instances as `None`.

Unlike `updateStrategy` and `minReadySeconds`, there is no cluster-ConfigMap defaulting layer for this field.

## What triggers a whole-Instance restart

Under `RecreateInstanceOnPodRestart`, on an Instance at phase `Ready`:

- **A pod is `Failed`.** The trigger reason names the actual cause from the container's termination state (for example `pod deepseek-r1-native-engine-0-worker-0 container ome-container failed (OOMKilled, exit 137)`).
- **The live pod count is below desired** — a pod vanished (deleted, evicted) and left the Instance short of its set.
- **The main container restarted after the Instance became Ready.** The runner container (`ome-container`) carrying restart evidence dated after the Instance's `readySince` triggers a recreate even though the kubelet's in-place restart left the pod `Running` — the restarted process is no longer part of the process group formed at Ready. Container restarts that finished during boot never count, and sidecar or init container restarts never trigger: the kubelet's in-place restart is their recovery path.

Below `Ready`, one trigger applies in any phase: **a materialized gang lost a member while at least one survivor remains**. A gang that loses a member while still forming can never reach Ready on its own, so waiting for Ready would wait forever. This trigger reads nothing time-based — a gang that merely takes hours to load weights is not loss. Total loss (no survivors) is instead rebuilt by the create machinery as a fresh start, and a rebuild of a never-Ready Instance answers to the retry budget recorded against its revision, exactly as a fresh create does.

Under `None`, all of these triggers are off. Two behaviors are policy-independent:

- **An open restart always finishes.** Switching the policy mid-repair does not abandon a half-drained Instance; the in-flight attempt runs to completion.
- **The crash-loop repair** (next section) fires under every policy.

## The crash-loop repair (every policy)

A `Ready`, operation-free Instance holding a pod wedged in a terminal kubelet waiting state — `CrashLoopBackOff`, an image-pull failure (`ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`), or a container create/run error (`CreateContainerError`, `CreateContainerConfigError`, `RunContainerError`) — on the component's current revision for longer than the operator's stuck-pod grace is repaired through the same whole-Instance restart, under **every** restart policy: the pod cannot recover on its own and no other machinery owns that shape. `None` keeps its meaning for mere container restarts — a container that died and came back is not a wedge.

The grace window is `lifecycle.stuckPodGracePeriod` in the `inferenceservice-config` ConfigMap (the `ome-resources` chart ships `60s`); removing it disables this repair. Because this repair takes a still-serving Instance offline, it is admitted the way any capacity-taking attempt is — the per-component unavailability budget and the cross-component coordination gate must both admit it, and the retry budget recorded against the revision denies it once automatic attempts are spent. A component wedged on a bad revision therefore repairs at the configured pace, and a revision that keeps crash-looping is held rather than recycled forever. The pod-loss triggers above are different: they repair an outage rather than cause one, so they start without consuming any budget.

## How the restart runs

The Instance enters phase `Restarting`, its `incarnation` counter is bumped, and a Warning event `RestartTriggered` records the cause. Then:

1. **Diagnostics are preserved.** Before anything is torn down, the failing pod's termination details are recorded into the Instance's `lastFailure` status field (pod, container, reason, exit code, message) — the pod is about to be deleted, and this field is the surviving trace when a gang keeps recreating.
2. **Drain and delete.** Every surviving pod's `ome.io/serving` readiness gate flips to `False`; once the endpoints have withdrawn from the traffic rotation, all pods of the old incarnation are deleted.
3. **Recreate.** The full pod set is recreated at the bumped incarnation (stamped on pods via the `ome.io/instance-incarnation` label) — on the **same revision** the Instance was running. A restart never advances revisions; rolling template changes is the update machinery's job.
4. **Promote.** Once every new pod is runtime-ready, serving flips back on, `minReadySeconds` pacing (when set) is honored, and the Instance returns to `Ready` with a Normal event `RestartCompleted`.

Each attempt is bounded by the component's effective `instanceReadyTimeout` (the per-component `lifecycle.instanceReadyTimeout`, else the operator-wide value — the chart default is `30m`). An attempt that overruns is failed; a gang still missing a member can re-arm another attempt, each cycle bounded by the same deadline.

If the restart finds a pod matching the Instance's selector but missing the `ome.io/instance-incarnation` label, it refuses to delete anything: a Warning event `FoundOrphan` names the pod, and the restart waits until you re-classify or remove it.

Pausing a rollout does not suspend recovery — a paused component keeps repairing its existing Instances. Only a frozen pause starts no new repair (an open one still finishes its current step).

## What `restartPolicy` does not govern

- **Template changes.** Rolling an Instance to a new revision is [update strategies](/ome/docs/concepts/omenative-update-strategies); a restart rebuilds the Instance on the revision it was already running.
- **A new Instance that never becomes Ready.** That is the `instanceReadyTimeout` readiness deadline, which parks the failed attempt rather than restarting anything.
- **Instances parked at `Failed`.** Automatic recovery acts on running Instances; rebuilding a parked one is the manual [`ome.io/reset-instances` annotation](/ome/docs/tasks/reset-failed-instances) on the InferenceReplica (`all`, or a comma-separated list of Instance indices).

## Observing a restart

Per-Instance detail lives on the owning InferenceReplica; the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome-instance-list) reads it for you:

```bash
kubectl ome instance list deepseek-r1-native -n <namespace>
```

A row that was just repaired shows the bumped incarnation in its `IDX/INC` cell, and the `F` flag in the `AOF` column marks the preserved failure record. To read the record's contents — plus the Instance's conditions and any in-flight operation — switch to the single-instance deep dive, `kubectl ome instance status deepseek-r1-native 0 --component engine`. The record is the Instance's `lastFailure`:

```yaml
lastFailure:
  podName: deepseek-r1-native-engine-0-worker-0
  containerName: ome-container
  reason: OOMKilled
  exitCode: 137
  time: "2026-09-26T11:47:31Z"
```

On the InferenceReplica object itself, the per-Instance rows are stored either as the dense `status.instanceStatuses` list or grouped into `status.instanceStatusColumns`, depending on the manager's configured [status encoding](/ome/docs/administration/omenative-status-encoding); both decode to the same rows.

The events on the InferenceReplica tell the same story: `RestartTriggered` (Warning, with the cause and new incarnation) followed by `RestartCompleted` (Normal).

## Related resources

- [Deployment Modes and OMENative](/ome/docs/concepts/omenative) — what OMENative and Instances are, and how a component opts in
- [OMENative Update Strategies](/ome/docs/concepts/omenative-update-strategies) — how template changes roll, and the budgets that also pace crash-loop repairs
- [Gang Scheduling](/ome/docs/concepts/gang_scheduling) — how a multi-pod Instance's pods are scheduled together, including the recreated set
