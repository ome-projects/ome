---
title: Deployment modes and OMENative
description: OME runs InferenceService components as Kubernetes Deployments or on OMENative, which creates, updates, repairs and moves each replica as one unit. Opt in to OMENative, see how OME picks a component's mode, and read what OMENative creates and reports.
---

An [InferenceService](../serving/inference-services.md) component, the engine, decoder or router, runs in a deployment mode, which decides what creates and manages its pods: a Kubernetes Deployment, or [OMENative](../omenative/overview.md). On OMENative, the component's replicas run as [Instances](../omenative/instances.md). An Instance is one pod, or a leader and its workers, and OME creates, updates, repairs and moves it as one unit. You can put a whole InferenceService on OMENative, or one component.

## The deployment modes

| Mode | What runs the pods | How a component gets it |
| --- | --- | --- |
| `RawDeployment` | A Deployment named `{isvc}-{component}` | `spec.deploymentMode: RawDeployment` or the component's `ome.io/deploymentMode` annotation. The default when nothing sets a mode. |
| `OMENative` (since v1.3) | An InferenceReplica named `{isvc}-{component}`, which runs the replicas as Instances. See [OMENative](#omenative). | `spec.deploymentMode: OMENative` or the component's annotation. The default for an engine or decoder with a `leader` or `worker`. |
| `MultiNode` (deprecated) | A LeaderWorkerSet named `lws-{isvc}-{component}`. See [MultiNode](#multinode). | The component's annotation, on the engine or the decoder only. |

All three modes put a Service named `{isvc}-{component}` in front of the component. On RawDeployment, a change is a plain Deployment update, with no rollout group, canary or held revision. A RawDeployment or OMENative component also gets a PodDisruptionBudget, from its `minAvailable` or `maxUnavailable` or the `ome-resources` chart's default of `maxUnavailable: 1`: see [Configure pod disruption budgets](../../guides/deploy-models/configure-pod-disruption-budgets.md). [Autoscaler policy](../serving/autoscaler-policy.md) explains when it gets an autoscaler.

An InferenceService with both an engine and a decoder is prefill-decode disaggregated, a shape rather than a mode: the engine runs prefill and the decoder runs decode. On OMENative, each gets its own InferenceReplica, and the two can roll out as one [rollout group](../rollouts-and-traffic/rollout-groups.md).

## Opt in to OMENative {since=v1.3}

Opting in gives any component, even one with a single pod per replica, what OMENative does for its Instances:

- By default, an update brings the new pod up and serving before the old one drains.
- With the chart's settings, an update that fails because of the new revision itself, as with an image that can't be pulled, is retried with backoff. After the last attempt, OME holds the revision until a newer revision replaces it or you [release it](../../guides/roll-out-changes/release-a-held-revision.md).
- You can [migrate an Instance](../../guides/scale-and-migrate/request-an-instance-migration.md) off its node, and the replacement starts before the original is removed.
- Status has a row for each Instance, with its phase, revisions and pod counts.

[Rollout groups](../rollouts-and-traffic/rollout-groups.md), canaries included, need OMENative declared in the InferenceService, with `spec.deploymentMode` or the component's own annotation, or the webhook rejects them with `CoordinationRequiresOMENative` or `CanaryRequiresOMENative`. A `leader`, a `worker` or the runtime's annotation doesn't count.

### For the whole InferenceService

Set `spec.deploymentMode: OMENative` to run all the components on OMENative, the router included. The field takes `OMENative` or `RawDeployment`. A component's `ome.io/deploymentMode` annotation, from the InferenceService or the runtime, wins over it, so you can keep the router on a Deployment with `ome.io/deploymentMode: RawDeployment` in `spec.router.annotations`.

This InferenceService serves `llama-3-2-3b-instruct` from [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md) with one engine Instance:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: srt-llama-3-2-3b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

[Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md) applies an InferenceService like this one and checks its InferenceReplica, Instance and pod.

OMENative ignores a component's `deploymentStrategy` and paces rollouts with its `lifecycle.updateStrategy` instead: see [OMENative update strategies](omenative-update-strategies.md). The webhook can warn about the unused field. For the engine under `spec.deploymentMode: OMENative`, the warning is:

```text
engine.deploymentStrategy is set but deploymentMode is "OMENative"; deploymentStrategy applies only to RawDeployment and is ignored here — use engine.lifecycle.updateStrategy.rollingUpdate for rollout pacing
```

### For one component

To choose one component's mode, set its `ome.io/deploymentMode` annotation. This version annotates the engine and adds a router, which runs as a Deployment because nothing sets its mode:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: srt-llama-3-2-3b-instruct
  engine:
    annotations:
      ome.io/deploymentMode: OMENative
    minReplicas: 1
    maxReplicas: 1
  router:
    minReplicas: 1
    maxReplicas: 1
```

With both an engine and a decoder, declare both OMENative or neither. The webhook compares their annotations in the InferenceService, or `spec.deploymentMode`, and rejects a mismatch. For an engine annotated OMENative next to a decoder with no mode set, the message is:

```text
InvalidDeploymentModeCombination: engine deploymentMode="OMENative" does not match decoder deploymentMode=""; when either is OMENative, both must use the same value
```

## Change a component's mode {#change-a-components-mode}

When a component's mode changes, OME creates the new workload and leaves the old Deployment, InferenceReplica or LeaderWorkerSet running. The old pods keep their GPUs, so the new pods can stay `Pending` until you delete the old workload. When the component moves to OMENative, its Service `{isvc}-{component}` switches to the new pods at once, so it serves nothing until they're ready.

While ingress creation is off, as it is by default, the [external Service](../rollouts-and-traffic/ingress.md#the-external-service) `{isvc}` fronts the router, or the engine when there's no router, unless the InferenceService is cluster-local. It sends requests to that component's old and new pods alike until you delete the old workload.

For example, after you move the engine of `llama-3-2-3b-instruct` from a Deployment to OMENative, delete the engine's Deployment:

```bash
kubectl delete deployment llama-3-2-3b-instruct-engine -n llama-demo
```

```output
deployment.apps "llama-3-2-3b-instruct-engine" deleted from llama-demo namespace
```

## How OME resolves the mode

For a component the InferenceService declares, OME merges in the `engineConfig`, `decoderConfig` or `routerConfig` of its [serving runtime](../runtimes/serving-runtimes.md), and the InferenceService's values win. It takes the component's mode from the first of these that sets one:

1. The component's `ome.io/deploymentMode` annotation, in the InferenceService or the runtime: `spec.engine.annotations` or `engineConfig.annotations` for the engine. OME accepts `RawDeployment`, `OMENative`, `MultiNode` (deprecated) and `VirtualDeployment`, and skips any other value.
2. The InferenceService's `spec.deploymentMode`.
3. A `leader` or `worker` on the engine or the decoder, from the InferenceService or the runtime, which makes the component `OMENative`. The router has no `leader` or `worker`.
4. `RawDeployment`.

So a runtime's annotation wins over `spec.deploymentMode`, unless the InferenceService sets the annotation on the component itself. In the InferenceService's own `metadata.annotations`, the annotation sets no component's mode: only `VirtualDeployment` works there, as [Other mode values](#other-mode-values) shows.

[`kubectl ome runtime effective`](../../reference/kubectl-ome/runtime.md#effective) shows the components' modes and the rules that set them, as in `RawDeployment (Default)`.

When OME picks the runtime for you, it skips runtimes whose `engineConfig` or `decoderConfig` annotation declares a different mode from the InferenceService's for that component: see [Runtime deployment-mode matching](../../reference/matching/runtime-deployment-mode-matching.md).

## OMENative {since=v1.3}

For an InferenceService's OMENative component, OME writes an [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) named `{isvc}-{component}` from the InferenceService, and the InferenceReplica controller creates, updates and replaces the component's Instances. Make your changes in the InferenceService, and get or describe the InferenceReplica, short name `irep`, to see what's happening.

You can also author a [standalone InferenceReplica](../omenative/overview.md#standalone-inferencereplicas). It always runs on OMENative and has no InferenceService to select its deployment mode. The names, generated objects and InferenceService status described below refer to the InferenceService-managed form.

### Instances

An OMENative component runs as [Instances](../omenative/instances.md). An Instance is one pod, or, with a `leader` and a `worker`, a leader pod and `worker.size` worker pods that OME starts, stops and replaces together. The autoscaler, `minReplicas` and `maxReplicas` count Instances, not pods.

OME names the pods `{isvc}-{component}-{index}-{runner}-{ordinal}`, as in `llama-3-2-3b-instruct-engine-0-default-0` for the engine above: see [What an Instance is](../omenative/instances.md#what-an-instance-is).

### What OME creates

Besides the pods, OME creates these objects for an OMENative component:

| Object | Name | What it's for |
| --- | --- | --- |
| Service | `{isvc}-{component}` | Takes the component's traffic, as in the other modes |
| Headless Service | `{isvc}-{component}-headless` | Gives the pods DNS names, `{pod}.{isvc}-{component}-headless`, that resolve before the pods are ready, so a leader and its workers can find one another |
| Revision Services | `{isvc}-{component}-rev-{hash}`, and the same with `-headless` | One pair per revision, used to drain pods, connect components of the same revision and analyze canaries |
| ControllerRevision | `{isvc}-{component}-{hash}` | One per version of the pod template, which the InferenceReplica rolls Instances between |
| PodGroup | `{isvc}-{component}-{index}` | One per multi-pod Instance, for [gang scheduling](../serving/gang-scheduling.md), when the scheduler-plugins PodGroup CRD is installed |

OME also creates the PodDisruptionBudget and autoscaler from [The deployment modes](#the-deployment-modes). It adds a PodMonitor that scrapes all the pods, workers included, when the Prometheus operator's PodMonitor CRD was installed before the manager started.

For a multi-pod Instance, the Service selects only leaders when the InferenceService itself sets `worker` on the component, and workers too when only the runtime does. To select only leaders, add `leader: {}` and `worker: {}` to the component in the InferenceService, which keep the runtime's settings. Without a router, the [external Service](../../guides/networking/expose-without-ingress.md#expose-a-multi-node-engine) `{isvc}` sends requests to the workers too.

### Pod labels and environment

OME adds these labels to the pods, on top of those in the component's pod template. Where a key is in both, OME's value wins.

| Label | Value |
| --- | --- |
| `ome.io/inferenceservice` | The InferenceService's name |
| `component` | `engine`, `decoder` or `router` |
| `ome.io/managed-by` | `OMENative` |
| `ome.io/instance-index` | The Instance's index |
| `ome.io/instance-incarnation` | The Instance's [incarnation](../omenative/instances.md#incarnation): `1` when new, and up by one for each restart and each update that recreates its pods |
| `ome.io/runner` | `default`, `leader` or `worker` |
| `ome.io/pod-ordinal` | The pod's ordinal within its runner |
| `ome.io/revision-hash` | The hash of the pod's revision |
| `ome.io/pairing-protocol` | The revision's prefill-decode pairing protocol, when it has one |
| `scheduling.x-k8s.io/pod-group` | The Instance's PodGroup, on the pods of a multi-pod Instance |

It also sets these environment variables:

| Variable | Value |
| --- | --- |
| `OME_INFERENCESERVICE_NAME` | The InferenceService's name |
| `OME_COMPONENT` | `engine`, `decoder` or `router` |
| `OME_COMPONENT_REPLICAS` | The component's number of Instances |
| `OME_INSTANCE_INDEX` | The Instance's index |
| `OME_RUNNER` | `default`, `leader` or `worker` |
| `OME_RUNNER_SIZE` | The number of pods in the pod's runner: `1` for `default` and `leader`, the worker size for `worker` |
| `OME_RUNNER_INDEX` | The pod's position in its runner, from `0`. Always `0` for `default` and `leader`, even when the pod's ordinal is `1`. |
| `OME_INSTANCE_SUBDOMAIN` | `{isvc}-{component}-{index}`. It isn't a DNS name, so reach the leader through `OME_LEADER_ADDRESS`. |
| `OME_LEADER_ADDRESS` | Multi-pod Instances only. The leader's DNS name, `{isvc}-{component}-{index}-leader-0.{isvc}-{component}-headless` |
| `OME_INSTANCE_POD_RANK` | Multi-pod Instances only. The pod's rank in the Instance: `0` for the leader, and `k + 1` for worker `k` |
| `OME_INSTANCE_POD_COUNT` | Multi-pod Instances only. The number of pods in the Instance |
| `OME_<PEER>_ENDPOINT` | With `spec.rollout.groups` only. One per other component, as in `OME_DECODER_ENDPOINT`: `{isvc}-{peer}.{namespace}.svc.cluster.local` |
| `OME_<PEER>_REVISION_ENDPOINT` | With `spec.rollout.groups` only, when the pod pairs with a revision of the peer. That revision's Service, `{isvc}-{peer}-rev-{hash}.{namespace}.svc.cluster.local` |

OME sets them in all containers but the init containers, over any variable with the same name. The pods of a multi-pod Instance that request `google.com/tpu` also get `TPU_WORKER_HOSTNAMES`, `TPU_WORKER_ID` and the other `TPU_*` variables that multi-host JAX reads. A runtime written for a LeaderWorkerSet uses these instead of the `LWS_*` variables and labels: [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md#step-2-make-an-omenative-copy-of-the-runtime) has the mapping.

OMENative pods also have the readiness gate `ome.io/serving`. OME sets it to `False` to take the pod out of rotation without stopping it, as it does during a drain or before an in-place update.

## Observe OMENative in status {since=v1.3}

An OMENative component reports conditions, Instance counts and a row per Instance.

### Conditions

The InferenceReplica's `Ready` condition says whether the component serves, so you can wait on it:

```bash
kubectl wait --for=condition=Ready inferencereplica/llama-3-2-3b-instruct-engine -n llama-demo --timeout=15m
```

```output
inferencereplica.ome.io/llama-3-2-3b-instruct-engine condition met
```

A rollout in flight makes the condition `Unknown`, so `kubectl wait` keeps waiting until it finishes. The reason is the first row in this table that applies:

| Reason | Status | Message |
| --- | --- | --- |
| `InstanceFailed` | `False` | `At least one Instance has Phase=Failed` |
| `Staged` | `True` | `Staged at partition {partition}: {updated}/{total} Instances on {revision}, {held} held on the prior revision` |
| `RolloutInProgress` | `Unknown` | `Rolling out {revision} ({updated}/{total} Instances on target)` |
| `NoReplicas` | `False` | `InferenceReplica has no desired Instances` |
| `AllInstancesReady` | `True` | `{ready}/{total} Instances Ready` |
| `MinimumAvailable` | `True` | `{serving}/{total} Instances serving (min {minimum})` |
| `ReplicaCountMismatch` | `False` | `{serving}/{total} Instances serving, need {minimum} (no rollout in flight)` |

The minimum is the desired number of Instances less the `maxUnavailable` of the component's update strategy, or all of them when it isn't set. `Staged` means the rollout holds at a partition on purpose: see [OMENative update strategies](omenative-update-strategies.md).

Other conditions flag problems without changing `Ready`:

| Condition | `True` when | Reasons when `True`, `False` |
| --- | --- | --- |
| `RolloutStalled` | Mid-rollout, an Instance not yet on the new revision has failed, as with a new pod in `CrashLoopBackOff`. The old pods can keep `EngineReady`, `DecoderReady` or `RouterReady` `True`. | `InstancesFailing`, `Progressing` |
| `DrainOverdue` | An Instance being removed is past the deadline of the operation that drains it. | `DrainsOverdue`, `DrainsWithinDeadline` |
| `GangSchedulingUnavailable` | The scheduler-plugins PodGroup CRD isn't installed, so a multi-pod Instance's pods may schedule partially. | `PodGroupCRDNotInstalled`, `GangSchedulingAvailable` |
| `MigrationPolicyUnconfigured` | A migration request waits because `ome.controller.lifecycle.audit` isn't set. The chart sets it. | `MigrationCapacityUnconfigured`, `MigrationCapacityConfigured` |
| `InstanceReadyTimeoutUnconfigured` | Both the component's `lifecycle.instanceReadyTimeout` and the operator's are unset, so an Instance that stays unready waits for you instead of failing. The chart sets `ome.controller.lifecycle.instanceReadyTimeout: 30m`. | `InstanceReadyTimeoutUnconfigured`, `InstanceReadyTimeoutConfigured` |

OME also sets the InferenceService's `EngineReady`, `DecoderReady` or `RouterReady` condition for an OMENative component, with the same minimum:

| Reason | Status | Message |
| --- | --- | --- |
| `Ready` | `True` | `All Instances are Ready` |
| `MinimumAvailable` | `True` | `{serving}/{total} Instances serving (min {minimum})` |
| `InsufficientAvailable` | `False` | `{serving}/{total} Instances serving, need {minimum}` |
| `NoReplicas` | `False` | `Component has no desired Instances` |

### Lifecycle status

The InferenceService reports an OMENative component's Instance counts and revisions under `status.components.{component}.lifecycle`, a [LifecycleStatus](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-LifecycleStatus). Show the engine's:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'custom-columns=REPLICAS:.status.components.engine.lifecycle.replicas,READY:.status.components.engine.lifecycle.readyReplicas,SERVING:.status.components.engine.lifecycle.servingReplicas,REVISION:.status.components.engine.lifecycle.currentRevision'
```

```output
REPLICAS   READY   SERVING   REVISION
1          1       1         llama-3-2-3b-instruct-engine-5d8f9c7a
```

`readyReplicas`, `servingReplicas` and `availableReplicas` count Instances as [Readiness and availability](../omenative/instances.md#readiness-and-availability) defines them. The other fields:

| Field | What it reports |
| --- | --- |
| `currentRevision` | The ControllerRevision that serves traffic |
| `updateRevision` | The ControllerRevision being rolled out |
| `replicas` | Instances, in any phase |
| `updatedReplicas` | Instances on `updateRevision` |
| `updatedReadyReplicas` | Instances on `updateRevision` whose pods are all ready |
| `rolloutHold` | The gate that last held the rollout, why, and since when. Cleared once the rollout advances or completes. |

`rolloutHold` names one of these gates:

- `Plan` waits for the rollout group to settle on a valid plan.
- `Pairing` keeps a compatible engine and decoder pair serving while the pairing protocol changes.
- `Ratio` keeps the ratio between the components within tolerance.
- `Sequential` waits for another component's turn or soak.
- `Budget` keeps to the surge and unavailability budgets.
- `RetryBlock` backs off before it retries a failed update.
- `Held` stops updates to a revision whose retries ran out, until a newer revision replaces it or you [release it](../../guides/roll-out-changes/release-a-held-revision.md).

### Per-Instance status

OME keeps a row for each Instance on the InferenceReplica only, in `status.instanceStatuses`, or in `status.instanceStatusColumns` in the compact `ColumnarV2` encoding. The chart turns that encoding on, and OME then uses it whenever it's smaller. [Where Instance status lives](../omenative/instances.md#where-instance-status-lives) explains the fields.

[kubectl ome instance](../../reference/kubectl-ome/instance.md) reads the rows in either encoding. For an InferenceService named `chat` in the namespace `prod`, with one ready engine Instance:

```bash
kubectl ome instance list chat -n prod
```

```output
COMP     IDX/INC   PHASE   PODS    REVS          AOF   EVIDENCE
engine   0/1       Ready   1/1/1   chat...ne-a   A--   OK
```

## MultiNode (deprecated) {#multinode}

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

A `MultiNode` component runs as a [LeaderWorkerSet](https://lws.sigs.k8s.io) named `lws-{isvc}-{component}`, with a leader pod and its worker pods per replica. It needs LeaderWorkerSet installed in the cluster, and gets no PodDisruptionBudget or autoscaler. Its Service `{isvc}-{component}` sends traffic to the leader pods of every LeaderWorkerSet in the namespace, so keep the component's LeaderWorkerSet the only one there.

!!! warning "Upgrading from v1.2.2"
    On v1.2.2, a `leader` or `worker` made an engine or decoder `MultiNode`. Since v1.3 it makes it OMENative, unless the component's `ome.io/deploymentMode` annotation, in the InferenceService or the runtime, names a mode. The annotation v1.2.2 added to the InferenceService's metadata doesn't count. So upgrading moves such a component to new OMENative pods and leaves its LeaderWorkerSet running.

    OMENative pods get no `LWS_*` variables or `leaderworkerset.sigs.k8s.io` labels, so a runtime that uses them, such as the catalog's `srt-deepseek-rdma`, breaks. Change it to use the [OME variables and labels](#pod-labels-and-environment), or, until you can, [pin the component to `MultiNode`](../../guides/omenative/move-from-leaderworkerset.md#pin-a-component-to-multinode) before you upgrade. A pinned LeaderWorkerSet's pods restart once after the upgrade.

## Other mode values

| Value | Where | What happens |
| --- | --- | --- |
| `VirtualDeployment` | The InferenceService's own `metadata.annotations` | Legacy. OME creates or updates no workload, Service or ingress, marks the InferenceService Ready with the reason `VirtualDeployment`, and sets its URL to `{isvc}.{namespace}.svc.{cluster domain}`. |
| `VirtualDeployment`, or `MultiNode` on the router | A component's annotation | The manager logs `Failed to reconcile component` and retries, with no event. It creates or updates no workload for that component or any after it: engine, then decoder, then router. |
| Any other value, such as v1.2.2's `MultiNodeRayVLLM` | A component's annotation | OME skips it, and the next rule in [How OME resolves the mode](#how-ome-resolves-the-mode) decides. |
| Anything but `RawDeployment` | `ome.controller.deploymentMode` in the `ome-resources` chart | The manager logs `Failed to initialize deployment configuration` and exits at startup. |

## Next steps

- [OMENative](../omenative/overview.md): what OMENative does for a component.
- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md): opt an InferenceService in and check its Instances.
- [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md): run an engine as a leader and workers.
- [OMENative update strategies](omenative-update-strategies.md): how OMENative replaces an Instance's pods when the spec changes.
- [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md): move `MultiNode` components off LeaderWorkerSet.
