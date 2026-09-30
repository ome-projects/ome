---
title: Labels and annotations
description: OME reads and sets these labels and annotations on InferenceServices, models, runtimes, nodes and the resources it generates.
---

OME reads the labels and annotations that you put on your resources, and it sets others on the objects it creates. This page lists them by the resource they go on. The annotations that tune circuit breaking, retries and timeouts have their own page, [Traffic annotations](traffic-annotations.md).

The **Read or set by** column names the components that read or set each key. Unless a row says who sets a key, you set it. These components act on the keys:

- **The controller**: the OME controller manager, which reconciles your resources and generates their workloads.
- **The admission webhook**: the webhook that checks and defaults OME's resources when you create or update them.
- **The pod webhook**: the webhook that changes serving pods as they're created. See [Pods](#pod-annotations).
- **The model agent**: the DaemonSet that downloads models to each node. See [Run the model agent](../../guides/operate-ome/model-agent.md).
- **The metadata Job**: the Job that reads the metadata of a model stored on a PVC. See [Serve models from a PVC](../../guides/deploy-models/serve-models-from-pvc.md).
- **kubectl ome**: the [kubectl plugin](../kubectl-ome/overview.md). Its commands that set annotations are alpha.
- **Alfred**: OME's [GPU cluster caretaker](../../concepts/scheduling/alfred.md), which is alpha.
- **The OME scheduler**: a [second scheduler](../../concepts/scheduling/ome-scheduler.md) that places gangs of pods. It's alpha and needs Kubernetes 1.35.
- **ome-quota-manager**: the component that renders AcceleratorQuotas into Kueue objects. See [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md).

A row that ends with "Since v1.3." is for a key that v1.2.2 doesn't have. Label and annotation values are strings, so quote `"true"` and numbers in YAML.

Since v1.3, the admission webhook rejects an InferenceService annotation that starts with `ome.io/` when it isn't a key the webhook knows but is within two edits of one, and names the closest key. The webhook knows the circuit breaker, retry and timeout annotations, `ome.io/traffic-drain`, `ome.io/managed-by-conflict-acked`, `ome.io/rollout-ready-timeout`, `ome.io/revision-history-limit`, `ome.io/rollout-promote` and `ome.io/rollout-rollback`. It doesn't catch a misspelling of any other key, `ome.io/rollout-paused` included. See [Admission validation](traffic-annotations.md#admission-validation).

## Annotations

### InferenceService {#inferenceservice-annotations}

These go in an [InferenceService](../../concepts/serving/inference-services.md)'s `metadata.annotations`, unless a row says they go in a component's `annotations`, such as `spec.engine.annotations`.

OME copies the InferenceService's annotations, with each component's own `annotations`, to the objects it generates for the component, such as its pods and Services. The component's value wins. OME doesn't copy `ome.io/rollout-promote`, `ome.io/rollout-rollback`, `internal.ome.io/in-place-image-transition` or kubectl's `kubectl.kubernetes.io/last-applied-configuration`. Services also don't get the pod-only keys: the keys that start with `prometheus.io/`, `k8s.grafana.com/`, `loki.grafana.com/`, `networking.gke.io/`, `rdma.ome.io/` or `runtime.ome.io/`, and `ome.io/inject-model-init`, `ome.io/inject-fine-tuned-adapter` and `ome.io/inject-serving-sidecar`.

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

#### Deployment and runtime {#deployment-and-runtime}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/deploymentMode` | `RawDeployment`, `OMENative`, `MultiNode` ([deprecated](../../guides/omenative/move-from-leaderworkerset.md)) or `VirtualDeployment` | Read by the controller, the admission webhook, kubectl ome and Alfred | In a component's `annotations`, sets the component's [deployment mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode). It wins over the same annotation in the runtime's `engineConfig`, `decoderConfig` or `routerConfig`, over `spec.deploymentMode`, and over a `leader` or `worker` block, which since v1.3 makes an engine or decoder OMENative. OME skips a value it doesn't know. The engine and decoder run in `RawDeployment`, `OMENative` or `MultiNode`, and the router in `RawDeployment` or `OMENative`. Another mode, such as `VirtualDeployment`, fails the reconcile: OME doesn't create or update the serving workload for that component, or for the components after it in the order engine, decoder, router. When OME selects the runtime for you, it also skips runtimes that declare another mode for the component: see [Runtime deployment-mode matching](../matching/runtime-deployment-mode-matching.md). In the InferenceService's own `metadata.annotations`, the annotation sets no component's mode. There, `VirtualDeployment` makes OME skip the rest of the reconcile: it doesn't create or update any workload, Service or ingress, sets the URL to the service's cluster-local host, and marks the InferenceService Ready with the reason `VirtualDeployment`. Any other mode but `RawDeployment` there makes the webhook warn about each component that sets `deploymentStrategy`: see [For the whole InferenceService](../../concepts/architecture/deployment-modes.md#for-the-whole-inferenceservice). The `OMENative` value is new in v1.3. |
| `ome.io/accelerator-class` | The name of an [AcceleratorClass](../../concepts/runtimes/accelerator-classes.md) | Read by the controller | When OME selects the runtime for you, it pools this class with the classes in `spec.acceleratorSelector` and in the engine's and decoder's `acceleratorOverride`, and keeps only the runtimes whose `acceleratorRequirements.acceleratorClasses` list every one. It doesn't set any component's class. See [Runtime accelerator-class matching](../matching/runtime-accelerator-class-matching.md). |
| `ome.io/runtime-sync` | Any token | Read by the controller. Set by `kubectl ome runtime sync`, as `cli-runtime-sync-<UUID>`, or by you | Moves the InferenceService to its runtime's latest revision once, even with `spec.runtime.autoSync: false`. OME acts on a token that isn't empty and differs from `status.lastRuntimeSyncToken`, and waits while `spec.runtime.revision` is set. See [kubectl ome runtime sync](../../concepts/runtimes/runtime-revisions.md#kubectl-ome-runtime-sync). |

#### Legacy autoscaling {#legacy-autoscaling}

These keys configure the autoscaler of a `RawDeployment` component that has no `autoscaler` block, of its own or from its runtime, and no AutoscalerPolicy. A component's own annotation wins over the InferenceService's. OMENative components ignore them. See [Which setting wins](../../concepts/serving/component-autoscaling.md#which-setting-wins).

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/autoscalerClass` | `hpa`, `keda` or `external` | Read by the controller and the admission webhook | Picks the autoscaler: see [Autoscaler classes](#autoscaler-classes). The webhook rejects other values on the InferenceService. Since v1.3, it also rejects the annotation on the InferenceService while `spec.engine`, `spec.decoder` or `spec.router` has an `autoscaler` block, with a message that starts with `AutoscalerAnnotationConflict`. |
| `ome.io/targetUtilizationPercentage` | A whole number from `"1"` to `"100"` | Read by the controller and the admission webhook | The CPU utilization target for `hpa`. The default is 80. On its own, without `ome.io/autoscalerClass`, it selects `hpa`. The webhook rejects other values. |
| `ome.io/metrics` | `cpu` or `memory` | Read by the admission webhook | The webhook checks it only when `ome.io/autoscalerClass` is `hpa`, and rejects other values. OME never applies it: see [Scale metrics](#scale-metrics). |

#### Scheduling queues {#scheduling-queues}

These set queue labels on the pods of `RawDeployment` components and on the leader and worker pods of `MultiNode` ([deprecated](../../guides/omenative/move-from-leaderworkerset.md)) components. OME reads each pod's merged annotations, so the keys work on the InferenceService or on a component. OMENative pods don't get these labels: put the `kueue.x-k8s.io/queue-name` label on the InferenceService instead, as [InferenceService labels](#inferenceservice-labels) describes.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/volcano-queue` | A Volcano queue name | Read by the controller | Sets the pod label `volcano.sh/queue-name`. When it's set, OME ignores `ome.io/dedicated-ai-cluster`, even with `kueue-enabled`. |
| `ome.io/dedicated-ai-cluster` | A queue name | Read by the controller | Sets the pod label `volcano.sh/queue-name`. With `kueue-enabled`, it sets `kueue.x-k8s.io/queue-name` instead, and adds `kueue.x-k8s.io/priority-class: kueue-scheduling-high-priority`: see [Priority classes](#priority-classes). |
| `kueue-enabled` | Any value | Read by the controller | Its presence switches `ome.io/dedicated-ai-cluster` to the Kueue labels. The value doesn't matter, so `"false"` switches too. |

#### Ingress {#ingress-annotations}

These override the `ingress` settings of the `inferenceservice-config` ConfigMap for one InferenceService, and OME reads them on every reconcile. See [Per-service overrides](../../guides/networking/configure-ingress.md#per-service-overrides). Ingress creation is off by default. To keep an InferenceService inside the cluster whatever the ConfigMap says, set `ome.io/ingress-disable-creation` to `"true"` and add the `networking.knative.dev/visibility: cluster-local` label, which [InferenceService labels](#inferenceservice-labels) describes.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/ingress-disable-creation` | `"true"` or `"false"` | Read by the controller | Overrides `disableIngressCreation`. `"true"` turns ingress creation off, and any other value turns it on. |
| `ome.io/ingress-domain` | A domain | Read by the controller | Overrides `domain`. Ignored when empty. |
| `ome.io/ingress-domain-template` | A domain template | Read by the controller | Overrides `domainTemplate`. Ignored when empty. |
| `ome.io/ingress-url-scheme` | `http` or `https` | Read by the controller | Overrides `urlScheme`. Ignored when empty. |
| `ome.io/ingress-gateway` | A gateway, as `<namespace>/<name>` | Read by the controller | Overrides `omeIngressGateway`, and an empty value keeps it. Any value, even an empty one, takes the InferenceService out of `namespaceIngressGateways` for the primary gateway. See [Override the gateways for one service](../../guides/networking/multiple-gateways.md#override-the-gateways-for-one-service). Since v1.3. |
| `ome.io/ingress-additional-gateways` | A JSON array of objects with `omeIngressGateway`, `ingressDomain` and an optional `class` | Read by the controller | Overrides `additionalIngressGateways`. Any value takes the InferenceService out of `namespaceIngressGateways` for the additional gateways. OME ignores a value that isn't valid JSON, records no error, and uses `additionalIngressGateways`. Since v1.3. |
| `ome.io/ingress-per-isvc-subdomain` | `"true"` or `"false"` | Read by the controller | Overrides `perISVCSubdomain`. `"true"` turns it on, and any other value turns it off. See [Override the scheme for one service](../../guides/networking/gateway-host-schemes.md#override-the-scheme-for-one-service). Since v1.3. |
| `ome.io/ingress-shared-host-prefix` | A host prefix | Read by the controller | Overrides `sharedHostPrefix`. An empty value means the bare domain. Since v1.3. |
| `ome.io/ingress-consistent-hash-headers` | Header names, separated by commas | Read by the controller | Overrides `consistentHashHeaders`. Since v1.3. |
| `ome.io/ingress-path-template`, `ome.io/ingress-additional-domains` | A path template; domains | Read by the controller | OME reads them, but neither has an effect. |

#### Traffic {#traffic}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/circuit-breaker-*`, `ome.io/retry-*`, `ome.io/timeout-*`, the `ome.io/btp.` and `ome.io/dr.` prefixes, `ome.io/managed-by-conflict-acked`, `ome.io/service-type` and `ome.io/load-balancer-ip` | See the linked page | Read by the controller and the admission webhook | Circuit breaker limits, retries, connection timeouts and other fields of the backend policy that OME writes for the gateway, an Envoy Gateway BackendTrafficPolicy or an Istio DestinationRule, and the type and load balancer IP of the Services that OME creates. The webhook doesn't check `ome.io/service-type` or `ome.io/load-balancer-ip`, and OME doesn't act on `ome.io/managed-by-conflict-acked`. See [Traffic annotations](traffic-annotations.md). Since v1.3, except `ome.io/service-type` and `ome.io/load-balancer-ip`, which v1.2.2 has too. |

#### Rollout actions {#rollout-actions}

These act on the rollouts of OMENative components. The alpha commands of [kubectl ome rollout](../kubectl-ome/rollout.md) set them after checks of their own. An annotation you write by hand skips those checks.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/rollout-paused` | `"true"` or `freeze` | Read by the controller. Set by `kubectl ome rollout pause`, and removed by `kubectl ome rollout resume`, or by you | `"true"` stops OME from starting update, create or migration work: migrations stop advancing, a canary holds on its step, and OME doesn't act on a promote or a rollback. The restart policy still repairs Instances. `freeze` stops those repairs too. Only these exact values count, and the webhook doesn't check the value. `kubectl ome rollout pause` sets `"true"`, and never lowers `freeze`. See [What a pause holds](../../guides/roll-out-changes/pause-and-resume-a-rollout.md#what-a-pause-holds). Since v1.3. |
| `ome.io/rollout-promote` | The canary's `canaryRevisionHash`, from `status.canary` or `status.components.<component>.canary` | Read by the controller and the admission webhook. Set by `kubectl ome rollout promote`, or by you | Advances the canary past a manual gate, which is a pause step with no duration. It also forces the canary past an analysis step, where `kubectl ome rollout promote` needs `--override-analysis --yes`, and releases the `CanaryPreStep` hold that a repin can leave before a step with more traffic. The command refuses during that hold, so set the annotation yourself. It doesn't cut a timed pause short, and a hash that isn't the current canary's never advances anything. OME removes the annotation once the advance is saved, or when the rollout completes on its final step. The webhook requires lowercase letters and digits, at least 6 of them. See [Promote a manually gated step](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#step-2-promote-a-manually-gated-step). Since v1.3. |
| `ome.io/rollout-rollback` | `"true"` or `"false"` | Read by the controller and the admission webhook. Set by `kubectl ome rollout rollback`, or by you | `"true"` rolls the canary back to the stable revision and holds the rejected revision. `"false"` is the same as no annotation, and the webhook rejects other values. It stays until you remove it, and while it's `"true"`, OME rolls back any new revision too, so remove it before you apply a fix. While it's there, `kubectl ome rollout pause`, `promote`, `rollback` and `repin`, `kubectl ome scale`, `kubectl ome runtime sync` and `kubectl ome migration start` refuse to run. On a paused service, `kubectl ome rollout resume --discard-pending-actions --yes` removes it. See [Roll back the canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary). Since v1.3. |
| `ome.io/rollout-repin` | `rp1:<12 hex digits>` or `now` | Read by the controller. Set by `kubectl ome rollout repin`, or by you | Pins a drifted rollout plan to its current source without restarting the rollout. `rp1:` carries a digest of the current plan, and OME refuses the repin when the plan it renders has another digest. `now` skips that check, and kubectl ome never sends it. OME removes the annotation whenever it acts on it. See [Repin by annotation](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md#repin-by-annotation). Since v1.3. |

#### OMENative operations {#omenative-operations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/rollout-ready-timeout` | A duration, such as `"20m"` | Read by the controller and the admission webhook | How long OME waits for new Instances to leave `Pending`, and for an analysis to reach a conclusive result. It wins over the canary's `readyTimeout`, and over `defaultReadyTimeout` in the `rollout` settings of `inferenceservice-config`, which the ome-resources chart sets to 15m with `ome.controller.rollout.defaultReadyTimeout`. The webhook rejects a value that isn't a duration, and warns about one above zero but under a minute. OME ignores zero and negative values. See [readyTimeout](../rollouts/canary-progression.md#readytimeout). Since v1.3. |
| `ome.io/revision-history-limit` | A positive whole number | Read by the controller and the admission webhook | Caps the ControllerRevisions that each OMENative component keeps besides its live ones, which OME never deletes. It wins over the chart's `ome.controller.lifecycle.revisionHistoryLimit`, which is 10. With neither, OME doesn't prune. The webhook rejects other values. Since v1.3. |
| `ome.io/migration-request-v1-<request-id>` | JSON with `schemaVersion` `v1`, `component`, `instance`, `from_node`, `hint_target_nodes`, `reason`, `requested_at` and `requested_by` | Read by the admission webhook and the controller. Set by `kubectl ome migration start`, by Alfred, or by you | Asks OMENative to migrate one Instance. The webhook checks the request, and the controller acts on it and deletes the annotation. See [Migration requests](../../concepts/omenative/migration-and-transient-scale.md#migration-requests) and [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md). Since v1.3. |

#### Multi-cluster placement {#multi-cluster-placement}

Multi-cluster placement and routing are alpha and still in development. These keys go on the source InferenceService in the control-plane cluster, except the ones OME sets on the InferenceServices it derives for the workload clusters.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/accelerator-requirements`, `ome.io/cluster-selector` | Label selectors, such as `gpu=gb300` or `metadata.name=cluster-a` | Read by the controller and kubectl ome | Choose the WorkloadClusters for the service. When both are set, a cluster must match both. One of them alone is enough to place the service, and with neither, OME doesn't place it. OME reads them only when `spec.placement` isn't set, and strips them from derived copies. Alpha. Since v1.3. |
| `ome.io/local-queue` | A Kueue LocalQueue name | Read by the controller | Sets the `kueue.x-k8s.io/queue-name` label on the engine, decoder and router of the derived InferenceService. It wins over the queue that the operator configures, and with neither there's no label. Alpha. Since v1.3. |
| `ome.io/global-host` | A hostname | Read by the controller | The service's global hostname. It wins over `globalHostTemplate`. See [Choose the global host](../../guides/multi-cluster/publish-a-global-endpoint.md#choose-the-global-host). Alpha. Since v1.3. |
| `ome.io/traffic-drain` | JSON keyed by drain ID, such as `{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}` | Read by the controller and the admission webhook. Set by `kubectl ome traffic drain` and `kubectl ome traffic undrain`, or by you | The routing controller gives each drained cluster weight 0, and records the drain in `drainRefs`. Removing the last drain removes the annotation. The webhook checks the value, but the control-plane chart doesn't install that webhook: there, an invalid value fails every reconcile, and the last valid TrafficMap stays. OME strips the annotation from derived copies. See [How a drain is recorded](../../guides/multi-cluster/drain-a-workload-cluster.md#how-a-drain-is-recorded). Alpha. Since v1.3. |
| `ome.io/placement-origin-uid` | The source InferenceService's UID | Set by the controller. Read by the controller and kubectl ome | On derived InferenceServices. Alpha. Since v1.3. |
| `ome.io/placement-execution` | JSON | Set and read by the controller | On derived InferenceServices: the placement plan that the copy may carry out, with the plan's ID and revision, the source's and the cluster's UIDs, and `pauseSurge`, which stops new operations that need extra capacity. OME projects it into the InferenceReplica's `spec.placementExecution`. Alpha. Since v1.3. |
| `ome.io/rollout-plan-source` | `<group-index>=<policy>@<digest>` entries, separated by semicolons | Set and read by the controller | On derived InferenceServices: records which RolloutPolicy each group's plan came from, when placement inlines a policy into the derived service. OME doesn't copy a value from the source. Alpha. Since v1.3. |

#### Alfred {#alfred-annotations}

[Alfred](../../concepts/scheduling/alfred.md), which is alpha, reads these from an InferenceService's `metadata.annotations`. [Workload annotations](../scheduling/alfred-configuration.md#workload-annotations) has the details. The only key Alfred sets is `ome.io/migration-request-v1-<request-id>`.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `alfred.ome.io/movable` | `"true"` or `"false"` | Read by Alfred | With `"false"`, Alfred plans no move for the InferenceService. The default is Alfred's `defaultMovable` setting, which is `true`. Alfred ignores a value that isn't a boolean. Alpha. Since v1.3. |
| `alfred.ome.io/priority` | A number from `"0"` to `"1"` | Read by Alfred | The score of the InferenceService's evacuations for node health and maintenance. In execute mode, Alfred admits higher scores first among evacuations of the same kind. The default is 0.5. Alpha. Since v1.3. |
| `alfred.ome.io/cooldown-minutes` | A whole number, 0 or more | Read by Alfred | How many minutes after the InferenceService's last migration Alfred waits before it plans another move for it, in place of Alfred's `perWorkloadCooldownMinutes` setting. `0` means no cooldown. Moves off unhealthy nodes use `healthCooldownFloorMinutes` instead. In execute mode, one value in the cluster that isn't a whole number, or is negative, stops every migration request, with the reason `InvalidCooldown`. Alpha. Since v1.3. |
| `alfred.ome.io/spot-policy` | `avoid`, `migrate` or `ignore` | Read by Alfred | `avoid` keeps the InferenceService off spot nodes. `migrate` always boosts the score of its defragmentation moves off spot nodes, and `ignore` never does. Other values follow Alfred's `spotPolicy` setting. Alpha. Since v1.3. |
| `alfred.ome.io/tenant-group` | Any string | Read by Alfred | With Alfred's `allowCrossTenantOptimization` setting, which is on by default, a pending pod in another namespace can make a move of this InferenceService an emergency when the pod's InferenceService has the same group. Alpha. Since v1.3. |

### InferenceReplica {#inferencereplica-annotations}

OME creates one [InferenceReplica](ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) for each OMENative component, named `<isvc>-<component>`.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/controller-write` | `"true"` | Set by the controller. Read by the admission webhook | The webhook denies a create or update without it, except an update that changes only finalizers. OME's InferenceReplicas carry it, so an update that keeps it, such as `kubectl annotate`, passes. Since v1.3. |
| `ome.io/parent-generation` | A whole number | Set by the controller. Read by the controller and kubectl ome | The InferenceService generation that OME last applied. Since v1.3. |
| `ome.io/revision-excluded-annotation-keys` | Annotation keys, sorted and separated by commas | Set and read by the controller | The InferenceService annotations that the component doesn't declare in its own `annotations`, which OME leaves out of the revision hash. So changing an InferenceService annotation doesn't change the component's revision, and changing the component's does. Since v1.3. |
| `ome.io/release-held-revision` | A held revision's name, such as `chat-engine-7f9c4d2b` | Read by the controller. Set by `kubectl ome instance release-held`, or by you | Releases the Held retry block of a revision that the controller has stopped retrying. When a Held block matches, the controller removes it, records a `RetryBlockReleased` event and deletes the annotation. When none matches, it records a `RetryBlockReleaseSkipped` event and deletes the annotation. See [Release by annotation](../../guides/roll-out-changes/release-a-held-revision.md#release-by-annotation). Since v1.3. |
| `ome.io/reset-instances` | `all`, or Instance indexes separated by commas; case-sensitive | Read by the controller | Rebuilds Failed Instances by deleting their pods, and leaves the others alone. It skips a failed Instance that has a serving pod, or that failed during an update or a migration. The controller acts once, records an event and deletes the annotation. For an invalid value, it resets nothing, deletes the annotation and records an `InstancesResetRejected` warning event. No kubectl ome command sets it. See [Request the reset](../../guides/omenative/reset-failed-instances.md#step-2-request-the-reset). Since v1.3. |

### ServingRuntime and ClusterServingRuntime {#runtime-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/engine` | `sglang-pd` | Read by the admission webhook | Fills in an SGLang prefill-decode preset where the runtime leaves fields unset: an engine runner in prefill mode with bootstrap port 8998, a decoder runner in decode mode, both on `lmsysorg/sglang:latest` with port 30000, and a router on `lmsysorg/sglang-router:latest` with port 30080. The runtime's own fields win. The webhook denies other values. Since v1.3. |
| `ome.io/runtime-profile` | `"true"` | Read by the admission webhook | Marks the runtime as a profile for other runtimes to inherit from. The webhook rejects a profile without `disabled: true`. See [Define a profile and inherit from it](../../concepts/runtimes/runtime-inheritance.md#define-a-profile-and-inherit-from-it). Since v1.3. |
| `ome.io/inherit-from` | The parent runtime's name | Read by the admission webhook and the controller | The runtime inherits the parent's spec only, and the child's own fields win. See [What admission checks](../../concepts/runtimes/runtime-inheritance.md#what-admission-checks). Since v1.3. |
| `ome.io/deploymentMode` | As for the [InferenceService](#deployment-and-runtime) | Read by the controller | In the `annotations` of `engineConfig`, `decoderConfig` or `routerConfig`, sets the component's mode for the InferenceServices that use the runtime, unless the InferenceService's component sets its own. |

### BaseModel and ClusterBaseModel {#basemodel-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.oracle.com/skip-config-parsing` | `"true"`, in any letter case | Read by the model agent | Stops the agent from reading the model's files, so the model keeps only the metadata you set in its spec. OME ignores it for a model on a PVC. See [What OME learns from the model](../../concepts/models/base-models.md#what-ome-learns-from-the-model). |
| `models.ome.io/category` | Any string, such as `LARGE` | Read by the controller | Sets the pod label `base-model-size`. The default is `SMALL`. |

### Pods {#pod-annotations}

The pod webhook changes only pods with the `ome.io/inferenceservice` label, in namespaces without a `control-plane` label. When an injection fails, the webhook denies the pod, and for a Deployment the ReplicaSet records a `FailedCreate` event.

#### Keys you set {#pod-keys-you-set}

Set these on the InferenceService, on a component, or in the runtime's component config. OME copies them to the pods.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/inject-model-init` | `"true"`, exactly | Read by the pod webhook | Adds the `model-init` init container, which decrypts the model with OCI Vault, as the `modelInit` key of `inferenceservice-config` configures it. The container gets `ome.io/base-model-name`, `ome.io/base-model-format`, the three decryption keys below, and the pod label `base-model-type`, or `Serving` without it. For a TensorRT-LLM model it also gets the format version and the GPU count, and the webhook denies a pod whose `ome-container` has no `nvidia.com/gpu` limit. |
| `ome.io/base-model-decryption-key-name`, `ome.io/base-model-decryption-secret-name` | A key name; a Secret name | Read by the pod webhook | Passed to `model-init`. |
| `ome.io/disable-model-decryption` | `"true"` or `"false"` | Read by the pod webhook | Passed to `model-init`. The default is `"false"`. |
| `ome.io/inject-serving-sidecar` | `"true"`, exactly | Read by the pod webhook | Adds the sidecar that the `servingSidecar` key of `inferenceservice-config` configures. The sidecar gets `ome.io/fine-tuned-weight-ft-strategy`. |
| `ome.io/enable-metric-aggregation` | `"true"` or `"false"` | Read and set by the pod webhook | When it's absent, the webhook sets it from `enableMetricAggregation` in the `metricsAggregator` key of `inferenceservice-config`, which the chart sets to `"false"`. `"true"` configures a container named `queue-proxy`, when the pod has one, to aggregate metrics from the port and path in `prometheus.ome.io/port` and `prometheus.ome.io/path`, and exposes them on port 9088. OME never creates a `queue-proxy` container. Since v1.3. |
| `ome.io/enable-prometheus-scraping` | `"true"` or `"false"` | Read and set by the pod webhook | When it's absent, the webhook sets it from `enablePrometheusScraping` in the same key, which the chart sets to `"false"`. `"true"` sets `prometheus.io/port` to 9091, or 9088 with aggregation, and `prometheus.io/path` to `/metrics`, overwriting any value. It doesn't set `prometheus.io/scrape`. Since v1.3. |
| `prometheus.ome.io/port`, `prometheus.ome.io/path` | A port; a path | Read by the pod webhook | The port and path that metric aggregation reads from. The defaults are 8080 and `/metrics`. Only aggregation uses them. Since v1.3. |
| `prometheus.ome.io/extra-endpoints` | `<port-name>:<path>` entries, separated by commas | Read by the controller | Adds endpoints, scraped every 10 seconds, to the PodMonitor of a `RawDeployment` or `MultiNode` ([deprecated](../../guides/omenative/move-from-leaderworkerset.md)) component. OME skips malformed entries. See [The PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd). Since v1.3. |
| `rdma.ome.io/auto-inject` | `"true"`, exactly | Read by the pod webhook | Adds the RDMA settings of `rdma.ome.io/profile` to the container. |
| `rdma.ome.io/profile` | `oci-roce`, `cks-gb-sglang` or `cks-gb-rdma` | Read by the pod webhook | `oci-roce`, the default, adds NCCL environment variables, `/dev/shm`, `/dev/infiniband` and the `IPC_LOCK` capability. `cks-gb-sglang` adds the same mounts and capability, with its own environment variables for SGLang prefill-decode over IPv6. `cks-gb-rdma` adds only 16 Multus attachments, `ibs<hca>p<port>-macvlan` in `cw-multus`, which it merges into `k8s.v1.cni.cncf.io/networks`. Since v1.3, no profile makes the container privileged, because a privileged container sees every GPU on the node. So the container can open the RDMA devices only when you request your cluster's RDMA device-plugin resource, such as `rdma/hca_shared_devices_a: 1`, or set `privileged: true` on it yourself. On v1.2.2, `oci-roce` also adds `CAP_SYS_ADMIN`, and makes the container privileged unless the container sets `privileged` itself. The webhook denies a pod with an unknown profile or a malformed networks annotation. `cks-gb-sglang` and `cks-gb-rdma` are new in v1.3. |
| `rdma.ome.io/container-name` | A container name | Read by the pod webhook | The container that gets the RDMA settings. The default is `ome-container`, and the webhook skips a container that doesn't exist. |
| `runtime.ome.io/container-name` | A container name | Read by the pod webhook | The container that the `runtime.ome.io/` profiles change. The default is `ome-container`. Since v1.3. |
| `runtime.ome.io/shm-profile` | `default` | Read by the pod webhook | Mounts a memory-backed emptyDir at `/dev/shm`, with no size limit. Since v1.3. |
| `runtime.ome.io/probe-profile` | `sglang-http` or `vllm-http` | Read by the pod webhook | Adds whichever readiness, liveness and startup probes the container lacks, as HTTP GET probes of `/health` on `runtime.ome.io/probe-port`. Since v1.3. |
| `runtime.ome.io/probe-port` | A port | Read by the pod webhook | The probes' port. The default is 30000. Since v1.3. |
| `runtime.ome.io/observability-profile` | `prometheus` | Read by the pod webhook | Sets `prometheus.io/scrape` to `"true"`, `prometheus.io/port` to `runtime.ome.io/observability-port`, and `prometheus.io/path` to `/metrics`, where the pod doesn't set them. Since v1.3. |
| `runtime.ome.io/observability-port` | A port | Read by the pod webhook | The metrics port. The default is 30000. Since v1.3. |

The webhook denies a pod with an unknown `runtime.ome.io/` profile or an invalid port.

#### Keys OME sets {#pod-keys-ome-sets}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/inject-fine-tuned-adapter` | The first FineTunedWeight's name | Set by the controller. Read by the pod webhook | The webhook adds an init container that downloads the adapter from OCI Object Storage. See [What the pods get](../../concepts/models/fine-tuned-weights.md#what-the-pods-get). |
| `ome.io/fine-tuned-weight-ft-strategy` | The FineTunedWeight's `hyperParameters.strategy` | Set by the controller. Read by the controller and the pod webhook | Passed to the serving sidecar. When the strategy is missing or isn't a string, the reconcile fails, and OME doesn't create or update the serving workload. |
| `ome.io/fine-tuned-serving-with-merged-weights` | `"true"` | Set by the controller. Read by the controller and the pod webhook | Set when the FineTunedWeight's configuration has `merged_weights: true`. The controller then doesn't mount the base model's files, and the pod webhook's init container downloads the merged weights as one archive. |
| `ome.io/base-model-name` | The model's name | Set by the controller. Read by the pod webhook | Passed to `model-init`. |
| `ome.io/base-model-vendor` | The model's vendor | Set and read by the controller | Set when the model has a vendor. |
| `ome.io/base-model-format` | The model's format | Set by the controller. Read by the controller and the pod webhook | The controller uses it for the model's mount paths, and the webhook passes it to `model-init`. |
| `ome.io/base-model-format-version` | The format's version | Set by the controller. Read by the pod webhook | Set when the format has a version. |
| `ome.io/serving-runtime` | The runtime's name | Set by the controller | OME doesn't read it. |
| `prometheus.io/scrape`, `prometheus.io/port`, `prometheus.io/path` | `"true"`; a port; a path | Set by the pod webhook, or copied from the runtime or the InferenceService | Read by a Prometheus that finds pods by annotation, such as the `ome-inferenceservice-pods` job of OME's chart: see [InferenceService pods](../../guides/operate-ome/metrics.md#inferenceservice-pods). OME removes them from the worker pods of `MultiNode` ([deprecated](../../guides/omenative/move-from-leaderworkerset.md)) components. |
| `internal.ome.io/in-place-image-transition` | JSON, such as `{"targetImages":{...}}` | Set and read by the controller | OME sets it on an OMENative pod during an in-place image update, and removes it when the update ends. OME strips it from new pods, and doesn't copy it from the InferenceService. Since v1.3. |

### Deployments, HPAs and ScaledObjects {#autoscaler-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/autoscaler-propagated-metadata-keys` | The copied label keys, then the copied annotation keys, as two lists separated by commas and joined by a vertical bar | Set and read by the controller | On HPAs and ScaledObjects: records the keys that OME copied from the component, so that it can remove the ones the component drops and leave keys that other controllers set alone. Since v1.3. |
| `ome.io/last-rendered-autoscaler` | The rendered autoscaler, as JSON | Set and read by the controller | On the Deployment of a `RawDeployment` component that uses an AutoscalerPolicy, which is alpha: records each autoscaler OME renders. While the component holds because OME can't use its policy, OME applies the recorded autoscaler again. Without a record, it leaves any existing autoscaler alone and creates none. See [Fail-closed behavior](../../concepts/serving/autoscaler-policy.md#fail-closed-behavior). Since v1.3. |

### ControllerRevisions {#controllerrevision-annotations}

OME stores [runtime revisions](../../concepts/runtimes/runtime-revisions.md) as ControllerRevisions in its own namespace.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/created-by` | `ome-controller` | Set by the controller. Read by the admission webhook, the controller and kubectl ome | On runtime revisions. The webhook blocks changes only to revisions that carry it, and garbage collection handles only those. |
| `ome.io/gc-eligible-since` | An RFC 3339 time | Set and read by the controller | On a runtime revision that OME no longer keeps: no InferenceService uses it, and it isn't among the newest revisions with its runtime's name, ten by default. OME deletes the revision once the grace period, 24h by default, has passed, and removes the annotation if it keeps the revision again before that. See [Garbage collection](../../concepts/runtimes/runtime-revisions.md#garbage-collection). |
| `ome.io/pairing-protocol` | A token, such as `nixl-v2`, of at most 63 characters | Set and read by the controller | On the revisions of OMENative engines and decoders, from `spec.rollout.pairingProtocol`. An engine revision and a decoder revision pair only when their tokens are equal, and an empty token pairs with any. Since v1.3. |

### AutoscalerPolicy and RolloutPolicy {#policy-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/allow-in-use-delete` | `"true"` | Read by the admission webhook | Lets you delete a policy that components still reference. Both kinds are alpha and off by default. See [Delete an AutoscalerPolicy](../../concepts/serving/autoscaler-policy.md#delete-a-policy) and [Delete a RolloutPolicy](../../concepts/rollouts-and-traffic/rollout-policy.md#delete-a-policy). Since v1.3. |

### PodGroups {#podgroup-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/topology-key` | A node label key | Set by the controller. Read by the OME scheduler and Alfred | On the PodGroup of a multi-pod OMENative Instance, copied from the component's `topologyKey` when that's set. OME creates PodGroups only when the PodGroup CRD is installed. The OME scheduler, which is alpha and needs Kubernetes 1.35, reads the annotation that its `podGroupTopologyKeyAnnotation` argument names, which the chart keeps at `ome.io/topology-key`. The annotation wins over the scheduler's `topologyKey` argument, and with neither, the gang's pods stay `Pending`. See [Labels and annotations](../scheduling/ome-scheduler-configuration.md#labels-and-annotations) in the scheduler's reference. Since v1.3. |

### AcceleratorQuota copies {#acceleratorquota-copy-annotations}

For multi-cluster serving, which is alpha, ome-quota-manager copies [AcceleratorQuotas](ome.v1beta1.md#ome-io-v1beta1-AcceleratorQuota) from the control-plane cluster to the workload clusters.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/quota-origin-uid` | The source AcceleratorQuota's UID | Set by ome-quota-manager | Nothing reads it. Alpha. Since v1.3. |
| `ome.io/quota-source-generation` | The source's generation | Set and read by ome-quota-manager | The copy reports it in `status.sourceGeneration`. Alpha. Since v1.3. |
| `ome.io/quota-cluster` | The WorkloadCluster's name | Set by ome-quota-manager | Nothing reads it. Alpha. Since v1.3. |

### ConfigMaps {#configmap-annotations}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `models.ome.io/node-name`, `models.ome.io/managed-by` | The node's name; `model-agent` | Set by the model agent | On the model agent's ConfigMap for each node, in the agent's namespace, `ome` by default. OME doesn't read them. |
| `models.ome/pvc-metadata-last-error` | An error message | Set by the metadata Job. Read by the controller | On the ConfigMap of a model on a PVC: the last error from reading the model's metadata. See [The metadata Job fails](../../guides/deploy-models/serve-models-from-pvc.md#the-metadata-job-fails). |

## Labels

### InferenceService {#inferenceservice-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `networking.knative.dev/visibility` | `cluster-local` | Read by the controller | OME creates no external Service for the InferenceService, and deletes an existing one. It still creates an Ingress or HTTPRoutes when ingress creation is on. The HTTPRoutes get the InferenceService's other labels, but not this one. See [Cluster-local services](../../concepts/rollouts-and-traffic/ingress.md#cluster-local-services). |
| `kueue.x-k8s.io/queue-name` | A Kueue LocalQueue name | Read by the controller | OME copies it to the pods. For OMENative, OME leaves it off the pods of a component that requests none of the resources in `ome.controller.quotaAcceleratorResources`, which are `nvidia.com/gpu` and `google.com/tpu` by default. An empty list keeps it on every component. Changing the label doesn't change a component's revision. |

### Pods {#pod-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/inferenceservice` | The InferenceService's name | Set and read by the controller. Read by the pod webhook | Marks the pods that the pod webhook changes. |
| `component` | `engine`, `decoder` or `router` | Set by the controller. Read by the controller, kubectl ome and Alfred | The pod's component. |
| `serving-runtime` | The runtime's name | Set by the controller | OME doesn't read it. |
| `fine-tuned-serving` | `"true"` or `"false"` | Set by the controller | Whether the pod serves a FineTunedWeight. OME doesn't read it. |
| `base-model-name`, `base-model-size`, `base-model-type` | The model's name; `models.ome.io/category`, or `SMALL`; `Serving` | Set by the controller. The pod webhook reads `base-model-type` | Set when the InferenceService has a base model. The pod webhook passes `base-model-type` to `model-init`. OME doesn't read the other two. |
| `base-model-vendor`, `fine-tuned-weight-ft-strategy`, `fine-tuned-serving-with-merged-weights` | The model's vendor; the FineTunedWeight's strategy; `"true"` or `"false"` | Set by the controller | `base-model-vendor` is set when the model has a vendor, and the other two on pods that serve a FineTunedWeight. OME doesn't read them. The [pod annotations](#pod-keys-ome-sets) of the same names, with the `ome.io/` prefix, carry the same information. |
| `app` | The component's workload name, such as `<isvc>-engine`, cut to 63 characters | Set and read by the controller | On the pods of `RawDeployment` components and the leader pods of `MultiNode` ([deprecated](../../guides/omenative/move-from-leaderworkerset.md)) components. The component's Service, PodDisruptionBudget and PodMonitor select pods by it. |
| `volcano.sh/queue-name`, `kueue.x-k8s.io/queue-name`, `kueue.x-k8s.io/priority-class` | A queue name; a priority class name | Set by the controller | From the annotations in [Scheduling queues](#scheduling-queues), or, for `kueue.x-k8s.io/queue-name`, from the InferenceService's label. |
| `ome.io/placement-group` | Any value | Read by the OME scheduler | On a pod or its PodGroup: asks for the gang to be placed together with other gangs, which the OME scheduler doesn't support, so it keeps the gang's pods `Pending` with the reason `partner placement groups are not supported`. OME never sets it. The label's name is the scheduler's `unsupportedPlacementGroupLabel` argument. See [Labels and annotations](../scheduling/ome-scheduler-configuration.md#labels-and-annotations) in the scheduler's reference. Alpha. Since v1.3. |

#### OMENative pods {#omenative-pod-labels}

OME sets these on the pods of OMENative components. See [Pod labels and environment](../../concepts/architecture/deployment-modes.md#pod-labels-and-environment).

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/managed-by` | `OMENative` | Set and read by the controller | Marks the pods that OMENative manages. Since v1.3. |
| `ome.io/instance-index` | A whole number | Set and read by the controller | The [index](../../concepts/omenative/instances.md#index) of the pod's Instance. Since v1.3. |
| `ome.io/instance-incarnation` | A whole number | Set and read by the controller | The Instance's [incarnation](../../concepts/omenative/instances.md#incarnation). Since v1.3. |
| `ome.io/runner` | `default`, `leader` or `worker` | Set and read by the controller | The pod's [runner](../../concepts/omenative/instances.md#runners). Since v1.3. |
| `ome.io/pod-ordinal` | A whole number | Set and read by the controller | The pod's place in its Instance: from 0 to the Instance's size minus 1 in a multi-pod Instance. A single-pod Instance's pod is `0` or `1`, alternating each time OME replaces the pod by surging before it drains. Since v1.3. |
| `ome.io/revision-hash` | A revision hash | Set and read by the controller | The hash of the pod's revision. Since v1.3. |
| `ome.io/pairing-protocol` | The component's `spec.rollout.pairingProtocol` | Set by the controller | On engine and decoder pods, and on their per-revision routing Services, for information. Since v1.3. |
| `scheduling.x-k8s.io/pod-group` | The PodGroup's name | Set by the controller. Read by the OME scheduler | On the pods of a multi-pod Instance only. See [One PodGroup per multi-pod Instance](../../concepts/serving/gang-scheduling.md#one-podgroup-per-multi-pod-instance). Since v1.3. |

### Nodes {#node-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `models.ome.io/clusterbasemodel.<name>`, `models.ome.io/<namespace>.basemodel.<name>` | `Updating`, `Ready` or `Failed` | Set by the model agent. Read by the controller | A model's state on the node, for a ClusterBaseModel and a BaseModel. The controller gives serving pods a `<key>: Ready` node selector. OME shortens the key with a hash when a ClusterBaseModel's name is longer than 32 characters, or a BaseModel's namespace and name together are longer than 38. See [Node labels and status](../../concepts/models/base-models.md#node-labels-and-status) and [Check where a model is ready](../../guides/operate-ome/model-agent.md#step-3-check-where-a-model-is-ready). |
| `node.kubernetes.io/instance-type`, `beta.kubernetes.io/instance-type` | The node's instance type | Read by the model agent | The agent reads the first, or the second when the first is missing, to match TensorRT-LLM models to the node. See [TensorRT-LLM models](../../guides/operate-ome/model-agent.md#tensorrt-llm-models). |

### BaseModel and ClusterBaseModel {#basemodel-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `models.ome/reserve-model-artifact` | `"true"`, in any letter case | Read by the model agent | Keeps the model's files on the nodes when you delete the model. The label must be on the model when the agent processes the deletion. See [Keep a model's files](../../guides/operate-ome/configure-model-artifact-retention.md#step-1-keep-a-models-files-with-modelsomereserve-model-artifact). |

### ControllerRevisions {#controllerrevision-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/runtime-of` | The runtime's name | Set and read by the controller. Read by kubectl ome | On runtime revisions. |
| `ome.io/runtime-of-kind` | `ServingRuntime` or `ClusterServingRuntime` | Set by the controller. Read by kubectl ome | On runtime revisions. |
| `ome.io/runtime-of-namespace` | The ServingRuntime's namespace, or empty for a ClusterServingRuntime | Set by the controller. Read by kubectl ome | On runtime revisions. |
| `ome.io/revision-hash` | The short hash of the resolved runtime spec | Set and read by the controller. Read by kubectl ome | On runtime revisions, and part of the revision's name. OME reuses a revision with the same `ome.io/runtime-of` and `ome.io/revision-hash`. |
| `ome.io/inferenceservice`, `component`, `ome.io/managed-by` | The InferenceService's name; the component; `OMENative` | Set and read by the controller | On the revisions of OMENative components. Since v1.3. |

### Derived InferenceServices {#derived-inferenceservice-labels}

Multi-cluster placement is alpha and still in development.

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/placement-origin` | The source InferenceService's UID | Set by the controller. Read by the controller and kubectl ome | Ties the derived InferenceService to its source. Alpha. Since v1.3. |
| `ome.io/placement-control-plane` | The control plane's ID | Set and read by the controller | Set only when the controller has `--placement-control-plane-id`, which the chart sets from `ome.multicluster.placementControlPlaneID`. Garbage collection then deletes only the derived InferenceServices of its own control plane. Alpha. Since v1.3. |

### WorkloadClusters {#workloadcluster-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/autoscaler-policy`, `ome.io/rollout-policy` | `v1beta1` | Read by the controller | Mark a workload cluster as able to resolve AutoscalerPolicies or RolloutPolicies. For a service that uses a policy, placement skips the clusters without the matching label, with the reason `CapabilityMissing`. Alpha. Since v1.3. |

### Endpoint publisher resources {#endpoint-publisher-labels}

The `gatewayapi` endpoint publisher runs in the controller on the control-plane cluster, and labels the HTTPRoute, Services and other objects it writes for a global endpoint. See [How publishing works](../../guides/multi-cluster/publish-a-global-endpoint.md#how-publishing-works).

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/managed-by` | `placement-endpoint` | Set and read by the controller | With the two labels below, marks the objects that the publisher owns. Alpha. Since v1.3. |
| `ome.io/placement-endpoint-isvc`, `ome.io/placement-endpoint-isvc-namespace` | The source InferenceService's name; its namespace | Set and read by the controller | Name the InferenceService that each object belongs to. The publisher checks them, with `ome.io/managed-by`, before it changes an existing object. Alpha. Since v1.3. |
| `ome.io/placement-cluster` | The workload cluster's name | Set by the controller | On the objects for one workload cluster. OME doesn't read it. Alpha. Since v1.3. |

### Kueue objects {#kueue-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/quota-managed-by` | The quota manager's field manager, `ome-quota-manager` by default | Set and read by ome-quota-manager | On the Kueue Cohorts, ClusterQueues and LocalQueues that ome-quota-manager renders from AcceleratorQuotas in workload mode. See [How quotas work](../../guides/operate-ome/accelerator-quota.md#how-quotas-work). Since v1.3. |
| `ome.io/accelerator-quota` | The AcceleratorQuota's name | Set and read by ome-quota-manager | The quota node that the object comes from. Since v1.3. |

### AcceleratorQuota copies {#acceleratorquota-copy-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `ome.io/quota-origin` | The control plane's configured identity | Set and read by ome-quota-manager | Tells a copy from an AcceleratorQuota you wrote in the workload cluster. Alpha. Since v1.3. |

### Model ConfigMaps, Jobs and RBAC {#model-configmap-labels}

| Key | Values | Read or set by | Description |
| --- | --- | --- | --- |
| `models.ome/basemodel-status` | `"true"` | Set by the model agent. Read by the controller | On the model agent's ConfigMap for each node, which records the state of each model on the node. |
| `node` | The node's name | Set by the model agent | On the same ConfigMap. OME doesn't read it. |
| `models.ome/pvc-status` | `"true"` | Set by the metadata Job. Read by the controller | On the ConfigMap in OME's namespace that holds a PVC model's metadata. The controller watches the ConfigMaps with this label, and updates the model when the metadata Job writes one. |
| `models.ome/model-name`, `models.ome/model-scope` | The model's name; `namespaced` or `cluster` | Set by the metadata Job on its ConfigMap, and by the controller on the metadata Job. The controller reads them on the Job | Tie the ConfigMap and the Job to their model. The controller finds a model's metadata Jobs by them, to delete the Jobs when you delete the model or move it off the PVC. |
| `app.kubernetes.io/component`, `models.ome/metadata-source-namespace` | `ome-model-metadata-job`; the Job's namespace | Set by the controller | When the metadata Job runs outside OME's namespace, OME creates a ServiceAccount for it in the Job's namespace and a RoleBinding in OME's namespace, so that the Job can write its ConfigMap. Both get `app.kubernetes.io/component`, and the RoleBinding also gets `models.ome/metadata-source-namespace`. OME doesn't read them: they let you list these objects. |

## Special values

### Autoscaler classes {#autoscaler-classes}

The values of `ome.io/autoscalerClass`:

| Class | What OME does |
| --- | --- |
| `hpa` | Creates an HPA that scales on CPU utilization, at `ome.io/targetUtilizationPercentage`, or 80%. |
| `keda` | Doesn't work since v1.3. OME resolves it to a KEDA autoscaler with no triggers. It creates or updates the Deployment, creates no ScaledObject, and fails the reconcile with an error that contains `KEDA autoscaler requires at least one trigger`, so the component's status isn't updated. On v1.2.2, OME built a Prometheus trigger from `spec.kedaConfig` or the `autoscaling.keda.sh/` annotations, which v1.3 removes: see [Removed in v1.3](ome.v1beta1.md#removed-in-v1-3). Use an `autoscaler` block with `class: KEDA` instead: see [KEDA](../../concepts/serving/component-autoscaling.md#keda). |
| `external` | Creates no autoscaler, so another controller can scale the Deployment. See [External and None](../../concepts/serving/component-autoscaling.md#external-and-none). |

An `autoscaler` block takes other class names: `HPA`, `KEDA`, `External` and `None`.

### Scale metrics {#scale-metrics}

The admission webhook accepts `cpu` and `memory` in `ome.io/metrics` only when `ome.io/autoscalerClass` is `hpa`, and OME never applies either. The HPA that `hpa` creates scales on CPU. To scale on memory or other metrics, set `hpa.metrics` in an `autoscaler` block: see [HPA](../../concepts/serving/component-autoscaling.md#hpa).

### Priority classes {#priority-classes}

When `ome.io/dedicated-ai-cluster` and `kueue-enabled` switch a pod to Kueue, OME sets the pod label `kueue.x-k8s.io/priority-class: kueue-scheduling-high-priority`, but it doesn't create that priority class. Create a Kueue WorkloadPriorityClass with that name. OME sets no Volcano priority class.

## Related pages

- [Traffic annotations](traffic-annotations.md): the circuit breaker, retry, timeout and Service type annotations.
- [Configure ingress](../../guides/networking/configure-ingress.md): the cluster settings that the ingress annotations override.
- [Run the model agent](../../guides/operate-ome/model-agent.md): the agent that labels the nodes where a model is ready.
