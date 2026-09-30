---
title: Introduction
description: "OME is a Kubernetes operator for serving large language models: it manages models and runtimes as resources, matches them, and runs the workloads."
---

OME, the Open Model Engine, is a Kubernetes operator for serving large language models. You create an [InferenceService](../concepts/serving/inference-services.md) that names a model, a [serving runtime](../concepts/runtimes/serving-runtimes.md) or both. OME can put the weights on your nodes, or leave them to the runtime. It picks a runtime if you don't name one, and creates the workloads that serve the model.

Since v1.3, OME can run any component on [OMENative](../concepts/omenative/overview.md), its own workload engine. OMENative creates, updates, repairs and moves each replica as one unit, and can roll out prefill and decode together. Components that span several nodes use it by default. Two optional, alpha add-ons look after the GPU cluster underneath. The [OME scheduler](../concepts/scheduling/ome-scheduler.md) schedules a group of pods together, and [Alfred](../concepts/scheduling/alfred.md) watches the GPU nodes and recommends migrations.

## How OME serves a model

When OME manages the weights:

1. You create a [BaseModel or ClusterBaseModel](../concepts/models/base-models.md) that says where the weights live: Hugging Face, OCI Object Storage, the nodes' own disks or a PersistentVolumeClaim.
2. The optional [model agent](../guides/operate-ome/model-agent.md), a DaemonSet, downloads the weights to the nodes you choose, or finds them there. It labels the nodes where the model is ready, and the serving pods run only on those nodes. By default, OME also reads the model's architecture, size and capabilities from its files. A model on a PersistentVolumeClaim skips the model agent: its serving pods mount the claim.
3. You create an InferenceService that names the model. It can also name a serving runtime, the pod template for an engine such as SGLang or vLLM. If it doesn't, OME picks the runtime that best matches the model, among those that set `autoSelect: true`.
4. OME builds the serving pods from the runtime's template, with the InferenceService's own settings taking precedence, and creates the workloads that run them. The [deployment mode](#deployment-modes) decides the kind of workload.
5. By default, OME creates a Service named after the InferenceService, and you send requests to it. Turn on [ingress creation](../concepts/rollouts-and-traffic/ingress.md), and OME creates an Ingress or Gateway API routes instead.

Since v1.3, you can instead skip steps 1 and 2 and [name only a runtime](../concepts/serving/inference-services.md#the-runtime), which loads the weights itself.

You install OME with Helm. The `ome-crd` chart installs OME's resource definitions, and the `ome-resources` chart runs the manager, OME's control plane. Since v1.3, `ome-resources` runs the model agent only when you set `modelAgent.enabled=true`, as [Install OME](install.md) does. The optional `ome-serving` chart deploys ready-made models, runtimes and InferenceServices from [OME's catalog](pre-configured-models.md).

## Deployment modes

An InferenceService has up to three components: an engine that runs the model, an optional decoder for prefill-decode disaggregation, and an optional router in front of them. Each component runs in a [deployment mode](../concepts/architecture/deployment-modes.md):

| Mode | What runs the pods | When a component gets it |
| --- | --- | --- |
| OMENative | Since v1.3. OME itself, which manages the pods of a replica as one unit. | By default when it has a leader and workers, or when you set `deploymentMode: OMENative` in the InferenceService's spec. |
| RawDeployment | A Deployment. | By default for other components. |
| `MultiNode` (deprecated) | A LeaderWorkerSet. | When the component's `ome.io/deploymentMode` annotation asks for it. On v1.2.2, by default when it has a leader and workers. |

To move a component off `MultiNode`, see [Move from LeaderWorkerSet to OMENative](../guides/omenative/move-from-leaderworkerset.md).

## Other features

| Feature | What you get |
| --- | --- |
| [Accelerator classes](../concepts/runtimes/accelerator-classes.md) | An InferenceService can pick a GPU type from its runtime's list, by name or with a policy such as `Cheapest`, and OME runs the model on that type's nodes. |
| [Fine-tuned weights](../concepts/models/fine-tuned-weights.md) | Serve weights fine-tuned from a base model, such as a LoRA adapter. |
| [Autoscaling](../concepts/serving/component-autoscaling.md) | A component with no autoscaling settings gets a HorizontalPodAutoscaler that targets 80% average CPU utilization. |
| [Runtime revisions](../concepts/runtimes/runtime-revisions.md) | Pin an InferenceService to a snapshot of its runtime, and roll forward or back when you choose. |
| [Rollout groups](../concepts/rollouts-and-traffic/rollout-groups.md) | Since v1.3. Roll out OMENative components, such as prefill and decode, as one group by canary, blue-green or rolling update. A canary can promote or roll back on Prometheus metrics. |
| [Traffic policy](../concepts/rollouts-and-traffic/traffic-policy.md) | Since v1.3. Set how Envoy Gateway or Istio balances requests across an InferenceService's pods, for example by least request or by consistent hashing for session affinity. |
| [Accelerator quotas](../guides/operate-ome/accelerator-quota.md) | Since v1.3. Divide a cluster's GPUs among teams with a tree of AcceleratorQuotas. The quota manager, from the `ome-quota-manager` chart, turns the tree into Kueue cohorts and queues. |
| [`kubectl ome`](../reference/kubectl-ome/overview.md) | Since v1.3. A kubectl plugin. [`status`](../reference/kubectl-ome/status.md) shows why an InferenceService is or isn't ready, and [`runtime explain`](../reference/kubectl-ome/runtime.md) shows which runtimes match its model, and why. |

## Alpha features {since=v1.3}

These features are alpha: their API and behavior can change between releases. Each stays unused until you install it, turn it on or set its field.

| Feature | What it does | How to turn it on |
| --- | --- | --- |
| [OME scheduler](../concepts/scheduling/ome-scheduler.md) | A second scheduler that schedules the pods of a [PodGroup](../concepts/serving/gang-scheduling.md) together, and can pack them into one rack or other topology domain. | Install the `ome-scheduler` chart, which needs Kubernetes 1.35 and the scheduler-plugins PodGroup CRD. Set `schedulerName: ome-scheduler` on the runtime or the component. See [Use the OME scheduler](../guides/operate-ome/ome-scheduler.md). |
| [Alfred](../concepts/scheduling/alfred.md) | Watches for fragmented GPU capacity and unhealthy nodes, and recommends migrations that fix them. | Install the `ome-alfred` chart. It starts no migration while `alfredConfig.mode` is `recommend-only`, the default. See [Run Alfred in recommend-only mode](../guides/scheduling/run-alfred.md). |
| [Component autoscaler](../concepts/serving/component-autoscaling.md) | The `autoscaler` block picks and configures a component's autoscaler: `HPA`, `KEDA`, `External` (one you run) or `None`. | Set `autoscaler` on the component or on its runtime. The `KEDA` class needs KEDA installed. |
| [Autoscaler policies](../concepts/serving/autoscaler-policy.md) | Reusable HPA or KEDA templates that components reference with `autoscalerPolicyRef`. | Set `ome.autoscalerPolicy.enabled: true` in both the `ome-crd` and `ome-resources` charts. |
| [Rollout policies](../concepts/rollouts-and-traffic/rollout-policy.md) | Reusable canary, blue-green or rolling-update progressions that rollout groups reference with `policyRef`. | Set `ome.rolloutPolicy.enabled: true` in both the `ome-crd` and `ome-resources` charts. |
| [Multi-cluster placement](../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) and [routing](../concepts/rollouts-and-traffic/traffic-map.md) | Still in development. A control-plane cluster places InferenceServices that set `placement` onto WorkloadClusters, and runs none itself. Routing adds a TrafficMap that tells a gateway how to split their traffic. | On the control-plane cluster, set `ome.multicluster.enabled: true` and `ome.multicluster.role: control-plane` in the `ome-resources` chart. For routing, also set `ome.multicluster.config.routing.enabled: true`. |

The InferenceService fields `placement`, `routing` and `scalingPolicy` are alpha too, and so are the `kubectl ome` commands that change resources. See [Guarded actions](../reference/kubectl-ome/guarded-actions.md).

## Next steps

- [Install OME](install.md): install cert-manager and the OME charts, and check that OME is running.
- [Serve your first model](serve-your-first-model.md): serve a small model and send it a request.
- [How OME works](../concepts/architecture/how-ome-works.md): how the manager, the model agent and OME's resources turn a model and a runtime into serving pods.
- [OMENative overview](../concepts/omenative/overview.md): how OME runs each replica of a model as one Instance, and updates, repairs and migrates it.
