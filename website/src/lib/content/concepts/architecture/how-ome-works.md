---
title: How OME works
description: "How the OME manager, the model agent and OME's custom resources turn a model and a runtime into serving pods."
---

OME serves large language models on Kubernetes. You describe what to serve and how in OME's custom resources. OME can bring the weights to your nodes or leave them to your runtime. It picks a runtime when you don't name one, and creates and manages the pods that serve the model. Since v1.3, OME can run any component on [OMENative](../omenative/overview.md), its own workload engine, which manages each replica as one unit. Components that span several nodes use it by default.

## Components

The manager is OME's core, and OMENative runs inside it. Two optional, alpha components work with OMENative: the [OME scheduler](../scheduling/ome-scheduler.md) places its gangs of pods, and [Alfred](../scheduling/alfred.md) recommends when to migrate its Instances.

| Component | What it does | Where it runs |
| --- | --- | --- |
| [Manager](../../guides/operate-ome/configure-the-controller.md) | OME's control plane. Its controllers turn OME's resources into serving workloads, and its webhooks check the resources and add containers and settings to serving pods. | The Deployment `ome-controller-manager`, with three replicas by default, from the `ome-resources` chart |
| [OMENative](../omenative/overview.md) | Since v1.3. OME's workload engine. It creates a component's pods itself and manages each replica as one Instance. Multi-node components use it by default, and any component can opt in. | Inside the manager, on by default |
| [Model agent](../../guides/operate-ome/model-agent.md) | Downloads the models that select its node, and labels the node when a model is ready there. | The DaemonSet `ome-model-agent-daemonset`, from the `ome-resources` chart when you turn it on |
| ome-agent | Downloads fine-tuned weights into the pods that serve them, and reads the config files of `pvc://` models. | An init container in serving pods, and Jobs |
| [OME scheduler](../scheduling/ome-scheduler.md) | Since v1.3. Alpha. A second scheduler that places each gang of pods together in one accelerator domain. Pods opt in with `schedulerName`. It runs only on Kubernetes 1.35. | A Deployment, `ome-scheduler` by default, from the `ome-scheduler` chart |
| [Alfred](../scheduling/alfred.md) | Since v1.3. Alpha. The GPU cluster caretaker. It watches for fragmented GPU capacity and unhealthy nodes and, by default, only recommends Instance migrations. | The Deployment `ome-alfred`, from the `ome-alfred` chart |
| [Quota manager](../../guides/operate-ome/accelerator-quota.md) | Since v1.3. Keeps the status of AcceleratorQuotas up to date, and can turn the quota tree into Kueue queues. | A Deployment, `ome-quota-manager` by default, from the `ome-quota-manager` chart |
| [kubectl ome](../../reference/kubectl-ome/overview.md) | Since v1.3. The OME CLI. It inspects OME's resources and runs alpha actions on them, with your Kubernetes identity. | Your machine, as a kubectl plugin |

## Resources

All of OME's kinds have the API version `ome.io/v1beta1`. OME writes InferenceReplicas and TrafficMaps itself, and you write the rest.

| Kind | Scope | Purpose |
| --- | --- | --- |
| [InferenceService](../serving/inference-services.md) | Namespaced | Serves a model. It names a model, a runtime or both, and describes the engine, decoder and router. |
| [BaseModel, ClusterBaseModel](../models/base-models.md) | Namespaced, Cluster | A model: where its weights live and which nodes keep a copy. |
| [FineTunedWeight](../models/fine-tuned-weights.md) | Cluster | Weights fine-tuned from a base model, which an InferenceService serves on top of that model. |
| [ServingRuntime, ClusterServingRuntime](../runtimes/serving-runtimes.md) | Namespaced, Cluster | How to serve a family of models: the models it supports, and the containers and settings of the engine, decoder and router. |
| [AcceleratorClass](../runtimes/accelerator-classes.md) | Cluster | A kind of GPU or other accelerator, and how to find the nodes that have it. |
| [BenchmarkJob](../serving/benchmarks.md) | Namespaced | A genai-bench run against an InferenceService or a URL. |
| [AutoscalerPolicy](../serving/autoscaler-policy.md) | Namespaced | Since v1.3. Alpha, and off by default. An autoscaling template that InferenceService components attach by name. |
| [RolloutPolicy](../rollouts-and-traffic/rollout-policy.md) | Namespaced | Since v1.3. Alpha, and off by default. A rollout progression that InferenceService rollout groups refer to. |
| [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) | Namespaced | Since v1.3. The workload of one OMENative component, which runs the component's pods as Instances. |
| [AcceleratorQuota](../../guides/operate-ome/accelerator-quota.md) | Cluster | Since v1.3. One entry in a tree of quotas that divides accelerators among teams. |
| [WorkloadCluster](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) | Cluster | Since v1.3. Alpha, and in development. A cluster that OME can place workloads onto, for multi-cluster serving, which is off by default. |
| [TrafficMap](../rollouts-and-traffic/traffic-map.md) | Namespaced | Since v1.3. Alpha, and in development. How the traffic of a multi-cluster InferenceService splits across clusters. |

An InferenceService can use the BaseModels and ServingRuntimes in its own namespace, and every ClusterBaseModel and ClusterServingRuntime.

## How weights reach the nodes

On each node that a [BaseModel or ClusterBaseModel](../models/base-models.md) selects, the model agent:

1. Downloads an `hf://` or `oci://` model from `spec.storage.storageUri` into the directory that `spec.storage.path` names, or finds a `local://` model already there.
2. Reads the model's config files. From them, OME fills in the model's architecture, size and other fields that runtime selection uses.
3. Labels its node with the model's state there: `Updating`, then `Ready` or `Failed`. The label key names the model, such as `models.ome.io/clusterbasemodel.qwen2-5-7b-instruct`.

OME gives the engine and decoder pods a node selector on that label with the value `Ready`, so they run only on nodes that have the model. Until a node does, they stay `Pending`. Each pod mounts the model's directory read-only, and OME sets the `MODEL_PATH` environment variable to it. See [Base models](../models/base-models.md#how-weights-reach-the-nodes).

Since v1.3, the `ome-resources` chart installs the model agent only when you set `modelAgent.enabled: true`, as [Install OME](../../getting-started/install.md) does. Without it, every model except a `pvc://` model stays `In_Transit`, and the pods that serve it stay `Pending`.

A `pvc://` model stays on its PersistentVolumeClaim, and the serving pods mount the claim. See [Serve models from a PVC](../../guides/deploy-models/serve-models-from-pvc.md).

A [fine-tuned weight](../models/fine-tuned-weights.md) downloads into the engine and decoder pods, through an init container that runs ome-agent.

Since v1.3, an InferenceService can skip this path and name only a runtime, which [loads the weights itself](../serving/inference-services.md#the-runtime).

## From InferenceService to pods

An [InferenceService](../serving/inference-services.md) serves one model with up to three components:

- the **engine**, which runs the model;
- optionally, a **decoder**, for prefill-decode disaggregation: the engine's pods run the prefill phase, and the decoder's pods the decode phase;
- optionally, a **router** in front of them, which sends requests to their pods.

When you create or change an InferenceService, OME works through these steps:

1. **Find the model** that `spec.model` names. The kind defaults to ClusterBaseModel, so an InferenceService that uses a [BaseModel](../models/base-models.md) sets `spec.model.kind: BaseModel`.
2. **Choose the runtime.** When `spec.runtime` names a runtime, OME uses it. Otherwise, it picks the best match among the runtimes that support the model and allow automatic selection. See [How runtimes match the model](../models/base-models.md#how-runtimes-match-the-model). By default, the InferenceService follows changes to its runtime. To pin it to one revision, see [Runtime revisions and pinning](../runtimes/runtime-revisions.md).
3. **Build the components.** OME starts from the runtime's `engineConfig`, `decoderConfig` and `routerConfig`, and the InferenceService's own settings win. When the runtime lists accelerator classes and the InferenceService names a class or a selection policy, OME picks an [AcceleratorClass](../runtimes/accelerator-classes.md) for the engine and decoder. Their pods get the class's node selector. See [Select accelerators](../../guides/deploy-models/select-accelerators.md).
4. **Create each component's workload** for its [deployment mode](#deployment-modes), and a Service for it. Both are named after the InferenceService and the component, such as `llama-engine` for the engine of `llama`.
5. **Expose the InferenceService.** By default, OME creates a ClusterIP Service named after the InferenceService, which sends requests to the router's pods, or to the engine's when there's no router. When you turn on ingress creation, OME creates a Kubernetes Ingress or Gateway API HTTPRoutes instead. See [Ingress and external access](../rollouts-and-traffic/ingress.md).
6. **Report status.** OME records the InferenceService's URL, the state of its components, and conditions such as `Ready` in its status. Since v1.3, [`kubectl ome status`](../../reference/kubectl-ome/status.md) shows in one report why the InferenceService is or isn't ready.

If OME can't find a runtime, for example because the runtime was deleted, it records a `RuntimeNotFound` warning event and leaves the serving workload as it is.

### Deployment modes and OMENative {#deployment-modes}

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

Since v1.3, an engine or decoder with a `leader` or `worker` block runs on OMENative by default. The block can come from the InferenceService or its runtime. Any other component runs as a RawDeployment. To run all of an InferenceService's components on OMENative, the router included, set `spec.deploymentMode: OMENative`.

```text
OMENative:      InferenceService -> InferenceReplica -> pods, grouped into Instances
RawDeployment:  InferenceService -> Deployment -> ReplicaSet -> pods
```

On OMENative, each replica of a component is an [Instance](../omenative/instances.md): one pod, or a leader pod and its worker pods for a model that spans nodes. By default, when a pod of a multi-pod Instance fails, OME recreates all of its pods. When the scheduler-plugins PodGroup CRD is installed, OME also creates a PodGroup for each multi-pod Instance, so that a gang-aware scheduler, such as the [OME scheduler](../scheduling/ome-scheduler.md), places its pods together. See [Gang scheduling](../serving/gang-scheduling.md).

A component's `ome.io/deploymentMode` annotation wins over all of this. Since v1.3, it's the only way to run a component in the deprecated `MultiNode` mode, on a LeaderWorkerSet. On v1.2.2, a `leader` or `worker` block makes a component `MultiNode`, and there's no OMENative mode. See [How OME resolves the mode](deployment-modes.md#how-ome-resolves-the-mode).

## Next steps

- [OMENative overview](../omenative/overview.md): how OMENative creates, updates, repairs and migrates Instances.
- [Deployment modes and OMENative](deployment-modes.md): how OME picks a component's mode, and how to change it.
- [The OME scheduler](../scheduling/ome-scheduler.md) and [Alfred](../scheduling/alfred.md): alpha gang scheduling and migration advice for OMENative's Instances.
- [Serving runtimes](../runtimes/serving-runtimes.md): how a runtime declares the models it supports and the pods it runs.
- [InferenceService](../serving/inference-services.md): the fields of an InferenceService and its components.
