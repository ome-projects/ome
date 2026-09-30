---
title: InferenceService
description: An InferenceService names a model, a serving runtime or both, and declares the engine, decoder and router components that OME turns into workloads.
---

An InferenceService serves a model. It names a [base model](../models/base-models.md), a [serving runtime](../runtimes/serving-runtimes.md) or both, and declares the components that serve the model: an engine, and optionally a decoder and a router. OME fills in each component from the runtime, runs it as a Deployment or on [OMENative](../omenative/overview.md), and reports in `status` when the InferenceService is ready and where to send requests.

## A minimal InferenceService

This InferenceService serves the ClusterBaseModel `llama-3-2-1b-instruct` with the ClusterServingRuntime `vllm-llama-3-2-1b-instruct`. Create both from `config/models/meta/Llama-3.2-1B-Instruct.yaml` and `config/runtimes/vllm/llama-3-2-1b-instruct-rt.yaml` in the OME repository. The [model agent](../../guides/operate-ome/model-agent.md) downloads the model to your nodes. It's off by default since v1.3: turn it on as [Install OME](../../getting-started/install.md#step-3-install-ome) shows, and set `modelAgent.hostPath` to `/raid/models`, where the models in `config/models/` keep their weights. The model is gated, so the agent needs your Hugging Face token in the Secret `hf-token` in the `ome` namespace; see [Credentials](../models/base-models.md#credentials). Wait until the model is `Ready`, as [Model lifecycle](../models/base-models.md#model-lifecycle) describes: until then, no runtime matches it, and the webhook rejects the InferenceService.

Create a namespace for it:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine: {}
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

The empty `engine` section declares the engine, and the runtime fills it in. With no runtime named, OME picks one that supports the model, and the webhook's warning says which. If none does, the webhook rejects the InferenceService with a message that starts with `no supporting runtime found for model llama-3-2-1b-instruct`. [The webhook rejects the InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md#the-webhook-rejects-the-inferenceservice) lists the other messages.

With the chart's default settings, OME creates:

- A Deployment and a Service named `llama-3-2-1b-instruct-engine`. The pods run only on nodes where the model is ready, as [Node labels and status](../models/base-models.md#node-labels-and-status) describes.
- A HorizontalPodAutoscaler and a PodDisruptionBudget of the same name. The autoscaler keeps CPU use at 80% with 1 to 3 replicas, and the budget lets one pod be unavailable at a time.
- A Service named `llama-3-2-1b-instruct` in front of the engine. It's how you reach the model while ingress creation is off, the default; see [Ingress and external access](../rollouts-and-traffic/ingress.md).

Wait for the InferenceService to be ready:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

`status.url` is where to send requests. With ingress creation off, it's the address of the Service in front of the model:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo -o jsonpath='{.status.url}'
```

```output
http://llama-3-2-1b-instruct.llama-demo.svc.cluster.local:8080
```

To send a request from outside the cluster, port-forward that Service, as [Serve your first model](../../getting-started/serve-your-first-model.md#step-7-send-a-request) does.

## Model and runtime references

An InferenceService names a model, a runtime or both:

| You set | What OME does |
| --- | --- |
| `spec.model` | Picks the runtime that [best supports the model](../models/base-models.md#how-runtimes-match-the-model), mounts the model's weights and points `MODEL_PATH` at them. |
| `spec.model` and `spec.runtime` | Serves the model with the runtime you name. |
| `spec.runtime` | Since v1.3. Serves with the runtime, which loads the weights itself: see [The runtime](#the-runtime). |

The webhook rejects an InferenceService that sets neither: `at least one of spec.model or spec.runtime must be set`.

### The model

`spec.model.kind` defaults to `ClusterBaseModel`. To serve a [BaseModel](../models/base-models.md#basemodel-and-clusterbasemodel) from the InferenceService's namespace, set `kind: BaseModel`:

```yaml
spec:
  model:
    name: llama-3-2-1b-instruct
    kind: BaseModel
```

Leave `kind` out and OME needs a ClusterBaseModel of that name, even when a BaseModel in the namespace has it. When neither exists, the webhook rejects the InferenceService with a message that starts with `referenced model "llama-3-2-1b-instruct" not found in namespace "llama-demo"`.

`spec.model.fineTunedWeights` names [fine-tuned weights](../models/fine-tuned-weights.md) to serve on top of the model. OME can't serve a model with `Sharded` [distribution](../models/base-models.md#distribution) in this release.

Since v1.3, `spec.model.overlays` mounts other models into every serving pod, for the runner to choose between at request time. Each overlay is at `/opt/ml/model-overlays/<name>`, and `OVERLAY_<NAME>_MODEL_PATH` holds that path, with the name upper-cased and its hyphens turned into underscores. OME doesn't switch models itself. If an overlay is disabled or deleted later, OME leaves it out, and the `OverlaysReady` condition says why.

### The runtime

Since v1.3, you can leave out `spec.model` when the runtime loads the weights itself, from its container or from a volume or init container that you add under `spec.engine`. Such an InferenceService works without a BaseModel or the model agent. OME skips all model work: runtime selection, `MODEL_PATH`, the model volumes, the model-ready node selector and `status.modelStatus`. Overlays and fine-tuned weights need `spec.model`. A runtime whose arguments use `$(MODEL_PATH)`, as the catalog runtimes do, gets that text literally unless you set `MODEL_PATH` in the runner's `env`. On v1.2.2, every InferenceService needs `spec.model`.

This InferenceService serves the same model without `spec.model`. Its vLLM engine downloads the weights from the Hugging Face repository that `MODEL_PATH` names, with the token in the `hf-token` Secret that [Credentials](../models/base-models.md#credentials) creates in `llama-demo`:

```yaml title="isvc-runtime-only.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-runtime-only
  namespace: llama-demo
spec:
  runtime:
    name: vllm-llama-3-2-1b-instruct
  engine:
    runner:
      env:
        - name: MODEL_PATH
          value: meta-llama/Llama-3.2-1B-Instruct
        - name: HF_TOKEN
          valueFrom:
            secretKeyRef:
              name: hf-token
              key: token
```

A runtime you name can be a ClusterServingRuntime or a ServingRuntime in the InferenceService's namespace, as [How OME resolves the name](../../guides/deploy-models/reference-a-runtime-explicitly.md#how-ome-resolves-the-name) shows. Since v1.3, when that runtime doesn't declare support for the model, the webhook admits the InferenceService with a warning. It starts with `runtime "<runtime>" does not declare support for model "<model>"`. If the runtime goes away later, OME leaves the running workload as it is and, since v1.3, sets the `RuntimeReady` condition to `False` until a runtime resolves.

By default, a change to the runtime rolls out to the InferenceService. To pin it to a snapshot of the runtime, set `spec.runtime.autoSync: false`, and optionally `spec.runtime.revision`: see [Runtime revisions and pinning](../runtimes/runtime-revisions.md).

## Components

An InferenceService has up to three components:

| Component | What it does |
| --- | --- |
| `engine` | Runs the model server. Every InferenceService needs one: without it, OME creates no workload and records a `DeploymentModeError` event. With a decoder, the engine runs the prefill phase. |
| `decoder` | Runs the decode phase, for prefill-decode disaggregated serving: see [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md). Needs an engine. |
| `router` | Receives requests and spreads them across the engine, or the engine and decoder. Without a router, requests go to the engine. |

A component exists only when you declare it. The runtime's `engineConfig`, `decoderConfig` and `routerConfig` fill in the components you declare, and your values win, so a runtime's `routerConfig` adds no router by itself.

Each component is a pod spec, so it takes pod fields such as `nodeSelector` and `tolerations`. It also has these fields:

| Field | Components | What it does |
| --- | --- | --- |
| `runner` | All | The main container. Your settings, such as `image`, `args` or `resources`, override the runtime's. |
| `minReplicas`, `maxReplicas` | All | The replica range. Unset bounds come from the runtime, then from the chart's [replica defaults](../../guides/operate-ome/set-replica-defaults.md): 1 to 3 for the engine and decoder, 1 to 2 for the router. |
| `labels`, `annotations` | All | Added to the component's workload and pods. |
| `minAvailable`, `maxUnavailable` | All | The component's PodDisruptionBudget, in place of the default budget. Set one of them. |
| `deploymentStrategy` | All | The Deployment's update strategy, for components that run as a RawDeployment. |
| `timeoutSeconds` | All | The request timeout on the component's HTTPRoute. See [Configure route timeouts](../../guides/networking/configure-route-timeouts.md). |
| `servicePortAppProtocols` | All | Since v1.3. The `appProtocol` of each Service port. See [Set Service app protocols](../../guides/networking/set-service-app-protocols.md). |
| `lifecycle` | All | Since v1.3. How an OMENative component updates and restarts its Instances, and how long each may take to become ready. See [OMENative update strategies](../architecture/omenative-update-strategies.md) and [Instance restart policy](../omenative/instance-restart-policy.md). |
| `autoscaler`, `autoscalerPolicyRef` | All | Since v1.3. How the component scales. See [Scaling](#scaling). |
| `leader`, `worker` | Engine, decoder | Run each replica as a leader pod and worker pods. See [Multi-node serving](#multi-node-serving). |
| `topologyKey`, `topologySpread`, `topologySpreadKey` | Engine, decoder | Since v1.3. Keep each OMENative Instance's pods in one topology domain, and spread Instances across failure domains. |
| `acceleratorOverride` | Engine, decoder | The component's own accelerator choice. See [Accelerator selection](#accelerator-selection). |
| `config` | Router | Settings passed to the router's container as environment variables. |

## Deployment modes {#how-ome-picks-the-deployment-mode}

Each component runs as a Deployment, the default, or, since v1.3, on [OMENative](../omenative/overview.md). OMENative runs the component as an InferenceReplica, which manages each replica as one [Instance](../omenative/instances.md): one pod, or a leader and its workers. OME creates, updates, repairs, moves and removes each Instance as one unit.

An engine or decoder with a `leader` or `worker`, set in the InferenceService or its runtime, runs on OMENative by default. Any component can [opt in](../architecture/deployment-modes.md#opt-in-to-omenative) for per-Instance status, held revisions when updates keep failing, and updates that by default bring the new Instance up before the old one drains. Opt in the whole InferenceService with `spec.deploymentMode: OMENative`, or one component with its `ome.io/deploymentMode` annotation. `MultiNode` (deprecated), which only that annotation selects, runs a component as a LeaderWorkerSet and needs the LeaderWorkerSet controller.

[Deployment modes and OMENative](../architecture/deployment-modes.md) compares the modes and shows how OME resolves a component's mode.

## Multi-node serving

A model too large for one node's GPUs runs each replica across several pods. The engine or decoder declares them with `leader` and `worker`: each replica gets one leader pod and `worker.size` worker pods. The default `worker.size` is the runtime's value, or 1. A `runner` under `leader` or `worker` needs a `name`. Since v1.3, the webhook rejects a `leader` without a `worker`, or the reverse, with a message that starts with `InvalidLeaderWorkerPairing`.

On OMENative, the leader and its workers form one Instance and get `OME_*` environment variables, such as `OME_LEADER_ADDRESS`. When the PodGroup CRD is installed, OME also creates a PodGroup for each Instance, for [gang scheduling](gang-scheduling.md). [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md) walks through it.

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

The pre-configured multi-node runtimes read the `LWS_*` environment variables that a LeaderWorkerSet sets, so they need the deprecated `MultiNode` mode. To serve one as it is, set the component's `ome.io/deploymentMode` annotation to `MultiNode`, or [make an OMENative copy of it](../../guides/omenative/move-from-leaderworkerset.md#step-2-make-an-omenative-copy-of-the-runtime). On v1.2.2, `leader` and `worker` select `MultiNode`: [pin such a component](../../guides/omenative/move-from-leaderworkerset.md#pin-a-component-to-multinode) before you upgrade, to keep it there.

## Accelerator selection

When the runtime lists accelerator classes in `acceleratorRequirements`, OME can pick an [accelerator class](../runtimes/accelerator-classes.md#how-services-choose-a-class) for the engine and the decoder. The class sets their node selector and, when the runner sets none, their GPU resources. Name a class in `spec.acceleratorSelector` or in a component's `acceleratorOverride`, or a policy, such as `Cheapest`, that picks one. Otherwise, OME picks no class. [Select accelerators](../../guides/deploy-models/select-accelerators.md) shows how.

## Scaling {since=v1.3}

Each component scales on its own, between its `minReplicas` and `maxReplicas`, by default with a HorizontalPodAutoscaler that keeps CPU use at 80%. The component's `autoscaler` block, which is alpha, can pick an [HPA](component-autoscaling.md#hpa) on your metrics, [KEDA](component-autoscaling.md#keda), or [your own autoscaler or none](component-autoscaling.md#external-and-none) instead. The block can also come from the runtime or from an [AutoscalerPolicy](autoscaler-policy.md), which is alpha and off by default; [Which setting wins](component-autoscaling.md#which-setting-wins) gives the order. A RawDeployment component can [scale to zero with KEDA](../../guides/scale-and-migrate/scale-to-zero-with-keda.md).

`spec.scalingPolicy` is alpha too. Its only working mode is `Independent`, the default; the webhook rejects `Proportional` and `Pinned` with `ScalingModeNotImplemented`. The [`kubectl ome autoscale`](../../reference/kubectl-ome/autoscale.md) command shows the autoscaling in effect for each component and where it came from.

On v1.2.2, `kedaConfig`, `scaleTarget` and `scaleMetric` configure scaling: [Upgrade from v1.2.2](component-autoscaling.md#upgrade-from-v1-2-2) shows how to move them to `autoscaler`.

## Rollouts and traffic {since=v1.3}

On OMENative, related components, such as a prefill engine and its decoder, roll out together: `spec.rollout` puts them in up to three [rollout groups](../rollouts-and-traffic/rollout-groups.md). Each group rolls out a change by blue-green, rolling update or canary. Blue-green is the default. A canary moves capacity to the new revision in steps, with timed or manual pauses or Prometheus analysis, and failed analysis rolls it back to the stable revision. With the chart's settings, a revision whose updates keep failing is retried with backoff, then held until you [release it](../../guides/roll-out-changes/release-a-held-revision.md) or a newer revision replaces it.

A group can take its steps from a [rollout policy](../rollouts-and-traffic/rollout-policy.md) through `policyRef`. Rollout policies are alpha and off by default: set `ome.rolloutPolicy.enabled: true` in both the `ome-crd` and `ome-resources` Helm charts, then restart the controller, as [Turn on the feature](../rollouts-and-traffic/rollout-policy.md#turn-on-the-feature) describes.

Rollout groups and canaries need OMENative set in the InferenceService itself: `spec.deploymentMode: OMENative`, or the component's `ome.io/deploymentMode` annotation. A RawDeployment component, the default for a single-pod one, rolls out as a plain Deployment update, following its `deploymentStrategy`, with no group, canary or held revision.

[`kubectl ome rollout`](../../reference/kubectl-ome/rollout.md) shows a rollout's progress and what holds it. Its alpha subcommands pause and resume a rollout, promote or roll back a canary, and repin a drifted plan.

`spec.traffic` sets load balancing, session affinity and endpoint override in your gateway's backend policy, an Envoy Gateway BackendTrafficPolicy or an Istio DestinationRule, as [Traffic policy](../rollouts-and-traffic/traffic-policy.md) describes. It doesn't split requests between revisions.

`spec.placement` and `spec.routing` serve one InferenceService from several clusters. They're alpha, still in development and off by default. OME reads them only on a multi-cluster control plane, with `ome.multicluster.enabled: true` set in the `ome-resources` chart. See [Traffic map](../rollouts-and-traffic/traffic-map.md).

## Status

The `Ready` condition is `True` when both `EngineReady` and `IngressReady` are. With ingress creation off, the default, `IngressReady` is `True` with reason `IngressDisabled`, so `Ready` follows the engine alone: check `DecoderReady` and `RouterReady` too. [When IngressReady is True](../rollouts-and-traffic/ingress.md#when-ingressready-is-true) covers an Ingress and the Gateway API.

| Condition | What it says |
| --- | --- |
| `EngineReady` | The engine's workload is available. |
| `DecoderReady` | The decoder's workload is available. Only when you declare a decoder. |
| `RouterReady` | The router's workload is available. Only when you declare a router. |
| `IngressReady` | The Ingress or HTTPRoutes that OME creates are ready, or ingress creation is off. |
| `OverlaysReady` | Since v1.3. Only when you declare overlays: `True` with reason `AllOverlaysMounted`, or `False` with reason `OverlaysSkipped` and the overlays it left out. |
| `RuntimeReady` | Since v1.3. Only after a runtime problem: `False` with reason `RuntimeNotFound`, then `True` with reason `RuntimeResolved` once a runtime resolves. |
| `Ready` | The InferenceService can take requests. |

[Rollout groups](../rollouts-and-traffic/rollout-groups.md) and [traffic policies](../rollouts-and-traffic/traffic-policy.md) add their own conditions.

The `RUNTIME` column of `kubectl get inferenceservice` shows `spec.runtime.name`, so it's empty when OME picks the runtime. The engine's workload names the runtime in its `serving-runtime` label:

```bash
kubectl get deployment llama-3-2-1b-instruct-engine -n llama-demo -o jsonpath='{.metadata.labels.serving-runtime}'
```

```output
vllm-llama-3-2-1b-instruct
```

`status.modelStatus` says whether the model loaded. It stays unset for an InferenceService without `spec.model`. Its `transitionStatus` is `InProgress` until the InferenceService is ready, then `UpToDate`. When the engine's main container fails, it's `BlockedByFailedLoad`, and `lastFailureInfo` holds the container's exit code and message.

`status.components.<component>` holds the component's `url`, once its workload is available. Since v1.3, it also reports the component's [autoscaling](component-autoscaling.md#read-the-result-in-status) and, on OMENative, its [revisions and Instance counts](../architecture/deployment-modes.md#lifecycle-status) and [rollout phase and canary progress](../../reference/rollouts/canary-progression.md).

## Next steps

- [Deployment modes and OMENative](../architecture/deployment-modes.md): how each mode runs a component, and when to choose OMENative.
- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md): opt a single-pod InferenceService in, and check its InferenceReplica, Instance and pod.
- [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md): run prefill on the engine and decode on the decoder, each on OMENative.
- [Component autoscaling](component-autoscaling.md): scale a component with an HPA, KEDA or your own autoscaler.
- [Rollout groups](../rollouts-and-traffic/rollout-groups.md): roll out changes to OMENative components together or in order.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md) and [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): name the runtime, or find out why OME can't pick one.
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md): find out why an InferenceService is rejected or isn't ready.
