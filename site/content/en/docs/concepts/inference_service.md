---
title: "Inference Service"
date: 2023-03-14
weight: 30
description: >
  InferenceService is the primary resource that manages the deployment and serving of machine learning models in OME.
---

## What is an InferenceService?

An InferenceService is the central Kubernetes resource in OME that orchestrates the complete lifecycle of model serving. It acts as a declarative specification that describes how you want your AI models deployed, scaled, and served across your cluster.

Think of InferenceService as the "deployment blueprint" for your AI workloads. It brings together models (defined by BaseModel/ClusterBaseModel), runtimes (defined by ServingRuntime/ClusterServingRuntime), and infrastructure configuration to create a complete serving solution.

## Architecture Overview

OME uses a **component-based architecture** where InferenceService can be composed of multiple specialized components:

- **Model**: References the AI model to serve (BaseModel/ClusterBaseModel)
- **Runtime**: References the serving runtime environment (ServingRuntime/ClusterServingRuntime)
- **Engine**: Main inference component that processes requests
- **Decoder**: Optional component for disaggregated serving (prefill-decode separation)
- **Router**: Optional component for request routing and load balancing

### New vs Deprecated Architecture

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
spec:
  model:
    name: llama-3-70b-instruct
  runtime:
    name: vllm-text-generation
  engine:
    minReplicas: 1
    maxReplicas: 3
    resources:
      requests:
        nvidia.com/gpu: "1"
```

### Top-Level Model Reference (`spec.model`)

The current way to select a model is the top-level `spec.model` field, a `ModelRef` that points at a `BaseModel` or `ClusterBaseModel`. Because the reference lives at the top level (not inside a component), the model is managed independently of the serving configuration and is shared by the Engine, Decoder, and Router.

| Attribute          | Type     | Description                                                     |
|--------------------|----------|------------------------------------------------------------------|
| `name`             | string   | Name of the model resource being referenced (required).          |
| `kind`             | string   | Resource kind. Defaults to `ClusterBaseModel`; use `BaseModel` for a namespace-scoped model. |
| `apiGroup`         | string   | API group of the referenced resource. Defaults to `ome.io`.      |
| `fineTunedWeights` | []string | Optional references to fine-tuned weights to apply on top of the base model. |

```yaml
spec:
  model:
    name: llama-3-70b-instruct
    kind: ClusterBaseModel      # or BaseModel for a namespaced model
    fineTunedWeights:
      - my-lora-adapter          # optional, applied on top of the base model
```


## Component Types

### Engine Component

The **Engine** is the primary inference component that processes model requests. It handles model loading, inference execution, and response generation.

```yaml
spec:
  engine:
    # Pod-level configuration
    serviceAccountName: custom-sa
    nodeSelector:
      accelerator: nvidia-a100

    # Component configuration
    minReplicas: 1
    maxReplicas: 10
    scaleMetric: cpu
    scaleTarget: 70

    # Container configuration
    runner:
      image: custom-vllm:latest
      resources:
        requests:
          nvidia.com/gpu: "2"
        limits:
          nvidia.com/gpu: "2"
      env:
        - name: CUDA_VISIBLE_DEVICES
          value: "0,1"
```

### Decoder Component

The **Decoder** is used for disaggregated serving architectures where the prefill (prompt processing) and decode (token generation) phases are separated for better resource utilization.

```yaml
spec:
  decoder:
    minReplicas: 2
    maxReplicas: 8
    runner:
      resources:
        requests:
          cpu: "4"
          memory: "8Gi"
```

### Router Component

The **Router** handles request routing, cache awareness load balancing, or prefill and decode disaggregation load balancing.

```yaml
spec:
  router:
    minReplicas: 1
    maxReplicas: 3
    config:
      routing_strategy: "round_robin"
      health_check_interval: "30s"
    runner:
      resources:
        requests:
          cpu: "1"
          memory: "2Gi"
```

## Deployment Modes

OME automatically selects the optimal deployment mode based on your configuration:

| Mode                              | Description                                 | Use Cases                                                         | Infrastructure                                                                |
|-----------------------------------|---------------------------------------------|-------------------------------------------------------------------|-------------------------------------------------------------------------------|
| **Raw Deployment**                | Standard Kubernetes Deployment              | Stable workloads, predictable traffic, no cold starts             | Kubernetes Deployments + Services                                             |
| **Multi-Node**                    | Distributed inference across multiple nodes | Large models (DeepSeek), models that can not fit in a single node | LeaderWorkerSet                                                               |
| **Prefill-Decode Disaggregation** | Disaggregated serving architecture          | Maximizing resource utilization, better performance,              | Raw Deployments or LeaderWorkerSet(if the model can not fit in a single node) |

### Raw Deployment Mode (Default)

Uses standard Kubernetes Deployments with full control over pod lifecycle and scaling.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
spec:
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 10
```

This deployment mode offers direct Kubernetes management with standard HPA-based autoscaling, no cold starts, and is ideal for stable, predictable workloads.

### Multi-Node Mode

Enables distributed model serving across multiple nodes using LeaderWorkerSet.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: deepseek-chat
spec:
  model:
    name: deepseek-r1  # Large model requiring multiple GPUs
  engine:
    minReplicas: 1
    maxReplicas: 2
    # Worker node configuration
    worker:
      size: 1  # Number of worker nodes
```
This deployment mode enables distributed inference using LeaderWorkerSet, with support for multi-GPU and multi-node setups, and is optimized for large language models through automatic coordination between nodes

> **⚠️ WARNING**: Multi-node configurations typically require high-performance networking such as RoCE or InfiniBand, and performance may vary depending on the underlying network topology and hardware provided by different cloud vendors.

### Disaggregated Serving (Prefill-Decode)

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: deepseek-ep-disaggregated
spec:
  model:
    name: deepseek-r1

  # Router handles request routing and load balancing for prefill-decode disaggregation
  router:
    minReplicas: 1
    maxReplicas: 3

  # Engine handles prefill phase
  engine:
    minReplicas: 1
    maxReplicas: 3

  # Decoder handles token generation
  decoder:
    minReplicas: 2
    maxReplicas: 8
```

## Multi-Node Serving

When a model is too large to fit on a single node, the Engine (and, for disaggregated serving, the Decoder) can be spread across multiple nodes using a **leader/worker** topology. OME renders this topology as a [LeaderWorkerSet](https://github.com/kubernetes-sigs/lws), which schedules the leader pod and its worker pods as a single co-scheduled group.

### When Multi-Node Mode is Selected

OME derives the deployment mode from the component spec rather than requiring you to name it explicitly. A component runs in **Multi-Node** mode as soon as it defines a `leader` or `worker` block:

- If `engine.leader` **or** `engine.worker` is set, the Engine is deployed as Multi-Node.
- If `decoder.leader` **or** `decoder.worker` is set, the Decoder is deployed as Multi-Node.
- Otherwise the component falls back to Raw Deployment.

You can also force a specific deployment mode with the `ome.io/deploymentMode` annotation (for example `MultiNode`); when present, the annotation takes precedence over the inferred mode.

> **Note**: The `decoder` component only supports Raw Deployment or Multi-Node.

### Leader and Worker Specs

`leader` and `worker` are available on both `EngineSpec` and `DecoderSpec`:

| Attribute      | Type       | Description                                                                              |
|----------------|------------|------------------------------------------------------------------------------------------|
| `leader`       | LeaderSpec | Pod/container spec for the single coordinating leader node.                               |
| `worker`       | WorkerSpec | Pod/container spec for the worker nodes, plus the number of workers via `worker.size`.    |

**LeaderSpec** and **WorkerSpec** both embed a full `PodSpec` and an optional `runner` container override, so you can tune image, resources, environment, and scheduling separately for the leader and the workers. `WorkerSpec` adds one extra field:

| Attribute | Type | Description                                                                                                  |
|-----------|------|--------------------------------------------------------------------------------------------------------------|
| `size`    | int  | Number of worker pods in the group. The total group size mapped to the LeaderWorkerSet is `1` (leader) + `size`. |

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: deepseek-r1-multinode
spec:
  model:
    name: deepseek-r1
  runtime:
    name: srt-multi-node-deepseek-r1-rdma
  engine:
    minReplicas: 1
    maxReplicas: 1
    # Leader coordinates distributed inference
    leader:
      runner:
        resources:
          requests:
            nvidia.com/gpu: "8"
          limits:
            nvidia.com/gpu: "8"
    # Worker nodes perform distributed processing directed by the leader
    worker:
      size: 1   # one worker pod in addition to the leader (group size = 2)
      runner:
        resources:
          requests:
            nvidia.com/gpu: "8"
          limits:
            nvidia.com/gpu: "8"
```

Here `minReplicas`/`maxReplicas` scale the number of leader/worker **groups** (each a complete LeaderWorkerSet replica), while `worker.size` controls how many worker pods sit inside a single group.

> **⚠️ WARNING**: Multi-node configurations typically require high-performance networking such as RoCE or InfiniBand. Performance depends on the underlying network topology and hardware provided by different cloud vendors.

## Automatic Parallelism (PARALLELISM_SIZE)

Runtimes need a total parallelism degree (for example SGLang's `--tp-size`/`--dp-size`), but the right value depends on how the InferenceService is sized — GPUs per pod and the number of worker pods. Instead of hard-coding it, OME computes the value at reconcile time and injects it into the Engine and Decoder runner containers as the `PARALLELISM_SIZE` environment variable. Catalog runtimes reference it in their launch arguments and rely on this contract:

```yaml
# From a catalog runtime (config/runtimes/srt/deepseek-rdma-rt.yaml)
command:
  - python3
  - -m
  - sglang.launch_server
  - --tp-size
  - $(PARALLELISM_SIZE)    # Kubernetes expands this from the injected env var
```

### How the Value is Computed

```
PARALLELISM_SIZE = <GPUs per pod> × (1 + worker.size)
```

- **GPUs per pod** is read from the runner container's resources after the runtime's and AcceleratorClass's resource defaults have been merged. OME walks the operator-configured accelerator resource names (the `acceleratorResources` key of the `inferenceservice-config` ConfigMap, a JSON list such as `["nvidia.com/gpu", "amd.com/gpu"]`) in order and uses the first name present on the container, checking `limits` before `requests`. When `acceleratorResources` is not configured, only `nvidia.com/gpu` is recognized.
- **1 + worker.size** counts the leader — or the single pod, outside multi-node mode — plus the worker pods. `worker.size` counts as `0` when no `worker` block is set, so a single-pod service gets `PARALLELISM_SIZE = <GPUs per pod>`.

For the multi-node example above (8 GPUs per pod, `worker.size: 1`), every engine container gets `PARALLELISM_SIZE=16`. The variable is injected into the leader/primary runner and the worker runner separately, each computed from that container's own GPU resources — normally identical, since leader and workers are sized alike.

### When It is Skipped or Overwritten

The injection runs for the Engine and Decoder in **every deployment mode**, whenever the merged component spec has a runner container (the Router serves no model and never gets the variable). Two cases change that:

- **No recognized accelerator resource on the container** — the variable is not injected at all. An argument referencing `$(PARALLELISM_SIZE)` then keeps the literal string, so a runtime that uses the variable must actually end up with GPU resources on its runner (see [Accelerator Selection](#accelerator-selection)).
- **The runtime's matched `supportedModelFormats` entry has an `acceleratorConfig` for the selected AcceleratorClass with a `tensorParallelismOverride`** — the override owns the parallelism flags (OME rewrites the tensor/pipeline-parallel flags such as `--tp-size`/`--tensor-parallel-size` in the args directly) and skips the `PARALLELISM_SIZE` computation for that component. See [Per-Accelerator Model Configuration](/ome/docs/concepts/serving_runtime/#per-accelerator-model-configuration).

When the computation does run, it **overwrites** any `PARALLELISM_SIZE` already present in the runner's environment, including values you set in `runner.env`. To pin a different parallelism degree, put the literal value in the runtime's arguments or use a `tensorParallelismOverride` — don't set the env var.

## Accelerator Selection

OME can select the accelerator (GPU class) for an InferenceService declaratively instead of requiring hard-coded `nodeSelector` and resource values. Selection is configured through `spec.acceleratorSelector` and can be overridden per component.

### spec.acceleratorSelector

| Attribute          | Type                   | Description                                                                                     |
|--------------------|------------------------|-------------------------------------------------------------------------------------------------|
| `acceleratorClass` | string                 | Explicitly selects a specific AcceleratorClass. Takes precedence over `constraints` and `policy`. |
| `constraints`      | AcceleratorConstraints | Requirements that a matching accelerator must satisfy.                                           |
| `policy`           | AcceleratorSelectionPolicy | Tie-breaking policy applied when multiple accelerators match the constraints.                |

#### Selection Policy

`policy` chooses among the accelerators that satisfy the constraints:

| Value            | Behavior                                                        |
|------------------|-----------------------------------------------------------------|
| `BestFit`        | Selects the accelerator that best matches the model requirements. |
| `Cheapest`       | Selects the lowest-cost accelerator that meets the requirements.  |
| `MostCapable`    | Selects the most powerful accelerator available.                  |
| `FirstAvailable` | Selects the first matching accelerator (fastest scheduling).      |

#### Accelerator Constraints

| Attribute                     | Type     | Description                                                                     |
|-------------------------------|----------|---------------------------------------------------------------------------------|
| `minMemory`                   | int64    | Minimum accelerator memory in GB.                                               |
| `maxMemory`                   | int64    | Maximum accelerator memory in GB (useful for cost control).                     |
| `minComputePerformanceTFLOPS` | int64    | Minimum compute performance in TFLOPS.                                          |
| `minArchitectureVersion`      | string   | Minimum architecture version (NVIDIA compute capability or equivalent).         |
| `requiredFeatures`            | []string | Features that must be present on the accelerator.                               |
| `excludedClasses`             | []string | AcceleratorClasses to avoid.                                                    |
| `architectureFamilies`        | []string | Limits selection to specific families, e.g. `["nvidia-hopper", "nvidia-ampere"]`. |
| `preferredPrecisions`         | []string | Numeric precisions in order of preference, e.g. `["fp8", "fp16", "fp32"]`.       |

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-70b
spec:
  model:
    name: llama-3-3-70b-instruct
  acceleratorSelector:
    policy: Cheapest
    constraints:
      minMemory: 80                 # at least 80 GB per accelerator
      architectureFamilies:
        - nvidia-hopper
        - nvidia-ampere
      preferredPrecisions:
        - fp8
        - fp16
  engine:
    minReplicas: 1
    maxReplicas: 3
```

The accelerator that OME resolves is reported back in the component status under `status.components.<component>.selectedAccelerator`, including the class name, the reason it was chosen, and the `nodeSelector`/resource requests that were applied.

### Per-Component Accelerator Override

Both `EngineSpec` and `DecoderSpec` expose an `acceleratorOverride` field of the same `AcceleratorSelector` type. When set, it overrides the top-level `spec.acceleratorSelector` for that component only. This is useful for disaggregated serving, where the prefill (engine) and decode (decoder) phases may benefit from different hardware.

```yaml
spec:
  model:
    name: deepseek-r1
  # Cluster-wide default for this service
  acceleratorSelector:
    policy: MostCapable
  engine:
    minReplicas: 1
    maxReplicas: 3
    # Engine (prefill) overrides to a memory-optimized class
    acceleratorOverride:
      constraints:
        minMemory: 141
        architectureFamilies:
          - nvidia-hopper
  decoder:
    minReplicas: 2
    maxReplicas: 8
    # Decoder keeps the service-level selector (MostCapable)
```

## Specification Reference

| Attribute           | Type              | Description                                              |
|---------------------|-------------------|----------------------------------------------------------|
| **Core References** |                   |                                                          |
| `model`             | ModelRef          | Reference to BaseModel/ClusterBaseModel to serve         |
| `runtime`           | ServingRuntimeRef | Reference to ServingRuntime/ClusterServingRuntime to use |
| **Components**      |                   |                                                          |
| `engine`            | EngineSpec        | Main inference component configuration                   |
| `decoder`           | DecoderSpec       | Optional decoder component for disaggregated serving     |
| `router`            | RouterSpec        | Optional router component for request routing            |
| **Autoscaling**     |                   |                                                          |
| `kedaConfig`        | KedaConfig        | KEDA event-driven autoscaling configuration              |

### ModelRef Specification

| Attribute          | Type     | Description                                    |
|--------------------|----------|------------------------------------------------|
| `name`             | string   | Name of the BaseModel/ClusterBaseModel         |
| `kind`             | string   | Resource kind (defaults to "ClusterBaseModel") |
| `apiGroup`         | string   | API group (defaults to "ome.io")               |
| `fineTunedWeights` | []string | Optional fine-tuned weight references          |

### ServingRuntimeRef Specification

| Attribute  | Type   | Description                                         |
|------------|--------|-----------------------------------------------------|
| `name`     | string | Name of the ServingRuntime/ClusterServingRuntime    |
| `kind`     | string | Resource kind (defaults to "ClusterServingRuntime") |
| `apiGroup` | string | API group (defaults to "ome.io")                    |

### Component Configuration

All components (Engine, Decoder, Router) share this common configuration structure:

| Attribute                  | Type               | Description                                               |
|----------------------------|--------------------|-----------------------------------------------------------|
| **Pod Configuration**      |                    |                                                           |
| `serviceAccountName`       | string             | Service account for the component pods                    |
| `nodeSelector`             | map[string]string  | Node labels for pod placement                             |
| `tolerations`              | []Toleration       | Pod tolerations for tainted nodes                         |
| `affinity`                 | Affinity           | Pod affinity and anti-affinity rules                      |
| `volumes`                  | []Volume           | Additional volumes to mount                               |
| `containers`               | []Container        | Additional sidecar containers                             |
| **Scaling Configuration**  |                    |                                                           |
| `minReplicas`              | int                | Minimum number of replicas (default: 1)                   |
| `maxReplicas`              | int                | Maximum number of replicas                                |
| `scaleTarget`              | int                | Target value for autoscaling metric                       |
| `scaleMetric`              | string             | Metric to use for scaling (cpu, memory, concurrency, rps) |
| `containerConcurrency`     | int64              | Maximum concurrent requests per container                 |
| `timeoutSeconds`           | int64              | Request timeout in seconds                                |
| **Traffic Management**     |                    |                                                           |
| `canaryTrafficPercent`     | int64              | Percentage of traffic to route to canary version          |
| **Resource Configuration** |                    |                                                           |
| `runner`                   | RunnerSpec         | Main container configuration                              |
| `leader`                   | LeaderSpec         | Leader node configuration (multi-node only)               |
| `worker`                   | WorkerSpec         | Worker node configuration (multi-node only)               |
| **Deployment Strategy**    |                    |                                                           |
| `deploymentStrategy`       | DeploymentStrategy | Kubernetes deployment strategy (RawDeployment only)       |
| **KEDA Configuration**     |                    |                                                           |
| `kedaConfig`               | KedaConfig         | Component-specific KEDA configuration                     |

### RunnerSpec Configuration

| Attribute      | Type                 | Description                                |
|----------------|----------------------|--------------------------------------------|
| `name`         | string               | Container name                             |
| `image`        | string               | Container image                            |
| `command`      | []string             | Container command                          |
| `args`         | []string             | Container arguments                        |
| `env`          | []EnvVar             | Environment variables                      |
| `resources`    | ResourceRequirements | CPU, memory, and GPU resource requirements |
| `volumeMounts` | []VolumeMount        | Volume mount points                        |

### KEDA Autoscaling

By default, Raw Deployment components scale with the Kubernetes Horizontal Pod Autoscaler (HPA) driven by the `scaleMetric`/`scaleTarget` fields (CPU, memory, concurrency, or RPS). **KEDA** (Kubernetes Event-driven Autoscaling) is an alternative that lets you scale on **custom, application-level signals** pulled from Prometheus — for example time-per-output-token, request latency, or queue depth — rather than the built-in resource metrics.

Reach for KEDA when:

- HPA's CPU/memory/concurrency metrics do not correlate well with your model's real load.
- You want to scale on a serving-specific Prometheus metric (e.g. `sglang_time_per_output_token_seconds`).
- Your Prometheus endpoint requires authentication (Grafana Cloud, mTLS, bearer tokens).

KEDA is configured either at the service level via `spec.kedaConfig` or per component via the component's own `kedaConfig`. Set `enableKeda: true`, point `promServerAddress` at your Prometheus endpoint, supply a `customPromQuery` (use `%s` where the InferenceService name should be substituted), and define `scalingThreshold` with a `scalingOperator`. For authenticated endpoints, reference a KEDA `TriggerAuthentication` via `authenticationRef` and set `authModes`. See the fully worked Grafana Cloud example below.

| Attribute           | Type                     | Description                                                        |
|---------------------|--------------------------|--------------------------------------------------------------------|
| `enableKeda`        | bool                     | Whether to enable KEDA autoscaling                                 |
| `promServerAddress` | string                   | Prometheus server URL for metrics                                  |
| `customPromQuery`   | string                   | Custom Prometheus query for scaling                                |
| `scalingThreshold`  | string                   | Threshold value for scaling decisions                              |
| `scalingOperator`   | string                   | Comparison operator (GreaterThanOrEqual, LessThanOrEqual)          |
| `authenticationRef` | ScalerAuthenticationRef  | Reference to TriggerAuthentication for Prometheus authentication   |
| `authModes`         | string                   | Authentication mode (basic, tls, bearer, custom)                   |

#### ScalerAuthenticationRef Specification

| Attribute | Type   | Description                                                                    |
|-----------|--------|--------------------------------------------------------------------------------|
| `name`    | string | Name of the TriggerAuthentication or ClusterTriggerAuthentication resource     |
| `kind`    | string | Kind of auth resource (TriggerAuthentication or ClusterTriggerAuthentication)  |

#### Example: KEDA with Grafana Cloud Authentication

When using Grafana Cloud or other authenticated Prometheus endpoints, you need to create a `TriggerAuthentication` resource and reference it in your InferenceService:

```yaml
# 1. Create a secret with Grafana Cloud credentials
apiVersion: v1
kind: Secret
metadata:
  name: grafana-cloud-auth
  namespace: my-namespace
type: Opaque
stringData:
  username: "123456"  # Grafana Cloud instance ID
  password: "glc_xxx" # Grafana Cloud API token with metrics:read scope
---
# 2. Create a TriggerAuthentication resource
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: grafana-cloud-prometheus-auth
  namespace: my-namespace
spec:
  secretTargetRef:
    - parameter: username
      name: grafana-cloud-auth
      key: username
    - parameter: password
      name: grafana-cloud-auth
      key: password
---
# 3. Reference it in InferenceService
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: my-model
  namespace: my-namespace
  annotations:
    ome.io/autoscalerClass: keda
spec:
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 7
    kedaConfig:
      enableKeda: true
      promServerAddress: "https://prometheus-prod-39-prod-eu-north-0.grafana.net/api/prom"
      authenticationRef:
        name: grafana-cloud-prometheus-auth
        kind: TriggerAuthentication
      authModes: "basic"
      customPromQuery: |
        histogram_quantile(0.5,
          sum by(le) (
            rate(sglang_time_per_output_token_seconds_bucket{ome_io_inferenceservice="%s"}[5m])
          )
        )
      scalingThreshold: "0.07"
      scalingOperator: "GreaterThanOrEqual"
```


## Status and Monitoring

### InferenceService Status

The InferenceService status provides comprehensive information about the deployment state:

```yaml
status:
  url: "http://llama-chat.default.example.com"
  address:
    url: "http://llama-chat.default.svc.cluster.local"
  conditions:
    - type: Ready
      status: "True"
      lastTransitionTime: "2024-01-15T10:30:00Z"
    - type: IngressReady
      status: "True"
      lastTransitionTime: "2024-01-15T10:25:00Z"
  components:
    engine:
      url: "http://llama-chat-engine.default.example.com"
      latestReadyRevision: "llama-chat-engine-00001"
      latestCreatedRevision: "llama-chat-engine-00001"
      traffic:
        - revisionName: "llama-chat-engine-00001"
          percent: 100
          latestRevision: true
    router:
      url: "http://llama-chat-router.default.example.com"
      latestReadyRevision: "llama-chat-router-00001"
  modelStatus:
    transitionStatus: "UpToDate"
    modelRevisionStates:
      activeModelState: "Loaded"
      targetModelState: "Loaded"
```

### Condition Types

| Condition        | Description                                 |
|------------------|---------------------------------------------|
| `Ready`          | Overall readiness of the InferenceService   |
| `IngressReady`   | Network routing is configured and ready     |
| `EngineReady`    | Engine component is ready to serve requests |
| `DecoderReady`   | Decoder component is ready (if configured)  |
| `RouterReady`    | Router component is ready (if configured)   |

### Model Status and Troubleshooting

`status.modelStatus` is the primary place to look when an InferenceService is stuck or not becoming Ready. It has three parts: an overall `transitionStatus`, the per-revision `modelRevisionStates`, and — when something goes wrong — a `lastFailureInfo` block.

#### transitionStatus

`transitionStatus` tells you whether the serving endpoints reflect the current spec or are still converging:

| Value                 | Meaning                                                       |
|-----------------------|---------------------------------------------------------------|
| `UpToDate`            | The endpoints reflect the current spec (steady state).        |
| `InProgress`          | Waiting for the target model to reach the active model's state. |
| `BlockedByFailedLoad` | The target model failed to load — inspect `lastFailureInfo`.  |
| `InvalidSpec`         | The spec failed validation — inspect `lastFailureInfo`.       |

#### modelState

`modelRevisionStates` reports `activeModelState` (the model currently serving) and `targetModelState` (the model being rolled out). Each uses these values:

| State          | Description                             |
|----------------|-----------------------------------------|
| `Pending`      | Model is not yet registered             |
| `Standby`      | Model is available but not loaded (loads on first use) |
| `Loading`      | Model is currently loading              |
| `Loaded`       | At least one copy of the model is loaded and ready for inference |
| `FailedToLoad` | All copies of the model failed to load  |

#### lastFailureInfo

When `transitionStatus` is `BlockedByFailedLoad` or `InvalidSpec` (or a `modelState` is `FailedToLoad`), `lastFailureInfo` explains why:

| Field               | Description                                                          |
|---------------------|----------------------------------------------------------------------|
| `reason`            | High-level failure class (see table below).                          |
| `message`           | Detailed human-readable error message.                               |
| `location`          | Component the failure relates to (usually the Pod name).             |
| `modelRevisionName` | Internal revision/ID of the model tied to the failing spec.          |
| `exitCode`          | Exit status from the last container termination, when applicable.    |
| `time`              | When the failure occurred or was discovered.                         |

Common `reason` values and what they point to:

| Reason                       | What to check                                                                 |
|------------------------------|-------------------------------------------------------------------------------|
| `BaseModelNotFound`          | The referenced BaseModel/ClusterBaseModel does not exist (cluster or namespace). |
| `BaseModelNotReady`          | The base model exists but has not finished downloading / is not Ready.        |
| `BaseModelDisabled`          | The base model is disabled.                                                   |
| `BaseModelDeprecated`        | The base model is deprecated.                                                 |
| `FineTunedWeightsNotFound`   | A referenced fine-tuned weight does not exist.                               |
| `FineTunedWeightsDisabled`   | A referenced fine-tuned weight is disabled.                                  |
| `FineTunedWeightsDeprecated` | A referenced fine-tuned weight is deprecated.                                |
| `FineTuneWeightLoadFailed`   | Fine-tuned weights failed to load.                                           |
| `ModelLoadFailed`            | The model failed to load inside the ServingRuntime container.                |
| `ContainerStartupFailed`     | The serving container failed to start before becoming ready (check `exitCode`, logs). |
| `RuntimeUnhealthy`           | The ServingRuntime containers failed to start or are unhealthy.              |
| `RuntimeDisabled`            | The selected ServingRuntime is disabled.                                     |
| `NoSupportingRuntime`        | No ServingRuntime supports the specified model type.                         |
| `RuntimeNotRecognized`       | No ServingRuntime is defined with the specified runtime name.               |
| `InvalidRouterSpec`          | The router spec is invalid.                                                  |

#### Debugging Workflow

1. Read `transitionStatus`. `UpToDate` means the model layer is healthy — if the service still is not Ready, the problem is elsewhere (ingress, networking, or a component readiness condition).
2. If it is `InProgress`, the target model is still loading; watch `modelRevisionStates.targetModelState` move toward `Loaded`.
3. If it is `BlockedByFailedLoad` or `InvalidSpec`, read `lastFailureInfo.reason` and `lastFailureInfo.message`, then act on the table above.

```bash
# Inspect the full model status block
kubectl get inferenceservice llama-chat -o jsonpath='{.status.modelStatus}' | jq

# Or view it inline
kubectl get inferenceservice llama-chat -o yaml | grep -A 20 "modelStatus:"
```

## Runtime Pinning

By default `spec.runtime.autoSync` is `true`, so OME re-renders each component's pod spec from the **live** ServingRuntime on every reconcile. Setting `spec.runtime.autoSync: false` pins the InferenceService to a `ControllerRevision` snapshot of the runtime (surfaced as `status.pinnedRevisionName`), so later edits to the runtime do not roll out until you opt in. You then roll forward by bumping the `ome.io/runtime-sync` annotation or by setting `spec.runtime.revision` to a specific snapshot.

For the complete pinning, roll-forward, and rollback workflow, see [Runtime Revisions](/ome/docs/concepts/runtime-revision).

## Deployment Mode Selection

Choose the appropriate deployment mode based on your requirements:

| Requirement                         | Recommended Mode |
|-------------------------------------|------------------|
| Stable, predictable load            | Raw Deployment   |
| No cold starts                      | Raw Deployment   |
| Large model requiring multiple GPUs | Multi-Node       |
| Distributed inference               | Multi-Node       |
| Maximum performance                 | Multi-Node       |

## Best Practices

### Resource Management

1. **GPU Allocation**: Always specify GPU resources explicitly
```yaml
runner:
  resources:
    requests:
      nvidia.com/gpu: "1"
    limits:
      nvidia.com/gpu: "1"
```

2. **Memory Sizing**: Allow 2-4x model size for memory
```yaml
runner:
  resources:
    requests:
      memory: "32Gi"  # For 8B parameter model
```

3. **CPU Allocation**: Provide adequate CPU for preprocessing
```yaml
runner:
  resources:
    requests:
      cpu: "4"
```

### Scaling Configuration

1. **Set Appropriate Limits**:
```yaml
engine:
  minReplicas: 1     # Prevent scale-to-zero for latency
  maxReplicas: 10    # Control costs
  scaleTarget: 70    # 70% CPU utilization target
```

2. **Use KEDA for Custom Metrics**:
```yaml
kedaConfig:
  enableKeda: true
  customPromQuery: "avg_over_time(vllm:request_latency_seconds{service='%s'}[5m])"
  scalingThreshold: "0.5"  # 500ms latency threshold
```

### Troubleshooting

1. **Check Component Status**:
```bash
kubectl get inferenceservice llama-chat -o yaml
kubectl describe inferenceservice llama-chat
```

2. **Monitor Pod Logs**:
```bash
kubectl logs -l serving.ome.io/inferenceservice=llama-chat
```

3. **Check Resource Usage**:
```bash
kubectl top pods -l serving.ome.io/inferenceservice=llama-chat
```
