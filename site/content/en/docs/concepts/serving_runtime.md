---
title: "Serving Runtime"
date: 2023-03-14
weight: 25
description: >
  Cluster Serving Runtime is a cluster-scoped resource that manages the runtime environment for model serving.
---
The only difference between the two is that one is namespace-scoped and the other is cluster-scoped.

A _ClusterServingRuntime_ defines the templates for Pods that can serve one or more particular model.
Each ClusterServingRuntime defines key information such as the container image of the runtime and a list of the models that the runtime supports.
Other configuration settings for the runtime can be conveyed through environment variables in the container specification.

These CRDs allow for improved flexibility and extensibility, enabling users to quickly define or customize reusable runtimes without having to modify
any controller code or any resources in the controller namespace.

The following is an example of a ClusterServingRuntime:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-mistral-7b-instruct
spec:
  supportedModelFormats:
    - name: safetensors
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.36.2"
      modelArchitecture: MistralForCausalLM
      autoSelect: true
      priority: 1
  protocolVersions:
    - openAI
  modelSizeRange:
    max: 9B
    min: 5B
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
      resources:
        requests:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
        limits:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
    minReplicas: 1
    maxReplicas: 3
```

Several out-of-the-box _ClusterServingRuntimes_ are provided with OME so that users can quickly deploy common models without having to define the runtimes themselves.

### SGLang Runtimes

> **Note:** SGLang is our flagship supporting runtime, offering the latest serving engine with the most optimal performance. It provides cutting-edge features including multi-node serving capabilities, prefill-decode disaggregated serving, and Large-scale Cross-node Expert Parallelism (EP) for optimal performance at scale.

| Name                                         | Model Framework | Model Format | Model Architecture             |
|----------------------------------------------|-----------------|--------------|--------------------------------|
| deepseek-rdma-pd-rt                          | transformers    | safetensors  | DeepseekV3ForCausalLM          |
| deepseek-rdma-rt                             | transformers    | safetensors  | DeepseekV3ForCausalLM          |
| e5-mistral-7b-instruct-rt                    | transformers    | safetensors  | MistralModel                   |
| llama-3-1-70b-instruct-pd-rt                 | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-1-70b-instruct-rt                    | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-11b-vision-instruct-rt             | transformers    | safetensors  | MllamaForConditionalGeneration |
| llama-3-2-1b-instruct-pd-rt                  | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-1b-instruct-rt                     | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-3b-instruct-pd-rt                  | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-3b-instruct-rt                     | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-90b-vision-instruct-rt             | transformers    | safetensors  | MllamaForConditionalGeneration |
| llama-3-3-70b-instruct-pd-rt                 | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-3-70b-instruct-rt                    | transformers    | safetensors  | LlamaForCausalLM               |
| llama-4-maverick-17b-128e-instruct-fp8-pd-rt | transformers    | safetensors  | Llama4ForConditionalGeneration |
| llama-4-maverick-17b-128e-instruct-fp8-rt    | transformers    | safetensors  | Llama4ForConditionalGeneration |
| llama-4-scout-17b-16e-instruct-pd-rt         | transformers    | safetensors  | Llama4ForConditionalGeneration |
| llama-4-scout-17b-16e-instruct-rt            | transformers    | safetensors  | Llama4ForConditionalGeneration |
| mistral-7b-instruct-pd-rt                    | transformers    | safetensors  | MistralForCausalLM             |
| mistral-7b-instruct-rt                       | transformers    | safetensors  | MistralForCausalLM             |
| mixtral-8x7b-instruct-pd-rt                  | transformers    | safetensors  | MixtralForCausalLM             |
| mixtral-8x7b-instruct-rt                     | transformers    | safetensors  | MixtralForCausalLM             |

### VLLM Runtimes

| Name                                        | Model Framework | Model Format | Model Architecture             |
|---------------------------------------------|-----------------|--------------|--------------------------------|
| e5-mistral-7b-instruct-rt                   | transformers    | safetensors  | MistralModel                   |
| llama-3-1-405b-instruct-fp8-rt              | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-1-nemotron-nano-8b-v1-rt            | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-1-nemotron-ultra-253b-v1-rt         | transformers    | safetensors  | DeciLMForCausalLM              |
| llama-3-2-11b-vision-instruct-rt            | transformers    | safetensors  | MllamaForConditionalGeneration |
| llama-3-2-1b-instruct-rt                    | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-2-3b-instruct-rt                    | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-3-70b-instruct-rt                   | transformers    | safetensors  | LlamaForCausalLM               |
| llama-3-3-nemotron-super-49b-v1-rt          | transformers    | safetensors  | DeciLMForCausalLM              |
| llama-4-maverick-17b-128e-instruct-fp8-rt   | transformers    | safetensors  | Llama4ForConditionalGeneration |
| llama-4-scout-17b-16e-instruct-rt           | transformers    | safetensors  | Llama4ForConditionalGeneration |
| mistral-7b-instruct-rt                      | transformers    | safetensors  | MistralForCausalLM             |
| mixtral-8x7b-instruct-rt                    | transformers    | safetensors  | MixtralForCausalLM             |

## Spec Attributes

Available attributes in the `ServingRuntime` spec:

### Core Configuration

| Attribute                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                          |
|---------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `disabled`                                        | Disables this runtime                                                                                                                                                                                                                                                                                                                                                                                                |
| `supportedModelFormats`                           | List of model format, architecture, and type supported by the current runtime                                                                                                                                                                                                                                                                                                                                        |
| `supportedModelFormats[ ].name`                   | Name of the model format (deprecated, use `modelFormat.name` instead)                                                                                                                                                                                                                                                                                                                                                |
| `supportedModelFormats[ ].modelFormat`            | ModelFormat specification including name and version                                                                                                                                                                                                                                                                                                                                                                 |
| `supportedModelFormats[ ].modelFormat.name`       | Name of the model format, e.g., "safetensors", "ONNX", "TensorFlow SavedModel"                                                                                                                                                                                                                                                                                                                                       |
| `supportedModelFormats[ ].modelFormat.version`    | Version of the model format. Used in validating that a runtime supports a model. It Can be "major", "major.minor" or "major.minor.patch"                                                                                                                                                                                                                                                                             |
| `supportedModelFormats[ ].modelFramework`         | ModelFramework specification including name and version                                                                                                                                                                                                                                                                                                                                                              |
| `supportedModelFormats[ ].modelFramework.name`    | Name of the library, e.g., "transformer", "TensorFlow", "PyTorch", "ONNX", "TensorRTLLM"                                                                                                                                                                                                                                                                                                                             |
| `supportedModelFormats[ ].modelFramework.version` | Version of the framework library                                                                                                                                                                                                                                                                                                                                                                                     |
| `supportedModelFormats[ ].modelArchitecture`      | Name of the model architecture, used in validating that a model is supported by a runtime, e.g., "LlamaForCausalLM", "GemmaForCausalLM"                                                                                                                                                                                                                                                                              |
| `supportedModelFormats[ ].quantization`           | Quantization scheme applied to the model, e.g., "fp8", "fbgemm_fp8", "int4"                                                                                                                                                                                                                                                                                                                                          |
| `supportedModelFormats[ ].autoSelect`             | Set to true to allow the ServingRuntime to be used for automatic model placement if this model is specified with no explicit runtime. The default value is false.                                                                                                                                                                                                                                                    |
| `supportedModelFormats[ ].priority`               | Priority of this serving runtime for auto selection. This is used to select the serving runtime if more than one serving runtime supports the same model format. <br/>The value should be greater than zero. The higher the value, the higher the priority. Priority is not considered if AutoSelect is either false or not specified. Priority can be overridden by specifying the runtime in the InferenceService. |
| `protocolVersions`                                | Supported protocol versions (i.e. openAI or cohere or openInference-v1 or openInference-v2)                                                                                                                                                                                                                                                                                                                          |
| `modelSizeRange`                                  | Model size range is the range of model sizes supported by this runtime                                                                                                                                                                                                                                                                                                                                               |
| `modelSizeRange.min`                              | Minimum size of the model in bytes                                                                                                                                                                                                                                                                                                                                                                                   |
| `modelSizeRange.max`                              | Maximum size of the model in bytes                                                                                                                                                                                                                                                                                                                                                                                   |
| `acceleratorRequirements`                         | Accelerator (GPU) requirements this runtime needs (see [Accelerator Requirements](#accelerator-requirements))                                                                                                                                                                                                                                                                                                        |

### Accelerator Requirements

A runtime can declare the accelerators it supports and the minimum hardware it needs through the optional `acceleratorRequirements` field on the `ServingRuntimeSpec`. This lets OME match a runtime against the accelerators available in the cluster (and against an InferenceService's `acceleratorSelector`) when selecting where to place a workload.

| Field                                    | Type     | Description                                                                                          |
|------------------------------------------|----------|------------------------------------------------------------------------------------------------------|
| `acceleratorRequirements.acceleratorClasses`          | []string | Names of the `AcceleratorClass` resources this runtime supports                                     |
| `acceleratorRequirements.minMemory`                   | int64    | Minimum accelerator memory required, in GB                                                          |
| `acceleratorRequirements.minComputePerformanceTFLOPS` | int64    | Minimum compute capability required, in TFLOPS                                                      |
| `acceleratorRequirements.minArchitectureVersion`      | string   | Minimum architecture version (NVIDIA compute capability or equivalent)                              |
| `acceleratorRequirements.requiredFeatures`            | []string | Hardware features that must be present, e.g., `["tensor-cores", "fp8", "nvlink"]`                   |
| `acceleratorRequirements.preferredPrecisions`         | []string | Numeric precisions in order of preference, e.g., `["fp8", "fp16", "fp32"]`                          |

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-llama-fp8
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
      modelArchitecture: LlamaForCausalLM
      quantization: fp8
      autoSelect: true
  acceleratorRequirements:
    acceleratorClasses:
      - nvidia-h100
      - nvidia-h200
    minMemory: 80
    requiredFeatures:
      - tensor-cores
      - fp8
    preferredPrecisions:
      - fp8
      - fp16
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
```

### Per-Accelerator Model Configuration

The requirements above apply to the whole runtime. In addition, an individual entry in `supportedModelFormats` may carry per-accelerator overrides through its `acceleratorConfig` map. This is useful when the same model format needs different settings depending on which accelerator class it lands on - for example a larger GPU may run with a smaller tensor-parallel size, or a specific accelerator may need extra runtime flags or environment variables.

`acceleratorConfig` is keyed by accelerator class name; each value is an `AcceleratorModelConfig`:

| Field                                          | Type              | Description                                                                                                   |
|------------------------------------------------|-------------------|---------------------------------------------------------------------------------------------------------------|
| `minMemoryPerBillionParams`                    | int64             | Memory required per billion parameters (bytes). Used to compute whether the model fits on the accelerator     |
| `tensorParallelismOverride`                    | TensorParallelismConfig | Overrides the default parallelism settings (see below)                                                   |
| `runtimeArgsOverride`                          | []string          | Accelerator-specific runtime arguments, merged into the runner container's args                              |
| `environmentOverride`                          | map[string]string | Accelerator-specific environment variables, applied to the runner container                                  |

`tensorParallelismOverride` is a `TensorParallelismConfig`:

| Field                  | Type  | Description                          |
|------------------------|-------|--------------------------------------|
| `tensorParallelSize`   | int64 | Size of the tensor parallelism       |
| `pipelineParallelSize` | int64 | Size of the pipeline parallelism     |
| `dataParallelSize`     | int64 | Size of the data parallelism         |

When a model is placed on a given accelerator class, OME looks up the matching entry in `acceleratorConfig` and applies it: `environmentOverride` values are merged into the runner container's environment, `runtimeArgsOverride` entries are merged into its args, and any `tensorParallelismOverride` sizes are translated into the runtime's own flags (for example `--tp-size`/`--tensor-parallel-size` for the tensor-parallel size, `--pp-size`/`--pipeline-parallel-size` for pipeline parallelism, and `--dp-size`/`--data-parallel-size` for data parallelism). User-provided values on the InferenceService runner take precedence over these overrides.

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-llama-multi-accelerator
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
      modelArchitecture: LlamaForCausalLM
      autoSelect: true
      acceleratorConfig:
        nvidia-a100-80gb:
          minMemoryPerBillionParams: 2000000000
          tensorParallelismOverride:
            tensorParallelSize: 4
          runtimeArgsOverride:
            - --mem-fraction-static=0.9
          environmentOverride:
            NCCL_P2P_LEVEL: NVL
        nvidia-h100:
          tensorParallelismOverride:
            tensorParallelSize: 2
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
```

### Component Configuration

The ServingRuntime spec supports three main component configurations:

#### Engine Configuration

| Attribute                       | Description                                                                                                                         |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `engineConfig`                  | Engine configuration for model serving                                                                                              |
| `engineConfig.runner`           | Container specification for the main engine container                                                                               |
| `engineConfig.runner.image`     | Container image for the engine                                                                                                      |
| `engineConfig.runner.resources` | Kubernetes [limits or requests](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits) |
| `engineConfig.runner.env`       | List of environment variables to pass to the container                                                                              |
| `engineConfig.minReplicas`      | Minimum number of replicas, defaults to 1 but can be set to 0 to enable scale-to-zero                                               |
| `engineConfig.maxReplicas`      | Maximum number of replicas for autoscaling                                                                                          |
| `engineConfig.scaleTarget`      | Integer target value for the autoscaler metric                                                                                      |
| `engineConfig.scaleMetric`      | Scaling metric type (concurrency, rps, cpu, memory)                                                                                 |
| `engineConfig.volumes`          | List of volumes that can be mounted by containers                                                                                   |
| `engineConfig.nodeSelector`     | Node selector for pod scheduling                                                                                                    |
| `engineConfig.affinity`         | Affinity rules for pod scheduling                                                                                                   |
| `engineConfig.tolerations`      | Tolerations for pod scheduling                                                                                                      |
| `engineConfig.leader`           | Leader configuration for multi-node deployments                                                                                     |
| `engineConfig.worker`           | Worker configuration for multi-node deployments                                                                                     |

#### Router Configuration

| Attribute                  | Description                                        |
|----------------------------|----------------------------------------------------|
| `routerConfig`             | Router configuration for request routing           |
| `routerConfig.runner`      | Container specification for the router container   |
| `routerConfig.config`      | Additional configuration parameters for the router |
| `routerConfig.minReplicas` | Minimum number of router replicas                  |
| `routerConfig.maxReplicas` | Maximum number of router replicas                  |

#### Decoder Configuration

| Attribute                   | Description                                                             |
|-----------------------------|-------------------------------------------------------------------------|
| `decoderConfig`             | Decoder configuration for PD (Prefill-Decode) disaggregated deployments |
| `decoderConfig.runner`      | Container specification for the decoder container                       |
| `decoderConfig.minReplicas` | Minimum number of decoder replicas                                      |
| `decoderConfig.maxReplicas` | Maximum number of decoder replicas                                      |
| `decoderConfig.leader`      | Leader configuration for multi-node decoder deployments                 |
| `decoderConfig.worker`      | Worker configuration for multi-node decoder deployments                 |

#### Multi-Node Configuration

For both `engineConfig` and `decoderConfig`, multi-node deployments are supported:

| Attribute       | Description                                                       |
|-----------------|-------------------------------------------------------------------|
| `leader`        | Leader node configuration for coordinating distributed processing |
| `leader.runner` | Container specification for the leader node                       |
| `worker`        | Worker nodes configuration for distributed processing             |
| `worker.size`   | Number of worker pod instances                                    |
| `worker.runner` | Container specification for worker nodes                          |


>**Note:** `ClusterServingRuntime` support the use of template variables of the form `{{.Variable}}` inside the container spec. These should map to fields inside an
InferenceService's [metadata object](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta). The primary use of this is for passing in
InferenceService-specific information, such as a name, to the runtime environment.

## Using ClusterServingRuntimes

When users reference a model in their InferenceService, they can explicitly specify the name of a _ClusterServingRuntime_ or _ServingRuntime_. For example:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: mistral-7b-instruct
  namespace: mistral-7b-instruct
spec:
  engine:
    minReplicas: 1
    maxReplicas: 1
  model:
    name: mistral-7b-instruct
  runtime:
    name: srt-mistral-7b-instruct
```

Here, the runtime specified is `srt-mistral-7b-instruct`. The optional `spec.runtime.kind` field scopes how that name is resolved, and it defaults to
`ClusterServingRuntime`: the OME controller first looks for a ClusterServingRuntime with that name, and only if none exists does it fall back to a
ServingRuntime in the InferenceService's namespace. So if a namespaced ServingRuntime and a ClusterServingRuntime share the same name, a bare `name:`
reference resolves the ClusterServingRuntime. To select the namespaced runtime instead, set `kind: ServingRuntime`; that reference resolves only in the
InferenceService's namespace and never falls back to cluster scope. See
[Reference a Serving Runtime Explicitly](/ome/docs/tasks/run-workloads/reference-a-runtime-explicitly/) for the full resolution and validation behavior.

Users can also implicitly specify the runtime by setting the `autoSelect` field to `true` in the `supportedModelFormats` field of the _ClusterServingRuntime_.
```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: mistral-7b-instruct
  namespace: mistral-7b-instruct
spec:
  engine:
    minReplicas: 1
    maxReplicas: 1
  model:
    name: mistral-7b-instruct
```

## Runtime Selection Logic

The OME controller uses an enhanced runtime selection algorithm to automatically choose the best runtime for a given model. The selection process includes several steps:

### Runtime Discovery

The controller searches for compatible runtimes in the following order:
1. **Namespace-scoped ServingRuntimes** in the same namespace as the InferenceService
2. **Cluster-scoped ClusterServingRuntimes** available across the cluster

### Enabled Status

The runtime must not be disabled. Runtimes can be disabled by setting the `disabled` field to `true` in the ServingRuntime spec.

### Model Format Support

The runtime must support the model's complete format specification, which includes several components:

- **Model Format**: The storage format of the model (e.g., "safetensors", "ONNX", "TensorFlow SavedModel")
- **Model Format Version**: The version of the model format (e.g., "1", "2.0")
- **Model Framework**: The underlying framework or library (e.g., "transformer", "TensorFlow", "PyTorch", "ONNX", "TensorRTLLM")
- **Model Framework Version**: The version of the framework library (e.g., "4.0", "2.1")
- **Model Architecture**: The specific model implementation (e.g., "LlamaForCausalLM", "GemmaForCausalLM", "MistralForCausalLM")
- **Quantization**: The quantization scheme applied to the model (e.g., "fp8", "fbgemm_fp8", "int4")

All these attributes must match between the model and the runtime's `supportedModelFormats` for the runtime to be considered compatible.

### Model Size Range

The `modelSizeRange` field defines the minimum and maximum model sizes that the runtime can support. This field is optional, but when provided, it helps the controller identify a runtime that matches the model size within the specified range. If multiple runtimes meet the size requirement, the controller will choose the runtime with the range closest to the model size.

### Protocol Version Support

The runtime must support the requested protocol version. Protocol versions include:
- `openAI`: OpenAI-compatible API format
- `openInference-v1`: Open Inference Protocol version 1
- `openInference-v2`: Open Inference Protocol version 2

If no protocol version is specified in the InferenceService, the controller defaults to `openAI`.

### Auto-Selection

The runtime must have `autoSelect` enabled for at least one supported format. This ensures that only runtimes explicitly marked for automatic selection are considered during the selection process.

### Priority

If more than one serving runtime supports the same model `architecture`, `format`, `framework`, `quantization`,
and `size range` with same `version`, then we can optionally specify `priority` for the serving runtime.
Based on the `priority` the runtime is automatically selected if no runtime is explicitly specified. Note that, `priority` is valid only if `autoSelect` is `true`. Higher value means higher priority.

For example, let's consider the serving runtimes `srt-mistral-7b-instruct` and `srt-mistral-7b-instruct-2`.
Both the serving runtimes support the `MistralForCausalLM` model architecture,
`transformers` model framework, `safetensors` model format, version `1` and both supports
the `protocolVersion` openAI. Also note that `autoSelect` is enabled in both the serving runtimes.

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-mistral-7b-instruct
spec:
  supportedModelFormats:
    - name: safetensors
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.36.2"
      modelArchitecture: MistralForCausalLM
      autoSelect: true
      priority: 1
  protocolVersions:
    - openAI
  modelSizeRange:
    max: 9B
    min: 5B
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
      resources:
        requests:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
        limits:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
    minReplicas: 1
    maxReplicas: 3
```

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-mistral-7b-instruct-2
spec:
  supportedModelFormats:
    - name: safetensors
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.36.2"
      modelArchitecture: MistralForCausalLM
      autoSelect: true
      priority: 2
  protocolVersions:
    - openAI
  modelSizeRange:
    max: 9B
    min: 5B
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
      resources:
        requests:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
        limits:
          cpu: 10
          memory: 30Gi
          nvidia.com/gpu: 2
```

**Constraints of priority**

- The higher priority value means higher precedence. The value must be greater than 0.
- The priority is valid only if auto select is enabled otherwise the priority is not considered.
- The serving runtime with priority takes precedence over the serving runtime with priority not specified.
- Two support model formats with the same name and the same version cannot have the same priority.
- If more than one serving runtime supports the model format and none of them specified the priority then, there is no guarantee _which_ runtime will be selected.
- If a serving runtime supports multiple versions of a models, then it should have the same priority.

> **⚠️ WARNING**: If multiple runtimes list the same format and/or version as auto-selectable and the priority is not specified, the runtime is selected based on the `creationTimestamp` i.e. the most recently created runtime is selected. So there is no guarantee _which_ runtime will be selected. So users and cluster-administrators should enable `autoSelect` with care.
