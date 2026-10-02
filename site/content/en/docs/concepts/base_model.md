---
title: "Base Model"
date: 2023-03-14
weight: 5
description: >
  Base Model defines foundation models that can be automatically downloaded, parsed, and served across your cluster.
---

## What is a Base Model?

A Base Model in OME is a Kubernetes resource that represents a foundation AI model (like GPT, Llama, or Mistral) that you want to use for inference workloads. Think of it as a blueprint that tells OME where to find your model, how to download it, and where to store it on your cluster nodes.

When you create a BaseModel resource, OME automatically handles the complex process of downloading the model files, parsing the model's configuration to understand its capabilities, and making it available across your cluster nodes where AI workloads can use it.

## BaseModel vs ClusterBaseModel

OME provides two types of model resources:

**BaseModel** is namespace-scoped, meaning it exists within a specific Kubernetes namespace. If you create a BaseModel in the "team-a" namespace, only workloads in that namespace can use it. This is perfect for team-specific models or when you want to isolate model access.

**ClusterBaseModel** is cluster-scoped, meaning it's available to workloads in any namespace across your entire cluster. This is ideal for organization-wide models that multiple teams need to access, like a shared Llama-3 model that everyone uses.

Both types use exactly the same specification format - the only difference is their visibility scope.

## Basic Example

Here's a simple BaseModel to get you started:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-70b-instruct
spec:
  vendor: meta
  version: "3.1"
  disabled: false
  modelType: llama
  modelArchitecture: LlamaForCausalLM
  modelParameterSize: "70B"
  maxTokens: 8192
  modelCapabilities:
    - text-to-text
  modelFormat:
    name: safetensors
    version: "1.0.0"
  modelFramework:
    name: transformers
    version: "4.36.0"
  storage:
    storageUri: oci://n/ai-models/b/llm-store/o/meta/llama-3.1-70b-instruct/
    path: /raid/models/llama-3.1-70b-instruct
    storageKey: oci-credentials
    parameters:
      region: us-phoenix-1
      auth_type: InstancePrincipal
    nodeSelector:
      node.kubernetes.io/instance-type: GPU.A100.4
```

## Specification Reference

Available attributes in the BaseModel/ClusterBaseModel spec:

| Attribute                      | Type              | Description                                                              |
|--------------------------------|-------------------|--------------------------------------------------------------------------|
| **Core Configuration**         |                   |                                                                          |
| `vendor`                       | string            | Vendor of the model (e.g., "meta", "mistral", "openai")                  |
| `version`                      | string            | Version of the model (e.g., "3.1", "1.0")                                |
| `disabled`                     | boolean           | Whether the model is disabled. Defaults to false                         |
| `displayName`                  | string            | User-friendly name of the model                                          |
| **Model Identification**       |                   |                                                                          |
| `modelType`                    | string            | Architecture family (e.g., "llama", "mistral", "deepseek_v3")            |
| `modelArchitecture`            | string            | Specific implementation (e.g., "LlamaForCausalLM", "MistralForCausalLM") |
| `modelParameterSize`           | string            | Human-readable parameter count (e.g., "7B", "70B", "405B")               |
| `maxTokens`                    | int32             | Maximum number of tokens the model can process                           |
| `modelCapabilities`            | []string          | Model capabilities (see [Model Capabilities](#model-capabilities))       |
| `apiCapabilities`              | []string          | OpenAI-style API endpoints the model serves (see [API Capabilities](#api-capabilities)) |
| **Model Format and Framework** |                   |                                                                          |
| `modelFormat.name`             | string            | Format name (e.g., "safetensors", "onnx", "pytorch")                     |
| `modelFormat.version`          | string            | Format version (e.g., "1", "2.0")                                        |
| `modelFramework.name`          | string            | Framework name (e.g., "transformers", "onnx", "tensorrt")                |
| `modelFramework.version`       | string            | Framework version (e.g., "4.36.0", "1.14.0")                             |
| `quantization`                 | string            | Quantization scheme (see [Quantization Types](#quantization-types))      |
| **Storage Configuration**      |                   |                                                                          |
| `storage.storageUri`           | string            | Source URI of the model                                                  |
| `storage.path`                 | string            | Local path where model will be stored on nodes                           |
| `storage.schemaPath`           | string            | Path to model schema or configuration within storage                     |
| `storage.storageKey`           | string            | Name of Kubernetes Secret containing storage credentials                 |
| `storage.parameters`           | map[string]string | Storage-specific parameters (region, auth_type, etc.)                    |
| `storage.nodeSelector`         | map[string]string | Node labels that must match for model placement                          |
| `storage.nodeAffinity`         | NodeAffinity      | Advanced node selection rules                                            |
| **Serving Configuration**      |                   |                                                                          |
| `modelConfiguration`           | RawExtension      | Model-specific configuration as JSON                                     |
| `diffusionPipeline`            | DiffusionPipelineSpec | Pipeline metadata for diffusion models (see [Diffusion Models](#diffusion-models)) |
| `additionalMetadata`           | map[string]string | Additional key-value metadata                                            |

## Storage Backends

OME supports multiple storage backends to work with your existing infrastructure:

### Cloud Object Storage

Store your models in OCI Object Storage using this URI format:
```
oci://n/{namespace}/b/{bucket}/o/{object_path}
```

Example:
```yaml
storage:
  storageUri: "oci://n/mycompany/b/ai-models/o/llama/llama-3-70b/"
  path: "/raid/models/llama-3-70b-instruct"
  parameters:
    region: "us-phoenix-1"
    auth_type: "InstancePrincipal"
```

### CNCF ModelPack (OCI registry)

Pull a [CNCF ModelPack](https://github.com/modelpack/model-spec) artifact from a container registry:
```
modelpack://{registry}/{repository}[:{tag}|@{digest}]
```

Example:
```yaml
storage:
  storageUri: "modelpack://ghcr.io/myorg/llama-3-70b:v1"
  path: "/raid/models/llama-3-70b-instruct"
```

The model agent pulls the artifact through a running [`llmman serve`](https://github.com/llmmanorg/llmman)
daemon. The daemon address comes from `LLMMAN_HOST` (default `127.0.0.1:17434`), and the `llmman` binary
must be on `PATH` (or set `OME_LLMMAN_BIN`). Neither is needed unless a `modelpack://` URI is used.

> **Note**: `oci://` is unchanged and still addresses Oracle Cloud Object Storage.

### Hugging Face Hub

Download models directly from Hugging Face Hub:
```
hf://{model-id}[@{branch}]
```

Example:
```yaml
storage:
  storageUri: "hf://meta-llama/Llama-3.3-70B-Instruct"
  path: "/models/llama-3.3-70b"
  storageKey: "huggingface-token"
```

#### Hugging Face Parameters

| Parameter   | Description              | Example         |
|-------------|--------------------------|-----------------|
| `revision`  | Git revision to download | `main`, `v1.0`  |
| `cache_dir` | Local cache directory    | `/tmp/hf_cache` |

### Persistent Volume Claims (PVC)

Reference models already stored in Kubernetes persistent volumes:
```
pvc://[{namespace}:]{pvc-name}/{sub-path}
```

Example:
```yaml
storage:
  storageUri: "pvc://model-storage/llama-models/llama-3-70b"
  path: "/local/models/llama-3-70b"
```

> **Note**: For BaseModel resources, if no namespace is specified, the PVC is assumed to be in the same namespace as the BaseModel. For ClusterBaseModel resources, you must specify the namespace explicitly using the colon separator format: `pvc://namespace:pvc-name/path`.

### Vendor Storage

For proprietary or vendor-specific storage systems:
```
vendor://{vendor-name}/{resource-type}/{resource-path}
```

Example:
```yaml
storage:
  storageUri: "vendor://nvidia/models/llama-70b-tensorrt"
  path: "/opt/models/llama-70b-tensorrt"
```

## Node Selection

Control which nodes download and store your models using node selectors and affinity rules:

### Simple Node Selection
```yaml
storage:
  storageUri: "oci://n/mycompany/b/models/o/llama-70b/"
  nodeSelector:
    node.kubernetes.io/instance-type: "GPU.A100.4"
    models.ome.io/storage-tier: "fast-ssd"
```

### Advanced Node Affinity

The `nodeAffinity` field supports standard Kubernetes node affinity with these operators:

| Operator       | Description                              |
|----------------|------------------------------------------|
| `In`           | Node label value must be in the list     |
| `NotIn`        | Node label value must not be in the list |
| `Exists`       | Node must have the label key             |
| `DoesNotExist` | Node must not have the label key         |
| `Gt`           | Numeric value must be greater than       |
| `Lt`           | Numeric value must be less than          |

Example:
```yaml
storage:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: "node.kubernetes.io/instance-type"
          operator: In
          values: ["GPU.A100.4", "GPU.H100.8"]
        - key: "models.ome.io/available-storage"
          operator: Gt
          values: ["500Gi"]
```

## Automatic Model Discovery

OME automatically analyzes your models to extract important metadata. When the Model Agent downloads a model, it looks for a `config.json` file and uses specialized parsers for different model architectures.

### Supported Model Types

OME currently supports automatic parsing for:

- **Llama Family Models** (Llama 3, 3.1, 3.2, and 4)
- **DeepSeek Models** (including DeepSeek V3 with MoE architecture)
- **Mistral and Mixtral** (standard and mixture-of-experts models)
- **Microsoft Phi Models** (including Phi-3 Vision for multimodal)
- **Qwen Models** (Qwen2 family)
- **Multimodal Models** (MLlama Vision models)

### What Gets Detected

The system automatically determines:
- **Model Type**: Architecture family (e.g., "llama", "mistral")
- **Model Architecture**: Specific implementation (e.g., "LlamaForCausalLM")
- **Parameter Count**: Total number of parameters
- **Context Length**: Maximum input context length
- **Framework Information**: AI framework and version
- **Data Type**: Model precision (float16, bfloat16, etc.)
- **Capabilities**: What the model can do (text generation, embeddings, vision)

### Quantization Types

The `quantization` field records the quantization scheme of the model weights as they are stored on disk. It is **metadata** that the model parser extracts from the model's configuration (for example `quant_method` in `config.json` or `hf_quant_config.json`) - OME does not quantize or re-quantize your model. The value is informational and is used during [runtime selection](/ome/docs/concepts/serving_runtime) so that a compatible runtime (one whose `supportedModelFormats` declares the same quantization) can be chosen.

Valid values for `quantization`:

| Type         | Description                                                                                                            |
|--------------|----------------------------------------------------------------------------------------------------------------------|
| `fp8`        | 8-bit floating point quantization                                                                                     |
| `fbgemm_fp8` | Facebook GEMM FP8 quantization                                                                                        |
| `int4`       | 4-bit integer quantization                                                                                            |
| `nvfp4`      | NVIDIA FP4, produced by ModelOpt's NVFP4 PTQ pipeline (two 4-bit values packed per byte, with FP8 per-group scales)  |
| `mxfp4`      | OCP Microscaling FP4, used by the OpenAI gpt-oss family (declared via `quant_method=mxfp4` in `config.json`)          |
| `compressed-tensors` | The compressed-tensors container format (vLLM / llm-compressor). Unlike the others this names a *format*, not a single precision - the actual bit width (FP8, INT8, INT4, ...) is declared per group in `quantization_config.config_groups`. |

### Model Capabilities

The `modelCapabilities` field describes what a model can do. Capabilities are detected automatically by the model parser where possible, and can also be set explicitly in the spec. They are used for filtering, display, and to help match a model to an appropriate runtime.

Two generations of values exist. The **legacy** values remain accepted for backward compatibility, while the **standardized** values (which describe an input-to-output transformation) are the preferred naming going forward.

Legacy capabilities:

| Capability          | Description                          |
|---------------------|--------------------------------------|
| `TEXT_GENERATION`   | Generates text from a text prompt    |
| `TEXT_SUMMARIZATION`| Summarizes input text                |
| `TEXT_EMBEDDINGS`   | Produces text embedding vectors      |
| `TEXT_RERANK`       | Reranks candidate documents          |
| `CHAT`              | Conversational / chat interaction    |
| `VISION`            | Understands image inputs             |

Standardized capabilities (preferred):

| Capability             | Description                                             |
|------------------------|---------------------------------------------------------|
| `EMBEDDING`            | Produces embedding vectors                               |
| `RERANK`               | Reranks candidate documents                             |
| `TEXT_TO_TEXT`         | Text input to text output (e.g., chat, completion)      |
| `TEXT_TO_AUDIO`        | Text input to audio output (e.g., text-to-speech)       |
| `TEXT_TO_IMAGE`        | Text input to image output                              |
| `TEXT_TO_VIDEO`        | Text input to video output                              |
| `IMAGE_TEXT_TO_TEXT`   | Image plus text input to text output (vision-language)  |
| `IMAGE_TEXT_TO_AUDIO`  | Image plus text input to audio output                   |
| `IMAGE_TEXT_TO_IMAGE`  | Image plus text input to image output                   |
| `IMAGE_TEXT_TO_VIDEO`  | Image plus text input to video output                   |
| `VIDEO_TEXT_TO_AUDIO`  | Video plus text input to audio output                   |
| `AUDIO_TO_TEXT`        | Audio input to text output (e.g., transcription)        |
| `AUDIO_TO_AUDIO`       | Audio input to audio output                             |
| `AUDIO_TEXT_TO_TEXT`   | Audio plus text input to text output                    |
| `AUDIO_TRANSLATION`    | Translates audio from one language to another           |

### API Capabilities

While `modelCapabilities` describes the kind of task a model performs, the separate `apiCapabilities` field lists the OpenAI-style API endpoints the model is able to serve. This lets clients and routers discover exactly which API surfaces are available for a given model.

| API Capability                   | Endpoint                        |
|----------------------------------|---------------------------------|
| `OPENAI_V1_CHAT_COMPLETIONS`     | `/v1/chat/completions`          |
| `OPENAI_V1_RESPONSES`            | `/v1/responses`                 |
| `OPENAI_V1_EMBEDDINGS`           | `/v1/embeddings`                |
| `OPENAI_V1_IMAGES_GENERATIONS`   | `/v1/images/generations`        |
| `OPENAI_V1_IMAGES_EDITS`         | `/v1/images/edits`              |
| `OPENAI_V1_AUDIO_SPEECH`         | `/v1/audio/speech`              |
| `OPENAI_V1_AUDIO_TRANSCRIPTIONS` | `/v1/audio/transcriptions`      |
| `OPENAI_V1_AUDIO_TRANSLATIONS`   | `/v1/audio/translations`        |
| `OPENAI_V1_REALTIME`             | `/v1/realtime`                  |

Example:

```yaml
spec:
  modelCapabilities:
    - TEXT_TO_TEXT
  apiCapabilities:
    - OPENAI_V1_CHAT_COMPLETIONS
    - OPENAI_V1_RESPONSES
```

### Diffusion Models

For diffusion models, the optional `diffusionPipeline` field on the BaseModel spec captures pipeline-specific metadata mirrored from the model's `model_index.json`. This allows runtimes to validate that they can serve a given diffusion pipeline. Its fields map directly to the entries of a diffusers pipeline:

| Field                  | Description                                                                          |
|------------------------|--------------------------------------------------------------------------------------|
| `className`            | The pipeline implementation, e.g., `StableDiffusionXLPipeline` or `QwenImagePipeline`|
| `scheduler`            | The scheduler component (library + type)                                             |
| `textEncoder`          | The text-encoder component                                                           |
| `tokenizer`            | The tokenizer component                                                              |
| `transformer`          | The transformer (UNet/DiT) component                                                 |
| `vae`                  | The VAE component                                                                    |
| `additionalComponents` | Any other pipeline parts, keyed by their `model_index.json` entry                    |

Each component is described by a `library` (e.g., `diffusers` or `transformers`) and a `type` (the fully qualified class name, e.g., `FlowMatchEulerDiscreteScheduler`).

```yaml
spec:
  modelCapabilities:
    - TEXT_TO_IMAGE
  diffusionPipeline:
    className: StableDiffusionXLPipeline
    scheduler:
      library: diffusers
      type: EulerDiscreteScheduler
    vae:
      library: diffusers
      type: AutoencoderKL
```

### Disabling Automatic Parsing

If you need to specify model information manually:

```yaml
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: custom-model
  annotations:
    ome.io/skip-config-parsing: "true"
spec:
  modelType: "custom"
  modelArchitecture: "CustomForCausalLM"
  modelParameterSize: "70B"
  maxTokens: 4096
  modelCapabilities:
    - TEXT_GENERATION
```

## Model Status and Lifecycle

### Model States

Each model on each node goes through these states:

- **Ready**: Successfully downloaded and available for use
- **Updating**: Currently being downloaded or updated
- **Failed**: Download or validation failed
- **Deleted**: Removed from the node

### Status Fields

The BaseModel status contains these fields:

| Field | Type | Description |
|-------|------|-------------|
| `state` | string | Overall model state (Creating, Ready, Failed) |
| `lifecycle` | string | Lifecycle stage of the model |
| `nodesReady` | []string | List of nodes where model is ready |
| `nodesFailed` | []string | List of nodes where model failed |

Example status:
```yaml
status:
  state: Ready
  lifecycle: Ready
  nodesReady:
    - worker-node-1
    - worker-node-2
  nodesFailed: []
```

### Checking Model Status

View model status across your cluster:

```bash
# Check all models
kubectl get clusterbasemodels

# Check model status on specific nodes
kubectl get configmaps -n ome -l models.ome/basemodel-status=true

# Find nodes with a specific model ready
kubectl get nodes -l "models.ome/model-uid=Ready"
```

## Authentication

### OCI Authentication Methods

- **Instance Principal**: Uses the compute instance's identity (recommended for OCI)
- **User Principal**: Uses specific user credentials stored in secrets
- **Resource Principal**: For OKE with resource principals
- **OKE Workload Identity**: Service account-based authentication

### Hugging Face Authentication

For private or gated models, provide an access token:

```yaml
# Create a secret with your Hugging Face token
apiVersion: v1
kind: Secret
metadata:
  name: hf-token
data:
  token: <base64-encoded-token>
```

#### Using Custom Secret Key Names

By default, the Model Agent looks for a key named "token" in your secret. However, you can specify a custom key name using the `secretKey` parameter:

```yaml
# Create a secret with a custom key name
apiVersion: v1
kind: Secret
metadata:
  name: hf-credentials
data:
  access-token: <base64-encoded-token>

---
# Reference it in your BaseModel
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: private-model
spec:
  storage:
    storageUri: "hf://my-org/private-model"
    storageKey: "hf-credentials"
    parameters:
      secretKey: "access-token"  # Specify the custom key name
```

This is useful when:
- You have existing secrets with different key names
- You're following specific naming conventions in your organization
- You need to store multiple tokens in the same secret

## Complete Configuration Example

Here's a comprehensive BaseModel configuration showing all available options:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-70b-instruct
  labels:
    vendor: "meta"
    model-family: "llama"
    parameter-size: "70b"
  annotations:
    ome.io/skip-config-parsing: "false"
spec:
  # Basic model information
  vendor: "meta"
  version: "3.1"
  disabled: false
  displayName: "Llama 3.1 70B Instruct"

  # Model identification
  modelType: "llama"
  modelArchitecture: "LlamaForCausalLM"
  modelParameterSize: "70B"
  maxTokens: 8192
  modelCapabilities:
    - text-to-text

  # Model format and framework
  modelFormat:
    name: "safetensors"
    version: "1.0.0"
  modelFramework:
    name: "transformers"
    version: "4.36.0"
  quantization: "fp8"

  # Storage configuration
  storage:
    storageUri: "oci://n/ai-models/b/llm-store/o/meta/llama-3.1-70b-instruct/"
    path: "/raid/models/llama-3.1-70b-instruct"
    schemaPath: "config.json"
    storageKey: "oci-model-credentials"

    parameters:
      region: "us-phoenix-1"
      auth_type: "InstancePrincipal"

    # Target appropriate hardware
    nodeSelector:
      node.kubernetes.io/instance-type: "GPU.A100.4"
      models.ome.io/storage-tier: "nvme"

    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: "accelerator.nvidia.com/gpu-product"
            operator: In
            values: ["A100-SXM4-80GB", "H100-SXM5-80GB"]
          - key: "models.ome.io/available-storage"
            operator: Gt
            values: ["200Gi"]

  # Model-specific configuration
  modelConfiguration: |
    {
      "temperature": 0.7,
      "top_p": 0.9,
      "max_new_tokens": 2048
    }

  # Additional metadata
  additionalMetadata:
    license: "Llama 3.1 Community License"
    description: "Meta Llama 3.1 70B Instruct model for chat and instruction following"
    use_cases: "chat,assistant,instruction_following"
    cost_center: "ai-research"
    owner: "ml-platform-team"
```

## Fine-Tuned Models

### FineTunedWeight Specification

FineTunedWeight resources reference BaseModels and add fine-tuning specific configuration:

```yaml
apiVersion: ome.io/v1beta1
kind: FineTunedWeight
metadata:
  name: llama-70b-finance-lora
spec:
  # Reference to base model
  baseModelRef:
    name: llama-3-70b-instruct
    namespace: default

  # Fine-tuning configuration
  modelType: LoRA
  hyperParameters: |
    {
      "lora_rank": 16,
      "lora_alpha": 32,
      "learning_rate": 1e-4
    }

  # Storage for fine-tuned weights
  storage:
    storageUri: oci://n/mycompany/b/fine-tuned/o/llama-70b-finance-lora/
    path: /raid/fine-tuned/llama-70b-finance-lora

  # Training job reference
  trainingJobRef:
    name: llama-finance-training-job
    namespace: training
```

### FineTunedWeight Spec Attributes

| Attribute         | Type            | Description                                  |
|-------------------|-----------------|----------------------------------------------|
| `baseModelRef`    | ObjectReference | Reference to the base model                  |
| `modelType`       | string          | Fine-tuning method (e.g., "LoRA", "Adapter") |
| `hyperParameters` | RawExtension    | Fine-tuning hyperparameters as JSON          |
| `configuration`   | RawExtension    | Additional configuration as JSON             |
| `storage`         | StorageSpec     | Storage configuration for fine-tuned weights |
| `trainingJobRef`  | ObjectReference | Reference to the training job                |

## Best Practices

### Model Organization

- Use consistent naming conventions including version and parameter size
- Use labels to organize models by team, use case, or model family
- Use ClusterBaseModels for widely-used models, BaseModels for team-specific models

### Resource Planning

- Ensure nodes have sufficient storage (large models can be 100GB+)
- Use node affinity to target appropriate hardware
- Consider using fast local storage (NVMe SSDs) for model paths

### Security

- Store all credentials in Kubernetes Secrets
- Use workload identity or instance principals when possible
- Implement appropriate RBAC for model resource management

### Labels and Annotations

- Use labels for filtering and organization
- Use annotations for metadata that doesn't affect selection
- Consider using `ome.io/skip-config-parsing` for custom models

### Storage Configuration

- Use appropriate storage backends for your infrastructure
- Configure node selectors to target appropriate hardware
- Set reasonable storage paths with sufficient capacity

## Using Models in InferenceServices

Once your BaseModel is ready, reference it in an InferenceService:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
spec:
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 3
```

## Next Steps

- [Deploy an Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) using your BaseModel
- [Model Agent Administration](/ome/docs/administration/model-agent/) for operational details
- [Advanced Storage Configuration](/ome/docs/administration/storage/) for complex storage setups

For detailed technical and operational information, see the [Administration](/ome/docs/administration/) section.
