---
title: Base models
description: "BaseModel and ClusterBaseModel tell OME where a model's weights live and which nodes get them; OME parses the model's architecture, size and capabilities."
---

A BaseModel or ClusterBaseModel lets OME manage a model's weights. You say where the weights live and which nodes keep a copy. OME downloads the weights to those nodes, and runs serving pods only on nodes that hold them. A model on a PersistentVolumeClaim is mounted into the pods instead. OME also reads the model's files to learn its architecture, size and capabilities, and uses them to pick a [serving runtime](../runtimes/serving-runtimes.md). An [InferenceService](../serving/inference-services.md) then serves the model by naming it.

Since v1.3, a base model is optional: an InferenceService can [name its runtime](../serving/inference-services.md#the-runtime) and load the weights another way. The [model agent](../../guides/operate-ome/model-agent.md) that downloads base models is optional too, so turn it on when you [install OME](../../getting-started/install.md#step-3-install-ome).

## BaseModel and ClusterBaseModel

The two kinds have the same spec and differ in scope:

| Kind | Scope | Used by |
| --- | --- | --- |
| BaseModel | Namespaced | InferenceServices in the same namespace |
| ClusterBaseModel | Cluster | InferenceServices in any namespace |

An InferenceService names its model in `spec.model.name`, and the model's kind in `spec.model.kind`, which defaults to `ClusterBaseModel`. To serve a BaseModel, set `kind: BaseModel`. With the wrong kind, OME admits the InferenceService but doesn't create or update its serving workload; see [An InferenceService that serves a BaseModel gets no pods](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md#an-inferenceservice-that-serves-a-basemodel-gets-no-pods).

Give BaseModels and ClusterBaseModels different names. When both kinds share a name, InferenceServices in the BaseModel's namespace get the BaseModel, even when they ask for the ClusterBaseModel.

This ClusterBaseModel downloads `Qwen/Qwen2.5-7B-Instruct` from Hugging Face to `/mnt/data/models/qwen/qwen2-5-7b-instruct` on the nodes that run the [model agent](../../guides/operate-ome/model-agent.md):

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: qwen2-5-7b-instruct
spec:
  storage:
    storageUri: hf://Qwen/Qwen2.5-7B-Instruct
    path: /mnt/data/models/qwen/qwen2-5-7b-instruct
```

Apply the file:

```bash
kubectl apply -f model.yaml
```

```output
clusterbasemodel.ome.io/qwen2-5-7b-instruct created
```

Set `storage.storageUri`, and for `hf://`, `oci://` and `local://` models, [`storage.path`](#how-weights-reach-the-nodes). OME fills in the [rest of the metadata](#what-ome-learns-from-the-model) from the model's files, and the [OME API](../../reference/api/ome.v1beta1.md) reference lists every field.

## Where the weights come from

`storage.storageUri` says where the weights are, and its scheme decides what OME does:

| Scheme | URI form | What OME does |
| --- | --- | --- |
| `hf` | `hf://{model-id}[@{revision}]` | Downloads the model from Hugging Face to `path` on the selected nodes. The revision defaults to `main`. |
| `oci` | `oci://n/{namespace}/b/{bucket}/o/{object_path}` | Downloads the model from OCI Object Storage to `path` on the selected nodes. |
| `pvc` | `pvc://{pvc-name}/{sub-path}` for a BaseModel, `pvc://{namespace}:{pvc-name}/{sub-path}` for a ClusterBaseModel | A metadata Job reads the model from the claim. Whatever the kind, only InferenceServices in the claim's namespace can use it. See [Serve models from a PVC](../../guides/deploy-models/serve-models-from-pvc.md). |
| `local` | `local://{path}` | Reads the model from a directory that's already on the selected nodes. See [Serve models from node-local storage](../../guides/deploy-models/serve-models-from-local-storage.md). |
| `vendor` | `vendor://{vendor-name}/{resource-type}/{resource-path}` | Downloads and reads nothing. The selected nodes report the model ready. |

!!! warning "No downloader for S3, Azure, GCS or GitHub"
    OME accepts `s3://`, `az://`, `gs://` and `github://` URIs, but has no downloader for them. The model's [node labels](#node-labels-and-status) stay `Updating`, and the model stays `In_Transit`.

### Credentials

`hf://` models read their token from a Secret that you name in `storage.key`. The agent reads the Secret from the model's namespace, or from `ome` for a ClusterBaseModel. The token comes from the Secret's `token` key, or from the key in `storage.parameters.secretKey`.

Create the namespace `llama-demo`, and a Secret in it with your Hugging Face token:

```bash
kubectl create namespace llama-demo
kubectl create secret generic hf-token -n llama-demo --from-literal=token="$HF_TOKEN"
```

```output
namespace/llama-demo created
secret/hf-token created
```

This BaseModel downloads the gated model `meta-llama/Llama-3.1-8B-Instruct` with the token:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-1-8b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
    path: /mnt/data/models/meta/llama-3-1-8b-instruct
    key: hf-token
```

The agent uses the first token it finds:

1. The Secret in `storage.key`.
2. `storage.parameters.token`, which leaves the token in plain text in the model's spec, so prefer a Secret.
3. The agent's own `HF_TOKEN` environment variable, if you set one.

Without a token, a gated model fails to download, and the node reports `Failed`.

`oci://` models authenticate with the method in `storage.parameters.auth`: `InstancePrincipal`, the default, `UserPrincipal`, `ResourcePrincipal` or `OkeWorkloadIdentity`. `storage.parameters.region` overrides the region.

## How weights reach the nodes

The [model agent](../../guides/operate-ome/model-agent.md) puts the weights on the nodes. It runs as the DaemonSet `ome-model-agent-daemonset`, by default on all nodes whose taints it tolerates, including nodes with the `nvidia.com/gpu` taint. [Helm values](../../guides/operate-ome/model-agent.md#helm-values) lists the settings that choose its nodes.

Since v1.3, the `ome-resources` chart installs the agent only when you set `modelAgent.enabled: true`. On v1.2.2, the chart always installs it. Without the agent, models stay `In_Transit`, except `pvc://` models.

`storage.path` is the directory on the nodes that holds the model. Serving pods mount it read-only at the same path, and OME sets the `MODEL_PATH` environment variable to it, which the pre-configured runtimes load the model from.

The agent sees the host directory in `modelAgent.hostPath`, `/mnt/data/models` by default. Keep `path` under it, or mount more directories with `modelAgent.extraVolumes` and `modelAgent.extraVolumeMounts`.

With `storage.downloadPolicy: ReuseIfExists`, models that use the same Hugging Face files share one copy on each node. The default, `AlwaysDownload`, gives each model its own copy. See [Share Hugging Face artifacts](../../guides/operate-ome/shared-hf-artifacts.md).

### Choose the nodes

By default, all nodes that run the agent keep a copy of the model. Two `storage` fields narrow that down, and a node must match both when you set both:

| Field | A node matches when |
| --- | --- |
| `nodeSelector` | It has all the labels in the selector, with the same values. |
| `nodeAffinity` | It matches one of the `requiredDuringSchedulingIgnoredDuringExecution` terms. The agent ignores preferred terms. Unlike in pod scheduling, `NotIn` doesn't match a node that lacks the label. |

This BaseModel keeps a copy on the nodes of two instance types:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-3-70b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: hf://meta-llama/Llama-3.3-70B-Instruct
    path: /mnt/data/models/meta/llama-3-3-70b-instruct
    key: hf-token
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node.kubernetes.io/instance-type
                operator: In
                values:
                  - BM.GPU.H100.8
                  - BM.GPU.H200.8
```

When you change the model or a node:

| Change | What happens |
| --- | --- |
| `nodeSelector` or `nodeAffinity` | Nodes that stop matching delete their copy, and nodes that start matching download it. Since v1.3, nodes that still match keep their copy. On v1.2.2, they process the model again. |
| The model's labels or annotations, or another `storage` field, such as `storageUri`, `path` or `key` | All matching nodes process the model again. A `downloadPolicy` change does this only for `hf://` models. |
| `displayName`, `modelCapabilities` or other descriptive metadata | Nodes keep their copy as it is. |
| A node's labels | Nothing, until you [restart the agent pod](../../guides/operate-ome/model-agent.md#a-node-never-gets-the-label) on that node. The new agent downloads models that now select the node, and keeps copies of models that no longer do. |

While a node processes the model, the model's label there is `Updating`, so new serving pods avoid that node until it's `Ready` again. Running pods keep running.

### Node labels and status

The agent labels each selected Node with the model's state there. The label key depends on the kind:

| Kind | Label key | Example |
| --- | --- | --- |
| ClusterBaseModel | `models.ome.io/clusterbasemodel.{name}` | `models.ome.io/clusterbasemodel.qwen2-5-7b-instruct` |
| BaseModel | `models.ome.io/{namespace}.basemodel.{name}` | `models.ome.io/llama-demo.basemodel.llama-3-1-8b-instruct` |

When a ClusterBaseModel's name is longer than 32 characters, or a BaseModel's namespace and name together are longer than 38, OME shortens the key with a hash. Read the key from the Node's labels then.

The label is `Updating` while the agent downloads or checks the model, then `Ready` or `Failed`.

The model's status lists the nodes where it's ready and where it failed:

```bash
kubectl get clusterbasemodel qwen2-5-7b-instruct \
  -o 'custom-columns=STATE:.status.state,READY:.status.nodesReady[*],FAILED:.status.nodesFailed[*]'
```

```output
STATE   READY                   FAILED
Ready   gpu-node-1,gpu-node-2   <none>
```

Or select the nodes by label:

```bash
kubectl get nodes -l models.ome.io/clusterbasemodel.qwen2-5-7b-instruct=Ready -o name
```

```output
node/gpu-node-1
node/gpu-node-2
```

OME runs an InferenceService's serving pods only on nodes where the model's label is `Ready`. `pvc://` models have no copy on the nodes, so their pods have no such constraint.

## Model lifecycle

`status.state` is the model's state across the cluster, shown in the `READY` column of `kubectl get basemodel` and `kubectl get clusterbasemodel`:

| State | Meaning |
| --- | --- |
| `In_Transit` | No node has the model `Ready` or `Failed` yet. Nodes are still downloading it, the model agent is off, or the model selects no node that runs the agent. |
| `Ready` | At least one node has the model ready. |
| `Failed` | No node has the model ready, and at least one has failed. |

A model is `Ready` as soon as one node has it, so some serving pods can stay `Pending` while other nodes download it. If a model stays `In_Transit` or becomes `Failed`, see [Troubleshooting](../../guides/operate-ome/model-agent.md#troubleshooting) in the model agent guide, or in the [PVC guide](../../guides/deploy-models/serve-models-from-pvc.md#troubleshooting) for a `pvc://` model.

Create an InferenceService once its model is `Ready`. OME needs the model's format, which it reads from the model's files, to choose or check a runtime. Without it, the admission webhook rejects the InferenceService with a message that ends in `model format name is required`.

### Disable a model

To stop new use of a model, set `disabled: true` in its spec. The admission webhook then rejects new and changed InferenceServices that use it, with `model qwen2-5-7b-instruct is disabled`. OME also stops creating or updating their serving workloads, and records a `ModelReconcileError` event with `specified base model qwen2-5-7b-instruct is disabled`. Running pods keep running, and the weights stay on the nodes.

### Delete a model {#deleting-a-model}

When you delete a model, a finalizer keeps it until the agent on each node that recorded the model reports its copy deleted. If a node's agent isn't running, the deletion waits until it runs again or the node leaves the cluster.

On each node, the agent removes the model's label and deletes the files of an `hf://` or `oci://` model from `path`. It keeps the files when another model, in any namespace, uses the same `path`, or when the model has the label `models.ome/reserve-model-artifact: "true"`. It leaves `local://` and `vendor://` files in place. Files shared through `ReuseIfExists` follow the rules in [Share Hugging Face artifacts](../../guides/operate-ome/shared-hf-artifacts.md). For `pvc://` models, see [Clean up](../../guides/deploy-models/serve-models-from-pvc.md#clean-up) in the PVC guide.

Set the reserve label when you create the model: adding a label later makes all matching nodes process the model again. See [Keep downloaded model files](../../guides/operate-ome/configure-model-artifact-retention.md).

## What OME learns from the model

Once a node has the weights, the agent reads the model's `config.json`, or `model_index.json` for a diffusion pipeline, and `hf_quant_config.json` if there is one. OME fills in these fields:

| Field | What OME records |
| --- | --- |
| `modelType` | The model type, such as `llama`. |
| `modelArchitecture` | The architecture, such as `LlamaForCausalLM`. |
| `modelFormat` | `safetensors` with version `1.0.0`, or `diffusers` for a diffusion pipeline. |
| `modelFramework` | `transformers` with the config's `transformers_version`, or `diffusers`. |
| `modelParameterSize` | The number of parameters, such as `3.21B`. |
| `maxTokens` | The context length. |
| `modelCapabilities` | What the model does, such as `TEXT_TO_TEXT`, `IMAGE_TEXT_TO_TEXT`, `EMBEDDING` or `TEXT_TO_IMAGE`. Some models get several. |
| `quantization` | `fp8`, `fbgemm_fp8`, `int4`, `nvfp4`, `mxfp4` or `compressed-tensors`. 4-bit GPTQ and AWQ models get `int4`. Other methods and unquantized models leave it empty. |
| `modelConfiguration` | A summary of the config, with keys such as `context_length` and `torch_dtype`. |
| `diffusionPipeline` | The pipeline's components, from `model_index.json`. |

OME fills in only empty fields, so a value that you set always wins. For `pvc://` models, a metadata Job fills in the same fields, except `modelConfiguration` and `diffusionPipeline`.

### Set the metadata yourself

Set these fields yourself:

- `modelFormat` and `modelFramework`, for `vendor://` models and models with unreadable files, such as a directory without `config.json`. These models still become `Ready`, but OME needs both fields to choose a runtime.
- `modelCapabilities`, when OME doesn't recognize the architecture and leaves an empty entry.
- `displayName`, `version`, `vendor` and `apiCapabilities`, which OME never fills in.

To skip reading a model's files, add the annotation `ome.oracle.com/skip-config-parsing: "true"`. OME then keeps only the metadata you set. For a `pvc://` model, the metadata Job ignores the annotation and fills in the fields you left empty. This BaseModel sets the metadata that the runtime `srt-llama-3-2-3b-instruct` supports:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
  annotations:
    ome.oracle.com/skip-config-parsing: "true"
spec:
  modelType: llama
  modelArchitecture: LlamaForCausalLM
  modelFormat:
    name: safetensors
    version: "1.0.0"
  modelFramework:
    name: transformers
    version: "4.45.0.dev0"
  modelParameterSize: "3.21B"
  modelCapabilities:
    - TEXT_TO_TEXT
  storage:
    storageUri: hf://meta-llama/Llama-3.2-3B-Instruct
    path: /mnt/data/models/meta/llama-3-2-3b-instruct
    key: hf-token
```

## How runtimes match the model

A [serving runtime](../runtimes/serving-runtimes.md) lists the models it supports in `supportedModelFormats`, by format, framework, architecture and quantization, and can limit their size with `modelSizeRange`. [Model version matching](../../reference/matching/model-version-matching.md) and [Diffusion pipeline runtime matching](../../reference/matching/diffusion-pipeline-runtime-matching.md) explain when a model matches.

When an InferenceService doesn't name a runtime, OME chooses among the matching runtimes that have an entry with `autoSelect: true`. A ServingRuntime in the InferenceService's namespace wins over any ClusterServingRuntime, and among runtimes of the same kind, the highest [score](../../reference/matching/runtime-selection-scoring.md) wins.

When an InferenceService names a runtime, OME uses it even if the runtime doesn't declare support for the model, and records a `RuntimeCompatibilityAdvisory` event. `Sharded` models are the exception; see [Distribution](#distribution).

## Distribution {since=v1.3}

`spec.distribution` decides how the weights are laid out. `PerNode`, the default, puts a full copy on each selected node; leave the field out to get it.

`Sharded` is for loading the model in chunks from a [model cache provider](../runtimes/serving-runtimes.md#supported-model-formats), which this release can't configure yet. The admission webhook rejects InferenceServices that use a `Sharded` model, even when they name a runtime. The message includes `no model cache provider is configured for sharded model loading`.

## Fine-tuned weights

A [FineTunedWeight](fine-tuned-weights.md) holds weights fine-tuned from the base model in its `baseModelRef`. To serve it, name the base model in `spec.model.name` and the FineTunedWeight in `spec.model.fineTunedWeights`. OME serves one fine-tuned weight per InferenceService.

## Next steps

- [Serve models from a PVC](../../guides/deploy-models/serve-models-from-pvc.md): serve weights from a PersistentVolumeClaim.
- [Serve models from node-local storage](../../guides/deploy-models/serve-models-from-local-storage.md): serve weights already on the nodes' disks.
- [Run the model agent](../../guides/operate-ome/model-agent.md): install, tune and troubleshoot the agent.
- [Share Hugging Face artifacts](../../guides/operate-ome/shared-hf-artifacts.md): keep one copy of shared Hugging Face files per node.
- [Serving runtimes](../runtimes/serving-runtimes.md): how a runtime declares the models it supports.
