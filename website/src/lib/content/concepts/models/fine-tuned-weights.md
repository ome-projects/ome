---
title: Fine-tuned weights
description: Serve weights fine-tuned from a base model, such as a LoRA adapter, from a zip file in OCI Object Storage.
---

A FineTunedWeight lets an [InferenceService](../serving/inference-services.md) serve weights fine-tuned from a [base model](base-models.md): an adapter, such as LoRA, or a full model with an adapter merged in. You upload the weights to OCI Object Storage as a zip file, describe them in a FineTunedWeight, and name it next to the base model in the InferenceService. An init container in each serving pod downloads them, and the [serving runtime](../runtimes/serving-runtimes.md) loads them.

## Merged weights and adapters

`configuration.merged_weights` says what the zip file holds. Merged weights work with a pre-configured runtime. An adapter needs a runtime of your own.

| | Merged weights | Adapter |
| --- | --- | --- |
| `configuration` | `merged_weights: true`, as a boolean | Any other value, or `{}` |
| Object OME downloads | The URI's object, with `-merged-weight` added to its name | The URI's object |
| Nodes | You choose them; see [What the pods get](#what-the-pods-get) | The base model's nodes |
| Runtime | One that loads `$(MODEL_PATH)`, as `srt-llama-3-2-3b-instruct` does | One that loads the base model from its `storage.path` and the adapter from `/opt/ml/model`. No pre-configured runtime does. |

A `pvc://` base model rules out adapters: the claim and the weights both mount at `/opt/ml/model`, so the API server rejects the pod spec.

## The FineTunedWeight resource

A FineTunedWeight is cluster-scoped, so InferenceServices in any namespace can use it. Before you run the examples:

- Install the `llama-3-2-3b-instruct` ClusterBaseModel and the `srt-llama-3-2-3b-instruct` ClusterServingRuntime from [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md), and wait until the model is `Ready`. OME needs the format that the [model agent](../../guides/operate-ome/model-agent.md) reads from the model's files.
- [Upload the weights](#where-the-weights-come-from).
- [Configure the download](#configure-the-download), once per cluster.

This FineTunedWeight holds a LoRA adapter merged into the base model:

```yaml title="fine-tuned-weight.yaml"
apiVersion: ome.io/v1beta1
kind: FineTunedWeight
metadata:
  name: llama-3-2-3b-instruct-finance
spec:
  baseModelRef:
    name: llama-3-2-3b-instruct
  modelType: LoRA
  hyperParameters:
    strategy: lora
    lora_rank: 16
    lora_alpha: 32
  configuration:
    merged_weights: true
  storage:
    storageUri: oci://n/mycompany/b/fine-tuned/o/llama-3-2-3b-instruct-finance
```

Apply the file:

```bash
kubectl apply -f fine-tuned-weight.yaml
```

```output
finetunedweight.ome.io/llama-3-2-3b-instruct-finance created
```

List FineTunedWeights:

```bash
kubectl get finetunedweights
```

```output
NAME                            DISABLED   VERSION   VENDOR   COMPARTMENTID   MODELTYPE   READY   AGE
llama-3-2-3b-instruct-finance                                                 LoRA                12s
```

READY stays blank: a FineTunedWeight has no status, so watch the InferenceService that serves it. Since v1.3, [`kubectl ome get ftw`](../../reference/kubectl-ome/get.md#finetunedweights-columns) shows each FineTunedWeight's type and base model.

## Fields

The API server requires the fields marked Yes, and OME needs the fields marked To serve.

| Field | Required | Description |
| --- | --- | --- |
| `baseModelRef` | Yes, with `name` | The base model the weights were trained from, with its `namespace` for a BaseModel. |
| `modelType` | Yes | A free-form string, here `LoRA`, for the MODELTYPE column. Serving ignores it. |
| `hyperParameters` | Yes | The training hyperparameters, with any keys. |
| `hyperParameters.strategy` | To serve | The training strategy as a string, here `lora`. OME copies it into a pod label, so it must be a valid label value. |
| `configuration` | To serve | Merged weights or an adapter; see [Merged weights and adapters](#merged-weights-and-adapters). |
| `storage.storageUri` | Yes | The zip file's `oci://` URI; see [Where the weights come from](#where-the-weights-come-from). |
| `disabled` | No | Shown in the DISABLED column. OME still serves a disabled FineTunedWeight. |

OME ignores the other fields in the [FineTunedWeightSpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-FineTunedWeightSpec) reference.

## Where the weights come from

Fine-tuned weights come from OCI Object Storage, as one zip file, even when the base model uses [another scheme](base-models.md#where-the-weights-come-from). Set `storage.storageUri` to `oci://n/{namespace}/b/{bucket}/o/{object_path}`. The init container unzips the file into `/opt/ml/model` and keeps its paths, so put `config.json` and the model's other files at the top level of the zip file.

For the example, upload the zip file to the bucket `fine-tuned` in the Object Storage namespace `mycompany`, as the object `llama-3-2-3b-instruct-finance-merged-weight`, with no `.zip` extension. Don't end the URI with `/`, or OME looks for `llama-3-2-3b-instruct-finance/-merged-weight`.

## Configure the download

The init container takes its image, OCI authentication and resources from the `fineTunedAdapter` entry of the `inferenceservice-config` ConfigMap in OME's namespace, `ome` here. All FineTunedWeights share this entry, and OME ignores their `storage.key` and `storage.parameters`.

The install doesn't write the entry, and the chart ignores its `ome.omeAgent.fineTunedAdapter` values. Until you add it, the webhook rejects the pods of every InferenceService that serves fine-tuned weights.

Copy `image`, `authType`, `compartmentId` and `region` from the `modelInit` entry, which `kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.modelInit}'` prints. This patch file uses the chart's defaults, with resources for the 3B model:

```yaml title="fine-tuned-adapter.yaml"
data:
  fineTunedAdapter: |-
    {
      "image": "ghcr.io/moirai-internal/ome-agent:v1.2.2",
      "authType": "InstancePrincipal",
      "compartmentId": "ocid1.compartment.oc1..dummy-compartment",
      "region": "ap-osaka-1",
      "memoryRequest": "24Gi",
      "memoryLimit": "24Gi",
      "cpuRequest": "4",
      "cpuLimit": "4"
    }
```

Every key but `region` is required, and the resources take Kubernetes quantities. Patch the ConfigMap:

```bash
kubectl patch configmap inferenceservice-config -n ome --type merge --patch-file fine-tuned-adapter.yaml
```

```output
configmap/inferenceservice-config patched
```

New pods pick up the entry without a restart.

!!! danger "Keep the entry valid JSON"
    The admission webhook parses this entry for every InferenceService pod, with or without fine-tuned weights. If the JSON is invalid, it rejects them all: running pods keep serving, but rollouts, scale-ups and pod replacements stall until you fix it.

The init container downloads and unzips into a memory-backed `emptyDir`. Set `memoryLimit` above the zip file's size plus the unzipped files' size, with room for the agent, or it runs out of memory. The weights stay in the node's memory until the pod is deleted and count toward the pod's memory limit. Leave room for them in the runner's memory limit.

## Use fine-tuned weights in an InferenceService

Name the FineTunedWeight in `spec.model.fineTunedWeights`, and the base model from its `baseModelRef` in `spec.model.name`: OME doesn't check that they match. Create a namespace for the example:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

This InferenceService serves the merged weights:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-finance
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
    fineTunedWeights:
      - llama-3-2-3b-instruct-finance
  runtime:
    name: srt-llama-3-2-3b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Follow these rules:

- List one FineTunedWeight, and only in `fineTunedWeights`. In `spec.model.name`, even with `kind: FineTunedWeight`, the webhook rejects it with `referenced model "llama-3-2-3b-instruct-finance" not found`.
- For a namespaced BaseModel, set `spec.model.kind: BaseModel`.
- OME sets `MODEL_PATH` to `/opt/ml/model` for a `vendor: meta` base model, as here, unless the runner sets it. For others, set it in `spec.engine.runner.env` if the runtime loads `$(MODEL_PATH)`.

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-finance created
```

Wait until it's ready. The first start takes a while, since the pod downloads the weights and pulls the SGLang image:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-finance -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-finance condition met
```

List the pods with their init containers and their merged-weights label:

```bash
kubectl get pods -n llama-demo -l ome.io/inferenceservice=llama-finance \
  -o 'custom-columns=NAME:.metadata.name,INIT:.spec.initContainers[*].name,MERGED:.metadata.labels.fine-tuned-serving-with-merged-weights,PHASE:.status.phase'
```

```output
NAME                                   INIT                 MERGED   PHASE
llama-finance-engine-7c9d8b6f5-x2kqp   fine-tuned-adapter   true     Running
```

Send requests to the `llama-finance` Service in `llama-demo`, as [Step 7: Send a request](../../getting-started/serve-your-first-model.md#step-7-send-a-request) shows, with `model` set to `meta-llama/Llama-3.2-3B-Instruct`, the name this runtime serves. A runtime that names the model `$(SERVED_MODEL_NAME)`, as `vllm-llama-3-1-8b-instruct` does, serves it as `/data/llama-3-2-3b-instruct-finance` instead. OME sets that variable for a `meta` base model, over the runtime's value.

### What the pods get

In the engine's and any decoder's pods, the admission webhook adds the `fine-tuned-adapter` init container, which downloads the weights. The runner container mounts them at `/opt/ml/model`, and must keep its default name, `ome-container`. For a Cohere T-Few adapter, with `vendor: cohere` and `strategy: tfew`, they mount under `/opt/ml/tfew`, and OME sets `TFEW_PATH`.

OME also labels and annotates the pods, as [Labels and annotations](../../reference/api/labels-and-annotations.md) lists.

!!! warning "Choose the nodes for merged weights"
    Pods that serve merged weights don't need the base model's files. So OME drops the node selectors from the base model's [node labels](base-models.md#node-labels-and-status), the runtime's `spec.nodeSelector` and the `discovery.nodeSelector` of the [accelerator class](../runtimes/accelerator-classes.md). To keep the pods on the right nodes, set `spec.engine.nodeSelector` in the InferenceService or `engineConfig.nodeSelector` in the runtime.

## When the InferenceService doesn't become ready

If `kubectl wait` times out, the error is in one of three places.

### The ReplicaSet can't create pods

The admission webhook rejected the pods. In the default [RawDeployment mode](../architecture/deployment-modes.md#the-deployment-modes), the engine's ReplicaSet records a `FailedCreate` event. Read its message with `kubectl describe replicaset -n llama-demo -l ome.io/inferenceservice=llama-finance`:

| Message contains | Cause |
| --- | --- |
| `failed to validate FineTunedAdapterInjector` | The entry is missing, or lacks `image`, `compartmentId` or `authType`. |
| `cpuLimit is required`, or `invalid memoryLimit "24GB"` | A resource key is missing, or not a Kubernetes quantity. |
| `unable to unmarshal fineTunedAdapter json string:` | The entry is invalid JSON. |
| `invalid OCI storage URI format` | `storage.storageUri` has the wrong form. |
| `No FineTunedWeight with the name: llama-3-2-3b-instruct-finance` | The FineTunedWeight was deleted, or never created. |
| `no main container ome-container specified` | The runner container has another name. |

### The init container fails

The pod shows `Init:Error`, then `Init:CrashLoopBackOff`. Read the init container's log with `kubectl logs llama-finance-engine-7c9d8b6f5-x2kqp -n llama-demo -c fine-tuned-adapter`. If the object is missing, the log shows `object llama-3-2-3b-instruct-finance-merged-weight not found in bucket fine-tuned`. Check the object's name and its `-merged-weight` suffix.

### No new pods and no events

OME logs the error and retries. Find it with `kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep llama-finance`:

| Error contains | Cause |
| --- | --- |
| `FineTunedWeight.ome.io "llama-3-2-3b-instruct-finance" not found` | No FineTunedWeight has that name. |
| `stacked fine-tuned serving is not supported yet` | `spec.model.fineTunedWeights` lists more than one FineTunedWeight. |
| `unexpected end of JSON input` | The FineTunedWeight has no `configuration`. |
| `hyper-parameter "strategy"` | `hyperParameters.strategy` is missing, or not a string. Since v1.3. |
| `must be unique` | The FineTunedWeight holds an adapter, and the base model uses `pvc://`. |

## Edit and delete a FineTunedWeight

### Edit a FineTunedWeight

An edit reaches only new pods:

- After you change `storage.storageUri`, new pods download from the new URI and running pods keep their weights, so replicas can serve different weights.
- A change to `merged_weights` or `hyperParameters.strategy` applies when OME next processes an InferenceService that uses the FineTunedWeight, for example after you edit the InferenceService. OME then replaces its pods.

To roll out new weights, upload them as a new object, create a FineTunedWeight for it, and set `spec.model.fineTunedWeights` to that name.

### Delete a FineTunedWeight

Delete the InferenceServices that use a FineTunedWeight, or remove it from their `spec.model.fineTunedWeights`, before you delete it:

```bash
kubectl delete inferenceservice llama-finance -n llama-demo
kubectl delete finetunedweight llama-3-2-3b-instruct-finance
```

```output
inferenceservice.ome.io "llama-finance" deleted from llama-demo namespace
finetunedweight.ome.io "llama-3-2-3b-instruct-finance" deleted
```

Kubernetes deletes a FineTunedWeight right away, even while InferenceServices use it. Their running pods keep serving, but OME can't update their serving workloads, and the webhook rejects their new pods.

## Next steps

- [Base models](base-models.md): the models that fine-tuned weights build on.
- [InferenceService](../serving/inference-services.md): the components that serve a model.
- [Serving runtimes](../runtimes/serving-runtimes.md): how a runtime declares its models and serving container.
