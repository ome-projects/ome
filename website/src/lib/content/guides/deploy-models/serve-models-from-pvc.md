---
title: Serve models from a PVC
description: "Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes."
---

Every serving pod reads the model from the same PersistentVolumeClaim, so you keep one copy of the weights, and the [model agent](../operate-ome/model-agent.md) downloads nothing to the nodes. You point a [BaseModel](../../concepts/models/base-models.md) at the claim with a `pvc://` URI, and a metadata Job reads the model's details from the claim. Then the pods of an [InferenceService](../../concepts/serving/inference-services.md) mount the claim read-only. The steps serve `meta-llama/Llama-3.2-1B-Instruct`.

If you don't have a populated claim yet, start with [Stage model weights](stage-model-weights.md). That guide downloads ungated Qwen3-0.6B to a shared PVC, then registers and serves it; this page covers the PVC URI, metadata Job and storage options in more detail.

<div class="prerequisites" markdown>

- OME installed, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- A Bound PersistentVolumeClaim `model-storage` in the namespace `llama-demo`, with the access mode `ReadOnlyMany` or `ReadWriteMany`. The metadata Job and the serving pods can run on different nodes, and each of them mounts the claim. A Job that first populates the volume needs write access; its consumers can mount a `ReadWriteMany` claim read-only without changing the claim's access mode.
- On the claim, the files of `meta-llama/Llama-3.2-1B-Instruct` in a Hugging Face layout, `config.json` plus the weight files, in the directory `llama-3-2-1b-instruct` at the root of the volume. The metadata Job runs as user 65532, so that user must be able to read them. World-readable files work.
- The [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-2-1b-instruct`. Check with `kubectl get clusterservingruntime srt-llama-3-2-1b-instruct`. If it's missing, apply [`config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml) from a clone of the OME repository.
- An amd64 node with an NVIDIA GPU, 10 CPUs and 30 GiB of memory free, which is what the runtime requests.

</div>

## The `pvc://` storage URI

A `pvc://` URI names the claim and the directory on it that holds the model. Its form depends on the kind of model:

| Kind | URI form | Example |
| --- | --- | --- |
| BaseModel | `pvc://{pvc-name}/{sub-path}` | `pvc://model-storage/llama-3-2-1b-instruct` |
| ClusterBaseModel | `pvc://{namespace}:{pvc-name}/{sub-path}` | `pvc://llama-demo:model-storage/llama-3-2-1b-instruct` |

A BaseModel uses a claim in its own namespace. A ClusterBaseModel names the claim's namespace before the colon. Either way, the metadata Job runs in the claim's namespace. The InferenceServices that serve the model must run there too, because a pod can only mount a claim in its own namespace.

The sub-path is required. It's the path from the root of the volume to the directory that holds `config.json`, or `model_index.json` for a diffusion model. It can be several levels deep, such as `models/llama-3-2-1b-instruct`. Serving pods mount it read-only at `/opt/ml/model`, and OME sets `MODEL_PATH` to that path.

## Step 1: Verify the PVC

Check the claim's status and access mode:

```bash
kubectl get pvc model-storage -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,STATUS:.status.phase,ACCESS:.status.accessModes[*]'
```

```output
NAME            STATUS   ACCESS
model-storage   Bound    ReadOnlyMany
```

`STATUS` must be `Bound`, and `ACCESS` should be `ReadOnlyMany` or `ReadWriteMany`.

## Step 2: Create the model

Create a BaseModel in `llama-demo` that points at the claim. For a ClusterBaseModel, see [Use a ClusterBaseModel](#use-a-clusterbasemodel).

Leave out `modelArchitecture`, `modelFormat` and the other metadata fields. OME fills them in from the model's files in Step 3, and keeps any that you set.

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: "pvc://model-storage/llama-3-2-1b-instruct"
```

Apply the file:

```bash
kubectl apply -f model.yaml
```

```output
basemodel.ome.io/llama-3-2-1b-instruct created
```

## Step 3: Watch the model become Ready

OME now runs a metadata Job that reads the model's `config.json` from the claim. From it, OME fills in the model's empty metadata fields and sets the state to `Ready`. Watch the state:

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo -w \
  -o custom-columns=NAME:.metadata.name,STATE:.status.state
```

```output
NAME                    STATE
llama-3-2-1b-instruct   In_Transit
llama-3-2-1b-instruct   Ready
```

Press Ctrl+C once the state is `Ready`. A state can repeat, and the column shows `<none>` until OME sets one.

Check what OME recorded:

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo \
  -o custom-columns=ARCHITECTURE:.spec.modelArchitecture,FORMAT:.spec.modelFormat.name,FRAMEWORK:.spec.modelFramework.name,VERSION:.spec.modelFramework.version
```

```output
ARCHITECTURE       FORMAT        FRAMEWORK      VERSION
LlamaForCausalLM   safetensors   transformers   4.45.0.dev0
```

`VERSION` comes from `transformers_version` in your copy's `config.json`, so yours can differ. [What OME learns from the model](../../concepts/models/base-models.md#what-ome-learns-from-the-model) lists the other fields that OME records.

## Step 4: Deploy an InferenceService

Once the model is `Ready`, create an InferenceService that serves it:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
    kind: BaseModel
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

`spec.model.kind` defaults to `ClusterBaseModel`, so `isvc.yaml` sets `kind: BaseModel`. The runtime `srt-llama-3-2-1b-instruct` declares support for what Step 3 recorded. It sets `autoSelect: false`, so OME uses it only when an InferenceService names it.

If the runtime doesn't declare support for your copy of the model, OME uses it anyway and records a `RuntimeCompatibilityAdvisory` event. Since v1.3, `kubectl apply` also prints a warning. See [Reference a runtime explicitly](reference-a-runtime-explicitly.md).

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

Wait for the InferenceService to be ready. The first start pulls the SGLang image, which can take several minutes:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

## Step 5: Send a request

In a second terminal, forward a local port to the engine's Service:

```bash
kubectl port-forward -n llama-demo svc/llama-3-2-1b-instruct-engine 8080:8080
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

Send a chat completion request. `jq` prints the model's answer:

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is a PersistentVolumeClaim?"}], "max_tokens": 64}' \
  | jq -r '.choices[0].message.content'
```

```output
A PersistentVolumeClaim (PVC) is a request for storage in Kubernetes that a pod uses to mount a PersistentVolume.
```

The answer varies from run to run.

## Use a ClusterBaseModel

A ClusterBaseModel works too, but only InferenceServices in the claim's namespace can use it. Its URI names that namespace:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-2-1b-instruct
spec:
  storage:
    storageUri: "pvc://llama-demo:model-storage/llama-3-2-1b-instruct"
```

!!! warning "Name clash with the catalog"
    The [catalog of pre-configured models](../../getting-started/pre-configured-models.md) has a ClusterBaseModel with this name that downloads the model from Hugging Face. If `kubectl get clusterbasemodel llama-3-2-1b-instruct` finds one, applying `model.yaml` changes it. Pick another name, and use it wherever the steps name the model.

Apply the file in place of the one in Step 2:

```bash
kubectl apply -f model.yaml
```

```output
clusterbasemodel.ome.io/llama-3-2-1b-instruct created
```

Then continue from Step 3. In the `kubectl get basemodel` commands, use `clusterbasemodel` and leave out `-n llama-demo`. In `isvc.yaml`, leave out `kind: BaseModel`. The metadata Job and the InferenceService still run in `llama-demo`.

## Configure the metadata Job

The metadata Job's settings are values of the `ome-resources` chart, under `ome.omeAgent.metadataJob`. Change them when the claim mounts only on some nodes, or when the Job needs other resources:

| Value | Default | Description |
| --- | --- | --- |
| `serviceAccount` | `ome-model-metadata` | ServiceAccount the Job runs as. OME creates it in the claim's namespace if it's missing. |
| `memoryRequest` | `256Mi` | Memory request. |
| `memoryLimit` | `512Mi` | Memory limit. |
| `cpuRequest` | `100m` | CPU request. |
| `cpuLimit` | `500m` | CPU limit. |
| `backoffLimit` | `2` | Retries before the Job fails. |
| `ttlSecondsAfterFinished` | `3600` | Seconds that Kubernetes keeps a finished Job. |
| `nodeSelector` | `{}` | Node labels that the Job's pod requires. |
| `tolerations` | `[]` | Taints that the Job's pod tolerates. |
| `affinity` | `{}` | Affinity rules for the Job's pod. |
| `priorityClassName` | `""` | PriorityClass of the Job's pod. |

A `0` for `backoffLimit` or `ttlSecondsAfterFinished` means the default, and an empty resource value sets no request or limit.

The Job runs the ome-agent image, set by `ome.omeAgent.image` and `ome.omeAgent.tag`. To pull it from your own registry, see [Install from a private registry](../../getting-started/private-registries.md).

For example, if the claim's CSI driver runs only on the GPU nodes, and those nodes carry the `nvidia.com/gpu` taint, set `nodeSelector` and `tolerations`:

```yaml title="values.yaml"
ome:
  omeAgent:
    metadataJob:
      nodeSelector:
        nvidia.com/gpu.present: "true"
      tolerations:
        - key: nvidia.com/gpu
          operator: Exists
          effect: NoSchedule
```

Add the values to the values file you install `ome-resources` with. `helm list -n ome` shows the installed chart version in its `CHART` column, such as `ome-resources-1.2.2`. Upgrade the release with that version, so that OME stays in step with the `ome-crd` chart:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.2.2 -f values.yaml
```

```output
Pulled: ghcr.io/moirai-internal/charts/ome-resources:1.2.2
Digest: sha256:703ff2932198d1356ec6977e7bba0bdfa013f50088eb96d097d6e49bf88914f5
Release "ome" has been upgraded. Happy Helming!
NAME: ome
LAST DEPLOYED: Mon Sep 28 02:41:07 2026
NAMESPACE: ome
STATUS: deployed
REVISION: 2
TEST SUITE: None
```

The controller reads these values when it starts. Since v1.3, `helm upgrade` restarts it when they change. On v1.2.2, restart it yourself:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

New settings apply to the Jobs that OME creates after the restart. To rerun a model's Job with them, delete the Job, as in [The metadata Job fails](#the-metadata-job-fails).

## Troubleshooting

Start with the model's state, and the reason and message on its `Ready` condition:

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo \
  -o 'custom-columns=STATE:.status.state,REASON:.status.conditions[?(@.type=="Ready")].reason,MESSAGE:.status.conditions[?(@.type=="Ready")].message'
```

For a missing claim:

```output
STATE    REASON        MESSAGE
Failed   PVCNotFound   PVC llama-demo/model-storage not found
```

| State | Reason | Meaning | Fix |
| --- | --- | --- | --- |
| `In_Transit` | `PVCNotBound` | The claim isn't Bound yet. | Wait for it to bind, or fix its storage class or volume. |
| `In_Transit` | `PVCMetadataExtracting` | The metadata Job is running, or waiting for its pod to start. | If it doesn't finish, see [The metadata Job fails](#the-metadata-job-fails). |
| `Failed` | `PVCInvalid` | The URI is malformed, or has the wrong form for the model's kind. | Fix the URI. See [The `pvc://` storage URI](#the-pvc-storage-uri). |
| `Failed` | `PVCNotFound` | OME can't find the claim. | Create the claim, or fix its name in the URI. OME checks again on its own. |
| `Failed` | `PVCConfigMissing` | The ome-agent image is unset, or OME fails to build the Job. The message says which. | Fix the `ome.omeAgent` values, as in [Configure the metadata Job](#configure-the-metadata-job). |
| `Failed` | `PVCMetadataExtractionFailed` | The Job failed. The message has its error. | See [The metadata Job fails](#the-metadata-job-fails). |

A `Ready` model shows `In_Transit` for a moment each time Kubernetes deletes its finished Job after `ttlSecondsAfterFinished`, because OME runs the Job again. Serving isn't affected.

### The metadata Job fails

List the model's Jobs and pods with `kubectl get jobs,pods -n llama-demo -l models.ome/model-name=llama-3-2-1b-instruct`, and read the pods' logs with `kubectl logs -n llama-demo -l models.ome/model-name=llama-3-2-1b-instruct`:

- If a pod stays `Pending`, `kubectl describe pod` shows why. If the claim mounts only on some nodes, set `nodeSelector` and `tolerations` as in [Configure the metadata Job](#configure-the-metadata-job). A sub-path that's missing from the volume also keeps the pod from starting.
- `no model_index.json or config.json found in /model` means that the sub-path is wrong, or that user 65532 can't read the directory.
- `permission denied` means that user 65532 can't read the model's files.

When the fix is a new sub-path in the URI, OME starts a new Job on its own. For other fixes, delete the Job, and OME creates a new one:

```bash
kubectl delete job -n llama-demo -l models.ome/model-name=llama-3-2-1b-instruct
```

```output
job.batch "llama-3-2-1b-instruct-metadata-26cec0b4" deleted from llama-demo namespace
```

The old failure can show until the new Job finishes.

### The webhook rejects the model

`kubectl apply` prints the reason after `denied the request:`. For a ClusterBaseModel whose URI has no namespace:

```text
ClusterBaseModel PVC URI must specify a namespace (format: pvc://{namespace}:{pvc-name}/{sub-path}), got "pvc://model-storage/llama-3-2-1b-instruct"
```

For a BaseModel whose URI has a namespace:

```text
namespaced BaseModel PVC URI must not specify a namespace; the BaseModel's own namespace is used, got "pvc://llama-demo:model-storage/llama-3-2-1b-instruct"
```

Other malformed URIs get `invalid PVC storage URI`, with the problem at the end. Here the sub-path is missing:

```text
invalid PVC storage URI "pvc://model-storage": invalid PVC storage URI format: missing subpath
```

Fix the URI to match [The `pvc://` storage URI](#the-pvc-storage-uri), and apply `model.yaml` again.

### Sharded distribution is rejected {since=v1.3}

The webhook rejects a `pvc://` model with `distribution: Sharded`:

```text
PVC storage URIs are not compatible with distribution=Sharded; use distribution=PerNode (or omit) for pvc:// models
```

Leave `distribution` out, or set it to `PerNode`.

### The webhook rejects the InferenceService

Since v1.3, the webhook rejects `isvc.yaml` until OME records the model's format in Step 3:

```text
runtime srt-llama-3-2-1b-instruct does not support model llama-3-2-1b-instruct: invalid model specification: modelFormat.name: model format name is required
```

Wait for the model to be `Ready`, as in [Step 3](#step-3-watch-the-model-become-ready), then apply `isvc.yaml` again.

Since v1.3, the webhook also rejects an InferenceService that uses the ClusterBaseModel from another namespace, here `default`:

```text
ClusterBaseModel "llama-3-2-1b-instruct" references PVC in namespace "llama-demo", but InferenceService is in namespace "default"; Kubernetes does not allow pods to mount PVCs from another namespace. Either move the InferenceService to namespace "llama-demo" or replicate the PVC into namespace "default".
```

Create the InferenceService in `llama-demo`. For other messages, see [The webhook rejects the InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md#the-webhook-rejects-the-inferenceservice).

### Serving pods don't start

Run `kubectl describe pod -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct`, and read the `Events` at the end of each pod's description:

- If there are no pods, check that `spec.model.kind` in `isvc.yaml` matches the kind of model you created. See [An InferenceService that serves a BaseModel gets no pods](../troubleshoot/troubleshoot-an-inferenceservice.md#an-inferenceservice-that-serves-a-basemodel-gets-no-pods).
- If a scheduling event says that no node fits, check that a node has the free GPU, CPU and memory listed in [Before you begin](#before-you-begin).
- A multi-attach error, or another volume event, means that the pod's node can't mount the claim. Usually another node has mounted a `ReadWriteOnce` claim. Use a `ReadOnlyMany` or `ReadWriteMany` claim.
- If the container starts and then exits, read its logs with `kubectl logs -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct`. A sub-path that points at a parent of the model's directory can pass Step 3, but the runtime can't load the model from it. Point the sub-path at the directory that holds `config.json`.

## Clean up

Delete the InferenceService and the model:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
kubectl delete basemodel llama-3-2-1b-instruct -n llama-demo
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
basemodel.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
```

For a ClusterBaseModel, run `kubectl delete clusterbasemodel llama-3-2-1b-instruct` instead of the second command.

Deleting the model deletes its metadata Jobs. The claim and the weights on it stay as they are.

OME keeps the ServiceAccount `ome-model-metadata` in `llama-demo` and the RoleBinding `ome-model-metadata-llama-demo` in `ome`, so that other models in `llama-demo` can use them. To remove them:

```bash
kubectl delete rolebinding -n ome -l models.ome/metadata-source-namespace=llama-demo
kubectl delete serviceaccount ome-model-metadata -n llama-demo
```

```output
rolebinding.rbac.authorization.k8s.io "ome-model-metadata-llama-demo" deleted from ome namespace
serviceaccount "ome-model-metadata" deleted from llama-demo namespace
```

OME creates them again the next time it runs a metadata Job in `llama-demo`.

## Next steps

- [Base models](../../concepts/models/base-models.md): every storage scheme, and what OME learns from a model.
- [Serve models from node-local storage](serve-models-from-local-storage.md): serve weights that are already on the nodes' disks, with a `local://` URI.
- [Expose a service without ingress](../networking/expose-without-ingress.md): reach the model through a LoadBalancer or NodePort Service.
- [Configure ingress](../networking/configure-ingress.md): expose the model through Kubernetes Ingress or Gateway API.
