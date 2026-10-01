---
title: Serve models from node-local storage
description: "Serve model weights already on your nodes' disks with a local:// storage URI, which OME validates in place and mounts read-only."
---

Serve a model from weights that are already on your GPU nodes' disks, with nothing to download. You point a [ClusterBaseModel](../../concepts/models/base-models.md) at their directory with a `local://` URI. OME then runs the pods of an [InferenceService](../../concepts/serving/inference-services.md) only on the nodes that hold the files, with the directory mounted read-only. The [model agent](../operate-ome/model-agent.md) checks the files on each node, and OME never deletes them. Since v1.3, a [base model is optional](../../concepts/models/base-models.md), but it's what tells OME where these weights are. The steps serve `meta-llama/Llama-3.2-1B-Instruct`.

<div class="prerequisites" markdown>

- OME installed in the namespace `ome` with the model agent, which needs `modelAgent.enabled: true` since v1.3, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md#step-3-install-ome).
- The files of `meta-llama/Llama-3.2-1B-Instruct` in a Hugging Face layout, `config.json` plus the `.safetensors` weight files, and a way to copy them onto the nodes.
- Two amd64 GPU nodes that run the model agent, `gpu-node-1` and `gpu-node-2`. The serving pod needs an NVIDIA GPU, 10 CPUs and 30 GiB of memory free on one of them, which is what the runtime requests.
- A namespace `llama-demo` at the `privileged` Pod Security level, because the serving pods mount the weights with a hostPath volume. If the cluster's default level is stricter, label the namespace `pod-security.kubernetes.io/enforce=privileged`. Other admission policies can block hostPath volumes too.
- The [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-2-1b-instruct`. Check with `kubectl get clusterservingruntime srt-llama-3-2-1b-instruct`. If it's missing, apply [`config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml) from a clone of the OME repository.

</div>

## The `local://` URI and `path`

A `local://` URI is `local://` followed by the directory's absolute path, so it has three slashes: `local:///mnt/data/models/llama-3-2-1b-instruct`.

Set `storage.path` to the same directory. The agent checks the directory in `path`, and the serving pods mount it at the same path, with `MODEL_PATH` set to it.

!!! warning "Always set `storage.path`"
    Without `path`, the model still becomes `Ready`, but its serving pods get no model volume, and SGLang fails to load the model.

## Step 1: Stage the weights and label the nodes

Copy the model's files into the same directory on each node that should serve the model. Use your own tools, such as your node image or a copy job. Put `config.json` and the weight files at the top of the directory, because the runtime loads them from there. By default, the agent sees only the host directory in `modelAgent.hostPath`, `/mnt/data/models`, so the steps use `/mnt/data/models/llama-3-2-1b-instruct` on `gpu-node-1` and `gpu-node-2`. For another directory, see [Use another directory](#use-another-directory).

For an OME-managed download onto the nodes, use an `hf://` or `oci://` model with [the model agent](../operate-ome/model-agent.md) instead. To prepare one shared PVC copy, follow [Stage model weights](stage-model-weights.md). Its `ome-agent replica` command supports PVC and OCI destinations, not `local://`; it doesn't populate every node's disk.

Check that the agent on each node sees the files:

```bash
for node in gpu-node-1 gpu-node-2; do
  pod=$(kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
    --field-selector spec.nodeName=$node -o name)
  echo "$node: $(kubectl exec -n ome $pod -- ls /mnt/data/models/llama-3-2-1b-instruct/config.json)"
done
```

```output
gpu-node-1: /mnt/data/models/llama-3-2-1b-instruct/config.json
gpu-node-2: /mnt/data/models/llama-3-2-1b-instruct/config.json
```

If `ls` reports `No such file or directory` on a node, check the path there.

The model in Step 2 selects the two nodes by a label. Without a [node selector](../../concepts/models/base-models.md#choose-the-nodes), the agent on every other node marks the model `Failed` there. If the two nodes already share a label that other nodes lack, such as their node pool's label, use it in Step 2 and skip the rest of this step. Otherwise, label them:

```bash
kubectl label node gpu-node-1 gpu-node-2 weights.example.com/llama-3-2-1b-instruct=true
```

```output
node/gpu-node-1 labeled
node/gpu-node-2 labeled
```

The agent reads its node's labels when it starts, so restart the agent pods on the two nodes. The DaemonSet starts new ones:

```bash
for node in gpu-node-1 gpu-node-2; do
  kubectl delete pod -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
    --field-selector spec.nodeName=$node
done
```

```output
pod "ome-model-agent-daemonset-4xkqz" deleted from ome namespace
pod "ome-model-agent-daemonset-9mfpt" deleted from ome namespace
```

The new agents check all the models on their nodes again, so those models show `Updating` there for a while.

## Step 2: Create the model

Create a ClusterBaseModel whose URI and `path` both name the directory from Step 1, and whose node selector matches the nodes' label:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-2-1b-instruct-local
spec:
  storage:
    storageUri: "local:///mnt/data/models/llama-3-2-1b-instruct"
    path: /mnt/data/models/llama-3-2-1b-instruct
    nodeSelector:
      weights.example.com/llama-3-2-1b-instruct: "true"
```

Leave out `modelArchitecture`, `modelFormat` and the other metadata fields. OME fills in the empty ones from the model's files in Step 3, and keeps any that you set.

Use only lowercase letters, digits and hyphens in the model's name. OME names the serving pods' volume after the model, and Kubernetes rejects a volume name with a dot, such as `llama-3.2-1b-instruct`.

For a namespaced model, see [Use a BaseModel](#use-a-basemodel).

Apply the file:

```bash
kubectl apply -f model.yaml
```

```output
clusterbasemodel.ome.io/llama-3-2-1b-instruct-local created
```

## Step 3: Watch the model become Ready

On each node that the model selects, the agent checks the directory and reads the model's metadata from `config.json`. Watch the model's state:

```bash
kubectl get clusterbasemodel llama-3-2-1b-instruct-local -w \
  -o custom-columns=NAME:.metadata.name,STATE:.status.state
```

```output
NAME                          STATE
llama-3-2-1b-instruct-local   In_Transit
llama-3-2-1b-instruct-local   Ready
```

Press Ctrl+C once the state is `Ready`. The state can repeat, or show `<none>` at first. After the restart in Step 1, this can take a while: see [How downloads run](../operate-ome/model-agent.md#workers-and-queues).

The model is `Ready` as soon as one node is, so check both nodes:

```bash
kubectl get clusterbasemodel llama-3-2-1b-instruct-local \
  -o 'custom-columns=STATE:.status.state,READY:.status.nodesReady[*],FAILED:.status.nodesFailed[*]'
```

```output
STATE   READY                   FAILED
Ready   gpu-node-1,gpu-node-2   <none>
```

Both nodes should be in `READY`. If a node is missing there, or the model stays `In_Transit`, see [The model isn't ready on a node](#the-model-isnt-ready-on-a-node).

Check what OME recorded from `config.json`:

```bash
kubectl get clusterbasemodel llama-3-2-1b-instruct-local \
  -o custom-columns=ARCHITECTURE:.spec.modelArchitecture,FORMAT:.spec.modelFormat.name,FRAMEWORK:.spec.modelFramework.name,VERSION:.spec.modelFramework.version
```

```output
ARCHITECTURE       FORMAT        FRAMEWORK      VERSION
LlamaForCausalLM   safetensors   transformers   4.45.0.dev0
```

`VERSION` comes from `transformers_version` in your copy's `config.json`, so yours can differ. If `FORMAT` is `<none>`, the agents failed to read `config.json`. Fix that before Step 4, as in [The webhook rejects the InferenceService](#the-webhook-rejects-the-inferenceservice).

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
    name: llama-3-2-1b-instruct-local
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

`spec.model.kind` defaults to `ClusterBaseModel`, so `isvc.yaml` leaves it out. The runtime sets `autoSelect: false`, so OME uses it only when an InferenceService names it.

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

Check the pod's node and its model volume:

```bash
kubectl get pods -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct \
  -o 'custom-columns=NODE:.spec.nodeName,HOSTPATH:.spec.volumes[?(@.name=="llama-3-2-1b-instruct-local")].hostPath.path,MOUNTPATH:.spec.containers[*].volumeMounts[?(@.name=="llama-3-2-1b-instruct-local")].mountPath,READONLY:.spec.containers[*].volumeMounts[?(@.name=="llama-3-2-1b-instruct-local")].readOnly'
```

```output
NODE         HOSTPATH                                 MOUNTPATH                                READONLY
gpu-node-1   /mnt/data/models/llama-3-2-1b-instruct   /mnt/data/models/llama-3-2-1b-instruct   true
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
  -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is a hostPath volume?"}], "max_tokens": 64}' \
  | jq -r '.choices[0].message.content'
```

```output
A hostPath volume mounts a file or directory from the node's filesystem into a pod.
```

The answer varies from run to run.

## Use another directory

To keep the weights outside `/mnt/data/models`, set `modelAgent.hostPath` to their directory, as [Install OME](../../getting-started/install.md#step-3-install-ome) describes. Or mount their directory into the agent as well, at the same path as on the host, because serving pods mount the host directory that `path` names. This values file mounts `/raid/models`:

```yaml title="values.yaml"
modelAgent:
  enabled: true
  extraVolumes:
    - name: raid-models
      hostPath:
        path: /raid/models
        type: DirectoryOrCreate
  extraVolumeMounts:
    - name: raid-models
      mountPath: /raid/models
```

`DirectoryOrCreate` creates the directory on nodes that lack it, so the agent pod starts there too. Add the values to the values file you install `ome-resources` with, then upgrade the release, as in [Step 1](../operate-ome/model-agent.md#step-1-turn-on-the-agent) of Run the model agent.

## Use a BaseModel

To make the model available only in `llama-demo`, create a [BaseModel](../../concepts/models/base-models.md#basemodel-and-clusterbasemodel) there instead, with the same `spec`. In the commands on the model, use `basemodel` and `-n llama-demo`. In `isvc.yaml`, add `kind: BaseModel` under `model`. The model's label on the nodes then has the key `models.ome.io/llama-demo.basemodel.llama-3-2-1b-instruct-local`.

## Troubleshooting

### The model isn't ready on a node

Check the model's label on every node:

```bash
kubectl get nodes \
  -o 'custom-columns=NODE:.metadata.name,STATE:.metadata.labels.models\.ome\.io/clusterbasemodel\.llama-3-2-1b-instruct-local'
```

For example, when the files are missing on `gpu-node-2`:

```output
NODE         STATE
gpu-node-1   Ready
gpu-node-2   Failed
gpu-node-3   <none>
```

| `STATE` | Meaning | What to do |
| --- | --- | --- |
| `Failed` | The agent's check failed. | Read the agent's log, below. |
| `Updating` | The agent is still checking the model. | If it stays, see [A model stays `Updating`](../operate-ome/model-agent.md#a-model-stays-updating). |
| `<none>` | The agent hasn't processed the model. That's expected on `gpu-node-3` and other nodes outside the model's node selector. | On a node that should have the model, see [A node never gets the label](../operate-ome/model-agent.md#a-node-never-gets-the-label). |

Read the agent's log on that node, here `gpu-node-2`:

```bash
pod=$(kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
  --field-selector spec.nodeName=gpu-node-2 -o name)
kubectl logs -n ome $pod | grep llama-3-2-1b-instruct
```

On a `Failed` node, look for one of these lines:

| Log line | Cause | Fix |
| --- | --- | --- |
| `Local model path does not exist` | The agent can't see the directory. | Put the files at that path, or [mount their directory into the agent](#use-another-directory). |
| `invalid local storage URI format: missing path` | The URI is `local://` with nothing after it. | Add the directory to the URI. |
| `unknown storage type for URI` | The URI has the wrong scheme. | Use `local://`. |

The agent doesn't retry a failed model on its own. After you fix the files, change an annotation on the model, with a new value each time, and the agents check it again:

```bash
kubectl annotate clusterbasemodel llama-3-2-1b-instruct-local example.com/recheck=1 --overwrite
```

```output
clusterbasemodel.ome.io/llama-3-2-1b-instruct-local annotated
```

### The webhook rejects the InferenceService

Since v1.3, the webhook rejects `isvc.yaml` until OME records the model's format in Step 3:

```text
runtime srt-llama-3-2-1b-instruct does not support model llama-3-2-1b-instruct-local: invalid model specification: modelFormat.name: model format name is required
```

Wait until the model is `Ready`, as in [Step 3](#step-3-watch-the-model-become-ready), then apply `isvc.yaml` again.

If the model is `Ready` but `FORMAT` was `<none>` in Step 3, the agents failed to read `config.json`. Their logs have `Failed to parse and update model config for local model`, with the reason. Put `config.json` at the top of the model's directory on each node, and [make the agents check the model again](#the-model-isnt-ready-on-a-node). Or set `modelFormat` and `modelFramework` in `model.yaml` yourself, as in [Set the metadata yourself](../../concepts/models/base-models.md#set-the-metadata-yourself).

### The serving pods aren't created

If the InferenceService has no pods, look for events about pods that Kubernetes refused to create:

```bash
kubectl get events -n llama-demo --field-selector reason=FailedCreate
```

When Pod Security rejects the hostPath volume, the message contains `hostPath volumes` at the `baseline` level, or `restricted volume type "hostPath"` at the `restricted` level. Use a namespace at the `privileged` level, as in [Before you begin](#before-you-begin).

With no such events, read the controller's logs with `kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1`:

- `a lowercase RFC 1123 label must consist of` means that the model's name has a dot. Create the model again under a name without dots, and use the new name in `isvc.yaml`.
- For a BaseModel, check that `isvc.yaml` sets `kind: BaseModel`. See [An InferenceService that serves a BaseModel gets no pods](../troubleshoot/troubleshoot-an-inferenceservice.md#an-inferenceservice-that-serves-a-basemodel-gets-no-pods).

### The serving pods don't start

Run `kubectl describe pod -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct`, and read the `Events` at the end of each pod's description:

- If a scheduling event says that no node fits, check that a node with the model's `Ready` label also has a free GPU, 10 CPUs and 30 GiB of memory.
- If the container starts and then exits, read its logs with `kubectl logs -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct`. If they show the literal text `$(MODEL_PATH)`, see [The serving pods have no model volume](#the-serving-pods-have-no-model-volume). Files in a subdirectory can pass Step 3, but the runtime loads the model from the top of the directory, so move them up.

### The serving pods have no model volume

When the pod check in [Step 4](#step-4-deploy-an-inferenceservice) shows `<none>` for the model volume, the model is missing `storage.path`. Add `path` to `model.yaml`, with the same directory as the URI, and apply it. The agents check the model again on their own.

A change to the model doesn't reach the serving pods, so delete the InferenceService and create it again:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

## Clean up

Delete the InferenceService and the model:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
kubectl delete clusterbasemodel llama-3-2-1b-instruct-local
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
clusterbasemodel.ome.io "llama-3-2-1b-instruct-local" deleted
```

If you labeled the nodes in Step 1, remove the label:

```bash
kubectl label node gpu-node-1 gpu-node-2 weights.example.com/llama-3-2-1b-instruct-
```

```output
node/gpu-node-1 unlabeled
node/gpu-node-2 unlabeled
```

To free the disk space, delete `/mnt/data/models/llama-3-2-1b-instruct` on each node yourself.

## Next steps

- [Serve models from a PVC](serve-models-from-pvc.md): serve weights from a PersistentVolumeClaim, with no copy on each node.
- [Base models](../../concepts/models/base-models.md): every storage scheme, how OME picks the nodes, and what it learns from a model.
- [Run the model agent](../operate-ome/model-agent.md): the agent's settings, its logs, and how it processes models.
