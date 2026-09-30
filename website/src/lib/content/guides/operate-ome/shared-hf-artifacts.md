---
title: Share Hugging Face artifacts
description: Keep one downloaded copy of a Hugging Face snapshot per node and share it across BaseModels and ClusterBaseModels with downloadPolicy ReuseIfExists.
since: v1.3
---

Set `storage.downloadPolicy: ReuseIfExists` on models that serve the same Hugging Face snapshot, such as a [ClusterBaseModel](../../concepts/models/base-models.md) and the BaseModels that teams create in their namespaces. A node then keeps one copy of the snapshot instead of one per model. The [model agent](model-agent.md) downloads it once, links each model's `path` to it, and deletes it when no model uses it. In the steps, a ClusterBaseModel and a BaseModel share `Qwen/Qwen2.5-7B-Instruct`, and you check the shared copy on a node.

<div class="prerequisites" markdown>

- OME installed from the `ome-resources` chart in the namespace `ome`, and `kubectl` access to the cluster, with permission to run `kubectl exec` in `ome`. See [Install OME](../../getting-started/install.md).
- The model agent turned on, as [Run the model agent](model-agent.md#step-1-turn-on-the-agent) describes. Since v1.3, the chart installs it only when you set `modelAgent.enabled: true`. On v1.2.2, the chart always installs it.
- Nodes that run the agent and can reach Hugging Face, with room for one copy of `Qwen/Qwen2.5-7B-Instruct`, which needs no token. The examples use three GPU nodes: `gpu-node-1`, `gpu-node-2` and `gpu-node-3`.
- The namespace `llama-demo`.

</div>

## How sharing works

The default policy, `AlwaysDownload`, puts a copy of the files in each model's `path`. See [DownloadPolicy](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-DownloadPolicy) in the API reference.

A snapshot is a Hugging Face repository at one commit. A node resolves the revision after `@` in an `hf://` URI, `main` by default, to its commit. Models that resolve to the same repository and commit share a copy, and so can [OCI mirrors](#share-oci-mirrors-of-hugging-face-snapshots) of that snapshot. Models with other sources ignore `downloadPolicy`.

Keep the `path` of shared models inside the models root, which is `modelAgent.hostPath` in the chart, `/mnt/data/models` by default. Models inside the root all use one copy on a node, and a ClusterBaseModel outside it fails. With the paths directly under the root, as in the steps, the copy is in `<models root>/_artifacts/<repository>/<commit>`.

The agent checks the copy against the commit's list of files on Hugging Face after a download and after it restarts, and downloads damaged or missing files again. During a repair, the models that use the copy are `Failed` on the node, and the agent restores their state when it finishes. If Hugging Face stays unreachable for 30 minutes after an agent restart, the models that use the copy fail on that node, even with a pinned commit. Restart the agent pod on the node once Hugging Face is back.

When you delete a model, or it stops selecting a node, the agent on that node removes the model's link. It keeps the link when another model has the same `path`, or when the model has the [reserve label](configure-model-artifact-retention.md#step-1-keep-a-models-files-with-modelsomereserve-model-artifact) `models.ome/reserve-model-artifact: "true"`. The agent deletes the shared copy once no model uses it and no link to it is left under the models root. A later model with the same snapshot reuses a copy that the agent kept.

!!! tip "Pin the commit"
    Nodes resolve `main` whenever they process a model, including after the agent restarts. Once `main` moves, models that a node processes again switch to the new commit, and the node downloads it. To keep your models on one snapshot, put the commit in the URI, as in `hf://Qwen/Qwen2.5-7B-Instruct@<commit>`.

## Step 1: Share a Hugging Face snapshot

Define a ClusterBaseModel and a BaseModel with the same source and `downloadPolicy: ReuseIfExists`. Their paths are directly under the chart's models root, `/mnt/data/models`. If you changed `modelAgent.hostPath`, use that directory instead.

```yaml title="models.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: qwen2-5-7b-instruct
spec:
  storage:
    storageUri: hf://Qwen/Qwen2.5-7B-Instruct
    path: /mnt/data/models/qwen2-5-7b-instruct
    downloadPolicy: ReuseIfExists
---
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: qwen2-5-7b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: hf://Qwen/Qwen2.5-7B-Instruct
    path: /mnt/data/models/llama-demo-qwen2-5-7b-instruct
    downloadPolicy: ReuseIfExists
```

Create them:

```bash
kubectl apply -f models.yaml
```

```output
clusterbasemodel.ome.io/qwen2-5-7b-instruct created
basemodel.ome.io/qwen2-5-7b-instruct created
```

On each node, the model that the agent processes first downloads the snapshot into `/mnt/data/models/_artifacts`. The other model waits for the download, then links its `path` to the copy without downloading.

!!! warning "Downloads that take over 30 minutes"
    A model waits at most 30 minutes for another model's download of the same snapshot, and then fails on that node. For a large model, create one model first, and the others once it's `Ready` on every node.

Wait until both models are `Ready` on every node. Their labels on the nodes show it:

```bash
kubectl get nodes -o 'custom-columns=NODE:.metadata.name,CLUSTERBASEMODEL:.metadata.labels.models\.ome\.io/clusterbasemodel\.qwen2-5-7b-instruct,BASEMODEL:.metadata.labels.models\.ome\.io/llama-demo\.basemodel\.qwen2-5-7b-instruct'
```

```output
NODE         CLUSTERBASEMODEL   BASEMODEL
gpu-node-1   Ready              Ready
gpu-node-2   Ready              Ready
gpu-node-3   Ready              Ready
```

## Step 2: Check the shared copy on the node

Read the files on `gpu-node-1` through its agent pod, which mounts the models root at the same path as the node. Find the pod:

```bash
kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
  --field-selector spec.nodeName=gpu-node-1 -o name
```

```output
pod/ome-model-agent-daemonset-4xkqz
```

Read the links at the paths of the two models:

```bash
kubectl exec -n ome ome-model-agent-daemonset-4xkqz -- \
  readlink /mnt/data/models/qwen2-5-7b-instruct /mnt/data/models/llama-demo-qwen2-5-7b-instruct
```

```output
_artifacts/Qwen/Qwen2.5-7B-Instruct/<commit>
_artifacts/Qwen/Qwen2.5-7B-Instruct/<commit>
```

Both paths are links, relative to `/mnt/data/models`, to the same directory. `<commit>` stands for the commit that `main` pointed to when the node resolved it. Serving pods mount the model's `path`, and read the shared files through the link.

Don't create, change or delete anything in `_artifacts`, or in the agent's `.hf-artifact-locks` directories next to it and next to each shared `path`.

## Move an existing model onto the shared copy

A node links a model's `path` to the shared copy only when nothing is at that `path` yet. So when you set `ReuseIfExists` on a model that's already on the nodes, change its `path` in the same edit. The nodes link the new `path` to the shared copy, and download the copy first where they have none. Check the new `path` with the `readlink` command from [Step 2](#step-2-check-the-shared-copy-on-the-node). Serving pods can keep mounting the old `path` for hours. To move them to the new `path`, change each InferenceService that serves the model, for example by adding an annotation. Once no serving pod mounts the old `path`, delete its files through the agent pod on each node, as [Keep downloaded model files](configure-model-artifact-retention.md#clean-up) shows.

After an upgrade from v1.2.2, the models that shared files through `ReuseIfExists` get copies of their own the next time a node processes them. Move them the same way.

## Share OCI mirrors of Hugging Face snapshots

An `oci://` model whose objects mirror a Hugging Face snapshot can share the snapshot's copy with `hf://` models and other mirrors. Name the snapshot in two annotations on the model, and end the object path in `storageUri` with `<hf-model-id>/<hf-model-sha>`:

| Annotation | Value |
| --- | --- |
| `hf-model-id` | The Hugging Face model ID, such as `meta-llama/Llama-3.1-8B-Instruct`. |
| `hf-model-sha` | The snapshot's full 40-character commit, not a branch or tag. |

Set `path` and `downloadPolicy: ReuseIfExists` too:

```yaml title="mirror.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b-instruct
  annotations:
    hf-model-id: meta-llama/Llama-3.1-8B-Instruct
    hf-model-sha: 0e9e39f249a16976918f6564b8830bc894c89659
spec:
  storage:
    storageUri: oci://n/mytenancy/b/model-mirror/o/hf-models/meta-llama/Llama-3.1-8B-Instruct/0e9e39f249a16976918f6564b8830bc894c89659
    path: /mnt/data/models/llama-3-1-8b-instruct
    downloadPolicy: ReuseIfExists
```

The agent trusts the annotations, and checks the files against object storage, not Hugging Face. So name the snapshot that the objects came from, and keep the objects of a mirrored commit unchanged. [Credentials](../../concepts/models/base-models.md#credentials) covers how the agent authenticates to OCI.

Changing only `downloadPolicy` doesn't make the nodes process an existing `oci://` model again. Set the policy and the annotations when you create the model, or [move the model](#move-an-existing-model-onto-the-shared-copy) to a new `path` in the same edit.

## Troubleshooting

### A model has a copy of its own

The `readlink` command from [Step 2](#step-2-check-the-shared-copy-on-the-node) prints nothing for a model's `path`, and `kubectl exec` reports `command terminated with exit code 1`. The model has a copy of its own on that node. The usual causes:

- The `path` already held files when you set `ReuseIfExists`.
- The model's `path`, or another shared model's `path`, is outside the models root.
- An OCI mirror's annotations are missing or invalid, or its object path doesn't end with them.
- The node couldn't resolve the revision to a commit, for example because Hugging Face was unreachable. The [agent's log](model-agent.md#troubleshooting) has `Cannot resolve HF revision for`, with the model and the error.

Fix the cause, then [move the model](#move-an-existing-model-onto-the-shared-copy) to a new `path`: the files already in its `path` stay its own.

### A model is `Failed` on a node

The model's label on the node is `Failed`, and the agent's log has `task failed with error:` followed by one of these messages:

| Message | Cause |
| --- | --- |
| `child path <path> is outside cluster model root <root>` | A ClusterBaseModel's `path` is outside the models root. |
| `invalid shared Hugging Face artifact child path` | The `path` is relative, or has `..` in it. |
| `direct HF child path cannot be inside the shared artifact directory`, or `overlaps the shared artifact directory` for an OCI mirror | A directory in `path` is named `_artifacts`. |
| `shared Hugging Face artifact child path is required` | An OCI mirror has no `path`. |
| `retry budget exhausted` | The model waited more than 30 minutes for the shared copy, for example while another model downloaded it or while Hugging Face was unreachable. |
| `failed content validation` | The files downloaded for an `hf://` model don't match the commit's list of files. |
| `cannot replace child path <path> still used by another model` | You changed the model's source, commit or policy, and another model has the same `path`. Give this model its own `path`. |
| `legacy HF artifact has descendants` | Other models on the node link to its files through v1.2.2's `ReuseIfExists`. Restart the agent pod once they have copies of their own, or give this model a new `path`. |

The agent doesn't try a failed model again by itself. After you fix the cause, restart the agent pod on the node: see [A model stays `Updating`](model-agent.md#a-model-stays-updating).

## Clean up

Delete the two models:

```bash
kubectl delete -f models.yaml
```

```output
clusterbasemodel.ome.io "qwen2-5-7b-instruct" deleted
basemodel.ome.io "qwen2-5-7b-instruct" deleted from llama-demo namespace
```

The command returns once the agent on every node has removed both models: see [Delete a model](../../concepts/models/base-models.md#deleting-a-model). Check that the shared copy is gone from the node:

```bash
kubectl exec -n ome ome-model-agent-daemonset-4xkqz -- ls /mnt/data/models/_artifacts/Qwen/Qwen2.5-7B-Instruct
```

The command prints nothing: the agent deleted the copy's directory, and left the empty directories above it.

## Next steps

- [Run the model agent](model-agent.md): the agent's settings, and how it downloads and checks models.
- [Base models](../../concepts/models/base-models.md): how a model chooses its nodes, and what happens when you delete it.
- [Keep downloaded model files](configure-model-artifact-retention.md): keep a model's files on the nodes after you delete it.
