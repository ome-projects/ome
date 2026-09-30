---
title: Keep downloaded model files
description: Keep a model's downloaded files on the nodes after you delete the model, with the models.ome/reserve-model-artifact label.
---

When you delete a [base model](../../concepts/models/base-models.md), or change the nodes it selects, the [model agent](model-agent.md) deletes the files it downloaded there. Give the model the label `models.ome/reserve-model-artifact: "true"` first, and the files stay. You can then create the model again later, even after you reinstall OME, and serve it without downloading the weights again.

<div class="prerequisites" markdown>

- [OME installed](../../getting-started/install.md) in the namespace `ome`, with the [model agent turned on](model-agent.md#step-1-turn-on-the-agent).
- `kubectl` access to the cluster, with permission to run `kubectl exec` in `ome`.

</div>

## Step 1: Add the reserve label {#step-1-keep-a-models-files-with-modelsomereserve-model-artifact}

The steps use the ClusterBaseModel `qwen2-5-7b-instruct`, from `hf://Qwen/Qwen2.5-7B-Instruct`, on three GPU nodes: `gpu-node-1`, `gpu-node-2` and `gpu-node-3`. Its `path` is under `/mnt/data/models`, the default `modelAgent.hostPath`.

The label works on BaseModels and ClusterBaseModels, and its value is `true` in any letter case. Its key starts with `models.ome/`, not `models.ome.io/` like the node labels. Add it before you delete the model or change its `nodeSelector` or `nodeAffinity`: a label that you add afterwards doesn't bring the files back.

=== "New model"

    Save this ClusterBaseModel as `model.yaml`:

    ```yaml title="model.yaml"
    apiVersion: ome.io/v1beta1
    kind: ClusterBaseModel
    metadata:
      name: qwen2-5-7b-instruct
      labels:
        models.ome/reserve-model-artifact: "true"
    spec:
      storage:
        storageUri: hf://Qwen/Qwen2.5-7B-Instruct
        path: /mnt/data/models/qwen/qwen2-5-7b-instruct
    ```

    Create it:

    ```bash
    kubectl apply -f model.yaml
    ```

    ```output
    clusterbasemodel.ome.io/qwen2-5-7b-instruct created
    ```

=== "Existing model"

    Add the label to the model:

    ```bash
    kubectl label clusterbasemodel qwen2-5-7b-instruct models.ome/reserve-model-artifact=true
    ```

    ```output
    clusterbasemodel.ome.io/qwen2-5-7b-instruct labeled
    ```

    Each node that has the model then checks its files again. Until it's done, the model's label on that node is `Updating`, and new serving pods for the model avoid the node. Running pods keep running.

    !!! warning "If the model has no `path`"
        Deleting an `hf://` or `oci://` model that has no `path` field makes the agent crash and restart on every node, and the model stays. This is a known bug: to avoid it, set the model's `path` to an empty string first, with the patch in [The agent keeps restarting](#the-agent-keeps-restarting).

Check that the model is `Ready` on the nodes:

```bash
kubectl get nodes \
  -o 'custom-columns=NODE:.metadata.name,STATE:.metadata.labels.models\.ome\.io/clusterbasemodel\.qwen2-5-7b-instruct'
```

```output
NODE         STATE
gpu-node-1   Ready
gpu-node-2   Ready
gpu-node-3   Ready
```

## Step 2: Delete the model and check the files

Delete the model:

```bash
kubectl delete clusterbasemodel qwen2-5-7b-instruct
```

```output
clusterbasemodel.ome.io "qwen2-5-7b-instruct" deleted
```

kubectl then waits until the agents have processed the deletion and OME has removed the model. The model's label is gone from the nodes:

```bash
kubectl get nodes \
  -o 'custom-columns=NODE:.metadata.name,STATE:.metadata.labels.models\.ome\.io/clusterbasemodel\.qwen2-5-7b-instruct'
```

```output
NODE         STATE
gpu-node-1   <none>
gpu-node-2   <none>
gpu-node-3   <none>
```

The files stay on the nodes. To see them, find the agent pod on a node, here `gpu-node-1`:

```bash
kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
  --field-selector spec.nodeName=gpu-node-1 -o name
```

```output
pod/ome-model-agent-daemonset-4xkqz
```

The agent pod mounts the models directory at the node's path, so check for a model file through it:

```bash
kubectl exec -n ome ome-model-agent-daemonset-4xkqz -- ls /mnt/data/models/qwen/qwen2-5-7b-instruct/config.json
```

```output
/mnt/data/models/qwen/qwen2-5-7b-instruct/config.json
```

## Reuse the kept files

To serve the kept files again, create a model with the same `storageUri` and `path`. The agents check the files against the source, and download only the ones that are missing or differ. The model becomes `Ready` without downloading the kept files again. This applies to the default `downloadPolicy`, `AlwaysDownload`. For `ReuseIfExists` models, see [Share Hugging Face artifacts](shared-hf-artifacts.md).

To serve the files without downloading anything, create a `local://` model with the directory as its URI and its `path`, such as `local:///mnt/data/models/qwen/qwen2-5-7b-instruct`. See [Serve models from node-local storage](../deploy-models/serve-models-from-local-storage.md).

## When the agent deletes files

The agent deletes a node's copy of an `hf://` or `oci://` model when you delete the model. It also deletes it when you change the model's `storage.nodeSelector` or `storage.nodeAffinity` and the node stops matching. It deletes the whole `storage.path` directory, including files that OME didn't put there and the directories of other models inside it. The source on Hugging Face or in object storage stays. You manage the files of `local://` and `pvc://` models yourself: for a PVC, see [Clean up](../deploy-models/serve-models-from-pvc.md#clean-up) in the PVC guide.

The agent keeps the files in these cases:

| Case | What to know |
| --- | --- |
| The model has the reserve label. | See [Step 1](#step-1-keep-a-models-files-with-modelsomereserve-model-artifact). |
| Another model, in any namespace, has the same `path`. | The paths must match exactly: `/mnt/data/models/qwen` and `/mnt/data/models/qwen/` differ. Deleting the last of these models deletes the files, unless it has the reserve label. |
| Other models share the files through `downloadPolicy: ReuseIfExists`. | See [Share Hugging Face artifacts](shared-hf-artifacts.md). |

When it keeps the files, the agent still removes the model's label from the node, and OME stops tracking the files. They stay until you delete them, as in [Clean up](#clean-up).

## Troubleshooting

### The files are gone after a deletion

Create the model again to download the files, and add the label before the next deletion. The usual causes:

- The label came too late. It must be on the model before you delete it or change its nodes.
- The key or value is wrong. The key is `models.ome/reserve-model-artifact`, and the value is `true`.
- Another model's deletion removed them, as [When the agent deletes files](#when-the-agent-deletes-files) describes.

### The model stays after you delete it

The model stays until the agents on its nodes report their copies deleted. The controller logs the nodes it's waiting for:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'Waiting for model to be cleared'
```

Each line names the model, and lists in `nodes` the nodes that still have it. On those nodes:

- If the agent isn't running, the deletion resumes when it starts again. If the node is gone for good, delete its Node object, and OME stops waiting for it.
- If the agent couldn't delete the files, its log has `Failed to delete model` or `Failed to delete Hugging Face model`, with the error. Fix the cause, such as the directory's permissions, then delete the agent pod, as in [A model stays `Updating`](model-agent.md#a-model-stays-updating). The new pod retries the deletion when it starts.
- If the agent keeps restarting, see [The agent keeps restarting](#the-agent-keeps-restarting).

### The agent keeps restarting

When the agent pods on every node restart again and again after you delete an `hf://` or `oci://` model, check the model's `path`. The lists in [Clean up](#clean-up) show `<none>` for a model that has no `path` field, and deleting such a model crashes the agent. This is a known bug. Set the model's `path` to an empty string:

```bash
kubectl patch clusterbasemodel qwen2-5-7b-instruct --type merge -p '{"spec":{"storage":{"path":""}}}'
```

```output
clusterbasemodel.ome.io/qwen2-5-7b-instruct patched
```

For a BaseModel, run `kubectl patch basemodel <name> -n <namespace>` with the same patch. The agents then process the deletion, and OME removes the model. Use an empty string, not another directory: the agents could delete that directory.

An `oci://` model with no `path` also crashes the agent when the agent downloads it. Give every `hf://` and `oci://` model a `path`, as [How weights reach the nodes](../../concepts/models/base-models.md#how-weights-reach-the-nodes) describes.

## Clean up

Before you delete the kept files, check that no model uses the directory. List the `path` of every model:

```bash
kubectl get clusterbasemodels -o 'custom-columns=NAME:.metadata.name,PATH:.spec.storage.path'
```

```output
NAME   PATH
```

```bash
kubectl get basemodels -A -o 'custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,PATH:.spec.storage.path'
```

```output
NAMESPACE   NAME   PATH
```

The example cluster has no other model, so the lists are empty. In your cluster, no model's `path` should be `/mnt/data/models/qwen/qwen2-5-7b-instruct` or a directory inside it. Then delete the directory through the agent pod on each node that kept it, starting with `gpu-node-1`:

```bash
kubectl exec -n ome ome-model-agent-daemonset-4xkqz -- rm -r /mnt/data/models/qwen/qwen2-5-7b-instruct
```

The command prints nothing when it succeeds. Repeat it with the agent pods on `gpu-node-2` and `gpu-node-3`.

## Next steps

- [Run the model agent](model-agent.md): the agent's settings, and how it downloads and checks models.
- [Share Hugging Face artifacts](shared-hf-artifacts.md): keep one copy of a Hugging Face model's files on each node for several models.
- [Base models](../../concepts/models/base-models.md): how a model chooses its nodes, and what happens when you delete it.
- [Serve models from node-local storage](../deploy-models/serve-models-from-local-storage.md): serve files that are already on the nodes.
- [Labels and annotations](../../reference/api/labels-and-annotations.md): the labels that OME reads, including `models.ome/reserve-model-artifact`.
