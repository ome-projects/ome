---
title: Run the model agent
description: Turn on the optional model agent for models stored on the nodes, and configure how it prepares them.
---

The model agent puts the weights of BaseModels and ClusterBaseModels on your nodes. It runs as the DaemonSet `ome-model-agent-daemonset`. On each node, it gets the files of the models that select the node, reads what each model is, and labels the node when the model is `Ready` there. OME runs a model's serving pods only on nodes with that label.

Since v1.3, the agent is optional and off by default. Turn it on to serve [base models](../../concepts/models/base-models.md) stored on the nodes: `hf://`, `oci://`, `local://` and `vendor://` models. Until you turn it on, those models stay `In_Transit`, and OME can't serve them. You don't need it for `pvc://` models, or for InferenceServices that name only a [runtime that loads its own weights](../../concepts/serving/inference-services.md#the-runtime).

<div class="prerequisites" markdown>

- OME installed from the `ome-resources` chart in the namespace `ome`, which the agent reports to, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- Helm, and the values file you install `ome-resources` with.
- For Step 3, `jq` and a model stored on the nodes. The examples use the ClusterBaseModel `qwen2-5-7b-instruct` from [Base models](../../concepts/models/base-models.md), on three GPU nodes that it selects: `gpu-node-1`, `gpu-node-2` and `gpu-node-3`.

</div>

## Step 1: Turn on the agent

On v1.2.2, the chart always installs the agent, so go to [Step 2](#step-2-check-the-agent-on-each-node). Otherwise, add the value to the values file you install `ome-resources` with:

```yaml title="values.yaml"
modelAgent:
  enabled: true
```

The agent runs on all nodes whose taints it tolerates, GPU nodes included. Each agent pod requests 10 CPUs and `100Gi` of memory, and its priority class, `system-node-critical`, lets Kubernetes evict lower-priority pods to make room. The agent keeps the weights in the node's `/mnt/data/models`, which needs room for the models that select the node. To use fewer nodes or another directory, also set `gpuNodesOnly`, `nodeSelector` or `hostPath` from [Helm values](#helm-values).

The value needs chart version 1.3.0 or later. Upgrade the release with the chart version you already run, 1.3.0 in this example:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Until v1.3 is released, upgrade from a checkout of `main` instead, as in [Install from source](../../getting-started/install.md#install-from-source).

Helm prints `Release "ome" has been upgraded. Happy Helming!`. Wait for the agent pods to start:

```bash
kubectl rollout status daemonset/ome-model-agent-daemonset -n ome
```

While the pods start, kubectl prints `Waiting for daemon set` lines. When all of them are available, it prints:

```output
daemon set "ome-model-agent-daemonset" successfully rolled out
```

## Step 2: Check the agent on each node

Each node that should hold models needs a ready agent pod. List the agent pods with their nodes:

```bash
kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
  -o 'custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,READY:.status.containerStatuses[0].ready'
```

```output
NAME                              NODE         READY
ome-model-agent-daemonset-4xkqz   gpu-node-1   true
ome-model-agent-daemonset-9mfpt   gpu-node-2   true
ome-model-agent-daemonset-tq7wd   gpu-node-3   true
```

If a node is missing, or a pod shows `false`, see [Troubleshooting](#troubleshooting).

## Step 3: Check where a model is ready

Show the label of `qwen2-5-7b-instruct` on all nodes:

```bash
kubectl get nodes \
  -o 'custom-columns=NODE:.metadata.name,STATE:.metadata.labels.models\.ome\.io/clusterbasemodel\.qwen2-5-7b-instruct'
```

```output
NODE         STATE
gpu-node-1   Ready
gpu-node-2   Ready
gpu-node-3   Updating
```

The model is ready on `gpu-node-1` and `gpu-node-2`. On `gpu-node-3`, the agent is still downloading or checking the files, and the label turns `Ready`, or `Failed`, when it's done. Nodes outside the model's selector show `<none>`. For a BaseModel, the label key is `models.ome.io/{namespace}.basemodel.{name}`. Long names are shortened with a hash, as [Node labels and status](../../concepts/models/base-models.md#node-labels-and-status) describes.

The agent also records each model in a ConfigMap named after the node, in `ome`, under the key `clusterbasemodel.{name}` or `{namespace}.basemodel.{name}`. In the JSON entry, `status` matches the label, `config` holds the model's details once it's ready, and `progress` shows how far a Hugging Face download has got. Show the name and state from the entry on `gpu-node-1`:

```bash
kubectl get configmap gpu-node-1 -n ome \
  -o jsonpath='{.data.clusterbasemodel\.qwen2-5-7b-instruct}' | jq '{name, status}'
```

```output
{
  "name": "qwen2-5-7b-instruct",
  "status": "Ready"
}
```

## Configure the agent {#step-2-change-a-setting}

Set the agent's values under `modelAgent` in your values file, and upgrade the release as in [Step 1](#step-1-turn-on-the-agent).

### Helm values

| Value | Default | What it does |
| --- | --- | --- |
| `enabled` | `false` | Since v1.3. Installs the agent. |
| `gpuNodesOnly` | `false` | When `true`, runs the agent only on nodes with the `gpuNodeLabel` label. |
| `gpuNodeLabel` | `key: nvidia.com/gpu.present`, `value: "true"` | The node label that `gpuNodesOnly` selects. |
| `nodeSelector` | `{}` | More node labels that the agent's nodes must have. |
| `tolerations` | `nvidia.com/gpu` taints with the effect `NoSchedule` | The agent pod's tolerations. |
| `affinity` | Not set | The agent pod's affinity. |
| `resources` | 10 CPUs and `100Gi` of memory, requested and limited | The agent container's resources on each node. |
| `hostPath` | `/mnt/data/models` | The node directory for models, which must hold each model's `path`. Set `/raid/models` to apply the models in `config/models/`. |
| `extraVolumes`, `extraVolumeMounts` | `[]` | More volumes for the agent pod, and their mounts, for more host directories of models. |
| `env` | `{}` | The agent's [environment variables](#environment-variables). |
| `image` | `model-agent` from `global.hub`, at the chart's OME version | The agent's image: `hub`, `repository`, `tag` and `pullPolicy`. |
| `imagePullSecrets` | `global.imagePullSecrets` | Secrets for pulling the image. |
| `priorityClassName` | `system-node-critical` | The agent pod's priority class. |
| `modelVerificationConcurrency` | `0` | Since v1.3. How many `oci://` files the agent verifies at once, across all models. `0` means 2, the number of download workers. |
| `health.port` | `8080` | The probes' port. Leave it at `8080`, the port the agent listens on. |

!!! warning "A change to the agent's pod restarts every agent"
    The DaemonSet replaces the agent pods one node at a time. Each new agent checks its models again. Until it's done, their labels are `Updating`, and OME places no new serving pods for them on that node. Pods that already run there keep running.

### Environment variables

Set the agent's environment variables in `modelAgent.env`, a map of names to values:

```yaml title="values.yaml"
modelAgent:
  enabled: true
  env:
    LOG_LEVEL: debug
```

| Variable | What it does |
| --- | --- |
| `LOG_LEVEL` | The log level: `debug`, `info`, `warn` or `error`. `info` by default. |
| `HF_TOKEN` | The Hugging Face token for models without one of their own. |
| `ENDPOINT` | The Hugging Face endpoint, `https://huggingface.co` by default. |
| `MAX_CONCURRENT_DOWNLOADS` | How many files of a Hugging Face model download at once, 4 by default. |

The chart writes these values into the DaemonSet in plain text, so give each model its token through its Secret instead: see [Credentials](../../concepts/models/base-models.md#credentials). Other variables named after the agent's command-line flags have no effect, such as `DOWNLOAD_RETRY`, which the chart's values.yaml suggests.

## How downloads run {#workers-and-queues}

The chart runs two download workers, so at most two models download at once on a node, and the others wait their turn. After a restart, the agent's checks of its models queue up the same way.

The agent doesn't retry a failed model by itself. It tries again when [its pod restarts](#a-model-stays-updating), or when [a change to the model](../../concepts/models/base-models.md#choose-the-nodes) makes the nodes process it again.

When two `oci://` models name the same files, one downloads them, and the other reuses them once they're ready, or downloads them itself after 30 minutes.

With `storage.downloadPolicy: ReuseIfExists`, models that use the same Hugging Face files share one copy on each node: see [Share Hugging Face artifacts](shared-hf-artifacts.md).

### TensorRT-LLM models

For an `oci://` model with `modelFormat.name: tensorrtllm`, the agent downloads only the objects built for its node's GPU, whose names contain `/{gpu}/`, such as `/H100/`. It finds the GPU name by looking up the node's `node.kubernetes.io/instance-type` label in the ConfigMap `model-agent-config-map`, and uses an unlisted instance type as it is. When no object matches, the model fails on the node.

Since v1.3, `modelAgent.instanceTypeMap` doesn't change the ConfigMap, and the map lacks `BM.GPU.B300.8`, so a model with objects under `/B300/` fails on B300 nodes. This is a known bug. Edit the ConfigMap's `instance-type-map` key instead, and run `kubectl rollout restart daemonset/ome-model-agent-daemonset -n ome`. After each `helm upgrade`, check the key, and do both again if your entry is gone. On v1.2.2, add the instance type to `modelAgent.instanceTypeMap`.

## Metrics {#health-checks-and-metrics}

The agent serves Prometheus metrics at `/metrics` on port 8080. Most have the labels `model_type`, `namespace` and `name`. `model_type` is `BaseModel` or `ClusterBaseModel`, and `namespace` is empty for a ClusterBaseModel:

| Metric | Type | What it records |
| --- | --- | --- |
| `model_agent_downloads_success_total` | Counter | Models that became `Ready` on the node. |
| `model_agent_downloads_failed_total` | Counter | Models that failed on the node while the agent got their files. |
| `model_agent_download_duration_seconds` | Histogram | How long models took to become `Ready`. The buckets stop at 51.2 seconds, so longer downloads land in `+Inf`. |
| `model_agent_download_bytes_total` | Counter | Bytes of the `oci://` models the agent verified, including files already on the node. |
| `model_agent_verifications_total` | Counter | Verifications of `oci://` downloads, with a `result` label: `success` or `failure`. |
| `model_agent_md5_checksum_failed_total` | Counter | Failed verifications. |
| `model_agent_verification_duration_seconds` | Histogram | How long verifications took. |
| `model_agent_rate_limit_total` | Counter | Hugging Face rate-limit errors. |

Since v1.3, `podMonitor.enabled: true` creates the PodMonitor `ome-model-agent` for the Prometheus Operator, which needs the Operator's `monitoring.coreos.com/v1` CRDs. `podMonitor.interval`, `podMonitor.scrapeTimeout` and `podMonitor.additionalLabels` tune it. [Collect metrics](metrics.md) covers the other ways to scrape the agent.

## Troubleshooting

The agent logs what it does with each model. Find the agent pod on a node, here `gpu-node-3`:

```bash
kubectl get pods -n ome -l app.kubernetes.io/component=ome-model-agent-daemonset \
  --field-selector spec.nodeName=gpu-node-3 -o name
```

```output
pod/ome-model-agent-daemonset-tq7wd
```

Then read its log lines about the model:

```bash
kubectl logs -n ome pod/ome-model-agent-daemonset-tq7wd | grep qwen2-5-7b-instruct
```

Look for the line that ends in `status to Updating before download`, where the agent starts on the model, and for errors after it.

### The model stays `In_Transit` or `Failed`

A model stays `In_Transit` while no node has it `Ready` or `Failed`:

- The agent is off: turn it on as in [Step 1](#step-1-turn-on-the-agent).
- The model's nodes lack an agent pod: see [A node never gets the label](#a-node-never-gets-the-label).
- The nodes are still downloading it: see [A model stays `Updating`](#a-model-stays-updating).

When the model is `Failed`, read the log of the agent on a node where its label is `Failed`:

| Cause | Log line |
| --- | --- |
| An `oci://` file differs from its size or MD5 in object storage, or changed during the download. | `MD5 or size mismatch for`, then `All download attempts failed for model` |
| The agent can't read the model's [Secret](../../concepts/models/base-models.md#credentials), which a ClusterBaseModel keeps in `ome`, so a gated model fails. | `Failed to retrieve secret` |
| A TensorRT-LLM model has no objects for the node's GPU: see [TensorRT-LLM models](#tensorrt-llm-models). | `no suitable objects found for shape` |
| Hugging Face answered with an error or a rate limit. | The error |

After you fix the cause, restart the agent pod on the node, as in [A model stays `Updating`](#a-model-stays-updating). For an `oci://` model, the new agent downloads only the mismatched files.

### A model stays `Updating`

A model's label can stay `Updating` because:

- The agent restarted: see the warning in [Configure the agent](#step-2-change-a-setting).
- The model is waiting its turn, or still downloading. For a Hugging Face model, `progress` in the node's ConfigMap shows how far it has got: see [Step 3](#step-3-check-where-a-model-is-ready).
- Another `oci://` model is downloading the same files. The log has `Same-path model` lines that end in `before starting duplicate download`: see [How downloads run](#workers-and-queues).
- The agent has no downloader for the model's scheme, `s3://`, `az://`, `gs://` or `github://`, and logs `unknown storage type`. See [Where the weights come from](../../concepts/models/base-models.md#where-the-weights-come-from).
- The agent can't parse the `oci://` URI, and logs `Failed to get target directory path for model` with the reason. Use the form from the same section.

After you fix the cause, delete the agent pod on the node. Its replacement processes all the models on the node again:

```bash
kubectl delete pod -n ome ome-model-agent-daemonset-tq7wd
```

kubectl confirms the deletion, and the DaemonSet starts a new agent pod on the node.

### A node never gets the label

When the model's label never appears on a node:

- The node has no agent pod: check the list from [Step 2](#step-2-check-the-agent-on-each-node). The agent's `tolerations`, `gpuNodesOnly`, `nodeSelector`, `affinity` and `resources` decide where it fits: see [Helm values](#helm-values).
- The model doesn't select the node. Compare its `storage.nodeSelector` and `storage.nodeAffinity` with the node's labels: see [Choose the nodes](../../concepts/models/base-models.md#choose-the-nodes).
- The node's labels changed after the agent started. The agent doesn't watch its node's labels, so restart its pod, as in [A model stays `Updating`](#a-model-stays-updating).
- It's a `pvc://` model, which needs no label.

### The agent pod isn't ready

The pod is ready once it can write to its models directory, `modelAgent.hostPath`. Ask a pod from [Step 2](#step-2-check-the-agent-on-each-node) for its readiness through the API server:

```bash
kubectl get --raw "/api/v1/namespaces/ome/pods/ome-model-agent-daemonset-tq7wd:8080/proxy/healthz?verbose"
```

```output
[+]model-agent-health ok
healthz check passed
```

When the check fails, kubectl reports an error, and the answer ends in `reason withheld`. Ask the same pod for `/healthz/model-agent-health` to see the reason.

When the pod keeps restarting, check that `modelAgent.health.port` is `8080`, and check the `path` of your `hf://` and `oci://` models. The agent crashes on each node that an `oci://` model with no `path` field selects. It also crashes on all nodes while an `hf://` or `oci://` model with no `path` is being deleted. This is a known bug: give every `hf://` and `oci://` model a `path`, and see [The agent keeps restarting](configure-model-artifact-retention.md#the-agent-keeps-restarting).

### The model's status doesn't list its nodes

When the model's label is on the nodes but its `status.nodesReady` stays empty, check that OME runs in the namespace `ome`. The agent always reports to `ome`, and no Helm value changes that, so install OME there.

## Clean up

Since v1.3, you can turn the agent off. First, delete the BaseModels and ClusterBaseModels stored on the nodes, and wait until they're gone. Each model's deletion waits for the agent on its nodes, as [Delete a model](../../concepts/models/base-models.md#deleting-a-model) describes. Then remove `enabled: true` from the values, and upgrade the release as in [Step 1](#step-1-turn-on-the-agent).

## Next steps

- [Share Hugging Face artifacts](shared-hf-artifacts.md): share one copy of a Hugging Face model's files between models.
- [Keep downloaded model files](configure-model-artifact-retention.md): keep a model's files on the nodes after you delete it.
- [Serve models from node-local storage](../deploy-models/serve-models-from-local-storage.md): serve weights already on your nodes' disks.
- [Base models](../../concepts/models/base-models.md): how a model chooses its nodes, and what OME learns from it.
- [Collect metrics](metrics.md): scrape the agent and the rest of OME.
