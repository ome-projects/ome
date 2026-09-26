---
title: "Serve Models from Node-Local Storage"
linkTitle: "Serve Models from Local Storage"
weight: 20
date: 2026-09-26
description: >
  Learn how to serve model weights that already exist on your nodes' local filesystem, using the local:// storage URI.
---

This page shows you how to point a BaseModel or ClusterBaseModel at model weights that are already sitting on the local disks of your nodes, using the `local://` storage URI. You'll learn how the model agent validates the files in place instead of downloading anything, why `storage.path` must be set alongside the URI, and how the weights are hostPath-mounted read-only into InferenceService pods.

Local-storage models behave differently from downloaded models in three ways:

- **Nothing is downloaded.** The model agent only checks that the path exists on its node and parses the model configuration to extract metadata.
- **Nothing is ever deleted.** Deleting the BaseModel removes node labels and status bookkeeping, but the files on disk are always preserved.
- **Serving pods mount the host directory.** The generated pod gets a read-only hostPath mount of `storage.path`, so the runtime loads the weights straight from the node's filesystem.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- Model weights already present on the local filesystem of at least one node, in a standard layout — a directory holding a Hugging Face-style `config.json` (text models) or `model_index.json` (diffusion models) plus the weight files
- The same absolute path on **every** node that should serve the model — the path in the URI is used verbatim on each node

The model agent runs as a DaemonSet and validates the path from **inside its own pod**, which by default only mounts the chart's `modelAgent.hostPath` directory (default `/mnt/data/models`). Keep your weights under that directory, or mount the extra host directory into the agent with the `modelAgent.extraVolumes` / `modelAgent.extraVolumeMounts` chart values — the container `mountPath` must equal the host path, because the agent checks the same path string it later reports.

Also note that serving pods use hostPath volumes, which the `restricted` [Pod Security Standard](https://kubernetes.io/docs/concepts/security/pod-security-standards/) disallows — the namespace running the InferenceService must permit them.

## The local:// storage URI

```
local://{path}
```

The path is taken verbatim — everything after the `local://` prefix. For an absolute path that means three slashes:

```
local:///mnt/data/models/llama-3-2-1b-instruct
```

Unlike `pvc://`, there is no namespace component; BaseModel and ClusterBaseModel use exactly the same URI format. The only validation is that the path is non-empty.

Be careful with typos: the admission webhook only enforces the `pvc://` URI shape rules, so a malformed `local://` URI is accepted at `kubectl apply` time and only surfaces later, when the model agent fails the model on every node.

## Always set storage.path as well

The `storage.path` field and the URI play two different roles:

- The **model agent** validates and parses `storage.path` if it is set, and falls back to the URI's path otherwise.
- The **InferenceService controller** only looks at `storage.path` when generating the serving pod. Without it, the pod gets **no model volume and no `MODEL_PATH` environment variable**, and the runtime has nothing to load — even though the model itself shows `Ready`.

Always set `storage.path` to the same absolute path as the URI.

## Step 1: Stage the weights on the nodes

Place the model directory at the same path on every node that should serve it, for example:

```
/mnt/data/models/llama-3-2-1b-instruct/
├── config.json
├── generation_config.json
├── model.safetensors
├── tokenizer.json
└── tokenizer_config.json
```

How the files get there is up to you — pre-baked node images, a provisioning DaemonSet, or manual copy.

## Step 2: Create the model

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: "local:///mnt/data/models/llama-3-2-1b-instruct"
    path: /mnt/data/models/llama-3-2-1b-instruct
EOF
```

A ClusterBaseModel is identical — same URI format, no namespace anywhere:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-2-1b-instruct
spec:
  storage:
    storageUri: "local:///mnt/data/models/llama-3-2-1b-instruct"
    path: /mnt/data/models/llama-3-2-1b-instruct
EOF
```

Every node running the model agent processes the model independently, and nodes that don't have the files are recorded as failed (see the next step). If only some nodes carry the weights, scope the model to them with `storage.nodeSelector`, or with `storage.nodeAffinity` (only `requiredDuringSchedulingIgnoredDuringExecution` terms are evaluated by the agent):

```yaml
spec:
  storage:
    storageUri: "local:///mnt/data/models/llama-3-2-1b-instruct"
    path: /mnt/data/models/llama-3-2-1b-instruct
    nodeSelector:
      models.example.com/has-llama: "true"
```

You don't need to fill in `modelArchitecture`, `modelParameterSize`, or `modelCapabilities`: the agent parses the model directory on the node and the controller fills in the unset spec fields (model type, architecture, parameter size, capabilities, framework, format, quantization, max tokens). Values you set explicitly are kept. Unlike PVC models, a directory whose configuration can't be parsed does **not** fail the model — it can still become `Ready`, just without auto-filled metadata, so set those fields manually in that case.

## Step 3: Watch the model become Ready

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo -w
```

The model starts at `In_Transit` and becomes `Ready` as soon as **one** node has validated the path. On each successful node the agent also sets a node label that serving pods later schedule against:

```bash
# BaseModel:            models.ome.io/{namespace}.basemodel.{name}
# ClusterBaseModel:     models.ome.io/clusterbasemodel.{name}
kubectl get nodes -l 'models.ome.io/llama-demo.basemodel.llama-3-2-1b-instruct=Ready'
```

(Very long model or namespace names are truncated with a hash to fit the 63-character label limit.)

The per-node results are aggregated into the model's status:

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo \
  -o jsonpath='{.status.nodesReady}{"\n"}{.status.nodesFailed}{"\n"}'
```

Nodes where the path doesn't exist (or isn't visible inside the agent pod) land in `status.nodesFailed`. That's expected when the weights only live on some nodes and you didn't scope the model with `storage.nodeSelector`. The model only goes `Failed` when **no** node has validated it.

## Step 4: Deploy an InferenceService

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
    kind: BaseModel
  engine:
    minReplicas: 1
    maxReplicas: 1
EOF
```

For a local-storage model, OME generates the serving pod as follows:

- The pod gets a volume named after the model with a `hostPath` source pointing at `storage.path`.
- The runtime container mounts that volume **read-only at the same path** — there is no remap to `/opt/ml/model` as with PVC models.
- The `MODEL_PATH` environment variable is set to `storage.path`, so runtimes that launch with `--model-path="$MODEL_PATH"` work unchanged.
- A required node selector `models.ome.io/...: Ready` pins the pods to nodes where the agent has validated the files. Runtime and accelerator node selectors still apply as usual.

Verify the mount on the running pod:

```bash
kubectl get pod -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct \
  -o jsonpath='{.items[0].spec.volumes}' | jq
```

From here the service behaves like any other InferenceService — see [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) for testing and monitoring.

## Troubleshooting

**Model `Failed`, or a node unexpectedly in `nodesFailed`:** the path doesn't exist on that node, or it exists on the host but isn't mounted into the model agent pod (see [Before you begin](#before-you-begin)). Check the agent's logs on the affected node:

```bash
kubectl get pods -n ome -o wide -l app.kubernetes.io/component=ome-model-agent-daemonset
kubectl logs -n ome <model-agent-pod-on-that-node>
```

Look for `local model path does not exist` (wrong or unmounted path) or `invalid local storage URI` (URI typo — remember these are not caught at apply time).

**Model is `Ready` but the runtime container can't find the weights:** `storage.path` is missing from the model spec, so no model volume or `MODEL_PATH` was generated. Add it (matching the URI path) and the pods will be regenerated.

**Serving pod stuck `Pending` with "didn't match Pod's node affinity/selector":** no schedulable node carries the `models.ome.io/...=Ready` label — either the model isn't Ready on any node, or the nodes that have it don't satisfy the runtime/accelerator selectors.

## Cleanup

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
kubectl delete basemodel llama-3-2-1b-instruct -n llama-demo
```

Deleting the model removes the node labels and per-node status entries. The weights on the nodes are never touched — the agent explicitly skips file deletion for local storage.

## Next steps

- [Base Model concepts](/ome/docs/concepts/base_model) — the full BaseModel field reference
- [Serve Models from a PVC](/ome/docs/tasks/run-workloads/serve-models-from-pvc/) — the equivalent flow for weights on a PersistentVolumeClaim
- [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) — testing, monitoring, and scaling the service
