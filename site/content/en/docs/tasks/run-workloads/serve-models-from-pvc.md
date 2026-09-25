---
title: "Serve Models from a PVC"
linkTitle: "Serve Models from PVC"
weight: 15
date: 2026-09-25
description: >
  Learn how to serve model weights that already live on a PersistentVolumeClaim, without any download step.
---

This page shows you how to point a BaseModel or ClusterBaseModel at model weights that are already stored on a Kubernetes PersistentVolumeClaim (PVC). You'll learn the `pvc://` storage URI format and its namespace rules, how OME extracts model metadata from the PVC, and how the claim is mounted directly into InferenceService pods.

PVC-backed models differ from every other storage backend in one important way: **nothing is downloaded**. The model agent DaemonSet skips `pvc://` models entirely — there is no per-node copy, no node labeling, and `status.nodesReady` stays empty. Instead, the BaseModel controller validates the claim, runs a short metadata-extraction Job against it, and the serving pods mount the claim read-only and load the weights in place.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- A **Bound** PVC that contains the model weights in a standard layout — a directory holding a Hugging Face-style `config.json` (text models) or `model_index.json` (diffusion models) plus the weight files

If more than one engine replica can be scheduled, keep in mind that every pod mounts the same claim read-only, so the underlying PersistentVolume must support access from all the nodes involved (for example `ReadOnlyMany` or `ReadWriteMany`).

## The pvc:// storage URI

```
pvc://{pvc-name}/{sub-path}                # BaseModel (namespaced)
pvc://{namespace}:{pvc-name}/{sub-path}    # ClusterBaseModel
```

| Component   | Rules                                                                                       |
|-------------|---------------------------------------------------------------------------------------------|
| `namespace` | Separated from the PVC name by a **colon**. Lowercase alphanumeric and hyphens, max 63 chars. |
| `pvc-name`  | Name of an existing, Bound PersistentVolumeClaim.                                            |
| `sub-path`  | **Required.** Directory inside the volume that holds the model. May contain slashes; must not contain `..` segments. |

The namespace rule depends on the resource kind, and the admission webhook rejects violations at `kubectl apply` time:

- A namespaced **BaseModel** must **not** specify a namespace — the PVC is always resolved in the BaseModel's own namespace.
- A **ClusterBaseModel** has no namespace of its own, so its URI **must** carry the namespace prefix.
- `distribution: Sharded` is not compatible with `pvc://` storage and is also rejected at admission.

## Step 1: Verify the PVC

Check that the claim is Bound and that the sub-path you plan to reference contains the model:

```bash
kubectl get pvc model-storage -n llama-demo
```

Expected output:

```
NAME            STATUS   VOLUME     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
model-storage   Bound    pv-0042    500Gi      ROX            fss            2d
```

A PVC that exists but is still `Pending` keeps the model in the `In_Transit` state (condition reason `PVCNotBound`) until it binds; a PVC that doesn't exist moves the model to `Failed` (reason `PVCNotFound`) and the controller re-checks automatically once you create it.

## Step 2: Create the model

For a namespaced BaseModel, omit the namespace — the PVC must live in the BaseModel's namespace:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: "pvc://model-storage/llama-3-2-1b-instruct"
EOF
```

For a ClusterBaseModel, name the PVC's namespace explicitly with the colon separator:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-2-1b-instruct
spec:
  storage:
    storageUri: "pvc://llama-demo:model-storage/llama-3-2-1b-instruct"
EOF
```

You don't need to fill in `modelArchitecture`, `modelParameterSize`, or `modelCapabilities` — the metadata-extraction Job populates them for you (see the next step). Any value you do set explicitly is kept; extraction only fills fields that are unset.

## Step 3: Watch the model become Ready

Once the PVC is Bound, the BaseModel controller creates a one-shot Job named `<model-name>-metadata-<hash>` in the PVC's namespace. The Job mounts the URI's sub-path of the claim read-only and runs `ome-agent model-metadata`, which parses the model directory (`config.json` or `model_index.json`) and reports the result back through a status ConfigMap in the OME control-plane namespace. The controller then fills in the unset spec fields (model type, architecture, parameter size, capabilities, framework, format, quantization) and flips the model to `Ready`.

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo -w
```

The `READY` column moves `In_Transit` → `Ready`. To see the extraction Job itself:

```bash
kubectl get jobs -n llama-demo -l models.ome/model-name=llama-3-2-1b-instruct
```

The model's conditions tell you exactly where in the flow you are:

```bash
kubectl get basemodel llama-3-2-1b-instruct -n llama-demo -o jsonpath='{.status.conditions}' | jq
```

| State        | `SourceReachable` | `Ready` | Reason                        | Meaning                                                     |
|--------------|-------------------|---------|-------------------------------|-------------------------------------------------------------|
| `In_Transit` | `True`            | `False` | `PVCNotBound`                 | The PVC exists but hasn't bound yet.                        |
| `In_Transit` | `True`            | `False` | `PVCMetadataExtracting`       | The metadata Job is running.                                |
| `Ready`      | `True`            | `True`  | `PVCValidated` / `PVCMetadataReady` | Extraction succeeded; the model is servable.          |
| `Failed`     | `False`           | `False` | `PVCInvalid`                  | The URI is malformed or violates the namespace rules — edit the CR. |
| `Failed`     | `False`           | `False` | `PVCNotFound`                 | The referenced PVC does not exist in the resolved namespace. |
| `Failed`     | `False`           | `False` | `PVCConfigMissing`            | The controller has no ome-agent image configured (see below). |
| `Failed`     | `True`            | `False` | `PVCMetadataExtractionFailed` | The PVC is fine but the model directory couldn't be parsed — check the Job logs. |

Completed Jobs are cleaned up automatically (TTL 3600 seconds by default), and deleting the model deletes its Job and status ConfigMap.

## Step 4: Deploy an InferenceService

Create the InferenceService **in the same namespace as the PVC**. The generated pod references the claim by name, and Kubernetes only resolves PVC references within the pod's own namespace — a pod in another namespace would be stuck with a "claim not found" event:

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

For a PVC-backed model, OME generates the serving pod differently than for downloaded models:

- The pod gets a volume named after the model with a `persistentVolumeClaim` source (`claimName` from the URI, `readOnly: true`).
- The runtime container mounts that volume read-only at `/opt/ml/model`, using the URI's sub-path as the mount's `subPath`. The `storage.path` field is not used for PVC models — the mount path is always `/opt/ml/model`.
- The `MODEL_PATH` environment variable is set to `/opt/ml/model`, so runtimes that launch with `--model-path="$MODEL_PATH"` work unchanged.
- The "model ready on node" affinity that downloaded models rely on is skipped — PVCs aren't tied to the nodes the model agent labels, so the Kubernetes scheduler places pods based on PVC accessibility. Runtime and accelerator node selectors still apply as usual.

Verify the mount on the running pod:

```bash
kubectl get pod -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct \
  -o jsonpath='{.items[0].spec.volumes}' | jq
```

From here the service behaves like any other InferenceService — see [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) for testing and monitoring.

## Configuring the metadata Job

The extraction Job's settings come from the `omeAgent` block of the `inferenceservice-config` ConfigMap, which the `ome-resources` Helm chart renders from `ome.omeAgent.metadataJob` values:

```yaml
ome:
  omeAgent:
    metadataJob:
      serviceAccount: ome-model-metadata
      memoryRequest: 256Mi
      memoryLimit: 512Mi
      cpuRequest: 100m
      cpuLimit: 500m
      backoffLimit: 2
      ttlSecondsAfterFinished: 3600
      # Scheduling hints for the Job pod. Set these when the PVC's CSI
      # driver only mounts on a subset of nodes, or the volume lives on
      # tainted (e.g. GPU) nodes.
      nodeSelector: {}
      tolerations: []
      affinity: {}
      priorityClassName: ""
```

The chart also installs the `ome-model-metadata` ServiceAccount and a ClusterRole that lets the Job write its status ConfigMap. When a Job needs to run in a namespace other than the OME control-plane namespace (the normal case), the controller creates the ServiceAccount there and the required RoleBinding automatically.

If the `omeAgent` block is missing entirely (for example, on an installation that predates it), PVC-backed models fail with reason `PVCConfigMissing`; upgrading the chart or adding the block and restarting the controller fixes it.

## Troubleshooting

**Model stuck at `In_Transit` with `PVCMetadataExtracting`:** the Job pod may be unschedulable (CSI driver not available on any eligible node) or crash-looping. Inspect it:

```bash
kubectl get jobs -n llama-demo -l models.ome/model-name=llama-3-2-1b-instruct
kubectl logs -n llama-demo job/<job-name>
```

If the Job exhausts its retries, the model transitions to `Failed` with the Job's failure message on the `Ready` condition.

**`Failed` with `PVCMetadataExtractionFailed`:** the mounted directory could not be parsed as a model — most often the sub-path is wrong or points to the volume root instead of the directory containing `config.json`. Fix the URI's sub-path; changing the URI creates a fresh Job.

**InferenceService pod stuck in `ContainerCreating`:** check `kubectl describe pod` for volume events. A "persistentvolumeclaim not found" event means the InferenceService is not in the PVC's namespace; an attach/mount timeout usually means the PV's access mode doesn't allow this node (or a second node, for multi-replica services).

## Cleanup

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
kubectl delete basemodel llama-3-2-1b-instruct -n llama-demo
```

Deleting the model removes its metadata Job and status ConfigMap; the PVC and your weights are untouched.

## Next steps

- [Base Model concepts](/ome/docs/concepts/base_model) — all storage backends and the full BaseModel field reference
- [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) — testing, monitoring, and scaling the service
