---
title: "Model Artifact Deletion and Retention"
linkTitle: "Model Artifact Retention"
weight: 8
description: >
  When the model agent deletes downloaded model files from a node, and how to
  keep them with the models.ome/reserve-model-artifact label.
---

The model agent DaemonSet downloads each `BaseModel` and `ClusterBaseModel` to
every eligible node and, by default, deletes that node-local copy when the
model no longer belongs on the node. This page describes exactly when files
are deleted, the checks that skip deletion, and how to retain files on disk
with the `models.ome/reserve-model-artifact` label.

Deletion only ever affects the node-local copy. The source — an OCI Object
Storage bucket or a Hugging Face repository — is never touched.

## When the agent deletes on-node files

Each node's agent removes its local copy of a model in two situations:

- **The model resource is deleted.** Deleting a `BaseModel` or
  `ClusterBaseModel` triggers cleanup on every node that holds a copy. An
  agent that was down when the deletion happened catches up on startup by
  scanning for models with a deletion timestamp.
- **A placement change makes the node ineligible.** The agent re-evaluates
  `spec.storage.nodeSelector` and
  `spec.storage.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution`
  on every model update. A node that held the model but no longer matches
  deletes its local copy; a node that newly matches downloads one. Nodes that
  remain eligible keep their copy untouched.

Note that editing placement is therefore not just additive: narrowing a
`nodeSelector` reclaims disk on the nodes it excludes.

## What deletion removes

For each affected node, the agent:

1. Deletes the model directory — `spec.storage.path`, or
   `<models-root>/<storageUri>` (models root defaults to `/mnt/models`) when
   `path` is unset — unless one of the checks below skips it.
2. Removes the model's readiness label from the node
   (`models.ome.io/clusterbasemodel.<name>` or
   `models.ome.io/<namespace>.basemodel.<name>`).
3. Removes the model's entry from the per-node status ConfigMap (named after
   the node, in the agent's namespace, default `ome`).

Steps 2 and 3 always happen, even when the files themselves are preserved:
after removal the node no longer advertises the model, regardless of what is
left on disk.

## Storage backends the agent never deletes

File deletion applies to models the agent downloaded itself, i.e. `oci://`,
`modelpack://` and `hf://` sources. Other backends only get the label and status cleanup:

- `local://` — the files are pre-existing and user-managed; delete is a
  file no-op by design.
- `pvc://` — the model agent neither downloads nor deletes PVC-backed
  models; the BaseModel controller manages them.
- `vendor://` — remote API models have no files on the node.

## Safety checks that skip file deletion

Before deleting an `oci://`, `modelpack://` or `hf://` model directory, the agent skips the
file removal (but still cleans up the label and status entry) when any of the
following holds:

- **The path is still referenced.** Another `BaseModel` or
  `ClusterBaseModel` declares the exact same `spec.storage.path`. This
  protects deliberately shared paths when only one of the resources is
  removed.
- **The model carries the reserve label.** See the next section.
- **A shared Hugging Face parent still has consumers.** Models using
  `downloadPolicy: ReuseIfExists` are reference-counted; see
  [Reference-counted deletion](/ome/docs/administration/shared-hf-artifacts/#reference-counted-deletion).
- **The agent cannot verify it is safe.** If listing model resources fails,
  or the node status entry needed for the shared-artifact check cannot be
  read, the agent errs toward preserving the files.

When another model references a descendant of the directory being deleted,
the agent removes only unreferenced files and subdirectories. It preserves
the referenced subtree and its ancestor directories. For example, a reference
to `/mnt/models/model/releases/rev-002` protects that revision while allowing
old top-level weights and unreferenced revisions to be removed. Comparisons
use directory boundaries, so `model-bf16` is not a descendant of `model`.
Ordinary cleanup also preserves internal `_artifacts` and `.hf-artifact-locks`
directories; shared artifact cleanup manages the weights in `_artifacts`.

## Retaining files with `models.ome/reserve-model-artifact`

To keep the downloaded weights on disk while removing the model resource (or
removing nodes from its placement), label the `BaseModel` or
`ClusterBaseModel` with `models.ome/reserve-model-artifact: "true"`:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b-instruct
  labels:
    models.ome/reserve-model-artifact: "true"
spec:
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
    path: /mnt/models/llama-3-1-8b-instruct
```

Or label an existing model before deleting it:

```bash
kubectl label clusterbasemodel llama-3-1-8b-instruct \
  models.ome/reserve-model-artifact=true
kubectl delete clusterbasemodel llama-3-1-8b-instruct
```

Rules:

- The value must be `"true"`; the comparison is case-insensitive (`"True"`
  and `"TRUE"` also work). Any other value — including `"false"` or an empty
  string — leaves normal deletion in effect.
- The label must be on the resource **before** the deletion or placement
  change is processed. Label first, then delete.
- It works on both `BaseModel` and `ClusterBaseModel`, and covers both
  triggers: resource deletion and a node dropping out of placement.
- For a model attached to a shared Hugging Face artifact, the label
  preserves the model's symlink at `spec.storage.path`; whether the shared
  parent copy itself survives is decided by its reference count, as described
  in
  [Shared Hugging Face Artifacts](/ome/docs/administration/shared-hf-artifacts/#reference-counted-deletion).

After a reserved deletion the node label and status entry are gone, so OME no
longer tracks the retained files. They stay on each node's disk until you
remove them yourself (or a future model deletes or reuses that path), so
account for them in node storage planning.

## Reusing retained files

Recreating a model that points at the same `spec.storage.path` enqueues a
normal download. For OCI Object Storage sources the agent validates existing
files against the bucket (size and MD5) and downloads only missing or invalid
files, so a recreated model typically becomes `Ready` without re-fetching the
retained weights. For shared Hugging Face artifacts, a preserved symlink that
still points at its parent copy re-attaches; note that a plain retained
directory at `path` is never adopted into sharing (see
[when the agent falls back to a per-model copy](/ome/docs/administration/shared-hf-artifacts/#when-the-agent-falls-back-to-a-per-model-copy)).

For the agent's download pipeline, configuration flags, and the per-node
status ConfigMap, see
[Model Agent Administration](/ome/docs/administration/model-agent/). The
label is also listed in the
[labels and annotations reference](/ome/docs/reference/labels-and-annotations).
