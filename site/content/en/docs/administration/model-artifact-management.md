---
title: "Model Artifact Management"
linkTitle: "Model Artifacts"
weight: 7
aliases:
  - /docs/administration/shared-hf-artifacts/
  - /docs/administration/model-artifact-retention/
description: >
  Configure Direct and Shared model storage, reuse downloaded files, and manage
  deletion, retention, eviction, and rehydration on each node.
---

The model agent manages node-local files for `BaseModel` and `ClusterBaseModel`
resources. This guide covers their storage layout and lifecycle in one place.
Deleting or evicting local files never deletes the remote source.

## Choose a storage layout

- **Direct** downloads files from Hugging Face or OCI Object Storage into
  the model's own directory.
- **Shared** stores one immutable Hugging Face snapshot in a parent directory;
  each model's configured path is a symlink to that parent. Eligible Hugging
  Face sources and OCI Object Storage mirrors opt in with
  `downloadPolicy: ReuseIfExists`.

A Hugging Face source URI does not by itself imply the Shared layout. Likewise,
an OCI Object Storage source may use either layout when it mirrors an eligible
Hugging Face snapshot.

Choose the operation according to what should remain:

| Goal | Operation | Result on this node |
| --- | --- | --- |
| Remove a model resource or exclude a node from placement | [Deletion](#when-the-agent-deletes-on-node-files) | Removes local files when safe, readiness, and the model's status entry. |
| Keep files after deletion | [Reserve label](#retaining-model-files) | Retains files under the rules below; removes readiness and the model's status entry. |
| Release local ownership while keeping the model resource | [Eviction](#evicting-direct-or-shared-artifacts-while-keeping-the-model) | Removes eligible files and records `Evicted`. |
| Restore evicted files or validate resident files | [Rehydration](#restoring-or-validating-an-artifact) | Validates or repairs files and acknowledges a new request before publishing Ready. |

File deletion and retention apply to `hf://`, `oci://`, and
`modelpack://` sources. Manual eviction and rehydration support Hugging Face and
OCI Object Storage artifacts only. Shared reuse requires an eligible Hugging Face
snapshot, either from Hugging Face or an OCI Object Storage mirror. Local, PVC,
and vendor sources have separate ownership rules, described below.

For agent deployment, configuration flags, monitoring, and troubleshooting, see
[Model Agent Administration](/ome/docs/administration/model-agent/).

## Shared storage and download policy

Setting `spec.storage.downloadPolicy: ReuseIfExists` opts a model into
node-level sharing. Models that resolve to the same immutable snapshot
identity — a Hugging Face model ID plus a 40-character commit SHA — share one
**parent** copy per node under a reserved `_artifacts` directory. Each model's
`spec.storage.path` becomes a symlink into that parent, and the parent is only
deleted when its last consumer is gone.

<a id="the-downloadpolicy-field"></a>

```yaml
spec:
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
    path: /mnt/models/llama-3-1-8b-instruct
    downloadPolicy: ReuseIfExists
```

- `AlwaysDownload` — the default, and the behavior when the field is unset.
  Every model resource downloads its own copy at `path`.
- `ReuseIfExists` — opt in to shared artifacts. Eligible models that resolve
  to the same model ID and commit SHA reuse one downloaded copy per node.

Reuse additionally requires:

- `spec.storage.path` is set to an explicit absolute path. It must not
  contain `..` or any path component named `_artifacts`.
- For a `ClusterBaseModel`, `path` must be inside the agent's models root
  directory (`--models-root-dir`, default `/mnt/models`). Namespaced
  `BaseModel` paths may live outside the models root; the shared parent is
  then created next to them.
- The source is either a direct `hf://` URI or an OCI Object Storage mirror
  of a Hugging Face snapshot (see below). Other storage backends ignore
  `ReuseIfExists` and keep per-model downloads.

<a id="direct-hugging-face-sources"></a>

### Hugging Face sources

An `hf://` source uses Shared storage when the reuse requirements above are met.

For `hf://{org}/{repo}[@{revision}]` URIs the model agent establishes the
snapshot identity itself: it resolves the revision (a branch or tag; `main`
when omitted) to the snapshot's immutable commit SHA through the Hugging Face
Hub API. If the URI already pins a 40-character commit SHA, it is used as-is
without a Hub call. No annotations are needed.

Two `ClusterBaseModel` resources like these share one copy per node:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b-instruct
spec:
  vendor: meta
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
    path: /mnt/models/llama-3-1-8b-instruct
    downloadPolicy: ReuseIfExists
---
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b-chatbot
spec:
  vendor: meta
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
    path: /mnt/models/llama-3-1-8b-chatbot
    downloadPolicy: ReuseIfExists
```

Both URIs resolve `main` to the same commit SHA, so the first model to be
processed on a node downloads the snapshot once; the second attaches to it
with a symlink and downloads nothing.

If the agent cannot resolve an immutable revision (for example, the Hub is
unreachable) and the model is not already attached to a shared copy, it logs
a warning and falls back to a Direct download.

<a id="oci-mirrors-of-hugging-face-snapshots"></a>

### OCI Object Storage mirrors of Hugging Face snapshots

You can download a Hugging Face snapshot separately and upload it to OCI Object
Storage. This guide calls that copy a **mirror**. OME can then download the model
files from OCI Object Storage instead of Hugging Face. Creating and maintaining
the mirror is outside OME's scope.

An `oci://` source carries no Hugging Face identity of its own, so reuse requires
two annotations on the `BaseModel` or `ClusterBaseModel` that declare which
snapshot the mirror holds:

```yaml
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
    path: /mnt/models/llama-3-1-8b-instruct
    downloadPolicy: ReuseIfExists
```

- `hf-model-id` is the `org/repo` Hugging Face model ID (at most two
  segments).
- `hf-model-sha` is the full 40-character hexadecimal commit SHA of the
  mirrored snapshot — not a branch or tag name.
- The object path in the `storageUri` must end with
  `<hf-model-id>/<hf-model-sha>` (the SHA segment is matched
  case-insensitively). A prefix that does not match the annotations is
  ignored for reuse and the model falls back to a Direct download.

The agent trusts these annotations — it performs no remote check against the
Hub. The mirror owner is responsible for keeping the object prefix's contents
identical to the declared snapshot, and for treating each `<sha>` prefix as
immutable.

Because sharing is keyed on the snapshot identity, a direct `hf://` model and
an OCI Object Storage mirror model that declare the same model ID and commit SHA
share the same parent copy on a node.

### What it looks like on the node

For `ClusterBaseModel` consumers the parent copy lives under the models root;
for namespaced `BaseModel` consumers it lives next to the model's `path`
(`<dir-of-path>/_artifacts/...`). With the two `ClusterBaseModel` resources
above, a node's models root looks like:

```
/mnt/models
├── _artifacts
│   └── meta-llama
│       └── Llama-3.1-8B-Instruct
│           └── 0e9e39f249a16976918f6564b8830bc894c89659
│               ├── .ome-hf-artifact-ready
│               ├── config.json
│               ├── model-00001-of-00004.safetensors
│               └── ...
├── llama-3-1-8b-instruct -> _artifacts/meta-llama/Llama-3.1-8B-Instruct/0e9e39f249a16976918f6564b8830bc894c89659
└── llama-3-1-8b-chatbot -> _artifacts/meta-llama/Llama-3.1-8B-Instruct/0e9e39f249a16976918f6564b8830bc894c89659
```

- The parent directory is
  `_artifacts/<hf-model-id>/<commit-sha>`. The first eligible model on the
  node downloads into it under an exclusive lock; concurrent consumers wait
  and then attach without downloading.
- Each consumer's `path` is a relative symlink to the parent. Serving
  runtimes read the model through the symlink like any other directory.
- `.ome-hf-artifact-ready` inside the parent marks a completed, validated
  download. Do not create, edit, or delete it by hand.
- `.hf-artifact-locks` directories (next to `_artifacts` and next to child
  paths) hold the agent's cross-process lock files. Never delete them.
- A given snapshot has exactly one recorded parent per node. Its location is
  fixed by the first model that downloads it; later consumers attach to that
  recorded location. Keeping all sharing models' paths under the models root
  is the simplest way to guarantee they land in the same parent.

The per-node model status ConfigMap records each Shared parent and the models
attached to it. It is named after the node and lives in the agent's namespace
(default `ome`). Parent entries use the `artifact.huggingface.` prefix and
report `Ready`, `Updating`, or `Failed`. Each model also has its own status
entry and Ready label.

The agent manages these records and the lock files; do not edit them by hand.

If a parent's files are damaged or a model update forces a re-download, the
agent repairs the parent in place while temporarily marking its child models
`Failed`, then restores their statuses; it never resets a parent that other
models are still attached to without that repair flow.

### Reference-counted deletion

Deleting one consumer never deletes files another consumer still uses:

1. The agent removes the deleted model's entry from the parent's `children`
   map, then removes the model's symlink.
2. Only when the **last** recorded reference is gone does the agent consider
   deleting the parent files — and before doing so it re-scans the model
   store directory for any remaining symlink that points into the parent,
   including links it has no record of. If it finds one, the files are
   preserved. Pending downloads that have not attached yet also protect the
   parent from deletion.
3. The symlink itself is kept when another live model resource still declares
   the same `path`, or when the deleted model carries the
   `models.ome/reserve-model-artifact: "true"` label.

So in the example above, deleting `llama-3-1-8b-chatbot` removes its symlink
and its reference; the parent copy stays for `llama-3-1-8b-instruct`.
Deleting both removes the parent directory as well.

Interrupted Shared cleanup resumes from its recorded state. New downloads
wait for cleanup to finish before reusing the parent.

### When the agent falls back to a per-model copy

`ReuseIfExists` is best effort. Models that cannot use Shared storage download
files directly to their `path`:

- `downloadPolicy` is unset or `AlwaysDownload`.
- `path` is unset or empty.
- The source is neither `hf://` nor an eligible OCI Object Storage mirror (missing or
  invalid `hf-model-id`/`hf-model-sha` annotations, or an object prefix that
  does not end with `<model-id>/<sha>`).
- The model is a TensorRT-LLM base model subject to GPU shape filtering —
  its downloaded file set differs per node, so there is no single shareable
  snapshot.
- A real directory or file already exists at `path`. Existing unshared copies
  are never adopted into `_artifacts` or overwritten; to migrate a model,
  delete it (or move it to a new `path`) and recreate it with
  `ReuseIfExists`.

Switching a shared model back to `AlwaysDownload`, or changing its source or
`path`, detaches it first — its reference and symlink are removed under the
same rules as deletion — before the Direct download proceeds. When the model
is reprocessed, changing the pinned revision or `hf-model-sha` is an identity
change: the model detaches from the old parent and downloads (or reuses) a
parent for the new commit SHA. Changing only `hf-model-id` or `hf-model-sha`
does not trigger DownloadOverride. To apply an annotation-only identity change
immediately, also [request model repair](/ome/docs/reference/labels-and-annotations/#requesting-model-repair).

## Deletion and retention

Deletion removes the model's node status entry. Eviction instead keeps
an `Evicted` entry so the model can be restored later.

### When the agent deletes on-node files

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

### What deletion removes

For each affected node, the agent:

1. Deletes the model directory — `spec.storage.path`, or
   `<models-root>/<storageUri>` (models root defaults to `/mnt/models`) when
   `path` is unset — unless one of the checks below skips it.
2. Removes the model's readiness label from the node
   (`models.ome.io/clusterbasemodel.<name>` or
   `models.ome.io/<namespace>.basemodel.<name>`).
3. Removes the model's entry from the per-node status ConfigMap (named after
   the node, in the agent's namespace, default `ome`).

The agent removes readiness and the status entry even when files are retained.
Errors during cleanup can interrupt this sequence.

### Storage backends the agent never deletes

File deletion applies to models the agent downloaded itself, i.e. `oci://`,
`modelpack://` and `hf://` sources. Other backends only get the label and status cleanup:

- `local://` — the files are pre-existing and user-managed; delete is a
  the agent leaves them on disk.
- `pvc://` — the model agent neither downloads nor deletes PVC-backed
  models; the BaseModel controller manages them.
- `vendor://` — remote API models have no files on the node.

### Safety checks that skip file deletion

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
  [Reference-counted deletion](#reference-counted-deletion).
- **The agent cannot verify it is safe.** If listing model resources fails,
  or the node status entry needed for the shared-artifact check cannot be
  read, the agent errs toward preserving the files.

When another model references a descendant of the directory being deleted,
the agent removes only unreferenced files and subdirectories. It preserves
the referenced subtree and its ancestor directories. For example, a reference
to `/mnt/models/model/releases/rev-002` protects that revision while allowing
old top-level weights and unreferenced revisions to be removed.

<a id="retaining-model-files"></a>

### Retaining files with `models.ome/reserve-model-artifact`

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
  [Reference-counted deletion](#reference-counted-deletion).

After a reserved deletion the node label and status entry are gone, so OME no
longer tracks the retained files. They stay on each node's disk until you
remove them yourself (or a future model deletes or reuses that path), so
account for them in node storage planning.

### Reusing retained files

Recreating a model that points at the same `spec.storage.path` enqueues a
normal download. For OCI Object Storage sources the agent validates existing
files against the bucket (size and MD5) and downloads only missing or invalid
files, so a recreated model typically becomes `Ready` without re-fetching the
retained weights. For shared Hugging Face artifacts, a preserved symlink that
still points at its parent copy re-attaches; note that a plain retained
directory at `path` is never adopted into sharing (see
[when the agent falls back to a per-model copy](#when-the-agent-falls-back-to-a-per-model-copy)).

## Evicting Direct or Shared artifacts while keeping the model

For a model with a recorded Shared relationship or a Direct copy from Hugging
Face or OCI Object Storage on the node, set:

```yaml
metadata:
  annotations:
    ome.io/artifact-residency: Evicted
```

The value is case-sensitive. On eligible nodes, the agent withdraws readiness,
releases the model's Shared reference or Direct directory, and records
`Evicted` in the per-node status ConfigMap. The model resource and remote
source remain. Files needed by other models may stay on disk, so `Evicted`
means this model's ownership was released, not that every shared byte was removed.

Direct eviction requires recorded ownership of a directory under the model
store root. It refuses symlink destinations, reserved artifact directories,
and paths used by another model. Shared eviction follows the recorded parent
relationship, even if the download policy has changed. Missing or unsupported
ownership preserves existing files and fails the request. If neither recorded
ownership nor a local destination exists, the agent can acknowledge eviction
without deleting anything.

Direct eviction records unfinished cleanup so a later eligible task can
resume it, including after an agent restart. This recovery does not apply to
Direct cleanup triggered by resource deletion or placement changes. A model that
becomes ineligible may need a new task to resume cleanup. Do not edit cleanup
records or remove lock files.

Downloads cannot publish Ready while eviction remains requested. If a failed
eviction is not retried automatically, retain the eviction request and update
the model resource to trigger another attempt.

### Out of scope

Direct and Shared eviction, and Shared rehydration, do not support:

- Changing an existing Model CR's `storage.path` as part of eviction or rehydration.
- Evicting a model while another Model CR uses the same target `storage.path`.

Choose eviction only after deciding that serving workloads no longer require
this model. The agent checks local file ownership, not Pods or serving demand.

## Restoring or validating an artifact

Remove `ome.io/artifact-residency: Evicted` and set a new, nonempty
`ome.io/artifact-rehydration-id` on an HF or OCI Object Storage model. For example,
use `restore-20260929-1`. Update both annotations in one patch. If separate updates
are necessary, set the new rehydration ID before removing `Evicted`.

The agent restores missing files or validates files already on the node.
Shared restoration can reattach an existing Ready parent without rechecking
its content solely for the new request. Damaged parents use the coordinated
repair flow. Files used by another model are protected from unsafe replacement.

When a Direct restore starts with no local directory, queued models with identical
`spec.storage` do not block the download. Recorded users, pending
cleanup, and existing directories still receive the usual ownership checks.

### Direct Hugging Face restoration

Direct HF restoration requires a nonempty `storage.path` under the agent's model
store. TensorRT-LLM models with missing `additionalMetadata.type` or type
`Serving` are excluded, as are tasks using a TensorRT-LLM base-model
shape filter. Eligible directories are validated against the file list for a fixed
Hugging Face revision. Healthy files stay at the configured path without
downloading, even when other models reference them. Missing or corrupt files
are repaired only after readiness withdrawal and reference checks; inspection
errors preserve bytes. Non-reuse HF policies continue to use Direct storage.

<a id="direct-oci-restoration"></a>

### Direct OCI Object Storage restoration

The agent checks local files against the Object Storage listing and repairs
missing or invalid files. HF identity annotations are not required.
Listing or inspection failures preserve existing files; empty listings and
unsafe object paths fail the request. TensorRT-LLM serving models use the
node's shape-specific file selection.

Cancellation stops the transfer when possible. New work on the same path waits
for the active transfer to finish.

### Storage layout after restoration

Pending eviction cleanup finishes before restoration begins. Existing Direct
directories stay Direct. Once eviction has removed the old files, the current
source and download policy determine the restored layout: an eligible HF
snapshot may use Shared storage; OCI Object Storage sources that are not eligible
for Shared storage remain Direct.

Downloads without a rehydration request keep their existing behavior.

### Completion reports and retries

Each node records `Ready` with the current model UID and rehydration request
ID in its status ConfigMap, then publishes the model's Ready label. Check the
request ID in the ConfigMap to confirm completion; a pre-existing Ready label
does not acknowledge a new request.

At startup and periodically (every five minutes by default), the agent checks
for missing reports or Ready labels and queues validation as needed. Completed
requests are left alone. An explicitly recorded failure is not retried with
the same request ID; address the cause and submit a new ID.

Recovery is tied to the Node instance observed at startup. Do not reuse a Node
name for a replacement Node instance.

For the full storage API, see the
[BaseModel concept](/ome/docs/concepts/base_model) and the
[API reference](/ome/docs/reference/ome.v1beta1). For the agent's general
download pipeline and configuration, see
[Model Agent Administration](/ome/docs/administration/model-agent/).
