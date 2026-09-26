---
title: "Shared Hugging Face Artifacts"
linkTitle: "Shared HF Artifacts"
weight: 7
description: >
  Share one downloaded copy of a Hugging Face snapshot per node across multiple
  BaseModels and ClusterBaseModels with downloadPolicy: ReuseIfExists.
---

By default, every `BaseModel` and `ClusterBaseModel` gets its own full copy of
its weights on every node the model agent downloads it to — even when several
model resources point at exactly the same Hugging Face snapshot. For a 70B
model that is hundreds of gigabytes of duplicated disk per extra resource.

Setting `spec.storage.downloadPolicy: ReuseIfExists` opts a model into
node-level sharing. Models that resolve to the same immutable snapshot
identity — a Hugging Face model ID plus a 40-character commit SHA — share one
**parent** copy per node under a reserved `_artifacts` directory. Each model's
`spec.storage.path` becomes a symlink into that parent, and the parent is only
deleted when its last consumer is gone.

## The `downloadPolicy` field

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
  `ReuseIfExists` and keep the ordinary per-model download.

## Direct Hugging Face sources

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
a warning and falls back to an ordinary, unshared download.

## OCI mirrors of Hugging Face snapshots

Many clusters mirror Hugging Face snapshots into OCI Object Storage and point
models at the mirror. An `oci://` source carries no Hugging Face identity of
its own, so reuse requires two annotations on the `BaseModel` or
`ClusterBaseModel` that declare which snapshot the mirror holds:

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
  ignored for reuse and the model falls back to an ordinary download.

The agent trusts these annotations — it performs no remote check against the
Hub. The mirror owner is responsible for keeping the object prefix's contents
identical to the declared snapshot, and for treating each `<sha>` prefix as
immutable.

Because sharing is keyed on the snapshot identity, a direct `hf://` model and
an OCI-mirror model that declare the same model ID and commit SHA share the
same parent copy on a node.

## What it looks like on the node

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

The agent also records the parent in the per-node model status ConfigMap
(named after the node, in the agent's namespace, default `ome`) under a key
prefixed `artifact.huggingface.`:

```json
{
  "key": "artifact.huggingface.meta-llama.Llama-3.1-8B-Instruct.<hash>.0e9e39f2...",
  "status": "Ready",
  "identity": {
    "modelId": "meta-llama/Llama-3.1-8B-Instruct",
    "commitSha": "0e9e39f249a16976918f6564b8830bc894c89659"
  },
  "localPath": "/mnt/models/_artifacts/meta-llama/Llama-3.1-8B-Instruct/0e9e39f2...",
  "children": {
    "clusterbasemodel.llama-3-1-8b-instruct": "/mnt/models/llama-3-1-8b-instruct",
    "clusterbasemodel.llama-3-1-8b-chatbot": "/mnt/models/llama-3-1-8b-chatbot"
  }
}
```

`status` is `Ready`, `Updating` (a download, repair, or deletion holds the
parent lock), or `Failed`. `children` maps each consuming model to its
symlink path — this is the reference count. Individual models still report
their own `Ready` status through the usual model entries and node labels.

If a parent's files are damaged or a model update forces a re-download, the
agent repairs the parent in place while temporarily marking its child models
`Failed`, then restores their statuses; it never resets a parent that other
models are still attached to without that repair flow.

## Reference-counted deletion

Deleting one consumer never deletes files another consumer still uses:

1. The agent removes the deleted model's entry from the parent's `children`
   map, then removes the model's symlink.
2. Only when the **last** recorded reference is gone does the agent consider
   deleting the parent files — and before doing so it re-scans the model
   store directory for any remaining symlink that points into the parent,
   including links it has no record of. If it finds one, the files are
   preserved.
3. The symlink itself is kept when another live model resource still declares
   the same `path`, or when the deleted model carries the
   `models.ome/reserve-model-artifact: "true"` label.

So in the example above, deleting `llama-3-1-8b-chatbot` removes its symlink
and its reference; the parent copy stays for `llama-3-1-8b-instruct`.
Deleting both removes the parent directory as well.

## When the agent falls back to a per-model copy

`ReuseIfExists` is best-effort admission into sharing; ineligible models keep
the ordinary download behavior at their `path`:

- `downloadPolicy` is unset or `AlwaysDownload`.
- `path` is unset or empty.
- The source is neither `hf://` nor an eligible OCI mirror (missing or
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
same rules as deletion — before the ordinary download proceeds. Changing the
pinned revision or `hf-model-sha` is an identity change: the model detaches
from the old parent and downloads (or reuses) a parent for the new commit
SHA.

For the full storage API, see the
[BaseModel concept](/ome/docs/concepts/base_model) and the
[API reference](/ome/docs/reference/ome.v1beta1). For the agent's general
download pipeline and configuration, see
[Model Agent Administration](/ome/docs/administration/model-agent/).
