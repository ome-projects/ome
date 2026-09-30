---
title: "Model Weight Replication"
linkTitle: "Model Replication"
weight: 8
description: >
  Copy model weights between Hugging Face, OCI Object Storage, and PVCs with
  the ome-agent replica command.
---

OME ships a replication agent — `ome-agent replica` — that copies model
weights from one storage location to another: seed a private OCI Object
Storage mirror from Hugging Face, copy a bucket across regions or tenancies,
or fill a PVC that models and runtimes will read from.

Replication involves no custom resource and no controller. It is a one-shot
run of the `ome-agent` binary driven entirely by a config file, typically
wrapped in a Kubernetes `Job` using the same `ome-agent` image that serves
model-init and metadata duties (see
[Private Registries](/ome/docs/installation/private-registries/) for the image
reference). It is distinct from the
[model agent](/ome/docs/administration/model-agent/) DaemonSet, which
downloads models *onto nodes* for serving; the replication agent copies
weights *between storage systems*.

## Supported source → target pairs

The source and target are declared as storage URIs. Exactly these six
combinations are supported; any other pair fails at startup with
`unsupported replication`:

| Source | Target |
| --- | --- |
| `hf://` (Hugging Face) | `oci://` (OCI Object Storage) |
| `hf://` | `pvc://` |
| `oci://` | `oci://` |
| `oci://` | `pvc://` |
| `pvc://` | `oci://` |
| `pvc://` | `pvc://` |

URI formats:

- `oci://n/{namespace}/b/{bucket}/o/{prefix}` — every object under the
  prefix. A trailing `/` is appended to the prefix automatically.
- `hf://{model-id}[@{branch}]` — a Hugging Face model repository; the branch
  defaults to `main`. Source only.
- `pvc://{pvc-name}/{sub-path}` or `pvc://{namespace}:{pvc-name}/{sub-path}` —
  the sub-path is required and must not contain `..` segments.

## Running the agent

```bash
ome-agent replica --config <path-to-config.yaml> [--debug]
```

- `-c, --config` — path to the YAML config file. Required; the agent refuses
  to start without one.
- `-d, --debug` — enable debug logging.

Every config key can be overridden by an environment variable named
`OME_AGENT_` plus the upper-cased key with dots replaced by underscores:
`source.storage_uri` becomes `OME_AGENT_SOURCE_STORAGE_URI`, `hf_token`
becomes `OME_AGENT_HF_TOKEN`. This is the recommended way to inject secrets
from Kubernetes `Secret`s instead of writing them into the config file.

A commented sample config ships in the repository at
`config/ome-agent/ome-agent.yaml` (it also carries keys for the other
`ome-agent` subcommands, which the replica command ignores).

### Exit behavior

The run either completes fully or exits non-zero:

- On a replication error the agent writes the message to
  `/dev/termination-log` (visible in the pod's termination status) and exits
  with code 1. Size-limit and empty-source aborts also exit with code 1, but
  report the reason only in the container log.
- If the process is interrupted (for example a `SIGTERM` during a node drain)
  before replication completed, it also exits non-zero. A wrapping `Job`
  therefore never records success for a partial copy.

The agent never retries a failed run; schedule retries through the Job's
`backoffLimit` or your own orchestration.

## Configuration

Top-level settings:

| Key | Default | Purpose |
| --- | --- | --- |
| `local_path` | — (required) | Local working directory. Scratch space for bucket-to-bucket copies, and the mount point for PVC endpoints (see below). |
| `num_connections` | `10` | Parallel object transfers for OCI uploads and downloads. Must be greater than 0. |
| `download_size_limit_gb` | `650` | Abort if the source weights exceed this total size. |
| `enable_size_limit_check` | `true` | Enforce `download_size_limit_gb`. |
| `hf_download_timeout` | `72h` | Overall timeout for a Hugging Face snapshot download. |
| `hf_download_stale_progress_timeout` | `30m` | Abort a Hugging Face download that makes no bytes/files progress for this long. |
| `target_artifact_reuse_allowed` | `false` | For OCI targets: skip replication when the target already holds a complete artifact, and coordinate concurrent replicas with an upload lock. |
| `artifact_upload_lock_owner_id` | unset | Lets retries of one replication operation reuse a leftover upload lock. |
| `artifact_upload_lock_timeout` | `120h` | How long to wait on another replica's upload lock before treating it as stale. |

Before any data moves, the agent lists the source objects and sums their
sizes. When `enable_size_limit_check` is true and the total exceeds
`download_size_limit_gb`, or when the source contains no weights at all, the
run aborts.

### The `source` and `target` blocks

Each side declares its `storage_uri` (both required) and enables the client
matching the URI scheme. A side whose URI is `oci://` must set
`oci.enabled: true` in its block, and a side whose URI is `pvc://` must set
`pvc.enabled: true` — otherwise startup fails with an error such as
`required Source.OCIOSDataStore is nil`. Hugging Face sources need no
`enabled` flag; the Hub client is always available.

OCI credentials are configured per side under `source.oci` / `target.oci`, so
source and target can live in different regions and tenancies:

| Key | Default | Purpose |
| --- | --- | --- |
| `oci.enabled` | `false` | Build an Object Storage client for this side. |
| `oci.auth_type` | — (required) | `UserPrincipal`, `InstancePrincipal`, `ResourcePrincipal`, or `OkeWorkloadIdentity`. |
| `oci.region` | unset | Region of this side's bucket. |
| `oci.compartment_id` | unset | Compartment OCID. |
| `oci.enable_obo_token` | `false` | Authenticate with an on-behalf-of token. |
| `oci.obo_token` | unset | The OBO token; required when `enable_obo_token` is true. |

A Hugging Face source is configured with top-level keys consumed by the Hub
client:

| Key | Default | Purpose |
| --- | --- | --- |
| `hf_token` | unset | Access token for gated or private repositories. |
| `endpoint` | `https://huggingface.co` | Hub endpoint. |
| `cache_dir` | `/tmp/.cache/huggingface` | Hub client cache directory. |
| `max_concurrent_downloads` | `4` | Parallel snapshot download workers. |
| `enable_dedup` | `false` | Enable Xet chunk deduplication. |

### PVC endpoints and `local_path`

The agent does not mount PVCs itself — it simply reads and writes the local
filesystem. When a side is `pvc://`, the pod spec must mount that PVC so the
agent's paths resolve:

- **PVC source** (`pvc://…/{sub-path}` → OCI or PVC): the agent reads from
  `<local_path>/<sub-path>`. Mount the source PVC at `local_path`.
- **PVC target of `hf://` or `oci://`**: the agent writes into
  `<local_path>/<target-sub-path>`. Mount the target PVC at `local_path`.
- **PVC → PVC**: the agent reads from `<local_path>/<source-sub-path>` and
  writes to `<local_path>/<target-pvc-name>/<target-sub-path>`. Mount the
  source PVC at `local_path` and the target PVC at
  `<local_path>/<target-pvc-name>`. Both URIs must use the same namespace
  form (both bare `pvc://name/…` or both `pvc://ns:name/…` with the same
  namespace); if source and target are identical the run is a successful
  no-op.

## How each pair moves data

- **`hf://` → `oci://`** — downloads the full snapshot at the requested
  branch into the scratch directory `<local_path>/replica`, then uploads
  every file to `<target-prefix>/<relative-path>` with `num_connections`
  parallel workers. The scratch directory is removed on success.
- **`oci://` → `oci://`** — streams object by object through
  `<local_path>/replica`: each object is downloaded, then uploaded under the
  target prefix (the source prefix in each object name is replaced by the
  target prefix). The scratch directory is removed only after all objects
  finish, so size it for the whole artifact.
- **`pvc://` → `oci://`** — uploads every file under
  `<local_path>/<source-sub-path>` to `<target-prefix>/<relative-path>`.
- **`hf://` → `pvc://`** — downloads the snapshot directly into
  `<local_path>/<target-sub-path>`.
- **`oci://` → `pvc://`** — downloads each object into
  `<local_path>/<target-sub-path>`; each file keeps its full source object
  name as its path under that directory, including the source prefix
  directories.
- **`pvc://` → `pvc://`** — copies the source tree to
  `<local_path>/<target-pvc-name>/<target-sub-path>`, preserving file modes.

For `hf://` → `oci://` and `oci://` → `oci://`, provision `local_path` with
enough disk for the entire model (an `emptyDir` or scratch PVC in a Job).
Existing objects or files at the target are overwritten; the agent never
deletes extra objects already present at the target prefix.

## Checksums on OCI targets

For the three pairs with an OCI target, `target.checksum` attaches a checksum
of each uploaded file as object metadata:

```yaml
target:
  storage_uri: "oci://n/mytenancy/b/model-mirror/o/models/llama-3-1-8b/"
  checksum:
    upload_enabled: true
    algorithm: "md5"    # md5 or sha256
    concurrency: 8
  oci:
    enabled: true
    auth_type: "InstancePrincipal"
    region: "us-chicago-1"
```

The checksum is computed from the local file just before upload and stored
base64-encoded under the metadata key `opc-meta-md5` or `opc-meta-sha256`.
`concurrency` caps how many checksums are computed at once (when unset or
less than 1, checksums are computed one at a time). Checksum configuration is
ignored for PVC targets.

## Artifact reuse and upload locking (OCI targets)

By default every run re-copies everything. For OCI targets,
`target_artifact_reuse_allowed: true` makes runs idempotent and safe to race,
using two sentinel objects directly under the target prefix:

- `.ome-artifact-complete` — written after every object has uploaded
  successfully.
- `.ome-artifact-upload.lock` — held while a replica is uploading.

With reuse enabled, a run first inspects the target: if the completion marker
and at least one weight object are present, it skips replication and exits
successfully. Otherwise it acquires the upload lock (atomically — only one
replica can hold it), deletes any stale completion marker, replicates, writes
the completion marker, and releases the lock. A run that finds the lock held
by someone else polls the target every 30 seconds until the artifact
completes, the lock is released, or the lock's age exceeds
`artifact_upload_lock_timeout` (then treated as stale and deleted). Both
sentinels are bookkeeping only: they are never replicated as model content
when the target is later used as a source.

Set `artifact_upload_lock_owner_id` to let retries of the *same* replication
operation adopt a lock left behind by a killed attempt instead of waiting for
it to expire. Use one ID for all retries of one operation and a new ID for
each independent operation, and make sure the previous attempt has actually
stopped before retrying with the same ID — the agent only compares IDs. All
replicas targeting the same artifact should use the same
`artifact_upload_lock_timeout`, longer than the longest replication you
expect, since a replica with a shorter timeout can delete a lock whose upload
is still running.

Two permissions notes: locking and reuse require list, read, write, and
delete permission on the target bucket; and even with reuse *disabled* the
agent deletes any existing `.ome-artifact-complete` object before uploading
(so readers cannot treat a half-overwritten prefix as complete), which also
requires delete permission on OCI targets.

## Example: mirror a Hugging Face model into OCI Object Storage

A ConfigMap holds the replication config, and a Job runs the agent with the
Hugging Face token injected from a Secret:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: llama-replica-config
  namespace: ome
data:
  ome-agent.yaml: |
    local_path: /workspace
    num_connections: 10

    source:
      storage_uri: "hf://meta-llama/Llama-3.1-8B-Instruct"

    target:
      storage_uri: "oci://n/mytenancy/b/model-mirror/o/models/meta-llama/Llama-3.1-8B-Instruct"
      checksum:
        upload_enabled: true
        algorithm: "md5"
        concurrency: 8
      oci:
        enabled: true
        auth_type: "InstancePrincipal"
        region: "us-chicago-1"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: replicate-llama-3-1-8b
  namespace: ome
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: replica
          image: ghcr.io/moirai-internal/ome-agent:v1.2.2
          args: ["replica", "--config", "/config/ome-agent.yaml"]
          env:
            - name: OME_AGENT_HF_TOKEN
              valueFrom:
                secretKeyRef:
                  name: hf-token
                  key: token
          volumeMounts:
            - name: config
              mountPath: /config
            - name: workspace
              mountPath: /workspace
      volumes:
        - name: config
          configMap:
            name: llama-replica-config
        - name: workspace
          emptyDir:
            sizeLimit: 40Gi
```

The `workspace` volume must hold the full snapshot before upload. For an
`oci://` → `pvc://` copy, replace the `emptyDir` with the target PVC and set
the target block to:

```yaml
target:
  storage_uri: "pvc://model-store/models/llama-3-1-8b"
  pvc:
    enabled: true
```

Once the weights are in the bucket or PVC, point a `BaseModel` or
`ClusterBaseModel` at them — see the
[BaseModel concept](/ome/docs/concepts/base_model) for the storage spec that
consumes `oci://` and `pvc://` URIs.
