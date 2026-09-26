---
title: "Pre-configured Models and Runtimes (ome-serving)"
linkTitle: "ome-serving Chart"
weight: 10
description: >
  Install the optional ome-serving Helm chart to deploy pre-configured ClusterBaseModels, SGLang ClusterServingRuntimes, and InferenceServices from a built-in catalog of over 160 models.
---

The `ome-serving` chart is an optional add-on that turns a one-line values
change into a complete serving stack for a catalog model: the model resource,
a matching [SGLang](https://github.com/sgl-project/sglang) runtime, a dedicated
namespace, and an `InferenceService`. The chart ships a built-in registry of
over 160 models (Qwen, Llama, DeepSeek, Mistral, Gemma, Phi, and more) in
`charts/ome-serving/templates/_helpers.tpl`; the registry supplies the model
architecture, transformers version, size range, and served model name, so you
only choose the model, its storage, and a GPU count.

Every model in `charts/ome-serving/values.yaml` ships `enabled: false`, so
installing the chart with no overrides renders nothing.

## Prerequisites

- OME installed: the `ome-crd` chart first, then `ome-resources` (see the
  [installation guide](/ome/docs/installation/)).
- GPU nodes exposing `nvidia.com/gpu`. The generated runtimes request GPUs and
  tolerate the `nvidia.com/gpu` taint.
- A checkout of the OME repository — the chart is installed from
  `charts/ome-serving`.

## Install

Enable one or more catalog models, either on the command line:

```shell
helm install ome-serving charts/ome-serving \
  --set models.qwen2-7b-instruct.enabled=true
```

or with a values file:

```yaml
# my-models.yaml
models:
  qwen2-7b-instruct:
    enabled: true
```

```shell
helm install ome-serving charts/ome-serving -f my-models.yaml
```

For each enabled model the chart creates:

| Resource | Name | Condition |
|----------|------|-----------|
| `ClusterBaseModel` | `<model-name>` | unless `clusterScope: false` or `createModel: false` |
| `BaseModel` | `<model-name>` in the target namespace | only when `namespaceScope: true`, unless `createModel: false` |
| `ClusterServingRuntime` | `srt-<model-name>` | unless `createRuntime: false` |
| `Namespace` | `<model-name>` (or `namespace`) | always |
| `InferenceService` | `<model-name>` in that namespace | always |

Verify:

```shell
kubectl get clusterbasemodel qwen2-7b-instruct
kubectl get clusterservingruntime srt-qwen2-7b-instruct
kubectl get inferenceservice -n qwen2-7b-instruct
```

The model agent then downloads the weights to each node and the
InferenceService comes up once the download and the SGLang startup probe
complete. See [Base Model](/ome/docs/concepts/base_model/) for model download
status and authentication (for example a Hugging Face token for gated models).

!!! Note
    The generated InferenceService does not pin a runtime; OME's
    [runtime selection](/ome/docs/concepts/serving_runtime/) matches it to the
    generated `srt-<model-name>` runtime. Automatic selection only considers
    runtimes whose supported formats set `autoSelect: true`, and that flag
    comes from the model's registry entry in `templates/_helpers.tpl` — some
    entries set `autoSelect: false`. Check the registry entry before enabling
    a model; for an `autoSelect: false` entry, write your own InferenceService
    that sets `spec.runtime.name: srt-<model-name>` instead of relying on the
    generated one.

## Per-model options

Model names under `models:` must match a registry entry in
`templates/_helpers.tpl`; rendering fails with `Model '<name>' not found in
registry` otherwise (only when a runtime is rendered — see
[`createRuntime`](#creating-only-the-model-or-only-the-runtime) below).

### Scope: cluster-wide or namespaced model

- `clusterScope` (default `true`) — create a `ClusterBaseModel`.
- `namespaceScope` (default `false`) — create a namespaced `BaseModel`
  instead. The target namespace comes from `namespace` and defaults to the
  model name; the generated Namespace and InferenceService follow it.

```yaml
models:
  qwen2-7b-instruct:
    enabled: true
    clusterScope: false
    namespaceScope: true
    namespace: ml-team
```

`namespace` only takes effect when `namespaceScope: true`; with the default
cluster scope, the Namespace and InferenceService are always named after the
model.

### Storage: one of three shorthands

Exactly one storage source is required per model; rendering fails if none is
set. When several are set, `hfModelId` wins over `oci`, which wins over
`storageUri`:

- `hfModelId: Qwen/Qwen2-7B-Instruct` renders
  `storageUri: hf://Qwen/Qwen2-7B-Instruct`.
- `oci: {namespace: <ns>, bucket: <bucket>, object: <path>}` renders
  `storageUri: oci://n/<ns>/b/<bucket>/o/<path>`.
- `storageUri: <uri>` is passed through verbatim, for any
  [storage backend](/ome/docs/concepts/base_model/#storage-backends) BaseModel
  supports (`pvc://`, `vendor://`, ...).

Optional fields passed through to the model's `spec.storage`: `path` (node
path for the downloaded weights), `schemaPath`, `key` (the Secret key used to
authenticate storage access), `parameters`, `nodeSelector`, and
`nodeAffinity`.

`vendor` and `capabilities` (for example `[TEXT_TO_TEXT]`,
`[IMAGE_TEXT_TO_TEXT]`, `[EMBEDDING]`) are also set per model and passed to
the model resource; the shipped `values.yaml` already fills them in for every
catalog entry.

### Creating only the model, or only the runtime

Both flags default to `true`:

- `createModel: false` — skip the `ClusterBaseModel`/`BaseModel`. Useful when
  the model resource already exists or is managed elsewhere.
- `createRuntime: false` — skip the `ClusterServingRuntime`. Useful when a
  shared runtime already serves this architecture. This also skips the
  registry lookup, so it is the only way to enable the few `values.yaml`
  entries that have no registry entry (for example `deepseek-r1` and
  `qwen3-14b`).

The Namespace and InferenceService are rendered for every enabled model
regardless of these flags — there is no flag to skip them.

### Runtime options

```yaml
models:
  qwen2-7b-instruct:
    enabled: true
    runtime:
      gpus: 1
      memFrac: "0.85"
      extraArgs: ["--max-running-requests", "512"]
```

| Field | Default | Effect |
|-------|---------|--------|
| `gpus` | `1` | `--tp-size`, the `nvidia.com/gpu` request/limit, and the CPU/memory preset |
| `image` | `defaults.image` (`docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64`) | SGLang container image |
| `routerImage` | `defaults.routerImage` (`fra.ocir.io/idqj093njucb/smg:v0.2.4.post1-dev`) | SGLang router image |
| `memFrac` | `defaults.memFrac` (`"0.9"`) | `--mem-frac` |
| `extraArgs` | — | appended to the `sglang.launch_server` command |
| `modelCacheProviders` | — | `supportedModelFormats[].modelCacheProviders`; opt in only for runtime images that can load sharded models through the provider |
| `ibDevice` | `defaults.ibDevice` (`mlx5_0`) | PD mode only, see below |
| `rdmaProfile` | `defaults.rdmaProfile` (`oci-roce`) | PD mode only, see below |

CPU and memory come only from the `gpuPresets` table, keyed by `gpus`:

```yaml
gpuPresets:
  1: { cpu: 10, memory: 30Gi }
  2: { cpu: 20, memory: 80Gi }
  4: { cpu: 20, memory: 160Gi }
  8: { cpu: 40, memory: 320Gi }
```

Rendering fails if `gpus` has no matching preset (the catalog's
`llama-4-maverick-17b-128e-instruct` entry sets `gpus: 16`, so enabling it
requires adding a `16:` preset). To change CPU or memory, override the preset;
per-model `runtime.cpu`/`runtime.memory` values that appear in the shipped
`values.yaml` are not read by the current templates.

### Replicas

`defaults.minReplicas`/`defaults.maxReplicas` (both `1`) apply everywhere. Per
model, set flat `minReplicas`/`maxReplicas` for the engine, or nested blocks
per component:

```yaml
models:
  qwen2-7b-instruct:
    enabled: true
    engine:
      minReplicas: 1
      maxReplicas: 3
    router:            # in non-PD mode, adding this block opts the
      minReplicas: 1   # InferenceService into a router component
      maxReplicas: 1
```

### PD mode (prefill-decode disaggregation)

`pdMode: true` switches the model to disaggregated serving:

```yaml
models:
  kimi-k2-instruct:
    enabled: true
    pdMode: true
    runtime:
      gpus: 8
      ibDevice: mlx5_0
      rdmaProfile: oci-roce
    decoder:
      minReplicas: 1
      maxReplicas: 2
```

What changes when `pdMode: true`:

- The runtime's engine runs SGLang with `--disaggregation-mode prefill` and
  `--disaggregation-ib-device <ibDevice>`, and a `decoderConfig` is added that
  runs `--disaggregation-mode decode` with the same resources.
- Engine and decoder pods get RDMA auto-injection annotations
  (`rdma.ome.io/auto-inject`, `rdma.ome.io/profile`,
  `rdma.ome.io/container-name`), `hostNetwork: true` with
  `dnsPolicy: ClusterFirstWithHostNet`, and — for the `oci-roce` and
  `cks-gb-sglang` profiles — a privileged security context.
- The router runs with `--pd-disaggregation` and separate
  `--prefill-selector`/`--decode-selector` flags instead of a single selector.
- The InferenceService gains `decoder` and `router` components (in PD mode the
  router is mandatory, not opt-in).
- Health probes use `/health` instead of `/health_generate`.

PD mode requires RDMA-capable GPU nodes (InfiniBand or RoCE); OME's pod
mutating webhook reads the `rdma.ome.io/*` annotations and injects the RDMA
configuration for the chosen profile.

## Uninstall

```shell
helm uninstall ome-serving
```

!!! warning
    Uninstalling deletes everything the chart rendered, including the
    per-model Namespaces — and with them anything else you created in those
    namespaces.

## Adding models outside the catalog

The registry lives in `charts/ome-serving/templates/_helpers.tpl`. To serve a
model that is not listed, add an entry with its architecture, transformers
version, size range, and served name, then reference it under `models:`:

```yaml
my-custom-model:
  architecture: LlamaForCausalLM
  transformersVersion: "4.43.0"
  autoSelect: true
  priority: 1
  sizeRange: ["7B", "9B"]
  servedName: my-org/my-custom-model
```

Alternatively, set `createRuntime: false` and match the model against a
runtime you manage yourself.
