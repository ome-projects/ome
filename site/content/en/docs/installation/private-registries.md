---
title: "Private Registries"
linkTitle: "Private Registries"
weight: 10
description: >
  Point every OME image at your own or mirrored registry with global.hub, and supply pull credentials with imagePullSecrets.
---

This page covers installing OME when its container images must come from your
own registry — a corporate mirror, a cloud provider registry, or an air-gapped
environment. It applies to the `ome-resources` chart, which deploys or
configures every OME image; the `ome-crd` chart ships only CRDs and pulls
nothing.

Two separate concerns are involved, and both usually need attention:

- **Where images are pulled from** — `global.hub` and the per-image values.
- **How pulls are authenticated** — `global.imagePullSecrets` for the pods the
  chart manages, plus standard Kubernetes mechanisms for the pods OME creates
  at runtime.

Runtime images (the SGLang and vLLM images referenced by `ServingRuntime` and
`ClusterServingRuntime` specs, see `config/runtimes/`) and model weights are
not chart images: mirror those separately and edit the runtime specs to point
at your mirror.

## How image references are built

Most images in `charts/ome-resources/values.yaml` are bare repository names
(`ome-manager`, `model-agent`, `ome-agent`, `genai-bench`). At render time the
chart's `ome.imageWithHub` helper joins them with `global.hub`:

- If `global.hub` is set and the repository contains **no** `/`, the image is
  `<hub>/<repository>:<tag>`.
- If the repository already contains a `/` (a fully qualified reference), the
  hub is ignored and the image renders as `<repository>:<tag>`.

`global.hub` defaults to `ghcr.io/moirai-internal`. The images it governs, with
their current defaults (`ome.version: v1.2.2`):

| Image (default) | Runs as | Values keys |
| --- | --- | --- |
| `ghcr.io/moirai-internal/ome-manager:v1.2.2` | controller Deployment | `ome.controller.image`, `ome.controller.tag` |
| `ghcr.io/moirai-internal/model-agent:v1.2.2` | model-agent DaemonSet (when `modelAgent.enabled=true`) | `modelAgent.image.repository`, `modelAgent.image.tag` |
| `ghcr.io/moirai-internal/ome-agent:v1.2.2` | model-init init container, fine-tuned-adapter serving sidecar, and model metadata Job | `ome.omeAgent.image`, `ome.omeAgent.tag` |
| `ghcr.io/moirai-internal/genai-bench:0.1.113` | BenchmarkJob pods | `ome.benchmarkJob.image`, `ome.benchmarkJob.tag` |

The controller and model-agent images land directly in their Deployment and
DaemonSet. The ome-agent and genai-bench images land in the
`inferenceservice-config` and `benchmarkjob-config` ConfigMaps, from which the
controller reads them when it builds serving pods, metadata Jobs, and benchmark
Jobs — so changing `global.hub` reroutes those runtime-created pods too.

**The bundled Prometheus is the exception.** Its image is rendered as-is from
`prometheus.image.repository` (default `docker.io/prom/prometheus`) and
`prometheus.image.tag` (default `v3.0.1`), without the hub helper. Setting
`global.hub` does not touch it: override its repository with a fully qualified
path, or set `prometheus.enabled=false` if you run your own Prometheus.

The default tags above track `ome.version` (`v1.2.2`) except genai-bench and
Prometheus; check the `values.yaml` of the chart version you install.

## Point everything at a mirror

Mirror the images, then set `global.hub` to the mirrored path prefix and
override the Prometheus repository:

```yaml
# registry-values.yaml
global:
  hub: registry.example.com/ome
  imagePullSecrets:
    - name: ome-registry-cred

prometheus:
  image:
    repository: registry.example.com/ome/prometheus
```

```bash
helm upgrade --install ome charts/ome-resources \
  --namespace ome -f registry-values.yaml
```

For an air-gapped install, the complete set to mirror at the defaults is:

- `ghcr.io/moirai-internal/ome-manager:v1.2.2`
- `ghcr.io/moirai-internal/model-agent:v1.2.2`
- `ghcr.io/moirai-internal/ome-agent:v1.2.2`
- `ghcr.io/moirai-internal/genai-bench:0.1.113`
- `docker.io/prom/prometheus:v3.0.1`

plus whatever runtime images your `ServingRuntime`s reference and, for
BaseModels downloaded from the internet, the model weights themselves.

## Override a single image

Because the helper skips the hub whenever the repository contains a `/`, a
fully qualified per-image value opts that one image out of `global.hub`:

```yaml
ome:
  controller:
    image: registry.example.com/custom/ome-manager
    tag: v1.2.2-patched
```

The model agent additionally accepts a per-image hub: a non-empty
`modelAgent.image.hub` replaces `global.hub` for the model-agent image only,
leaving the repository name bare:

```yaml
modelAgent:
  image:
    hub: registry.other.example.com/ome
```

A `BenchmarkJob` can also override its own image per job via
`spec.podOverride.image`, which takes precedence over the configured
`ome.benchmarkJob.image`.

## Supply pull credentials

Create a docker-registry Secret in the release namespace:

```bash
kubectl -n ome create secret docker-registry ome-registry-cred \
  --docker-server=registry.example.com \
  --docker-username=<user> \
  --docker-password=<token>
```

`global.imagePullSecrets` (a list of `{name: ...}` references, default empty)
is applied to the pods **the chart itself manages**:

- the controller Deployment — overridable with
  `ome.controller.imagePullSecrets`;
- the model-agent DaemonSet — overridable with `modelAgent.imagePullSecrets`;
- the bundled Prometheus Deployment — `global.imagePullSecrets` only, no
  per-component override.

The per-component values are a *replacement*, not a merge: when
`ome.controller.imagePullSecrets` is set, the controller pod gets exactly that
list and `global.imagePullSecrets` is ignored for it.

## Pods OME creates at runtime

The controller creates pods in workload namespaces, and those pods do **not**
inherit `global.imagePullSecrets`. Their images come from the chart-rendered
ConfigMaps (so `global.hub` already points them at your mirror), but pull
credentials follow the standard Kubernetes rules in each pod's own namespace:

- **Serving pods** — these carry the model-init init container, the
  fine-tuned-adapter serving sidecar, and the runtime image. Set
  `imagePullSecrets` on the InferenceService component spec
  (`spec.engine.imagePullSecrets`, and the same field on `decoder` and
  `router`), or inside a runtime's `engineConfig`/`decoderConfig`/
  `routerConfig` so every InferenceService using that runtime inherits it.
  Component-level secrets propagate to leader and worker pods in multi-node
  deployments, and the referenced Secrets must exist in the
  InferenceService's namespace. Note that the `ServingRuntime` *top-level*
  pod spec also has an `imagePullSecrets` field, but the reconcilers read
  only the merged component specs, so that field does not reach generated
  pods — use the component configs. Alternatively, attach the secret to the
  namespace's `default` ServiceAccount.
- **BenchmarkJob pods** — neither the generated pod spec nor
  `spec.podOverride` carries an `imagePullSecrets` field, so attach the secret
  to the `default` ServiceAccount of the BenchmarkJob's namespace:

  ```bash
  kubectl -n <benchmark-namespace> patch serviceaccount default \
    -p '{"imagePullSecrets": [{"name": "ome-registry-cred"}]}'
  ```

- **Model metadata Jobs** (PVC-backed models) — the Job runs in the
  namespace of the model's PVC under the ServiceAccount named by
  `ome.omeAgent.metadataJob.serviceAccount` (default `ome-model-metadata`).
  The controller creates that ServiceAccount only when it is missing and never
  updates an existing one, so patching it with pull secrets is safe:

  ```bash
  kubectl -n <pvc-namespace> patch serviceaccount ome-model-metadata \
    -p '{"imagePullSecrets": [{"name": "ome-registry-cred"}]}'
  ```

## Verify before installing

Render the chart and inspect every image reference — both the pod specs
(`image:`) and the ConfigMap-embedded ones (`"image":`):

```bash
helm template ome charts/ome-resources -f registry-values.yaml \
  | grep -E 'image:|"image"'
```

Every line should name your registry. A bare, unqualified reference (for
example after setting `global.hub` to an empty string) leaves pods in
`ImagePullBackOff`.

The separate `ome-quota-manager` chart has its own `global.hub` with the same
semantics — see [Accelerator Quota](/docs/administration/accelerator-quota/).
