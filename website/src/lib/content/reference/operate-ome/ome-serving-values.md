---
title: ome-serving chart values
description: Look up the ome-serving Helm chart's values for each catalog model, with their defaults, the GPU presets and the PD mode settings.
---

The `ome-serving` chart deploys models from OME's catalog, as [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md) shows. Set these values under a model's key in your values file, then run `helm upgrade --install` again.

Default is what the chart uses without the value. The catalog's entry for a model, in the chart's `values.yaml`, sets some values, such as `hfModelId` and `runtime.gpus`, and yours replace them.

## Model values

| Value | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Deploys the model. |
| `path` | None | `storage.path`: the directory on each node that holds the weights, which the serving pods mount. It must be inside the model agent's host directory, `/mnt/data/models` by default. |
| `hfModelId` | The catalog's | A repository on Hugging Face. The storage URI becomes `hf://` followed by it. |
| `oci.namespace`, `oci.bucket`, `oci.object` | None | An object in OCI Object Storage. The storage URI becomes `oci://n/<namespace>/b/<bucket>/o/<object>`. |
| `storageUri` | None | The storage URI itself. See [Where the weights come from](../../concepts/models/base-models.md#where-the-weights-come-from). |
| `key` | None | `storage.key`: the Secret with a Hugging Face token, for a gated model. The model agent reads it from the namespace `ome`; see [Credentials](../../concepts/models/base-models.md#credentials). |
| `parameters` | None | `storage.parameters`, such as `secretKey`, the key in the Secret that holds the token. |
| `nodeSelector`, `nodeAffinity` | None | `storage.nodeSelector` and `storage.nodeAffinity`, which choose the nodes that download the model. See [Choose the nodes](../../concepts/models/base-models.md#choose-the-nodes). |
| `vendor`, `capabilities` | The catalog's | The model's `vendor` and `modelCapabilities`. |
| `createModel` | `true` | `false` skips the ClusterBaseModel and the BaseModel. |
| `createRuntime` | `true` | `false` creates no runtime, so the model needs no registry entry. OME picks another runtime that supports the model and sets `autoSelect: true`. |
| `clusterScope` | `true` | `false` skips the ClusterBaseModel, which the chart's InferenceService uses, so leave it on. |
| `namespaceScope` | `false` | `true` also creates a BaseModel named after the key, for InferenceServices of your own that set `kind: BaseModel`. |
| `namespace` | The key | With `namespaceScope: true`, the namespace of the InferenceService and the BaseModel. The chart creates it, so pick a new one for each model. |
| `pdMode` | `false` | Splits the model's serving into prefill and decode. See [PD mode](#pd-mode). |

A model that the chart creates needs one of `hfModelId`, `oci` and `storageUri`, or the chart fails to render. With more than one, `hfModelId` wins, then `oci`. To load a catalog model from somewhere other than Hugging Face, set `hfModelId: null`, which removes the catalog's value, and set `oci` or `storageUri`.

With `createModel: false`, create a ClusterBaseModel named after the key yourself, or the webhook rejects the InferenceService. The files in `config/models/` use paths under `/raid/models`, which work only when you install OME with `modelAgent.hostPath=/raid/models`, as [Step 3 of Install OME](../../getting-started/install.md#step-3-install-ome) explains.

## Runtime values

Set these under the model's `runtime` key. Keep a `runtime` key for each model whose runtime the chart creates: without one, the chart fails to render.

| Value | Default | Description |
| --- | --- | --- |
| `gpus` | `1` | The GPUs that the engine requests, and SGLang's `--tp-size`. The engine's CPU and memory come from the [GPU preset](#gpu-presets) for this number. |
| `image` | `defaults.image` | The SGLang image, for the engine and, in PD mode, the decoder. |
| `memFrac` | `defaults.memFrac` | SGLang's `--mem-frac`. |
| `extraArgs` | None | More arguments, added at the end of SGLang's command. |
| `routerImage` | `defaults.routerImage` | The router's image. |
| `ibDevice` | `defaults.ibDevice` | In PD mode, SGLang's `--disaggregation-ib-device`. |
| `rdmaProfile` | `defaults.rdmaProfile` | In PD mode, the RDMA profile that OME applies to the engine and decoder pods. |

Helm replaces a list instead of merging it, so a model's `extraArgs` in your values file replaces the catalog's. To add an argument to Phi-4-mini-instruct, list `--trust-remote-code` too.

Some catalog entries set `cpu` and `memory`, which the chart ignores: the engine's CPU and memory come only from its GPU preset.

## Replicas

Each component's minimum and maximum replicas come from the model's values, or else from `defaults.minReplicas` and `defaults.maxReplicas`, which are both `1`:

| Component | Values |
| --- | --- |
| Engine | `engine.minReplicas` and `engine.maxReplicas`, or `minReplicas` and `maxReplicas` |
| Decoder, in PD mode | `decoder.minReplicas` and `decoder.maxReplicas` |
| Router | `router.minReplicas` and `router.maxReplicas` |

Outside PD mode, the InferenceService gets a router when the model's `router` value is a map with at least one key, such as `router: {minReplicas: 1}`. The chart fails to render when `engine`, `decoder` or `router` isn't a map, such as `router: true`. The chart treats `0` as unset, so you can't set a count to `0`.

## Defaults

A model that doesn't set a runtime value gets it from the chart's `defaults`. To change it for every model, set it under `defaults` at the top level of your values file:

```yaml title="charts/ome-serving/values.yaml (excerpt)"
defaults:
  image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
  routerImage: fra.ocir.io/idqj093njucb/smg:v0.2.4.post1-dev
  memFrac: "0.9"
  minReplicas: 1
  maxReplicas: 1
  # PD (Prefill-Decode disaggregation) mode defaults
  ibDevice: mlx5_0        # InfiniBand device for RDMA
  rdmaProfile: oci-roce   # RDMA profile for network
```

To pull the images from your own registry, set `image` and `routerImage` to your copies. The chart has no value for image pull Secrets, and its routers run as the ServiceAccount `<key>-router`, not the namespace's `default`: see [Serving pods](../../getting-started/private-registries.md#serving-pods).

## GPU presets

The engine, and the decoder in PD mode, request the CPU and memory of the preset for their `gpus`, with limits equal to the requests:

| `gpus` | CPU | Memory |
| --- | --- | --- |
| 1 | 10 | 30Gi |
| 2 | 20 | 80Gi |
| 4 | 20 | 160Gi |
| 8 | 40 | 320Gi |

A `gpus` value without a preset makes the chart fail to render, including the catalog's `gpus: 16` for `llama-4-maverick-17b-128e-instruct`. To use another number of GPUs, or other resources, add or change the preset under `gpuPresets` in your values file, which changes it for every model that uses that number. The router's limits are always 1 CPU and 2 GiB.

## PD mode

`pdMode: true` splits the model's serving into prefill and decode. The engine runs SGLang with `--disaggregation-mode prefill`, the InferenceService gets a decoder that runs it with `--disaggregation-mode decode`, and a router finds the engine and decoder pods by their labels. The engine and the decoder use the InfiniBand device `ibDevice`, so PD mode needs GPU nodes with RDMA networking that one of OME's [RDMA profiles](../api/labels-and-annotations.md#pod-keys-you-set) supports. Since v1.3, the profiles are `oci-roce`, `cks-gb-sglang` and `cks-gb-rdma`. On v1.2.2, the only one is `oci-roce`.

In PD mode, the engine and decoder pods:

- use their node's network. Each one takes port 8080 on its node, so a node runs at most one engine or decoder pod in PD mode, whichever model it serves.
- run SGLang in a privileged container, which can use all of its node's devices, with the profile `oci-roce`, and since v1.3 with `cks-gb-sglang` too.

With a profile that OME doesn't know, OME's pod webhook blocks the pods, and the component's ReplicaSet records `FailedCreate` events.

The chart sets no deployment mode, so the engine, decoder and router run in RawDeployment mode, where a change is a plain Deployment update. Since v1.3, [OMENative](../../concepts/omenative/overview.md) can roll prefill and decode out as one group, by canary, blue-green or rolling update: see [Rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md). Until v1.3 is released, that needs [a build of `main`](../../getting-started/install.md#install-from-source). To use it, install the model and its runtime, with `pdMode: true`, as in [Install a model without serving it](../../getting-started/pre-configured-models.md#install-a-model-without-serving-it). Then create an InferenceService of your own that sets `spec.deploymentMode: OMENative`, as in [Serve a prefill-decode model](../../guides/omenative/serve-a-prefill-decode-model.md#step-2-create-the-inferenceservice).

## Related pages

- [Pre-configured models and runtimes](../../getting-started/pre-configured-models.md): install the chart, and deploy models from the catalog.
- [Base models](../../concepts/models/base-models.md): the storage settings that the model values set.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): how OME picks a runtime for a model.
- [Install from a private registry](../../getting-started/private-registries.md): copy the images, and let the serving pods pull them.
