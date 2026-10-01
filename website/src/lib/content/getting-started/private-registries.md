---
title: Install from a private registry
description: "Run OME from your own or a mirrored registry: copy its images, set global.hub, and give pods pull credentials with imagePullSecrets."
---

Run OME from your own registry: a corporate mirror, or a registry in an air-gapped network. You install OME as in [Install OME](install.md), with two changes: `global.hub` points OME's images at your registry, and a pull Secret gives pods the credentials for it.

<div class="prerequisites" markdown>

- A registry that your nodes can pull from. The examples use `registry.example.com`, with OME's images under `registry.example.com/ome`.
- The images in [Images to copy](#images-to-copy), copied into that path.
- If Helm can't reach `ghcr.io`, the charts as files. On a machine that can, run `helm pull oci://ghcr.io/moirai-internal/charts/ome-resources --version 1.2.2`, and the same for `ome-crd`. Copy the `.tgz` files across, and give Helm their paths in place of the `oci://` URLs.
- cert-manager and the `ome-crd` chart, installed as in the first two steps of [Install OME](install.md). cert-manager has images of its own to copy; `ome-crd` has none.

</div>

## Images to copy

The chart names OME's own images `<global.hub>/<repository>:<tag>`, where the hub is `ghcr.io/moirai-internal` by default. Setting `global.hub` moves them all, including the images that OME adds to pods later. Copy them with their names and tags unchanged. A node pulls an image only when a pod runs it, so skip the images of features that you don't use.

| Image | Used by | Set with |
| --- | --- | --- |
| `ghcr.io/moirai-internal/ome-manager:v1.2.2` | The controller | `ome.controller.image`, `ome.controller.tag` |
| `ghcr.io/moirai-internal/model-agent:v1.2.2` | The [model agent](../guides/operate-ome/model-agent.md) | `modelAgent.image.repository`, `modelAgent.image.tag` |
| `ghcr.io/moirai-internal/ome-agent:v1.2.2` | Serving pods' optional `model-init` and `serving-sidecar` containers, and metadata Jobs for models on PVCs | `ome.omeAgent.image`, `ome.omeAgent.tag` |
| `ghcr.io/moirai-internal/genai-bench:0.0.3` | BenchmarkJobs. The chart's default tag has no image, so Step 2 sets `0.0.3` | `ome.benchmarkJob.image`, `ome.benchmarkJob.tag` |
| `ghcr.io/moirai-internal/multinode-prober:v1.2.2` | The `MultiNodeRayVLLM` deployment mode, on v1.2.2 only | `ome.multinodeProber.image`, `ome.multinodeProber.tag` |
| `docker.io/prom/prometheus:v3.0.1`, copied as `registry.example.com/ome/prometheus:v3.0.1` | The chart's Prometheus, since v1.3. `global.hub` doesn't move it | `prometheus.image.repository`, `prometheus.image.tag` |
| Your runtimes' images | Serving containers and routers | Each runner's `image`: see [Serving pods](#serving-pods) |

Since v1.3, the [OME scheduler](../guides/operate-ome/ome-scheduler.md) and [Alfred](../guides/scheduling/run-alfred.md), both alpha, and the [quota manager](../guides/operate-ome/accelerator-quota.md) have charts of their own. Each takes its own `global.hub` and `global.imagePullSecrets`. OME's releases don't publish their images, so you build them.

## Step 1: Create a pull Secret

Skip this step if your registry allows anonymous pulls, or your nodes already have credentials for it.

Create a Secret with your registry's credentials in the namespace `ome`:

```bash
kubectl create secret docker-registry ome-registry-cred -n ome \
  --docker-server=registry.example.com \
  --docker-username=<user> \
  --docker-password=<token>
```

```output
secret/ome-registry-cred created
```

Step 4 adds the Secret to your models' namespaces.

## Step 2: Point OME at your registry

Add these values to the `values.yaml` from [Install OME](install.md#step-3-install-ome), or create the file with them:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
  imagePullSecrets:
    - name: ome-registry-cred

modelAgent:
  enabled: true

ome:
  benchmarkJob:
    tag: "0.0.3"

prometheus:
  image:
    repository: registry.example.com/ome/prometheus
```

| Value | What it does |
| --- | --- |
| `global.hub` | The path that holds your copies of OME's images. |
| `global.imagePullSecrets` | The pull Secrets of the controller, the model agent and, since v1.3, Prometheus. Leave it out if you skipped Step 1. |
| `modelAgent.enabled` | Deploys the model agent, as in [Install OME](install.md#step-3-install-ome). |
| `ome.benchmarkJob.tag` | The tag of the genai-bench image that BenchmarkJobs run. |
| `prometheus.image.repository` | Your copy of the Prometheus image. Since v1.3. |

`ome.controller.imagePullSecrets` and `modelAgent.imagePullSecrets` replace `global.imagePullSecrets` for their own pod. If your values set either one, add `ome-registry-cred` to it.

Install or upgrade OME with the values file:

=== "Helm 4"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      -f values.yaml \
      --server-side=false
    ```

    [Install OME](install.md#step-3-install-ome) explains why OME needs `--server-side=false`.

=== "Helm 3"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      -f values.yaml
    ```

Helm reports the release's status as `deployed`, and on an upgrade the controller and model agent pods restart with images from your registry. On a first source installation, admission can race webhook startup. If that happens, follow [webhook troubleshooting](install.md#the-first-install-fails-with-a-webhook-error), then retry the same command with the same image settings.

## Step 3: Check the images

List the image references in the manifest that Helm installed:

```bash
helm get manifest ome -n ome | grep -E 'image:|"image":'
```

```output
        "image": "registry.example.com/ome/multinode-prober:v1.2.2",
        "image":  "registry.example.com/ome/ome-agent:v1.2.2",
        "image": "registry.example.com/ome/ome-agent:v1.2.2",
        "image": "registry.example.com/ome/ome-agent:v1.2.2",
        "image": "registry.example.com/ome/genai-bench:0.0.3",
        image: registry.example.com/ome/model-agent:v1.2.2
        image: registry.example.com/ome/ome-manager:v1.2.2
```

Every line should name your registry. Since v1.3, the multinode-prober line is gone, and the list ends with the Prometheus image.

Then wait for the controller and the model agent to be ready, as in [Step 4 of Install OME](install.md#step-4-verify-the-installation).

## Step 4: Set up your model namespaces

OME also runs pods in your models' namespaces: serving pods, BenchmarkJob pods and the metadata Jobs of models on PVCs. Your values give pull secrets only to the pods in `ome`, so if your registry needs credentials, create the Secret in each of these namespaces too. For `qwen3-0-6b`, the namespace of [Serve your first model](serve-your-first-model.md):

```bash
kubectl create namespace qwen3-0-6b
kubectl create secret docker-registry ome-registry-cred -n qwen3-0-6b \
  --docker-server=registry.example.com \
  --docker-username=<user> \
  --docker-password=<token>
```

```output
namespace/qwen3-0-6b created
secret/ome-registry-cred created
```

### Serving pods

First, point your runtimes at your registry. A runtime names its images in every `runner` under its `engineConfig`, `decoderConfig` and `routerConfig`, including those in `leader` and `worker` blocks. An InferenceService can override them. The `ome-serving` chart takes them from `defaults.image` and `defaults.routerImage`, which are `docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64` and `fra.ocir.io/idqj093njucb/smg:v0.2.4.post1-dev` by default: see [ome-serving chart values](../reference/operate-ome/ome-serving-values.md#defaults).

Since v1.3, a runtime with the annotation `ome.io/engine: sglang-pd` fills in a runner that has no image with `lmsysorg/sglang:latest` or `lmsysorg/sglang-router:latest` from Docker Hub, so set every runner's image.

Then add the Secret to the InferenceService's `engine`, and to its `decoder` and `router` if it runs them. When you create the InferenceService in [Serve your first model](serve-your-first-model.md#step-5-create-the-inferenceservice), add it like this:

```yaml title="qwen3-0-6b-isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-0-6b
spec:
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
    imagePullSecrets:
      - name: ome-registry-cred
```

To cover every InferenceService that uses a runtime, set `imagePullSecrets` in the runtime's `engineConfig`, `decoderConfig` or `routerConfig` instead. The runtime's top-level `imagePullSecrets` doesn't reach the pods.

Engine and decoder pods that have no `imagePullSecrets` get the pull secrets of their ServiceAccount: the namespace's `default`, unless the component sets `serviceAccountName`. Routers run as their own ServiceAccount, named after the InferenceService with `-router` added, so give them `imagePullSecrets`. On v1.2.2, where `leader` and `worker` blocks make a component `MultiNode` (deprecated), set `imagePullSecrets` in those blocks too.

If you serve [fine-tuned weights](../concepts/models/fine-tuned-weights.md#configure-the-download), the `fineTunedAdapter` entry that you add to `inferenceservice-config` names the adapter's image. Point its `image` at your registry.

### BenchmarkJob and metadata Job pods {#benchmarkjob-pods}

These pods get pull secrets only from their ServiceAccount, so give it the Secret. Check its list of pull secrets first: the first patch below replaces the list.

=== "BenchmarkJob"

    The pods of a [BenchmarkJob](../guides/deploy-models/run-benchmarks.md) run as the namespace's `default` ServiceAccount, and so does the multinode prober on v1.2.2. Check its pull secrets:

    ```bash
    kubectl get serviceaccount default -n qwen3-0-6b -o jsonpath='{.imagePullSecrets}'
    ```

    If the output is empty, give the ServiceAccount the Secret:

    ```bash
    kubectl patch serviceaccount default -n qwen3-0-6b \
      -p '{"imagePullSecrets": [{"name": "ome-registry-cred"}]}'
    ```

    ```output
    serviceaccount/default patched
    ```

    When it prints a list without `ome-registry-cred`, add the Secret to the list instead, so that other pods that run as `default` keep theirs:

    ```bash
    kubectl patch serviceaccount default -n qwen3-0-6b --type=json \
      -p '[{"op": "add", "path": "/imagePullSecrets/-", "value": {"name": "ome-registry-cred"}}]'
    ```

    ```output
    serviceaccount/default patched
    ```

=== "Metadata Job"

    The metadata Job of a [model on a PVC](../guides/deploy-models/serve-models-from-pvc.md) runs in the PVC's namespace, as the ServiceAccount `ome-model-metadata`. OME creates it without pull secrets when it's missing, and uses an existing one as it is, so create it yourself first:

    ```bash
    kubectl create serviceaccount ome-model-metadata -n qwen3-0-6b
    ```

    ```output
    serviceaccount/ome-model-metadata created
    ```

    If it already exists, the command fails with an `already exists` error. Check its pull secrets:

    ```bash
    kubectl get serviceaccount ome-model-metadata -n qwen3-0-6b -o jsonpath='{.imagePullSecrets}'
    ```

    If the output is empty, give the ServiceAccount the Secret:

    ```bash
    kubectl patch serviceaccount ome-model-metadata -n qwen3-0-6b \
      -p '{"imagePullSecrets": [{"name": "ome-registry-cred"}]}'
    ```

    ```output
    serviceaccount/ome-model-metadata patched
    ```

    When it prints a list without `ome-registry-cred`, add the Secret to the list instead:

    ```bash
    kubectl patch serviceaccount ome-model-metadata -n qwen3-0-6b --type=json \
      -p '[{"op": "add", "path": "/imagePullSecrets/-", "value": {"name": "ome-registry-cred"}}]'
    ```

    ```output
    serviceaccount/ome-model-metadata patched
    ```

To check, run the tab's `kubectl get` command again: the list now includes `ome-registry-cred`.

## Override a single image

Add these values to `values.yaml`, and run the command from Step 2 again. A repository that contains a `/` is used as written, without `global.hub`:

```yaml
ome:
  controller:
    image: registry.example.com/custom/ome-manager
    tag: v1.2.2-patched
```

`modelAgent.image.hub` replaces `global.hub` for the model agent only. A BenchmarkJob that sets `spec.podOverride.image` runs that image instead of the chart's, so point it at your registry too.

Since v1.3, the upgrade restarts the controller when its ConfigMaps change. On v1.2.2, after you change `ome.omeAgent.image` or `ome.omeAgent.tag` but not the controller's image, restart the controller so that metadata Jobs get the new image:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

## Troubleshooting

A pod that can't pull an image stays in `ErrImagePull` or `ImagePullBackOff`. Run `kubectl describe pod` to see the container that's waiting: the pod's events give the image reference and the registry's answer.

### A pod pulls from the old registry

Fix the image by container:

- The controller or the model agent: run [Step 2](#step-2-point-ome-at-your-registry) again, and check the result with [Step 3](#step-3-check-the-images).
- `model-init` or `serving-sidecar`: the pod predates your change. Delete it, and its replacement gets the new image.
- `fine-tuned-adapter`: fix the `fineTunedAdapter` entry as in [Serving pods](#serving-pods), then delete the pod.
- The serving container or the router: point the runner images at your registry, as in [Serving pods](#serving-pods).

### The Prometheus pod pulls from Docker Hub {since=v1.3}

`global.hub` doesn't move the Prometheus image. Set `prometheus.image.repository` to your copy, as in [Step 2](#step-2-point-ome-at-your-registry), or set `prometheus.enabled` to `false` if you run your own Prometheus.

### The image isn't found

The path or the tag in the reference doesn't match your copy. Copy the image under that name, or fix the value that sets it: see [Images to copy](#images-to-copy).

### The registry refuses the pull

The pod has no Secret with credentials that the registry accepts:

- Pods in `ome`: check that `ome-registry-cred` is in `ome` with the right credentials, and that the pod's pull secrets name it, as in [Step 2](#step-2-point-ome-at-your-registry).
- Serving pods: add the Secret as in [Serving pods](#serving-pods).
- BenchmarkJob and metadata Job pods: give the ServiceAccount the Secret, as in [BenchmarkJob and metadata Job pods](#benchmarkjob-pods). A pod gets its ServiceAccount's pull secrets only when it's created, so then replace the pod. Delete a metadata Job's pod, and the Job creates another. A BenchmarkJob's Job doesn't retry a failed pod, so delete the Job, and OME creates a new Job and pod.

## Next steps

- [Serve your first model](serve-your-first-model.md): point its runtime's image at your copy, and add the Secret to its InferenceService as in [Serving pods](#serving-pods). Skip its `kubectl create namespace`, because Step 4 created the namespace.
- [Pre-configured models and runtimes](pre-configured-models.md): install models and runtimes from OME's catalog, and point their images at your registry with the chart's values.
