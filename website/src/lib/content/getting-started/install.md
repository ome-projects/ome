---
title: Install OME
description: Install cert-manager, then the ome-crd and ome-resources Helm charts, and check that the OME controller and the optional model agent are running.
---

Install OME with Helm, and check that the OME controller and the optional [model agent](../guides/operate-ome/model-agent.md) are running. First you install cert-manager, which issues the certificate for OME's admission webhooks. Then you install two OME charts: `ome-crd` adds OME's custom resources, and `ome-resources` runs the controller and, when you turn it on, the model agent. [How OME works](../concepts/architecture/how-ome-works.md) explains each component.

The release commands below are pinned to v1.2.2; check the [releases](https://github.com/ome-projects/ome/releases) when choosing a version. This documentation also covers development features marked Since v1.3, such as [OMENative](../concepts/omenative/overview.md), [Alfred](../concepts/scheduling/alfred.md) and the [OME scheduler](../concepts/scheduling/ome-scheduler.md). Use the separate [source installation](#install-from-source) for those guides, with images, CRDs and charts from the same checkout. Installing the v1.2.2 release does not enable them.

<div class="prerequisites" markdown>

- A Kubernetes cluster running version 1.28 or newer.
- `kubectl`, with the rights to create CustomResourceDefinitions, ClusterRoles, ClusterRoleBindings and webhook configurations, as the `cluster-admin` role has.
- Helm 3 or Helm 4.
- Room for OME's pods. The controller runs 3 replicas that request 2 CPUs and 4 GiB of memory each. The model agent runs a pod on each node that requests 10 CPUs and 100 GiB of memory. [Step 3](#step-3-install-ome) shows how to keep it off some nodes.

</div>

!!! warning "Install into the ome namespace"
    Install both OME charts into the namespace `ome`. In any other namespace, OME's webhook configurations never get their CA bundle. The API server then rejects every new or changed [InferenceService](../concepts/serving/inference-services.md), model and runtime, and every new serving pod.

## Step 1: Install cert-manager and the PodMonitor CRD {#step-1-install-cert-manager}

If your cluster already runs cert-manager, skip to [The PodMonitor CRD](#the-podmonitor-crd). Otherwise, install it with its CRDs:

```bash
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version v1.21.2 \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true
```

Helm installs cert-manager and reports the release's status as `deployed`, followed by cert-manager's notes. The `ome-resources` chart brings its own issuer, so you don't need to create one.

Wait for cert-manager to be available:

```bash
kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=5m
```

```output
deployment.apps/cert-manager condition met
deployment.apps/cert-manager-cainjector condition met
deployment.apps/cert-manager-webhook condition met
```

### The PodMonitor CRD {since=v1.3}

[RawDeployment](../concepts/architecture/deployment-modes.md) and `MultiNode` (deprecated) components need the Prometheus Operator's PodMonitor CRD, or their InferenceServices never become Ready. RawDeployment is the default for a component without a leader and workers, such as the engine in [Serve your first model](serve-your-first-model.md). If you run the Prometheus Operator, you already have the CRD. Otherwise, install the CRD on its own:

```bash
kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.79.2/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
```

```output
customresourcedefinition.apiextensions.k8s.io/podmonitors.monitoring.coreos.com serverside-applied
```

The controller looks for the CRD only when it starts. If OME is already running, restart it with `kubectl rollout restart deployment/ome-controller-manager -n ome`.

An OMENative-only installation can omit the PodMonitor CRD: OMENative skips creating PodMonitors when that API is absent. The [CPU-only OMENative lab](../guides/omenative/learn-omenative.md) uses this smaller setup. Turning off the chart's bundled Prometheus does not remove RawDeployment's PodMonitor requirement.

## Step 2: Install the CRDs

Install the `ome-crd` chart, which adds OME's CustomResourceDefinitions, as the release `ome-crd`:

```bash
helm upgrade --install ome-crd oci://ghcr.io/moirai-internal/charts/ome-crd \
  --version 1.2.2 \
  --namespace ome --create-namespace
```

Helm installs the release and reports its status as `deployed`.

`--version` takes the release's version without the leading `v`. Install the same version of both OME charts, and pass `--version` on every upgrade, or Helm installs the latest.

Check that the API server serves OME's resources:

```bash
kubectl api-resources --api-group=ome.io
```

```output
NAME                     SHORTNAMES   APIVERSION       NAMESPACED   KIND
acceleratorclasses                    ome.io/v1beta1   false        AcceleratorClass
basemodels                            ome.io/v1beta1   true         BaseModel
benchmarkjobs                         ome.io/v1beta1   true         BenchmarkJob
clusterbasemodels                     ome.io/v1beta1   false        ClusterBaseModel
clusterservingruntimes                ome.io/v1beta1   false        ClusterServingRuntime
finetunedweights                      ome.io/v1beta1   false        FineTunedWeight
inferenceservices        isvc         ome.io/v1beta1   true         InferenceService
servingruntimes                       ome.io/v1beta1   true         ServingRuntime
```

Since v1.3, the list also has `acceleratorquotas`, `inferencereplicas`, `trafficmaps` and `workloadclusters`.

## Step 3: Install OME

The `ome-resources` chart runs the OME controller, as the Deployment `ome-controller-manager`, and the model agent, as the DaemonSet `ome-model-agent-daemonset`. The model agent downloads the weights of BaseModels and ClusterBaseModels to your nodes. Since v1.3, it's optional and off by default. Models on a PersistentVolumeClaim and [runtimes that fetch their own weights](../concepts/serving/inference-services.md#the-runtime) don't need it.

Keep your settings for the chart in a values file. This one turns on the model agent, because [Serve your first model](serve-your-first-model.md) uses a ClusterBaseModel:

```yaml title="values.yaml"
modelAgent:
  enabled: true
```

Install the chart as the release `ome`, with the values file:

=== "Helm 4"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      -f values.yaml \
      --server-side=false
    ```

    Helm 4 installs with server-side apply by default, and a later upgrade then fails with a conflict over the CA bundle that cert-manager writes into OME's webhook configurations. With `--server-side=false`, Helm 4 applies the chart on the client side, as Helm 3 does, and later upgrades keep that method.

=== "Helm 3"

    ```bash
    helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --version 1.2.2 \
      --namespace ome \
      -f values.yaml
    ```

Helm installs the release and reports its status as `deployed`. On a source install, admission of the chart's default runtime can race webhook startup. If that happens, follow [the webhook troubleshooting steps](#the-first-install-fails-with-a-webhook-error).

Pass the same values file to every upgrade: `helm upgrade -f` sets every value that isn't in the file back to the chart's default.

The model agent runs a pod on each node, including GPU nodes with the taint `nvidia.com/gpu`. Its priority class is `system-node-critical`, so Kubernetes can evict lower-priority pods to make room for it. To change where it runs, or what the chart installs, add these values to `values.yaml` and run the command again:

| Value | Default | What it does |
| --- | --- | --- |
| `modelAgent.gpuNodesOnly` | `false` | `true` runs the model agent only on nodes with the label `nvidia.com/gpu.present=true`, which the NVIDIA GPU Operator sets. |
| `modelAgent.nodeSelector` | `{}` | Runs the model agent only on the nodes that it selects. |
| `modelAgent.hostPath` | `/mnt/data/models` | The directory on each node where the model agent keeps model weights. A model's `storage.path` must be inside it. To apply the models in `config/models/`, set `/raid/models`. |
| `prometheus.enabled` | `true` | Runs Prometheus as the Deployment `ome-prometheus`. OME queries it for canary analysis, and KEDA triggers can use it. `false` leaves it out. Since v1.3. |

[Helm values](../guides/operate-ome/model-agent.md#helm-values) lists the model agent's other settings. Since v1.3, the chart also creates a [ClusterServingRuntime](../concepts/runtimes/serving-runtimes.md) named `default-runtime`, which OME uses only for InferenceServices that name it.

## Step 4: Verify the installation

Wait for the controller and the model agent to be ready:

```bash
kubectl wait --for=condition=Available deployment/ome-controller-manager -n ome --timeout=5m
kubectl rollout status daemonset/ome-model-agent-daemonset -n ome --timeout=5m
```

```output
deployment.apps/ome-controller-manager condition met
daemon set "ome-model-agent-daemonset" successfully rolled out
```

Then check which images they run:

```bash
kubectl get deployments,daemonsets -n ome \
  -o custom-columns='KIND:.kind,NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image'
```

```output
KIND         NAME                        IMAGE
Deployment   ome-controller-manager      ghcr.io/moirai-internal/ome-manager:v1.2.2
DaemonSet    ome-model-agent-daemonset   ghcr.io/moirai-internal/model-agent:v1.2.2
```

Since v1.3, the list also has the Deployment `ome-prometheus`, with the image `docker.io/prom/prometheus:v3.0.1`.

OME is ready to serve a model.

## Optional components

Add these when you need their features. The controller looks for most of these components' CRDs only when it starts, so if you install one after OME, restart the controller with `kubectl rollout restart deployment/ome-controller-manager -n ome`.

| Component | What it adds | See |
| --- | --- | --- |
| The `ome-scheduler` chart | The [OME scheduler](../concepts/scheduling/ome-scheduler.md), which places each gang of pods in one accelerator domain. It installs only on Kubernetes 1.35, and needs the scheduler-plugins PodGroup CRD. Alpha, since v1.3. | [Use the OME scheduler](../guides/operate-ome/ome-scheduler.md) |
| The `ome-alfred` chart | [Alfred](../concepts/scheduling/alfred.md), OME's GPU cluster caretaker, which watches the GPU nodes and recommends migrations. By default, it only recommends. Alpha, since v1.3. | [Run Alfred in recommend-only mode](../guides/scheduling/run-alfred.md) |
| The scheduler-plugins PodGroup CRD | Gang scheduling for OMENative components, with a scheduler that reads PodGroups, such as the OME scheduler. Since v1.3. | [Gang scheduling](../concepts/serving/gang-scheduling.md) |
| The `ome-quota-manager` chart | Accelerator quotas, which it renders into Kueue cohorts and queues. Its workload mode needs Kueue. Since v1.3. | [Set accelerator quotas](../guides/operate-ome/accelerator-quota.md) |
| The `ome-serving` chart | Ready-made models, runtimes and InferenceServices from OME's catalog. | [Pre-configured models and runtimes](pre-configured-models.md) |
| Prometheus Operator | A PodMonitor for each InferenceService component, so that Prometheus scrapes the serving pods. Since v1.3. | [The PodMonitor CRD](#the-podmonitor-crd), [Collect metrics](../guides/operate-ome/metrics.md) |
| KEDA | Autoscaling with KEDA ScaledObjects. | [Scaling](../concepts/serving/inference-services.md#scaling) |
| Gateway API | HTTPRoutes in place of Ingresses. Off by default. | [Configure ingress](../guides/networking/configure-ingress.md) |
| Envoy Gateway or Istio | Load-balancing settings for an InferenceService. Since v1.3. | [Traffic policy](../concepts/rollouts-and-traffic/traffic-policy.md) |
| LeaderWorkerSet (deprecated) | The deprecated `MultiNode` mode. Since v1.3, [OMENative](../concepts/omenative/overview.md) serves multi-node models without it. On v1.2.2, multi-node serving needs it. | [Move from LeaderWorkerSet to OMENative](../guides/omenative/move-from-leaderworkerset.md) |

The `ome-scheduler`, `ome-alfred` and `ome-quota-manager` charts need images that you build yourself. Each chart's guide shows how.

## Install from source

This is the development path for `main` and the features marked Since v1.3. It is separate from the pinned v1.2.2 installation above: use the controller image and both Helm charts from the **same source checkout**. Source charts still default to v1.2.2 images, so installing a local chart without image overrides does not install a source build.

Use a disposable development cluster for these steps. The charts install cluster-wide CRDs and admission webhooks. Before upgrading an existing installation, preserve its values and review the API and configuration changes between your old and new commits; the small lab profile below is not an upgrade profile for an existing deployment.

### Build the controller image

Start from a clean checkout of the source commit you want to run. Install Docker, Go 1.26 or newer, a C/C++ toolchain, `pkg-config`, the platform's OpenSSL development libraries and Rust with Cargo, as in [Set up a development environment](../contributing/development-setup.md#before-you-begin). Authenticate to a container registry that your cluster can pull from.

The stock image targets run `make fmt` and `make vet` on the host before building. Prepare the local Xet library first; all three Dockerfiles also build Xet inside their build stage. A manager-only installation needs only the manager image, but these stock build commands are not a Rust-free build path.

```bash
OME_SOURCE_TAG="src-$(git rev-parse HEAD)"
OME_IMAGE_REGISTRY=registry.example.com/ome
OME_IMAGE_PLATFORM=linux/amd64

make xet-build
make push-manager-image \
  REGISTRY="$OME_IMAGE_REGISTRY" TAG="$OME_SOURCE_TAG" ARCH="$OME_IMAGE_PLATFORM"
```

Replace the registry and choose the platform of your Kubernetes nodes, for example `linux/arm64` for an ARM64 lab. The command builds and pushes `ome-manager` with the full source commit in its tag. Treat that tag as immutable: changed source needs a new commit and tag, not a replacement image under the old tag. The controller uses `IfNotPresent`, so reusing a mutable tag can leave old images on nodes. Check `git diff` after the build, because the Make targets can format source files; if build inputs changed, commit them and rebuild with the new tag.

### Install a manager-only profile

This profile is enough when a serving runtime downloads its own weights, as in [Deploy an InferenceService](../guides/deploy-models/deploy-an-inferenceservice.md), or for the [CPU-only OMENative lab](../guides/omenative/learn-omenative.md). Neither needs the model-agent DaemonSet or an OME-agent image. The controller does not need a GPU; the model-serving workload may still need one.

Install cert-manager as in [Step 1](#step-1-install-cert-manager). Also install the PodMonitor CRD if you will use RawDeployment or MultiNode; an OMENative-only lab can omit it. Then install the CRDs from this checkout:

```bash
helm upgrade --install ome-crd ./charts/ome-crd --namespace ome --create-namespace
```

Save this manager-only profile. Its single replica and reduced resource requests are for a small lab, not high availability or a production sizing recommendation:

```yaml title="source-values.yaml"
ome:
  controller:
    replicaCount: 1
    resources:
      requests:
        cpu: 100m
        memory: 256Mi
      limits:
        cpu: "1"
        memory: 1Gi
modelAgent:
  enabled: false
prometheus:
  enabled: false
```

This also leaves out bundled Prometheus. Metrics-driven autoscaling and canary analysis need a suitable metrics backend when you add those features.

Install OME from the same checkout, setting all three OME image tags explicitly. The optional agents will use the matching tag if you enable them later:

=== "Helm 4"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      -f source-values.yaml \
      --set-string global.hub="$OME_IMAGE_REGISTRY" \
      --set-string ome.controller.tag="$OME_SOURCE_TAG" \
      --set-string ome.omeAgent.tag="$OME_SOURCE_TAG" \
      --set-string modelAgent.image.tag="$OME_SOURCE_TAG" \
      --server-side=false
    ```

=== "Helm 3"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      -f source-values.yaml \
      --set-string global.hub="$OME_IMAGE_REGISTRY" \
      --set-string ome.controller.tag="$OME_SOURCE_TAG" \
      --set-string ome.omeAgent.tag="$OME_SOURCE_TAG" \
      --set-string modelAgent.image.tag="$OME_SOURCE_TAG"
    ```

Keep the values file, source commit, registry and tag together, and pass the same image overrides on every upgrade. For a new source revision, rebuild the images you use under its new tag, upgrade `ome-crd` first, then `ome-resources`. Do not mix a newer controller with older CRDs, or the source charts with a v1.2.2 controller.

If installation fails while admitting the default runtime, check [webhook startup](#the-first-install-fails-with-a-webhook-error). Otherwise, wait for the controller and check its image:

```bash
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
kubectl get deployment ome-controller-manager -n ome \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}{"\n"}'
```

The image must match `$OME_IMAGE_REGISTRY/ome-manager:$OME_SOURCE_TAG`. There is no model-agent DaemonSet to wait for in this profile. In `helm list -n ome`, the local charts have the placeholder version `0.1.0`; use your recorded source commit and the deployed image to identify the build.

### Add managed model weights when needed

Build only the additional images required by your storage path, from the same checkout and with the same tag:

| Storage path | Additional image and chart settings |
| --- | --- |
| BaseModel or ClusterBaseModel weights cached on nodes | Build `model-agent`, set `modelAgent.enabled: true`, and choose its node selectors, resource requests and `hostPath`. The model's `storage.path` must be inside that host path. |
| A model registered from an existing PVC | Build `ome-agent` for the metadata Job. The model-agent DaemonSet can stay disabled. The PVC must already contain the model files. |
| Runtime-managed downloads, with no model resource | Neither agent is required. Keep the manager-only profile. |

For node-local managed weights:

```bash
make push-model-agent-image \
  REGISTRY="$OME_IMAGE_REGISTRY" TAG="$OME_SOURCE_TAG" ARCH="$OME_IMAGE_PLATFORM"
```

For PVC metadata Jobs or other OME-agent-backed operations:

```bash
make push-ome-agent-image \
  REGISTRY="$OME_IMAGE_REGISTRY" TAG="$OME_SOURCE_TAG" ARCH="$OME_IMAGE_PLATFORM"
```

The OME-agent image target also invokes `make xet-build` on the host. It supplies Jobs and init containers; it is not a DaemonSet that you turn on globally. Encrypted models and other agent-backed operations may need it in addition to the node-local model agent.

For node-local weights, edit `source-values.yaml` to enable and size the model agent, then repeat the Helm command above with all image overrides. Its defaults request 10 CPUs and 100 GiB of memory **per node**, so do not enable it unchanged on a small lab. For PVC-backed weights, the image override already configures the metadata Job without enabling the DaemonSet. Continue with [Stage model weights](../guides/deploy-models/stage-model-weights.md) or [Serve models from a PVC](../guides/deploy-models/serve-models-from-pvc.md).

## Move a manifest install to the Helm charts

If you installed OME with `make install` or `kubectl apply -k config/default`, you can [move it to the Helm charts](../guides/operate-ome/move-to-the-helm-charts.md) and keep your InferenceServices, models and runtimes in place.

## Troubleshooting

### Helm reports resource mapping not found

If cert-manager or OME's CRDs are missing, installing the `ome` release fails before it creates anything, and names each object of an unknown kind:

```text
Error: unable to build kubernetes objects from release manifest: [resource mapping not found for name: "serving-cert" namespace: "ome" from "": no matches for kind "Certificate" in version "cert-manager.io/v1"
ensure CRDs are installed first, resource mapping not found for name: "default-runtime" namespace: "" from "": no matches for kind "ClusterServingRuntime" in version "ome.io/v1beta1"
ensure CRDs are installed first, resource mapping not found for name: "selfsigned-issuer" namespace: "ome" from "": no matches for kind "Issuer" in version "cert-manager.io/v1"
ensure CRDs are installed first]
```

`Certificate` and `Issuer` mean that cert-manager isn't installed: install it as in [Step 1](#step-1-install-cert-manager). `ClusterServingRuntime` means that OME's CRDs aren't installed: install them as in [Step 2](#step-2-install-the-crds). Then run the command from Step 3 again.

### The first install fails with a webhook error {since=v1.3}

During a first source installation, the chart's default runtime can reach admission before the webhook certificate, its CA bundle or the controller is ready. One possible error is:

```text
Error: Internal error occurred: failed calling webhook "clusterservingruntime.ome-webhook-server.validator": could not get REST client: unable to load root certificates: unable to parse bytes as PEM block
```

Check whether Helm created the certificate and controller, and inspect their status:

```bash
kubectl get certificate/serving-cert deployment/ome-controller-manager -n ome
```

If they exist, wait for both:

```bash
kubectl wait --for=condition=Ready certificate/serving-cert -n ome --timeout=5m
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

```output
certificate.cert-manager.io/serving-cert condition met
deployment "ome-controller-manager" successfully rolled out
```

Then retry the same `helm upgrade --install` command with the same values and image settings. If it still fails, check that both releases are in the namespace `ome`, inspect the certificate's events and the controller's logs, and verify cert-manager's CA injection. If the certificate or Deployment was not created, resolve the earlier Helm error instead of waiting for a missing resource.

### An upgrade fails with a conflict

On Helm 4, `helm upgrade` can fail with a server-side apply conflict over the `clientConfig.caBundle` field of an OME webhook configuration. Run the upgrade again with `--server-side=false`, as in [Step 3](#step-3-install-ome).

### The controller or the model agent isn't ready

If the check in Step 4 times out, it prints:

```text
error: timed out waiting for the condition on deployments/ome-controller-manager
```

Find the pod that isn't running with `kubectl get pods -n ome`, and read its events with `kubectl describe pod -n ome`, followed by the pod's name:

- A pod that stays `Pending` with `Insufficient cpu` or `Insufficient memory` needs more room than its node has. Keep the model agent off smaller nodes with `modelAgent.gpuNodesOnly` or `modelAgent.nodeSelector`, or lower `modelAgent.resources`.
- `ErrImagePull` or `ImagePullBackOff` means that the node can't pull the image. If your nodes pull from a mirror, see [Install from a private registry](private-registries.md).
- On v1.2.2, the controller's pods don't become ready when OME's CRDs are missing. Install them as in [Step 2](#step-2-install-the-crds).

## Uninstall

Delete your InferenceServices, models, [accelerator classes](../concepts/runtimes/accelerator-classes.md) and [benchmark jobs](../concepts/serving/benchmarks.md) before you uninstall OME. Only the controller removes their finalizers, so once it's gone, you can't delete them or OME's CRDs. Deleting a model also deletes its weights from the nodes. To keep them, first give the model the label `models.ome/reserve-model-artifact: "true"`.

Check that none are left:

```bash
kubectl get inferenceservices,basemodels,clusterbasemodels,acceleratorclasses,benchmarkjobs --all-namespaces
```

```output
No resources found
```

Since v1.3, add `inferencereplicas,acceleratorquotas` to the command: InferenceReplicas carry the finalizer `ome.io/ir-teardown`, and AcceleratorQuotas carry `ome.io/accelerator-quota`, which only the quota manager removes.

Then uninstall the `ome` release, and the `ome-crd` release after it:

```bash
helm uninstall ome -n ome
helm uninstall ome-crd -n ome
```

```output
release "ome" uninstalled
release "ome-crd" uninstalled
```

Uninstalling `ome-crd` deletes OME's CRDs, and with them any OME objects left in the cluster. Check that the API server no longer serves them:

```bash
kubectl api-resources --api-group=ome.io
```

```output
NAME   SHORTNAMES   APIVERSION   NAMESPACED   KIND
```

The namespace `ome` stays. Delete it once both releases are gone:

```bash
kubectl delete namespace ome
```

```output
namespace "ome" deleted
```

cert-manager and the PodMonitor CRD stay installed, since other applications can use them.

## Next steps

- [Deploy an InferenceService](../guides/deploy-models/deploy-an-inferenceservice.md): use a runtime that downloads its own model weights. Requires the source installation.
- [Learn OMENative on a CPU-only cluster](../guides/omenative/learn-omenative.md): inspect the serving lifecycle without GPUs or model downloads. Requires the source installation.
- [Serve your first model](serve-your-first-model.md): serve a small model and send it a request.
- [Pre-configured models and runtimes](pre-configured-models.md): install ready-made models, runtimes and InferenceServices from OME's catalog.
- [Serve a model on OMENative](../guides/omenative/serve-a-model-on-omenative.md): run a model on OME's own workload controller. Since v1.3.
- [Use the OME scheduler](../guides/operate-ome/ome-scheduler.md): place each gang of pods in one accelerator domain. Alpha, since v1.3.
- [Run Alfred in recommend-only mode](../guides/scheduling/run-alfred.md): let Alfred watch your GPU nodes and recommend migrations. Alpha, since v1.3.
