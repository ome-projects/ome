---
title: Install OME
description: Install cert-manager, then the ome-crd and ome-resources Helm charts, and check that the OME controller and the optional model agent are running.
---

Install OME with Helm, and check that the OME controller and the optional [model agent](../guides/operate-ome/model-agent.md) are running. First you install cert-manager, which issues the certificate for OME's admission webhooks. Then you install two OME charts: `ome-crd` adds OME's custom resources, and `ome-resources` runs the controller and, when you turn it on, the model agent. [How OME works](../concepts/architecture/how-ome-works.md) explains each component.

The commands install v1.2.2, the latest release. The features marked Since v1.3, such as [OMENative](../concepts/omenative/overview.md), [Alfred](../concepts/scheduling/alfred.md) and the [OME scheduler](../concepts/scheduling/ome-scheduler.md), are in `main` but not yet in a release. To use them now, [install from source](#install-from-source).

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

Helm installs the release and reports its status as `deployed`. Since v1.3, the first install fails with [a webhook error](#the-first-install-fails-with-a-webhook-error): wait, then run the command again.

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

To run a build of `main`, with the features marked Since v1.3, build and push OME's three images from a checkout, then install the charts from it. The build needs Docker and Go, and the OME agent image also needs a Rust toolchain: see [Set up a development environment](../contributing/development-setup.md#before-you-begin). From the checkout, run:

```bash
make push-manager-image push-model-agent-image push-ome-agent-image REGISTRY=registry.example.com/ome TAG=dev
```

The command builds the images for `linux/amd64`, and pushes `ome-manager`, `model-agent` and `ome-agent` to `registry.example.com/ome` with the tag `dev`. Set `ARCH` for another platform.

Install cert-manager and the PodMonitor CRD as in [Step 1](#step-1-install-cert-manager), then the CRDs from `charts/ome-crd`:

```bash
helm upgrade --install ome-crd ./charts/ome-crd --namespace ome --create-namespace
```

Helm installs the release and reports its status as `deployed`.

Point the `ome-resources` chart at your images in `values.yaml`:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
ome:
  controller:
    tag: dev
  omeAgent:
    tag: dev
modelAgent:
  enabled: true
  image:
    tag: dev
```

Then install OME from `charts/ome-resources`:

=== "Helm 4"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      -f values.yaml \
      --server-side=false
    ```

=== "Helm 3"

    ```bash
    helm upgrade --install ome ./charts/ome-resources \
      --namespace ome \
      -f values.yaml
    ```

The first install fails, because the webhook certificate isn't ready yet:

```output
Release "ome" does not exist. Installing it now.
Error: Internal error occurred: failed calling webhook "clusterservingruntime.ome-webhook-server.validator": could not get REST client: unable to load root certificates: unable to parse bytes as PEM block
```

Wait, then run the command again, as [The first install fails with a webhook error](#the-first-install-fails-with-a-webhook-error) shows. In `helm list -n ome`, both charts have the version 0.1.0, the placeholder version in a checkout.

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

The first install of the `ome` release fails, because the webhook certificate isn't ready yet:

```text
Error: Internal error occurred: failed calling webhook "clusterservingruntime.ome-webhook-server.validator": could not get REST client: unable to load root certificates: unable to parse bytes as PEM block
```

Helm still creates the rest of the release. Wait for the certificate and the controller:

```bash
kubectl wait --for=condition=Ready certificate/serving-cert -n ome --timeout=5m
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
```

```output
certificate.cert-manager.io/serving-cert condition met
deployment "ome-controller-manager" successfully rolled out
```

Then run the same `helm upgrade --install` command again. If it fails with the same error, run it once more. If it keeps failing, check that both releases are in the namespace `ome`.

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

- [Serve your first model](serve-your-first-model.md): serve a small model and send it a request.
- [Pre-configured models and runtimes](pre-configured-models.md): install ready-made models, runtimes and InferenceServices from OME's catalog.
- [Serve a model on OMENative](../guides/omenative/serve-a-model-on-omenative.md): run a model on OME's own workload controller. Since v1.3.
- [Use the OME scheduler](../guides/operate-ome/ome-scheduler.md): place each gang of pods in one accelerator domain. Alpha, since v1.3.
- [Run Alfred in recommend-only mode](../guides/scheduling/run-alfred.md): let Alfred watch your GPU nodes and recommend migrations. Alpha, since v1.3.
