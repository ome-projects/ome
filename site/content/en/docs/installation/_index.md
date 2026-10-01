---
title: "Installation"
linkTitle: "Installation"
weight: 2
description: >
  Installing OME to a Kubernetes Cluster
---

<!-- toc -->
- [Before you begin](#before-you-begin)
  - [Required Components](#required-components)
  - [Optional Components](#optional-components)
  - [1. Install Cert Manager (Required)](#1-install-cert-manager-required)
  - [2. Install Istio (Optional - Virtual service ingress)](#2-install-istio-optional---virtual-service-ingress)
  - [3. Install KEDA (Optional - Custom metrics scaling)](#3-install-keda-optional---custom-metrics-scaling)
  - [4. Install Prometheus (Optional - Custom metrics scaling)](#4-install-prometheus-optional---custom-metrics-scaling)
  - [5. Install LeaderWorkerSet (Optional - MultiNode mode only)](#5-install-leaderworkerset-optional---multinode-mode-only)
  - [6. Install Kueue (Optional - Job scheduling)](#6-install-kueue-optional---job-scheduling)
  - [7. Clone OME repository](#7-clone-ome-repository)
- [Install the latest development version](#install-the-latest-development-version)
  - [Uninstall](#uninstall)
- [Move a manifest install to the Helm charts](#move-a-manifest-install-to-the-helm-charts)
<!-- /toc -->

## Before you begin

OME supports multiple deployment modes to enable `InferenceService` deployment with Kubernetes resources:

- **`RawDeployment`** (Default): Uses standard Kubernetes Deployment, Service, Ingress and HorizontalPodAutoscaler. Supports mounting multiple volumes but does not support scale to/from zero. Optionally supports custom metrics scaling with KEDA and Prometheus.
- **`OMENative`** (development, since v1.3): OME manages serving pods through InferenceReplicas. Supports single-node and multi-node workloads without LeaderWorkerSet.
- **`MultiNode`** (deprecated on `main`): Enables multi-node deployment for models that require distributed computing. **Requires: LeaderWorkerSet (LWS)**.

Prefill-decode disaggregation is a workload topology, not a `deploymentMode`
value. On development builds, use OMENative for its engine and decoder;
multi-node workloads on v1.2.2 require LeaderWorkerSet.

### Required Components

Make sure the following conditions are met:

- A Kubernetes cluster with version 1.27 or newer is running. Learn how to [install the Kubernetes tools](https://kubernetes.io/docs/tasks/tools/).
- The kubectl command-line tool has communication with your cluster.
- The cluster has a [cert-manager](https://cert-manager.io/docs/installation/) installed (minimum version 1.9.0).

### Optional Components

The following components are optional and only required for specific features:

| Component                 | Required For               | Description                                                |
|---------------------------|----------------------------|------------------------------------------------------------|
| **Istio**                 | Virtual Services           | Service mesh for traffic management (minimum version 1.19) |
| **KEDA**                  | Custom metrics autoscaling | Kubernetes Event-driven Autoscaling                        |
| **Prometheus**            | Custom metrics autoscaling | Metrics collection and monitoring                          |
| **LeaderWorkerSet (LWS)** | MultiNode mode             | Kubernetes API for distributed training workloads          |
| **Kueue**                 | Job scheduling             | Kubernetes-native job queueing                             |

!!! warning
    **Important**: If you plan to use the `MultiNode` deployment mode, you MUST install LeaderWorkerSet (LWS) BEFORE installing OME. The controller may panic if the CRD is not available when needed.

### 1. Install Cert Manager (Required)

**Required**

The minimally required Cert Manager version is 1.9.0, and you can refer to [Cert Manager installation guide](https://cert-manager.io/docs/installation/).

!!! Note
    Cert manager is required to provision webhook certs for production grade installation. Alternatively, you can run a self-signed certs generation script.

### 2. Install Istio (Optional - Virtual service ingress)

**Optional - Required only for Virtual Service ingress**

The minimally required Istio version is `1.19` and you can refer to the [Istio install guide](https://istio.io/latest/docs/setup/install).

Once Istio is installed, create `IngressClass` resource for istio:
```yaml
apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata:
  name: istio
spec:
  controller: istio.io/ingress-controller
```

!!! Note
    If you are running on a managed Kubernetes service, you can use the managed Istio service provided by the cloud provider.

!!! Note
    You can choose to install with other [Ingress controllers](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/) and create an `IngressClass` resource for your Ingress option.

### 3. Install KEDA (Optional - Custom metrics scaling)

**Optional - Required only for custom metrics autoscaling**

Please refer to [KEDA install guide](https://keda.sh/docs/2.6/deploy/).

### 4. Install Prometheus (Optional - Custom metrics scaling)

**Optional - Required only for custom metrics autoscaling with KEDA**

1. Get Helm Repository Information
```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```
2. Install kube-prometheus-stack
```shell
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack
```


### 5. Install LeaderWorkerSet (Optional - MultiNode mode only)

**Optional - Required for the MultiNode deployment mode**

Please refer to [LeaderWorkerSet installation guide](https://github.com/kubernetes-sigs/lws).

Example installation:
```shell
kubectl apply --server-side -f https://github.com/kubernetes-sigs/lws/releases/download/v0.3.0/lws-webhook.yaml
```

### 6. Install Kueue (Optional - Job scheduling)

**Optional - Required only for advanced job scheduling features**

Please refer to [Kueue installation guide](https://kueue.sigs.k8s.io/docs/installation/).

### 7. Clone OME repository

Clone the repository into a directory of your choice; it does not need to be
under `GOPATH`:

```shell
git clone https://github.com/ome-projects/ome.git
cd ome
```

Once you reach this point, you are ready to do a full build and deploy as
described below.


## Install the latest development version

Development features marked since v1.3 require a source build from `main`.
Use images, CRDs and Helm charts from the same checkout: `make install` alone
does not build a new controller image, and the source charts still default to
v1.2.2 images. Use a disposable development cluster, because OME installs
cluster-wide CRDs and admission webhooks. An existing installation needs an
upgrade review and its existing values, not the small lab profile below.

The stock image targets need Docker, Go 1.26 or newer and a local development
toolchain. Install Rust with Cargo, a C/C++ compiler, `pkg-config` and your
platform's OpenSSL development libraries. The targets run local `fmt` and
`vet`; prepare Xet before building. All three Dockerfiles also build Xet in
their build stage.

From a clean checkout, choose a registry your nodes can pull from and a platform
matching those nodes (`linux/arm64` for an ARM64 lab):

```shell
OME_SOURCE_TAG="src-$(git rev-parse HEAD)"
OME_IMAGE_REGISTRY=registry.example.com/ome
OME_IMAGE_PLATFORM=linux/amd64

make xet-build
make push-manager-image \
  REGISTRY="$OME_IMAGE_REGISTRY" TAG="$OME_SOURCE_TAG" ARCH="$OME_IMAGE_PLATFORM"
```

Treat the commit-derived tag as immutable. Changed source needs a new commit
and tag; reusing a tag can leave old images cached on nodes. Inspect `git diff`
after the build, since the Make targets can format source files.

Install cert-manager first. On current `main`, RawDeployment and MultiNode also
require the PodMonitor CRD, even if bundled Prometheus is disabled. If you do
not already have it through Prometheus Operator, install it before OME:

```shell
kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.79.2/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
```

An OMENative-only CPU lab can omit that CRD: OME skips OMENative PodMonitors
when the API is absent. Restart an existing controller after installing a new
optional CRD so that it discovers the API.

For a runtime that fetches its own weights, or a CPU-only OMENative lab,
deploy only the manager image. Save this as `source-values.yaml`. The single
replica and resource settings are for a small lab, not production or high
availability:

```yaml
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

Install both charts into `ome`, where the webhook CA annotations expect their
certificate. Set every OME image tag explicitly, including the optional agents:

```shell
helm upgrade --install ome-crd ./charts/ome-crd --namespace ome --create-namespace
helm upgrade --install ome ./charts/ome-resources \
  --namespace ome -f source-values.yaml \
  --set-string global.hub="$OME_IMAGE_REGISTRY" \
  --set-string ome.controller.tag="$OME_SOURCE_TAG" \
  --set-string ome.omeAgent.tag="$OME_SOURCE_TAG" \
  --set-string modelAgent.image.tag="$OME_SOURCE_TAG"
```

On Helm 4, add `--server-side=false` to the `ome-resources` command to avoid
server-side apply conflicts with cert-manager's webhook CA updates. Keep these
values and image overrides on each upgrade. For a new source revision, rebuild
the images you use and upgrade `ome-crd` before `ome-resources`.

If admission of the chart's default runtime races webhook startup, check that
`certificate/serving-cert` and `deployment/ome-controller-manager` exist in
`ome`. Wait for the certificate to be Ready and the Deployment to finish its
rollout, then retry the same Helm command. Persistent failures require checking
the certificate events, controller logs and CA injection; a first-install
webhook failure is not guaranteed.

Wait for the manager and check its image:

```shell
kubectl rollout status deployment/ome-controller-manager -n ome --timeout=5m
kubectl get deployment ome-controller-manager -n ome \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}{"\n"}'
```

For node-local managed BaseModel/ClusterBaseModel weights, also build and push
`model-agent` with `make push-model-agent-image` and the same `REGISTRY`, `TAG`
and `ARCH`. Enable `modelAgent.enabled` and size its resources and host path
before repeating the Helm command. Its defaults request 10 CPUs and 100 GiB
per node; do not enable them unchanged on a small lab.

For a model registered from an already-populated PVC, build and push
`ome-agent` with `make push-ome-agent-image` and the same image settings. OME
uses it for the metadata Job; the model-agent DaemonSet can stay disabled.
That image target also invokes the host Xet build. Encrypted models and other
agent-backed operations may need this image as well. Runtime-managed downloads
with no model resource need neither agent.


### Uninstall

Delete your OME workloads and models while their controllers are still running,
and wait for their finalizers to clear. Uninstall the resources chart before
the CRD chart:

```shell
helm uninstall ome -n ome
helm uninstall ome-crd -n ome
```

Uninstalling `ome-crd` deletes OME's CRDs and every remaining OME object.

## Move a manifest install to the Helm charts

Manifest installs (`make install`, or the release `manifests.yaml`) configure a
conversion webhook on the `inferenceservices.ome.io` CRD that the manager no
longer serves: a `spec.conversion` stanza, plus a
`cert-manager.io/inject-ca-from` annotation that makes cert-manager keep writing
a CA bundle into it. The `ome-crd` chart ships this CRD without a conversion
webhook, and applying it over that stanza can be rejected, for example with
`spec.conversion.strategy: Required value` or
`spec.conversion.webhookClientConfig: Forbidden`.

Before you install the `ome-crd` chart over a manifest install, remove the
annotation first, so cert-manager stops re-injecting the CA bundle, and then the
stanza:

```shell
kubectl annotate crd inferenceservices.ome.io cert-manager.io/inject-ca-from-
kubectl patch crd inferenceservices.ome.io --type=json \
  -p='[{"op":"remove","path":"/spec/conversion"}]'
```

The CRD serves a single version, so the webhook was never called and existing
InferenceServices are unaffected. The conversion strategy then reads `None`:

```shell
kubectl get crd inferenceservices.ome.io -o jsonpath='{.spec.conversion.strategy}'
```
