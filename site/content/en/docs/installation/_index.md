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
- **`MultiNode`**: Enables multi-node deployment for models that require distributed computing. **Requires: LeaderWorkerSet (LWS)**.
- **`PDDisaggregated`**: Enables prefill-decode disaggregated deployment for models that require most optimal performance. **Requires: LeaderWorkerSet (LWS) for larger models that require distributed computing**.

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

The Go tools require that you clone the repository to the
`src/sigs.k8s.io/ome` directory in your
[`GOPATH`](https://github.com/golang/go/wiki/SettingGOPATH).

To check out this repository:

1. Create your own
   [clone this repo](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
1. Clone it to your machine:

```shell
mkdir -p ${GOPATH}/src/github.com/ome-projects
cd ${GOPATH}/src/github.com/ome-projects
git clone https://github.com/ome-projects/ome.git
cd ome
```

Once you reach this point, you are ready to do a full build and deploy as
described below.


## Install the latest development version

To install the latest development version of OME in your cluster, run the
following command:

```shell
make install
```

The controller runs in the `ome` namespace.


### Uninstall

To uninstall OME, run the following command:

```shell
make uninstall
```

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
