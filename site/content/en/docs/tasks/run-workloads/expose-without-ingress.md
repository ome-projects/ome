---
title: "Expose an InferenceService Without Ingress"
linkTitle: "Expose Without Ingress"
weight: 20
date: 2026-09-27
description: >
  Learn how to reach an InferenceService through the plain Kubernetes Service OME creates when ingress creation is disabled, and how to make it a LoadBalancer or NodePort with the ome.io/service-type annotation.
---

This page shows you how to expose an InferenceService without an Ingress, HTTPRoute, or VirtualService. When ingress creation is disabled — cluster-wide or per service — OME instead creates a plain Kubernetes Service named exactly after the InferenceService, called the **external Service**. You control its `spec.type` (ClusterIP, NodePort, or LoadBalancer) with the `ome.io/service-type` annotation and pass cloud load-balancer annotations through to it, so you can hand traffic to a cloud load balancer, a node-port setup, or your own networking layer instead of an ingress controller.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- A running or planned InferenceService — see [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/)

## How the external Service works

OME reconciles the external Service after the per-component Services on every pass. It creates one only when **all** of the following hold:

- Ingress creation is disabled for this InferenceService — either `disableIngressCreation: true` in the cluster ingress configuration or the `ome.io/ingress-disable-creation: "true"` annotation on the InferenceService (the annotation overrides the cluster setting in both directions).
- The InferenceService is not labeled `networking.knative.dev/visibility: cluster-local`. That label keeps a service internal, so no externally reachable Service is created for it.
- The InferenceService spec declares an `engine` or a `router` component. A spec that declares neither has nothing for the Service to target.

The Service is created in the InferenceService's namespace with the InferenceService's own name (no `-engine`/`-router` suffix) and is owned by the InferenceService, so deleting the InferenceService deletes it too. Its properties:

- **Target component** — the selector targets the router's pods (`component: router`) if `spec.router` is set, otherwise the engine's pods (`component: engine`), always scoped with `ome.io/inferenceservice: <name>`. The router is preferred because it fronts the engine (and decoder) when present.
- **Port** — a single TCP port named `http`. The number is read from the first port of the targeted component's own generated Service (`<name>-router` or `<name>-engine`), which mirrors the runner container's port; `targetPort` is the same number. If that Service can't be read yet, the port falls back to `8080`.
- **Type** — from the `ome.io/service-type` annotation on the InferenceService: `LoadBalancer`, `NodePort`, or `ClusterIP`. Unset or any other value defaults to `ClusterIP`.
- **Annotations** — annotations on the InferenceService whose keys start with `service.beta.kubernetes.io/`, `cloud.google.com/`, or `service.kubernetes.io/` are copied onto the Service, so cloud-provider load-balancer settings flow through. Other annotation prefixes are **not** copied.

When the conditions stop holding — for example you re-enable ingress creation or add the cluster-local label — OME deletes the external Service again on the next reconcile.

With ingress disabled, the InferenceService's `status.url` and `status.address.url` point at the external Service's in-cluster DNS name with the Service port, using the `urlScheme` from the ingress configuration (default `http`), for example `http://<name>.<namespace>.svc.cluster.local:8080`.

## Step 1: Disable ingress creation

Cluster-wide, set `disableIngressCreation` in the `ingress` entry of the `inferenceservice-config` ConfigMap — see [Ingress Administration](/ome/docs/administration/ingress/) for the full configuration reference:

```yaml
data:
  ingress: |
    {
      "disableIngressCreation": true
    }
```

Or per InferenceService, with the override annotation:

```yaml
metadata:
  annotations:
    ome.io/ingress-disable-creation: "true"
```

The annotation wins over the ConfigMap for that InferenceService, so you can disable ingress for a single service on a cluster that otherwise creates ingress resources — or set it to `"false"` to keep ingress for one service on a cluster where creation is disabled globally.

## Step 2: Set the Service type and load-balancer annotations

Add `ome.io/service-type` and any cloud-provider Service annotations to the InferenceService metadata:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-1b-demo
  annotations:
    ome.io/ingress-disable-creation: "true"
    ome.io/service-type: LoadBalancer
    # Cloud-provider annotations are copied through to the Service, e.g. on AWS:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
    service.beta.kubernetes.io/aws-load-balancer-internal: "true"
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
EOF
```

For `NodePort`, set `ome.io/service-type: NodePort`; Kubernetes allocates the node port. For a plain in-cluster endpoint, leave the annotation off — the Service defaults to `ClusterIP`.

## Step 3: Verify

Check the Service OME created — it carries the InferenceService's exact name:

```bash
kubectl get service llama-3-2-1b-instruct -n llama-1b-demo
```

Example output:

```
NAME                    TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)          AGE
llama-3-2-1b-instruct   LoadBalancer   10.96.113.22   203.0.113.42    8080:31627/TCP   2m
```

The InferenceService status points at the same endpoint:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-1b-demo -o jsonpath='{.status.url}'
```

Example output:

```
http://llama-3-2-1b-instruct.llama-1b-demo.svc.cluster.local:8080
```

Then send a request to the external IP (or node IP and node port) on the Service port:

```bash
curl http://203.0.113.42:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "llama-3-2-1b-instruct", "messages": [{"role": "user", "content": "Hello"}]}'
```

## Scope of the annotations

`ome.io/service-type` is read from object metadata wherever OME builds a Service, and InferenceService annotations propagate onto the generated component resources. Setting it on the InferenceService therefore changes the type of **every** generated Service — `<name>-engine`, `<name>-decoder`, `<name>-router`, and the external Service — not just the external one. Keep that in mind before setting `LoadBalancer`: each LoadBalancer Service provisions its own cloud load balancer.

`ome.io/load-balancer-ip` sets `spec.loadBalancerIP` on the generated **component** Services when their type is LoadBalancer. The external Service does not honor it — pin its address with your cloud provider's Service annotations instead (which Step 2 shows are copied through).

Both annotations are listed in the [Labels and Annotations reference](/ome/docs/reference/labels-and-annotations/).

## Troubleshooting

**No Service with the InferenceService's name exists:** confirm ingress creation is actually disabled for this service (`ome.io/ingress-disable-creation` annotation or the cluster ConfigMap), that the spec declares an `engine` or `router` component, and that the InferenceService is not labeled `networking.knative.dev/visibility: cluster-local` — any of these makes OME skip the external Service.

**Changed a cloud-provider annotation but the Service didn't pick it up:** OME rewrites the external Service's annotations only when its spec (selector, ports, or type) also changes. Delete the Service (`kubectl delete service <name>`) — it is owned and watched, so OME recreates it immediately with the current annotations. Expect a brief endpoint change if it is a LoadBalancer.

**`ome.io/service-type` seems ignored:** the value must be exactly `LoadBalancer`, `NodePort`, or `ClusterIP` (case-sensitive); anything else silently falls back to `ClusterIP`.

**The Service exists but has no endpoints:** the selector targets the router pods when `spec.router` is set, otherwise the engine pods. Check the pods are running and labeled to match: `kubectl get pods -l ome.io/inferenceservice=<name>,component=engine` (or `component=router`).

## Next steps

- [Ingress Administration](/ome/docs/administration/ingress/) — the cluster ingress configuration and the full list of per-service override annotations
- [Labels and Annotations reference](/ome/docs/reference/labels-and-annotations/) — all `ome.io/*` annotations
- [Set appProtocol on Generated Services](/ome/docs/tasks/run-workloads/set-service-app-protocols/) — protocol hints on the component Services the external Service fronts
