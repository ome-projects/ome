---
title: "Set appProtocol on Generated Services"
linkTitle: "Service appProtocol"
weight: 20
date: 2026-09-26
description: >
  Learn how to set Kubernetes appProtocol values on the Service ports OME generates, so gateways and service meshes speak the right protocol to your model server.
---

This page shows you how to use the `servicePortAppProtocols` field to set Kubernetes [`appProtocol`](https://kubernetes.io/docs/concepts/services-networking/service/#application-protocol) values on the ports of the Services OME generates for an InferenceService component. Gateway API implementations and service meshes read `appProtocol` to decide which protocol to speak on the hop from the proxy to the backend pod — for example, `kubernetes.io/h2c` tells the gateway to use cleartext HTTP/2, which gRPC needs end-to-end instead of a downgrade to HTTP/1.1.

Each component spec (`spec.engine`, `spec.decoder`, `spec.router`) accepts a `servicePortAppProtocols` map from **generated Service port name** to **appProtocol value**. OME copies the value verbatim onto the matching `ServicePort.appProtocol`; it does not interpret or validate it, so you can use the standard Kubernetes values (`kubernetes.io/h2c`, `kubernetes.io/ws`, `kubernetes.io/wss`) or an implementation-specific value your gateway or mesh understands (such as `grpc`).

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- A running or planned InferenceService — see [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/)

## How OME names Service ports

The generated Service for a component (`<name>-engine`, `<name>-decoder`, or `<name>-router`) mirrors the ports of the **first container** in the component's pod spec — with the catalog runtimes that is the runner container, for example `ome-container` for SGLang engines. For multi-node deployments the Service is built from the leader pod spec. Two cases determine the map key:

- **The container declares ports** — each container port becomes a Service port with the same name, port number, and target port. The map key is the **container port name** (for example `http1` in the SGLang engine runtimes, `http` in the router runtimes).
- **The container declares no ports** — OME generates a single default Service port on port 8080, named after the container. The map key is then the **container name** (for example `ome-container`).

Keys that don't match any port on the first container are silently ignored — there is no error or event — so double-check the spelling against the runtime's declared port names. Ports of sidecar or other containers are not exposed on the Service and cannot be given an `appProtocol`.

## Step 1: Find the port name

Look up the container ports the serving runtime declares for the component:

```bash
kubectl get clusterservingruntime srt-gemma-3-1b-it \
  -o jsonpath='{.spec.engineConfig.runner.ports}'
```

Example output:

```
[{"containerPort":8080,"name":"http1","protocol":"TCP"}]
```

The key you need is `http1`. If the output is empty, the runner declares no ports, so use the container name instead:

```bash
kubectl get clusterservingruntime srt-gemma-3-1b-it \
  -o jsonpath='{.spec.engineConfig.runner.name}'
```

## Step 2: Set servicePortAppProtocols on the component

Add the map to the component spec of your InferenceService:

```bash
kubectl apply -f - <<EOF
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: gemma-3-1b-it
  namespace: gemma-demo
spec:
  model:
    name: gemma-3-1b-it
  runtime:
    name: srt-gemma-3-1b-it
  engine:
    minReplicas: 1
    maxReplicas: 1
    servicePortAppProtocols:
      http1: kubernetes.io/h2c
EOF
```

The same field works on `spec.decoder` and `spec.router`; each component's map only affects that component's own Service.

You can add or change the map on a running InferenceService. The controller updates the ports of the existing Service in place — the Service is not recreated and its ClusterIP is preserved.

## Step 3: Verify the generated Service

```bash
kubectl get service gemma-3-1b-it-engine -n gemma-demo -o jsonpath='{.spec.ports}' | jq
```

Expected output:

```json
[
  {
    "appProtocol": "kubernetes.io/h2c",
    "name": "http1",
    "port": 8080,
    "protocol": "TCP",
    "targetPort": 8080
  }
]
```

Ports without a matching map entry keep an unset `appProtocol`, so you can annotate just the traffic port and leave, say, a metrics port untouched.

## Setting a default in the ServingRuntime

`servicePortAppProtocols` is also part of the runtime component configs (`engineConfig`, `decoderConfig`, `routerConfig` in a ServingRuntime or ClusterServingRuntime), so a runtime author can set it once for every InferenceService that uses the runtime:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: my-grpc-runtime
spec:
  # ...
  engineConfig:
    servicePortAppProtocols:
      grpc1: kubernetes.io/h2c
    runner:
      name: ome-container
      ports:
        - containerPort: 8080
          name: grpc1
          protocol: TCP
      # ...
```

The runtime's map is the base and the InferenceService's map is laid on top: entries the InferenceService sets override same-named runtime entries, and runtime entries it doesn't touch still apply.

## Troubleshooting

**The Service port has no `appProtocol`:** the map key doesn't match. Compare it against the actual generated port names (`kubectl get service <name>-engine -o jsonpath='{.spec.ports[*].name}'`) — remember that a container with no declared ports produces a port named after the container, not `http`.

**The value changed but the Service didn't:** only the InferenceService's component spec and its runtime's component config feed the map. Editing the Service directly is reverted on the next reconcile, because the controller rewrites the Service ports to match the spec.

**The gateway still speaks HTTP/1.1:** `appProtocol` is a hint that the routing implementation must support (in Gateway API terms, backend protocol selection). Check your gateway or mesh documentation for which values it honors.

## Next steps

- [Inference Service concepts](/ome/docs/concepts/inference_service) — the full component field reference
- [Ingress Administration](/ome/docs/administration/ingress/) — configuring the ingress and Gateway API integration that consumes these Services
