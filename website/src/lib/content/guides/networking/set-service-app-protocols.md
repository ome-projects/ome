---
title: Set appProtocol on services
description: Set Kubernetes appProtocol values on the Service ports OME generates, so gateways and service meshes use the right protocol for your model server.
since: v1.3
---

OME creates a Service for each component of an [InferenceService](../../concepts/serving/inference-services.md). This guide sets the Kubernetes [`appProtocol`](https://kubernetes.io/docs/concepts/services-networking/service/#application-protocol) of those Services' ports with the `servicePortAppProtocols` field, so a gateway or service mesh uses the right protocol to reach your model server, such as cleartext HTTP/2 for an engine that serves gRPC. You set the field on a component of the InferenceService, or as a default in its [serving runtime](../../concepts/runtimes/serving-runtimes.md).

<div class="prerequisites" markdown>

- OME installed, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- The [ClusterBaseModel](../../concepts/models/base-models.md) `llama-3-1-8b-instruct`, Ready. It's one of the [pre-configured models](../../getting-started/pre-configured-models.md), and it's gated on Hugging Face, so it needs an access token.
- The ClusterServingRuntime `srt-llama-3-1-8b-instruct-grpc` from the OME repository, whose engine serves gRPC on a port named `grpc1` and whose router serves HTTP. Check the engine's port with `kubectl get clusterservingruntime srt-llama-3-1-8b-instruct-grpc -o jsonpath='{.spec.engineConfig.runner.ports[*].name}'`, which prints `grpc1`. If the runtime is missing, apply [`config/runtimes/srt/meta/llama-3-1-8b-instruct-grpc-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-1-8b-instruct-grpc-rt.yaml) from a clone of the OME repository. The ome-serving chart's runtime of the same name serves HTTP on a port named `http1`, so this guide doesn't work with it.
- A node with an NVIDIA GPU, 10 CPUs and 30 GiB of memory free, which is what the runtime's engine requests, and room for the router's 1 CPU and 2 GiB. The runtime's SGLang image is built for amd64.
- An InferenceService `llama-3-1-8b-instruct` in the namespace `llama-demo` that uses them. To create it, apply the manifest in [Step 2](#step-2-set-serviceportappprotocols) without its `servicePortAppProtocols` lines.
- A gateway or service mesh that reads `appProtocol`. Its documentation lists the values it supports.
- Ingress creation turned on, if a gateway should reach these Services through OME's [Ingress or HTTPRoutes](../../concepts/rollouts-and-traffic/ingress.md). It's off by default. See [Configure ingress](configure-ingress.md).

</div>

## How OME names Service ports

For each component that runs pods, OME creates a Service named after the InferenceService and the component. This guide's InferenceService gets `llama-3-1-8b-instruct-engine` and `llama-3-1-8b-instruct-router`. A decoder gets `llama-3-1-8b-instruct-decoder`.

Each Service takes its ports from the first container of the component's pods:

- With the catalog runtimes, that's the runtime's runner: `ome-container` for engines and decoders, and `router` for routers.
- If a component lists containers of its own, the Service takes the ports of the first one listed. OME adds the runner after the listed containers, unless one of them has the runner's name.
- For a component that runs a leader and workers, the Service selects the leader pod, and takes the ports of the leader's first container.

What the first container declares sets the Service's ports and the keys of `servicePortAppProtocols`:

| The first container | The Service's ports | The key |
| --- | --- | --- |
| Declares ports | One for each container port, with the same name, number and protocol, or TCP when the container port sets none. The port and the target port are both the container port's number. | The port's name, such as `grpc1` |
| Declares no ports | One TCP port, 8080, named after the container | The container's name, such as `ome-container` |

Most of the catalog's SGLang runtimes name the engine's port `http1`, or `grpc1` when the engine serves gRPC, and the router's port `http`. [Step 1](#step-1-find-the-port-name) reads the names from the Services.

The value is copied onto the port's `appProtocol` as written. Kubernetes defines these values:

- `kubernetes.io/h2c`: HTTP/2 over cleartext, with prior knowledge. gRPC without TLS uses it.
- `kubernetes.io/ws`: WebSocket over cleartext.
- `kubernetes.io/wss`: WebSocket over TLS.

A value can also be an IANA service name, such as `http`, or a name with a domain prefix that your gateway or mesh defines. It must follow the syntax of a Kubernetes label key. OME doesn't check the value, and it ignores a key that matches no port, without an error, an event or a condition.

!!! note "The Service named after the InferenceService"
    While ingress creation is off, which it is by default, OME also creates a Service named after the InferenceService, `llama-3-1-8b-instruct`, unless the InferenceService has the label `networking.knative.dev/visibility: cluster-local`. It sends traffic to the router's pods, or to the engine's when there's no router, on one port named `http`. OME never sets an `appProtocol` on it: `servicePortAppProtocols` changes only the component Services.

## Step 1: Find the port name

Read the port names from the component Services:

```bash
kubectl get service llama-3-1-8b-instruct-engine llama-3-1-8b-instruct-router -n llama-demo \
  -o 'custom-columns=SERVICE:.metadata.name,PORTS:.spec.ports[*].name'
```

```output
SERVICE                        PORTS
llama-3-1-8b-instruct-engine   grpc1
llama-3-1-8b-instruct-router   http
```

The engine's port is `grpc1` and the router's is `http`, as the runtime declares them. For a component whose first container declares no ports, this shows the container's name, such as `ome-container`, and that name is the key.

## Step 2: Set servicePortAppProtocols

Add `servicePortAppProtocols` to the engine, with the port's name as the key and the protocol as the value. The engine serves gRPC without TLS, so this manifest marks its `grpc1` port as `kubernetes.io/h2c`:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-1-8b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-1-8b-instruct
  runtime:
    name: srt-llama-3-1-8b-instruct-grpc
  engine:
    minReplicas: 1
    maxReplicas: 1
    servicePortAppProtocols:
      grpc1: kubernetes.io/h2c
  router:
    minReplicas: 1
    maxReplicas: 1
```

The InferenceService uses a ClusterBaseModel, the default kind, so it doesn't set `spec.model.kind`. It names the runtime, which sets `autoSelect: false`, so OME never picks it on its own.

`spec.engine`, `spec.decoder` and `spec.router` each take their own `servicePortAppProtocols`, and each map changes only that component's Service. The field is part of [`ComponentExtensionSpec`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-ComponentExtensionSpec).

You can change the map on a running InferenceService. OME updates the ports of the existing Service in place, so the Service keeps its ClusterIP. When you remove a key, OME clears that port's `appProtocol`, unless the runtime sets one for the port.

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-1-8b-instruct configured
```

If the runtime doesn't declare support for your copy of the model, kubectl also prints the admission webhook's warning. The webhook admits the change anyway, because the InferenceService names its runtime. See [Reference a runtime explicitly](../deploy-models/reference-a-runtime-explicitly.md).

## Step 3: Check the Service

Print each port of the engine's Service with its `appProtocol`:

```bash
kubectl get service llama-3-1-8b-instruct-engine -n llama-demo \
  -o jsonpath='{range .spec.ports[*]}{.name}{" "}{.appProtocol}{"\n"}{end}'
```

```output
grpc1 kubernetes.io/h2c
```

A port without an `appProtocol` prints its name alone. The router's Service doesn't change: the router has no map, so its `http` port has no `appProtocol`.

## Set a default in the ServingRuntime

A ServingRuntime or ClusterServingRuntime takes the same field in `engineConfig`, `decoderConfig` and `routerConfig`, so a runtime author can set the protocol once for every InferenceService that uses the runtime. This part of a runtime marks the engine's `grpc1` port:

```yaml
spec:
  engineConfig:
    servicePortAppProtocols:
      grpc1: kubernetes.io/h2c
    runner:
      name: ome-container
      ports:
        - containerPort: 8080
          name: grpc1
          protocol: TCP
```

The runtime's map and the InferenceService's map combine key by key:

- The runtime's map is the base. A key that both set takes the InferenceService's value, and runtime keys that the InferenceService doesn't set still apply.
- To leave a port unset that the runtime marks, set its key to `""` in the InferenceService.
- A runtime's map applies only to components that the InferenceService has. For example, `routerConfig.servicePortAppProtocols` applies only when the InferenceService has a `spec.router` section, since OME creates no router without one.

## Troubleshooting

### The port has no `appProtocol`

The key doesn't match a port of the component's Service, or its value is empty. OME ignores a key that matches no port without an error, an event or a condition, so compare the key with what [Step 1](#step-1-find-the-port-name) prints:

- When the first container declares no ports, the key is the container's name, such as `ome-container`, not `http`.
- The map must be on the component whose Service has the port: `spec.engine` for `llama-3-1-8b-instruct-engine`, and `spec.router` for `llama-3-1-8b-instruct-router`.
- Only the first container's ports are on the Service, so the ports of other containers can't get an `appProtocol`.
- A key set to `""` in the InferenceService leaves the port unset, even when the runtime sets it.

### The Service keeps its old ports

OME owns the ports and selector of the component Services, and sets them from the InferenceService and its runtime. When you edit a Service's ports directly, OME puts them back. Set the protocol in the InferenceService or the runtime instead.

If the ports don't change after you apply the InferenceService, look for `Failed to update Service` in the OME manager's log. Only the leader among the manager's replicas reconciles, so read the logs of every replica. This command assumes OME runs in the namespace `ome`:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep -E 'Failed to (create|update) Service'
```

Each matching line names the Service and carries the API server's error. OME records no event for it. The API server rejects an `appProtocol` that doesn't follow label key syntax, such as one with a space, so the Service keeps its old ports, and OME retries. For a component that has no Service yet, the log says `Failed to create Service`, and the Service isn't created.

Until you fix the value in the InferenceService or the runtime, each reconcile stops at that Service. OME reconciles the engine, then the decoder, then the router, so it doesn't create or update the components after the failing one, or the Ingress, HTTPRoutes and Service named after the InferenceService.

### The gateway still uses HTTP/1.1

`appProtocol` is a hint, and each gateway and mesh supports its own set of values. Check that yours supports the value you set.

Also check that the traffic goes through the Service you marked:

- When ingress creation is on, OME's Ingress rules and HTTPRoutes send the InferenceService's traffic to the router's Service, or to the engine's when there's no router, and give each component a route to its own Service. They always use the Service's first port.
- The Service named after the InferenceService, which OME creates while ingress creation is off, never has an `appProtocol`.

## Next steps

- [Configure ingress](configure-ingress.md): turn on ingress creation, so a gateway reaches these Services through OME's Ingress or HTTPRoutes.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): what a runtime's component configs set, and how an InferenceService builds on them.
