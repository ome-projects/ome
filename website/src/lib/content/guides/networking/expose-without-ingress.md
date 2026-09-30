---
title: Expose a service without ingress
description: Put an InferenceService behind a LoadBalancer or NodePort Service with the ome.io/service-type annotation, without an Ingress controller or a Gateway.
---

The `ome.io/service-type` annotation puts an [InferenceService](../../concepts/serving/inference-services.md) behind a `LoadBalancer` or `NodePort` Service, so clients outside the cluster reach the model without an Ingress controller or a Gateway. Use `LoadBalancer` on a cluster that provisions load balancers, and `NodePort` when clients can reach the nodes. To route traffic through an Ingress or a Gateway instead, see [Configure ingress](configure-ingress.md).

<div class="prerequisites" markdown>

- OME installed as the release `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- The BaseModel `llama-3-2-1b-instruct` in the namespace `llama-demo`, and the ClusterServingRuntime `srt-llama-3-2-1b-instruct`, as in [Serve models from a PVC](../deploy-models/serve-models-from-pvc.md) up to its Step 3. To use an InferenceService that you already run, add the annotations from Step 2 to it, and change the names in the commands.
- For a `LoadBalancer` Service, a cluster that provisions load balancers, and a machine that can reach them. Step 2's example is for Azure Kubernetes Service.
- For a `NodePort` Service, a machine that can reach the nodes.

</div>

## How the external Service works

While ingress creation is off, OME puts a `ClusterIP` Service named after the InferenceService in front of its router, or its engine when there's no router. This is the external Service. It listens on the port of the router's or the engine's Service, 8080 for `srt-llama-3-2-1b-instruct`. Whatever its type, `status.url` stays its address in the cluster, here `http://llama-3-2-1b-instruct.llama-demo.svc.cluster.local:8080`. For the details, see [The external Service](../../concepts/rollouts-and-traffic/ingress.md#the-external-service).

!!! warning "Give your own Services other names"
    OME treats any Service with the InferenceService's name as the external Service, even one you created. It rewrites that Service, and deletes it when you turn ingress creation on.

## Step 1: Check that ingress creation is off

Ingress creation is off by default. Check the cluster's setting:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.ingress}' | jq '.disableIngressCreation'
```

```output
true
```

If it prints `false`, ingress creation is on. To turn it off for this InferenceService, add `ome.io/ingress-disable-creation: "true"` to its annotations in Step 2. To turn it off for every InferenceService, see [Turn off ingress creation](configure-ingress.md#turn-off-ingress-creation). Either way, the Ingress or HTTPRoutes that OME created before keep serving traffic until you delete them, as that section shows.

## Step 2: Set the Service type and load balancer annotations

Save this version of the InferenceService from [Serve models from a PVC](../deploy-models/serve-models-from-pvc.md#step-4-deploy-an-inferenceservice), with annotations for a load balancer inside the cluster's network:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
  annotations:
    ome.io/service-type: LoadBalancer
    service.beta.kubernetes.io/azure-load-balancer-internal: "true"
spec:
  model:
    name: llama-3-2-1b-instruct
    kind: BaseModel
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
    annotations:
      ome.io/service-type: ClusterIP
```

- `ome.io/service-type: LoadBalancer` makes the external Service a `LoadBalancer` Service. For a `NodePort` Service, use `NodePort`. The value is case-sensitive.
- `service.beta.kubernetes.io/azure-load-balancer-internal: "true"` asks Azure for a load balancer with an address in the cluster's virtual network. Replace it with your provider's annotations. OME copies only annotations that start with `service.beta.kubernetes.io/`, `cloud.google.com/` or `service.kubernetes.io/`.
- `ome.io/service-type: ClusterIP` in the engine's `annotations` keeps the engine's Service, `llama-3-2-1b-instruct-engine`, inside the cluster. Without it, that Service takes the InferenceService's type when OME creates it, and becomes a second load balancer. Add the same line to `spec.router.annotations` and `spec.decoder.annotations` when the InferenceService has those components.

!!! warning "A load balancer exposes the model to anyone who can reach it"
    Nothing authenticates the requests sent to the Service. Without an annotation that keeps the load balancer internal, your provider can give it a public address. Keep it internal, or put an authenticating gateway in front of the model. See [Security](../../concepts/rollouts-and-traffic/ingress.md#security).

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

If the InferenceService already exists, kubectl prints `configured` instead. OME copies these annotations to the engine's pods too, so a running `RawDeployment` engine replaces its pods, which load the model again.

## Step 3: Check the Service and send a request

Wait for the InferenceService to be ready:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

List the Services that OME created for it:

```bash
kubectl get service -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct \
  -o 'custom-columns=NAME:.metadata.name,COMPONENT:.metadata.labels.component,TYPE:.spec.type,PORT:.spec.ports[*].port'
```

```output
NAME                           COMPONENT          TYPE           PORT
llama-3-2-1b-instruct          external-service   LoadBalancer   8080
llama-3-2-1b-instruct-engine   engine             ClusterIP      8080
```

The external Service has the type you set, and the engine's Service stays `ClusterIP`. An [OMENative](../../concepts/omenative/overview.md) engine also has a headless Service and two Services for each revision, which are always `ClusterIP`.

Then send the model a request from outside the cluster:

=== "LoadBalancer"

    Read the load balancer's address. Depending on your provider, it's an IP address or a hostname:

    ```bash
    kubectl get service llama-3-2-1b-instruct -n llama-demo \
      -o jsonpath='{.status.loadBalancer.ingress[0].ip}{.status.loadBalancer.ingress[0].hostname}{"\n"}'
    ```

    ```output
    10.224.0.100
    ```

    The InferenceService can be `Ready` before the load balancer exists, and until then the command prints an empty line. If it stays empty, look for your provider's events in `kubectl describe service llama-3-2-1b-instruct -n llama-demo`.

    From a machine that can reach the address, send a chat completion request to port 8080, with your address in place of `10.224.0.100`:

    ```bash
    curl -s http://10.224.0.100:8080/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is a load balancer?"}], "max_tokens": 64}' \
      | jq -r '.choices[0].message.content'
    ```

    `jq` prints the model's answer, which varies from run to run.

=== "NodePort"

    Read the port that Kubernetes opened on every node:

    ```bash
    kubectl get service llama-3-2-1b-instruct -n llama-demo -o jsonpath='{.spec.ports[0].nodePort}{"\n"}'
    ```

    ```output
    31627
    ```

    From a machine that can reach the nodes, send a chat completion request to that port on any node's address. `kubectl get nodes -o wide` lists the addresses. Replace `10.224.0.4` and `31627` with yours:

    ```bash
    curl -s http://10.224.0.4:31627/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is a node port?"}], "max_tokens": 64}' \
      | jq -r '.choices[0].message.content'
    ```

    `jq` prints the model's answer, which varies from run to run.

## Expose a multi-node engine

Without a router, the external Service sends traffic to every pod of the engine, workers included, so it suits an engine whose replicas are one pod each. For a [multi-node engine](../omenative/serve-a-multi-node-model.md), give the InferenceService a router, or make the engine's Service the load balancer:

- Set `ome.io/service-type: LoadBalancer` and your provider's annotations in `spec.engine.annotations`, and leave `ome.io/service-type` off the InferenceService's `metadata.annotations`.
- Declare the engine's `leader` and `worker` in the InferenceService, so that the engine's Service sends traffic only to the leaders. If the runtime already sets them, add `leader: {}` and `worker: {}`, which keep the runtime's settings.
- Set the annotations before OME creates the engine's Service. Since v1.3, OME sets a component Service's type and annotations only when it creates the Service.

## Troubleshooting

Start with the InferenceService's `IngressReady` condition:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o 'custom-columns=READY:.status.conditions[?(@.type=="IngressReady")].status,REASON:.status.conditions[?(@.type=="IngressReady")].reason,MESSAGE:.status.conditions[?(@.type=="IngressReady")].message'
```

```output
READY   REASON            MESSAGE
True    IngressDisabled   Ingress creation is disabled, using external service for access
```

If the reason isn't `IngressDisabled`, ingress creation is on for the InferenceService, and OME creates routes instead of the external Service: see [Step 1](#step-1-check-that-ingress-creation-is-off). For other problems, see [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md).

### OME doesn't create the Service

The InferenceService has the label `networking.knative.dev/visibility: cluster-local`, which stops OME from creating the external Service and makes OME delete an existing one. Remove the label to expose the InferenceService.

For other errors, search the controller's logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'external service'
```

### The Service stays a ClusterIP Service

- The value of `ome.io/service-type` isn't exactly `LoadBalancer` or `NodePort`. The webhook accepts any value, and OME treats the others, such as `loadbalancer`, as `ClusterIP`.
- The annotation is in a component's `annotations`. The external Service reads only the InferenceService's `metadata.annotations`.

### An annotation or setting is missing from the Service

OME copies only annotations with the prefixes that Step 2 lists, so GKE's `networking.gke.io/load-balancer-type` never reaches the external Service. OME also resets any change that you make to a `LoadBalancer` or `NodePort` external Service, such as setting `externalTrafficPolicy`. For these settings, create a Service of your own, with another name and the selector of the engine's Service, or of the router's when there's one. Read the selector:

```bash
kubectl get service llama-3-2-1b-instruct-engine -n llama-demo -o jsonpath='{.spec.selector}'
```

```output
{"app":"llama-3-2-1b-instruct-engine"}
```

The output is for a `RawDeployment` engine. An OMENative engine's Service has a different selector.

### The engine's Service is a load balancer too

The InferenceService had `ome.io/service-type: LoadBalancer` when OME created the engine's Service. Add `ome.io/service-type: ClusterIP` to the engine's `annotations` as in Step 2, apply the file, and delete the engine's Service. OME creates it again as a `ClusterIP` Service:

```bash
kubectl delete service llama-3-2-1b-instruct-engine -n llama-demo
```

```output
service "llama-3-2-1b-instruct-engine" deleted from llama-demo namespace
```

### Requests to a multi-node engine fail

Without a router, the external Service sends requests to the worker pods too. See [Expose a multi-node engine](#expose-a-multi-node-engine).

## Clean up

Delete the InferenceService. The Services that OME created for it go with it, and your provider removes the load balancer:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
```

To keep the InferenceService without the load balancer, remove `ome.io/service-type` from `metadata.annotations` in `isvc.yaml`, and apply the file again. OME changes the external Service back to a `ClusterIP` Service. Leave the other annotations in place, since changing them restarts a `RawDeployment` engine.

## Next steps

- [Configure ingress](configure-ingress.md): route traffic through a Kubernetes Ingress or Gateway API HTTPRoutes instead, at hostnames under your domain.
- [Ingress and external access](../../concepts/rollouts-and-traffic/ingress.md): how the external Service, the routes and the component Services fit together.
- [Traffic annotations](../../reference/api/traffic-annotations.md#service-type-annotation): the reference for `ome.io/service-type`, and the component Services it also reaches.
