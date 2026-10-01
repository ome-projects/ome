---
title: Run a standalone InferenceReplica
description: Run, scale and update a CPU HTTP workload directly, then use a ServingRuntime as an alternative to inline pod templates.
since: v1.3
---

Create an [InferenceReplica](../../concepts/omenative/overview.md#standalone-inferencereplicas) without an InferenceService. You own its spec; OME manages its Instances, pods and revision history. This is useful when you need one independently managed pod set, without InferenceService traffic routing or coordinated engine/decoder rollouts.

The workload returns a fixed HTTP response. It needs no model, GPU, model agent or model storage. For the InferenceService-based version of this exercise, start with [Learn OMENative](learn-omenative.md).

<div class="prerequisites" markdown>

- A disposable OME source installation matching public commit `bc1f94db` on the v1.3 development line. Use the manager-only profile in [Install from source](../../getting-started/install.md#install-from-source), with matching CRDs and enabled admission webhooks. v1.2.2 does not support this guide.
- `kubectl`, `curl` and an explicitly selected kubeconfig for that environment, not a production context.
- Permission to create a namespace, InferenceReplicas, a Service and a ServingRuntime; read pods and revisions; update the replica's scale subresource; and port-forward pods.
- Linux workers that can pull and run `docker.io/hashicorp/http-echo:1.0.0`, with room for three small pods. Each pod requests 25 millicores and 32 MiB; a rolling update temporarily adds one pod to the two steady-state Instances.

</div>

Run the commands from the matching OME checkout. All files below live in `config/samples/docs/standalone-http/`. They are complete examples; do not apply the whole directory at once, because the runtime-reference form is an optional second workload.

## Step 1: Create an inline-template replica

Create the lab namespace:

```yaml title="namespace.yaml"
apiVersion: v1
kind: Namespace
metadata:
  name: ome-standalone-lab
```

```bash
kubectl apply -f config/samples/docs/standalone-http/namespace.yaml
```

This replica starts one Instance containing one pod. A single-pod Instance uses a runner named `default` with `size: 1`; the container inside that pod has its own name.

```yaml title="inferencereplica.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceReplica
metadata:
  name: standalone-http
  namespace: ome-standalone-lab
spec:
  component: engine
  replicas: 1
  minReadySeconds: 5
  lifecycle:
    instanceReadyTimeout: 2m
    updateStrategy:
      type: SurgeThenDrain
      rollingUpdate:
        maxSurge: 1
        maxUnavailable: 0
  runners:
    - name: default
      size: 1
      template:
        metadata:
          labels:
            app.kubernetes.io/name: standalone-http
        spec:
          terminationGracePeriodSeconds: 10
          containers:
            - name: ome-container
              image: docker.io/hashicorp/http-echo:1.0.0
              args:
                - -listen=:8080
                - -text=hello from standalone v1
              ports:
                - name: http
                  containerPort: 8080
              readinessProbe:
                httpGet:
                  path: /
                  port: http
                periodSeconds: 2
              resources:
                requests:
                  cpu: 25m
                  memory: 32Mi
                limits:
                  cpu: 100m
                  memory: 64Mi
```

There is no `parentRef` or InferenceService owner reference. Keep `component: engine` unchanged; the component is immutable. On an InferenceReplica, put the availability delay in **`spec.minReadySeconds`**, not inside `lifecycle`.

The readiness probe checks the HTTP server. Five continuous ready seconds make its Instance available for rollout pacing. The explicit two-minute readiness window excludes admission gates and external holds; see [Readiness deadlines](set-instance-readiness-deadlines.md).

```bash
kubectl apply -f config/samples/docs/standalone-http/inferencereplica.yaml
kubectl wait inferencereplica/standalone-http -n ome-standalone-lab --for=condition=Ready --timeout=5m
kubectl wait inferencereplica/standalone-http -n ome-standalone-lab --for=jsonpath='{.status.availableReplicas}'=1 --timeout=5m
kubectl get inferencereplica standalone-http -n ome-standalone-lab
kubectl get pods -n ome-standalone-lab -l app.kubernetes.io/name=standalone-http
```

Expect desired, ready and available counts of `1`, and one running pod. OME creates a ControllerRevision and a headless Service for the pod set; it does not create an InferenceService or its traffic endpoint.

## Step 2: Provide an HTTP endpoint

Create your own Service using the label in the pod template:

```yaml title="service.yaml"
apiVersion: v1
kind: Service
metadata:
  name: standalone-http
  namespace: ome-standalone-lab
spec:
  selector:
    app.kubernetes.io/name: standalone-http
  ports:
    - name: http
      port: 8080
      targetPort: http
```

```bash
kubectl apply -f config/samples/docs/standalone-http/service.yaml
kubectl port-forward service/standalone-http -n ome-standalone-lab 18080:8080
```

Leave the port-forward running. In a second terminal:

```bash
curl --fail --silent --show-error http://127.0.0.1:18080/
```

The configured response is `hello from standalone v1`. A port-forward selects one pod; repeated requests do not demonstrate Service load balancing. This Service belongs to you and is not garbage-collected when you delete the replica.

## Step 3: Scale and update

Unlike a projected replica, this is the resource you edit directly. Scale it to two Instances:

```bash
kubectl scale inferencereplica standalone-http -n ome-standalone-lab --replicas=2
kubectl wait inferencereplica/standalone-http -n ome-standalone-lab --for=jsonpath='{.status.availableReplicas}'=2 --timeout=5m
```

Each Instance still has one pod. An omitted count or `replicas: 0` runs **one** Instance; standalone replicas do not support scale-to-zero. Delete the replica when you want no workload.

Before changing the template, record its current target revision:

```bash
kubectl get inferencereplica standalone-http -n ome-standalone-lab \
  -o jsonpath='{.status.updateRevision}{"\n"}'
```

The checked-in JSON patch changes the response argument in this example's single container:

```json title="update-response.json"
[
  {
    "op": "replace",
    "path": "/spec/runners/0/template/spec/containers/0/args/1",
    "value": "-text=hello from standalone v2"
  }
]
```

```bash
kubectl patch inferencereplica standalone-http -n ome-standalone-lab --type=json \
  --patch-file=config/samples/docs/standalone-http/update-response.json
kubectl get inferencereplica standalone-http -n ome-standalone-lab --watch \
  -o custom-columns='GENERATION:.metadata.generation,OBSERVED:.status.observedGeneration,TARGET:.status.updateRevision,CURRENT:.status.currentRevision,READY:.status.conditions[?(@.type=="Ready")].status,UPDATED-READY:.status.updatedReadyReplicas,AVAILABLE:.status.availableReplicas'
```

Wait until `TARGET` differs from the old revision, `CURRENT` equals `TARGET`, `OBSERVED` equals `GENERATION`, `READY` is `True`, and both counts are `2`. Press Ctrl-C to stop watching. Checking only the old Ready condition can mistake an earlier successful rollout for the new one.

`SurgeThenDrain` allows one extra pod and waits for its replacement to become available before draining the old pod. Keep capacity for three pods during this update. Restart the port-forward from Step 2 after the rollout, then repeat the request; the configured response is now `hello from standalone v2`.

These commands changed the live resource, not the YAML file. For a workload you keep, record the new replica count and arguments in your source manifest.

## Alternative: Render from a runtime

Use `runtimeRef` instead of inline `runners` when several replicas should reuse a runtime. The runtime must declare the replica's component: this `engine` replica needs `engineConfig`. A model reference is optional; this CPU server uses none.

Create the runtime in the same namespace:

```yaml title="servingruntime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: http-echo
  namespace: ome-standalone-lab
spec:
  engineConfig:
    terminationGracePeriodSeconds: 10
    runner:
      name: ome-container
      image: docker.io/hashicorp/http-echo:1.0.0
      args:
        - -listen=:8080
        - -text=hello from runtime reference
      ports:
        - name: http
          containerPort: 8080
      readinessProbe:
        httpGet:
          path: /
          port: http
        periodSeconds: 2
      resources:
        requests:
          cpu: 25m
          memory: 32Mi
        limits:
          cpu: 100m
          memory: 64Mi
```

Create a second, independent replica. Do not copy the first replica's `runners` into it: references and inline templates are mutually exclusive.

```yaml title="runtime-ref-replica.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceReplica
metadata:
  name: standalone-http-ref
  namespace: ome-standalone-lab
spec:
  component: engine
  replicas: 1
  minReadySeconds: 5
  runtimeRef:
    apiGroup: ome.io
    kind: ServingRuntime
    name: http-echo
  lifecycle:
    instanceReadyTimeout: 2m
    updateStrategy:
      type: SurgeThenDrain
      rollingUpdate:
        maxSurge: 1
        maxUnavailable: 0
```

```bash
kubectl apply -f config/samples/docs/standalone-http/servingruntime.yaml
kubectl apply -f config/samples/docs/standalone-http/runtime-ref-replica.yaml
kubectl wait inferencereplica/standalone-http-ref -n ome-standalone-lab --for=condition=Ready --timeout=5m
```

The first example's Service selects only its inline-template pods. For this alternative, use the replica's published selector to choose a pod and forward directly:

```bash
OME_HTTP_REF_SELECTOR="$(kubectl get inferencereplica standalone-http-ref -n ome-standalone-lab -o jsonpath='{.status.labelSelector}')"
OME_HTTP_REF_POD="$(kubectl get pods -n ome-standalone-lab -l "$OME_HTTP_REF_SELECTOR" -o jsonpath='{.items[0].metadata.name}')"
kubectl port-forward "pod/$OME_HTTP_REF_POD" -n ome-standalone-lab 18081:8080
```

In another terminal:

```bash
curl --fail --silent --show-error http://127.0.0.1:18081/
```

The configured response is `hello from runtime reference`. There is still no model resource or model download.

This form follows the **live** runtime. Changing its pod template causes a new replica revision; `runtimeRef.autoSync: false` and a pinned `runtimeRef.revision` are rejected. A runtime edit need not change the replica's own generation, so watch its target revision and rollout readiness as well as `observedGeneration`. OME renders the runners in memory; it does not add them to `spec.runners`.

## Boundaries and troubleshooting

- Keep standalone names distinct from InferenceServices and their projected replicas in the namespace. Do not set `parentRef`, an InferenceService owner reference or controller-only placement fields. An InferenceService cannot adopt this example as its engine through a user-authored reference.
- OME does not create an HPA or KEDA scaler for a standalone replica. Those classes in `spec.autoscaler` are rejected; create your own scaler targeting the replica's scale subresource if needed. This lab has no autoscaler.
- Manual Instance migration and InferenceService rollout groups are not standalone workflows. Commands in `kubectl ome` that take an InferenceService name do not automatically accept this resource.
- Read standalone conditions and events with `kubectl describe inferencereplica standalone-http -n ome-standalone-lab`. If pods stay Pending, inspect their scheduling events, quota and admission gates. For image or HTTP probe failures, inspect the pod's events and `ome-container` logs before changing timeouts.
- If a reference cannot render, the replica reports the reason in its Ready condition, such as `RuntimeNotFound` or `RuntimePieceMissing`. Creating a missing runtime, or restoring its `engineConfig`, allows rendering to resume. Admission success alone does not prove the runtime exists.

For a failed template update, correct the template and watch the resulting new revision. [Reset failed Instances](reset-failed-instances.md) describes the InferenceReplica reset annotation; do not substitute an InferenceService-targeted CLI command for a standalone resource.

## Clean up

Stop both port-forwards. Remove the user-managed Service and delete the replicas, waiting for OMENative's teardown:

```bash
kubectl delete -f config/samples/docs/standalone-http/service.yaml
kubectl delete -f config/samples/docs/standalone-http/inferencereplica.yaml --wait=true --timeout=5m
kubectl delete -f config/samples/docs/standalone-http/runtime-ref-replica.yaml --ignore-not-found=true --wait=true --timeout=5m
kubectl delete -f config/samples/docs/standalone-http/servingruntime.yaml --ignore-not-found=true
kubectl get pods -n ome-standalone-lab
```

There should be no lab pods left. If teardown stalls or pods remain, inspect their finalizers and events and follow [Recover stuck deletions](recover-stuck-deletions.md); do not remove finalizers just to make the command finish.

Delete the namespace only if it contains nothing else you need:

```bash
kubectl delete -f config/samples/docs/standalone-http/namespace.yaml --wait=true --timeout=5m
```

For coordinated components and OME-managed traffic, continue with [Learn OMENative through an InferenceService](learn-omenative.md). The [standalone overview](../../concepts/omenative/overview.md#standalone-inferencereplicas) summarizes the ownership and template-source contract.
