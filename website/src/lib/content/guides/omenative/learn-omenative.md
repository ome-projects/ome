---
title: Learn OMENative with an HTTP workload
description: Create a small CPU workload, inspect its Instances, scale it and roll out a new response before introducing model weights or GPUs.
since: v1.3
---

Run a small HTTP server to learn how [OMENative](../../concepts/omenative/overview.md) manages a workload. You create a [ServingRuntime](../../concepts/runtimes/serving-runtimes.md) and an [InferenceService](../../concepts/serving/inference-services.md), then inspect, scale and update the Instances OME creates. The server returns a fixed string; it does not perform model inference.

This runtime-only path needs no BaseModel, model agent, model storage or AcceleratorClass. After the lab, move to [Serve your first model](../../getting-started/serve-your-first-model.md) for a real model server.

<div class="prerequisites" markdown>

- A disposable Kubernetes environment with OME built from the v1.3 development source, including matching CRDs and enabled admission webhooks. Follow [Install from source](../../getting-started/install.md#install-from-source); the v1.2.2 release cannot run this lab.
- `kubectl` and `curl`, and a kubeconfig for that environment. Select that kubeconfig explicitly in your shell; do not use a production context.
- Permission to create a namespace, ServingRuntime and InferenceService, inspect their pods and revisions, and forward a pod port.
- Linux workers that can pull `docker.io/hashicorp/http-echo:1.0.0`. Verify that this versioned image supports your worker architecture and can run in your environment.
- Room for four small pods during the update: each requests 25 millicores and 32 MiB of memory, with limits of 100 millicores and 64 MiB.

</div>

The example files are in `config/samples/docs/omenative-http` in the OME repository. Run the commands from a checkout of the same source used to build OME. The YAML below matches those files. Command output is illustrative; hashes and generation numbers depend on your installation.

## Step 1: Create the namespace and runtime

The namespace keeps the lab's resources together:

```yaml title="namespace.yaml"
apiVersion: v1
kind: Namespace
metadata:
  name: ome-http-lab
```

The runtime supplies the server image, its arguments and its HTTP readiness probe. It omits model matching rules because the InferenceService names it explicitly.

```yaml title="servingruntime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: http-echo
  namespace: ome-http-lab
spec:
  engineConfig:
    terminationGracePeriodSeconds: 10
    runner:
      name: ome-container
      image: docker.io/hashicorp/http-echo:1.0.0
      args:
        - -listen=:8080
        - -text=hello from OMENative v1
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

Apply them in order:

```bash
kubectl apply -f config/samples/docs/omenative-http/namespace.yaml
kubectl apply -f config/samples/docs/omenative-http/servingruntime.yaml
```

```output
namespace/ome-http-lab created
servingruntime.ome.io/http-echo created
```

## Step 2: Create two Instances

The InferenceService selects that namespaced runtime and opts into OMENative. `autoscaler.class: None` makes the declared replica count authoritative, so the lab needs neither HPA nor KEDA.

```yaml title="inferenceservice.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: http-lab
  namespace: ome-http-lab
spec:
  deploymentMode: OMENative
  runtime:
    apiGroup: ome.io
    kind: ServingRuntime
    name: http-echo
  engine:
    minReplicas: 2
    maxReplicas: 2
    autoscaler:
      class: None
    lifecycle:
      minReadySeconds: 5
      instanceReadyTimeout: 2m
      updateStrategy:
        type: SurgeThenDrain
        rollingUpdate:
          maxSurge: 1
          maxUnavailable: 0
```

Each Instance has one pod. The probe makes the pod ready; after five continuous ready seconds it becomes [available](../../concepts/omenative/instances.md#readiness-and-availability) for rollout pacing. The two-minute readiness deadline counts active waiting; admission and operator holds can pause that clock. See [Set Instance readiness deadlines](set-instance-readiness-deadlines.md) for the exclusions.

```bash
kubectl apply -f config/samples/docs/omenative-http/inferenceservice.yaml
kubectl wait inferenceservice/http-lab -n ome-http-lab --for=condition=Ready --timeout=5m
kubectl wait inferencereplica/http-lab-engine -n ome-http-lab --for=jsonpath='{.status.availableReplicas}'=2 --timeout=5m
```

```output
inferenceservice.ome.io/http-lab created
inferenceservice.ome.io/http-lab condition met
inferencereplica.ome.io/http-lab-engine condition met
```

Inspect the generated InferenceReplica:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o custom-columns='NAME:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas,AVAILABLE:.status.availableReplicas'
```

```output
NAME              DESIRED   READY   AVAILABLE
http-lab-engine   2         2       2
```

OME owns this replica's spec. Make changes to the InferenceService; the [standalone form](../../concepts/omenative/overview.md#standalone-inferencereplicas) is a separate way to author a replica directly.

To practice that ownership model, [run a standalone replica](run-a-standalone-replica.md) after this lab. It uses the same HTTP image but has no InferenceService or OME-managed traffic endpoint.

## Step 3: Send an HTTP request

Forward the engine Service's port in one terminal:

```bash
kubectl port-forward service/http-lab-engine -n ome-http-lab 8080:8080
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

Leave it running. In another terminal, send a request:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/
```

```output
hello from OMENative v1
```

A port-forward selects one pod behind the Service; repeated requests through it do not test load balancing across all Instances.

## Step 4: Scale to three Instances

Change the InferenceService's replica bounds together:

```bash
kubectl patch inferenceservice http-lab -n ome-http-lab --type=merge \
  -p '{"spec":{"engine":{"minReplicas":3,"maxReplicas":3}}}'
kubectl wait inferencereplica/http-lab-engine -n ome-http-lab --for=jsonpath='{.status.availableReplicas}'=3 --timeout=5m
```

```output
inferenceservice.ome.io/http-lab patched
inferencereplica.ome.io/http-lab-engine condition met
```

There are now three Instances, each with one pod. You changed the desired count, not the pod template.

## Step 5: Roll out a new response

First note the current target revision:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab \
  -o jsonpath='{.status.updateRevision}{"\n"}'
```

```output
http-lab-engine-<old-hash>
```

Override the runner's arguments on this InferenceService. Keep the same runner name as the runtime and supply the complete argument list:

```bash
kubectl patch inferenceservice http-lab -n ome-http-lab --type=merge \
  -p '{"spec":{"engine":{"runner":{"name":"ome-container","args":["-listen=:8080","-text=hello from OMENative v2"]}}}}'
```

```output
inferenceservice.ome.io/http-lab patched
```

The changed pod template creates a new revision. `SurgeThenDrain` starts a replacement before removing its old pod. The budget permits one extra pod at a time, and the replacement must be available before the old pod drains. Leave room for that fourth pod.

Watch the replica until `TARGET` differs from the old hash, `OBSERVED` equals `GENERATION`, `READY` is `True`, and both `UPDATED-READY` and `AVAILABLE` are `3`. The replica's Ready condition checks that the rollout has converged, including promotion to the target revision. Press Ctrl-C to stop watching:

```bash
kubectl get inferencereplica http-lab-engine -n ome-http-lab --watch \
  -o custom-columns='GENERATION:.metadata.generation,OBSERVED:.status.observedGeneration,TARGET:.status.updateRevision,READY:.status.conditions[?(@.type=="Ready")].status,UPDATED-READY:.status.updatedReadyReplicas,AVAILABLE:.status.availableReplicas'
```

```output title="Illustrative completed rollout"
GENERATION   OBSERVED   TARGET                       READY   UPDATED-READY   AVAILABLE
3            3          http-lab-engine-<new-hash>   True    3               3
```

The old revision remains a recorded pod template until revision retention removes it; OME does not rewrite that template into the new one. The InferenceService and runtime are the inputs you edit.

Stop the port-forward from Step 3 with Ctrl-C, then start it again. Its selected pod may have been replaced during the rollout:

```bash
kubectl port-forward service/http-lab-engine -n ome-http-lab 8080:8080
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

In the second terminal, repeat the request:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/
```

```output
hello from OMENative v2
```

The patches changed the live InferenceService; the sample files remain unchanged. For a deployment you keep, record the new replica bounds and runner arguments in your own source manifest too.

## Troubleshooting

### No InferenceReplica appears

Check the InferenceService's conditions and events with `kubectl describe inferenceservice http-lab -n ome-http-lab`. Verify that the controller, CRDs and webhooks come from the same development source and that the runtime is in `ome-http-lab`. This lab does not work on v1.2.2.

### Pods stay Pending or cannot pull the image

Use `kubectl describe pod -n ome-http-lab -l ome.io/inferenceservice=http-lab` to read scheduler and image-pull events. Check registry access, worker architecture, namespace quota and available CPU/memory. If the namespace is enrolled in Kueue or another admission system, meet that system's queue requirements or use a lab namespace outside its scope.

### The update stops before three Instances are ready

Read the pod events and logs before changing the replica count. An update needs capacity for a replacement pod; the current three pods using all available quota leaves it Pending. For a failed image or readiness check, see [Reset failed Instances](reset-failed-instances.md) and [Release a held revision](../roll-out-changes/release-a-held-revision.md). Fix the underlying cause before requesting recovery.

### The response still says v1

Check Step 5's target revision and updated-ready count, then restart the port-forward. A successful initial `Ready` condition alone does not prove the new revision finished rolling out.

## Clean up

To practice diagnosis before deleting the lab, continue with [Recover a failed HTTP workload](recover-a-failed-http-workload.md). It uses these three Instances and introduces a deliberate readiness failure.

Stop the port-forward, then delete the InferenceService and wait for its replica to drain:

```bash
kubectl delete inferenceservice http-lab -n ome-http-lab --wait=true --timeout=5m
kubectl wait inferencereplica/http-lab-engine -n ome-http-lab --for=delete --timeout=5m
```

```output
inferenceservice.ome.io "http-lab" deleted
```

The second command succeeds silently if the replica is already gone; otherwise it reports that the delete condition was met. If deletion stalls, use [Recover stuck deletions](recover-stuck-deletions.md).

Delete the namespace only if you used it solely for this lab. This also removes the runtime and all other resources in that namespace:

```bash
kubectl delete namespace ome-http-lab --wait=true --timeout=5m
```

```output
namespace "ome-http-lab" deleted
```

## Next steps

- [Serve your first model](../../getting-started/serve-your-first-model.md): replace the HTTP exercise with model inference.
- [OMENative update strategies](../../concepts/architecture/omenative-update-strategies.md): choose how a real workload replaces its pods.
- [Serve a multi-node model](serve-a-multi-node-model.md): make one Instance span a leader and workers.
- [Standalone InferenceReplicas](../../concepts/omenative/overview.md#standalone-inferencereplicas): manage a pod set without an InferenceService.
