# vLLM with a node-local LMCache pool

These manifests run a single-pod vLLM engine using the LMCache multiprocess
connector. Each selected node gets its own L1 cache. Multiple services in the
same namespace can reference the pool.

## Prerequisites

Install the OME CRDs and manager from this branch. Creating the pool starts
one LMCache server per selected node. OME creates the DaemonSet, ClusterIP
Service and connection ConfigMap in the pool namespace.

- Label the intended NVIDIA GPU nodes and set their selector and tolerations
  in the pool. Both workloads must tolerate node taints. The server uses the
  NVIDIA RuntimeClass (`nvidia` by default, configurable with
  `spec.workloads[].runtimeClassName`), sees all GPUs and shares them with
  vLLM through host IPC. OME sets `privileged: false` on the server container.
  The namespace must allow `hostIPC: true` for both the server and vLLM Pods.
  Grant pool write access only to trusted administrators.
  Keep engines sharing nodes and host IPC within one trust domain.
- Use a driver compatible with the two images. The sample pins the CUDA 12.9
  standalone server digest and selects a matching LMCache v0.5.1 client
  image on the vLLM ServingRuntime. The server reserves the per-node L1
  budget plus 5 GiB of process memory by default. Override its resources
  under `spec.workloads[].runner.resources` for your environment.
- The Service selects servers by `ome.io/kvcachepool-server-uid` and routes
  to local ready endpoints only. Use your cluster's network policies to
  restrict ingress on the transfer port (5555) to trusted consumers. Node-local
  routing and namespace-scoped references do not authenticate network clients.
- Make the [Qwen model](model.yaml) available using the normal OME model setup.

## Start the service

Apply the files to the same namespace from the repository root:

```sh
kubectl apply -f config/samples/kvcachepool/model.yaml
kubectl apply -f config/samples/kvcachepool/lmcache-node-local.yaml
kubectl wait --for=condition=Ready kvcachepool/lmcache-node-local --timeout=10m
kubectl apply -f config/samples/kvcachepool/vllm-runtime.yaml
kubectl apply -f config/samples/kvcachepool/inferenceservice.yaml
kubectl get inferenceservice qwen-cache -o yaml
```

Check these signals separately:

| Signal | What it establishes |
| --- | --- |
| Pool `Ready=True` | The DaemonSet has rolled out on all desired nodes and OME published the connection ConfigMap. |
| Service `KVCacheConfigured=True` | OME reconciled the engine workload with the pool's connection. |
| Service `EngineReady=True` | The engine passed its normal readiness check. |
| External cache hits or retrieved chunks | A request used the external cache. Measure this on the vLLM or LMCache metrics endpoint. |

The engine template gets the pool's node selector, a deterministic hash seed,
and the pool's connection JSON as one vLLM argument. A conflicting node
selector or an existing connector argument blocks the binding and appears in
the service conditions. Keep admission webhooks from injecting a second KV
connector. For host IPC, leave the runner's `/dev/shm` unmounted and omit
the OME shm profile.
A missing or unready pool holds the desired rollout. Existing Pods follow
their connector's runtime failure behavior. This profile configures L1 only.

## Check reuse

Send a long prompt twice to the same node with a common prefix spanning
several LMCache chunks. Use separate engine instances, or clear only vLLM's
local prefix cache before the second request, and record external-hit tokens
or provider chunks retrieved before and after it. Keep model weights,
tokenizer, KV dtype, attention layout and parallelism identical for consumers
expected to reuse KV. Use a new pool for incompatible revisions or different
trust domains. Pool references and connection data stay in the service's
namespace. This profile uses no backend Secret.

For a single-engine smoke test, temporarily include
`--no-enable-prefix-caching` in the runtime runner's arguments before
starting the service. Forward the engine Pod's port 8080 to localhost, then
run the following probe against the example model. The two prompts share
1,024 token IDs and have different final tokens.

```sh
python3 - <<'PY'
import json
import time
from urllib.request import Request, urlopen

base = "http://127.0.0.1:8080"

def external_hits():
    with urlopen(base + "/metrics", timeout=10) as response:
        lines = response.read().decode().splitlines()
    counters = [line for line in lines if line.startswith("vllm:external_prefix_cache_hits_total{")]
    if not counters:
        raise RuntimeError("vLLM did not expose the external prefix cache hit counter")
    return sum(float(line.split()[-1]) for line in counters)

for suffix in [9, 10]:
    before = external_hits()
    body = {"model": "qwen", "prompt": [7, 8] * 512 + [suffix], "max_tokens": 1, "temperature": 0}
    request = Request(base + "/v1/completions", data=json.dumps(body).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=120) as response:
        print(json.load(response)["usage"])
    time.sleep(3)
    delta = external_hits() - before
    print("External prefix hit tokens:", delta)
    if suffix == 10 and delta <= 0:
        raise RuntimeError("No external KV reuse observed")
PY
```

Run the probe on an otherwise idle engine with a fresh pool so the counter
delta can be attributed to the request.

For performance comparisons, run the same prompt mix and concurrency against
an unbound engine, then against cold and warm bound engines. Record TTFT p50
and p95, generated tokens/s, host CPU and memory for both processes, and Pod
network bytes per request. Compare runs on identical hardware and keep local
prefix caching settings fixed across them.

## Release the pool

Remove the service or remove its pool reference and let the old engine Pods
drain. The pool finalizer waits for every referencing service and its remaining
workloads before removing the DaemonSet, Service and connection ConfigMap.
Deleting one service leaves the pool available to other services.

## Recover after a cache server restart

A new server starts with an empty L1 and needs each vLLM worker to register
its GPU KV tensors again. With LMCache v0.5.1, a worker that observes a
failed heartbeat followed by a successful one re-registers automatically.
A fast replacement between successful heartbeats can leave the worker
unaware that the server has changed.

After the server is ready, send a fresh prefix twice. If external hits stay
at zero on the second request and the server reports an unregistered GPU
context, restart the affected vLLM Pod and repeat the probe.
[LMCache issue #4642](https://github.com/LMCache/LMCache/issues/4642)
