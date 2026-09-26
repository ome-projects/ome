---
title: "Controller Configuration"
linkTitle: "Controller Configuration"
weight: 6
description: >
  Command-line flags for the OME controller manager, including reconcile throughput tuning and runtime-revision garbage collection.
---

The OME controller manager (`ome-manager`) is configured through command-line flags passed to its container. This page documents those flags and how to change them on a running cluster.

## Setting flags

The flags are parsed once at startup, so a change takes effect when the controller pod restarts. No image rebuild is required.

- **Helm** (recommended): set them in your chart values / the manager Deployment's `args` and run `helm upgrade`.
- **Directly on the Deployment** (quick, but reverted by the next `helm upgrade`):

```bash
kubectl -n ome patch deployment <ome-controller-manager> --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--runtime-revision-retention=20"},
  {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--runtime-revision-grace-period=72h"}
]'
```

The manager logs its resolved configuration at startup, so you can confirm a value landed:

```bash
kubectl -n ome logs deploy/<ome-controller-manager> | grep -iE 'retention|gracePeriod'
```

## Flags

### Server and webhook

| Flag                  | Type | Default | Description                                                        |
|-----------------------|------|---------|--------------------------------------------------------------------|
| `--webhook`           | bool | `false` | Enable the webhook server.                                         |
| `--webhook-port`      | int  | `9443`  | Port the webhook server binds to.                                  |
| `--health-probe-addr` | string | `:8081` | Address the health/readiness probe endpoint binds to.            |
| `--enable-http2`      | bool | `false` | Enable HTTP/2 for the metrics and webhook servers.                 |

### Leader election

| Flag                            | Type     | Default       | Description                                                                                            |
|---------------------------------|----------|---------------|--------------------------------------------------------------------------------------------------------|
| `--leader-elect`                | bool     | `false`       | Enable leader election so only one manager is active.                                                  |
| `--leader-election-namespace`   | string   | OME namespace | Namespace for the leader-election lease.                                                               |
| `--leader-elect-lease-duration` | duration | unset         | How long the lease stays valid after its last renewal, and so the longest a standby waits before taking over from a leader that stopped renewing. |
| `--leader-elect-renew-deadline` | duration | unset         | How long the leader keeps retrying a failed renewal before giving up leadership and exiting.           |
| `--leader-elect-retry-period`   | duration | unset         | Gap between renewal attempts inside a renew window, and the interval at which a standby polls for an expired lease. |

The three timing flags have no in-binary default and must be supplied together or not at all; see [Tuning leader election timing](#tuning-leader-election-timing).

### Metrics

| Flag                     | Type   | Default  | Description                                                                                     |
|--------------------------|--------|----------|-------------------------------------------------------------------------------------------------|
| `--metrics-bind-address` | string | `:8080`  | Address the metrics endpoint binds to (`:8443` for HTTPS, `:8080` for HTTP, `0` to disable).   |
| `--metrics-secure`       | bool   | `false`  | Serve metrics over HTTPS.                                                                        |

### Reconcile throughput

| Flag                                            | Type  | Default | Description                                                                                                       |
|-------------------------------------------------|-------|---------|--------------------------------------------------------------------------------------------------------------------|
| `--inferenceservice-max-concurrent-reconciles`  | int   | unset   | Max InferenceService reconciles running in parallel (distinct objects only).                                       |
| `--inferencereplica-max-concurrent-reconciles`  | int   | unset   | Max InferenceReplica reconciles running in parallel (distinct objects only).                                       |
| `--kube-api-qps`                                | float | unset   | Steady-state client-side request rate to the apiserver, shared by the manager cache/client, the direct clientset, and the webhook handlers. |
| `--kube-api-burst`                              | int   | unset   | Token-bucket burst above `--kube-api-qps`, absorbing the request spikes of a reconcile fan-out or an informer resync. |

None of the four has an in-binary default. The `ome-resources` chart supplies the operative values (`4`/`4`/`100`/`200`); zero or unset falls back to controller-runtime's defaults — one worker per controller and `20` QPS / `30` burst. See [Tuning reconcile throughput](#tuning-reconcile-throughput).

### Runtime-revision garbage collection

These control cleanup of the OME-managed `ControllerRevision` snapshots created for [runtime pinning](/ome/docs/concepts/runtime-revision).

| Flag                             | Type     | Default | Description                                                                                    |
|----------------------------------|----------|---------|------------------------------------------------------------------------------------------------|
| `--runtime-revision-retention`   | int      | `10`    | Number of `ControllerRevision` snapshots to retain per source runtime before garbage collection. |
| `--runtime-revision-grace-period`| duration | `24h`   | How long a snapshot must stay unreferenced and over-retention before the GC deletes it.        |

Logging is configured with the standard controller-runtime zap flags (for example `--zap-log-level`, `--zap-encoder`).

## Tuning leader election timing

The binary carries no leader election timings of its own. When the three timing flags are unset, controller-runtime's built-in defaults apply: `15s` lease duration, `10s` renew deadline, `2s` retry period. The `ome-resources` chart supplies `60s`/`40s`/`8s` by default through `ome.controller.leaderElection`, and renders the flags only when all three values are present:

```yaml
ome:
  controller:
    leaderElection:
      leaseDuration: "60s"
      renewDeadline: "40s"
      retryPeriod: "8s"
```

The three durations constrain each other, and the manager validates them at startup — a bad value is an immediate, named failure at boot rather than a late error partway through manager start:

- **All or none.** Setting only one or two of the flags is rejected; supply the complete set or leave all three unset.
- **All positive.** A zero or negative duration is rejected.
- **`--leader-elect-lease-duration` must exceed `--leader-elect-renew-deadline`.** Otherwise the lease can expire while its holder is still renewing, handing the lock to a standby while the old leader still believes it is active.
- **`--leader-elect-renew-deadline` must exceed 1.2× `--leader-elect-retry-period`** (the retry period jittered by client-go's jitter factor). Otherwise a renew window cannot hold one complete renewal attempt.

When sizing the values: controller-runtime caps each renewal request at half the renew deadline, so a renew window holds two attempts and the leader survives exactly one hung apiserver request. At controller-runtime's defaults that is a ~10s budget, which a routine etcd stall can exhaust — costing the leader its lease and restarting the pod. The chart's `40s` renew deadline buys two 20s attempts instead. The cost is failover latency: a standby waits up to the lease duration (`60s` at the chart default) before taking over, so raise these values only as far as your cluster's apiserver latency actually needs.

## Tuning reconcile throughput

These four flags answer one symptom: on a large cluster (thousands of InferenceServices), spec changes take a long time to be acted on because the controllers cannot drain their reconcile backlog. Two separate limits cap that throughput, and they need to be raised together.

**Worker concurrency.** `--inferenceservice-max-concurrent-reconciles` and `--inferencereplica-max-concurrent-reconciles` set how many reconciles each controller runs in parallel. Concurrency applies across distinct objects only — controller-runtime serializes per object key, so a single InferenceService is never reconciled by two workers at once, and raising the counts is safe. The binary has no built-in value; the chart supplies `4` for each controller, and `0`/unset falls back to controller-runtime's single worker.

**Client-side API rate limits.** `--kube-api-qps` and `--kube-api-burst` set the client-go rate limit on the one `rest.Config` every connection to the local apiserver is built from, so the manager's cache and client, the direct clientset, and the webhook handlers all draw from the same budget. QPS is the steady-state request rate; burst is the token-bucket headroom above it that absorbs the request spike of a reconcile fan-out or an informer resync. Again there is no in-binary value — the chart supplies `100` QPS / `200` burst, and `0`/unset falls back to controller-runtime's `20` QPS / `30` burst. The two limits are independent: supplying only one is honored, and the other keeps its fallback.

The chart values, under `ome.controller`:

```yaml
ome:
  controller:
    inferenceServiceMaxConcurrentReconciles: 4
    inferenceReplicaMaxConcurrentReconciles: 4
    kubeAPIQPS: 100
    kubeAPIBurst: 200
```

The chart renders each flag only when its value is non-zero, so setting a value to `0` omits the flag and restores the controller-runtime fallback.

The rate-limit floor scales with the worker counts: each worker issues several reads and writes per reconcile pass, so adding workers while leaving the `20` QPS fallback in place just moves the queue from the controller into the client rate limiter — the reconciles start, but each one stalls waiting for request tokens, long before the apiserver itself is the bottleneck.

### Recognizing client-side throttling

That stall is visible in the manager log. Requests delayed by the local limiter are logged by client-go as:

```
Waited for 1.034s due to client-side throttling, not priority and fairness, request: GET:https://10.96.0.1:443/apis/ome.io/v1beta1/...
```

As the message itself says, this is the client's own limiter, not apiserver-side API Priority and Fairness — the fix is raising `--kube-api-qps` / `--kube-api-burst`, not apiserver capacity. Sustained lines like this during steady operation mean the rate limit, not the worker count, is the current bottleneck.

The manager logs the effective limits at startup, so you can confirm a change landed (the controller-runtime fallback shows as `"qps": 20, "burst": 30`):

```bash
kubectl -n ome logs deploy/<ome-controller-manager> | grep 'Configured API client rate limits'
```

## Tuning runtime-revision garbage collection

The garbage collector keeps the newest `--runtime-revision-retention` snapshots per runtime and never deletes a snapshot that an InferenceService still references (via `spec.runtime.revision` or `status.pinnedRevisionName`). Anything else becomes GC-eligible; once it has stayed unreferenced and over-retention for longer than `--runtime-revision-grace-period`, it is deleted.

- Raise **retention** to keep a longer rollback history per runtime.
- Lower the **grace period** to reclaim storage sooner; raise it to keep a longer safety window before deletion.
- The grace period is a Go duration, so minutes and hours both work (`30m`, `72h`). There is no day unit - a week is `168h`.

See [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision) for the full pinning workflow.
