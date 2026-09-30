---
title: Controller manager flags
description: Look up the OME controller manager's command-line flags, their defaults and the ome-resources Helm values that set them.
---

The OME controller manager reads these flags from the `manager` container of the `ome-controller-manager` Deployment when it starts. The `ome-resources` chart always passes a few of them, and sets many others from Helm values. To change a flag, see [Configure the controller](../../guides/operate-ome/configure-the-controller.md).

Default is what the manager uses without the flag. The Helm value column names the `ome-resources` value that sets the flag, with the chart's default in parentheses. None means that you set the flag with a patch: see [Flags without a Helm value](#flags-without-a-helm-value).

## Servers and metrics

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--webhook` | `false` | None. The chart always passes it. | Runs the admission webhook server. |
| `--webhook-port` | `9443` | None | The port of the webhook server. The chart's webhook Service expects 9443. |
| `--health-probe-addr` | `:8081` | None | The address of the `/healthz` and `/readyz` endpoints. The chart's probes expect port 8081. |
| `--enable-http2` | `false` | None | Serves HTTP/2 on the metrics and webhook servers. |
| `--metrics-bind-address` | `:8080` | None. The chart always passes `:8080`. | The address of the metrics endpoint. `0` turns the endpoint off. The chart's metrics Service expects port 8080. |
| `--metrics-secure` | `false` | None | Serves the metrics endpoint over HTTPS. |

## Leader election

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--leader-elect` | `false` | None. The chart always passes it. | Elects a leader among the replicas, so that only one of them reconciles. |
| `--leader-election-namespace` | `ome` | None | The namespace of the leader-election Lease. |
| `--leader-elect-lease-duration` | `15s` | `ome.controller.leaderElection.leaseDuration` (`60s`) | Since v1.3. How long the Lease stays valid after its last renewal. [Lease timing](../../guides/operate-ome/configure-the-controller.md#lease-timing) gives the rules for the three timing flags. |
| `--leader-elect-renew-deadline` | `10s` | `ome.controller.leaderElection.renewDeadline` (`40s`) | Since v1.3. How long the leader retries a failed renewal before it gives up leadership and exits. |
| `--leader-elect-retry-period` | `2s` | `ome.controller.leaderElection.retryPeriod` (`8s`) | Since v1.3. The time between renewal attempts, and how often a standby checks the Lease. |

## Reconcile throughput {since=v1.3}

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--inferenceservice-max-concurrent-reconciles` | `1` | `ome.controller.inferenceServiceMaxConcurrentReconciles` (`4`) | How many InferenceServices the manager reconciles in parallel. See [Tune reconcile throughput](../../guides/operate-ome/configure-the-controller.md#tune-reconcile-throughput). |
| `--inferencereplica-max-concurrent-reconciles` | `1` | `ome.controller.inferenceReplicaMaxConcurrentReconciles` (`4`) | How many InferenceReplicas the manager reconciles in parallel. |
| `--kube-api-qps` | No limit | `ome.controller.kubeAPIQPS` (`100`) | The most requests per second that each API client of the manager sends. The manager has a client per resource type. |
| `--kube-api-burst` | `0` | `ome.controller.kubeAPIBurst` (`200`) | How many requests each API client can send at once before the `--kube-api-qps` rate applies. Required with `--kube-api-qps`, and has no effect without it. |

## Controllers

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--config-cache-ttl` | `30s` | `ome.controller.configCacheTTL` (`"30s"`) | Since v1.3. How long the manager caches the `inferenceservice-config` ConfigMap. `0` turns the cache off. See [Set the cache TTL](../../guides/operate-ome/configure-the-controller.md#set-the-cache-ttl). |
| `--accelerator-resources` | Every Component | `ome.controller.quotaAcceleratorResources` (`nvidia.com/gpu`, `google.com/tpu`), not `acceleratorResources` | Since v1.3. The comma-separated resource names that send a Component through quota admission when its pods request one. Match the same flag on `ome-quota-manager`. See [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md). |
| `--enable-inferencereplica-controller` | `true` | None | Since v1.3. Runs the InferenceReplica controller, which creates and updates the pods of [OMENative](../../concepts/omenative/overview.md) services. |
| `--runtime-revision-retention` | `10` | None | How many revisions of each runtime name the manager keeps, newest first, even when no InferenceService uses them. See [Tune runtime-revision garbage collection](../../guides/operate-ome/configure-the-controller.md#tune-runtime-revision-garbage-collection). |
| `--runtime-revision-grace-period` | `24h` | None | How long the manager waits before it deletes a revision that it no longer keeps. |

## Logging

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--zap-encoder` | `json` | None. The chart always passes `console`. | The log format: `json` or `console`. |
| `--zap-log-level` | `info` | None | The lowest level that's logged: `debug`, `info`, `error`, `panic`, or an integer above 0 for more verbose levels. |
| `--zap-stacktrace-level` | `error` | None | The lowest level that logs a stack trace: `info`, `error` or `panic`. |
| `--zap-time-encoding` | `rfc3339` | None | The timestamp format: `epoch`, `millis`, `nanos`, `iso8601`, `rfc3339` or `rfc3339nano`. |
| `--zap-devel` | `false` | None | Uses development defaults: the `console` format, the `debug` level, and stack traces from the `warn` level. |

## Multi-cluster {since=v1.3}

!!! note "Alpha"
    Multi-cluster is alpha and still in development. These flags and their behavior can change between releases.

Multi-cluster is off by default. The chart passes these flags only when `ome.multicluster.enabled` is `true`. See [Configure routing health probes](../../guides/multi-cluster/routing-health-probes.md) and [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md).

| Flag | Default | Helm value | Description |
| --- | --- | --- | --- |
| `--enable-multicluster` | `false` | `ome.multicluster.enabled` (`false`) | Runs the multi-cluster controllers, such as the WorkloadCluster registry. |
| `--multicluster-role` | Empty | `ome.multicluster.role` (`""`) | `control-plane` makes this manager place InferenceServices on workload clusters instead of running their pods, and implies `--enable-multicluster`. Leave it empty on other clusters. |
| `--allow-exec-credentials` | `false` | `ome.multicluster.execCredentials.enabled` (`false`) | Lets WorkloadCluster kubeconfigs run an exec credential plugin in the manager pod. The plugin's binary has to be in the manager image. |
| `--exec-credential-allowed-commands` | `aws,gke-gcloud-auth-plugin,kubelogin` | `ome.multicluster.execCredentials.allowedCommands` (`aws,gke-gcloud-auth-plugin,kubelogin`) | The plugin commands that `--allow-exec-credentials` allows, comma-separated. A name allows that command from the `PATH`, and an absolute path allows only that binary. |
| `--placement-control-plane-id` | Empty | `ome.multicluster.placementControlPlaneID` (`""`) | The identity that this control plane stamps on the InferenceServices that it creates on workload clusters. Give each control plane its own when several share a workload cluster. |

## Flags without a Helm value

To set a flag whose Helm value is None, patch the arguments of the `manager` container, as [Configure the controller](../../guides/operate-ome/configure-the-controller.md#flags-without-a-helm-value) shows. A later `helm upgrade` can remove a patched flag, so check your flags after each upgrade.

## Related pages

- [Configure the controller](../../guides/operate-ome/configure-the-controller.md): set a flag, and tune leader election, reconcile throughput and the config cache.
- [Collect metrics](../../guides/operate-ome/metrics.md): scrape the manager's metrics endpoint.
- [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md): the quota admission that `--accelerator-resources` scopes.
- [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md): the revisions that the garbage collection flags keep.
