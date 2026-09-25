# Multicluster Helm option audit

Remote authentication belongs to the platform. The member charts define the
permissions it grants a remote principal. The controller and quota manager
still need their own local ServiceAccounts for their local API operations.

The access templates create only ClusterRoles and optional bindings to existing
identities. They create no remote ServiceAccounts or token Secrets. Placement
access is opt-in, matching quota access. Empty `subjects` is valid: a separate
platform application can own the binding.

## Access and topology

| Option | Decision and reason |
| --- | --- |
| `ome.multiclusterAccess.enabled` | Keep, default false. Installs the member-side placement role independently of the local controller's role. |
| `ome.multiclusterAccess.subjects` | Optional. Empty for externally managed bindings; otherwise binds existing identities. |
| `quotaManager.remoteAccess.enabled` | Keep, default false. Installs the member-side quota role. |
| `quotaManager.remoteAccess.name` | Keep. Existing platform ACLs and bindings refer to this role name; renaming it is an access migration. |
| `quotaManager.remoteAccess.subjects` | Optional, with the same semantics as placement access. |
| `quotaManager.remoteAccess.serviceAccount.create`, `.staticToken` | Removed. Authentication provisioning does not belong in the remote-permission chart. |
| `ome.multicluster.enabled`, `.role` | Retained for compatibility. A pure control plane needs only one conceptual mode, but the binary also supports transport plus local reconciliation (`enabled=true`, empty role). The chart currently gates flags on `enabled`, and routing validation requires both `enabled=true` and `role=control-plane`. Consolidating these requires a deliberate values/CLI migration, not deleting one setting from an existing install. |
| `ome.multicluster.placementControlPlaneID` | Keep stable. Ownership marker used by derived-object GC and cache scoping; changing it can strand existing objects. |
| `quotaManager.mode` | Keep. Management projects quotas; workload mode measures local capacity and materializes Kueue objects. |
| `quotaManager.projection.origin` | Keep stable. Ownership marker for projected-quota sweeps and edit protection. This is independent of placement ownership. |
| `quotaManager.projection.fieldManager` | Keep. SSA ownership of projected specs; it is not the origin label and has a different function. |
| `quotaManager.projection.defaultDistributionPolicy` | Optional fallback. Redundant when every leaf budget already resolves an explicit distribution policy; still useful for other trees. Cohort projections carry topology, not hub containment budgets. |

Both access roles remain separate because they grant different capabilities.
This does **not** enforce separate authenticated principals. Both binaries read
the same WorkloadCluster kubeconfig. Exec authentication can produce a different
principal for each pod identity; an inline bearer token is the same identity in
both processes.

## Authentication controls

| Option | Decision and reason |
| --- | --- |
| `ome.multicluster.execCredentials.enabled` | Keep explicit permission to execute a kubeconfig plugin inside the controller process. |
| `ome.multicluster.execCredentials.allowedCommands` | Keep an exact command allowlist. The chart takes a comma-separated string; an absolute path must match exactly. |
| `quotaManager.projection.execCredentials.enabled` | Keep: a separate process must opt in independently. |
| `quotaManager.projection.execCredentials.allowedCommands` | Keep. This chart takes a list, joined into the same CLI representation. |
| Pod annotations, `extraEnv`, `extraVolumes`, `extraVolumeMounts` | Keep when required by the chosen identity provider. An exec allowlist does not install a plugin or supply its certificates. |

The two charts take different shapes for the same allowlist (a comma-separated
string and a list). The controller's default allowlist
(`aws,gke-gcloud-auth-plugin,kubelogin`) also differs from the quota manager's
empty allowlist. Prefer an explicit list of the commands actually installed;
changing defaults should cover both chart and CLI consumers together.
Authentication policy is independent of the access templates, which grant
permissions only.

## Transport and placement tuning

All options below are under `ome.multicluster.config`. Each has a consumer in
`cmd/manager/multicluster.go`; none is an entirely disconnected Helm value.

| Options | Decision and applicability |
| --- | --- |
| `workloadCluster.clientQPS`, `.clientBurst`, `.perCallTimeout` | Keep as optional remote API limits. Distinct from the controller's local API limits. |
| `workloadCluster.cacheEnabled` | Keep. Enables the derived-ISVC informer cache and status event funnel. Removing it changes convergence and API load. |
| `workloadCluster.healthInterval`, `.connectionGrace` | Keep. Probe frequency and disconnection grace serve different purposes. |
| `workloadCluster.eventsBatchPeriod` | Optional debounce for kubeconfig-Secret events. |
| `workloadCluster.establishInitial`, `.establishMax`, `.reconnectRetryMax` | Advanced tuning for remote-watch establishment and retry. Useful for slow/unreachable members. |
| `workloadCluster.funnelResyncInterval`, `.funnelBufferSize` | Advanced tuning, relevant when the cache/funnel is enabled. Membership-watch refresh and event buffering are different from ISVC reconciliation. |
| `placement.requeueInterval` | Keep as the polling cadence when the funnel is disabled and for non-steady placement progress. |
| `placement.statusBatchPeriod`, `.statusSafetyRequeue` | Keep for event debounce and missed-event recovery when the funnel is enabled. |
| `placement.gcInterval` | Keep. Orphan cleanup has a separate lifecycle from readiness reconciliation. |
| `placement.maxConcurrentReconciles`, `.fanoutTimeout` | Keep as worker and remote-operation bounds. |
| `placement.winnerLostGrace` | Relevant to sticky single-active placement; not a Split distribution control. |
| `placement.dispatcherMode`, `.dispatcherStepSize`, `.dispatcherRoundTimeout` | Candidate-dispatch tuning for single-active placement. The step/dwell knobs are unused by AllAtOnce; Split follows its apportionment path. |
| `placement.localQueue` | Optional fallback only. A source's `ome.io/local-queue` annotation wins. Leave empty for a fleet with per-service quota leaves. |

Empty/zero entries mostly delegate to consuming-package defaults; they need not
be repeated in GitOps overrides. They should remain documented advanced options,
not be removed solely because one deployment does not override them.

Timing defaults that no value above exposes live in `workloadcluster/options.go`,
`backoff.go`, and `placement`; they apply to every caller, including non-Helm
installs.

## Endpoint and routing options

| Options under `ome.multicluster.config` | Decision and applicability |
| --- | --- |
| `endpoint.globalHostTemplate`, `.globalGateway`, `.routeNamespace`, `.backendPort` | Keep for Gateway API publication. Not required by an external publisher. Empty global gateway disables the legacy endpoint path. |
| `endpoint.gatewayBackend.rewriteHostname` | Gateway-specific per-home Host rewrite. |
| `endpoint.gatewayBackend.tls.enabled`, `.wellKnownCACertificates` | Gateway-specific upstream TLS policy. |
| `endpoint.gatewayBackend.endpointSlices.enabled`, `.addressRefreshInterval` | Gateway-specific fallback when ExternalName resolution is insufficient. |
| `routing.enabled` | Keep. Opts into TrafficMap generation; disabling it also participates in publisher cleanup. |
| `routing.observer.maxConcurrentReconciles`, `.maxConcurrentRequests`, `.maxResponseBytes`, `.minPeriod`, `.maxSamples` | Keep chart defaults. Validation requires these bounds when routing is enabled, even if active probes are not configured. |
| `routing.publisher.name`, `.resyncInterval`, `.options` | Keep. Selects and configures the publication backend and its periodic recovery cadence. |
| `routing.probe` (`path`, `method`, `acceptStatuses`, `gateStatuses`, `period`, `timeout`, `failureThreshold`, `successThreshold`, `allFailedPolicy`) | Optional active endpoint health checks. Omit when readiness-based health is sufficient. |
| `routing.capacity` (`path`, `method`, `format`, `options`, `samples`, `quorum`, `period`, `timeout`, `maxAge`) | Optional endpoint-reported capacity ceiling. Independent of probing; omit when planned capacity is sufficient. |

With an external publisher, Gateway backend settings, probes, and capacity polling
need no override unless those capabilities are deliberately used. They remain
valid chart features for other deployments. Publisher options are backend-owned;
the chart cannot remove URLs, TLS paths, target templates, or timeouts on behalf
of that backend.

## Adjacent feature switches

Policy CRD gates must match the resource-chart gates (`ome.rolloutPolicy` and
`ome.autoscalerPolicy`). Ref-only rollout plans are resolved on the control
plane and rendered into derived specs; the runtime itself remains member-owned.
`ome.autoscalerPolicy.preflight.memberGetTimeoutSeconds` and
`.skewDeadlineSeconds` bound remote policy reads and unresolved-policy skew;
they are separate from transport timeouts and are not needed by a service
without AutoscalerPolicy references.

`modelAgent`, the local scheduler, KEDA, and bundled Prometheus need not run on
a pure placement control plane. Member clusters still need the components their
workloads use. The chart's generic pod scheduling, monitoring, identity, and
certificate options are independent deployment concerns, not redundant
multicluster controls.
