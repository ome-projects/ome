# Alfred nested-KWOK E2E

This harness runs a self-contained KWOK Kubernetes API server in an isolated
namespace of an existing host cluster. A real Alfred pod connects to that
nested API; only the nested Nodes and workload Pods are simulated. No local
container runtime is required, and no fake Node is added to the host cluster.

The fragmentation fixture creates ten fake 8-GPU nodes, places one movable
1-GPU RawDeployment OME pod on each node, and leaves an 8-GPU demand pending.
Seven GPUs free on every node seats no 8-GPU job at all, so observed size-8
fragmentation is high. RawDeployment migration has no executable contract,
however, and therefore contributes no reclaimable fragmentation.

Two settings are E2E-only, both for determinism rather than to force the
result. `demandBlendLambda: 1.0` takes the scoring weights entirely from the
shipped size prior instead of from observed demand, so the score does not
drift as pods come and go. The loop intervals and cooldowns are compressed to
seconds and single minutes so a cycle is observable inside the verifier's
120-second window.

## Run

Requires `kubectl` and `jq` on PATH. The make targets provision kustomize into
`bin/` themselves; only a direct `hack/alfred-e2e/create.sh` invocation needs
`KUSTOMIZE_BIN` pointed at one.

The host kubeconfig must point to a disposable/test Kubernetes cluster. The
Alfred image must match the host node architecture. For a private image, name
an existing `kubernetes.io/dockerconfigjson` pull secret to copy into the
isolated namespace.

```bash
make alfred-e2e-cluster \
  ALFRED_E2E_HOST_KUBECONFIG=/path/to/host-kubeconfig \
  ALFRED_E2E_IMG=<registry>/<repo>/alfred:<tag> \
  ALFRED_E2E_PULL_SECRET_SOURCE=ome/alfred-build-registry

make alfred-e2e-fragmentation \
  ALFRED_E2E_HOST_KUBECONFIG=/path/to/host-kubeconfig
```

The second target succeeds only when:

- the synthetic 8-GPU demand is rejected by the scheduler as `Unschedulable`;
- `alfred_cluster_fragmentation_score` is exactly `0` while
  `alfred_fragmentation_observed{size="8"}` exceeds `0.25`;
- `alfred_omenative_unavailable` is exactly `1` because the fixture publishes
  no executor capability Lease;
- `alfred-recommendations/last-cycle.json` contains at least one `advisory`
  recommendation with `RawDeploymentMigrationUnsupported`; and
- the fixture InferenceService has no `ome.io/migration-request-v1-*`
  annotation.

The nested fixture intentionally publishes no OMENative executor capability
Lease. Alfred must therefore report the executor unavailable and keep any
OMENative candidate advisory; CRD presence alone cannot enable execution. The
Dispatcher boundary is also absent: Alfred reports RawDeployment advice but
cannot turn it into migration annotations.

Re-run assertions without recreating the fixture:

```bash
make alfred-e2e-verify \
  ALFRED_E2E_HOST_KUBECONFIG=/path/to/host-kubeconfig
```

Delete the host namespace, including the nested cluster and its complete
simulated state:

```bash
make alfred-e2e-clean \
  ALFRED_E2E_HOST_KUBECONFIG=/path/to/host-kubeconfig
```

Optional overrides:

- `ALFRED_E2E_NAMESPACE` — host namespace, default `alfred-e2e`.
- `ALFRED_E2E_HOST_CONTEXT` — explicit context inside the host kubeconfig.
- `ALFRED_E2E_NESTED_PORT` — local tunnel port, default `18080`.
- `KWOK_CLUSTER_IMAGE` — nested KWOK image, default
  `registry.k8s.io/kwok/cluster:v0.8.0-k8s.v1.33.12`.
- `KUSTOMIZE_BIN` — kustomize binary, default `bin/kustomize`.

The nested KWOK container runs as root: its all-in-one image declares no
`USER` and writes control-plane state as root, so it meets the `baseline` Pod
Security Standard but not `restricted`. Both containers drop all capabilities,
disallow privilege escalation, and set `RuntimeDefault` seccomp.
