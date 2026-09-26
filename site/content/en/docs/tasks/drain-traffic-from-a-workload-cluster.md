---
title: "Drain Traffic from a Workload Cluster"
linkTitle: "Drain Traffic"
weight: 25
date: 2026-09-26
description: >
  Add one removable traffic-drain override with kubectl ome traffic drain, and remove it again with traffic undrain.
---

This page shows you how to place a **guarded, durable traffic-drain override** on a multi-cluster InferenceService with `kubectl ome traffic drain`, and how to remove it with `kubectl ome traffic undrain`. Both commands are **alpha**.

A drain does not move traffic by itself. It records one independently removable entry in the `ome.io/traffic-drain` annotation on the control-plane InferenceService — a request that the routing flow hold every route arm serving the named WorkloadCluster at weight zero until the entry is removed. The multi-cluster routing that consumes this annotation is still under active development, so treat the command as what it verifiably is: a guarded annotation write, whose acceptance by the API server is **not** TrafficMap convergence and **not** data-plane realization.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome).
- You need `get` and `patch` on `inferenceservices` in the `ome.io` group; see the [action RBAC rule](/ome/docs/tasks/kubectl-ome/#required-rbac).
- The target must be the **control-plane** InferenceService (the one you authored) and it must declare multi-cluster placement inputs — `spec.placement.requirements` or `spec.placement.clusterSelector` (or their legacy annotation equivalents `ome.io/accelerator-requirements` / `ome.io/cluster-selector`). Setting only `spec.placement.mode` is not enough. An ordinary single-cluster service is refused with `traffic action refused: target is not eligible for cross-cluster traffic routing`, and so is a derived copy that the placement flow created on a workload cluster.

## `--workload-cluster` is the drain target; `--cluster` is not

`kubectl ome traffic drain` takes two similarly named flags that do very different things:

- **`--workload-cluster`** (drain-specific, required) names the **WorkloadCluster resource** whose traffic arm you want held at zero. This is the drain target.
- **`--cluster`** (inherited by every command from the standard kubectl connection flags) selects the **kubeconfig cluster entry** — which Kubernetes API server the plugin talks to. It never selects what gets drained.

The command refuses to guess: running `traffic drain` without `--workload-cluster` fails before any API call with

```
traffic drain requires --workload-cluster; --cluster selects the Kubernetes API cluster
```

The value of `--workload-cluster` is validated only as a DNS-1123 name — the command does not check that a WorkloadCluster with that name exists. An override naming a cluster that no route arm serves simply sits pending (see below), so double-check the name against `kubectl get workloadclusters`.

## Add one drain override

Preview first with a client dry-run, which reads the service, prints the preview, and sends no patch:

```bash
kubectl ome traffic drain chat -n prod \
  --workload-cluster=worker-a \
  --id=maintenance-a \
  --reason="planned maintenance" \
  --dry-run=client
```

`--id` is a DNS-1123 label you choose; it is the key under which the override is stored and the handle you later pass to `undrain`. `--reason` is required, bounded operator context (1–256 bytes, trimmed printable text, no line breaks); it is stored **verbatim in the annotation**, readable by anyone who can read the service, so keep it non-secret — values that look like credentials are refused outright.

Then run the same command without `--dry-run`. The preview and the `Confirm this exact action? [y/N]` prompt go to **stderr** and show the exact target (name, UID, resourceVersion), the override ID, cluster, quoted reason, the override count change, and the follow-up command. Confirmation needs a real terminal; in scripts pass `--yes`, otherwise the command fails with `action not confirmed; noninteractive input requires --yes`. **stdout** carries exactly one typed `ActionResult`:

```json
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ActionResult",
  "collectedAt": "2026-09-26T10:00:00Z",
  "action": "traffic drain",
  "target": {"kind": "InferenceService", "namespace": "prod", "name": "chat", "uid": "…", "resourceVersion": "42"},
  "dryRun": "none",
  "accepted": true,
  "applied": true,
  "message": "API accepted traffic annotation request; not TrafficMap convergence.",
  "followUp": "kubectl ome traffic status chat -n prod --context=…",
  "traffic": {"overrideID": "maintenance-a", "cluster": "worker-a", "overridesBefore": 0, "overridesAfter": 1}
}
```

`--dry-run=server` sends the identical guarded patch with `dryRun=All`, so the API server validates it without persisting anything (`"message": "API dry-run accepted; no changes persisted."`).

## What acceptance means — and does not mean

`"accepted": true` means the API server accepted the guarded annotation patch. It does **not** mean:

- **TrafficMap convergence.** Where the routing flow is enabled, a control-plane routing controller maintains a TrafficMap (a namespaced resource named after the InferenceService, `kubectl get tm chat -n prod`). On consuming the annotation it records the override ID in the matching arms' `spec.entries[].drainRefs`, holds those arms at weight zero, and reports the `OverrideActive` condition — `OverridesApplied` when at least one arm is held, `OverridesPending` when no routable arm matches the named cluster (for example, a misspelled `--workload-cluster`). None of that has been observed yet when the command returns.
- **Data-plane realization.** Even a converged TrafficMap is controller-reported state, not proof that gateways stopped sending requests.

Follow up with the command the result prints:

```bash
kubectl ome traffic status chat -n prod
```

`traffic status` shows bounded controller-reported traffic evidence from the InferenceService; it too does not prove data-plane realization or live reachability.

## Remove the override

Undrain takes only the service and the override ID — `--workload-cluster` and `--reason` are not accepted:

```bash
kubectl ome traffic undrain chat -n prod --id=maintenance-a
```

It removes **exactly that one ID**. Every other override ID and every unrelated annotation is preserved byte-for-byte; removing the last override removes the `ome.io/traffic-drain` annotation entirely. Undraining an ID that does not exist is refused (`traffic undrain refused: override ID does not exist`), as is draining an ID that already exists, even with identical values (`traffic drain refused: override ID already exists`) — remove it first if you want to change its cluster or reason.

The annotation value is a strict JSON object keyed by override ID:

```yaml
metadata:
  annotations:
    ome.io/traffic-drain: '{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}'
```

Because separate drains are separate keys, two operators can hold two clusters independently and release them independently.

## Safety guards and refusals

Both commands fail closed rather than "fix" anything they did not write:

- **Exact-identity patch.** The mutation is a JSON Patch that first `test`s the service's exact UID and `resourceVersion` read during preview. If anything changed the object in between — or a same-name service was recreated — the patch is rejected with `guarded annotation patch rejected; refresh traffic status and retry explicitly` and exit code `3`; rerun to retry against fresh state.
- **Strict parsing of existing state.** A malformed existing annotation, duplicate override IDs, unknown fields, unsafe stored values, more than 64 overrides, or a value over 32 KiB refuses the action before any preview. Normalizing broken drain state away could silently restore traffic during an incident, so the command never does.
- **Closed flag parser.** Any unknown or malformed flag fails with `invalid traffic action flags; use --help` before a client is constructed.
- **Bounded requests.** The whole action runs under a 45-second context, each API request is capped at 10 seconds (a shorter `--request-timeout` is preserved), responses are size-bounded and verified to be the requested InferenceService, and redirects are refused. An unverifiable response fails with "outcome unknown, check traffic status".

## Flag reference

| Flag | Commands | Description |
|------|----------|-------------|
| `--id` | drain, undrain | Required. DNS-1123 label naming the override entry. |
| `--workload-cluster` | drain | Required. WorkloadCluster DNS name to drain — not the kubeconfig `--cluster`. |
| `--reason` | drain | Required. Bounded, non-secret operator context, stored verbatim in the annotation. |
| `--yes` | both | Confirm the exact preview without an interactive prompt. |
| `--dry-run` | both | `none` (default), `client` (no patch sent), or `server` (`dryRun=All`). |
| `-o, --output` | both | `table` (default), `wide`, `json`, or `yaml` for the stdout ActionResult. |
