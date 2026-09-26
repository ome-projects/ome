---
title: "Runtime Revisions and Pinning"
linkTitle: "Runtime Revisions"
weight: 8
description: >
  Pin an InferenceService to a snapshot of its ServingRuntime for stable, roll-forward/rollback-controlled updates.
---

By default an InferenceService always renders its pods from the **live** ServingRuntime it references: whenever the runtime changes, the next reconcile picks up the new spec. Runtime **pinning** lets you decouple an InferenceService from live runtime changes by pinning it to an immutable **snapshot** of the runtime, so updates roll out only when you ask for them.

Snapshots are stored as Kubernetes `ControllerRevision` objects in the OME namespace, are content-addressed (deduplicated by hash), and are garbage-collected on a configurable schedule.

## `autoSync`: live vs. pinned

The behavior is controlled by `spec.runtime.autoSync` on the InferenceService:

| `autoSync` | Behavior |
|------------|----------|
| `true` (default) | Pods re-render from the **live** runtime every reconcile. Runtime edits take effect immediately. |
| `false` | The InferenceService is **pinned** to a `ControllerRevision` snapshot. Live runtime changes are detected but not applied until you roll forward. |

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: my-service
spec:
  model:
    name: my-model
  runtime:
    name: my-runtime
    autoSync: false   # pin to a snapshot instead of tracking live
```

> **Note:** Setting `autoSync: false` is required before `spec.runtime.revision` can be used - the admission webhook rejects a `revision` pin while `autoSync` is `true`, because a live-tracking service would silently ignore the pin.

## How pinning works

Once `autoSync: false` is set, the controller manages the pin through these transitions:

1. **First reconcile** - the controller resolves the live runtime spec, finds-or-creates a `ControllerRevision` for it (reusing an existing snapshot if one with the same content hash already exists), and records its name in `status.pinnedRevisionName`. The service now renders from that snapshot.
2. **Steady state** - on each reconcile the controller compares the hash of the live runtime to the pinned snapshot. If they match, nothing changes.
3. **Drift** - if the live runtime hash differs from the pinned snapshot, the controller sets the `RuntimeDrifted` status condition to `True` and **keeps rendering from the old snapshot** (it does not adopt the new runtime automatically). The service keeps running unchanged until you roll forward.

### Status fields

| Field | Meaning |
|-------|---------|
| `status.pinnedRevisionName` | Name of the `ControllerRevision` currently driving the pods. Empty when `autoSync` is `true`. |
| `status.lastRuntimeSyncToken` | The value of the `ome.io/runtime-sync` annotation the controller last acted on (used to make roll-forward one-shot). |
| `RuntimeDrifted` condition | `True` when the live runtime has drifted from the pin. Reasons include `RevisionMismatch` (live spec differs) and `RevisionMissing` (the pinned/explicit revision no longer exists). |

## Rolling forward to the latest runtime

When the runtime has changed and you want the pinned service to adopt it, set the **`ome.io/runtime-sync` annotation** to a new value. This acknowledges the drift; the controller advances the pin to a fresh snapshot of the current live runtime, updates `pinnedRevisionName`, and clears `RuntimeDrifted`.

The advance is **one-shot**: the controller records the token in `status.lastRuntimeSyncToken`, so the same value will not roll forward again. To adopt a later runtime change, set the annotation to a new value.

You can bump the annotation by hand, or use the guarded [kubectl-ome](/ome/docs/tasks/kubectl-ome) action, which checks eligibility, previews the change before patching, and emits a request ID you can wait on.

### Guarded roll-forward: `kubectl ome runtime sync` (alpha)

```bash
kubectl ome runtime sync my-service -n team-a
```

The command refuses to patch unless the service is an eligible pin right now:

- `spec.runtime.autoSync` is `false` and `status.pinnedRevisionName` resolves to a consistent snapshot;
- drift is actually reported: `RuntimeDrifted` is `True` with reason `RevisionMismatch` (a missing source runtime or missing revision is not eligible);
- `spec.runtime.revision` is not set — an explicit rollback pin is never advanced;
- no earlier sync token is still pending, and there is no in-flight rollout/canary-rollback, held-revision, or pending replica work.

It prints a preview on stderr (pinned and live runtime hashes, the predicted new revision name, sources) and asks for confirmation; pass `--yes` to skip the prompt. `--dry-run=client` runs every check locally and sends nothing; `--dry-run=server` submits the patch with `dryRun=All` so the API server validates it without persisting. Snapshot history is read from the OME control-plane namespace (`--ome-namespace`, default `ome`).

Instead of a hand-picked value, the patch sets `ome.io/runtime-sync` to a unique token (`cli-runtime-sync-<uuid>`), guarded by JSON-Patch `test` preconditions on the service's UID, `resourceVersion`, and previous annotation value. If anything in the safety snapshot changed between preview and send, the command exits with a precondition error and patches nothing; rerun it to retry.

Two caveats:

- The token asks the controller to adopt **whatever the live runtime is when it consumes the token**, not the exact spec that was previewed.
- Success means the API server **accepted the annotation**, not that the controller advanced the pin — the result reports "consumption and convergence not observed".

stdout carries a single typed `ActionResult`; with `-o json` it includes the `requestID` (a v4 UUID) used below.

### Confirming the controller acknowledged the request

`kubectl ome wait` blocks until the controller has consumed the token:

```bash
kubectl ome wait my-service -n team-a \
  --for=runtime-sync=acknowledged \
  --request-id=123e4567-e89b-42d3-a456-426614174000   # requestID from the ActionResult
```

It polls the InferenceService every 5 seconds (60s default timeout, tune with `--timeout`) and exits `0` only when the same service reports the exact token in both the `ome.io/runtime-sync` annotation and `status.lastRuntimeSyncToken`, still has a managed pin, and no longer reports a `RuntimeDrifted` condition — that is, the controller advanced the pin for this request. Exit code `2` means the state was not observed within the timeout. Acknowledgment is not proof that pods re-rendered or became ready; check `status.pinnedRevisionName` and readiness separately.

### Manual roll-forward: bump the annotation

Without the plugin, overwrite the annotation directly:

```bash
kubectl annotate isvc my-service ome.io/runtime-sync="$(date +%s)" --overwrite
```

This is a plain overwrite: nothing verifies that the service is pinned and drifted, or that a rollback or rollout is not in flight, and a timestamp token cannot be verified with `kubectl ome wait` (which requires the UUID from the guarded action). Watch `status.lastRuntimeSyncToken` and the `RuntimeDrifted` condition yourself to confirm the advance.

## Pinning to a specific revision (rollback)

To pin to a specific, already-existing snapshot - for example to roll back to a previous runtime - set `spec.runtime.revision` to the `ControllerRevision` name:

```yaml
spec:
  runtime:
    name: my-runtime
    autoSync: false
    revision: cr-my-runtime-a1b2c3d4   # pin to this exact snapshot
```

An explicit `revision` overrides drift handling. If the named revision does not exist, the controller sets `RuntimeDrifted` with reason `RevisionMissing` rather than silently falling back to the live runtime.

> **Note:** A `revision` can only point at a snapshot that already exists. Snapshots for a *new* runtime version come into existence when an InferenceService rolls forward (via `ome.io/runtime-sync`) or is first pinned. To see which snapshots exist, use `kubectl ome runtime history` (below), or query the labels directly: `kubectl -n ome get controllerrevisions -l ome.io/runtime-of=<runtime-name>`.

### Listing snapshots: `kubectl ome runtime history` (alpha)

The [kubectl-ome](/ome/docs/tasks/kubectl-ome) plugin lists the snapshots available to a service without a hand-written label query. It takes the **InferenceService**, not the runtime: the command resolves the service's runtime itself, then lists that runtime's `ControllerRevision` snapshots from the OME control-plane namespace (`--ome-namespace`, default `ome`):

```bash
kubectl ome runtime history my-service -n team-a
```

```
WINDOW    REVISION         CREATED           ROLES   CHECK   LIVE    ISSUES
C/B/1/1   cr...#91d02f4a   26-09-24T09:15Z   H       OK      MATCH   R0/G0
C/B/1/1   cr...#2130c7ba   26-09-20T14:02Z   AQRH    OK      DIFF    R0/G0
```

One row per snapshot, newest first. The default table is compact (every line fits an 80-column terminal), so the cells are coded:

| Column | Meaning |
|--------|---------|
| `WINDOW` | The observation window, repeated on every row: observation state, completeness, pages seen, pages asked. `C/B` = the listing completed, so the window is complete up to GC retention (see below); `P/I` = truncated — older snapshots exist that are not shown; `U/I` = the listing failed; `N/N` = history was not requested. |
| `REVISION` | Snapshot name. Names longer than 14 columns display as `PREFIX#DIGEST`, where the digest is an eight-hex display-only digest of the full name — not a Kubernetes identity and not the runtime content hash. |
| `CREATED` | Creation time, UTC, `YY-MM-DDTHH:MMZ`. |
| `ROLES` | Why the row is in the report: `A` = active (the snapshot currently driving the pods), `Q` = requested (`spec.runtime.revision`), `R` = reported (`status.pinnedRevisionName`), `H` = history (found by the label listing). |
| `CHECK` | Internal consistency of the snapshot: `OK`, `BAD`, or `?`. |
| `LIVE` | Content-hash relation to the current live runtime: `MATCH`, `DIFF`, `AMB` (short-hash collision), or `?`. On a drifted pin the active row shows `DIFF`. |
| `ISSUES` | `R<n>/G<n>` — issue counts for this revision and for the report as a whole. The issue codes themselves appear only in `-o wide`. |

The compact `REVISION` cell is display-only — a truncated `PREFIX#DIGEST` cannot be pasted into `spec.runtime.revision`. Use `-o wide` for full revision names, timestamps, content hashes, sources, and exact issue codes, or `-o json|yaml` for machine-readable output:

```bash
kubectl ome runtime history my-service -n team-a -o wide
```

The listing is bounded: at most two 500-item pages (1,000 revisions), with the `WINDOW` column reporting whether that window is complete or truncated. The pinned (`R`) and requested (`Q`) revisions are read directly by name, so they appear even when they have aged out of the listed window. The command is read-only and never prints raw runtime specs, ControllerRevision data, status messages, resource versions, or synchronization tokens.

## Garbage collection

OME-managed `ControllerRevision` snapshots are pruned by a background garbage collector so they do not accumulate. Per source runtime, the collector keeps the newest N snapshots and never deletes one that any InferenceService still references (`spec.runtime.revision` or `status.pinnedRevisionName`). Everything else is marked (with the `ome.io/gc-eligible-since` timestamp annotation) and deleted after a grace period.

Two controller flags tune this behavior:

| Flag | Default | Description |
|------|---------|-------------|
| `--runtime-revision-retention` | `10` | Snapshots retained per source runtime before GC. |
| `--runtime-revision-grace-period` | `24h` | How long a snapshot stays unreferenced and over-retention before deletion. |

See [Controller Configuration](/ome/docs/administration/controller-configuration) for how to set these on a running cluster.

## RBAC

The controller manager creates, updates, and deletes `ControllerRevision` objects (`apps` API group) in the OME namespace to manage pins and GC. Its ClusterRole must grant `create;get;list;watch;update;patch;delete` on `controllerrevisions`. The bundled Helm chart grants this by default.

## Reference

- Annotations and labels: [`ome.io/runtime-sync`, `ome.io/gc-eligible-since`, and the `ome.io/runtime-of*` labels](/ome/docs/reference/labels-and-annotations)
- API fields: [`ServingRuntimeRef` and `InferenceServiceStatus`](/ome/docs/reference/ome.v1beta1)
- Related concept: [Inference Service](/ome/docs/concepts/inference_service)
