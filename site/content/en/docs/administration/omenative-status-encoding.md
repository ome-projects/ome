---
title: "InferenceReplica Status Encoding"
linkTitle: "IR Status Encoding"
weight: 13
date: 2026-09-27
description: >
  Configure the required omenativeStatus block that selects how the manager stores InferenceReplica per-Instance status, verify a DenseV1 ↔ ColumnarV2 transition with ome-status-preflight, and roll back safely.
---

The OME manager stores the per-Instance status of every `InferenceReplica` in
one of two representations:

- **DenseV1** — the dense `status.instanceStatuses` list, one entry per
  Instance. This is the logical shape every consumer works with, and the
  rollback target.
- **ColumnarV2** — the `status.instanceStatusColumns` payload under the
  `status.instanceStatusEncoding: ColumnarV2` marker: the same rows grouped
  into columns, which serialize much smaller for large, uniform fleets.

Both representations decode to the same logical rows in the same order; the
choice is purely about how the object is stored in etcd and how close a large
InferenceReplica gets to the API server's request-size limit.

Which representation the manager **writes** is selected by the
`omenativeStatus` block of the manager configuration. The block is
**required**: a manager with no value, or an unknown one, refuses to start,
whether or not any InferenceReplica exists in the cluster. This page covers
the block itself, how a transition between representations behaves, how to
verify one with the `ome-status-preflight` tool, and how to roll back to
DenseV1 safely.

Field-by-field semantics of the InferenceReplica status API itself are in the
generated API reference; they are not repeated here.

## The `omenativeStatus` configuration block

The block is the `omenativeStatus` key of the `inferenceservice-config`
ConfigMap in the OME controller namespace. It is read **once at manager
startup**; changing it requires a manager restart. The value is a single
strict JSON object — an unknown field, a duplicate field, or trailing content
is a startup error, not a warning:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  omenativeStatus: |-
    {
      "instanceStatusEncoding": "ColumnarV2",
      "maxDecodedInstances": 20000
    }
```

With the `ome-resources` Helm chart, the block is rendered from
`ome.controller.omenativeStatus` in `values.yaml`; the chart refuses to render
an invalid combination at `helm install`/`helm upgrade` time:

```yaml
ome:
  controller:
    omenativeStatus:
      instanceStatusEncoding: ColumnarV2
      maxDecodedInstances: 20000
```

| Field | Description |
|-------|-------------|
| `instanceStatusEncoding` | The status **write target**: `ColumnarV2` or `DenseV1`. Required; the binary carries no default. Under `ColumnarV2`, every status write builds both representations and keeps the columns **only when they are strictly smaller** than the dense list — a tie keeps the dense list, so a healthy ColumnarV2 fleet legitimately holds a mix of both stored forms. Under `DenseV1`, every write is the dense list. |
| `maxDecodedInstances` | The **fail-closed decode ceiling**: the largest number of per-Instance rows a stored ColumnarV2 payload may expand to. A positive integer, **required** under `ColumnarV2`. Optional under `DenseV1`, where leaving it unset means no ColumnarV2 payload can be decoded at all — see the rollback section before removing it. |

The chart and the reference `config/configmap/inferenceservice.yaml` currently
default to `ColumnarV2` with `maxDecodedInstances: 20000`. Earlier builds
shipped `DenseV1` as the default, so upgrading an existing installation across
that flip is exactly the transition described below.

Beyond parsing the block, the manager runs a **schema preflight** at startup:
it verifies through OpenAPI v3 discovery that the *served* InferenceReplica
schema carries the ColumnarV2 representation union (the marker and columns
fields and the CEL rules that keep them mutually exclusive with the dense
list). A manager that can decode columns refuses to start against a cluster
whose CRDs cannot store or validate them — so **upgrade the `ome-crd` chart
before the manager image**, in every cluster.

### Sizing `maxDecodedInstances`

The bound is not a tuning knob; it is your deployment profile's ceiling on
transient rows in one InferenceReplica: desired replicas **plus** surge,
migration, replacement, and retained terminal rows. Two rules follow from its
fail-closed nature:

- **Raise it deliberately, before any InferenceReplica can reach it.** A
  stored ColumnarV2 payload with more rows than the bound fails closed: the
  manager refuses to decode the object rather than acting on a partial row
  set.
- **Never lower it below the largest stored ColumnarV2 row count.** The
  preflight report's `largestColumnarV2Rows` field is exactly the number a
  replacement bound must still fit.

The default of `20000` is four times the 5,000-row fixtures this repository
qualifies the codec against: a full surge of the largest qualified fleet, with
room for retained rows.

## How a transition behaves

The manager converts each object to the configured target **as it reconciles
it**. When an object's stored representation differs from the target, a
transition gate rewrites the representation first — a representation-only
write with the logical rows unchanged and no lifecycle effect on that pass —
and requeues, so the next pass starts from the converged object. Under a
ColumnarV2 target, a DenseV1 object is rewritten only when the columns would
actually be strictly smaller; otherwise it stays dense and no write happens.

Each committed conversion is observable:

- a Normal event, reason `InstanceStatusConverted`, on the InferenceReplica:
  *"per-Instance status was rewritten from X to Y (N Instances); lifecycle
  reconciliation resumes on the next pass"*;
- the `ome_omenative_ir_status_conversions_total{from,to}` counter.

There is no background sweep: an object that is never reconciled is never
converted. This is why "wait for conversions to finish" is verified with the
preflight census below rather than assumed after a fixed delay.

### What raw readers see

A ColumnarV2-stored object carries **no** `status.instanceStatuses` rows;
`kubectl get inferencereplica -o yaml` shows `instanceStatusEncoding:
ColumnarV2` and the `instanceStatusColumns` payload instead. Any tooling of
your own that reads the dense list directly from the raw object will see an
empty row set on such objects until it decodes the columns.

### Metrics

| Metric | Labels | Meaning |
|--------|--------|---------|
| `ome_omenative_ir_status_conversions_total` | `from`, `to` | Committed representation-only writes (`dense_v1`, `columnar_v2`). |
| `ome_omenative_ir_status_writes_total` | `encoding`, `result` | Status write attempts and terminal results (`attempt`, `committed`, `confirmed`, `conflict`, `rejected`, `error`). |
| `ome_omenative_ir_status_bytes` | `namespace`, `name`, `component`, `encoding` | Serialized status bytes of the most recent write attempt, per InferenceReplica — locates an object approaching the API request-size limit before a write is rejected. |
| `ome_omenative_ir_status_codec_errors_total` | `reason` | Codec failures by fixed-catalog reason (for example the cardinality limit). |

## Verifying with `ome-status-preflight`

`ome-status-preflight` is the operator tooling around a representation
transition. Its root command is a **read-only go/no-go check** you run before
and after a transition; it never writes to a cluster. Build it with:

```bash
make ome-status-preflight   # pure Go, no Rust toolchain needed → bin/ome-status-preflight
```

The command takes a YAML **inventory** — your attestation of the fleet: every
cluster to check, the manager image the fleet must run, where the manager and
its configuration live, and the `omenativeStatus` block every manager must
have loaded. Every field is explicit; the tool supplies no defaults, parses
the file strictly, and never uses the ambient kubeconfig or its current
context:

```yaml
# fleet.yaml
managerImage: ghcr.io/moirai-internal/ome-manager:v1.2.2
omenativeStatus:
  instanceStatusEncoding: ColumnarV2
  maxDecodedInstances: 20000
manager:
  namespace: ome
  deployment: ome-controller-manager
  container: manager
  configMap: inferenceservice-config
pageSize: 500
clusters:
  - name: prod-east
    kubeconfig: /secrets/kubeconfigs/prod-east
    context: prod-east-admin
  - name: prod-west
    kubeconfig: /secrets/kubeconfigs/prod-west
    context: prod-west-admin
```

```bash
ome-status-preflight --inventory fleet.yaml          # human-readable report
ome-status-preflight --inventory fleet.yaml --json   # machine-readable, for the audit trail
```

For each cluster the command reads, directly and uncached: the manager
Deployment image, the `omenativeStatus` block (parsed with the manager's own
strict parser), the served InferenceReplica schema through OpenAPI discovery,
and **every InferenceReplica in pages**, classifying each stored
representation with the same codec the manager uses, under the inventory's
bound. It reports go or no-go per cluster with a reason for every failure:

| Reason | Meaning |
|--------|---------|
| `unreachable` | The cluster, or one of its endpoints, could not be read. |
| `failed_page` | An InferenceReplica list page failed, so the census is incomplete. |
| `unexpected_image` | The manager Deployment is missing, has no manager container, or runs an image other than `managerImage`. |
| `configuration_mismatch` | The `omenativeStatus` block is missing, invalid, or differs from the inventory. |
| `stale_schema` | The served InferenceReplica schema fails the manager's startup schema preflight. |
| `columnar_v2_present` | ColumnarV2 objects remain while the target is DenseV1 — the rollback is not finished. |
| `columnar_v2_above_bound` | A ColumnarV2 object has more rows than the expected `maxDecodedInstances` allows. |
| `columnar_v2_undecodable` | A stored representation fails the codec for any other reason. |

The per-cluster `replicas:` line summarizes the census, including
`largestColumnarV2Rows` — the floor below which the bound must never be
lowered. Exit status is `0` on GO, `1` on NO-GO, `2` on a usage or inventory
error, and the report carries the SHA-256 digest of the inventory file so the
evidence names the exact attestation it was produced from.

## Enabling ColumnarV2 (or upgrading across the default flip)

1. **Upgrade CRDs first** (`ome-crd` chart) in every cluster, then the manager
   image. A run of the preflight flags a cluster whose served schema is stale
   (`stale_schema`) before a manager restart would fail on it.
2. **Run the preflight** with an inventory attesting the intended end state
   (target `ColumnarV2`, the chosen bound, the new image). Resolve every
   no-go reason — in particular `columnar_v2_above_bound`, which means the
   chosen bound is already too small for an existing object.
3. **Set the block and restart the manager** — via
   `ome.controller.omenativeStatus` on `helm upgrade`, or by editing the
   ConfigMap and restarting the Deployment. The block is read once at
   startup.
4. **Watch conversions happen**: `InstanceStatusConverted` events and
   `ome_omenative_ir_status_conversions_total{from="dense_v1",to="columnar_v2"}`.
   Dense objects whose columns would not be smaller are intentionally left
   dense; that is not a stuck transition.
5. **Re-run the preflight** and archive the `--json` report.

## Rolling back to DenseV1 safely

Rolling back is a flip of the target followed by **waiting for the
conversions to finish** — with three rules that fail closed if skipped:

1. **Keep `maxDecodedInstances` set.** The manager must still *decode* every
   stored ColumnarV2 object in order to rewrite it as DenseV1; a DenseV1
   target with no bound fails closed on every ColumnarV2 object instead of
   converting it. Remove the bound only after the census shows zero
   ColumnarV2 objects.
2. **Flip `instanceStatusEncoding` to `DenseV1`** and restart the manager
   (same mechanics as above). As each ColumnarV2 object is reconciled, the
   transition gate rewrites it as DenseV1 before any lifecycle pass touches
   it.
3. **Verify completion before any further rollback step.** Run the preflight
   with an inventory expecting the DenseV1 target: while any ColumnarV2
   object remains, the report is NO-GO with `columnar_v2_present` naming each
   object; GO means the fleet is fully dense. The
   `ome_omenative_ir_status_conversions_total{from="columnar_v2",to="dense_v1"}`
   counter settling corroborates it, but the preflight census is the
   authoritative check — an object that is never reconciled is never
   converted.

Only after a GO report may you:

- **roll the manager image back** to a build that predates the ColumnarV2
  schema. The manager image must **never** be rolled back to such a build
  while any object is still stored as ColumnarV2: an older manager cannot
  decode those objects, and to any raw reader they present an empty dense row
  set.
- **remove `maxDecodedInstances`** from a DenseV1 configuration, if you want
  the strictest posture (any ColumnarV2 payload that somehow appears then
  fails closed).

## Break-glass: the `repair` subcommand

`ome-status-preflight repair` is the explicit **break-glass writer** for one
object's stored per-Instance representation — for example an object the
preflight reports as `columnar_v2_undecodable`. It is **dry-run unless
`--apply` is given**:

```bash
# Report what would change; nothing is written.
ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml

# Perform the single precondition-guarded write.
ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml --apply
```

The replacement file holds **only** the complete DenseV1 row set and nothing
else:

```yaml
# rows.yaml
instanceStatuses:
- index: 0
  incarnation: 1
  phase: Ready
  runningRevision: example-engine-2f32f6fe
  podCount: 1
  servingPodCount: 1
  availablePodCount: 1
  admitted: true
```

The command reads the live object raw — without decoding the payload it is
about to replace — validates the replacement with the manager's own codec (a
nonempty DenseV1 `instanceStatuses` list, no marker, no columns, no other
status field, within the configured row bound), reads the cluster's
`omenativeStatus` block and **requires it to match the inventory**, and
selects the representation the cluster's configured target selects. Every
other status field is preserved from the live object; no lifecycle mutation
can be expressed through a repair. With `--apply` it performs exactly one
status write with the live `resourceVersion` as precondition and no conflict
retry — if the object changed since the read, the write is refused and
nothing changes; repeat from a fresh dry run. A refusal exits `1` with a
`refused: ...` message; transport or usage failures exit `2`.
