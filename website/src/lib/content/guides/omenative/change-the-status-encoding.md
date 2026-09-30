---
title: Change the Instance status encoding
description: Choose how OME stores per-Instance status on InferenceReplicas, as compact ColumnarV2 columns or a DenseV1 list, and switch a cluster between them.
since: v1.3
---

[OMENative](../../concepts/omenative/overview.md) keeps the status of each [Instance](../../concepts/omenative/instances.md) on its component's InferenceReplica, as compact `ColumnarV2` columns or a `DenseV1` list. The chart sets `ColumnarV2`, so a new installation needs no change. Switch to `DenseV1` when your own tools read the list. You set one Helm value, and the manager converts the InferenceReplicas after it restarts. The same steps switch back and change the bound, `maxDecodedInstances`.

<div class="prerequisites" markdown>

- OME 1.3.0 or later, installed with the `ome-crd` and `ome-resources` charts as the releases `ome-crd` and `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- Helm and `kubectl` access to the cluster, and your `ome-resources` values file, or the values that `helm get values ome -n ome` prints.

</div>

## Choose an encoding

| Encoding | Where the rows are | Use it when |
| --- | --- | --- |
| `ColumnarV2` (default) | `status.instanceStatusColumns`, with `status.instanceStatusEncoding: ColumnarV2`. Shared values are stored once. | Most clusters. It keeps large InferenceReplicas further from the API server's size limit. |
| `DenseV1` | `status.instanceStatuses`, one entry for each Instance. | Your own tools read `status.instanceStatuses`. |

Under `ColumnarV2`, OME writes the list instead when it's no larger than the columns, or when the InferenceReplica has more rows than `maxDecodedInstances`. A `ColumnarV2` cluster therefore holds both encodings, and that's normal.

A script that reads `status.instanceStatuses` sees no rows on an InferenceReplica stored as columns, while [`kubectl ome instance`](../../reference/kubectl-ome/instance.md) reads both encodings.

### The `omenativeStatus` settings

Set them under `ome.controller.omenativeStatus` in your values file. The chart checks them, and writes them to the `omenativeStatus` key of the `inferenceservice-config` ConfigMap.

| Setting | Chart default | Description |
| --- | --- | --- |
| `instanceStatusEncoding` | `ColumnarV2` | The encoding that OME writes: `ColumnarV2` or `DenseV1`. Required. |
| `maxDecodedInstances` | `20000` | The most rows that OME decodes from one ColumnarV2 InferenceReplica. Required with `ColumnarV2`. |

The manager reads the block only when it starts. It won't start when the block is missing or invalid, or when the cluster's InferenceReplica CRD is missing or too old. Upgrade the `ome-crd` chart before `ome-resources`. If you manage the ConfigMap yourself, add the block before you upgrade from v1.2.2.

### Size `maxDecodedInstances`

Keep the chart's `20000` unless one InferenceReplica can hold more rows, counting the extra Instances of a surge, migration or replacement, and its `Failed` and `Deleting` rows. A larger one is stored as a list, where the compact form would help most.

OME stops managing a ColumnarV2 InferenceReplica with more rows than the bound. Never lower the bound below `largestColumnarV2Rows` in the [preflight report](#check-a-fleet-with-ome-status-preflight). [`kubectl ome instance`](../../reference/kubectl-ome/instance.md) reads ColumnarV2 InferenceReplicas of up to 20,000 rows, whatever the bound.

## Step 1: Set the encoding

Check the block that the cluster runs now:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.omenativeStatus}{"\n"}'
```

On a default installation, it prints:

```output
{
  "instanceStatusEncoding": "ColumnarV2",
  "maxDecodedInstances": 20000
}
```

Set the new block in your values file. This example moves to `DenseV1`. To move back, set `ColumnarV2`.

!!! warning "Keep `maxDecodedInstances` when you move to DenseV1"
    OME must decode a ColumnarV2 InferenceReplica to rewrite it as a list, which needs a bound. Without one, OME stops managing those InferenceReplicas, with `cardinality_limit`. The chart keeps `20000` unless you set it to `null`.

```yaml title="values.yaml"
ome:
  controller:
    omenativeStatus:
      instanceStatusEncoding: DenseV1
      maxDecodedInstances: 20000
```

Keep your other values in the file, because `helm upgrade` resets any value that the file leaves out. Upgrade with the chart version that you run, from the `CHART` column of `helm list -n ome`, such as `ome-resources-1.3.0`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

The upgrade restarts the manager, which reads the new block. To use `kubectl` instead, edit the ConfigMap and restart the manager, as in [Change ConfigMap settings](../operate-ome/configure-the-controller.md#tune-the-config-cache). Set the same block in your values file too, so that the next upgrade keeps it.

Wait for the new pods:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

When they're running, the command ends with:

```output
deployment "ome-controller-manager" successfully rolled out
```

Check the block that the replicas loaded:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 \
  | grep 'Configured InferenceReplica status representation'
```

When a replica starts, it logs the `instanceStatusEncoding` and `maxDecodedInstances` that it loaded, here `DenseV1` and `20000`. A bound of `0` means that none is set. A manager that rejects the block exits instead: see [The manager doesn't start](#the-manager-doesnt-start).

## Step 2: Watch the InferenceReplicas convert

Most InferenceReplicas convert soon after the restart, depending on how many you have and on the manager's [throughput settings](../operate-ome/configure-the-controller.md#tune-reconcile-throughput). A conversion changes only how the rows are stored: the Instances and their pods carry on.

List the encodings:

```bash
kubectl get inferencereplicas -A \
  -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,ENCODING:.status.instanceStatusEncoding'
```

`ENCODING` shows `ColumnarV2` for columns, and `<none>` for a list. A move to `DenseV1` is done when all rows show `<none>`:

```output
NAMESPACE   NAME             ENCODING
serving     example-engine   <none>
```

Point list-reading tools at the InferenceReplicas only then. After a move to `ColumnarV2`, some rows keep `<none>` by design. An InferenceReplica that stays `ColumnarV2` under `DenseV1` is stuck: its status [can't be decoded](#status-cant-be-decoded), or is [too large for a list](#status-writes-are-rejected-as-too-large).

A conversion also records an `InstanceStatusConverted` event on the InferenceReplica:

```bash
kubectl get events -A --field-selector reason=InstanceStatusConverted
```

Its message names the InferenceReplica and says `per-Instance status was rewritten from ColumnarV2 to DenseV1`.

If Prometheus scrapes the manager, as [Collect metrics](../operate-ome/metrics.md) sets up, these series track the change. Only the leader records them, and the encoding labels are `dense_v1` or `columnar_v2`.

| Metric | Labels | What it shows |
| --- | --- | --- |
| `ome_omenative_ir_status_conversions_total` | `from`, `to` | Conversions. It stops growing when the switch is done. |
| `ome_omenative_ir_status_writes_total` | `encoding`, `result` | Status writes by `result`, where `rejected` means too large. |
| `ome_omenative_ir_status_bytes` | `namespace`, `name`, `component`, `encoding` | The size of an InferenceReplica's last status write, to spot one nearing the size limit. |
| `ome_omenative_ir_status_codec_errors_total` | `reason` | Status that failed to encode or decode. |

## Check a fleet with ome-status-preflight

`ome-status-preflight` checks one or more clusters before and after a switch, and gives each a `GO` or `NO-GO` verdict. The check only reads. For each cluster, it needs a kubeconfig whose context can read the manager's Deployment and ConfigMap, list InferenceReplicas in all namespaces, and read OpenAPI v3 discovery.

Build it into `bin/ome-status-preflight` with Go 1.26 or later, from an OME checkout at the release that your manager runs:

```bash
make ome-status-preflight
```

The tool reads an inventory, a YAML file with no defaults that says what the clusters should run and how to reach them. Find the manager image for it:

```bash
kubectl get deployment ome-controller-manager -n ome \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}{"\n"}'
```

```output
ghcr.io/moirai-internal/ome-manager:v1.3.0
```

Set `omenativeStatus` to the block that the clusters should run: the current one before a switch, and the new one after it. This inventory checks the move to `DenseV1`:

```yaml title="fleet.yaml"
managerImage: ghcr.io/moirai-internal/ome-manager:v1.3.0
omenativeStatus:
  instanceStatusEncoding: DenseV1
  maxDecodedInstances: 20000
manager:
  namespace: ome
  deployment: ome-controller-manager
  container: manager
  configMap: inferenceservice-config
pageSize: 500
clusters:
  - name: prod-east
    kubeconfig: /etc/ome/kubeconfigs/prod-east
    context: prod-east-admin
  - name: prod-west
    kubeconfig: /etc/ome/kubeconfigs/prod-west
    context: prod-west-admin
```

Run the check:

```bash
bin/ome-status-preflight --inventory fleet.yaml
```

The report gives each cluster a verdict, with a `- <reason>: <detail>` line per problem, and counts its InferenceReplicas by encoding. When all clusters pass, it ends with:

```output
result: GO (2 clusters)
```

The command exits with `0` on GO, `1` on NO-GO, and `2` on a bad flag or inventory. Add `--json` for a machine-readable report. The tool checks the ConfigMap. The log line in [Step 1](#step-1-set-the-encoding) shows what the running managers loaded.

| Reason | What it means |
| --- | --- |
| `unreachable` | The kubeconfig or context fails to load, or the API server fails to answer. |
| `unexpected_image` | The manager runs an image other than `managerImage`, or its container is unreadable. |
| `configuration_mismatch` | The ConfigMap's block differs from the inventory's, or is missing or invalid. |
| `stale_schema` | The InferenceReplica CRD is missing, or too old for ColumnarV2. |
| `failed_page` | Listing the InferenceReplicas failed partway. |
| `columnar_v2_present` | ColumnarV2 objects remain under a `DenseV1` target. The detail names them. |
| `columnar_v2_above_bound` | A ColumnarV2 object has more rows than the inventory's bound, or there's no bound. |
| `columnar_v2_undecodable` | A stored status is damaged: see [Repair a damaged status](#repair-a-damaged-status). |

## Troubleshooting

### `helm upgrade` fails on the `omenativeStatus` values

Helm rejects a bad block before it changes the cluster, with one of these messages:

- `ome.controller.omenativeStatus.instanceStatusEncoding must be DenseV1 or ColumnarV2, got "<value>"`
- `ome.controller.omenativeStatus.maxDecodedInstances must be a positive integer, got <value>`
- `ome.controller.omenativeStatus.maxDecodedInstances is required when instanceStatusEncoding is ColumnarV2`

Fix the value, and upgrade again.

### The manager doesn't start

The new manager pods restart over and over, and `kubectl rollout status` keeps waiting. Find the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 \
  | grep -E 'Failed to initialize OMENative status configuration|status schema preflight failed'
```

| Error | What to do |
| --- | --- |
| `Failed to initialize OMENative status configuration` | The error says `inferenceservice-config is missing the required "omenativeStatus" block`, or starts with `invalid omenativeStatus config:`. Fix the block, or set it through the chart. |
| `InferenceReplica status schema preflight failed`, with `is not ColumnarV2-capable` | The InferenceReplica CRD is missing or older than the manager. Upgrade the `ome-crd` chart to your `ome-resources` version, and restart the manager. |
| `InferenceReplica status schema preflight failed`, with another error | The manager couldn't read OpenAPI v3 discovery from the API server. |

### The status can't be decoded {#status-cant-be-decoded}

OME stops managing an InferenceReplica whose stored status it can't decode, and records a Warning event:

```bash
kubectl get events -A --field-selector reason=InstanceStatusDecodeFailed
```

The message names the InferenceReplica and the reason, in `per-Instance status cannot be decoded (<reason>)`:

- `cardinality_limit`: the object is stored as ColumnarV2 with more rows than `maxDecodedInstances`, or the block sets no bound. Raise or restore the bound, as in [Step 1](#step-1-set-the-encoding).
- Any other reason, such as `coverage` or `range_syntax`: the stored status is damaged. [Repair it](#repair-a-damaged-status).

### Status writes are rejected as too large

When the API server rejects an InferenceReplica's status write as too large, OME records a `StatusSizeExceeded` Warning event, and the InferenceReplica stalls until its status fits.

```bash
kubectl get events -A --field-selector reason=StatusSizeExceeded
```

The message gives the write's size and encoding, such as `status write of <n> bytes (DenseV1)`.

- On `DenseV1`, switch to `ColumnarV2` as in [Step 1](#step-1-set-the-encoding), with a bound that covers the InferenceReplica's rows.
- During a move to `DenseV1`, an InferenceReplica may be too large to store as a list, and it stays ColumnarV2. Set the target back to `ColumnarV2`.

### Repair a damaged status

`ome-status-preflight repair` replaces one InferenceReplica's stored rows with rows that you write, and keeps the rest of its status. Use it only for a damaged status, never for `cardinality_limit`. It uses the inventory from [the fleet check](#check-a-fleet-with-ome-status-preflight), and the cluster's context must be allowed to update `inferencereplicas/status`.

!!! warning "Rows must match the Instances"
    OME acts on the rows that you write, so each row must describe its Instance as it is now.

Write a row for every Instance, as a list under `instanceStatuses` and nothing else. An Instance's pods carry its index, incarnation and revision hash as [labels](../../concepts/architecture/deployment-modes.md#pod-labels-and-environment), and a revision is named `<InferenceService>-<component>-<hash>`:

```yaml title="rows.yaml"
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

Run the repair without `--apply` first. This example repairs `example-engine`, the engine of the InferenceService `example`:

```bash
bin/ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml
```

The dry run checks your rows, and the cluster's block against the inventory's, and writes nothing. Its report starts with `InferenceReplica status repair (dry run: nothing was written)` and ends with `Re-run with --apply to write.`

If the report looks right, write it:

```bash
bin/ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml --apply
```

The report starts with `InferenceReplica status repair (applied)`. If the object changed after the tool read it, the tool refuses the write: run the dry run again. A refusal writes nothing, prints a `refused:` message, and exits with `1`. Other errors print `error:` and exit with `2`.

## Next steps

- Read what a row holds in [Where Instance status lives](../../concepts/omenative/instances.md#where-instance-status-lives).
- Scrape the manager's metrics with [Collect metrics](../operate-ome/metrics.md).
- Look up every status field of an InferenceReplica in the [API reference](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplicaStatus).
