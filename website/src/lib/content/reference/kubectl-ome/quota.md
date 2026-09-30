---
title: kubectl ome quota
description: Show the declared AcceleratorQuota tree, reported budgets and materialization, and validate the quota topology with a scriptable exit code.
since: v1.3
---

`kubectl ome quota` shows the tree of [AcceleratorQuotas](../api/ome.v1beta1.md#ome-io-v1beta1-AcceleratorQuota) that divides a cluster's accelerators among teams, and what the quota manager reports for each node. It reads only AcceleratorQuotas, so usage is as the quota manager last reported it. [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md) builds the tree that the examples show, and [How quotas work](../../guides/operate-ome/accelerator-quota.md#how-quotas-work) explains its roles and budgets.

```text
kubectl ome quota SUBCOMMAND [ACCELERATORQUOTA] [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows the budgets, capacity, materialization and conditions that the quota manager reported. |
| [`tree`](#tree) | Shows the declared tree, with each node's budgets. |
| [`validate`](#validate) | Checks the declared tree, and exits `2` when it finds a problem. |

Each subcommand takes at most one AcceleratorQuota name. AcceleratorQuotas are cluster-scoped, so `-n` has no effect. The report fits in 80 columns: a longer line is cut, and ends in `#` and 8 hex digits of a hash. `-o wide` prints each part of the report as a row of JSON, uncut, and `-o json` and `-o yaml` print the whole report, as [Output formats](overview.md#output-formats) describes. [Required RBAC](overview.md#required-rbac) lists the permissions the commands need.

The reports mark how the CLI got each value with an evidence level:

| Evidence | Meaning |
| --- | --- |
| `Declared` | Read from an AcceleratorQuota's spec. |
| `Reported` | Read from the status that the quota manager wrote. |
| `Observed` | Read from the API server by this command: the snapshot of AcceleratorQuotas. |
| `Computed` | Worked out by the CLI, such as a node's place in the tree, or a problem. |
| `Unavailable` | The status holds no value that the CLI can show. |

## `status`

```text
kubectl ome quota status [ACCELERATORQUOTA] [flags]
```

`status` shows what the quota manager last reported for each node: budgets and what's charged against them, capacity on `root`, the materialization, and the Ready, Degraded and Materialized conditions. Name an AcceleratorQuota to see only that one. For one row per budget across the tree, run [`kubectl ome get acceleratorquotas`](get.md#acceleratorquotas-columns). To read the conditions' messages and what their reasons mean, see [A node isn't Ready or Materialized](../../guides/operate-ome/accelerator-quota.md#conditions).

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

Each AcceleratorQuota prints these lines, in order of name. A budget, capacity or condition line prints once for each entry in the status:

| Line | What it shows |
| --- | --- |
| `<name>: generation=<g> observed=<o> <freshness> (Reported)` | The AcceleratorQuota, its `metadata.generation`, and the `status.observedGeneration` that the quota manager last wrote. |
| `parent=<name> (<state>); sourceGeneration=<n> (<state>)` | The parent that the quota manager confirmed, `Unavailable` on `root`, and `status.sourceGeneration`. |
| `budgets:`, `capacity:`, `projections:`, `materialization:`, `conditions:` | Whether the status holds each part, as `<state>/<reason>`. |
| `budget <resource>/<flavor> nominal=<q> admitted=<q>`, then `reserved=<q> borrowed=<q>; clusters=<state>/<reason>` | One budget from `status.budgets`. |
| `capacity <resource>/<flavor> allocatable=<q> highWater=<q>`, then `sampled=<time> (<state>); highWater is historical`, then `clusters=<state>/<reason>` | Capacity from `status.capacity`, which only `root` reports. |
| `output=<state> reason=<reason>; lastApplied=<n> <freshness>` | The materialization: `Frozen` when the node or an ancestor breaks a rule, so its Kueue objects keep their last good state, and `Unknown/Defaulted` otherwise. Then the reason, and the generation the quota manager last applied. |
| `<type>=<status> reason=<reason> <freshness>` | The `Degraded`, `Materialized` and `Ready` conditions, in that order, with the status `True`, `False` or `Unknown`. |

Budget and capacity lines show these amounts:

| Amount | What it is |
| --- | --- |
| `nominal` | The node's resolved share. |
| `admitted` | The amount admitted against the share, borrowed amounts included. |
| `reserved` | The amount that workloads with a quota reservation hold, admitted or not. |
| `borrowed` | The amount admitted above the share. |
| `allocatable` | Capacity on Ready, uncordoned nodes. |
| `highWater` | The mark that budgets are checked against. It also counts cordoned and not-Ready nodes. |
| `sampled` | When the quota manager last measured capacity. Its state is `Reported/Unverifiable`, or `Unknown` when there's no time. |

In a single-cluster tree, as in [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md), `projections:` and each `clusters=` show `Unavailable/NotReported`, and `sourceGeneration` shows `0 (Unknown)`.

The CLI can't tell a zero from a value that the quota manager never wrote, so zero prints as `Unknown/Defaulted`, and a healthy node shows `output=Unknown/Defaulted reason=Unknown`. A reason that the CLI doesn't know prints as `Other`.

A freshness compares a generation in the status with the node's generation:

| Freshness | Meaning |
| --- | --- |
| `Current` | The status has caught up. |
| `Stale` | The status is behind. |
| `Unknown` | One of the generations is 0. |
| `Inconsistent` | The status is ahead, or a generation is negative. |

A part's state and reason are one of:

| State/reason | Meaning |
| --- | --- |
| `Available/Reported` | The status holds the part. |
| `Unavailable/NotReported` | The status doesn't hold the part. |
| `Invalid/<reason>` | The CLI rejected the part, and shows none of it: `Oversize` for more than 64 entries, `UnexpectedNonRootCapacity` for capacity on a node other than `root`, and `MalformedPayload` for a value that fails its checks, for example, a time later than your machine's clock. |
| `Unavailable/NoSupportedConditions`, `Available/UnsupportedTypesOmitted` | `conditions` only: the status has no Ready, Degraded or Materialized condition, or has other types, which the CLI leaves out. |

A part with more than 16 entries shows the first 16, and its line adds `; truncated 16/<total>`. With more than 64 AcceleratorQuotas, `status` without a name shows the first 64 and says so. Name an AcceleratorQuota to see one of the others.

`-o wide`, `-o json` and `-o yaml` also show when a condition last changed, when the node froze, and when the quota manager last applied it. In `-o json` and `-o yaml`, the AcceleratorQuotas are in `content.objects`, and a budget's amounts are under `usage`.

### Examples {#status-examples}

Show what the quota manager reported for the leaf `team-alpha`:

```bash
kubectl ome quota status team-alpha
```

```output
QUOTA STATUS (reported)
Reported evidence only; no enforcement or free-capacity claim.
Snapshot: Complete; 1 items / 1 pages
team-alpha: generation=2 observed=2 Current (Reported)
  parent=ml-research (Reported); sourceGeneration=0 (Unknown)
  budgets: Available/Reported
  capacity: Unavailable/NotReported
  projections: Unavailable/NotReported
  materialization: Available/Reported
  conditions: Available/Reported
  budget nvidia.com/gpu/a100 nominal=16 admitted=12
    reserved=14 borrowed=Unknown/Defaulted; clusters=Unavailable/NotReported
  output=Unknown/Defaulted reason=Unknown; lastApplied=2 Current
  Degraded=False reason=Admitted Current
  Materialized=True reason=Admitted Current
  Ready=True reason=Admitted Current
Zero optional scalars: Unknown/Defaulted; not proof of zero usage.
Compact identities/lines may be clipped; -o wide retains safe fields.
Computed ancestry: quota tree; no reported status path exists.
```

The quota manager has caught up with generation 2 of `team-alpha`, and wrote its Kueue objects for that generation. Of the leaf's 16 A100 GPUs, 12 are admitted. Workloads with a quota reservation hold 14, so 2 are reserved but not yet admitted. `borrowed` is 0, or wasn't reported, so it prints as `Unknown/Defaulted`.

Show the capacity that the quota manager measured on `root`:

```bash
kubectl ome quota status root
```

```output
QUOTA STATUS (reported)
Reported evidence only; no enforcement or free-capacity claim.
Snapshot: Complete; 1 items / 1 pages
root: generation=1 observed=1 Current (Reported)
  parent= (Unavailable); sourceGeneration=0 (Unknown)
  budgets: Unavailable/NotReported
  capacity: Available/Reported
  projections: Unavailable/NotReported
  materialization: Available/Reported
  conditions: Available/Reported
  capacity google.com/tpu/a100 allocatable=Unknown/Defaulted highWater=#df9ecb17
    sampled=2026-09-28T11:59:00Z (Reported/Unverifiable); highWater is #f94adb91
    clusters=Unavailable/NotReported
  capacity nvidia.com/gpu/a100 allocatable=32 highWater=32
    sampled=2026-09-28T11:59:00Z (Reported/Unverifiable); highWater is #f94adb91
    clusters=Unavailable/NotReported
  output=Unknown/Defaulted reason=Unknown; lastApplied=1 Current
  Degraded=False reason=Admitted Current
  Materialized=True reason=Admitted Current
  Ready=True reason=Admitted Current
Zero optional scalars: Unknown/Defaulted; not proof of zero usage.
Compact identities/lines may be clipped; -o wide retains safe fields.
Computed ancestry: quota tree; no reported status path exists.
```

`root` reports 32 allocatable A100 GPUs, with a high-water mark of 32. The quota manager lists each configured resource on each flavor, so `google.com/tpu` appears on `a100` with 0. The 0 prints as `Unknown/Defaulted` and makes the line too long, so `#df9ecb17` is a hash, not a value. A `sampled=` line with a time is always cut, so it ends in `highWater is #<hash>`. `-o wide` shows both lines in full.

## `tree`

```text
kubectl ome quota tree [ACCELERATORQUOTA] [flags]
```

`tree` shows the tree that the AcceleratorQuotas declare in their specs: each node under its parent, with its role and budgets. Name an AcceleratorQuota to see only it, its ancestors and its descendants. Problems anywhere in the tree still print.

### Flags {#tree-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#tree-output-fields}

After its header, the report prints the nodes, then the problems. The header's last line ends in `NoProblemsDetected` or `ProblemsDetected`. When you name an AcceleratorQuota, a `Selected ancestry and descendants: <name>` line comes before the nodes.

| Line | What it shows |
| --- | --- |
| `<name> [<role>]` | A node, indented two spaces for each level below `root`. The role is `Cohort` or `ClusterQueue`. A `ClusterQueue` is a tenant, and adds `tenant=` and its own name. |
| `budget <resource>/<flavor>=<nominal> (Declared)` | A budget from the node's `spec.budgets`, with its `nominal` amount, under the node. |
| `! <node>: <reason> (Computed; whole snapshot)` | A problem in the tree. [`validate`](#validate-output-fields) lists the reasons. |

Nodes print in order of their path from `root`, such as `/root/ml-research/team-alpha`. A node that the CLI can't place under `root`, as when its parent is missing, adds `(unresolved)` and prints unindented after the others.

!!! warning "Sibling names that share a prefix can misplace nodes"
    When one sibling's name is another's followed by `-` or `.`, such as `ml` and `ml-ops`, the children of `ml` print under `ml-ops` and look like its children. `-o wide` shows each node's `parentName` and `computedPath`. This is a known bug.

The report shows only each budget's `nominal`. `-o wide`, `-o json` and `-o yaml` show each node's fields:

| Field | What it shows |
| --- | --- |
| `name`, `role`, `tenant` | As in the report. |
| `parentName` | The parent that the node names in `spec.parentRef.name`. |
| `computedPath` | The node's path from `root`. A node that the CLI can't place has none. |
| `depth` | The number of levels below `root`: `0` for `root`, and `-1` for a node that the CLI can't place. |
| `selected` | `true` for the AcceleratorQuota you name. |
| `position` | `Resolved` when the CLI placed the node under `root`, and `Unresolved` otherwise. |
| `priorityTier` | The node's `spec.priorityTier`, when it's set. Nothing acts on it in this release. |
| `budgets` | Each budget's `resourceName`, `resourceFlavor` and `nominal`, and its `borrowingLimit` and `lendingLimit` when they're set. |

`distribution`, `policy`, `policySource` and `perCluster` show `Unavailable/NotConfigured` or `[]` when they aren't set, as in the example below.

In `-o wide`, the `source` rows list every AcceleratorQuota read, including those a named `tree` hides. In `-o json` and `-o yaml`, the nodes and problems are in `content.nodes` and `content.problems`.

### Examples {#tree-examples}

Show the whole tree:

```bash
kubectl ome quota tree
```

```output
QUOTA TREE (advisory)
Declared budgets; computed ancestry; advisory only.
No admission, enforcement, controller, Kueue, or capacity claim.
Snapshot: Complete; 4 items / 1 pages; NoProblemsDetected
root [Cohort]
  ml-research [Cohort]
    budget nvidia.com/gpu/a100=24 (Declared)
    team-alpha [ClusterQueue] tenant=team-alpha
      budget nvidia.com/gpu/a100=16 (Declared)
    team-beta [ClusterQueue] tenant=team-beta
      budget nvidia.com/gpu/a100=8 (Declared)
```

The Cohort `ml-research` sits under `root` with a budget of 24 A100 GPUs, and its two leaves take 16 and 8 of them.

Show `team-alpha`, its ancestors and its descendants, with every field:

```bash
kubectl ome quota tree team-alpha -o wide
```

```output
FIELD         VALUE
report        QuotaTreeReport
collectedAt   2026-09-28T12:00:00Z
target        team-alpha
snapshot      {"scope":"Cluster","completeness":"Complete","observedPages":1,"observedItems":4,"evidence":"Observed"}
structure     NoProblemsDetected
node          {"name":"root","role":"Cohort","computedPath":"/root","depth":0,"selected":false,"position":"Resolved","topologyEvidence":"Declared","positionEvidence":"Computed","distribution":"Unavailable/NotConfigured","budgets":[]}
node          {"name":"ml-research","role":"Cohort","parentName":"root","computedPath":"/root/ml-research","depth":1,"selected":false,"position":"Resolved","topologyEvidence":"Declared","positionEvidence":"Computed","distribution":"Unavailable/NotConfigured","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"24","policy":"Unavailable/NotConfigured","policySource":"Unavailable/NotConfigured","perCluster":[],"evidence":"Declared"}]}
node          {"name":"team-alpha","role":"ClusterQueue","tenant":"team-alpha","parentName":"ml-research","computedPath":"/root/ml-research/team-alpha","depth":2,"selected":true,"position":"Resolved","topologyEvidence":"Declared","positionEvidence":"Computed","distribution":"Unavailable/NotConfigured","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"16","policy":"Unavailable/NotConfigured","policySource":"Unavailable/NotConfigured","perCluster":[],"evidence":"Declared"}]}
source        {"kind":"AcceleratorQuota","name":"ml-research","generation":1,"evidence":"Declared","collectedAt":"2026-09-28T12:00:00Z"}
source        {"kind":"AcceleratorQuota","name":"root","generation":1,"evidence":"Declared","collectedAt":"2026-09-28T12:00:00Z"}
source        {"kind":"AcceleratorQuota","name":"team-alpha","generation":2,"evidence":"Declared","collectedAt":"2026-09-28T12:00:00Z"}
source        {"kind":"AcceleratorQuota","name":"team-beta","generation":1,"evidence":"Declared","collectedAt":"2026-09-28T12:00:00Z"}
warning       {"code":"AdvisoryOnly","message":"Declared budgets and computed ancestry do not prove admission, enforcement, controller installation, Kueue integration, materialization, or available capacity."}
```

## `validate`

```text
kubectl ome quota validate [ACCELERATORQUOTA] [flags]
```

`validate` checks the tree that the AcceleratorQuotas on the cluster declare, prints it with the problems it finds, and exits `2` when it finds any. Name an AcceleratorQuota to print only its part of the tree: `validate` still checks the whole tree, and fails on a problem anywhere in it. With no `root`, it reports `RootMissing` and exits `2`. By default, the quota manager creates `root`.

The quota manager's webhook, on by default, denies a change that breaks a rule. Where it runs, `validate` mostly finds problems in AcceleratorQuotas written before the webhook ran. `validate` doesn't check the tree's depth, or budgets against capacity: [`status`](#status) shows those problems, as `DepthExceeded` and `CapacityExceeded`. To check a change before you apply it, use `kubectl apply --dry-run=server`, as [Change the tree](../../guides/operate-ome/accelerator-quota.md#change-the-tree) shows.

### Flags {#validate-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#validate-output-fields}

The report is the [`tree`](#tree-output-fields) report under the title `QUOTA VALIDATION (advisory)`, with a first line `Result: <result>; complete fetched snapshot only.`, where the result is `NoProblemsDetected` or `ViolationsDetected`. A problem's reason is one of:

| Reason | Meaning |
| --- | --- |
| `RootMissing` | No AcceleratorQuota is named `root`. |
| `RootHasParent` | `root` sets `spec.parentRef`. |
| `RootRoleInvalid` | `root` isn't a `Cohort`. |
| `ParentMissing` | The node's parent doesn't exist, or a node other than `root` sets no `spec.parentRef`. |
| `ParentCycle` | The node's `spec.parentRef` names the node itself, or its ancestors form a loop. |
| `Unreachable` | An ancestor of the node can't be placed in the tree. |
| `NodeKindInvalid` | The node's parent is a `ClusterQueue`, which takes no children. |
| `ContainmentViolated` | The children's budgets for a resource and flavor add up to more than the node's own. A node with no budget for that resource and flavor doesn't limit its children. |
| `BudgetInvalid` | A `nominal`, `borrowingLimit` or `lendingLimit` is negative or can't be read. |

In `-o json` and `-o yaml`, `content.valid` is `true` or `false`, and the problems are in `content.topology.problems`. In every format, `validate` prints the report first. When it finds a problem, it then prints `error: advisory quota validation found violations in the complete snapshot` on stderr, and exits `2`.

### Examples {#validate-examples}

Validate the tree:

```bash
kubectl ome quota validate
```

```output
QUOTA VALIDATION (advisory)
Result: NoProblemsDetected; complete fetched snapshot only.
Declared budgets; computed ancestry; advisory only.
No admission, enforcement, controller, Kueue, or capacity claim.
Snapshot: Complete; 4 items / 1 pages; NoProblemsDetected
root [Cohort]
  ml-research [Cohort]
    budget nvidia.com/gpu/a100=24 (Declared)
    team-alpha [ClusterQueue] tenant=team-alpha
      budget nvidia.com/gpu/a100=16 (Declared)
    team-beta [ClusterQueue] tenant=team-beta
      budget nvidia.com/gpu/a100=8 (Declared)
```

The tree breaks no rule, so `validate` exits `0`.

Validate a tree in which `team-alpha`'s budget was raised to 20 while no webhook checked it:

```bash
kubectl ome quota validate
```

```output
QUOTA VALIDATION (advisory)
Result: ViolationsDetected; complete fetched snapshot only.
Declared budgets; computed ancestry; advisory only.
No admission, enforcement, controller, Kueue, or capacity claim.
Snapshot: Complete; 4 items / 1 pages; ProblemsDetected
root [Cohort]
  ml-research [Cohort]
    budget nvidia.com/gpu/a100=24 (Declared)
    team-alpha [ClusterQueue] tenant=team-alpha
      budget nvidia.com/gpu/a100=20 (Declared)
    team-beta [ClusterQueue] tenant=team-beta
      budget nvidia.com/gpu/a100=8 (Declared)
! ml-research: ContainmentViolated (Computed; whole snapshot)
```

The leaves add up to 28 GPUs, more than the 24 of `ml-research`, so the problem names `ml-research`, and `validate` exits `2`.

Validate the same tree, showing only `team-beta`:

```bash
kubectl ome quota validate team-beta
```

```output
QUOTA VALIDATION (advisory)
Result: ViolationsDetected; complete fetched snapshot only.
Declared budgets; computed ancestry; advisory only.
No admission, enforcement, controller, Kueue, or capacity claim.
Snapshot: Complete; 4 items / 1 pages; ProblemsDetected
Selected ancestry and descendants: team-beta
root [Cohort]
  ml-research [Cohort]
    budget nvidia.com/gpu/a100=24 (Declared)
    team-beta [ClusterQueue] tenant=team-beta
      budget nvidia.com/gpu/a100=8 (Declared)
! ml-research: ContainmentViolated (Computed; whole snapshot)
```

`team-beta` breaks no rule itself, but `validate` checks the whole tree, so it still reports the problem at `ml-research` and exits `2`.

## Errors {#errors}

A command that fails prints nothing to stdout, prints `error: <message>` on stderr, and exits `1`. The messages you're likely to see:

| Message | Cause |
| --- | --- |
| `AcceleratorQuota name is invalid` | The name isn't a valid Kubernetes object name. |
| `quota diagnostic accepts at most one AcceleratorQuota name`, `quota tree accepts at most one AcceleratorQuota name` | You gave more than one name. |
| `AcceleratorQuota client unavailable` | The CLI can't build a client from your kubeconfig. |
| `AcceleratorQuota target unavailable: NotFound` | `status NAME`: the AcceleratorQuota, or the AcceleratorQuota CRD, doesn't exist. |
| `AcceleratorQuota target was not found in the complete snapshot` | `tree NAME` or `validate NAME`: the AcceleratorQuota doesn't exist. |
| `AcceleratorQuota snapshot unavailable: Forbidden` | The API server refused the request. Check your credentials and [Required RBAC](overview.md#required-rbac). |
| `AcceleratorQuota snapshot unavailable: UnsupportedAPI` | The cluster has no AcceleratorQuota CRD, as when its ome-crd chart predates v1.3. |
| `AcceleratorQuota snapshot unavailable: TimedOut` | The read took more than 10 seconds, or the API server timed out. |
| `AcceleratorQuota snapshot unavailable: <reason>` | Another read failure: `Canceled` on Ctrl-C, `MalformedPayload` for an empty response, or `Unreadable`. |

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every subcommand that finishes, except `validate` when it finds a problem. |
| `1` | General error | Every subcommand, when it fails. [Errors](#errors) lists the messages. |
| `2` | Assertion unmet | `validate`, when it finds a problem, after it prints the report. |

## Related guides

- [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
