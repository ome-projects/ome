---
title: "Accelerator Quota"
linkTitle: "Accelerator Quota"
weight: 10
description: >
  Install the ome-quota-manager and author an AcceleratorQuota tree that OME renders into Kueue GPU budgets on a single cluster.
---

The `AcceleratorQuota` API turns accelerator capacity policy — which team may
hold how many chips of which hardware class — into Kubernetes objects. Each
`AcceleratorQuota` custom resource is one node of a single-rooted quota tree:
`Cohort` nodes group other nodes, and `ClusterQueue` nodes are leaves that are
themselves tenants and carry a budget per `(resource, flavor)` pair, which is
how [Kueue](https://kueue.sigs.k8s.io/) keys quota. The `ome-quota-manager`
renders that tree into stock Kueue objects: a Kueue `Cohort` per grouping, a
Kueue `ClusterQueue` per leaf, and a `LocalQueue` per enrolled namespace.

This page covers the **single-cluster** path: the tree is authored on the same
cluster that serves the workloads, and one quota manager both validates and
renders it. The API and chart also carry fields for a multi-cluster management
plane that projects per-cluster shares onto a fleet (`spec.distribution`,
`spec.budgets[].perCluster`, the chart's `management` mode, `projection` and
`remoteAccess` values). Multi-cluster reconciliation is still in development;
do not build on it yet.

## Architecture

`ome-quota-manager` is a separate Deployment from `ome-manager`, on purpose:
rendering the tree needs cluster-wide write on Kueue's `ClusterQueue` and
`Cohort`, and that grant should not ride the ServiceAccount that also runs the
InferenceService reconcilers and the fail-closed pod mutating webhook. Running
it separately keeps the blast radius of the quota plane to the quota plane, and
lets you scale and roll it independently of serving.

The manager runs three things on a single cluster (`mode: workload`):

- **The AcceleratorQuota controller.** It assembles every node on the cluster
  into a tree, re-runs the invariant checks the admission webhook runs
  (concurrent admissions can violate them even though each write was
  individually valid), writes each node's status, and renders the tree into
  Kueue.
- **Capacity derivation.** It sums the cluster's schedulable accelerators —
  chips on `Ready`, uncordoned nodes, attributed to Kueue `ResourceFlavor`s by
  their node labels — and records the result on the reserved root node's
  status, so budgets are checked against hardware that actually exists.
- **A validating webhook.** `failurePolicy=Fail`, gating `CREATE`, `UPDATE`
  and `DELETE` of `acceleratorquotas`. It turns a tree-breaking write into a
  rejected `kubectl apply` instead of a `Degraded` condition minutes later. It
  gates only this one cluster-scoped CRD, so a webhook outage blocks quota
  edits, never serving pods.

## Prerequisites

- **Kueue.** OME is built against Kueue v0.19 and writes the
  `kueue.x-k8s.io/v1beta2` API (`Cohort`, `ClusterQueue`, `LocalQueue`,
  `ResourceFlavor`). See the
  [Kueue installation guide](https://kueue.sigs.k8s.io/docs/installation/).
- **ResourceFlavors.** OME references flavors and does not create them or own
  node labeling. Define a `ResourceFlavor` per hardware class (for example
  `a100`, `h100`) before budgeting against it: a budget naming a flavor that
  does not exist on the cluster is not materialized and reports
  `FlavorMissing`.
- **The AcceleratorQuota CRD.** The `ome-crd` chart ships it. On a cluster
  without `ome-crd`, set `quotaManager.crd.install=true` so this chart installs
  it — but never do both: Helm's ownership check fails for whichever release
  claims the cluster-scoped CRD second. Without the CRD the manager crash-loops.
- No cert-manager is needed: by default the webhook generates and rotates its
  own serving certificate in process. Set
  `quotaManager.webhook.internalCertManagement=false` to have cert-manager
  supply it instead.

## Install ome-quota-manager

The chart lives at `charts/ome-quota-manager`. `quotaManager.mode` is required
— the chart fails to render without it, because there is no safe default. For
a single cluster, use `workload`.

Materialization (actually writing Kueue objects) is off until
`quotaManager.materialize.enrolledNamespaces` is set. The enrolled namespaces
are the namespaces this cluster serves budgeted workloads from: every leaf gets
a `LocalQueue` named after it in each of them, and every rendered
`ClusterQueue` admits exactly this set. This is deploy configuration rather
than a field on the CR because it describes the cluster, not the budget — many
tenants can share one namespace. Changing it requires a restart of the
Deployment.

```yaml
# quota-values.yaml
quotaManager:
  mode: workload
  materialize:
    enrolledNamespaces:
      - ml-serving
```

```bash
helm install ome-quota-manager ./charts/ome-quota-manager \
  --namespace ome \
  -f quota-values.yaml
```

If you install straight from the source tree, the default image reference is
unqualified and the pod will sit in `ImagePullBackOff`: either set
`global.hub` to your registry or side-load the image (the chart's install
notes show the exact commands for kind).

Verify the rollout, and that the webhook's CA bundle is real rather than the
rendered placeholder:

```bash
kubectl -n ome rollout status deployment/ome-quota-manager

kubectl get validatingwebhookconfiguration ome-quota-manager.ome.io \
  -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | base64 -d \
  | openssl x509 -noout -subject
```

A `Ready` pod means admission is live: with internal cert management the pod
stays unready until the CA is generated and injected.

Other values worth knowing about (see `charts/ome-quota-manager/values.yaml`
for the full commentary):

| Value | Default | What it does |
| --- | --- | --- |
| `quotaManager.mode` | *(required)* | `workload` renders the local tree into Kueue on this cluster. |
| `quotaManager.capacity.resources` | `google.com/tpu`, `nvidia.com/gpu` | Extended resource names that count as accelerator capacity, written in full. A vendor left off contributes no capacity, and a budget against it reports `CapacityExceeded`. Empty disables capacity derivation. |
| `quotaManager.capacity.hysteresisPercent` | `10` | How far observed capacity must fall below the recorded high-water mark before the mark follows it down. `0` disables damping. |
| `quotaManager.materialize.enrolledNamespaces` | `[]` | Namespaces served from this cluster. Empty disables materialization, so you can observe the tree before enforcing it. |
| `quotaManager.materialize.coverResources` | `cpu: 16M`, `memory: 16Pi`, `ephemeral-storage: 16Pi` | Non-accelerator ceilings every rendered ClusterQueue funds. Not a budget — see below. Emptying the map disables materialization. |
| `quotaManager.materialize.fieldManager` | component name | Owns the applied Kueue objects and is the value of the managed-by label. Two quota managers on one cluster must not share it. |
| `quotaManager.maxTreeDepth` | `5` | Greatest permitted distance from the root, in edges. `0` disables the check. |
| `quotaManager.resyncInterval` | `10m` | Re-runs the tree checks on an otherwise-idle cluster. |
| `quotaManager.crd.install` | `false` | Install the CRD from this chart. Only for clusters without `ome-crd`. |
| `quotaManager.webhook.enabled` | `true` | Serve the validating webhook. Disable only to debug a wedged cluster whose webhook certs or Service are broken. |

**Cover resources are a ceiling, not a budget.** Kueue refuses to admit a
workload requesting a resource its queue does not cover, and every serving pod
requests `cpu` and `memory` — so a queue budgeted only for accelerators would
admit nothing and report the reason nowhere. The defaults are deliberately
enormous; the accelerator entry is what actually limits a tenant. Sizing cuts
both ways and neither failure is loud: too low is a real limit on every tenant
(pods stay pending with nothing in an error state to say why — and cpu
suffixes bite: `9999m` is about 10 cores, not 9999), while Kueue sums a
cohort's quota over its children as `int64`, so a very large cover multiplied
by many sibling leaves can overflow.

## The reserved root

Every tree descends from a single parent-less node named `root`. On a workload
cluster you do not create it: the controller creates it automatically (a bare
`Cohort` with no `parentRef` and no budgets) as soon as capacity derivation is
enabled, and maintains the cluster's observed accelerator capacity in its
status:

```bash
kubectl get acceleratorquota root -o jsonpath='{.status.capacity}' | jq
```

Each entry reports, per `(resource, flavor)` pair:

- `allocatable` — the currently schedulable quantity: the flavor's resource
  summed over `Ready`, uncordoned nodes matching the flavor's node labels.
- `highWaterMark` — the greatest `allocatable` observed. Budget checks compare
  against the mark rather than the instantaneous value, so capacity that
  shrinks for reasons unrelated to entitlement — a drain, a cordon, a rolling
  node upgrade, a device-plugin restart — never marks a budget `Degraded`. The
  mark is lowered only once the observed value stays below it by more than the
  configured hysteresis band. Growth is always believed immediately.
- `observedAt` — when the value was last sampled.

## Author the tree

Each node is one cluster-scoped CR. Names are capped at **63 characters**
because a node's name is copied verbatim into a label value on every Kueue
object it materializes. A `Cohort` groups; a `ClusterQueue` is a leaf, must
set `parentRef`, and must carry at least one budget. A Cohort's budgets are an
authoring guardrail only — the number the containment check
(`parent ≥ sum(children)`) is enforced against — and may carry only
`resourceName`, `resourceFlavor` and `nominal`.

```yaml
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata:
  name: ml-research
spec:
  role: Cohort
  parentRef:
    name: root
  # Guardrail: children may not budget more than this in total.
  budgets:
    - resourceName: nvidia.com/gpu
      resourceFlavor: a100
      nominal: "24"
---
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata:
  name: team-alpha
spec:
  role: ClusterQueue
  parentRef:
    name: ml-research
  budgets:
    - resourceName: nvidia.com/gpu
      resourceFlavor: a100
      nominal: "16"
---
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata:
  name: team-beta
spec:
  role: ClusterQueue
  parentRef:
    name: ml-research
  budgets:
    - resourceName: nvidia.com/gpu
      resourceFlavor: a100
      nominal: "8"
      # Allow bursting up to 4 chips above nominal by borrowing idle
      # sibling capacity, and lend up to 4 while idle. See the warning below.
      borrowingLimit: "4"
      lendingLimit: "4"
```

The webhook validates each write against the whole tree — a bad `parentRef`, a
containment bust, an invalid role change — and rejects it at admission. You can
try a write without dirtying the tree; the webhook has no side effects, so a
server dry-run still runs the check:

```bash
kubectl apply --dry-run=server -f quota-tree.yaml
```

A leaf may also set `spec.priorityTier`, naming the Kueue
`WorkloadPriorityClass` used as the default for the leaf's workloads. It is a
default, not a partition — a workload declaring its own priority keeps it —
and OME references the class without creating it.

Some spec changes are deliberately constrained:

- `parentRef` is mutable. Re-parenting updates the materialized ClusterQueue's
  cohort in place, so the queue, its LocalQueues, and its admitted workloads
  all survive.
- `role` may only change under a drained-leaf / childless-grouping guard,
  because the change deletes the underlying Kueue object.

Inspect the nodes with the built-in short name and print columns:

```bash
kubectl get aq
```

The default view shows each node's role, parent, first budget entry
(resource, flavor, nominal, admitted) and its `Ready`/`Degraded` conditions;
`-o wide` adds the borrowed figure. Only the first budget entry fits a row —
use `kubectl ome quota tree` (below) for the full picture.

### Warning: borrowing overage is not currently reclaimable

`borrowingLimit` turns `nominal` from a ceiling into a floor plus overage.
OME sets no preemption policy on the rendered queues, and Kueue's default is
to never reclaim within a cohort — so a borrower holds what it took until its
own work finishes, and a leaf that bursts can leave a sibling waiting for the
share that sibling was guaranteed. Conversely, when `borrowingLimit` is unset
the rendered queue carries an explicit borrowing limit of **zero**: OME's
`nominal` means "this leaf's ceiling", while Kueue reads an absent limit as
unbounded borrowing.

## What gets rendered into Kueue

For the example tree above, with `ml-serving` enrolled, the manager applies:

- Kueue `Cohort` `ml-research` (and one for `root`) — pure topology with no
  `resourceGroups`. A Cohort node's guardrail budget deliberately does not
  materialize: Kueue's cohort quota is additive, so writing it would hand out
  quota on top of what the children already contribute.
- Kueue `ClusterQueue` `team-alpha` and `team-beta`, each with `cohortName`
  set to its parent and exactly one resource group covering the configured
  cover resources plus every budgeted resource, with `nominalQuota` taken from
  the budget. The queue's namespace selector admits exactly the enrolled
  namespaces.
- Kueue `LocalQueue` `team-alpha` and `team-beta` in `ml-serving`, each
  pointing at its ClusterQueue.

A workload charges a leaf by carrying the leaf's name in the standard
`kueue.x-k8s.io/queue-name` label, resolved through the LocalQueue in its own
namespace. The LocalQueue is a mandatory pointer, not a convenience: Kueue has
no way to name a ClusterQueue directly. Tenancy is not namespace-scoped — any
leaf's workloads may appear in any enrolled namespace.

Every object the manager applies carries two labels:
`ome.io/quota-managed-by` (the field manager, which is how the manager finds
its own objects) and `ome.io/accelerator-quota` (the owning node's name).
Objects without the managed-by label are **never written or garbage-collected**
— hand-provisioned Kueue queues coexist untouched, and if a node's rendered
object name is already taken by one the manager does not own, the manager
refuses to adopt it and reports `ObjectConflict` instead.

Deleting an `AcceleratorQuota` is the only path that removes its Kueue
objects: a finalizer (`ome.io/accelerator-quota`) holds the deletion until the
node's ClusterQueue and LocalQueues are reaped. No freeze or invariant
violation ever deletes them.

## Conditions

Each node reports three conditions. `Ready` and `Degraded` are written as a
mutually exclusive pair, so a reader never has to reconcile two conditions
that disagree.

| Condition | True means |
| --- | --- |
| `Ready` | The node's position in the tree is valid and its materialization matches the resolved budget. |
| `Degraded` | A computed invariant the controller re-checks at reconcile is violated. While it is True the node's last-good materialization is **frozen** — no Kueue objects are written or removed for it — so a misauthored parent never takes its children's admitted workloads down. |
| `Materialized` | Every Kueue object this node owns matches the resolved budget. Only stamped when materialization is enabled. |

A node frozen by an ancestor's violation reports the ancestor's cause in its
own condition message, so you fix the ancestor rather than hunting. The freeze
bookkeeping is durable in `status.materialization` (`frozen`, `frozenAt`,
`reason`, `lastAppliedGeneration`): what you are looking at in Kueue is the
output built from `lastAppliedGeneration`.

Condition reasons you will see on a single cluster:

| Reason | Meaning | What to do |
| --- | --- | --- |
| `Admitted` | Tree checks pass and materialization is current. | Nothing. |
| `ParentMissing` | `spec.parentRef` does not resolve to an existing node. | Create the parent or fix the reference. |
| `ParentCycle` | The `parentRef` chain loops. | Break the cycle. |
| `Unreachable` | The node itself is sound but an ancestor is missing or looping, so it has no position and must not materialize. | Fix the ancestor. |
| `NodeKindInvalid` | Structure contradicts the declared role: a leaf named as a parent, a leaf with no budget, or a grouping carrying leaf-only fields. | Fix the spec. |
| `DepthExceeded` | The node is further from the root than `maxTreeDepth`. | Flatten the tree or raise the bound. |
| `ContainmentViolated` | A parent's nominal for some `(resource, flavor)` pair is below the sum of its children's — a state two concurrent admissions can reach without either write being individually invalid. | Raise the parent's guardrail or shrink the children. |
| `CapacityExceeded` | A budget exceeds observed capacity (the high-water mark) beyond the hysteresis band. Also raised when the resource is missing from `capacity.resources` and so measures as zero. | Shrink the budget, add hardware, or list the resource in `quotaManager.capacity.resources`. |
| `FlavorMissing` | A budget names a `ResourceFlavor` that does not exist on this cluster; that budget is not materialized (an inactive ClusterQueue would admit nothing and report the reason only in Kueue's own status). | Create the flavor or fix the budget. |
| `Frozen` | Materialization is held at its last-good state because an invariant is violated. | Fix the underlying `Degraded` reason. |
| `MaterializationFailed` | The node's objects could not be written — an API error, a throttled write. Transient; clears on the next successful pass. | Usually nothing. |
| `ObjectConflict` | The node's rendered object name is already taken by an object this manager does not own. Adoption is refused; the node materializes nothing. Does **not** clear on its own. | Remove the conflicting object or rename the node. |

Beyond conditions, each node's `status.budgets` reports the resolved
allowance and the usage rolled up from the materialized queues per
`(resource, flavor)` pair: `nominal`, `admitted` (currently admitted,
including anything borrowed), `reserved` (held by workloads carrying a quota
reservation, admitted or not — never below `admitted`, and the gap is work
that owns the chips but has not started), and `borrowed` (admitted above the
nominal share). Cohorts and the root report the roll-up of the leaves beneath
them.

## Inspect with kubectl ome quota

The [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/) has a read-only
`quota` family:

```bash
# Tree view: ancestry, roles, budgets, per-node problems.
# An optional name selects that node's ancestors and descendants.
kubectl ome quota tree
kubectl ome quota tree team-alpha -o wide

# Reported budgets, capacity and materialization.
kubectl ome quota status
kubectl ome quota status team-alpha

# Assert the whole tree; exits 2 when advisory checks find violations,
# so it slots into CI. An empty cluster lacks the reserved root and fails.
kubectl ome quota validate
```

All three accept `-o table|wide|json|yaml`. The reports are advisory
snapshots of the CRs: they do not prove admission, enforcement, or Kueue
integration, and collection is bounded (at most 1000 objects within a
10-second deadline). The `get`/`list` rules on `ome.io` resources in the
plugin's baseline reader role already cover them.

## RBAC

Authoring the tree needs `create`/`update`/`delete` on the cluster-scoped
`acceleratorquotas.ome.io` resource — capacity admins only; `status` is
written solely by the quota controller. The chart's own RBAC is deliberately
conditional: the Node and ResourceFlavor read grants exist only while capacity
derivation is configured, and the cluster-wide Kueue write grants only while
materialization is (that is, while `enrolledNamespaces` and `coverResources`
are both non-empty) — so a cluster can observe its quota tree long before it
enforces one.
