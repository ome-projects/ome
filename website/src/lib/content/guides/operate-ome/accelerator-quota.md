---
title: Set accelerator quotas
description: Install the ome-quota-manager, split a cluster's GPUs among teams with an AcceleratorQuota tree, and charge workloads to each team's Kueue budget.
since: v1.3
---

An [AcceleratorQuota](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-AcceleratorQuota) tree divides a cluster's accelerators among teams. The quota manager, `ome-quota-manager`, checks the tree and turns it into [Kueue](https://kueue.sigs.k8s.io/) queues, which admit each team's workloads against its budget.

You install the quota manager and give 24 A100 GPUs to the group `ml-research`. You split them: 16 for `team-alpha`, and 8 for `team-beta`, which can borrow up to 4 more while `team-alpha` leaves them idle. Then you charge an InferenceService to a team.

<div class="prerequisites" markdown>

- OME built from `main`, as in [Install from source](../../getting-started/install.md#install-from-source), in the namespace `ome`. AcceleratorQuota isn't in a release yet. Keep the checkout: Step 1 builds from it.
- [Kueue](https://kueue.sigs.k8s.io/docs/installation/), serving `kueue.x-k8s.io/v1beta2`, such as v0.19.2, with its [pod integration](https://kueue.sigs.k8s.io/docs/tasks/run/plain_pods/) on: `pod` in the `integrations.frameworks` of Kueue's configuration. Kueue's manifests and Helm chart set it by default.
- Helm 3 or 4, and the rights to create cluster-scoped objects.
- Go 1.26 or later, Docker, and a registry that your nodes can pull from, here `registry.example.com/ome`.
- The namespace `ml-serving`, where the teams' workloads run.
- GPU nodes labeled with their GPU model, here four nodes with 8 GPUs each and the label `nvidia.com/gpu.product: NVIDIA-A100-SXM4-80GB`.

</div>

## How quotas work

AcceleratorQuotas, short name `aq`, are cluster-scoped and form a tree under `root`, which the quota manager creates. Every other node names its parent in `spec.parentRef.name`, and has one of two roles in `spec.role`:

- A `Cohort` groups other nodes, whose budgets add up to at most its own. It isn't a cap at run time: a team that borrows can take the group past it.
- A `ClusterQueue` is a team, with no children. Its workloads charge its budgets.

A budget in `spec.budgets` counts one resource on one Kueue ResourceFlavor, such as `nvidia.com/gpu` on `a100`. A Cohort's budget sets only `nominal`. A team's budget has three amounts:

| Field | Meaning |
| --- | --- |
| `nominal` | The team's share. Required. |
| `borrowingLimit` | How much more the team can borrow from the unused shares of any team in the tree, not only its siblings. Unset means 0. |
| `lendingLimit` | How much of its unused share other teams can borrow, at most `nominal`. Unset means all of it. |

A team's `priorityTier` has no effect in this release.

The quota manager writes a Cohort node to Kueue as a Cohort, and a team as a ClusterQueue with a LocalQueue in each enrolled namespace. These objects take their node's name and carry the label `ome.io/quota-managed-by: ome-quota-manager`.

The webhook denies a change that breaks the tree's rules. A node can also break one later, when a budget is more than the capacity that the quota manager measures. The quota manager then freezes the node and its descendants: their Kueue objects stay as they were, so admitted workloads keep running. Each budget is checked against capacity on its own, so to cap the total, give `root` a budget.

## Step 1: Install the quota manager

The quota manager has no published image, so build one in your OME checkout. The binary is static, so an image built `FROM scratch` can hold it:

```bash
mkdir -p quota-image
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
  -o quota-image/ome-quota-manager ./cmd/ome-quota-manager
cat > quota-image/Dockerfile <<'EOF'
FROM scratch
COPY ome-quota-manager /ome-quota-manager
USER 65532:65532
ENTRYPOINT ["/ome-quota-manager"]
EOF
docker build --platform linux/amd64 -t registry.example.com/ome/ome-quota-manager:dev quota-image
docker push registry.example.com/ome/ome-quota-manager:dev
```

`docker push` uploads the image, tagged `dev` like the images from Install from source. For Arm nodes, set `GOARCH=arm64` and `--platform linux/arm64`. The chart sets `runAsNonRoot` without a user ID, so the image needs its numeric `USER`.

Write the values. `enrolledNamespaces` lists the namespaces that the quotas govern. While it's empty, as it is by default, the quota manager writes nothing to Kueue:

```yaml title="quota-values.yaml"
global:
  hub: registry.example.com/ome
quotaManager:
  mode: workload
  image:
    tag: dev
  materialize:
    enrolledNamespaces:
      - ml-serving
```

The chart then pulls `registry.example.com/ome/ome-quota-manager:dev`. For a registry that needs credentials, see [Install from a private registry](../../getting-started/private-registries.md).

Install the chart:

=== "Helm 4"

    ```bash
    helm install ome-quota-manager ./charts/ome-quota-manager \
      --namespace ome -f quota-values.yaml --server-side=false
    ```

    `--server-side=false` keeps later upgrades from failing with a conflict over the webhook's CA bundle, as in [Install OME](../../getting-started/install.md#step-3-install-ome).

=== "Helm 3"

    ```bash
    helm install ome-quota-manager ./charts/ome-quota-manager \
      --namespace ome -f quota-values.yaml
    ```

The release notes warn that the chart doesn't install the AcceleratorQuota CRD. That's expected: `ome-crd` installed it. Wait for both replicas:

```bash
kubectl -n ome rollout status deployment/ome-quota-manager
```

```output
deployment "ome-quota-manager" successfully rolled out
```

The quota manager then creates `root`:

```bash
kubectl get aq -o custom-columns=NAME:.metadata.name,ROLE:.spec.role,PARENT:.spec.parentRef.name
```

```output
NAME   ROLE     PARENT
root   Cohort   <none>
```

## Step 2: Create the quota tree

The quota manager only reads Kueue ResourceFlavors, so create one that matches the A100 nodes:

```yaml title="a100-flavor.yaml"
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata:
  name: a100
spec:
  nodeLabels:
    nvidia.com/gpu.product: NVIDIA-A100-SXM4-80GB
```

```bash
kubectl apply -f a100-flavor.yaml
```

```output
resourceflavor.kueue.x-k8s.io/a100 created
```

The quota manager measures capacity once a minute and reports it on `root`:

```bash
kubectl get aq root -o jsonpath='{range .status.capacity[*]}{.resourceName}{" "}{.resourceFlavor}{" "}{.allocatable}{" "}{.highWaterMark}{"\n"}{end}'
```

```output
google.com/tpu a100 0 0
nvidia.com/gpu a100 32 32
```

The columns are a resource, a flavor, the accelerators on nodes that are Ready and not cordoned, and the high-water mark that budgets are checked against. The mark also counts cordoned and not-Ready nodes, so it stays put while you drain nodes. Measured resources appear on all flavors, so `google.com/tpu` shows 0.

Write the tree once `root` lists your resource and flavor. Until then, a budget for them can report `CapacityExceeded`. The tree puts the Cohort `ml-research` under `root`, with two teams under it:

```yaml title="quota-tree.yaml"
apiVersion: ome.io/v1beta1
kind: AcceleratorQuota
metadata:
  name: ml-research
spec:
  role: Cohort
  parentRef:
    name: root
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
      borrowingLimit: "4"
```

`ml-research` comes first: the webhook denies a team whose parent doesn't exist yet.

!!! warning "Borrowed GPUs aren't reclaimed"
    When `team-beta` borrows GPUs that `team-alpha` left idle, it keeps them until its own pods end, and `team-alpha`'s new pods wait. The ClusterQueues that the quota manager writes don't preempt borrowers, and serving pods run until you delete them. Set a `borrowingLimit` only where that wait is acceptable.

Apply the tree:

```bash
kubectl apply -f quota-tree.yaml
```

```output
acceleratorquota.ome.io/ml-research created
acceleratorquota.ome.io/team-alpha created
acceleratorquota.ome.io/team-beta created
```

Check that the nodes are valid and written to Kueue:

```bash
kubectl get aq -o custom-columns='NAME:.metadata.name,ROLE:.spec.role,PARENT:.spec.parentRef.name,READY:.status.conditions[?(@.type=="Ready")].status,MATERIALIZED:.status.conditions[?(@.type=="Materialized")].status'
```

```output
NAME          ROLE           PARENT        READY   MATERIALIZED
ml-research   Cohort         root          True    True
root          Cohort         <none>        True    True
team-alpha    ClusterQueue   ml-research   True    True
team-beta     ClusterQueue   ml-research   True    True
```

If a node shows `False`, see [A node isn't Ready or Materialized](#conditions).

The quota manager has written the teams' ClusterQueues:

```bash
kubectl get clusterqueues.kueue.x-k8s.io -l ome.io/quota-managed-by=ome-quota-manager -o custom-columns=NAME:.metadata.name,COHORT:.spec.cohortName
```

```output
NAME         COHORT
team-alpha   ml-research
team-beta    ml-research
```

It has also written their LocalQueues in `ml-serving`:

```bash
kubectl get localqueues.kueue.x-k8s.io -n ml-serving -l ome.io/quota-managed-by=ome-quota-manager -o custom-columns=NAME:.metadata.name,CLUSTERQUEUE:.spec.clusterQueue
```

```output
NAME         CLUSTERQUEUE
team-alpha   team-alpha
team-beta    team-beta
```

## Step 3: Charge workloads to a team

To charge an [InferenceService](../../concepts/serving/inference-services.md) in `ml-serving` to `team-beta`, label it with the team's name:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: team-beta
```

OME copies the label to the pods of all components, and Kueue keeps the pods `SchedulingGated` until `team-beta`'s ClusterQueue admits them.

On an [OMENative](../../concepts/omenative/overview.md) component, OME keeps the label only when the pods request an accelerator in `ome.controller.quotaAcceleratorResources`, `nvidia.com/gpu` or `google.com/tpu` by default. The wait for admission doesn't count against the [readiness deadline](../../concepts/omenative/instances.md#operations).

Kueue admits each pod on its own, so a nearly full budget can admit part of a multi-node Instance. Size budgets in whole Instances, with room for an update, which by default starts new pods before the old ones drain.

To move a workload to another team, change the label. On OMENative, running pods keep charging the old team until the next rollout replaces them.

Once Kueue admits the pods, `kubectl get aq team-beta` shows their GPUs under ADMITTED, next to the team's share under NOMINAL.

## Change the tree

Edit `quota-tree.yaml` and apply it again. To check a change without writing it, add `--dry-run=server`: the webhook still runs, and kubectl marks the objects `(server dry run)`.

Raising `team-alpha` to 20, for example, would bring the children of `ml-research` to 28, more than its 24. The apply fails with an error that ends in `admission webhook "acceleratorquota.ome-quota-manager.validator" denied the request: ml-research: budget nvidia.com/gpu on a100 is 24 but its children total 28`.

To change a node's role, delete the node and create it again: changing `spec.role` in place leaves the old Kueue object behind.

## Budget other accelerators

Two lists, one in each chart, decide which resources count as accelerators:

| Helm value | Chart | Default | What it decides |
| --- | --- | --- | --- |
| `quotaManager.capacity.resources` | `ome-quota-manager` | `google.com/tpu`, `nvidia.com/gpu` | The resources that the quota manager measures. A budget for any other resource reports `CapacityExceeded`. |
| `ome.controller.quotaAcceleratorResources` | `ome-resources` | `nvidia.com/gpu`, `google.com/tpu` | The resources that keep the queue label on an OMENative component, as the controller's [`--accelerator-resources`](../../reference/operate-ome/controller-manager-flags.md#controllers) flag. Pods that request none of them charge no budget. |

`ome.controller.acceleratorResources`, without `quota`, is a different list, unrelated to quotas.

To budget AMD GPUs as well, add `amd.com/gpu` to both lists. In the quota manager's values:

```yaml title="quota-values.yaml"
global:
  hub: registry.example.com/ome
quotaManager:
  mode: workload
  image:
    tag: dev
  capacity:
    resources:
      - amd.com/gpu
      - google.com/tpu
      - nvidia.com/gpu
  materialize:
    enrolledNamespaces:
      - ml-serving
```

```bash
helm upgrade ome-quota-manager ./charts/ome-quota-manager \
  --namespace ome -f quota-values.yaml
```

Helm reports the upgrade, and the quota manager's pods restart with the new list.

Then add it to the `ome-resources` values. Keep the values from [Install from source](../../getting-started/install.md#install-from-source) in the file, because `helm upgrade -f` resets every value that the file leaves out:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
ome:
  controller:
    tag: dev
    quotaAcceleratorResources:
      - nvidia.com/gpu
      - google.com/tpu
      - amd.com/gpu
  omeAgent:
    tag: dev
modelAgent:
  enabled: true
  image:
    tag: dev
```

```bash
helm upgrade ome ./charts/ome-resources --namespace ome -f values.yaml
```

Helm reports the upgrade, and the controller pods restart with the new list.

## Metrics

The quota manager serves Prometheus metrics on port 8080, at `/metrics`. OME's bundled Prometheus scrapes it when both releases are in `ome`: see [Collect metrics](metrics.md). For the Prometheus Operator, set `quotaManager.metrics.serviceMonitor.enabled: true`, and put the labels that your Prometheus selects in `quotaManager.metrics.serviceMonitor.additionalLabels`.

| Metric | Labels | Value |
| --- | --- | --- |
| `ome_quota_capacity_allocatable` | `resource`, `flavor` | Accelerators on Ready, uncordoned nodes. |
| `ome_quota_capacity_unavailable` | `resource`, `flavor` | Accelerators on cordoned or not-Ready nodes. |
| `ome_quota_capacity_unattributed` | `resource`, `reason` | Accelerators on nodes that match no flavor (`NoMatchingFlavor`) or several (`AmbiguousFlavor`). |
| `ome_quota_budget_nominal` | `plane`, `quota`, `role`, `resource`, `flavor` | A budget's `nominal`. `quota` is the node's name. |
| `ome_quota_budget_admitted` | Same | Accelerators admitted against the budget, borrowed ones included. |
| `ome_quota_budget_reserved` | Same | Accelerators held by workloads with a quota reservation, admitted or not. |
| `ome_quota_budget_borrowed` | Same | Accelerators admitted above `nominal`. |

## Let a group manage quotas

A ClusterRoleBinding to the built-in `view`, `edit` or `admin` role already lets users read AcceleratorQuotas. To let a group edit the tree, bind it to a ClusterRole of its own. Leave out `acceleratorquotas/status`, which only the quota manager writes:

```yaml title="accelerator-quota-admin.yaml"
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: accelerator-quota-admin
rules:
  - apiGroups: ["ome.io"]
    resources: ["acceleratorquotas"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: accelerator-quota-admin
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: accelerator-quota-admin
subjects:
  - apiGroup: rbac.authorization.k8s.io
    kind: Group
    name: quota-admins
```

```bash
kubectl apply -f accelerator-quota-admin.yaml
```

```output
clusterrole.rbac.authorization.k8s.io/accelerator-quota-admin created
clusterrolebinding.rbac.authorization.k8s.io/accelerator-quota-admin created
```

## Troubleshooting

### `kubectl apply` fails with a webhook error

A denial names the node and the rule that the change breaks. A failure to call the webhook means that no quota manager pod is ready. The webhook fails closed, so every create, update and delete of an AcceleratorQuota fails until one is.

### Pods stay `SchedulingGated`

`kubectl describe workloads -n ml-serving` shows why Kueue holds a pod. The usual causes:

- The team's quota is in use, by its own pods or by a team that borrowed it.
- The Workload reports `LocalQueue team-beta doesn't exist`. Add the namespace to `enrolledNamespaces`.
- The pod requests a resource missing from its ClusterQueue, such as an RDMA device.

A ClusterQueue lists its team's accelerators, plus CPU, memory and ephemeral storage with ceilings too high to limit anyone. To admit another resource, give it a ceiling in `coverResources`, and upgrade the release as in [Budget other accelerators](#budget-other-accelerators). Helm merges the map, so the defaults stay:

```yaml
quotaManager:
  materialize:
    coverResources:
      rdma/hca_shared_devices_a: 1M
```

### Pods run without charging a budget

Check that the pods carry `kueue.x-k8s.io/queue-name`, and that Kueue's pod integration is on. On OMENative, OME removes the label from pods that request no listed accelerator: see [Budget other accelerators](#budget-other-accelerators).

### A node isn't Ready or Materialized {#conditions}

Read the node's conditions with their messages:

```bash
kubectl get aq team-beta -o jsonpath='{range .status.conditions[*]}{.type}{"="}{.status}{" "}{.reason}{": "}{.message}{"\n"}{end}'
```

```output
Ready=True Admitted: node is a valid member of the quota tree
Degraded=False Admitted: no invariant violated
Materialized=True Admitted: enforcement objects match this node's budget
```

`Degraded` is the opposite of `Ready`, with the same reason. `Materialized` is set only while `enrolledNamespaces` lists a namespace. The reasons for `False`:

| Condition | Reason | Meaning |
| --- | --- | --- |
| `Ready` | `CapacityExceeded` | A budget on the node or an ancestor is over the high-water mark on `root`, or `root` measures nothing for its resource and flavor. |
| `Ready` | `ContainmentViolated`, `ParentMissing`, `ParentCycle`, `Unreachable`, `DepthExceeded` or `NodeKindInvalid` | The node or an ancestor breaks a rule of the tree. The message says which. |
| `Materialized` | `Frozen` | `Ready` is `False`, and the node's Kueue objects keep their last good state. |
| `Materialized` | `FlavorMissing` | A budget names a missing ResourceFlavor. Create it. The node's other budgets are written. |
| `Materialized` | `ObjectConflict` | Another owner's Kueue object has the node's name. Delete that object, or recreate the node under another name. |
| `Materialized` | `MaterializationFailed` | Applying a Kueue object failed, for example because a `lendingLimit` is over its `nominal`. The message gives the error. |

## Clean up

Delete the workloads that charge the teams, then the teams:

```bash
kubectl delete aq team-alpha team-beta
```

```output
acceleratorquota.ome.io "team-alpha" deleted
acceleratorquota.ome.io "team-beta" deleted
```

Then delete the Cohort. The webhook denies deleting a node that still has children:

```bash
kubectl delete aq ml-research
```

```output
acceleratorquota.ome.io "ml-research" deleted
```

Uninstall the chart:

```bash
helm uninstall ome-quota-manager -n ome
```

Helm reports that it kept the Secret `ome-quota-manager-webhook-cert`, and that the release is uninstalled.

Four objects remain: `root`, the Kueue Cohort `root`, the webhook Secret and the ResourceFlavor. With the quota manager gone, clear the finalizer on `root` yourself, then delete all four:

```bash
kubectl patch aq root --type=merge -p '{"metadata":{"finalizers":null}}'
kubectl delete aq root
kubectl delete cohorts.kueue.x-k8s.io root
kubectl delete secret -n ome ome-quota-manager-webhook-cert
kubectl delete resourceflavors.kueue.x-k8s.io a100
```

```output
acceleratorquota.ome.io/root patched
acceleratorquota.ome.io "root" deleted
cohort.kueue.x-k8s.io "root" deleted
secret "ome-quota-manager-webhook-cert" deleted
resourceflavor.kueue.x-k8s.io "a100" deleted
```

## Next steps

- [kubectl ome quota](../../reference/kubectl-ome/quota.md): `kubectl ome quota tree` prints the tree, and `validate` exits with status 2 when it breaks a rule.
- [Use the OME scheduler](ome-scheduler.md): OME's alpha second scheduler, for Kubernetes 1.35, places each opted-in gang in one topology domain.
