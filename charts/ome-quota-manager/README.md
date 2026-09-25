# Quota manager remote access

`quotaManager.mode` selects `management` (fleet projection) or `workload`
(local quota materialization). Each process uses its own local ServiceAccount.
Cross-cluster authentication comes from the platform through the kubeconfig
referenced by each WorkloadCluster.

On a member, enable the remote quota grant:

```yaml
quotaManager:
  mode: workload
  remoteAccess:
    enabled: true
    name: ome-quota-access
    subjects: []
```

This creates only the ClusterRole. Leave `subjects` empty when another GitOps
application owns the platform's ClusterRoleBinding. To manage that binding
here, list existing `User`, `Group`, or `ServiceAccount` subjects; ServiceAccount
subjects require a namespace. The access template creates no accounts or tokens.

The role grants AcceleratorQuota CRUD, without quota-status writes, hardware
reads, Kueue writes, or workload creation. Placement has a separate role so the
platform can grant either capability independently. Separate roles alone do
not guarantee separate identities: both processes read the same WorkloadCluster
kubeconfig, and the authenticated principal depends on the platform.

## Upgrading

The `remoteAccess.serviceAccount.create` and `.staticToken` options have been
removed. Supplying `remoteAccess.serviceAccount` while access is enabled fails
rendering with migration guidance, rather than silently ignoring credential
provisioning. Provision or transfer ownership of any required identity and
credential before upgrading, remove the obsolete values, and bind the existing
identity through `subjects` or a separately managed binding. An empty subject
list removes the chart-owned binding on Helm upgrade; verify external bindings
first. Argo CD deletion also depends on its effective pruning policy.

The quota role name and permissions are unchanged.

See [the multicluster option audit](../ome-resources/multicluster-options.md)
for the complete placement, transport, routing, and projection review.
