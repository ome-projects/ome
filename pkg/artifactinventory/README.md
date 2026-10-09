# Node artifact inventory

`Builder.Build(ctx, nodeName)` reads a node's model-agent ConfigMap, GETs the
BaseModels reported Ready, groups their artifact paths, and estimates size from
same-name `ReplicationJob` objects in `default`.

```go
builder := artifactinventory.NewBuilder(manager.GetAPIReader(), "ome")
inventory, err := builder.Build(ctx, nodeName)
```

Use an uncached Kubernetes reader with core and OME types registered in its
scheme. The caller supplies the context deadline and client request timeout.
There is no controller, command, background cache, or node filesystem access in
this package.

## Reading the code

| File | Responsibility |
| --- | --- |
| `source.go` | Read the node CM; select Ready BaseModel entries and GET their CRs |
| `builder.go` | Resolve shared/ordinary paths; group CRs; assemble the inventory |
| `size.go` | Read and validate ReplicationJob size; count each directory once |
| `types.go` | Public output types |

The supported deployment layouts are ordinary real directories at
`CR.spec.storage.path`, and registered shared entries whose `hfArtifactKey`
points to a parent with `localPath` and `children`. Artifact roots must not
overlap or use undeclared symlink aliases. The builder does not discover these
layouts from disk or support the older implicit reuse layout.

The Ready CM entries define the selection scope. ClusterBaseModels, vendor
models, non-Ready entries, and CRs annotated `ome.io/artifact-residency: Evicted`
are excluded. The builder does not add missing children from a parent's list
or require the CR's asynchronously updated `nodesReady` field.

## Size adapter and output

ReplicationJob is an external `ome.io/v1beta1` CRD; this package reads it as an
unstructured object without adding it to OME's API or installing its CRD.
The adapter checks a Completed Job's destination URI, region, and revision
against the model source, then reads `status.observed.sourceArtifactSizeBytes`.
This is a source-file byte estimate, not measured filesystem allocation.
Missing Jobs preserve the mapping with an unknown size. Other API read errors
fail the build. The target cluster must serve the ReplicationJob API for this
adapter to work; API discovery errors also fail the build.

Each output artifact includes `path`, `models`, `estimatedArtifactBytes`,
`evictionEligible`, and `issues`. Unknown size is `null`. A mapping or size issue
sets the entire group's eligibility to false. An error that cannot be assigned
to a known artifact returns `nil, error`, never a partial candidate inventory.
Different paths remain separate even when they use the same Job. Different Jobs
for one path leave its size unknown until file-set equivalence can be established.

Each model includes `nodesReady`, copied from `CR.status.nodesReady`, with an
empty array when absent. Callers can union these lists to estimate potential
node impact without extra API requests. The lists do not filter CM readiness
or cause the builder to inspect other nodes.

`evictionEligible=true` only means the inventory checks passed. The caller must
still check serving demand and eviction policy. The CM, CRs, and Jobs are not
read atomically; this output is not a deletion authorization.

The optional [RBAC example](../../config/artifact-inventory/rbac.yaml) grants
only the required GET permissions. It is not installed by the OME charts.

```sh
go test -race ./pkg/artifactinventory
```
