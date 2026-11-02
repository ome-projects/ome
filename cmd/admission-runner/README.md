# Admission runner

Validate proposed Kubernetes resources against the released OME admission
webhooks in an isolated API server. For example, a service with five minimum
replicas and two maximum replicas is rejected before an operator deploys it.

A runner image bundles the manager, this runner, and the Kubernetes
control-plane binaries (kube-apiserver and etcd) of the version the tests
use. Both binaries come from the same source revision. The manager runs with `--webhook-only`: it registers admission
handlers and their configuration, without registering controllers or loading
controller publication clients. Ordinary manager startup remains unchanged.

Clients supply the proposed and baseline resource catalogs, chart defaults,
CRDs, webhook configuration and operator ConfigMap. They also choose the
manager role, namespace, image digest, loopback address and time limits.
GitOps repository policy and deciding which destinations changed belong to
the caller.

## Input directory

The versioned input layout contains:

- `plan.json`: destination identifier, `omeRevision`, `managerNamespace`,
  `managerRole` (empty or `control-plane`).
- `baseline.yaml`, `proposed.yaml`: the five supported OME resource kinds.
- `catalog.yaml`: models/runtimes provided by the selected charts.
- `configmap.yaml`: the selected `inferenceservice-config` ConfigMap.
- `config/crd/full/*.yaml`: installed OME schemas.
- `config/webhook/manifests.yaml`: selected validating and mutating webhook configurations.

[manifest_validation.go](manifest_validation.go) loads retained baseline resources
and chart defaults into the private API with runtime preset mutation enabled,
before validating webhooks start. Models and parent
runtimes precede services. Validated dependencies
are stored only in the private API. CREATE/UPDATE dry runs must leave stored
service objects unchanged. A read-only manager identity and checks for workload
objects guard the isolated environment.

The ServingRuntime and ClusterServingRuntime preset mutators fill missing engine,
decoder and router settings when a runtime sets `ome.io/engine`. Explicit runtime
settings take precedence. Mutation runs before validation on each dry run, and
accepted runtime changes are stored with the populated fields for later lookups.
Unknown engine names are rejected. The Pod mutator is excluded because this runner
does not submit Pods.

Control-plane role preserves its disabled InferenceService webhook. Those
services receive strict CRD validation. Other admission handlers retain their
normal configuration. The input ConfigMap is not rewritten for controller
routing, because webhook-only startup does not initialize those controllers.

## Output

`report.json` records the selected revision/image, startup and request timings,
per-resource allow/reject decisions, webhook calls, dry-run state and object
immutability. Invalid proposed resources exit nonzero. Setup failures write
logs. The caller decides whether failures block deployment or remain advisory.

File names in this input/output layout and webhook paths are protocol
identifiers. Addresses, limits, identity names and executable paths are caller
configuration. The image entrypoint supplies its packaged binary paths and
neutral process settings; the caller supplies deployment-specific values.

```sh
go build ./cmd/admission-runner
```
