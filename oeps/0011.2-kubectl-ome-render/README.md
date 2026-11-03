# OEP-0011.2: Render the Effective InferenceService in kubectl-ome

<!-- toc -->
- [Summary](#summary)
- [Relationship to OEP-0011.1](#relationship-to-oep-00111)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Command Surface](#command-surface)
  - [User Stories](#user-stories)
    - [Preview a Runtime Change in a GitOps Pull Request](#preview-a-runtime-change-in-a-gitops-pull-request)
    - [Check a Deploy Default Change Before Rollout](#check-a-deploy-default-change-before-rollout)
  - [Security and RBAC](#security-and-rbac)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Render Pipeline](#render-pipeline)
  - [Inputs](#inputs)
  - [Rendering Every File InferenceService](#rendering-every-file-inferenceservice)
  - [Output Object](#output-object)
  - [Failure Behavior](#failure-behavior)
  - [Implementation Scope](#implementation-scope)
  - [Test Plan](#test-plan)
  - [Graduation Criteria](#graduation-criteria)
- [Open Questions](#open-questions)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

**Tl;dr:** add `kubectl ome runtime render ISVC`, which prints the
engine, decoder, and router specs the InferenceService controller acts
on: after runtime merge, deployment mode resolution, and the deploy
defaults from the `inferenceservice-config` ConfigMap. Inputs come from
the live cluster or from local manifest files. File mode applies the
`v1beta1` defaults from the supplied InferenceService CRD before rendering,
matching the objects an API server would store.

The controller builds these specs in memory on every reconcile and never
stores them. `runtime effective` (OEP-0011.1) reruns the merge but prints
only an allowlisted summary. `render` reuses the controller's exported
helpers, so there is no second copy of the rules, and prints a CLI-owned
object that cannot be applied.

## Relationship to OEP-0011.1

This OEP keeps OEP-0011.1's command family, report and exit-code
contracts, and namespace model. It changes two things:

1. **Redaction rule.** OEP-0011.1 says runtime and revision views
   "never emit complete pod templates, environment values, headers, or
   arbitrary extension payloads." This OEP scopes that rule to
   diagnostic reports and makes `runtime render` the one documented
   exception. Reports stay allowlisted.
2. **Implementation scope.** Besides `cmd/kubectl-ome/**` and
   `pkg/cli/**`, this OEP exports the existing deploy-config parser in
   `pkg/controller/v1beta1/controllerconfig`. Controller behavior does
   not change.

## Motivation

A component's spec comes from three sources:

1. the InferenceService's `engine`, `decoder`, and `router` blocks;
2. the selected ServingRuntime or ClusterServingRuntime, merged in by
   `MergeRuntimeSpecs`, possibly pinned to an older ControllerRevision;
3. the `deploy` block of `inferenceservice-config`, applied by
   `specdefaults` to unset fields: replica bounds, termination grace
   period, and, for OMENative, `minReadySeconds` and update strategy.

A change to any of them can change what runs, but there is no built-in
way to preview the result.

The API server also applies defaults from the installed InferenceService
CRD before the controller sees an object. In file mode those defaults must
come from the CRD rendered for the same OME revision; compiled Go defaults
cannot represent a different revision's schema.

### Goals

- Print the merged and defaulted component specs for one
  InferenceService, identical to what the controller computes from the
  same inputs. Service-level VirtualDeployment is the one exception; see
  [Render Pipeline](#render-pipeline).
- Support the live and active (pinned) views of `runtime effective`.
- Render fully offline from manifest files, so a preview can compare
  two Git revisions without cluster access.
- Apply the matching CRD's structural-schema defaults in file mode, while
  preserving explicit values such as `autoSync: false`.
- In file mode, render every InferenceService in the files in one run,
  so a preview does not need to know in advance which services a change
  affects.
- Reuse exported controller helpers for every step.
- Project a static Split hub source into one member's desired replica bounds
  before resolving that member's runtime, using the existing placement planner.
- Keep diagnostic reports allowlisted and unchanged.

### Non-Goals

- Rendering generated workloads (Deployments, LeaderWorkerSets, pods,
  Services). Those need the full reconciler and webhooks.
- Accelerator selection or injected resources, for alpha.
- Redaction. See [Security and RBAC](#security-and-rbac).
- Any API, CRD, controller behavior, webhook, chart, or RBAC change.
- An `apply` path.

## Proposal

### Command Surface

`kubectl ome runtime render [ISVC]` with these flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--view live\|active` | `live` | Merge the runtime as it is now, or the pinned ControllerRevision. `active` needs the cluster. |
| `-o yaml\|json` | `yaml` | Output encoding. No table form. |
| `-f, --filename PATH` | none | Read the InferenceService, runtimes, and models from files. Repeatable. |
| `--deploy-config PATH` | none | Read deploy defaults from a ConfigMap manifest. Required with `-f`. |
| `--inference-service-crd PATH` | none | Read API defaults from an `inferenceservices.ome.io` CRD manifest. Required with `-f`. |
| `--ome-namespace` | `ome` | Existing flag; where the live ConfigMap and ControllerRevisions are read. |

With `-f`, `--deploy-config`, and `--inference-service-crd`, the command
makes no API request.

`ISVC` is required in cluster mode. With `-f` and no `ISVC`, every
InferenceService in the files is rendered; see
[Rendering Every File InferenceService](#rendering-every-file-inferenceservice).

### User Stories

#### Preview a Runtime Change in a GitOps Pull Request

A pull request edits a ClusterServingRuntime's arguments. Its preview
renders every InferenceService from the base and head revisions, with
`-f`, `--deploy-config`, and `--inference-service-crd` pointing at the
rendered manifests and no `ISVC` argument, and posts the diff. The preview
never works out which services use the runtime: unaffected services render
identically and drop out of the diff. The reviewer sees every affected
component, and sees that a service which overrides the argument is
unaffected.

The matching CRD can come from `config/crd/full/ome.io_inferenceservices.yaml`
after `make manifests`, or from the same revision's `ome-crd` chart with
`helm template ome-crd ./charts/ome-crd --show-only
templates/ome.io_inferenceservices.yaml`.

#### Check a Deploy Default Change Before Rollout

A platform team plans to set `updateStrategy.engine.maxSurge`. They
render live services with the proposed ConfigMap through
`--deploy-config` and confirm that services with their own `maxSurge`
keep it and RawDeployment services do not change.

### Security and RBAC

The output contains complete specs, including literal environment
values and any credential placed inline. This is justified because
`render` prints nothing the caller cannot already read raw with
`kubectl get -o yaml`. Every live input is read with the caller's
identity:

| Input | Resource | Verb |
| --- | --- | --- |
| InferenceService | `ome.io` InferenceServices | `get` |
| Named runtime | ServingRuntimes or ClusterServingRuntimes | `get` |
| Auto-selected runtime | runtimes, and the BaseModel or ClusterBaseModel | `list`, `get` |
| Active view | `apps` ControllerRevisions in the OME namespace | `list` |
| Deploy defaults | ConfigMaps in the OME namespace | `get` |

The command never reads Secrets or dereferences `secretKeyRef`. The new
risk is convenience: one output collects values from several objects,
so they are easier to paste into a log by accident. Mitigations:

1. Help text warns that output may contain literal secrets.
2. No report kind embeds effective specs.
3. The accessor that exposes merged specs is render-only and returns
   deep copies, so report code cannot use it by accident.

### Risks and Mitigations

- **Output applied as a manifest.** That would freeze every default
  into the stored spec. Mitigation: `apiVersion: cli.ome.io/v1alpha1`,
  which the API server rejects.
- **Defaults silently missing.** Mitigation: a missing, unreadable, or
  invalid ConfigMap fails the command, as it fails the reconcile. There
  is no flag to skip defaults.
- **Offline inputs differ from the cluster.** Mitigation: the output
  lists the rendered resource inputs with their origin, and a missing input
  is an error. File mode requires the InferenceService CRD from the same OME
  revision and uses Kubernetes' structural-schema defaulting implementation.

## Design Details

### Render Pipeline

The controller (`pkg/controller/v1beta1/inferenceservice/controller.go`)
loads `DeployConfig`, returns early for service-level
VirtualDeployment, resolves and pins the runtime, calls
`MergeRuntimeSpecs` and `DetermineDeploymentModes`, then
`specdefaults.Engine/Decoder/Router`. `runtime effective` already
reruns everything but the first and last steps. `render` adds them:

```text
file InferenceService
  -> strict YAML-to-JSON decode
  -> v1beta1 CRD structural-schema defaults
  -> strict typed decode
cluster InferenceService or defaulted file InferenceService
  -> runtime resolution and pin (pkg/cli/effective, existing)
  -> MergeEffectiveComponents (gains a *DeployConfig argument)
       -> MergeRuntimeSpecs + DetermineDeploymentModes
       -> specdefaults.Engine/Decoder/Router
  -> RenderedInferenceService -> yaml or json
```

A nil `DeployConfig` keeps today's behavior for existing callers.

**Service-level VirtualDeployment.** The controller treats a service as
virtual when `InferenceServiceDeploymentMode` returns
`VirtualDeployment`: the deployment-mode annotation, or, without one,
`spec.deploymentMode`. It then only sets status URLs; it merges nothing,
applies no defaults, and creates no workloads. `render` uses the same
helper but does not stop early. It still merges and prints the
components as inspection data, not specs the controller reconciles:

- Every component's `deploymentMode` is `VirtualDeployment`, with source
  `ServiceAnnotation` or `ServiceSpec`.
- `specdefaults` is not called and `deployDefaults` is `NotApplicable`.
  The ConfigMap is still read and validated, as the controller does
  before its early return.
- The runtime is still required for the merge, but neither it nor any
  component `spec` field affects reconciliation.

### Inputs

**Cluster mode** (no `-f`): objects are read as `runtime effective`
reads them. Deploy defaults come from `--deploy-config` if given,
otherwise from `inferenceservice-config` in `--ome-namespace`.

**File mode** (`-f`): the command keeps InferenceServices, runtimes,
and models from the files and ignores other kinds with a notice.
Runtime resolution uses the same lookup interface as cluster mode.
`--deploy-config` and `--inference-service-crd` are required; ConfigMaps
and CRDs in `-f` files are not used implicitly. The CRD path must contain
exactly one `apiextensions.k8s.io/v1` namespaced
`inferenceservices.ome.io` CRD with a served `v1beta1` structural schema.
Its defaults are applied to each unstructured InferenceService before the
existing strict typed decoder rejects unknown fields. Explicit fields are
never overwritten. Each file-rendered object's `sources` records the CRD as
`kind: CustomResourceDefinition`, with the supplied path and `origin: File`.
`--view active` is rejected.

Both modes parse deploy defaults with `controllerconfig.ParseDeployConfig`.

### Rendering Every File InferenceService

With `-f` and no `ISVC`, the command renders every InferenceService in
the files, across all namespaces. `-n` only fills in a missing
namespace, as it does for a single service. Each service goes through
the same pipeline and the same runtime lookup as a single render, so
the result for a service equals rendering it by name.

- The output is one `RenderedInferenceServiceList`, sorted by namespace
  and name, even when the files hold a single service. The shape does
  not depend on the input count.
- If any service fails, for example because its runtime is missing
  from the files, the command reports every failing service on stderr,
  writes nothing to stdout, and exits `1`. A partial list would make
  the failed services look unchanged in a diff.
- Files with no InferenceService are an error, so a wrong path does
  not produce an empty diff.

Cluster mode still requires `ISVC`. Rendering every service in a
cluster is out of scope: it needs `list` across namespaces and has no
preview use yet.

### Offline Placement Projection

`kubectl ome runtime project-placement [ISVC] --member MEMBER` takes repeated
`-f` inputs containing hub InferenceServices and the complete WorkloadCluster
inventory, plus the hub's `--inference-service-crd`. It applies the existing
file decoder and CRD defaults, then calls the controller's exported
`ProjectSplitReplicas`. It supports explicit `ClusterAffinity` with mode `Split`
and makes no API request, runtime lookup, or credential lookup. A name selects
one source in the current namespace; otherwise every source is projected.

The command accepts both current weight forms: integer weights with per-component
`split.maxPerCluster` caps, and `*Replicas` ratios with independent `min` and `max`
shares of the source's component bounds. The planner rejects mixing those forms
or keeping `split` alongside ratios. Projection checks placement fields against
the linked API types before the custom presence-preserving decoder runs, so
removed fields such as scalar `weight` and `maxReplicasPerCluster` cannot be
silently dropped and change the plan.
Kubernetes' CRD schema validator checks the defaulted projection inputs, including
positive component caps. Placement's existing validator and planner enforce the
rules relating ratio weights, modes, and the split block.

The output is a CLI-owned envelope:

```yaml
apiVersion: cli.ome.io/v1alpha1
kind: PlacementProjection
member: member-a
items:                          # sorted by namespace, then name
  - apiVersion: ome.io/v1beta1
    kind: InferenceService
    metadata: {name: chat, namespace: prod}
    spec: { ...source with the member's desired replica bounds... }
```

The envelope cannot be applied. Each item preserves source labels, annotations,
and spec fields, with projected engine/decoder/router bounds and without server
metadata or status. These are inputs for runtime rendering, not full member
manifests derived by reconciliation. Status, admission, and retained rollout
replicas do not influence desired projection. A zero allocation omits the source;
no allocations produce `items: []`. Unsupported modes and invalid or duplicate
inputs fail the whole operation without output.

`runtime render -f` accepts this envelope and feeds its items through the same
strict typed decoding, member CRD validation/defaulting, and runtime resolution
as ordinary file inputs. It rejects malformed envelopes and duplicate identities
across projected and ordinary files. An empty projection renders an empty list;
ordinary file input with no InferenceService remains an error.

Projection and runtime merge remain separate commands: a caller uses the hub's
OME revision and CRD for placement, then the member's revision, CRD, runtime
files, and deploy config for runtime rendering. The same two stages on PR base
and proposed inputs show replica changes without copying allocation logic into
the preview caller. A live drift comparison still needs actual member state.

This adds only CLI code. The projection helper was already exported by #1247.

### Output Object

```yaml
apiVersion: cli.ome.io/v1alpha1
kind: RenderedInferenceService
metadata:
  name: chat
  namespace: prod
view: Live                      # Live or Active
sources:
  - kind: InferenceService
    name: prod/chat
    origin: Cluster             # Cluster or File
  - kind: ClusterServingRuntime
    name: shared-runtime
    origin: Cluster
    autoSync: false
    revision: runtime-revision  # omitted when spec.runtime.revision is unset
  - kind: ConfigMap
    name: ome/inferenceservice-config
    origin: Cluster
deployDefaults: Applied         # or NotApplicable (service-level Virtual)
components:
  engine:
    deploymentMode: OMENative
    deploymentModeSource: <as in runtime effective>
    spec: { ...effective EngineSpec... }
  decoder: { ... }
  router: { ... }
```

The runtime source reports the resolved `autoSync` value and the requested
`spec.runtime.revision` when set. In an Active view, the actual pinned snapshot
remains a separate `ControllerRevision` source. In file mode, `sources` also
includes the `CustomResourceDefinition` path used for API defaults. Cluster
mode does not read or report a CRD source.

Output is deterministic (no resource versions, UIDs, or status), so two
renders of the same inputs are byte-identical.

Rendering every file InferenceService wraps the objects in a list:

```yaml
apiVersion: cli.ome.io/v1alpha1
kind: RenderedInferenceServiceList
items:                          # sorted by namespace, then name
  - kind: RenderedInferenceService
    metadata: { name: chat, namespace: prod }
    ...
```

### Failure Behavior

| Case | Exit |
| --- | --- |
| Rendered | `0` |
| InferenceService, runtime, or pinned revision missing or unreadable | `1` |
| Deploy defaults missing, Forbidden, or invalid | `1` |
| `-f` without `--deploy-config` or `--inference-service-crd`, `--view active` with `-f`, or ambiguous file input | `1` |
| InferenceService CRD unreadable, malformed, mismatched, or without a structural `v1beta1` schema | `1` |
| No `ISVC` without `-f`, or `-f` files with no InferenceService | `1` |
| Any service fails while rendering every file InferenceService | `1` |

### Implementation Scope

`cmd/kubectl-ome/**`, `pkg/cli/**`, and a rename of
`parseDeployConfig` to `ParseDeployConfig` in
`pkg/controller/v1beta1/controllerconfig/configmap.go`. The CLI cannot
use `NewDeployConfig`, which reads the manager's `POD_NAMESPACE`.

Planned PRs:

1. This OEP.
2. Export `ParseDeployConfig`, thread `*DeployConfig` through
   `MergeEffectiveComponents` (nil at existing call sites), add a
   ConfigMap loader. Detect service-level Virtual with
   `InferenceServiceDeploymentMode`, passing the deploy config's
   `defaultDeploymentMode` (`RawDeployment` when there is none), instead
   of the annotation alone.
3. `runtime render` in cluster mode.
4. File mode.
5. Rendering every file InferenceService.

### Test Plan

[x] I/we understand that component owners may require updates to
existing tests before accepting changes necessary for this enhancement.

- `controllerconfig`: `ParseDeployConfig` accepts and rejects the same
  inputs as before.
- `pkg/cli/effective`: for RawDeployment, OMENative, leader and worker,
  and PD disaggregation fixtures, output equals calling the controller
  helpers directly. This guards against drift. VirtualDeployment
  fixtures, set by annotation and by `spec.deploymentMode`, resolve every
  component to `VirtualDeployment` and skip `specdefaults`.
- `pkg/cli/cmd/runtime`: each failure row, the `--deploy-config`
  override, `--view active`, and golden YAML and JSON outputs.
- Rendering every file InferenceService: a golden list across two
  namespaces; each item equals rendering that service by name; one
  failing service fails the run with nothing on stdout.
- CRD defaulting: checked-in generated-schema defaults, explicit overrides,
  a schema-default change, malformed and mismatched CRDs, retained strict
  unknown-field rejection, and equality between file and cluster rendering.
- Integration: TBD, possibly an envtest case comparing a reconciled
  workload's defaulted fields with `render` output.

### Graduation Criteria

- **Alpha:** both modes, both views, equality tests, secrets warning.
- **Beta:** used by a GitOps preview pipeline for one release without a
  reported mismatch; accelerator question decided.

## Open Questions

- Include accelerator-injected resources? They depend on
  AcceleratorClass and node state, which breaks offline rendering.
  Proposed for alpha: omit them.
- Is a stderr warning enough for secrets in shared logs, or should
  non-interactive use require an acknowledgement flag?

## Implementation History

- 2026-09-28: Provisional OEP-0011.2 created.
- 2026-10-01: File mode renders every InferenceService when no `ISVC`
  is given.
- 2026-10-15: File mode requires the matching InferenceService CRD and
  applies its `v1beta1` structural-schema defaults before typed decoding.

## Drawbacks

- Controller refactors must keep `specdefaults` and `ParseDeployConfig`
  callable from outside the manager.
- A second, unredacted output path sits next to the allowlisted reports.
- File mode duplicates some cluster lookup logic.
- Offline callers must render and pass a roughly 1.5 MiB CRD alongside the
  smaller deploy ConfigMap and workload manifests.

## Alternatives

1. **Add allowlisted fields to `runtime effective`.** Covers only
   chosen fields; a preview needs the whole spec. Rejected.
2. **Print a raw InferenceService.** Invites `kubectl apply`, and OEP-0011.1
   forbids non-`get` output that masquerades as an API object. Rejected.
3. **Persist the effective spec in status.** Grows every object,
   exposes literal values to status readers, and does not work offline.
   Rejected.
4. **A standalone preview tool.** This is the copied merge that drifts.
   Rejected.
5. **Controller dry-run endpoint.** New server surface, no offline use.
   Rejected for now.
6. **The caller lists the services and renders each by name.** The
   caller would have to parse manifests as `render` does, and to find
   the services a runtime change affects it would need runtime
   auto-selection: a second copy of the resolver. Rejected.
7. **An explicit `--all` flag.** Clearer at the call site, but
   `kubectl get -f` already reads "no name" as every object in the
   files. Rejected; revisit if cluster mode gains an all-services form.
8. **Per-item errors in the list.** Lets a preview show the healthy
   services, but a reader of the diff can miss a failure. Rejected for
   alpha.
