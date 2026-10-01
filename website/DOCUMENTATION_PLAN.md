# Documentation refresh

Working baseline: public commit `bc1f94db`, inspected on 2026-09-30.
The API comparison starts at `c0a13fb5`; the website redesign landed in
`c52daad2` (#1177). Fetching GitHub failed during this audit, so this baseline
does not establish that every remote merge has been inspected. The latest
local release tag is v1.2.2; features on main must not be presented as a
published v1.3 release.

## Outcome

A reader should be able to install a matching version of OME, serve a model,
change its deployment, and diagnose a failure using a connected set of
examples. Platform operators should have a separate path through scheduling,
capacity, cluster registration and traffic. Each path must say what is
required, what success looks like, and what to do when a step fails.

Keep the useful existing concept pages, reference tables and recovery guides.
Revise them when a source change or a reader's task exposes a specific gap.
Completion is measured by supported workflows and verified claims, not by
pages reviewed or commits created.

## Evidence and publication rules

1. Pin the public code, charts, CRDs, CLI and runtime recipe used by each
   workflow. Distinguish a released version from a source build and distinguish
   implementation from operational maturity.
2. Treat `site/` as maintained public content to reconcile with `website/`.
   Check the underlying implementation when the two disagree. While Hugo is
   live, correctness fixes shared by both sites belong in both copies.
3. Use examples ahead of public main to discover missing workflows and test
   cases. Confirm each capability against the public baseline before writing
   public instructions. Write public fixtures afresh with neutral names and
   explicit dependencies; do not copy private deployment configuration or
   test-harness assumptions.
4. Record whether an example was source-reviewed, schema-checked, checked
   against admission, or actually executed. These are different kinds of
   evidence. A successful schema check does not establish a working model
   server or a usable end-to-end tutorial.
5. Keep unresolved product behavior in a small issue list with public source
   evidence. Do not repeatedly rewrite prose around an unverified assumption.

## Findings that set the order

| Finding at the baseline                                                                                                  | Consequence                                                  | Work package |
| ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ | ------------ |
| Generated API reference still requires `InferenceReplica.parentRef` and omits newer placement/canary types               | Readers see an obsolete API contract                         | A            |
| Architecture presents InferenceReplicas as exclusively controller-owned                                                  | Standalone workloads are undiscoverable                      | A, C         |
| Canary docs say a failed canary cannot restart on a new target and capacity loss resets its clocks                       | Recovery instructions disagree with current behavior         | A            |
| Install defaults to v1.2.2 while OMENative guides require main                                                           | Following navigation does not produce matching prerequisites | B            |
| Deploy guide switches model, directory and prerequisites between journeys; runtime-only shares model-agent prerequisites | The simplest deployment path is unclear                      | B            |
| PVC/local guides start after weights already exist; new Hugo replication guide has no website equivalent                 | The storage workflow has a missing first step                | D            |
| Multi-cluster guide starts with two registered clusters                                                                  | There is no complete first-placement workflow                | F            |
| Five newer Hugo pages are unmapped and thirteen mapped pages have later Hugo commits                                     | Content needs reconciliation, not automatic duplication      | A, H         |
| Hugo-only changes do not trigger the website drift job                                                                   | Drift can accumulate without a report                        | H            |

The drift report identifies candidates, not thirteen proven content defects.
The website already covers most newer CLI material, ingress routing, accepted
deployment-mode values, explicit runtime lookup, retry holds, and migration
caps. Compare the actual content before changing it.

## Work packages

Each package has one writer responsible for the whole reader journey and one
independent review of correctness and usability. Parallel work owns disjoint
files; navigation, shared examples and cross-page edits are integrated once.
Keep each package reviewable as a focused PR. Local working edits do not need
per-page commits or intermediate merge commits.

| Package                                  | Deliverable and scope                                                                                                                                                                                                                                                                                      | Completion evidence                                                                                                                                      |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. Correct the current contract**      | Regenerate both API references; distinguish standalone/projected replicas; fix canary recovery and capacity timing; document startup-only settings; bring over the missing pod-batching guide                                                                                                              | Public source/test citations reviewed; content/link checks pass; generation has no unexplained drift                                                     |
| **B. One complete first deployment**     | Make the released and development install paths explicit. Use one model/name/storage convention from install through request. Start with an explicit runtime and service; make managed weights and AcceleratorClass optional branches. Add a small CPU controller lab separately from real model inference | Clean installation prerequisites, complete manifest, apply/readiness/request/cleanup sequence; actual HTTP response from the declared profile            |
| **C. OMENative and standalone replicas** | Extend the same lab through scale, update, failed revision and recovery. Add standalone runner and runtime-reference forms, ownership, readiness and scaling limits; then a leader/worker topology                                                                                                         | Admission and lifecycle cases checked on the target revision; reader can explain Instance count versus pod count and choose the correct control resource |
| **D. Weights and runtime recipes**       | Adapt the public `ome-agent replica` guide into stage weights → PVC/local/model registration → serve. Add a guide for authoring a runtime from an image. Preserve existing storage/reference detail behind the task flow                                                                                   | Artifact paths, access modes, credentials and cleanup responsibilities explicit; one complete supported transfer-and-serve route verified                |
| **E. PD rollout and recovery**           | Keep the useful 4-engine/2-decoder example; add asymmetric rounding, delayed decoder readiness, gates, failure/retry and rollback. Separate a CPU topology exercise from a real inference recipe with stated hardware/network requirements                                                                 | Public validator accepts the examples; observed component counts, traffic behavior and recovery match claims; no invented CLI output                     |
| **F. First multi-cluster deployment**    | Register members, establish roles/credentials/RBAC, prepare runtime/model availability, place a service, inspect allocation and publish an endpoint. Explain Legacy versus ClusterAffinity, Single/All/Split/SplitByCapacity, freshness and quota waits                                                    | Alpha/in-development label retained; connection, placement, capacity, admission, scheduling, readiness and routing verified as separate stages           |
| **G. Operate the serving platform**      | Build from those examples into OME scheduler, gang placement, quotas and Alfred. Distinguish traffic drain, Instance migration and cluster movement. Reuse existing diagnosis tables                                                                                                                       | Installation/version prerequisites and action ownership explicit; observable success/failure/recovery for each operation                                 |
| **H. Keep the docs current**             | Reconcile Hugo deltas and mappings, trigger drift on Hugo-only changes, record public source baselines for workflows, and define site cutover ownership                                                                                                                                                    | New Hugo page is reported; reconciled content advances its baseline only after review; changing API/CLI/chart behavior identifies affected workflows     |

Start with A, then B and D because they remove prerequisites assumed by
the later guides. C builds on B; E builds on C. F can proceed independently
once its alpha installation profile is pinned. Its capacity-based examples
must include the minimum quota-manager reporting and member capacity-reader
configuration; they cannot assume the later operations package supplied it.
G builds on the corresponding
single- or multi-cluster examples. Establish H's intake early and finish its
automation after the existing drift is reconciled.

## Acceptance for every workflow

- State the audience, outcome, supported version/commit, and required hardware,
  storage and permissions before the first command. Identify optional pieces.
- Supply the complete public manifests and steps in dependency order. Reuse
  the same names across related guides. Introduce extra components only when
  the task needs them.
- Explain expected observations and next actions, including one relevant
  failure and recovery path. Command output must be captured or explicitly
  illustrative, never presented as an executed result when it was not.
- Keep architecture explanations focused on user decisions. Put exhaustive
  fields, state tables and developer implementation details in reference or
  contributor material and link to them.
- Check rendered content, anchors and navigation. Run the existing website
  tests and build. Check changed complete OME manifests against CRDs and
  relevant admission rules; render Helm values against the matching chart.
- Execute tutorials in disposable test environments appropriate to their
  requirements. Record any missing runtime execution instead of calling the
  workflow validated. Do not use an ambient production kubeconfig.
- Review changes for private information and public-only references before
  preparing a PR. Preserve maturity/deprecation markers and optional model
  management.

## Existing review work

The earlier review recorded nine unfinished pages: labels and annotations,
the runtime and traffic CLI references, benchmark output storage, Service
application protocols, development setup, writing docs, and the reference
and contributing landing pages. Recheck their current contents and reuse
relevant findings in the packages above; historical worktree/agent status is
not evidence that a current page is complete or incomplete.

The previous bug list is input to triage. Separate resolved bugs, current
product defects, documentation mistakes and missing tutorials. Link a finding
to a public source and an owner before carrying it into a new issue or PR.

## Known boundaries to preserve

- Standalone InferenceReplica is implemented on the public baseline. It does
  not imply that InferenceService can reference user-authored replicas, that
  every `kubectl ome` command accepts them, or that standalone manual migration,
  runtime pinning and automatic HPA/KEDA creation are available.
- Multi-cluster reconciliation is implemented but remains alpha and in
  development. `WorkloadCluster.clusterProfileRef` is not implemented.
- Canary capacity rounds each component independently. `maintainRatio` is
  rejected for canary in the public validator; document supported PD behavior
  without promising ratio-guarded canaries.
- The placement CLI projects the legacy contract and lags newer allocation
  plans. Direct resource status may be needed to inspect ClusterAffinity and
  SplitByCapacity; do not invent corresponding CLI output.
- The final manual canary gate currently has a controller/CLI phase mismatch.
  Verify and track the product defect separately from the documentation
  correction; a green documentation test cannot resolve it.

## Current execution record

- Initial source, Hugo delta and representative workflow audit: complete at
  `bc1f94db`. GitHub freshness remains unverified because fetch failed.
- Package A's authored changes are implemented on `docs/content-refresh`:
  standalone ownership and limitations, canary recovery/capacity clocks,
  controller startup settings, and the pod-batching guide with navigation and
  its Hugo redirect. Shared correctness fixes are in both sites. No commits
  or external publication performed.
- The first batch's website lint, Svelte checks, 209 tests across 21 files, and production build
  passed with the available Node 26 runtime. Node 22 is the CI target and
  remains the preferred check runtime. The new guide's Helm values rendered
  successfully against the local chart. No Kubernetes workflow was executed.
- Initial `make docs-drift`: five unmapped pages, thirteen changed source
  pages. Pod batching now has a mapping; the remaining intake is still open.
- API generation remains blocked: downloading pinned `genref` v0.28.0 failed
  because network connections were denied, including the escalated retry.
  Generated references remain unchanged. Rerun `make generate-apiref` when
  the generator can be installed, then review both generated copies.

### First-workflow implementation batch

- B's authored path is implemented: released versus source installation,
  a manager-only lab profile, optional model-agent/OME-agent builds, and a
  complete namespaced runtime plus Qwen InferenceService through an HTTP
  request and cleanup. Shared source-install corrections are also in Hugo.
- C now has a CPU-only HTTP lab through inspection, scaling and a template
  update. It distinguishes desired, ready, available and updated replicas,
  checks the new revision rather than a stale Ready condition, and explains
  controller ownership. Standalone forms, deliberate failures/recovery and
  leader/worker exercises are still to implement.
- D now has HF-to-PVC staging through copy completion, metadata extraction,
  serving and cleanup. It makes RWX placement, UID/GID, partial-copy risks,
  optional credentials and mutable HF revisions explicit. Runtime authoring
  and additional transfer recipes remain open.
- Both runtime-only workflows have public fixtures in
  `config/samples/docs/`. Website tests compare each displayed manifest with
  its fixture; Go tests call the current runtime and InferenceService
  validators and runtime resolver, including the CPU scale/template patches.
- H's intake now triggers on Hugo-content-only and documentation-fixture-only
  changes. Model replication has a redirect to the staging guide. The current
  drift report still lists three unmapped CLI pages and thirteen later Hugo
  source changes; these need content comparison, not blanket baseline bumps.

Validation performed on this batch:

| Check                                                  | Result and limit                                                                                                                                                                     |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Website lint, Svelte check, tests and production build | Passed; 215 tests in 22 files, zero Svelte errors/warnings, available Node 26 runtime rather than CI's Node 22                                                                       |
| Complete documentation YAML against the public CRDs    | 123 objects passed on an isolated envtest 1.30.3 API server with `KUBECONFIG=/dev/null`; skipped one ResourceFlavor and two PodGroups because their optional CRDs were not installed |
| Admission and runtime resolution                       | Both fixture families and CPU scale/template updates passed; full `hack/docs-examples` package also passed using isolated envtest                                                    |
| Source-install Helm values                             | Manager-only renders one Deployment and no DaemonSet; enabling the model agent adds its DaemonSet; all OME image references use the selected source tag                              |
| Workflow trigger configuration                         | YAML parses and positive/negative path checks pass for the new Hugo/sample triggers                                                                                                  |
| Actual workload execution                              | Not performed: no image pull, model transfer, serving request or lifecycle run; no ambient cluster contacted                                                                         |

The authored B/C/D work is not end-to-end validated. Next, execute the CPU
lab in a disposable environment and the Qwen/PVC routes with suitable GPU
and shared storage, recording the actual images, model snapshot, timings and
responses. In parallel, extend the same fixture family with standalone and
failure/recovery cases rather than starting unrelated examples. Complete A's
generated reference once the pinned generator is available. No commits,
image pushes or publication have been performed by this refresh session.

### Standalone, recovery and registration batch

- C now includes a standalone HTTP replica with its own Service, scale and
  template update, plus a runtime-reference alternative without a model.
  The guide distinguishes direct ownership, `spec.minReadySeconds`, live
  runtime resolution and unsupported pins/scalers/migration/scale-to-zero.
  Its actual fixtures and JSON patch pass admission; negative cases exercise
  the documented ownership and template-source boundaries.
- The HTTP lab now continues through a deliberate wrong-port readiness
  failure, parent freeze, correction and revision-aware recovery. Unit tests
  feed readiness evidence into the public failure-disposition code: this
  failure does not create a revision retry block. Reset, release-held and
  corrected-template recovery remain distinct operations. No shared operator
  retry configuration is changed by the exercise.
- F has its first registration-to-placement path: source-matched hub/member
  profiles, dedicated short-lived credentials, RBAC review, registration,
  one `ClusterAffinity`/`Single` CPU placement, readiness and HTTP checks,
  rotation caveats and ordered cleanup. Independent review checked that
  cross-namespace workload creation is described as trusted-controller
  authority, and that connection Ready is not treated as proof of workload
  permissions, capacity or adoption of a rotated credential.
- H's three previously unmapped CLI pages now map to the existing get,
  admin and rollout references after content/source comparison. Missing
  pagination, bounded-read, advisory-evidence and malformed-rollout details
  were reconciled. The get reference covers standalone replicas; its stale
  encoding claim was also corrected in Hugo. The thirteen older mapped
  source deltas remain a separate review queue.
- All new YAML and JSON patches are tied to checked-in fixtures by website
  tests. The registration source spec/runtime pass admission and runtime
  resolution checks; both role overlays render against the public chart.
  Four focused standalone controller tests also pass, covering name/selector
  derivation, runtime rendering without a model, and EndpointSlice ownership.

Final batch checks passed: website lint, Svelte check (zero errors/warnings),
230 tests across 22 files and production build on the available Node 26;
the full `hack/docs-examples` and `hack/docs-drift` test packages;
and 133 documentation objects against isolated envtest 1.30.3. The same
three optional ResourceFlavor/PodGroup objects were skipped for absent CRDs.
The drift report now has no unmapped Hugo pages and thirteen later source
deltas; its nonzero drift exit is expected until that review queue is closed.

These are source, admission, schema and synthetic lifecycle checks, not a
live failure/recovery or two-cluster execution. Live credential issuance,
registration, placement and serving remain unverified, as do the earlier
GPU/PVC routes. No kubeconfig credentials were read, no clusters contacted,
and no commits or publication performed.

Next authored work: extend the CPU fixtures to leader/worker and PD topology
before adding coordinated rollout failures; add a separately scoped capacity
placement profile rather than relabeling the CPU registration smoke test as
`SplitByCapacity`. Runtime authoring and the remaining Hugo deltas can proceed
independently. API-reference regeneration remains blocked as recorded above.
