# Code Review Guidelines

Read `AGENTS.md` for repository architecture, commands, and conventions.

## Skills

Use the installed review toolkit on changed files:

1. **`pr-review-toolkit:silent-failure-hunter`** — check swallowed errors,
   inappropriate fallbacks, and missing error propagation.
2. **`pr-review-toolkit:pr-test-analyzer`** — check coverage of new or changed
   behavior, especially reconciliation retries and failure paths.
3. **`pr-review-toolkit:type-design-analyzer`** — use when new types are introduced
   to check invariants, encapsulation, and API compatibility.

## Severity

Prefix every inline comment with one of these markers:

| Marker | Severity | Meaning |
|--------|----------|---------|
| 🔴 | **Important** | A bug that should be fixed before merging |
| 🟡 | **Nit** | A minor issue, worth fixing but not blocking |
| 🟣 | **Pre-existing** | A bug not introduced by this PR |

Explain the concrete trigger, impact, and relevant code. Avoid speculative
findings. After posting inline comments, give a brief count per severity.

## Focus on

- Reconciliation correctness: idempotency, conflict retries, status conditions,
  owner references, finalizers, deletion handling, and safe requeues.
- API compatibility: CRD validation, defaults, optional fields, JSON tags,
  deepcopy/client generation, and regenerated manifests after API changes.
- Runtime and accelerator selection: deterministic scoring, supported model
  constraints, resource requests, and node placement.
- Workload generation: Deployments and LeaderWorkerSets, engine/decoder/router
  configuration, multi-node and prefill/decode layouts, traffic routing, and
  canary or blue-green transitions.
- Model lifecycle: download/replication failures, cancellation, partial files,
  cleanup, concurrent access, and propagation of storage/authentication errors.
- Security: RBAC scope, namespace isolation, admission validation, credential
  handling, and secrets exposed in logs or generated resources.
- Helm and manifest consistency: chart defaults, CRDs, RBAC, webhooks, and
  installation ordering (`ome-crd` before `ome-resources`).
- Tests that exercise changed behavior, error paths, and regression scenarios.

## Domain knowledge

- The manager reconciles Kubernetes resources; model-agent handles node-level
  model downloads and metadata; ome-agent handles download, replication,
  fine-tuned adapters, and encryption jobs/sidecars.
- Model, runtime, and accelerator selection must agree with the workload that
  the InferenceService controller generates.
- Check the status of related OEPs before assuming an API has a complete
  implementation. WorkloadCluster and InferenceReplica APIs alone do not imply
  that multi-cluster reconciliation is finished.
- Tests using envtest need Kubernetes binaries and the appropriate CRDs.
  `make test-no-xet` skips the Rust xet build and ome-agent command tests;
  it does not establish coverage of those paths.

## Skip

- Formatting-only changes and preferences already enforced by linters.
- Documentation-only changes under `site/`.
- Dependency version bumps with no behavior changes.
