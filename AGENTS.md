# AGENTS.md

Guidance for coding agents (Claude Code, Codex, and others) working in this repository.

## What this is

OME (Open Model Engine) is a Kubernetes operator for serving Large Language Models. It manages models and serving runtimes as custom resources, matches models to runtimes via weighted scoring, and generates the Kubernetes workloads that serve them. Go module: `sigs.k8s.io/ome`.

## Common commands

- `make test` — full unit-test suite with coverage. Runs `fmt`, `vet`, `manifests`, downloads envtest binaries, and builds the Rust `pkg/xet` library first (requires a Rust toolchain).
- `make test-no-xet` — same suite but skips the Rust xet build and the `ome-agent` cmd tests. Use this when no Rust toolchain is available.
- Single test: envtest-backed packages need `KUBEBUILDER_ASSETS`, e.g.
  `KUBEBUILDER_ASSETS="$(./bin/setup-envtest use 1.30 -p path)" go test ./pkg/controller/... -run TestName`
  (see the `TEST_ENV` block in the Makefile for the full environment). Pure-unit packages work with plain `go test ./pkg/<pkg>/... -run TestName`.
- `make integration-test` — Ginkgo tests under `tests/`.
- `make ci-lint` / `make lint-fix` — golangci-lint; `make fmt`, `make vet`, `make tidy` before submitting.
- `make generate && make manifests` — REQUIRED after any change to `pkg/apis/`. Regenerates deepcopy methods, client-go libraries, and the CRD/webhook/RBAC manifests in `config/crd/full`.
- `make dep-crds` — vendors optional third-party CRDs (KEDA, Gateway API, Kueue, scheduler-plugins) needed by some tests.
- `make ome-manager` / `make model-agent` / `make ome-agent` — build the individual binaries.

## Architecture

Three binaries in `cmd/`:

- **manager** — the operator control plane. Controllers live in `pkg/controller/v1beta1/` (inferenceservice is the core one: runtime selection, workload generation for Deployments/LeaderWorkerSet, traffic/ingress reconcilers, canary and blue-green rollout groups). Admission webhooks live in `pkg/webhook/`.
- **model-agent** — node-level agent that downloads models and parses metadata (architecture, parameter count, capabilities).
- **ome-agent** — job/sidecar agent for model download (Hugging Face, with Xet support via the Rust FFI library in `pkg/xet/`), replication, fine-tuned-adapter handling, and model encryption (enigma).

Key packages:

- `pkg/apis/ome/v1beta1/` — all CRD types (BaseModel/ClusterBaseModel, ServingRuntime/ClusterServingRuntime, InferenceService, FineTunedWeight, AcceleratorClass, BenchmarkJob). The WorkloadCluster and InferenceReplica APIs are merged but multi-cluster reconciliation is still in development — do not describe multi-cluster as a finished feature.
- `pkg/runtimeselector/` — weighted scoring that matches models to serving runtimes.
- `pkg/acceleratorclassselector/` — GPU selection policies (BestFit, Cheapest, MostCapable).
- `charts/` — `ome-crd` must be installed before `ome-resources`; `ome-serving` is an optional add-on of pre-configured models/runtimes/services.
- `config/models/` and `config/runtimes/` — the catalog of pre-configured models and SGLang (`srt`) / vLLM runtime definitions.
- `oeps/` — OME Enhancement Proposals. Major features and API changes require an OEP (see CONTRIBUTING.md); check `oeps/<n>/oep.yaml` for status before building on in-progress designs.
- `site/` — Hugo documentation site. Use Node 20 so `package-lock.json` stays in sync with CI.
- `hack/docs-examples/` and `hack/docs-drift/` — tooling for the redesigned documentation site, which lives in [ome-projects/ome-docs](https://github.com/ome-projects/ome-docs) and replaces `site/` at launch. That repository's CI runs these tools from a checkout of this one, at the commit pinned in its `ome.ref`, so keep them working when the API types or CRDs change. It also generates its API reference from the Go types in `pkg/apis/ome/v1beta1`, with its own templates. Until launch, documentation changes still go to `site/`.

## Conventions

### Commits

- Always DCO sign off (`git commit -s`); every commit carries a `Signed-off-by:` trailer.
- Author and sign-off identity must be the GitHub noreply address from `git config user.name` / `user.email` (`<id>+<user>@users.noreply.github.com`) — never a real email address.
- Never include AI attribution: no `Co-Authored-By: Claude` (or any `noreply@anthropic.com`) trailers, no "Generated with ..." lines, in commit messages or PR bodies.

### Code and PRs

- PR titles must carry one prefix: `[Bugfix]`, `[Core]`, `[API]`, `[Helm]`, `[Docs]`, `[CI/Tests]`, `[Misc]`, or `[OEP]`. Keep commit titles ≤52 characters, body lines ≤72.
- Code follows the [Google Go Style Guide](https://google.github.io/styleguide/go/).
- Pre-commit hooks are configured (`pip install pre-commit && pre-commit install`); they run gofmt, go vet, helm-lint (warnings as errors), codespell, and site lockfile sync.
- Bug fixes need a test that fails without the fix; rebase on latest `main` before submitting.
