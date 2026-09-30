# OME Docs Site Redesign Design

Status: Approved

Date: 2026-09-26

Baseline: `6502319d` (`[Bugfix] Re-pin Docsy and stop tidying site/go.mod (#1104)`)

## Context

The documentation site in `site/` is Hugo with the Docsy theme, published
to GitHub Pages at `ome-projects.github.io/ome`. An audit on 2026-09-26
found:

- search has no backend;
- the footer links were copied from Kueue's config and point at
  Kubernetes channels;
- analytics use a retired Universal Analytics ID, and the "Was this page
  helpful?" widget reports nowhere;
- nine internal links are broken;
- the version shown is v0.8.0;
- section landing pages are nearly empty.

Beyond the theme, the content has problems:

- Main has moved well past the last release. v1.2.2 (2026-08-04) shipped
  8 CRDs; main has 14. AcceleratorQuota, AutoscalerPolicy,
  InferenceReplica, RolloutPolicy, TrafficMap, WorkloadCluster and all of
  kubectl-ome are in no release, and no page says so.
- kubectl-ome has 17 top-level commands. Ten have a page, written as
  how-tos rather than reference. `cluster`, `get`, `instance`,
  `placement`, `quota`, `scale` and `version` have none.
- The home page claims TensorRT-LLM support, but `config/runtimes/` has
  no TensorRT-LLM runtime.

The SMG docs (`smg-project/smg-docs`, served at `lightseek.org/smg`) are
the model for the new site. They are a SvelteKit app on Cloudflare Pages
with a design system by Studio Noiich.

## Decisions

1. The new site is a sibling of SMG's. It uses SMG's design system and
   layout, with OME's own color (cobalt `#2447b8`) and a flat redraw of
   the OME orbit mark: a filled core, one orbit ring and a satellite, as
   in the approved mockups. SMG's brand marks are not reused.
2. It is server-rendered on Cloudflare like SMG, with D1 and live GitHub
   stats. `lightseek.org/ome` is the intended URL, so the base path is
   `/ome`. DNS and hosting are decided in the Launch spec.
3. The site lives in this repo, in `website/`, until there is a reason
   to move it.
4. Every page is rewritten. Nothing is ported as is.
5. This spec covers the foundation and the rewrite. Two later specs
   cover the rest: Automation (generated references, docs bot) and
   Launch (hosting, cutover, removing Hugo).
6. Markdown renders at build time, not per request.
7. The docs track main. A `since` marker labels behavior that isn't in a
   release yet.
8. The nightly docs bot keeps running on `site/` until launch. A drift
   script tracks what it changes, so the rewrite can fold those changes
   in.

## Goals

- The site looks and reads like SMG's docs, with OME's identity.
- Every current page is rewritten, verified against the code, and placed
  in one of five sections.
- Each kubectl-ome top-level command has one reference page.
- Search works on every page with no outside service.
- CI checks links, anchors, nav coverage and YAML examples.
- Readers can tell released behavior from unreleased behavior.

## Non-goals

- DNS, Cloudflare setup, a deploy workflow, the redirect site for
  `ome-projects.github.io/ome`, and removing Hugo. These belong to the
  Launch spec.
- Generating the CLI reference from cobra, moving the nightly docs bot
  and `docs-pr-worker` to `website/`, and replacing `build_site.py` and
  `install_hugo.py`. These belong to the Automation spec.
- Per-release doc versions.
- Moving the site to its own repo.
- Editing `site/`. It stays as is until launch.

## Information Architecture

There are five sections. Each section opens with a landing page of
cards, written as `index.md`. Groups within a section are directories.
URLs are file paths under `/ome/` without the `.md` extension, so
`guides/deploy-models/serve-models-from-pvc.md` is served at
`/ome/guides/deploy-models/serve-models-from-pvc`. The header links to
the first four sections; Contributing is linked from the footer and from
the Getting Started landing page.

Titles here are working titles; the rewrite sets the final ones. Paths
can change until launch, as long as `redirects.json` changes with them.
Page paths in each table are relative to the section directory, and
"Replaces" paths are relative to `site/content/en/docs/`.

### Getting Started (5 pages)

`getting-started/index.md` replaces the docs root, `_index.md`.

| Page | Title | Replaces |
|---|---|---|
| `introduction.md` | Introduction | `overview/_index.md` |
| `install.md` | Install OME | `installation/_index.md` |
| `serve-your-first-model.md` | Serve your first model | `tasks/run-workloads/deploy-inference-service.md` |
| `pre-configured-models.md` | Pre-configured models and runtimes | `installation/ome-serving.md` |
| `private-registries.md` | Install from a private registry | `installation/private-registries.md` |

### Guides (29 pages)

`guides/index.md` replaces `tasks/_index.md` and
`administration/_index.md`.

| Page | Title | Replaces |
|---|---|---|
| `deploy-models/select-accelerators.md` | Select accelerators | `tasks/run-workloads/select-accelerators.md` |
| `deploy-models/serve-models-from-pvc.md` | Serve models from a PVC | `tasks/run-workloads/serve-models-from-pvc.md` |
| `deploy-models/serve-models-from-local-storage.md` | Serve models from node-local storage | `tasks/run-workloads/serve-models-from-local-storage.md` |
| `deploy-models/reference-a-runtime-explicitly.md` | Reference a runtime explicitly | `tasks/run-workloads/reference-a-runtime-explicitly.md` |
| `deploy-models/troubleshoot-runtime-selection.md` | Troubleshoot runtime selection | `tasks/run-workloads/troubleshoot-runtime-selection.md` |
| `deploy-models/run-benchmarks.md` | Run benchmarks | `tasks/run-workloads/run-benchmarks.md` |
| `networking/configure-route-timeouts.md` | Configure route timeouts | `tasks/run-workloads/configure-route-timeouts.md` |
| `networking/set-service-app-protocols.md` | Set appProtocol on services | `tasks/run-workloads/set-service-app-protocols.md` |
| `networking/configure-ingress.md` | Configure ingress | `administration/ingress.md` |
| `networking/gateway-host-schemes.md` | Choose a Gateway API host scheme | `administration/gateway-host-schemes.md` |
| `networking/multiple-gateways.md` | Use multiple gateways | `administration/multiple-gateways.md` |
| `networking/namespace-gateways.md` | Use per-namespace gateways | `administration/namespace-gateways.md` |
| `roll-out-changes/pause-and-resume-a-rollout.md` | Pause and resume a rollout | `tasks/pause-and-resume-a-rollout.md` |
| `roll-out-changes/promote-or-roll-back-a-canary.md` | Promote or roll back a canary | `tasks/promote-or-rollback-a-canary.md` |
| `roll-out-changes/release-a-held-revision.md` | Release a held revision | `tasks/release-a-held-revision.md` |
| `roll-out-changes/repin-a-drifted-rollout-plan.md` | Repin a drifted rollout plan | `tasks/repin-a-drifted-rollout-plan.md` |
| `roll-out-changes/pace-rollouts-with-min-ready-seconds.md` | Pace rollouts with minReadySeconds | `tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md` |
| `scale-and-migrate/request-a-transient-scale.md` | Request a transient scale | `tasks/request-a-transient-scale.md` |
| `scale-and-migrate/request-an-instance-migration.md` | Request an instance migration | `tasks/request-an-instance-migration.md` |
| `multi-cluster/routing-health-probes.md` | Configure routing health probes (preview) | `administration/routing-health-probes.md` |
| `multi-cluster/drain-a-workload-cluster.md` | Drain a workload cluster (preview) | `tasks/drain-traffic-from-a-workload-cluster.md` |
| `operate-ome/configure-the-controller.md` | Configure the controller | `administration/controller-configuration.md` |
| `operate-ome/model-agent.md` | Run the model agent | `administration/model-agent.md` |
| `operate-ome/ome-scheduler.md` | Use the OME scheduler | `administration/ome-scheduler.md` |
| `operate-ome/accelerator-quota.md` | Set accelerator quotas | `administration/accelerator-quota.md` |
| `operate-ome/shared-hf-artifacts.md` | Share Hugging Face artifacts | `administration/shared-hf-artifacts.md` |
| `operate-ome/metrics.md` | Collect metrics | `administration/metrics.md` |
| `operate-ome/alerting.md` | Set up alerting | `administration/alerting.md` |
| `troubleshoot/troubleshoot-an-inferenceservice.md` | Troubleshoot an InferenceService | new |

The troubleshooting guide shows which kubectl-ome commands to run, in
order. It takes the how-to content out of the current kubectl-ome pages.

### Concepts (18 pages)

`concepts/index.md` replaces `concepts/_index.md`.

| Page | Title | Replaces |
|---|---|---|
| `architecture/how-ome-works.md` | How OME works | new |
| `architecture/deployment-modes.md` | Deployment modes and OMENative | `concepts/omenative.md` |
| `architecture/omenative-update-strategies.md` | OMENative update strategies | `concepts/omenative-update-strategies.md` |
| `models/base-models.md` | Base models | `concepts/base_model.md` |
| `models/fine-tuned-weights.md` | Fine-tuned weights | `concepts/fine_tuned_weight.md` |
| `runtimes/serving-runtimes.md` | Serving runtimes | `concepts/serving_runtime.md` |
| `runtimes/runtime-inheritance.md` | Runtime inheritance | `concepts/runtime_inheritance.md` |
| `runtimes/runtime-revisions.md` | Runtime revisions and pinning | `concepts/runtime-revision.md` |
| `runtimes/accelerator-classes.md` | Accelerator classes | `concepts/accelerator_class.md` |
| `serving/inference-services.md` | InferenceService | `concepts/inference_service.md` |
| `serving/gang-scheduling.md` | Gang scheduling | `concepts/gang_scheduling.md` |
| `serving/autoscaler-policy.md` | Autoscaler policy | `concepts/autoscaler_policy.md` |
| `serving/benchmarks.md` | Benchmarks | `concepts/benchmark.md` |
| `rollouts-and-traffic/rollout-policy.md` | Rollout policy | `concepts/rollout_policy.md` |
| `rollouts-and-traffic/rollout-groups.md` | Rollout groups | `concepts/rollout_groups.md` |
| `rollouts-and-traffic/traffic-policy.md` | Traffic policy | `concepts/traffic_policy.md` |
| `rollouts-and-traffic/traffic-map.md` | Traffic map (preview) | `concepts/traffic_map.md` |
| `rollouts-and-traffic/ingress.md` | Ingress and external access | `concepts/ingress.md` |

### Reference (29 pages)

`reference/index.md` replaces `reference/_index.md`.

| Page | Title | Replaces |
|---|---|---|
| `api/ome.v1beta1.md` | OME API (generated) | `reference/ome.v1beta1.md` |
| `api/labels-and-annotations.md` | Labels and annotations | `reference/labels-and-annotations.md` |
| `api/traffic-annotations.md` | Traffic annotations | `reference/traffic-annotations.md` |
| `kubectl-ome/overview.md` | kubectl-ome overview and install | `tasks/kubectl-ome.md` |
| `kubectl-ome/accelerator.md` | kubectl ome accelerator | `tasks/kubectl-ome-accelerator-explain.md` |
| `kubectl-ome/admin.md` | kubectl ome admin | `tasks/kubectl-ome-doctor.md` |
| `kubectl-ome/autoscale.md` | kubectl ome autoscale | `tasks/kubectl-ome-autoscale-status.md`, `tasks/kubectl-ome-autoscale-explain.md` |
| `kubectl-ome/cluster.md` | kubectl ome cluster (preview) | new |
| `kubectl-ome/get.md` | kubectl ome get | new |
| `kubectl-ome/instance.md` | kubectl ome instance | new |
| `kubectl-ome/logs.md` | kubectl ome logs | `tasks/kubectl-ome-logs.md` |
| `kubectl-ome/migration.md` | kubectl ome migration | `tasks/kubectl-ome-migration.md` |
| `kubectl-ome/placement.md` | kubectl ome placement (preview) | new |
| `kubectl-ome/quota.md` | kubectl ome quota | new |
| `kubectl-ome/rollout.md` | kubectl ome rollout | `tasks/kubectl-ome-rollout-explain.md`, `tasks/kubectl-ome-rollout-history.md`, `tasks/kubectl-ome-rollout-validate.md` |
| `kubectl-ome/runtime.md` | kubectl ome runtime | `tasks/kubectl-ome-runtime-effective.md`, `tasks/kubectl-ome-runtime-tree.md` |
| `kubectl-ome/scale.md` | kubectl ome scale | new |
| `kubectl-ome/status.md` | kubectl ome status | `tasks/kubectl-ome-status.md` |
| `kubectl-ome/traffic.md` | kubectl ome traffic | `tasks/kubectl-ome-traffic-explain.md`, `tasks/kubectl-ome-traffic-status.md` |
| `kubectl-ome/version.md` | kubectl ome version | new |
| `kubectl-ome/wait.md` | kubectl ome wait | `tasks/kubectl-ome-wait.md` |
| `kubectl-ome/guarded-actions.md` | Guarded actions | `reference/kubectl-ome-guarded-actions.md` |
| `matching/model-version-matching.md` | Model version matching | `reference/model-version-matching.md` |
| `matching/runtime-accelerator-class-matching.md` | Runtime accelerator-class matching | `reference/runtime-accelerator-class-matching.md` |
| `matching/runtime-deployment-mode-matching.md` | Runtime deployment-mode matching | `reference/runtime-deployment-mode-matching.md` |
| `matching/diffusion-pipeline-runtime-matching.md` | Diffusion pipeline runtime matching | `reference/diffusion-pipeline-runtime-matching.md` |
| `rollouts/canary-progression.md` | Canary progression | `reference/canary-progression.md` |
| `rollouts/canary-analysis.md` | Canary metric analysis | `reference/canary-analysis.md` |
| `storage/benchmark-output-storage.md` | Benchmark output storage | `reference/benchmark-output-storage.md` |

Each command page has a section per subcommand. For example, `admin`
covers `doctor` and `recommendations`.

### Contributing (3 pages)

`contributing/index.md` replaces `developer-guide/_index.md`.

| Page | Title | Replaces |
|---|---|---|
| `development-setup.md` | Set up a development environment | `developer-guide/contributing.md` |
| `pull-requests-and-oeps.md` | Pull requests and OEPs | part of `developer-guide/contributing.md` |
| `writing-docs.md` | Writing docs | new |

### Preview pages

Multi-cluster routing is alpha. Five pages are purely about it and get
`status: preview`:

- Configure routing health probes;
- Drain a workload cluster;
- Traffic map;
- `kubectl ome cluster`;
- `kubectl ome placement`.

Pages that mention InferenceReplica are not tagged, because
InferenceReplica also backs single-cluster OMENative services. Alpha
subcommands on other pages, such as `rollout repin`, get a note under
their heading instead.

## Home Page

The home page keeps SMG's home page section for section, with copy
written for OME:

1. **Hero:** "The Kubernetes operator for serving LLMs in production" and
   "Declare a model. OME picks the runtime, places it on the right GPUs,
   and rolls out every change safely." It has Get Started and View on
   GitHub buttons. Clicking the mark toggles SMG's ambient shader,
   recolored to cobalt.
2. **Metrics:**
   - 200+ pre-configured models (Llama, Qwen, DeepSeek, Gemma, and more);
   - 200+ runtime configs, tuned for SGLang, vLLM and TokenSpeed;
   - 3 deployment modes: OMENative, Deployment, or LWS (deprecated);
   - 17 kubectl ome commands to inspect, explain and act.

   These values are fixed in code, so `website/` doesn't depend on the
   rest of the repo. On 2026-09-26, `config/` had 207 models and 210
   runtimes.
3. **Works with:** SGLang, vLLM, TokenSpeed and the SMG router in the
   first row; Hugging Face, Kueue, KEDA and Gateway API in the second.
   LeaderWorkerSet was dropped when OME deprecated its LWS-backed mode.
   SMG is the router image in 192 of the 210 runtime configs. The
   Foundation PR ships the name pills from the mockup; logos replace
   them once each project's usage terms are checked.
4. **Why OME:** a paragraph and eight expandable items, led by the
   OMENative workload engine: models as Kubernetes resources, automatic
   runtime selection, GPU-aware placement, any serving topology, safe
   rollouts, the OME scheduler (alpha) and Alfred (alpha).
5. **How it works:** a diagram running from what you declare
   (InferenceService, BaseModel, ServingRuntime, AcceleratorClass),
   through selection and the controller, to OMENative pods, a
   Deployment or a deprecated LeaderWorkerSet, with the OME scheduler
   and Alfred in a "Runs alongside" group.
6. **Choose your path:** four cards leading to Getting Started, Guides,
   Concepts and Reference.
7. **Footer:** Open Model Engine, © OME Contributors, GitHub, Issues and
   Contributing.

The header badge shows `ome-projects/ome`, the star count and the latest
release, all fetched live. The hero title and subtitle live in D1, so
they can change without a deploy; they fall back to the copy above. The
other sections are code. SMG's beige surfaces shift slightly cooler to
sit with cobalt: the mockup uses `#eef0f5` for panels and `#d6dae3` for
the hero fade. The TensorRT-LLM claim is dropped.

## Doc Page Template

Kept from SMG unchanged:

- the three-column layout: sidebar, page, table of contents;
- collapsible sidebar groups;
- Edit and View source buttons;
- tabs, details, card grids and the prerequisites box;
- copy buttons on code blocks;
- SMG's mkdocs-style authoring syntax (`=== "Tab"`, `!!! warning`).

New for OME:

- **Callouts:** four types, each with its own color. They are note
  (`#2447b8`), tip (`#1f7a4d`), warning (`#a15c00`) and danger
  (`#b42318`). SMG styles every callout the same orange.
- **Preview status:** `status: preview` adds a Preview badge next to the
  title and a standard callout: "Multi-cluster routing is alpha. Fields
  and behavior can change between releases."
- **Draft status:** `status: draft` shows a banner in place of the
  body, linking to the Hugo page or pages the new page replaces.
- **Since badges:** see [Versioning](#versioning).
- **Code blocks:** a `title="model.yaml"` attribute shows a file name.
  An `output` fence renders command output in a light block with no
  copy button.
- **Navigation:** breadcrumbs, and previous and next links at the bottom
  of each page.
- **Search (⌘K):** a dialog on every page, plus a `/search` page.
- **CLI reference template:** synopsis, flags (including a pointer to
  the standard kubeconfig flags), output fields, exit codes, examples,
  and related guides.
- **API reference template:** a Field, Type and Description table with
  required badges and linked types.
- **404 page:** a short message with the search box.

### Front matter

```yaml
title: Serve models from a PVC        # required: H1, <title>, nav label
navLabel: Serve from a PVC            # optional: shorter sidebar label
description: Mount weights from ...   # required: lead, meta, search, cards
status: preview                       # optional: draft or preview
since: v1.3                           # optional: whole page is unreleased
generated: true                       # optional: set by genref; hides Edit
```

Unknown keys, a missing `title` or `description`, an unknown `status`,
or a `since` that isn't `vMAJOR.MINOR` fail the build.

### Authoring syntax

On top of SMG's syntax:

- `!!! note|tip|warning|danger "Optional title"` with an indented body.
  Other types fail the build.
- ```` ```yaml title="model.yaml" ```` for a titled block.
- ```` ```output ```` for command output.
- ```` ```yaml check=skip ```` to keep an intentionally invalid example
  out of the YAML check. The surrounding text must say why it is
  invalid. A config file with an `apiVersion` and `kind` that isn't
  applied to a cluster, such as a kubeconfig, needs it too.
- `## Heading {#custom-id}` sets an explicit id. Without it, the id is
  the slugified text, with `-1`, `-2` suffixes for duplicates.
- `## Heading {since=v1.3}` adds a since badge. The two combine:
  `{#custom-id since=v1.3}`.
- Internal links are relative `.md` paths with an optional `#anchor`,
  such as `../../concepts/models/base-models.md#storage`. They also
  work when browsing the files on GitHub.

## Architecture

### Location

The site is self-contained in `website/`. It has its own
`package.json`, pnpm lockfile, configs and CI workflow, and nothing in it
reaches into `../`. The repo, branch and content path live in one
constant, so the Edit links and GitHub badge follow if the site moves to
its own repo. `docs/` was not used because it reads like a folder of
Markdown and already holds specs. `site/` stays Hugo until launch.

```text
website/
  package.json  pnpm-lock.yaml  pnpm-workspace.yaml  .nvmrc  .node-version
  svelte.config.js  vite.config.ts  wrangler.jsonc  drizzle.config.ts
  drizzle/                      D1 migrations (hero copy)
  redirects.json                old-to-new page table
  src/lib/config/site.ts        repo, branch, content path, site name
  src/lib/config/nav.ts         sections, groups and page paths
  src/lib/content/<section>/    Markdown pages
  src/lib/markdown/             renderer and Vite plugin (build only)
  src/lib/server/               GitHub stats and D1 content blocks
  src/lib/components/           SMG components, recolored, and new ones
  src/routes/+page.svelte       home
  src/routes/[section=section]/[...slug]/   every doc and landing page
  src/routes/search/            search page
  src/routes/search.json/       prerendered search index
  src/routes/+error.svelte      404
```

### Starting point

The app starts from a copy of smg-docs and keeps its stack: SvelteKit 2
and Svelte 5 on `@sveltejs/adapter-cloudflare`, D1 through Drizzle,
marked, highlight.js and gsap, with Node 22 and pnpm 10. The base path is
set once, in `svelte.config.js`. These are not copied:

- SMG's brand marks, which are Studio Noiich's work for SMG;
- SMG's content and nav;
- the nightly doc sync, which belongs to the Automation spec.

### Build-time rendering

SMG bundles raw Markdown and renders it on every request. SMG's own
renderer, timed on OME pages, takes 1 to 2.5 ms warm and 23 to 39 ms for
the first render in a fresh process, mostly compiling highlight.js
grammars. The Workers free plan allows 10 ms of CPU per request, so the
first page a new isolate serves could fail.

A Vite plugin renders each `.md` file during the build. It parses and
validates the front matter, renders the body with marked and
highlight.js, and produces:

- a metadata module (title, nav label, description, status, since,
  generated flag, table of contents, heading ids and outgoing links),
  imported eagerly for nav, breadcrumbs, cards and checks;
- an HTML module, imported only when its page is served;
- plain text without code blocks, read only by the prerendered search
  endpoint.

The worker renders the page shell per request, so stars, the latest
release and the D1 hero copy stay live. marked and highlight.js leave
the worker bundle.

### Content registry and routing

One registry globs `src/lib/content/**/*.md` and looks pages up by exact
path. `x/index.md` serves `x`. One route,
`[section=section]/[...slug]`, serves every doc page and every landing
page; the `section` matcher accepts only the five sections, and unknown
paths return 404. SMG's per-section loaders match by filename suffix,
and its concepts landing page only works because `./index.md` sorts
before `./reliability/index.md`. Exact lookup removes that failure mode.

### Navigation

`nav.ts` lists each section's groups and their page paths in order.
Labels come from front matter (`navLabel`, else `title`). Breadcrumbs
show the section and group. Previous and next links follow nav order
within the section. Draft pages appear in the nav like any other page.

### Links

The renderer resolves relative `.md` links against the current file and
rewrites them to site URLs under the base path. SMG's per-section
special cases in `links.ts` go away. Links that leave
`src/lib/content/`, absolute links to the site's own paths, and links
to the Hugo site are errors. Links to other repo files use full GitHub
URLs.

### Search

`search.json` is prerendered at build time from the registry. Each entry
holds the page path, title, section, group, description, headings and
body text without code. The browser fetches it on the first ⌘K and
searches it with MiniSearch; `/search?q=` uses the same index. Draft
pages are indexed by title and description.

### GitHub stats and D1

`src/lib/server/github.ts` keeps SMG's caching (a memory cache plus the
edge cache: 15 minutes fresh, 24 hours stale, 60-second retry, 4-second
timeout, optional `GITHUB_TOKEN`). It points at `ome-projects/ome` and
also fetches the latest release. When a fetch fails, the badge hides that
value.

D1 holds only the hero copy, in SMG's `content_blocks` table. Section
landing pages are Markdown. Without a D1 binding, as in local
development, the page uses the default copy.

### Versioning

The docs track main. `since` marks behavior that isn't in every release.
It can be set in front matter for a page or on a heading. The page
compares it with the latest release, which the layout already fetches
for the badge:

- If `since` is newer than the latest release, the badge reads
  "Unreleased: coming in v1.3".
- If it is the same or older, the badge reads "New in v1.3".
- If the latest release is unknown, the badge reads "Since v1.3".

The comparison happens at request time, on the server, so no redeploy is
needed when a release ships. The build emits heading badges as spans
with a `data-since` attribute, and the page updates their text.

### API reference

`make generate-apiref` also writes the reference into
`website/src/lib/content/reference/api/ome.v1beta1.md`, using a second
set of genref templates under `hack/genref/website/`. They emit the
front matter above with `generated: true`, headings with `{#id}` anchors
for all 74 types, and the Field, Type and Description table from the doc
template. The Hugo copy is generated as before until launch.

### Edit and source links

Edit links go to
`https://github.com/ome-projects/ome/edit/main/website/src/lib/content/<path>`
and View source links to the raw file, both built from the constant in
`site.ts`. Generated pages show View source only.

### Repo plumbing

- `.github/workflows/website.yml` runs the checks in
  [Test Strategy](#test-strategy).
- Dependabot gets an npm entry for `/website`.
- CODEOWNERS gets a `/website/` line with the `/site/` owners.
- The labeler matches `website/**` for the documentation label.
- codespell skips `website/pnpm-lock.yaml`.
- `claude-code-review.yml` ignores `website/src/lib/content/**`, as it
  ignores `site/**`.
- AGENTS.md gets one line: `website/` is the redesigned site that
  replaces `site/` at launch, and until then doc changes still go to
  `site/`.
- NOTICE credits smg-docs.

There is no deploy workflow; that is the Launch spec. Until then,
review happens locally with `pnpm dev`.

## Content Plan

### Drafts first

The Foundation PR adds every page in the IA. Most are drafts: a title,
a description, `status: draft` and nothing else. The draft banner links
to the Hugo page or pages the new page replaces, found by reverse lookup
in `redirects.json`. New pages with nothing to replace say they haven't
been written yet. Nav, links and search work from the first PR, and the
link check never fails because a linked page isn't written yet.

The same PR fully writes the pages that set the voice:

- a guide: Serve models from a PVC;
- a concept: Base models;
- a command reference: `kubectl ome rollout`;
- the generated API reference;
- the five section landing pages;
- Writing docs.

### Order

After the Foundation PR, pages are rewritten in PRs of 5 to 10 pages, in
this order: Getting Started, Concepts, Guides, Reference, Contributing.
Concepts come before Guides because guides use their terms.

### Style

The Writing docs page is the style guide:

- Guide titles start with a verb, concept titles are nouns, and
  reference pages are named after the thing. All titles use sentence
  case.
- Second person, present tense, active voice.
- Guides follow one structure: Before you begin, numbered steps that
  each end with a check, Troubleshooting, Clean up, Next steps.
- Examples are complete and runnable, with concrete names instead of
  placeholders.
- Commands and their output go in separate blocks. Where a command's
  default output is too wide to read, the example selects columns so
  the output block shows exactly what the reader sees.
- The current `> **Note:**` blockquotes become callouts.

### Accuracy

Every field, default, flag and output is checked against the code at the
commit the PR is based on, not against the old page. CLI output comes
from the golden files in `pkg/cli/**/testdata` where they exist. YAML
examples go through the check in [Test Strategy](#test-strategy).
Behavior that isn't in v1.2.2 gets a `since` marker.

### Old-to-new table

`website/redirects.json` has one entry per current page:

```json
{ "old": "tasks/run-workloads/serve-models-from-pvc.md",
  "new": "guides/deploy-models/serve-models-from-pvc.md",
  "rewrittenFrom": "6502319d" }
```

`old` is relative to `site/content/en/docs/` and `new` to
`website/src/lib/content/`. When an old page's content is split across
new pages, `new` names the main one, which is also the redirect target.
`rewrittenFrom` is the commit whose version of the old page the rewrite
was checked against; it is `null` while the new page is a draft. At
launch this table becomes the redirect list.

### Drift

The nightly docs bot wrote 67 of the last 72 docs commits, about two a
day, and the Hugo site stays live until launch. Pausing the bot would
leave readers with weeks of stale docs, so it keeps running on `site/`.
`hack/docs-drift` reports:

- current pages missing from `redirects.json`;
- entries whose old page no longer exists;
- entries whose old page changed after `rewrittenFrom` (entries with
  `rewrittenFrom: null` are skipped, and so are entries whose new page
  has `generated: true`, such as the API reference, which is rebuilt
  from the Go types);
- entries whose new page doesn't exist.

Content PRs fold the reported changes in and move `rewrittenFrom`
forward.

## Test Strategy

### Website unit tests (Vitest)

- **Renderer:** heading ids (slugs, duplicates, `{#id}`), `since`
  attributes, code titles, `output` fences, the four callouts and the
  error for other types, tabs, and each front matter error.
- **Registry:** path-to-URL mapping, `index.md` pages, and exact lookup.
  A regression test puts `concepts/index.md` next to
  `concepts/x/index.md` and checks that both resolve.
- **Nav:** every page except section landings appears in the nav
  exactly once, and every nav entry resolves.
- **Links:** every internal link and `#anchor` in the content resolves.
  Links out of `src/lib/content/`, absolute self-links and links to the
  Hugo site fail.
- **Search index:** every page has one entry, code is excluded, headings
  are included, and every entry's URL resolves.
- **Redirects:** entries are well formed, `old` values are unique, and
  every `new` resolves.
- **Since:** the unreleased, released and unknown-release cases.

### Static checks

`pnpm lint` (prettier and eslint), `pnpm check` (`wrangler types`, then
`svelte-check`), and `pnpm build`.

### YAML examples (Go)

`hack/docs-examples` is a Go program, run by `make docs-examples`. For
each YAML block in the content that isn't marked `check=skip`, it splits
the documents and keeps those with `apiVersion` and `kind`. Each of
those needs `metadata.name` or `metadata.generateName`, so a config file
with an `apiVersion` and `kind`, such as a kubeconfig or a
Kustomization, is marked `check=skip`. When it finds no objects at
all, it fails, because the content path must be wrong. Otherwise it
starts an envtest API server (Kubernetes 1.30, the version `make test`
uses) with the CRDs from `config/crd/full`, creates any namespaces the
objects use, and dry-run creates each object with strict field
validation. Kinds from CRDs that aren't installed are skipped and
listed. Failures print the file, line and message.

This catches unknown fields, wrong types, enum values, missing required
fields and CEL rules. It does not run the admission webhooks, so
webhook-only rules stay a review item.

### Drift script (Go)

`hack/docs-drift` has table tests against a temporary git repo, and
`make docs-drift` runs it.

### CI

`website.yml` runs on every pull request, so its final check always
reports, and on pushes to main that touch the paths below. A first job
diffs the pull request against its base, and the other jobs run only
when a changed file is under `website/`, `hack/docs-examples/`,
`hack/docs-drift/`, `hack/genref/`, `config/crd/full/` or `pkg/apis/`,
or is `go.mod`, `go.sum`, `Makefile`, `Makefile-deps.mk` or the
workflow itself. CRD and API changes are included because they can
break examples and the API reference, and the Go and make files because
the checks build with them. Jobs:

- lint, check, test and build for the site;
- the Go tests for both hack programs and `make docs-examples`, which
  share one envtest install at the Makefile's `ENVTEST_K8S_VERSION`;
- `make generate-apiref`, which fails if the website's API reference
  page changes;
- the drift report, which never fails the workflow and writes its
  report to the job summary, because the bot changes `site/` daily;
- a final job for branch protection. It fails if the diff job failed or
  a blocking job didn't pass, and passes when no matching file changed.

A new push to a pull request cancels its running checks. Runs on main
aren't cancelled.

The repo-wide pre-commit run in `pr-validation.yml` also covers
`website/`.

## Delivery

1. This spec and its implementation plan.
2. The Foundation PR: the app, repo plumbing, all IA pages as drafts,
   the voice-setting pages, `redirects.json`, both hack programs and the
   genref templates.
3. Content PRs of 5 to 10 pages each, in the order above. Each PR
   replaces drafts with written pages, sets `rewrittenFrom` for the old
   pages it covers, and passes every check.

PR titles use the `[Docs]` prefix.

## Prerequisites

- smg-docs has no LICENSE. It needs Apache-2.0 before the Foundation PR
  is published, so OME can take the code cleanly with a credit in
  NOTICE.
- The layout and styles are Studio Noiich's design work, separate from
  the brand marks. Confirm that the license covers reusing them.

## Risks

- **Worker limits:** build-time rendering removes the per-request
  Markdown cost, and lazy HTML modules keep isolate startup small. The
  Launch spec checks bundle size against the chosen Workers plan.
- **Drift:** the bot changes `site/` daily. The drift script makes that
  visible, and launch requires a clean report.
- **Stale since markers:** markers name the next planned minor release.
  If the release gets a different number, the markers must change; the
  Automation spec can check this at release time.
- **Two sites during the rewrite:** contributors could edit the wrong
  one. AGENTS.md says which site takes changes, and the drift script
  catches what lands in `site/`.
- **Scale:** 89 pages is a lot of rewriting. Drafts keep the site
  complete at every step, and small PRs keep reviews short.

## Launch Gates

This spec is complete when:

- no page has `status: draft`;
- `make docs-drift` reports nothing;
- every check in `website.yml` passes on main.

Launch itself is the next spec.

## Later Specs

- **Automation:** generate the CLI reference's synopsis and flags from
  cobra; move the nightly docs bot and `docs-pr-worker` to `website/`;
  replace `build_site.py` and `install_hugo.py`.
- **Launch:** Cloudflare Pages project and D1 database, DNS for
  `lightseek.org/ome`, a deploy workflow, a redirect-only site for
  `ome-projects.github.io/ome` generated from `redirects.json`, removing
  `site/` and its workflows, and updating AGENTS.md, CODEOWNERS and the
  labeler.

## Expected Result

`website/` builds and passes its checks, every page in the IA is written
and verified against the code, the drift report is clean, and the site is
ready for the Launch spec.
