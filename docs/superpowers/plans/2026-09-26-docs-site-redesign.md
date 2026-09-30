# OME Docs Site Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Foundation PR for the redesigned docs site in `website/`, then rewrite every remaining draft page in content batches.

**Architecture:** `website/` starts from a copy of smg-docs (SvelteKit 2 and Svelte 5 on Cloudflare, D1 through Drizzle) recolored to OME cobalt. A Vite plugin renders each Markdown page at build time into a metadata module, a lazily imported HTML module and search text. One `[section=section]/[...slug]` route serves every page through an exact-path registry. Two Go programs in `hack/` check YAML examples against the CRDs in envtest and report drift between `site/` and the rewrite.

**Tech Stack:** SvelteKit 2, Svelte 5 (runes), TypeScript 6, Vite 8, Vitest 5, marked 18, highlight.js 11, MiniSearch 7, gsap 3, Drizzle with Cloudflare D1, wrangler 4, pnpm 10, Node 22; Go 1.26 with controller-runtime envtest.

**Spec:** [`docs/superpowers/specs/2026-09-26-docs-site-redesign-design.md`](../specs/2026-09-26-docs-site-redesign-design.md). Read it before starting any task. This plan says how; the spec says what and why.

---

## How this plan runs

### Phases

| Phase | Tasks | Runs | Starts from |
|---|---|---|---|
| A. Foundation | A1 app and content pipeline, A2 repo plumbing, A3 YAML example check, A4 drift report | In parallel, one worktree each | This plan's commit |
| B. Pages and UI | B1 docs UI and search, B2 home page, B3 generated API reference, B4 voice-setting pages | In parallel, one worktree each | Phase A merged |
| C. Integration | C1 merge, verify, review | One task | Phase B merged |
| D. Content | D1 to D11, 5 to 9 pages each | Two waves: D1 to D3 in parallel, then D4 to D11 in parallel | `docs/website-content`, which the orchestrator creates from `docs/site-redesign` once C1 is done; wave 2 starts after all of wave 1 has merged into it |

Phases A to C produce the Foundation PR. Each Phase D batch is its own PR. Batches own disjoint pages and `redirects.json` entries, so the batches in a wave merge in any order. Wave 1 goes first because the Guides and Reference pages in wave 2 link to headings on wave 1's Getting Started and Concepts pages; for the same reason, wave 2's PRs follow wave 1's.

### Ground rules

These apply to every task.

1. **Stay in your worktree and branch.** Commit only to your task branch. Don't push, don't open PRs, and don't touch files another task owns (see [File ownership](#file-ownership)).
2. **Commits.** Use this exact form:

   ```bash
   git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Short title"
   ```

   Titles are at most 52 characters and start with `[Docs]`, or `[CI/Tests]` for workflow-only commits. Body lines are at most 72 characters. Never add `Co-Authored-By` trailers, AI attribution or "Generated with" lines.
3. **Tools.** pnpm 10 lives outside the default PATH on the development machine. Run `export PATH=/tmp/ome-tools/bin:$PATH` in every shell that calls pnpm. Node 22 or newer works; `website/scripts/ensure-node22.mjs` only switches versions on older Node. Run pnpm commands from `website/`.
4. **Reference checkouts.** These paths exist on the development machine:
   - `/tmp/smg-docs-ref`: smg-project/smg-docs at `0cd30b3e1b9d44fd213394ce08764afb555a9aa2`. "Copy from SMG" means from here.
   - `/tmp/ome-render-proto`: the renderer prototype. Its `src/lib/**` is reproduced in [Appendix A](#appendix-a-renderer-and-registry-source); copy the files rather than retyping them.
   - `/tmp/plan-parts`: `nav.ts`, `redirects.json`, `pages.tsv`, `descriptions.tsv` and `make-drafts.py`, reproduced in Appendices B and C.
   - The approved mockups: `/Users/simolin/go/src/github.com/ome-projects/ome/.superpowers/brainstorm/68202-1790451826/content/home-page-v2.html` and `doc-template.html`. They are not in git, so worktrees don't have them; use the absolute paths.
5. **Checks before every commit.** For `website/` changes, run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build`, and fix every failure. For Go changes, run `go vet` and the package tests. Then run `~/.local/bin/pre-commit run --files <changed files>`; the hooks include trailing-whitespace, end-of-file-fixer, check-yaml, a 500 KB file size limit, codespell and go-mod-tidy. In a sandbox, go-mod-tidy can fail with "operation not permitted" on the Go module cache. That's the sandbox, not your change, as long as `git diff --exit-code go.mod go.sum` passes; rerun the other hooks with `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files <changed files>`.
6. **No new Go modules.** `go.mod` must not change; use packages already required, such as `sigs.k8s.io/controller-runtime`, `sigs.k8s.io/yaml` and `k8s.io/apimachinery`.
7. **Style.** Go follows the Google Go Style Guide. TypeScript and Svelte follow SMG's Prettier config (tabs, single quotes, no trailing commas, width 100). Comments explain why, in the density of the surrounding code. Svelte components use runes (`$props`, `$state`, `$derived`, `$effect`).
8. **Report back** with the branch name, the commit list, the output of the final checks, and anything that deviated from this plan and why.

### File ownership

A file has one owner per phase. Files created in Phase A can be edited in Phase B only by the listed Phase B owner.

| Files under `website/` unless noted | Phase A | Phase B |
|---|---|---|
| Configs, `package.json`, lockfile, `scripts/`, `drizzle/`, `static/favicon.svg`, `static/robots.txt` | A1 | none (B1 and B2 may add dependencies; C1 merges the lockfile) |
| `src/app.html`, `src/app.d.ts` | A1 | none |
| `src/hooks.server.ts` | added after Phase A, to answer paths outside the base with a 404 | none |
| `src/app.css` | A1 | B1, header rules only |
| `src/styles/docs.css`, `src/styles/search.css` | A1 creates | B1 |
| `src/styles/home.css` | A1 creates | B2 |
| `src/lib/markdown/**`, `src/lib/docs/**`, `src/lib/server/**`, `src/lib/stores/**`, `src/params/**` | A1 | none |
| `src/lib/config/site.ts`, `nav.ts`, `header.ts` | A1 | none |
| `src/lib/config/home.ts` | A1 creates | B2 |
| `src/routes/+layout.svelte`, `+layout.server.ts`, `+page.server.ts`, `search.json/**`, `[section=section]/[...slug]/+page.server.ts` | A1 | none |
| `src/routes/[section=section]/[...slug]/+page.svelte`, `src/routes/search/**`, `src/routes/+error.svelte` | A1 creates the first | B1 |
| `src/routes/+page.svelte` | A1 creates | B2 |
| `src/lib/components/SiteHeader.svelte`, `GitHubBadge.svelte` | A1 | B1 (SiteHeader only, to mount search) |
| `src/lib/components/SiteFooter.svelte`, `OrbitMark.svelte` | A1 | B2 |
| `src/lib/components/Doc*.svelte`, `Search*.svelte`, `Breadcrumbs.svelte`, `PageNav.svelte`, `src/lib/ui/**` | none | B1 |
| `src/lib/components/Home*.svelte`, `HeroMark.svelte`, `HeroShaderBackground.svelte`, `SectionLabel.svelte`, `PlusMark.svelte`, `ChoosePathArrow.svelte`, `src/lib/actions/**` | none | B2 |
| `src/lib/content/**` | A1 creates all 89 drafts | B3: `reference/api/ome.v1beta1.md`. B4: the five `index.md` landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md` and `reference/kubectl-ome/rollout.md`. B1: `contributing/writing-docs.md` |
| `redirects.json` | A1 | B3 and B4 set `rewrittenFrom` for their pages' entries; `writing-docs.md` is new and has no entry |
| `.github/**`, `.pre-commit-config.yaml`, `AGENTS.md`, `NOTICE` (repo root) | A2 | none |
| `hack/docs-examples/**`, Makefile target `docs-examples` (after `generate-apiref`) | A3 | none |
| `hack/docs-drift/**`, Makefile target `docs-drift` (after `help`, before `generate-apiref`) | A4 | none |
| `hack/genref/website/**`, `hack/genref/config.yaml`, Makefile target `generate-apiref`, `pkg/apis/ome/v1beta1/inference_service_status.go` (one comment) (repo root) | none | B3 |

In Phase D, a batch owns only its pages, their `redirects.json` entries and, when it changes a page's `title` or `description`, that page's landing card. See [What a batch owns](#what-a-batch-owns).

### Shared interfaces

Tasks in the same phase depend on each other only through these contracts. Don't change one without updating this section.

#### Renderer markup (A1 emits, B1 styles)

The renderer in `src/lib/markdown/render.ts` emits exactly this HTML. B1 styles these classes in `src/styles/docs.css` and doesn't change the renderer.

| Element | Markup |
|---|---|
| Section heading | `<h2 id="x">Text<a class="doc-headerlink" href="#x" aria-label="Permanent link">¶</a></h2>`; same for `h3`. `h1` in Markdown is a build error. |
| Since badge on a heading | `<span class="doc-since doc-since--{unreleased,released,unknown}" data-since="v1.3">Unreleased: coming in v1.3</span>` inside the heading, before the headerlink. The build emits it without the state class; the page loader adds the class and label per request. |
| Code block | `<div class="doc-code"><div class="doc-code-bar"><span class="doc-code-title">model.yaml</span><button type="button" class="doc-code-copy" aria-label="Copy to clipboard">Copy</button></div><pre class="doc-pre"><code class="hljs language-yaml">…</code></pre></div>`. Without `title="…"`, the title is the language label, such as `YAML` or `Shell`. |
| Output block | `<div class="doc-code doc-code--output"><div class="doc-code-bar"><span class="doc-code-title">Output</span></div><pre class="doc-pre"><code class="language-output">…</code></pre></div>`; no copy button. |
| Callout | `<aside class="doc-admonition doc-admonition--{note,tip,warning,danger}"><p class="doc-admonition-title">Title</p>…</aside>`; the title paragraph is omitted when the Markdown gives none. |
| Details | `<details class="doc-details doc-details--{type}"[ open]><summary>Title</summary>…</details>` |
| Tabs | `<div class="doc-tabbed-set"><input type="radio" class="doc-tab-input" name="doc-tab-set-N" id="doc-tab-N-0" checked>…<div class="doc-tab-labels"><label class="doc-tab-label" for="doc-tab-N-0">Tab</label>…</div><div class="doc-tab-panels"><div class="doc-tab-panel">…</div>…</div></div>`. Inputs come first so CSS can select the panel for the checked input with `:nth-of-type` and `~`. |
| Prerequisites | `<aside class="doc-prerequisites"><h2 id="before-you-begin">Before you begin<a class="doc-headerlink" …>¶</a></h2>…</aside>` |
| Card grid | `<div class="doc-grid"><div class="doc-card">…</div>…</div>` |
| Image | `<img src="/ome/images/x.svg" alt="…" loading="lazy">` |

Authoring syntax, for the Writing docs page: `!!! note "Title"` and `??? tip "Title"` (closed) or `???+ tip "Title"` (open) with a 4-space indented body; `=== "Tab"` with an indented body; `<div class="prerequisites" markdown>` … `</div>`; `<div class="grid cards" markdown>` … `</div>` whose items are `-   ` list entries; fences with a language and optional `title="…"`, `output` fences, and `yaml check=skip`; `## Heading {#id since=v1.3}`.

#### Landing cards (B4 writes, B1 styles)

Each section's `index.md` has one card per page in the section's nav, in nav order. Cards for a labeled nav group sit under an `h2` with the group's label; cards for an unlabeled group come before the first `h2`. A card is the page's title, linked, then its description, both exactly as in the page's front matter:

```markdown
## Deploy models

<div class="grid cards" markdown>

-   **[Serve models from a PVC](deploy-models/serve-models-from-pvc.md)**

    Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes.

</div>
```

It renders as:

```html
<div class="doc-grid"><div class="doc-card"><p><strong><a href="/ome/guides/deploy-models/serve-models-from-pvc">Serve models from a PVC</a></strong></p>
<p>Serve model weights that already live on a PersistentVolumeClaim …</p>
</div>…</div>
```

The title is the card's only link, so there's no "Read more" link text repeated on every card. `landing.py`, in Task B4 Step 1, prints a section's cards and checks all five landing pages. A task that changes a page's `title` or `description`, or the nav, updates the card and runs `python3 landing.py check`.

#### Doc page data (A1 returns, B1 renders)

`src/routes/[section=section]/[...slug]/+page.server.ts` returns:

```ts
{
	title: string;                 // page title; the layout builds <title> from it
	description: string;           // lead paragraph and <meta name="description">
	page: {
		path: string;              // content path, such as 'guides/index.md'
		route: string;             // URL path without base, such as 'guides'
		section: SectionId;
		title: string;
		status: 'draft' | 'preview' | null;
		generated: boolean;
		toc: TocEntry[];           // { depth: 2 | 3; id: string; text: string }[]; empty for drafts
	};
	html: string;                  // rendered body with since labels applied; '' for drafts
	tree: NavTree;                 // the section's sidebar, from $lib/docs/navigation
	position: PagePosition;        // { group: string | null; prev: NavLink | null; next: NavLink | null }
	since: { state: SinceState; label: string } | null;   // page-level since badge
	replaces: { old: string; url: string }[];             // drafts only: Hugo pages to link from the banner
	editUrl: string | null;        // null for generated pages
	sourceUrl: string;
}
```

The layout data also has `github: GitHubRepoStats` (`{ fullName, url, version, stars, forks }`, where `version` is the latest release tag or null).

#### Search (A1 provides, B1 uses)

- `GET {base}/search.json` is prerendered and returns `SearchEntry[]` (see `src/lib/docs/types.ts`).
- `loadSearch(base)` from `$lib/docs/search-client` fetches and indexes it once per page load and resolves to `search(query, limit = 20): SearchHit[]`, where `SearchHit` is `{ route, title, sectionLabel, group, description, status }`. A failed fetch rejects and is retried on the next call.
- Result URLs are `${base}/${hit.route}`.

#### API reference markup (B3 emits, B1 styles)

`reference/api/ome.v1beta1.md` is Markdown with raw HTML tables:

```markdown
## `BaseModel` {#ome-io-v1beta1-BaseModel}

BaseModel is the Schema for the basemodels API.

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>spec</code> <span class="doc-api-required">Required</span></td>
<td><a href="#ome-io-v1beta1-BaseModelSpec"><code>BaseModelSpec</code></a></td>
<td>

Where the weights live and how to serve them.

</td></tr>
</tbody>
</table>
```

Type links inside the table are raw `<a href="#…">` because Markdown links don't render inside HTML blocks; the blank lines around the description let Markdown render inside the cell. Descriptions escape `<` and `>`. External types link to their upstream docs with `https://` URLs.

The 14 kinds are `h2` headings. Every other type is an `h3` under `## Supporting types`, and starts with a "Used by" line that links the types using it. An embedded field's row has `<em>Embedded</em>` in the Field column instead of a name, and its description ends with "The fields of `X` appear directly in this object.", where `X` is the embedded type's short name. The page has about 250 headings, so B1 lists only the `h2` headings in its TOC.

#### Palette and mark (A1 defines, all tasks use)

`src/app.css` replaces every SMG `--smg-*` variable with an `--ome-*` one:

| Variable | Value | Replaces |
|---|---|---|
| `--ome-accent` | `#2447b8` | `--smg-orange` (`#b35a11`) |
| `--ome-accent-deep` | `#1b3690` | `--smg-orange-deep` (`#9a4d0e`) |
| `--ome-surface` | `#ffffff` | `--smg-surface` |
| `--ome-detail-surface` | `#eef0f5` | `--smg-detail-surface` (`#f2f0eb`) |
| `--ome-detail-chip` | `#dde3f0` | `--smg-detail-chip` (`#e8dcc8`) |
| `--ome-detail-stack` | `#e3e7ef` | `--smg-detail-stack` (`#ebe8e3`) |
| `--ome-gradient-mid` | `#d6dae3` | `--smg-gradient-mid` (`#d9d9d9`) |
| `--ome-orbit` | the orbit mark as a CSS mask (below) | `--smg-spark` |
| `--ome-note` | `#2447b8` | new |
| `--ome-tip` | `#1f7a4d` | new |
| `--ome-warning` | `#a15c00` | new |
| `--ome-danger` | `#b42318` | new |

Hard-coded warm colors are replaced too: `rgba(179, 90, 17, a)` becomes `rgba(36, 71, 184, a)` with the same alpha, `#ebe8e3` becomes `#e3e7ef`, `#e8dcc8` becomes `#dde3f0`, `#f2f0eb` becomes `#eef0f5`, and `#d9d9d9` and `#d9d7d2` become `#d6dae3`. Syntax highlighting colors stay as SMG has them.

```css
--ome-orbit: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 40 40'%3E%3Ccircle cx='20' cy='20' r='8' fill='%23000'/%3E%3Ccircle cx='20' cy='20' r='14' fill='none' stroke='%23000' stroke-width='2'/%3E%3Ccircle cx='31' cy='9.5' r='3' fill='%23000'/%3E%3C/svg%3E");
```

The mark is `src/lib/components/OrbitMark.svelte`: a 40×40 viewBox with a filled core (`r=8`), an orbit ring (`r=14`, stroke 2, opacity 0.8) and a satellite (`r=3` at `31, 9.5`), all in `currentColor`. It replaces every SMG brand mark (Logo, Symbol, FooterMark, ShiftMark, HeroMark's shapes); those files are never copied.

## Phase A: Foundation

Dispatch A1 to A4 at the same time, each in its own worktree branched from this plan's commit. They touch disjoint files. A1 is the largest; A2 to A4 are small.

### Task A1: The app and the content pipeline

**Branch:** `docs/website-app`

**Goal:** `website/` builds, serves all 89 IA pages as drafts through the build-time pipeline, and passes lint, check, test and build. The docs page and home page are deliberately minimal; B1 and B2 build the real UI on top.

**Files:**

- Copy from SMG, then modify: `website/{.gitignore,.node-version,.npmrc,.nvmrc,.prettierignore,.prettierrc,drizzle.config.ts,eslint.config.js,pnpm-workspace.yaml,svelte.config.js,tsconfig.json,vite.config.ts,wrangler.jsonc}`, `website/scripts/ensure-node22.mjs`, `website/drizzle/{0000_init.sql,meta/_journal.json,meta/0000_snapshot.json}`, `website/static/robots.txt`, `website/src/{app.html,app.d.ts,app.css}`, `website/src/lib/server/{github.ts,content.ts,db/index.ts,db/schema.ts}`, `website/src/lib/stores/hero-shader.ts`, `website/src/lib/components/{SiteHeader,SiteFooter,GitHubBadge}.svelte`, `website/src/routes/{+layout.svelte,+layout.server.ts,+page.server.ts}`
- Copy from the prototype (Appendix A): `website/src/lib/markdown/*`, `website/src/lib/docs/*`, `website/src/lib/config/site.ts`
- Copy from Appendix B: `website/src/lib/config/nav.ts`, `website/redirects.json`
- Generate: the 89 drafts under `website/src/lib/content/`
- Create: `website/package.json`, `website/pnpm-lock.yaml`, `website/README.md`, `website/drizzle/0001_seed_content.sql`, `website/static/favicon.svg`, `website/src/styles/{docs,home,search}.css`, `website/src/lib/config/{header,home}.ts`, `website/src/lib/docs/{pages,page-data}.ts` and their tests, `website/src/lib/components/OrbitMark.svelte`, `website/src/params/section.ts`, `website/src/routes/+page.svelte`, `website/src/routes/[section=section]/[...slug]/{+page.server.ts,+page.svelte}`, `website/src/routes/search.json/+server.ts`

Never copy SMG's `.github/`, `.vscode/`, `README.md`, `_redirects`, `scripts/doc-sync/`, `worker-configuration.d.ts`, `static/favicon.png`, `static/images/`, `src/lib/content/`, `src/lib/config/*nav.ts`, `src/lib/markdown/`, `src/lib/assets/`, the per-section routes, `src/routes/search/`, or any component other than the three listed. The brand marks (Logo, Symbol, FooterMark, ShiftMark, HeroMark) are Studio Noiich's work for SMG and must not appear in OME.

- [ ] **Step 1: Copy the SMG scaffold**

Run from the worktree root:

```bash
W="$PWD/website"
S=/tmp/smg-docs-ref
mkdir -p "$W"/{scripts,static,drizzle/meta,src/lib/server/db,src/lib/stores,src/lib/components,src/routes,src/styles}
cp "$S"/{.gitignore,.node-version,.npmrc,.nvmrc,.prettierignore,.prettierrc,drizzle.config.ts,eslint.config.js,pnpm-workspace.yaml,svelte.config.js,tsconfig.json,vite.config.ts,wrangler.jsonc} "$W"/
cp "$S"/scripts/ensure-node22.mjs "$W"/scripts/
cp "$S"/drizzle/0000_init.sql "$W"/drizzle/
cp "$S"/drizzle/meta/{_journal.json,0000_snapshot.json} "$W"/drizzle/meta/
cp "$S"/static/robots.txt "$W"/static/
cp "$S"/src/{app.html,app.d.ts,app.css} "$W"/src/
cp "$S"/src/lib/server/{github.ts,content.ts} "$W"/src/lib/server/
cp "$S"/src/lib/server/db/{index.ts,schema.ts} "$W"/src/lib/server/db/
cp "$S"/src/lib/stores/hero-shader.ts "$W"/src/lib/stores/
cp "$S"/src/lib/components/{SiteHeader,SiteFooter,GitHubBadge}.svelte "$W"/src/lib/components/
cp "$S"/src/routes/{+layout.svelte,+layout.server.ts,+page.server.ts} "$W"/src/routes/
```

- [ ] **Step 2: Write `website/package.json` and install**

```json
{
	"name": "ome-website",
	"private": true,
	"version": "0.0.1",
	"type": "module",
	"scripts": {
		"node:use": "fnm install && fnm use",
		"dev": "vite dev",
		"build": "node scripts/ensure-node22.mjs build",
		"preview": "node scripts/ensure-node22.mjs preview",
		"prepare": "svelte-kit sync || echo ''",
		"check": "node scripts/ensure-node22.mjs check",
		"check:watch": "svelte-kit sync && svelte-check --tsconfig ./tsconfig.json --watch",
		"gen": "node scripts/ensure-node22.mjs gen",
		"test": "vitest run",
		"lint": "prettier --check . && eslint .",
		"format": "prettier --write .",
		"db:generate": "drizzle-kit generate",
		"db:migrate:local": "node scripts/ensure-node22.mjs db:migrate:local"
	},
	"dependencies": {
		"gsap": "^3.15.0",
		"minisearch": "^7.2.0"
	},
	"devDependencies": {
		"@eslint/js": "^10.0.1",
		"@sveltejs/adapter-cloudflare": "^7.2.8",
		"@sveltejs/kit": "^2.57.0",
		"@sveltejs/vite-plugin-svelte": "^7.0.0",
		"@types/node": "^22.20.4",
		"drizzle-kit": "^0.31.10",
		"drizzle-orm": "^0.45.2",
		"eslint": "^10.4.0",
		"eslint-config-prettier": "^10.1.8",
		"eslint-plugin-svelte": "^3.17.0",
		"globals": "^17.4.0",
		"highlight.js": "^11.12.0",
		"marked": "^18.0.14",
		"prettier": "^3.8.1",
		"prettier-plugin-svelte": "^3.5.1",
		"svelte": "^5.55.2",
		"svelte-check": "^4.4.6",
		"typescript": "^6.0.3",
		"typescript-eslint": "^8.58.1",
		"vite": "^8.3.1",
		"vitest": "^5.0.2",
		"wrangler": "^4.81.0",
		"yaml": "^2.9.1"
	}
}
```

marked, highlight.js and yaml are devDependencies because they run only at build time; gsap and MiniSearch ship to the browser.

```bash
cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install
```

Expected: `pnpm-lock.yaml` is created and `prepare` runs `svelte-kit sync`.

- [ ] **Step 3: Adapt the configs**

`svelte.config.js`: change `base: '/smg'` to `base: '/ome'`. Nothing else changes.

`vite.config.ts`, replacing SMG's (which bundled raw Markdown with `assetsInclude`):

```ts
import { sveltekit } from '@sveltejs/kit/vite';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';
import svelteConfig from './svelte.config.js';
import { omeMarkdown } from './src/lib/markdown/plugin.ts';

const dir = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
	plugins: [
		// Renders Markdown at build time; must run before SvelteKit sees the imports.
		omeMarkdown({
			basePath: svelteConfig.kit?.paths?.base ?? '',
			contentDir: dir('./src/lib/content'),
			staticDir: dir('./static')
		}),
		sveltekit()
	],
	test: {
		include: ['src/**/*.test.ts'],
		environment: 'node'
	}
});
```

The renderer's modules import each other with `.ts` extensions, and this file imports the plugin the same way. Vite's native config loader, which Vite plans to make the default, needs the extensions; without them every Vite command prints a warning listing each import. SMG's `tsconfig.json` already sets `rewriteRelativeImportExtensions`, which lets TypeScript accept them. Keep the extensions if you touch those files.

`tsconfig.json`: set `"types": ["./worker-configuration.d.ts", "node"]` (SMG lists only the first). The build-time renderer imports `node:fs` and `node:path`. If svelte-check reports that `glob` doesn't exist on `ImportMeta`, add `"vite/client"` to the same list.

`wrangler.jsonc`: set `"name": "ome"`, `"database_name": "ome-db"`, and `"database_id": "00000000-0000-0000-0000-000000000000"` with the comment `// Placeholder until the Launch spec creates the database.` above it. Keep the compatibility date, flags, build output dir, binding name `DB` and `migrations_dir`.

`scripts/ensure-node22.mjs`: replace the `commands` object. OME gitignores `worker-configuration.d.ts` (SMG's is 541 KB, over the repo's file size limit), so `build` and `check` regenerate it instead of checking it:

```js
const commands = {
	build: 'wrangler types && vite build',
	preview: 'wrangler pages dev .svelte-kit/cloudflare --port 4173',
	check: 'wrangler types && svelte-kit sync && svelte-check --tsconfig ./tsconfig.json',
	gen: 'wrangler types',
	'db:migrate:local': 'wrangler d1 migrations apply ome-db --local'
};
```

`.gitignore`: delete the two `.claude/` lines and their comment, and append:

```gitignore

# Generated by `wrangler types`; `pnpm check` and `pnpm build` regenerate it.
worker-configuration.d.ts
```

`.prettierignore`: replace the `# Mirrored verbatim from smg/docs …` comment above `src/lib/content/` with `# Prettier would rewrite the mkdocs-style blocks in the Markdown pages.` Keep every other line.

`eslint.config.js`: replace the two-line comment above `'svelte/no-navigation-without-resolve': 'off'` with `// Links are built from base and content routes, not resolve().` Keep the rest.

`src/app.html`: delete the `favicon.png` line. Keep the SVG favicon and the Inter font links.

`src/app.d.ts`: delete the `declare module '*.md?raw'` block. Keep `Env` and `App.Platform`.

`website/README.md`:

````markdown
# OME website

The redesigned OME documentation site, served at `lightseek.org/ome` from
launch. Until launch, documentation changes still go to `../site/`.

## Develop

You need Node 22 or newer and pnpm 10.

```bash
pnpm install
pnpm dev
```

The site runs at http://localhost:5173/ome.

## Check

```bash
pnpm lint && pnpm check && pnpm test && pnpm build
```

Pages live in `src/lib/content/`, and `src/lib/config/nav.ts` orders them.
The Writing docs page, `src/lib/content/contributing/writing-docs.md`,
covers the syntax and style.

The layout and styles come from
[smg-docs](https://github.com/smg-project/smg-docs).
````

- [ ] **Step 4: Copy the renderer, registry, nav and redirects**

```bash
P=/tmp/ome-render-proto
mkdir -p website/src/lib/{markdown,docs,config}
cp "$P"/src/lib/markdown/*.ts website/src/lib/markdown/
cp "$P"/src/lib/docs/*.ts website/src/lib/docs/
cp "$P"/src/lib/config/site.ts website/src/lib/config/
cp /tmp/plan-parts/nav.ts website/src/lib/config/nav.ts
cp /tmp/plan-parts/redirects.json website/redirects.json
```

The files must match Appendices A and B. If `/tmp/ome-render-proto` or `/tmp/plan-parts` is missing, recreate the files from the appendices. They are already Prettier-formatted with SMG's settings, so `pnpm format` shouldn't change them. `/tmp/plan-parts/a1-validated` holds the validated `page-data.ts`, `pages.ts` and their tests; don't copy them, because Steps 7 to 10 add them test-first.

- [ ] **Step 5: Generate the drafts and run the tests**

```bash
python3 /tmp/plan-parts/make-drafts.py /tmp/plan-parts/pages.tsv /tmp/plan-parts/descriptions.tsv website/src/lib/content
cd website && pnpm test
```

Expected: `wrote 89 drafts; 0 pages already existed`, then 13 test files and 136 tests passing. `content.test.ts` renders every page and runs the nav, link, redirect and search checks. The script is in Appendix C.

- [ ] **Step 6: Commit the scaffold and pipeline**

```bash
cd website && pnpm format && pnpm lint && pnpm test
cd .. && git add website
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Scaffold website with build-time rendering" -m "Start website/ from smg-docs and add the Markdown pipeline, the
content registry, nav.ts, redirects.json and all 89 IA pages as
drafts."
```

`pnpm check` and `pnpm build` can still fail at this point, because routes and components aren't adapted yet. They must pass from Step 12 on.

- [ ] **Step 7: Write the failing page data test**

Create `website/src/lib/docs/page-data.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { docPageData } from './page-data';
import type { RedirectEntry } from './redirects';
import { createRegistry } from './registry';
import { testPage } from './testing';
import type { NavSection } from './types';

const nav: NavSection[] = [
	{
		id: 'guides',
		label: 'Guides',
		groups: [{ label: 'Deploy models', pages: ['deploy/a.md', 'deploy/b.md'] }]
	},
	{ id: 'reference', label: 'Reference', groups: [{ label: 'API', pages: ['api/ome.v1beta1.md'] }] }
];

const pages = [
	testPage('guides/index.md', { title: 'Guides' }),
	testPage('guides/deploy/a.md', {
		title: 'A',
		toc: [{ depth: 2, id: 'x', text: 'X' }],
		anchors: ['x'],
		links: [{ route: 'guides', anchor: null }]
	}),
	testPage('guides/deploy/b.md', { title: 'B', status: 'draft' }),
	testPage('reference/api/ome.v1beta1.md', { title: 'OME API', generated: true, since: 'v1.3' })
];

const html: Record<string, string> = {
	'guides/index.md': '<p>Landing</p>',
	'guides/deploy/a.md':
		'<h2 id="x">X<span class="doc-since" data-since="v1.3">Since v1.3</span></h2>',
	'reference/api/ome.v1beta1.md': '<p>API</p>'
};

const loaded: string[] = [];
const registry = createRegistry(pages, async (path) => {
	loaded.push(path);
	return html[path] ?? '';
});

const redirects: RedirectEntry[] = [
	{ old: 'tasks/a.md', new: 'guides/deploy/a.md', rewrittenFrom: 'abc1234' },
	{ old: 'tasks/b.md', new: 'guides/deploy/b.md', rewrittenFrom: null },
	{ old: 'tasks/b-more.md', new: 'guides/deploy/b.md', rewrittenFrom: null }
];

const context = { registry, nav, redirects, latestRelease: 'v1.2.2' };
const page = (path: string) => registry.getPageByPath(path)!;

describe('docPageData', () => {
	it('labels since badges against the latest release', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.html).toContain('class="doc-since doc-since--unreleased"');
		expect(data.html).toContain('Unreleased: coming in v1.3');
		expect(data.since).toBeNull();
	});

	it('places the page in its section', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.tree.landing?.route).toBe('guides');
		expect(data.position).toMatchObject({
			group: 'Deploy models',
			prev: { route: 'guides' },
			next: { route: 'guides/deploy/b' }
		});
	});

	it('sends only the metadata the page renders', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.page).toEqual({
			path: 'guides/deploy/a.md',
			route: 'guides/deploy/a',
			section: 'guides',
			title: 'A',
			status: null,
			generated: false,
			toc: [{ depth: 2, id: 'x', text: 'X' }]
		});
		expect(data.title).toBe('A');
		expect(data.description).toBe('About guides/deploy/a.md.');
	});

	it('links written pages to GitHub', async () => {
		const data = await docPageData(page('guides/deploy/a.md'), context);
		expect(data.editUrl).toBe(
			'https://github.com/ome-projects/ome/edit/main/website/src/lib/content/guides/deploy/a.md'
		);
		expect(data.sourceUrl).toBe(
			'https://raw.githubusercontent.com/ome-projects/ome/main/website/src/lib/content/guides/deploy/a.md'
		);
		expect(data.replaces).toEqual([]);
	});

	it('gives drafts no body and links the Hugo pages they replace', async () => {
		loaded.length = 0;
		const data = await docPageData(page('guides/deploy/b.md'), context);
		expect(data.html).toBe('');
		expect(loaded).toEqual([]);
		expect(data.replaces).toEqual([
			{ old: 'tasks/b.md', url: 'https://ome-projects.github.io/ome/docs/tasks/b/' },
			{ old: 'tasks/b-more.md', url: 'https://ome-projects.github.io/ome/docs/tasks/b-more/' }
		]);
	});

	it('hides the edit link on generated pages and labels page-level since', async () => {
		const data = await docPageData(page('reference/api/ome.v1beta1.md'), context);
		expect(data.editUrl).toBeNull();
		expect(data.sourceUrl).toContain('/reference/api/ome.v1beta1.md');
		expect(data.since).toEqual({ state: 'unreleased', label: 'Unreleased: coming in v1.3' });
	});

	it('falls back to a neutral label when the latest release is unknown', async () => {
		const data = await docPageData(page('reference/api/ome.v1beta1.md'), {
			...context,
			latestRelease: null
		});
		expect(data.since).toEqual({ state: 'unknown', label: 'Since v1.3' });
	});

	it('fails for a section missing from nav.ts', async () => {
		const orphan = testPage('concepts/x.md');
		await expect(docPageData(orphan, context)).rejects.toThrow('nav.ts has no section concepts');
	});
});
```

Run: `pnpm test src/lib/docs/page-data.test.ts`. Expected: FAIL, because `./page-data` doesn't exist.

- [ ] **Step 8: Implement `page-data.ts`**

Create `website/src/lib/docs/page-data.ts`:

```ts
import { editUrl, sourceUrl } from '../config/site';
import { buildNavTree, pagePosition, type NavTree, type PagePosition } from './navigation';
import { legacyUrl, replacedPages, type RedirectEntry } from './redirects';
import type { Registry } from './registry';
import { applySinceLabels, sinceLabel, sinceState, type SinceState } from './since';
import type { NavSection, PageMeta } from './types';

/** What the doc page route returns. B1's page component renders exactly this. */
export interface DocPageData {
	title: string;
	description: string;
	page: Pick<PageMeta, 'path' | 'route' | 'section' | 'title' | 'status' | 'generated' | 'toc'>;
	/** Rendered body with since labels applied; empty for drafts. */
	html: string;
	tree: NavTree;
	position: PagePosition;
	/** Page-level since badge. */
	since: { state: SinceState; label: string } | null;
	/** For drafts, the Hugo pages the banner links to. */
	replaces: { old: string; url: string }[];
	/** Null for generated pages, which are edited through their generator. */
	editUrl: string | null;
	sourceUrl: string;
}

export interface DocPageContext {
	registry: Registry;
	nav: readonly NavSection[];
	redirects: readonly RedirectEntry[];
	/** Latest release tag, such as `v1.2.2`, or null when GitHub is unreachable. */
	latestRelease: string | null;
}

/** Builds a doc page's data. Since labels are applied here, per request, so a release needs no redeploy. */
export async function docPageData(
	page: PageMeta,
	{ registry, nav, redirects, latestRelease }: DocPageContext
): Promise<DocPageData> {
	const section = nav.find((entry) => entry.id === page.section);
	if (!section) throw new Error(`nav.ts has no section ${page.section}`);
	const tree = buildNavTree(section, registry.getPageByPath);
	const draft = page.status === 'draft';

	return {
		title: page.title,
		description: page.description,
		page: {
			path: page.path,
			route: page.route,
			section: page.section,
			title: page.title,
			status: page.status,
			generated: page.generated,
			toc: page.toc
		},
		html: draft ? '' : applySinceLabels(await registry.loadHtml(page.path), latestRelease),
		tree,
		position: pagePosition(tree, page.route),
		since: page.since
			? {
					state: sinceState(page.since, latestRelease),
					label: sinceLabel(page.since, latestRelease)
				}
			: null,
		replaces: draft
			? replacedPages(redirects, page.path).map((old) => ({ old, url: legacyUrl(old) }))
			: [],
		editUrl: page.generated ? null : editUrl(page.path),
		sourceUrl: sourceUrl(page.path)
	};
}
```

Run: `pnpm test src/lib/docs/page-data.test.ts`. Expected: PASS, 8 tests.

- [ ] **Step 9: Write the failing registry wiring test**

Create `website/src/lib/docs/pages.test.ts`. It runs the real Vite plugin through `import.meta.glob`, the same way the app loads pages:

```ts
import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { registry } from './pages';

const contentDir = fileURLToPath(new URL('../content', import.meta.url));

describe('pages', () => {
	it('registers every Markdown file through the build-time plugin', () => {
		const files = readdirSync(contentDir, { recursive: true }).filter((file) =>
			String(file).endsWith('.md')
		);
		expect(registry.pages).toHaveLength(files.length);
		expect(registry.getPage('guides')?.path).toBe('guides/index.md');
	});

	it('loads HTML modules lazily', async () => {
		for (const page of registry.pages) {
			const html = await registry.loadHtml(page.path);
			expect(html === '').toBe(page.status === 'draft');
		}
	});
});
```

Run: `pnpm test src/lib/docs/pages.test.ts`. Expected: FAIL, because `./pages` doesn't exist.

- [ ] **Step 10: Implement `pages.ts`, the section matcher and the routes**

Create `website/src/lib/docs/pages.ts`:

```ts
import { createRegistry } from './registry';
import type { PageMeta } from './types';

const CONTENT = '/src/lib/content/';

const metas = import.meta.glob<PageMeta>('/src/lib/content/**/*.md', {
	query: '?meta',
	import: 'default',
	eager: true
});
const html = import.meta.glob<string>('/src/lib/content/**/*.md', {
	query: '?html',
	import: 'default'
});

/** Every page in src/lib/content. Metadata is bundled; HTML loads when a page is served. */
export const registry = createRegistry(Object.values(metas), (path) => {
	const load = html[`${CONTENT}${path}`];
	if (!load) throw new Error(`no HTML module for ${path}`);
	return load();
});
```

Run: `pnpm test src/lib/docs/pages.test.ts`. Expected: PASS.

Create `website/src/params/section.ts`:

```ts
import type { ParamMatcher } from '@sveltejs/kit';
import { isSectionId } from '$lib/docs/paths';

export const match: ParamMatcher = (param) => isSectionId(param);
```

Create `website/src/routes/[section=section]/[...slug]/+page.server.ts`:

```ts
import { error } from '@sveltejs/kit';
import { nav } from '$lib/config/nav';
import { docPageData } from '$lib/docs/page-data';
import { registry } from '$lib/docs/pages';
import { redirects } from '$lib/docs/redirects';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ params, parent }) => {
	const route = params.slug ? `${params.section}/${params.slug}` : params.section;
	const page = registry.getPage(route);
	if (!page) error(404, 'Page not found');

	const { github } = await parent();
	return docPageData(page, { registry, nav, redirects, latestRelease: github.version });
};
```

Create `website/src/routes/[section=section]/[...slug]/+page.svelte`. B1 replaces it; it only proves the data flows:

```svelte
<script lang="ts">
	let { data } = $props();
</script>

<article class="doc-article">
	<h1>{data.page.title}</h1>
	<p>{data.description}</p>
	{#if data.page.status === 'draft'}
		<p>This page hasn't been rewritten yet.</p>
	{:else}
		<!-- eslint-disable-next-line svelte/no-at-html-tags -->
		{@html data.html}
	{/if}
</article>
```

Create `website/src/routes/search.json/+server.ts`:

```ts
import { json } from '@sveltejs/kit';
import { nav } from '$lib/config/nav';
import { registry } from '$lib/docs/pages';
import { buildSearchIndex } from '$lib/docs/search';
import type { RequestHandler } from './$types';

export const prerender = true;

const CONTENT = '/src/lib/content/';

// Search text is read only here, at build time, so it stays out of the worker's page modules.
const text = import.meta.glob<string>('/src/lib/content/**/*.md', {
	query: '?search',
	import: 'default'
});

export const GET: RequestHandler = async () => {
	const entries = await buildSearchIndex(registry.pages, nav, (path) => {
		const load = text[`${CONTENT}${path}`];
		if (!load) throw new Error(`no search text for ${path}`);
		return load();
	});
	return json(entries);
};
```

- [ ] **Step 11: Adapt the server code, home data and layout**

`src/lib/server/github.ts`: import `site` and use it for the repo; rename the user agent and cache key. Everything else, including the caching and the `version` field from `releases/latest`, stays:

```ts
import { site } from '$lib/config/site';
```

```ts
const REPO = site.repo;
```

```ts
const EDGE_CACHE_KEY = 'https://ome-site.invalid/github/repo-stats';
```

```ts
		'User-Agent': 'ome-site',
```

`src/lib/server/content.ts`, `db/index.ts`, `db/schema.ts` and `src/lib/stores/hero-shader.ts` stay as copied.

Create `website/src/lib/config/home.ts`:

```ts
import type { ContentMap } from '$lib/server/content';

/** Hero copy. D1 can override it without a deploy; without a D1 binding, as in local development, these are used. */
export const homeDefaults: ContentMap = {
	'home.hero.title': 'The Kubernetes operator for serving LLMs in production',
	'home.hero.subtitle':
		'Declare a model. OME picks the runtime, places it on the right GPUs, and rolls out every change safely.'
};
```

Create `website/drizzle/0001_seed_content.sql`:

```sql
INSERT OR IGNORE INTO `content_blocks` (`key`, `value`, `updated_at`) VALUES
  ('home.hero.title', 'The Kubernetes operator for serving LLMs in production', datetime('now')),
  ('home.hero.subtitle', 'Declare a model. OME picks the runtime, places it on the right GPUs, and rolls out every change safely.', datetime('now'));
```

`src/routes/+page.server.ts`: change the first import to `import { homeDefaults } from '$lib/config/home';`. `+layout.server.ts` stays as copied.

Create `website/src/routes/+page.svelte`. B2 replaces it:

```svelte
<script lang="ts">
	import { base } from '$app/paths';

	let { data } = $props();
</script>

<section class="doc-article">
	<h1>{data.content['home.hero.title']}</h1>
	<p>{data.content['home.hero.subtitle']}</p>
	<p><a href="{base}/getting-started">Get started</a></p>
</section>
```

Create `website/src/lib/config/header.ts`:

```ts
import { base } from '$app/paths';

/** Header links. Contributing is linked from the footer and the Getting Started landing page. */
export const headerNavItems = [
	{ href: `${base}/getting-started`, label: 'Getting Started' },
	{ href: `${base}/guides`, label: 'Guides' },
	{ href: `${base}/concepts`, label: 'Concepts' },
	{ href: `${base}/reference`, label: 'Reference' }
] as const;
```

`src/routes/+layout.svelte`, replacing SMG's title logic and head. The `onNavigate` and hero shader effects are unchanged except for the comment noted below:

```svelte
<script lang="ts">
	import '../app.css';
	import '../styles/docs.css';
	import '../styles/home.css';
	import '../styles/search.css';
	import { onNavigate } from '$app/navigation';
	import { page } from '$app/stores';
	import SiteFooter from '$lib/components/SiteFooter.svelte';
	import SiteHeader from '$lib/components/SiteHeader.svelte';
	import { site } from '$lib/config/site';
	import { heroShaderActive, heroShaderEnergy } from '$lib/stores/hero-shader';
	import type { LayoutData } from './$types';

	onNavigate((navigation) => {
		if (typeof document === 'undefined') return;
		if (!document.startViewTransition) return;
		if (navigation.willUnload) return;
		if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

		// The home hero is full-viewport with a negative margin; morphing main's bounds looks like the hero sliding up.
		if (navigation.from?.route.id === '/' || navigation.to?.route.id === '/') return;

		return new Promise<void>((resolve) => {
			document.startViewTransition(async () => {
				resolve();
				await navigation.complete;
			});
		});
	});

	$effect(() => {
		if ($page.route.id !== '/') {
			heroShaderActive.set(false);
			heroShaderEnergy.set(0);
		}
	});

	// Pages set `title` and `description` in their load data.
	const pageTitle = $derived.by(() => {
		if ($page.error) {
			return `${$page.status === 404 ? 'Page not found' : 'Something went wrong'} · ${site.name}`;
		}
		const title: string | undefined = $page.data.title;
		return title ? `${title} · ${site.name}` : `${site.name} · ${site.longName}`;
	});
	const description = $derived<string>($page.data.description ?? site.description);

	let { data, children }: { data: LayoutData; children: import('svelte').Snippet } = $props();

	const isHome = $derived($page.route.id === '/');
</script>

<svelte:head>
	<title>{pageTitle}</title>
	<meta name="description" content={description} />
</svelte:head>

<div
	class="site-shell"
	class:site-shell--home={isHome}
	style:--hero-shader-energy={isHome ? $heroShaderEnergy : 0}
>
	<SiteHeader github={data.github} />
	<main class="site-main" class:site-main--docs={!isHome}>
		{@render children()}
	</main>
	<SiteFooter />
</div>
```

- [ ] **Step 12: Add the orbit mark and adapt the header and footer**

Create `website/src/lib/components/OrbitMark.svelte`:

```svelte
<script lang="ts">
	let {
		class: className = '',
		size = 24,
		title = null
	}: {
		class?: string;
		size?: number;
		/** Accessible name; without one the mark is decorative. */
		title?: string | null;
	} = $props();
</script>

<svg
	class={className}
	width={size}
	height={size}
	viewBox="0 0 40 40"
	xmlns="http://www.w3.org/2000/svg"
	role={title ? 'img' : undefined}
	aria-label={title ?? undefined}
	aria-hidden={title ? undefined : 'true'}
>
	<circle cx="20" cy="20" r="8" fill="currentColor" />
	<circle
		cx="20"
		cy="20"
		r="14"
		fill="none"
		stroke="currentColor"
		stroke-width="2"
		opacity="0.8"
	/>
	<circle cx="31" cy="9.5" r="3" fill="currentColor" />
</svg>
```

Create `website/static/favicon.svg`:

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 40"><circle cx="20" cy="20" r="8" fill="#2447b8"/><circle cx="20" cy="20" r="14" fill="none" stroke="#2447b8" stroke-width="2" opacity=".8"/><circle cx="31" cy="9.5" r="3" fill="#2447b8"/></svg>
```

`src/lib/components/SiteHeader.svelte`:

1. Replace `import { headerNavItems } from '$lib/config/nav';` with `import { headerNavItems } from '$lib/config/header';`.
2. Replace the `Logo` and `Symbol` imports with `import OrbitMark from '$lib/components/OrbitMark.svelte';` and `import { site } from '$lib/config/site';`.
3. Replace `<Logo />` with:

   ```svelte
   <OrbitMark size={24} />
   <span class="site-brand-name">{site.name}</span>
   ```

4. Replace both `<Symbol size={18} />` with `<OrbitMark size={18} />`.

Keep the Search link for now; B1 replaces it with the ⌘K trigger.

`src/lib/components/SiteFooter.svelte`, replacing the SMG links and marks. The mockup's footer has text links only:

```svelte
<script lang="ts">
	import { base } from '$app/paths';
	import OrbitMark from '$lib/components/OrbitMark.svelte';
	import { repoUrl, site } from '$lib/config/site';

	const year = new Date().getFullYear();

	const links = [
		{ label: 'GitHub', href: repoUrl, external: true },
		{ label: 'Issues', href: `${repoUrl}/issues`, external: true },
		{ label: 'Contributing', href: `${base}/contributing`, external: false }
	] as const;
</script>

<footer class="site-footer">
	<div class="site-footer-inner">
		<div class="site-footer-brand-block">
			<a href={base || '/'} class="site-footer-brand" aria-label="{site.name} home">
				<OrbitMark class="site-footer-mark" size={28} />
			</a>
			<p class="site-footer-name">{site.longName}</p>
		</div>

		<div class="site-footer-meta">
			<p class="site-footer-copy">© {year} OME Contributors</p>

			<ul class="site-footer-links">
				{#each links as link (link.label)}
					<li>
						<a
							class="site-footer-link"
							href={link.href}
							target={link.external ? '_blank' : undefined}
							rel={link.external ? 'noopener noreferrer' : undefined}
						>
							{link.label}
						</a>
					</li>
				{/each}
			</ul>
		</div>
	</div>
</footer>
```

`GitHubBadge.svelte` stays as copied; it already shows the release, stars and forks, and hides any value GitHub didn't return.

Run: `pnpm check && pnpm test`. Expected: both pass. If svelte-check fails on the build-time modules' types, fix `tsconfig.json` as described in Step 3; don't loosen `strict`.

- [ ] **Step 13: Split `app.css` and apply the palette**

B1 and B2 edit styles in parallel, so the 2,810-line stylesheet splits by the section comments SMG already uses. Line numbers are for SMG at `0cd30b3`:

| New file | Takes |
|---|---|
| `src/app.css` | lines 1 to 648 (tokens, inner-page glow, footer, header, mobile header, main), the `:root` rule inside `@media (max-width: 640px)` in "Mobile polish", and lines 2767 to 2810 (page transitions) |
| `src/styles/docs.css` | lines 649 to 818 (docs layout), 2050 to 2625 (doc article), and the `.docs-*` and `.doc-article` rules from "Mobile polish" |
| `src/styles/home.css` | lines 819 to 2049 (hero, metrics, works with, detail panel, why, how it works, choose your path) and the `.home-*` rules from "Mobile polish" |
| `src/styles/search.css` | only the comment `/* Search dialog and search page. */` |

Wrap each moved "Mobile polish" rule in the same `@media` query it came from. Don't change, drop or duplicate any rule. Check: `cat src/app.css src/styles/*.css | grep -c '{$'` equals the same count on SMG's `src/app.css`, plus the number of `@media` lines you added.

Then apply the palette from [Palette and mark](#palette-and-mark-a1-defines-all-tasks-use). On macOS:

```bash
sed -i '' \
  -e 's/--smg-orange-deep/--ome-accent-deep/g' \
  -e 's/--smg-orange/--ome-accent/g' \
  -e 's/--smg-surface/--ome-surface/g' \
  -e 's/--smg-detail-surface/--ome-detail-surface/g' \
  -e 's/--smg-detail-chip/--ome-detail-chip/g' \
  -e 's/--smg-detail-stack/--ome-detail-stack/g' \
  -e 's/--smg-gradient-mid/--ome-gradient-mid/g' \
  -e 's/--smg-spark/--ome-orbit/g' \
  -e 's/rgba(179, 90, 17,/rgba(36, 71, 184,/g' \
  -e 's/#b35a11/#2447b8/g' -e 's/#9a4d0e/#1b3690/g' \
  -e 's/#f2f0eb/#eef0f5/g' -e 's/#ebe8e3/#e3e7ef/g' -e 's/#e8dcc8/#dde3f0/g' \
  -e 's/#d9d9d9/#d6dae3/g' -e 's/#d9d7d2/#d6dae3/g' \
  src/app.css src/styles/*.css
```

In `:root`, replace the `--ome-orbit` value (still SMG's spark path) with the orbit mask from the palette section, and add:

```css
	--ome-note: #2447b8;
	--ome-tip: #1f7a4d;
	--ome-warning: #a15c00;
	--ome-danger: #b42318;
```

Style `.site-brand` as a flex row (`display: inline-flex; align-items: center; gap: 8px`) and add `.site-brand-name { font-weight: 700; font-size: 16px; letter-spacing: 0.02em; }`. The brand is `var(--ome-accent)` on the solid header and white over the home hero, following the header state classes SMG uses for its logo. Translate any non-English comments to English, and change comments that say "orange" to "cobalt".

Check: this prints nothing:

```bash
grep -rniE 'smg|shepherd|orange|b35a11|179, ?90' src/app.css src/styles src/lib/components src/routes src/lib/server src/lib/stores
```

- [ ] **Step 14: Verify the build and the bundle**

```bash
rm -f worker-configuration.d.ts && pnpm lint && pnpm test
pnpm check && pnpm build
```

Expected: all pass. Lint and test must pass without `worker-configuration.d.ts`, because CI runs them before `pnpm check` generates it.

Check that Markdown rendering stayed out of the worker and that the search index was prerendered:

```bash
grep -rlE 'registerLanguage|walkTokens' .svelte-kit/output/server .svelte-kit/cloudflare/_worker.js || echo "no renderer in the worker"
node -e 'const e = require("./.svelte-kit/cloudflare/ome/search.json"); console.log(e.length)'
```

Expected: `no renderer in the worker`, then `89`. If `search.json` is at a different path, find it with `find .svelte-kit/cloudflare -name search.json` and report the path.

Serve the production build and check the routes:

```bash
pnpm preview > /tmp/ome-preview.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:4173/ome && break; sleep 1; done
for path in '' /getting-started /guides/deploy-models/serve-models-from-pvc /reference/api/ome.v1beta1 /search.json /guides/nope /nope; do
  printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:4173/ome$path")" "/ome$path"
done
curl -s http://localhost:4173/ome/getting-started | grep -o '<title>[^<]*</title>'
pkill -f 'wrangler pages dev'
```

Expected: `200` for the first five paths, `404` for the last two, then `<title>Getting Started · OME</title>`. If `wrangler pages dev` can't start in your environment, run the same checks against `pnpm dev` on port 5173 (stop it with `pkill -f 'vite dev'`) and say so in your report.

- [ ] **Step 15: Commit**

```bash
cd .. && ~/.local/bin/pre-commit run --files $(git ls-files --others --modified --exclude-standard website)
git add website
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Serve doc pages and recolor the website" -m "Add the section route, page data, search index, OME header and
footer, the orbit mark, and the cobalt palette. Split app.css so the
docs and home styles can change independently."
```

### Task A2: Repo plumbing

**Branch:** `docs/website-plumbing`

**Goal:** CI, Dependabot, CODEOWNERS, the labeler, codespell, the Claude review workflow, AGENTS.md and NOTICE all know about `website/`. `website/` doesn't exist in this worktree yet; that's expected. Nothing here needs it to exist.

**Files:**

- Create: `.github/workflows/website.yml`
- Modify: `.github/dependabot.yml`, `.github/CODEOWNERS`, `.github/labeler.yml`, `.github/workflows/claude-code-review.yml`, `.pre-commit-config.yaml`, `AGENTS.md`, `NOTICE`

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/website.yml`. It follows smg-docs' CI (pnpm 10, Node 22, `--frozen-lockfile`, a final status job) and this repo's action versions:

```yaml
name: Website

on:
  pull_request:
    paths:
      - 'website/**'
      - 'hack/docs-examples/**'
      - 'hack/docs-drift/**'
      - 'config/crd/full/**'
      - '.github/workflows/website.yml'
  push:
    branches: [main]
    paths:
      - 'website/**'
      - 'hack/docs-examples/**'
      - 'hack/docs-drift/**'
      - 'config/crd/full/**'
      - '.github/workflows/website.yml'

permissions:
  contents: read

concurrency:
  group: website-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

jobs:
  site:
    name: Lint, check, test and build
    runs-on: ubuntu-latest
    timeout-minutes: 15
    defaults:
      run:
        working-directory: website
    steps:
      - uses: actions/checkout@v6
        with:
          persist-credentials: false
      - uses: pnpm/action-setup@v4
        with:
          version: 10
      - uses: actions/setup-node@v6
        with:
          node-version: 22
          cache: pnpm
          cache-dependency-path: website/pnpm-lock.yaml
      - run: pnpm install --frozen-lockfile
      # Lint and test run before check, which generates worker-configuration.d.ts.
      - run: pnpm lint
      - run: pnpm test
      - run: pnpm check
      - run: pnpm build

  examples:
    name: YAML examples and hack tests
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v6
        with:
          persist-credentials: false
      - uses: actions/setup-go@v6
        with:
          go-version: '1.26'
          cache: true
      - name: Test the hack programs
        run: |
          make envtest
          export KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)"
          go test ./hack/docs-examples/... ./hack/docs-drift/...
      - name: Check YAML examples against the CRDs
        run: make docs-examples

  drift:
    name: Drift report
    runs-on: ubuntu-latest
    timeout-minutes: 10
    # The nightly docs bot changes site/ every day, so drift is expected
    # until launch. The report is information, not a gate.
    continue-on-error: true
    steps:
      - uses: actions/checkout@v6
        with:
          # The report compares site/ history since each rewrite.
          fetch-depth: 0
          persist-credentials: false
      - uses: actions/setup-go@v6
        with:
          go-version: '1.26'
          cache: true
      - name: Report drift between site/ and website/
        # A built binary keeps docs-drift's exit status; go run and make
        # turn every failure into 1 and 2.
        run: |
          go build -o "$RUNNER_TEMP/docs-drift" ./hack/docs-drift
          set +e
          "$RUNNER_TEMP/docs-drift" > drift.txt 2>&1
          status=$?
          set -e
          cat drift.txt
          {
            echo '## Docs drift'
            echo
            case "$status" in
              0) echo 'No drift: every Hugo page is mapped and every rewrite is current.' ;;
              1) echo 'Content PRs should fold these changes in and move `rewrittenFrom` forward.'
                 echo
                 echo '```text'
                 cat drift.txt
                 echo '```' ;;
              *) echo 'The drift report failed to run; see the job log.' ;;
            esac
          } >> "$GITHUB_STEP_SUMMARY"
          if [ "$status" -eq 1 ]; then
            echo "::warning title=Docs drift::site/ changed since some pages were rewritten; see the job summary."
          fi
          # Drift (1) is reported; only a failure to run (2) marks the job failed.
          [ "$status" -le 1 ]

  status:
    name: Website status
    needs: [site, examples]
    if: always()
    runs-on: ubuntu-latest
    permissions: {}
    steps:
      - name: Check results
        run: |
          if [[ "${{ needs.site.result }}" != "success" || \
                "${{ needs.examples.result }}" != "success" ]]; then
            echo "A website check failed: site=${{ needs.site.result }}, examples=${{ needs.examples.result }}"
            exit 1
          fi
          echo "All website checks passed"
```

`make docs-examples` (Task A3) and `hack/docs-drift` (Task A4) don't exist in this worktree; C1 runs the workflow's commands end to end after the merge. docs-drift runs with no flags: its defaults are the repo layout.

- [ ] **Step 2: Validate the workflow**

```bash
python3 -c 'import yaml,sys; d=yaml.safe_load(open(".github/workflows/website.yml")); print(sorted(d["jobs"]))'
~/.local/bin/pre-commit run --files .github/workflows/website.yml
```

Expected: `['drift', 'examples', 'site', 'status']`, then every hook passes or is skipped. PyYAML reads the `on:` key as `True`; that's normal. If `actionlint` is installed (`which actionlint`), run `actionlint .github/workflows/website.yml` too; it isn't required.

- [ ] **Step 3: Commit the workflow**

```bash
git add .github/workflows/website.yml
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[CI/Tests] Add the website workflow" -m "Lint, check, test and build website/, check its YAML examples
against the CRDs, and report drift from site/ in the job summary."
```

- [ ] **Step 4: Update Dependabot, CODEOWNERS and the labeler**

In `.github/dependabot.yml`, add this entry after the `/site` npm entry:

```yaml
  # NPM dependencies for the redesigned documentation site
  - package-ecosystem: "npm"
    directory: "/website"
    schedule:
      interval: "weekly"
      day: "saturday"
    labels:
      - "dependencies"
      - "documentation"
      - "javascript"
```

In `.github/CODEOWNERS`, after the `/site/` line:

```text
/site/ @slin1237 @pallasathena92 @beiguo218

# website (redesigned documentation site)
/website/ @slin1237 @pallasathena92 @beiguo218
```

In `.github/labeler.yml`, add `website/**/*` to the `documentation` label:

```yaml
documentation:
  - changed-files:
    - any-glob-to-any-file:
      - '**/*.md'
      - 'site/**/*'
      - 'website/**/*'
      - 'README*'
```

- [ ] **Step 5: Update codespell and the Claude review workflow**

In `.pre-commit-config.yaml`, add the lockfile to the codespell `exclude` block, after the `package-lock` line:

```yaml
        exclude: |
          (?x)^(
            charts/.*/templates/.*\.yaml$|
            config/crd/.*|
            site/assets/.*\.svg$|
            .*go\.sum$|
            .*package-lock\.json$|
            website/pnpm-lock\.yaml$
          )$
```

In `.github/workflows/claude-code-review.yml`, add the content directory to `paths-ignore`, as `site/**` is:

```yaml
    paths-ignore:
      - '*.md'
      - 'site/**'
      - 'website/src/lib/content/**'
      - '*.lock'
```

- [ ] **Step 6: Update AGENTS.md and NOTICE**

In `AGENTS.md`, under "Key packages", add this line right after the `site/` line:

```markdown
- `website/` — the redesigned documentation site (SvelteKit on Cloudflare, Node 22 and pnpm 10) that replaces `site/` at launch. Until then, documentation changes still go to `site/`.
```

In `NOTICE`, add this after the contributors list, separated by a blank line:

```text
This product includes software derived from smg-docs
(https://github.com/smg-project/smg-docs), licensed under the Apache
License, Version 2.0: the layout, styles and components of the
documentation site in website/.
```

Don't publish anything that relies on this credit until smg-docs has a LICENSE file (see the spec's Prerequisites). This task only writes the credit.

- [ ] **Step 7: Check and commit**

```bash
python3 -c 'import yaml; [yaml.safe_load(open(f)) for f in [".github/dependabot.yml", ".github/labeler.yml", ".github/workflows/claude-code-review.yml", ".pre-commit-config.yaml"]]; print("yaml ok")'
~/.local/bin/pre-commit run --files .github/dependabot.yml .github/CODEOWNERS .github/labeler.yml .github/workflows/claude-code-review.yml .pre-commit-config.yaml AGENTS.md NOTICE
git add .github/dependabot.yml .github/CODEOWNERS .github/labeler.yml .github/workflows/claude-code-review.yml .pre-commit-config.yaml AGENTS.md NOTICE
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Wire website/ into repo tooling" -m "Add website/ to Dependabot, CODEOWNERS, the documentation label,
the codespell excludes and the Claude review ignores. Tell agents which
site takes documentation changes, and credit smg-docs in NOTICE."
```

Expected: `yaml ok`, and every hook passes or is skipped.

### Task A3: The YAML example check

**Branch:** `docs/website-examples`

**Goal:** `make docs-examples` checks every YAML example in `website/src/lib/content` against the CRDs in `config/crd/full`, in an envtest API server, and reports each failure with its file and line. When this check ran over the Hugo docs, 26 of their 82 objects failed. The rewrite must pass it.

**Files:**

- Create: `hack/docs-examples/extract.go`, `extract_test.go`, `object.go`, `object_test.go`, `check.go`, `check_test.go`, `main.go`
- Modify: `Makefile` (a `docs-examples` target after `generate-apiref`)

**How it works:**

- `extract.go` finds fenced `yaml` and `yml` blocks using the renderer's fence rules. It skips blocks marked `check=skip` in the info string, splits the rest at `---` into documents, and records the line each document starts on.
- `object.go` parses a document strictly, so duplicate keys are errors. A document without `apiVersion` or `kind` is a fragment and isn't checked. An object without `metadata.name` or `metadata.generateName` fails, so a config file with an `apiVersion` and `kind`, such as a kubeconfig or a Kustomization, needs `check=skip`.
- `check.go` looks up each object's kind through the API server's discovery. Kinds in groups the server doesn't serve (KEDA, Gateway API) are skipped and listed. Unknown kinds or versions in served groups fail. It creates the namespaces the examples use, then dry-run creates each object with strict field validation.
- `main.go` defaults its flags to the repo layout and exits 0 when every example passes, 1 when one fails, and 2 when it can't run or finds no objects.

This code was validated before the plan was written: every test below passes against this repo's CRDs.

**envtest:** `check_test.go` and the command need `KUBEBUILDER_ASSETS`. `make envtest` installs `bin/setup-envtest` with `go install`. If that fails because a sandbox can't write the Go module cache ("operation not permitted"), use the assets already on the development machine:

```bash
export KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64"
```

Otherwise:

```bash
make envtest
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)"
```

Without `KUBEBUILDER_ASSETS`, `TestCheck` skips; every other test still runs.

- [ ] **Step 1: Write the extraction test**

Create `hack/docs-examples/extract_test.go`:

`````go
package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExtractDocuments(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     []document
	}{
		{
			name:     "yaml block",
			markdown: "Intro\n\n```yaml\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 4, text: "a: 1"}},
		},
		{
			name:     "yml and attributes",
			markdown: "```YML title=\"model.yaml\"\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "tilde fence",
			markdown: "~~~yaml\na: 1\n~~~\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "check=skip",
			markdown: "```yaml check=skip\na: 1\n```\n",
		},
		{
			name:     "check=skip inside a title",
			markdown: "```yaml title=\"check=skip\"\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "other languages",
			markdown: "```bash\nkubectl apply -f model.yaml\n```\n",
		},
		{
			name:     "documents",
			markdown: "```yaml\na: 1\n---\nb: 2\n--- # comment\n\nc: 3\n\n```\n",
			want: []document{
				{path: "page.md", line: 2, text: "a: 1"},
				{path: "page.md", line: 4, text: "b: 2"},
				{path: "page.md", line: 7, text: "c: 3"},
			},
		},
		{
			name:     "indented in a tab",
			markdown: "=== \"Tab\"\n\n    ```yaml\n    a:\n      b: 1\n    ```\n",
			want:     []document{{path: "page.md", line: 4, text: "a:\n  b: 1"}},
		},
		{
			name:     "fence inside a longer fence",
			markdown: "````markdown\n```yaml\na: 1\n```\n````\n",
		},
		{
			name:     "unclosed fence",
			markdown: "```yaml\na: 1\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractDocuments("page.md", tc.markdown)
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(document{})); diff != "" {
				t.Errorf("extractDocuments() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
`````

- [ ] **Step 2: Run it to see it fail**

Run: `go test ./hack/docs-examples/`

Expected: a build failure starting with `undefined: document`.

- [ ] **Step 3: Write the extractor**

Create `hack/docs-examples/extract.go`:

```go
package main

import (
	"regexp"
	"strings"
)

// A document is one YAML document from a fenced yaml block.
type document struct {
	path string
	line int // 1-based line of the document's first non-blank line
	text string
}

var (
	// fenceOpen matches a line that opens a fenced code block, with the
	// renderer's rules: any indentation, three or more backticks or tildes,
	// then the info string.
	fenceOpen = regexp.MustCompile("^(\\s*)(`{3,}|~{3,})(.*)$")
	// attribute matches one key=value pair in an info string.
	attribute = regexp.MustCompile(`([\w-]+)=(?:"([^"]*)"|(\S+))`)
)

// extractDocuments returns the YAML documents in the fenced yaml blocks of
// a Markdown file, skipping blocks marked check=skip. Each block is
// dedented by its fence's indentation, so blocks inside tabs and callouts
// keep their YAML structure. As in CommonMark, a fence that is never
// closed runs to the end of the file.
func extractDocuments(path, markdown string) []document {
	lines := strings.Split(markdown, "\n")
	var docs []document
	for i := 0; i < len(lines); i++ {
		open := fenceOpen.FindStringSubmatch(lines[i])
		// A backtick fence's info string can't contain a backtick.
		if open == nil || (open[2][0] == '`' && strings.Contains(open[3], "`")) {
			continue
		}
		indent, marker, info := open[1], open[2], open[3]
		end := i + 1
		for end < len(lines) && !closesFence(lines[end], marker) {
			end++
		}
		if isCheckedYAML(info) {
			body := make([]string, 0, end-i-1)
			for _, line := range lines[i+1 : end] {
				body = append(body, dedent(line, len(indent)))
			}
			docs = append(docs, splitDocuments(path, i+2, body)...)
		}
		i = end
	}
	return docs
}

// closesFence reports whether line closes a fence opened with marker: a
// run of the same character, at least as long, with optional whitespace.
func closesFence(line, marker string) bool {
	trimmed := strings.TrimSpace(line)
	return len(trimmed) >= len(marker) && strings.Trim(trimmed, marker[:1]) == ""
}

// isCheckedYAML reports whether a fence's info string opens a yaml (or
// yml) block that isn't marked check=skip.
func isCheckedYAML(info string) bool {
	info = strings.TrimSpace(info)
	fields := strings.Fields(info)
	if len(fields) == 0 {
		return false
	}
	if name := strings.ToLower(fields[0]); name != "yaml" && name != "yml" {
		return false
	}
	for _, m := range attribute.FindAllStringSubmatch(info[len(fields[0]):], -1) {
		if m[1] == "check" && m[3] == "skip" {
			return false
		}
	}
	return true
}

// dedent removes up to n leading spaces or tabs from line.
func dedent(line string, n int) string {
	i := 0
	for i < n && i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	return line[i:]
}

// splitDocuments splits a yaml block's lines into documents at separator
// lines, dropping blank lines around each document. first is the file
// line of body[0].
func splitDocuments(path string, first int, body []string) []document {
	var docs []document
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && !isSeparator(body[i]) {
			continue
		}
		end := i
		for start < end && strings.TrimSpace(body[start]) == "" {
			start++
		}
		for end > start && strings.TrimSpace(body[end-1]) == "" {
			end--
		}
		if start < end {
			docs = append(docs, document{
				path: path,
				line: first + start,
				text: strings.Join(body[start:end], "\n"),
			})
		}
		start = i + 1
	}
	return docs
}

// isSeparator reports whether line separates YAML documents. As in
// kubectl, only whitespace and a comment may follow the "---".
func isSeparator(line string) bool {
	rest, ok := strings.CutPrefix(line, "---")
	if !ok {
		return false
	}
	rest = strings.TrimSpace(rest)
	return rest == "" || strings.HasPrefix(rest, "#")
}
```

- [ ] **Step 4: Run the test to see it pass**

Run: `go test ./hack/docs-examples/ -run TestExtractDocuments -v`

Expected: `--- PASS: TestExtractDocuments` with 10 passing subtests, then `ok`.

- [ ] **Step 5: Commit**

```bash
git add hack/docs-examples/extract.go hack/docs-examples/extract_test.go
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Extract YAML examples from Markdown" -m "hack/docs-examples reads fenced yaml blocks with the website
renderer's fence rules, skips blocks marked check=skip, and splits the
rest into documents with the line each one starts on."
```

- [ ] **Step 6: Write the object test**

Create `hack/docs-examples/object_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func TestParseObject(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		wantName string // empty when the document isn't an object
	}{
		{
			name:     "object",
			text:     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings",
			wantName: "settings",
		},
		{name: "fragment", text: "spec:\n  model:\n    name: llama"},
		{name: "no name", text: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  generateName: settings-"},
		{name: "list", text: "- a\n- b"},
		{name: "comment", text: "# Nothing here yet."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := parseObject(tc.text)
			if err != nil {
				t.Fatalf("parseObject() error: %v", err)
			}
			got := ""
			if obj != nil {
				got = obj.GetName()
			}
			if got != tc.wantName {
				t.Errorf("parseObject() name = %q, want %q", got, tc.wantName)
			}
		})
	}
}

func TestParseObjectErrors(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string // in the message, with the document on line 10
	}{
		{name: "syntax", text: "a: 1\n  b: 2", want: "line 11:"},
		{name: "duplicate key", text: "a: 1\na: 2", want: `line 11: key "a" already set in map`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := document{path: "page.md", line: 10, text: tc.text}
			_, err := parseObject(doc.text)
			if err == nil {
				t.Fatal("parseObject() succeeded, want an error")
			}
			if got := yamlMessage(doc, err); !strings.Contains(got, tc.want) {
				t.Errorf("yamlMessage() = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 7: Run it to see it fail**

Run: `go test ./hack/docs-examples/`

Expected: a build failure with `undefined: parseObject` and `undefined: yamlMessage`.

- [ ] **Step 8: Write the parser**

Create `hack/docs-examples/object.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// parseObject returns the Kubernetes object in a YAML document. It returns
// nil, nil when the document isn't one: it isn't a mapping, or it has no
// apiVersion, kind or metadata.name. Fragments of objects, Helm values and
// config files are not objects. Invalid YAML, including duplicate keys, is
// an error.
func parseObject(text string) (*unstructured.Unstructured, error) {
	data, err := yaml.YAMLToJSONStrict([]byte(text))
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, nil // Not a mapping.
	}
	obj := &unstructured.Unstructured{Object: fields}
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
		return nil, nil
	}
	return obj, nil
}

// docLine matches a line number in a YAML parser error. The parser counts
// lines from the start of the document.
var docLine = regexp.MustCompile(`\bline (\d+):`)

// yamlMessage returns err, an error from parsing doc, with its line
// numbers counted from the start of the file.
func yamlMessage(doc document, err error) string {
	return docLine.ReplaceAllStringFunc(err.Error(), func(match string) string {
		n, _ := strconv.Atoi(docLine.FindStringSubmatch(match)[1])
		return fmt.Sprintf("line %d:", doc.line+n-1)
	})
}
```

- [ ] **Step 9: Run the tests to see them pass**

Run: `go test ./hack/docs-examples/ -v -run 'TestParseObject'`

Expected: `--- PASS: TestParseObject` (5 subtests) and `--- PASS: TestParseObjectErrors` (2 subtests), then `ok`.

- [ ] **Step 10: Commit**

```bash
git add hack/docs-examples/object.go hack/docs-examples/object_test.go
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Parse YAML examples as objects" -m "Parse each document strictly, so duplicate keys fail, and report YAML
errors with line numbers counted from the start of the file. Documents
without apiVersion, kind and metadata.name are fragments."
```

- [ ] **Step 11: Write the check test**

Create `hack/docs-examples/check_test.go`. The page uses `~~~` fences so it can sit in a Go raw string; the line numbers in `want` are lines of that page.

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// examplesPage has one example of each outcome. Tilde fences keep it a raw
// string.
const examplesPage = `# Examples

~~~yaml title="valid.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b
spec:
  modelFormat:
    name: safetensors
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
---
apiVersion: v1
kind: Namespace
metadata:
  name: llama
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama
  namespace: llama
spec:
  model:
    name: llama-3-1-8b
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: in-default
data:
  key: value
~~~

~~~yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: unknown-field
spec:
  predictor:
    name: llama-3-1-8b
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: bad-enum
spec:
  deploymentMode: Serverless
---
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: missing-storage-uri
spec:
  storage:
    path: /models
---
apiVersion: v1
kind: Service
metadata:
  name: bad-port
spec:
  ports:
    - port: 700000
---
apiVersion: ome.io/v1beta1
kind: Model
metadata:
  name: unknown-kind
---
apiVersion: ome.io/v1alpha1
kind: InferenceService
metadata:
  name: unserved-version
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: duplicate-key
  name: again
~~~

~~~yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: not-installed
~~~

~~~yaml check=skip
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: marked-skip
spec:
  deploymentMode: Serverless
~~~

~~~yaml
spec:
  model:
    name: a-fragment
~~~
`

func TestCheck(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; the Website workflow shows how to set it")
	}
	log.SetLogger(logr.Discard())
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "full")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	c, err := newChecker(cfg)
	if err != nil {
		t.Fatal(err)
	}

	rep, err := c.check(context.Background(), extractDocuments("page.md", examplesPage))
	if err != nil {
		t.Fatalf("check() error: %v", err)
	}

	// The four valid objects and the four invalid ones the server knows.
	if rep.checked != 8 {
		t.Errorf("checked = %d, want 8", rep.checked)
	}
	wantSkipped := map[string]int{"keda.sh/v1alpha1 ScaledObject": 1}
	if diff := cmp.Diff(wantSkipped, rep.skipped); diff != "" {
		t.Errorf("skipped mismatch (-want +got):\n%s", diff)
	}
	want := []struct {
		line            int
		object, message string
	}{
		{37, "InferenceService unknown-field", `unknown field "spec.predictor"`},
		{45, "InferenceService bad-enum", `Unsupported value: "Serverless"`},
		{52, "ClusterBaseModel missing-storage-uri", "spec.storage.storageUri: Required value"},
		{60, "Service bad-port", "spec.ports[0].port: Invalid value: 700000"},
		{68, "Model unknown-kind", "the API server has no kind Model in ome.io/v1beta1"},
		{73, "InferenceService unserved-version", "the API server has no kind InferenceService in ome.io/v1alpha1"},
		{78, "", "invalid YAML: yaml: unmarshal errors:\n  line 82: key \"name\" already set in map"},
	}
	if len(rep.failures) != len(want) {
		t.Fatalf("got %d failures, want %d:\n%s", len(rep.failures), len(want), failureList(rep.failures))
	}
	for i, w := range want {
		got := rep.failures[i]
		if got.path != "page.md" || got.line != w.line || got.object != w.object || !strings.Contains(got.message, w.message) {
			t.Errorf("failure %d = %q, want line %d, object %q and a message containing %q", i, got, w.line, w.object, w.message)
		}
	}
}

func failureList(failures []failure) string {
	var b strings.Builder
	for _, f := range failures {
		b.WriteString(f.String() + "\n")
	}
	return b.String()
}
```

- [ ] **Step 12: Run it to see it fail**

Set `KUBEBUILDER_ASSETS` (see envtest above), then run: `go test ./hack/docs-examples/`

Expected: a build failure with `undefined: newChecker` and `undefined: failure`.

- [ ] **Step 13: Write the checker**

Create `hack/docs-examples/check.go`:

```go
package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A checker validates objects against an API server.
type checker struct {
	client client.Client
	// served holds the names of the API groups the server serves. The
	// core group's name is "".
	served map[string]bool
}

func newChecker(cfg *rest.Config) (*checker, error) {
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}
	groups, err := dc.ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("list API groups: %w", err)
	}
	served := make(map[string]bool)
	for _, g := range groups.Groups {
		served[g.Name] = true
	}
	return &checker{client: c, served: served}, nil
}

// A failure is an example that the YAML parser or API server rejected.
type failure struct {
	path    string
	line    int
	object  string // "Kind name", or empty for invalid YAML
	message string
}

func (f failure) String() string {
	if f.object == "" {
		return fmt.Sprintf("%s:%d: %s", f.path, f.line, f.message)
	}
	return fmt.Sprintf("%s:%d: %s: %s", f.path, f.line, f.object, f.message)
}

// A report is the outcome of a check.
type report struct {
	checked  int // objects sent to the API server
	failures []failure
	// skipped counts the objects whose API group the server doesn't
	// serve, by "group/version Kind".
	skipped map[string]int
}

// check validates the Kubernetes objects in docs. It creates the
// namespaces they use, then dry-run creates each object with strict field
// validation. It returns an error only when the API server fails, not
// when an example does.
func (c *checker) check(ctx context.Context, docs []document) (*report, error) {
	type example struct {
		doc document
		obj *unstructured.Unstructured
	}
	rep := &report{skipped: make(map[string]int)}
	var examples []example
	namespaces := make(map[string]bool)
	for _, doc := range docs {
		obj, err := parseObject(doc.text)
		if err != nil {
			rep.failures = append(rep.failures, failure{
				path: doc.path, line: doc.line, message: "invalid YAML: " + yamlMessage(doc, err),
			})
			continue
		}
		if obj == nil {
			continue
		}
		gvk := obj.GroupVersionKind()
		mapping, err := c.client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		switch {
		case meta.IsNoMatchError(err) && !c.served[gvk.Group]:
			rep.skipped[gvk.GroupVersion().String()+" "+gvk.Kind]++
			continue
		case meta.IsNoMatchError(err):
			rep.failures = append(rep.failures, failure{
				path: doc.path, line: doc.line, object: describe(obj),
				message: fmt.Sprintf("the API server has no kind %s in %s", gvk.Kind, gvk.GroupVersion()),
			})
			continue
		case err != nil:
			return nil, fmt.Errorf("map %s: %w", gvk, err)
		}
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if obj.GetNamespace() == "" {
				obj.SetNamespace(metav1.NamespaceDefault)
			}
			namespaces[obj.GetNamespace()] = true
		}
		examples = append(examples, example{doc: doc, obj: obj})
	}

	// Creating a namespace can fail for an example's reason, such as an
	// invalid name, so the error goes to the examples that use it.
	namespaceErrors := make(map[string]error)
	for _, name := range slices.Sorted(maps.Keys(namespaces)) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.client.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			namespaceErrors[name] = fmt.Errorf("create namespace %s: %w", name, err)
		}
	}

	for _, ex := range examples {
		rep.checked++
		err := namespaceErrors[ex.obj.GetNamespace()]
		if err == nil {
			err = c.client.Create(ctx, ex.obj, client.DryRunAll, client.FieldValidation("Strict"))
		}
		// A dry run stores nothing, so an object that already exists is a
		// namespace created above or by the server. It passed validation,
		// which runs first.
		if err != nil && !apierrors.IsAlreadyExists(err) {
			rep.failures = append(rep.failures, failure{
				path: ex.doc.path, line: ex.doc.line, object: describe(ex.obj), message: err.Error(),
			})
		}
	}

	slices.SortStableFunc(rep.failures, func(a, b failure) int {
		return cmp.Or(strings.Compare(a.path, b.path), cmp.Compare(a.line, b.line))
	})
	return rep, nil
}

func describe(obj *unstructured.Unstructured) string {
	return obj.GetKind() + " " + obj.GetName()
}

// print writes the failures, then a summary, to w.
func (r *report) print(w io.Writer) {
	for _, f := range r.failures {
		fmt.Fprintln(w, f)
	}
	fmt.Fprintf(w, "Checked %s: %s.\n", count(r.checked, "object"), count(len(r.failures), "problem"))
	if len(r.skipped) > 0 {
		fmt.Fprintln(w, "Skipped objects of kinds whose CRDs aren't installed:")
		for _, kind := range slices.Sorted(maps.Keys(r.skipped)) {
			fmt.Fprintf(w, "  %s: %d\n", kind, r.skipped[kind])
		}
	}
}

// count returns n and noun, pluralized.
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
```

- [ ] **Step 14: Write the command**

Create `hack/docs-examples/main.go`:

```go
// Command docs-examples checks the YAML examples in the documentation
// site against the API server's validation.
//
// It starts an envtest API server with the CRDs in -crds and reads every
// fenced yaml block in the Markdown files under -content that isn't marked
// check=skip. It creates the namespaces the examples use, then dry-run
// creates each object with strict field validation. That catches unknown
// fields, wrong types, enum values, missing required fields and CEL rules,
// but not rules that only the admission webhooks enforce, because envtest
// doesn't run them.
//
// Documents without apiVersion, kind and metadata.name aren't objects and
// aren't checked. Objects whose API group the server doesn't serve, such
// as KEDA's, are skipped and listed.
//
// It exits 0 when every example passes, 1 when one fails, and 2 when it
// can't run.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func main() {
	os.Exit(run())
}

func run() int {
	content := flag.String("content", "website/src/lib/content", "`directory` of Markdown pages")
	crds := flag.String("crds", "config/crd/full", "`directory` of CRDs to install")
	flag.Parse()
	// controller-runtime warns when nothing sets its logger.
	log.SetLogger(logr.Discard())

	docs, err := readDocuments(*content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "docs-examples: %v\n", err)
		return 2
	}

	env := &envtest.Environment{CRDDirectoryPaths: []string{*crds}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "docs-examples: start the envtest API server: %v\n", err)
		fmt.Fprintln(os.Stderr, "Run make docs-examples, which installs envtest and sets KUBEBUILDER_ASSETS.")
		return 2
	}
	defer func() {
		if err := env.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "docs-examples: stop the envtest API server: %v\n", err)
		}
	}()

	c, err := newChecker(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "docs-examples: %v\n", err)
		return 2
	}
	rep, err := c.check(context.Background(), docs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "docs-examples: %v\n", err)
		return 2
	}
	rep.print(os.Stdout)
	if len(rep.failures) > 0 {
		return 1
	}
	return 0
}

// readDocuments returns the YAML documents in the Markdown files under dir.
func readDocuments(dir string) ([]document, error) {
	var docs []document
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".md" {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		docs = append(docs, extractDocuments(path, string(data))...)
		return nil
	})
	return docs, err
}
```

- [ ] **Step 15: Run every test**

```bash
go vet ./hack/docs-examples/
go test ./hack/docs-examples/ -v 2>&1 | grep -E '^(--- |ok|FAIL)'
```

Expected, with `KUBEBUILDER_ASSETS` set: `--- PASS` for `TestCheck`, `TestExtractDocuments`, `TestParseObject` and `TestParseObjectErrors`, then `ok`. `TestCheck` takes several seconds because it starts an API server. If it says `--- SKIP`, `KUBEBUILDER_ASSETS` isn't set.

- [ ] **Step 16: Commit**

```bash
git add hack/docs-examples/check.go hack/docs-examples/check_test.go hack/docs-examples/main.go
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Check YAML examples in envtest" -m "Dry-run create each example with strict field validation against the
CRDs, after creating the namespaces the examples use. Kinds from groups
the server doesn't serve are skipped and listed."
```

- [ ] **Step 17: Add the make target**

In `Makefile`, add this target after `generate-apiref` and before `include Makefile-deps.mk`, in the Documentation section. Task A4 adds `docs-drift` before `generate-apiref`, so the two edits merge cleanly. `ENVTEST` is defined in `Makefile-deps.mk` and `ENVTEST_K8S_VERSION` near the top of `Makefile`; make expands them when the recipe runs.

```make
.PHONY: docs-examples
docs-examples: envtest ## 📚 Check the website's YAML examples against the CRDs
	@KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		$(GO_CMD) run ./hack/docs-examples
```

The recipe lines start with a tab.

- [ ] **Step 18: Try it on a page**

`website/` doesn't exist in this worktree, so point `-content` at a temporary page with one valid object and one invalid one:

````bash
tmp=$(mktemp -d)
mkdir -p "$tmp/guides"
cat > "$tmp/guides/example.md" <<'EOF'
---
title: Example
---

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b
spec:
  modelFormat:
    name: safetensors
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
```

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama
spec:
  model:
    name: llama-3-1-8b
  engine:
    minReplica: 1
```
EOF
go run ./hack/docs-examples -content "$tmp"; echo "exit=$?"
````

Expected (the temporary path differs):

```text
/tmp/…/guides/example.md:18: InferenceService llama: InferenceService in version "v1beta1" cannot be handled as a InferenceService: strict decoding error: unknown field "spec.engine.minReplica"
Checked 2 objects: 1 problem.
exit status 1
exit=1
```

`go run` prints `exit status 1` and exits 1 itself. Fix the example and run it again:

```bash
sed -i.bak 's/minReplica: 1/minReplicas: 1/' "$tmp/guides/example.md"
go run ./hack/docs-examples -content "$tmp"; echo "exit=$?"
rm -rf "$tmp"
```

Expected: `Checked 2 objects: 0 problems.` and `exit=0`.

Then run the make target:

```bash
make docs-examples; echo "exit=$?"
```

Expected until Task A1 merges: `docs-examples: lstat website/src/lib/content: no such file or directory`, then make's error, and `exit=2`. If `make envtest` fails in a sandbox, run `make -o envtest docs-examples ENVTEST=/tmp/fake-setup-envtest` instead, where `/tmp/fake-setup-envtest` is a script that prints the `KUBEBUILDER_ASSETS` path:

```bash
printf '#!/bin/sh\necho "%s"\n' "$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" > /tmp/fake-setup-envtest
chmod +x /tmp/fake-setup-envtest
```

- [ ] **Step 19: Check and commit**

```bash
~/.local/bin/pre-commit run --files hack/docs-examples/*.go Makefile
git diff --exit-code go.mod go.sum && echo "go.mod unchanged"
git add Makefile
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Add make docs-examples" -m "Run hack/docs-examples over website/src/lib/content with the envtest
binaries that make envtest installs."
```

Expected: every hook passes or is skipped, and `go.mod unchanged`. In a sandbox, the `go mod tidy` hook can fail with "operation not permitted" on the module cache; that's the sandbox, not the change, as long as `go.mod` and `go.sum` are unchanged.

### Task A4: The drift report

**Branch:** `docs/website-drift`

**Goal:** `make docs-drift` reports where `site/` has moved on since `website/` rewrote it. The nightly docs bot keeps changing `site/` until launch, and every change to a rewritten page has to reach its replacement.

**Files:**

- Create: `hack/docs-drift/main.go`, `hack/docs-drift/main_test.go`
- Modify: `Makefile` (a `docs-drift` target after `help` and before `generate-apiref`)

**How it works:** `website/redirects.json` (Task A1) is an array of `{ "old", "new", "rewrittenFrom" }` entries: the Hugo page under `site/content/en/docs`, the website page under `website/src/lib/content` that replaces it, and the last commit that changed the Hugo page when the website page was written, or `null` while the website page is a draft. The report lists:

- Hugo pages with no entry;
- entries whose Hugo page no longer exists;
- entries whose Hugo page changed after `rewrittenFrom`, with those commits (`git log rewrittenFrom..HEAD -- page`);
- entries whose website page doesn't exist.

It exits 0 when there's nothing to report, 1 when there is, and 2 when it can't run. `go run` and make turn every failure into their own exit codes, so the CI job (Task A2) runs a built binary. Every flag defaults to the repo layout, so neither caller passes flags.

This code was validated before the plan was written: the tests pass, and the binary reports no drift on this repo with the prototype's `redirects.json`, and reports a page whose `rewrittenFrom` is backdated.

- [ ] **Step 1: Write the test**

Create `hack/docs-drift/main_test.go`. `newRepo` builds a throwaway git repository with fixed identities and dates, so the commit lines are predictable:

```go
package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// testRepo is a git repository with Hugo pages a.md, b.md and c.md and
// website pages x.md and y.md.
type testRepo struct {
	dir    string
	first  string // adds every page, and d.md
	update string // changes a.md
	head   string // removes d.md
}

func newRepo(t *testing.T) testRepo {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		// Fixed identities and dates, and no user or system config, so
		// commits don't depend on the machine.
		cmd.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_AUTHOR_DATE=2026-01-02T03:04:05Z",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com", "GIT_COMMITTER_DATE=2026-01-02T03:04:05Z",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	git("init", "-q", "-b", "main")
	for _, page := range []string{"a.md", "b.md", "c.md", "d.md"} {
		write("site/content/en/docs/"+page, page+"\n")
	}
	write("website/src/lib/content/x.md", "x\n")
	write("website/src/lib/content/y.md", "y\n")
	git("add", ".")
	git("commit", "-q", "-m", "Add pages")
	repo := testRepo{dir: dir, first: git("rev-parse", "--short", "HEAD")}
	write("site/content/en/docs/a.md", "a, updated\n")
	git("commit", "-q", "-am", "Update a")
	repo.update = git("rev-parse", "--short", "HEAD")
	git("rm", "-q", "site/content/en/docs/d.md")
	git("commit", "-q", "-m", "Remove d")
	repo.head = git("rev-parse", "--short", "HEAD")
	return repo
}

func TestDrift(t *testing.T) {
	repo := newRepo(t)
	rewritten := func(rev string) *string { return &rev }
	a := entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.update)}
	b := entry{Old: "b.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}
	c := entry{Old: "c.md", New: "x.md"}

	tests := []struct {
		name    string
		entries []entry
		want    report
	}{
		{
			name:    "current",
			entries: []entry{a, b, c},
		},
		{
			name:    "changed after the rewrite",
			entries: []entry{{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.first)}, b, c},
			want: report{changed: []change{{
				entry:   entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.first)},
				commits: []string{repo.update + " 2026-01-02 Update a"},
			}}},
		},
		{
			name:    "drafts are not compared",
			entries: []entry{{Old: "a.md", New: "x.md"}, b, c},
		},
		{
			name:    "unmapped page",
			entries: []entry{a, c},
			want:    report{unmapped: []string{"b.md"}},
		},
		{
			name:    "removed page",
			entries: []entry{a, b, c, {Old: "d.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}},
			want:    report{oldMissing: []entry{{Old: "d.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}}},
		},
		{
			name:    "missing website page",
			entries: []entry{a, b, {Old: "c.md", New: "z.md"}},
			want:    report{newMissing: []entry{{Old: "c.md", New: "z.md"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drift(writeRedirects(t, repo, tc.entries))
			if err != nil {
				t.Fatalf("drift() error: %v", err)
			}
			if diff := cmp.Diff(&tc.want, got, cmp.AllowUnexported(report{}, change{})); diff != "" {
				t.Errorf("drift() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDriftErrors(t *testing.T) {
	repo := newRepo(t)
	tests := []struct {
		name string
		rev  string
		want string
	}{
		{name: "unknown commit", rev: "0123456789", want: "git log 0123456789..HEAD"},
		{name: "not a hash", rev: "--output=log", want: `rewrittenFrom "--output=log" isn't a commit hash`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := []entry{{Old: "a.md", New: "x.md", RewrittenFrom: &tc.rev}}
			_, err := drift(writeRedirects(t, repo, entries))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("drift() error = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// writeRedirects writes entries to the repository's redirects file, which
// stays untracked, and returns a config for the repository.
func writeRedirects(t *testing.T, repo testRepo, entries []entry) config {
	t.Helper()
	cfg := config{
		repo:      repo.dir,
		oldDir:    "site/content/en/docs",
		newDir:    "website/src/lib/content",
		redirects: "website/redirects.json",
	}
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, cfg.redirects), data, 0o644); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestPrint(t *testing.T) {
	rev := "1234abcd"
	tests := []struct {
		name string
		rep  report
		want string
	}{
		{
			name: "empty",
			want: "No drift: every Hugo page is mapped, and every rewrite is current.\n",
		},
		{
			name: "every section",
			rep: report{
				unmapped:   []string{"b.md"},
				oldMissing: []entry{{Old: "d.md", New: "y.md"}},
				changed: []change{{
					entry:   entry{Old: "a.md", New: "x.md", RewrittenFrom: &rev},
					commits: []string{"5678ef90 2026-01-03 Update a again", "90ab1234 2026-01-02 Update a"},
				}},
				newMissing: []entry{{Old: "c.md", New: "z.md"}},
			},
			want: `Hugo pages missing from website/redirects.json:
  b.md
Entries whose Hugo page no longer exists:
  d.md (replaced by y.md)
Hugo pages changed since they were rewritten:
  a.md, rewritten from 1234abcd as x.md:
    5678ef90 2026-01-03 Update a again
    90ab1234 2026-01-02 Update a
Entries whose website page doesn't exist:
  c.md (replaced by z.md)
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			tc.rep.print(&b, "website/redirects.json")
			if diff := cmp.Diff(tc.want, b.String()); diff != "" {
				t.Errorf("print() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `go test ./hack/docs-drift/`

Expected: a build failure starting with `undefined: entry`.

- [ ] **Step 3: Write the command**

Create `hack/docs-drift/main.go`:

```go
// Command docs-drift reports where the Hugo documentation in site/ has
// drifted from its rewrite in website/.
//
// website/redirects.json maps every Hugo page to the website page that
// replaces it, with rewrittenFrom: the last commit that changed the Hugo
// page when the website page was written, or null while the website page
// is a draft. docs-drift reports:
//
//   - Hugo pages missing from redirects.json;
//   - entries whose Hugo page no longer exists;
//   - entries whose Hugo page changed after rewrittenFrom;
//   - entries whose website page doesn't exist.
//
// It exits 0 when there is nothing to report, 1 when there is, and 2 when
// it can't run.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

func main() {
	os.Exit(run())
}

func run() int {
	var cfg config
	flag.StringVar(&cfg.repo, "repo", ".", "`directory` of the git repository")
	flag.StringVar(&cfg.oldDir, "old", "site/content/en/docs", "`directory` of the Hugo pages, relative to -repo")
	flag.StringVar(&cfg.newDir, "new", "website/src/lib/content", "`directory` of the website pages, relative to -repo")
	flag.StringVar(&cfg.redirects, "redirects", "website/redirects.json", "redirects `file`, relative to -repo")
	flag.Parse()

	rep, err := drift(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "docs-drift: %v\n", err)
		return 2
	}
	rep.print(os.Stdout, cfg.redirects)
	if !rep.empty() {
		return 1
	}
	return 0
}

type config struct {
	repo      string
	oldDir    string
	newDir    string
	redirects string
}

// An entry is one row of redirects.json.
type entry struct {
	Old           string  `json:"old"`
	New           string  `json:"new"`
	RewrittenFrom *string `json:"rewrittenFrom"`
}

// A change is a Hugo page that changed after it was rewritten.
type change struct {
	entry   entry
	commits []string // "hash date subject", newest first
}

// A report lists the drift between the Hugo pages and their rewrites.
type report struct {
	unmapped   []string // Hugo pages with no entry
	oldMissing []entry  // entries whose Hugo page no longer exists
	changed    []change
	newMissing []entry // entries whose website page doesn't exist
}

func (r *report) empty() bool {
	return len(r.unmapped)+len(r.oldMissing)+len(r.changed)+len(r.newMissing) == 0
}

// print writes the report to w. redirects names the redirects file.
func (r *report) print(w io.Writer, redirects string) {
	if r.empty() {
		fmt.Fprintln(w, "No drift: every Hugo page is mapped, and every rewrite is current.")
		return
	}
	if len(r.unmapped) > 0 {
		fmt.Fprintf(w, "Hugo pages missing from %s:\n", redirects)
		for _, p := range r.unmapped {
			fmt.Fprintf(w, "  %s\n", p)
		}
	}
	if len(r.oldMissing) > 0 {
		fmt.Fprintln(w, "Entries whose Hugo page no longer exists:")
		for _, e := range r.oldMissing {
			fmt.Fprintf(w, "  %s (replaced by %s)\n", e.Old, e.New)
		}
	}
	if len(r.changed) > 0 {
		fmt.Fprintln(w, "Hugo pages changed since they were rewritten:")
		for _, c := range r.changed {
			fmt.Fprintf(w, "  %s, rewritten from %s as %s:\n", c.entry.Old, *c.entry.RewrittenFrom, c.entry.New)
			for _, commit := range c.commits {
				fmt.Fprintf(w, "    %s\n", commit)
			}
		}
	}
	if len(r.newMissing) > 0 {
		fmt.Fprintln(w, "Entries whose website page doesn't exist:")
		for _, e := range r.newMissing {
			fmt.Fprintf(w, "  %s (replaced by %s)\n", e.Old, e.New)
		}
	}
}

// commitHash matches an abbreviated or full commit hash.
var commitHash = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// drift compares the Hugo pages with the redirects file and the website
// pages.
func drift(cfg config) (*report, error) {
	data, err := os.ReadFile(filepath.Join(cfg.repo, cfg.redirects))
	if err != nil {
		return nil, err
	}
	var entries []entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse %s: %w", cfg.redirects, err)
	}
	oldPages, err := listPages(filepath.Join(cfg.repo, cfg.oldDir))
	if err != nil {
		return nil, err
	}

	rep := &report{}
	mapped := make(map[string]bool)
	for _, e := range entries {
		mapped[e.Old] = true
		switch {
		case !oldPages[e.Old]:
			rep.oldMissing = append(rep.oldMissing, e)
		case e.RewrittenFrom != nil:
			commits, err := commitsSince(cfg.repo, *e.RewrittenFrom, path.Join(filepath.ToSlash(cfg.oldDir), e.Old))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", e.Old, err)
			}
			if len(commits) > 0 {
				rep.changed = append(rep.changed, change{entry: e, commits: commits})
			}
		}
		if _, err := os.Stat(filepath.Join(cfg.repo, cfg.newDir, filepath.FromSlash(e.New))); errors.Is(err, fs.ErrNotExist) {
			rep.newMissing = append(rep.newMissing, e)
		} else if err != nil {
			return nil, err
		}
	}
	for _, p := range slices.Sorted(maps.Keys(oldPages)) {
		if !mapped[p] {
			rep.unmapped = append(rep.unmapped, p)
		}
	}
	return rep, nil
}

// listPages returns the Markdown files under dir, as slash-separated paths
// relative to dir.
func listPages(dir string) (map[string]bool, error) {
	pages := make(map[string]bool)
	err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(p) != ".md" {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		pages[filepath.ToSlash(rel)] = true
		return nil
	})
	return pages, err
}

// commitsSince returns the commits after rev that changed file, newest
// first, as "hash date subject". file is relative to the repository.
func commitsSince(repo, rev, file string) ([]string, error) {
	if !commitHash.MatchString(rev) {
		return nil, fmt.Errorf("rewrittenFrom %q isn't a commit hash", rev)
	}
	cmd := exec.Command("git", "-C", repo, "log", "--date=short", "--format=%h %ad %s", rev+"..HEAD", "--", file)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log %s..HEAD: %v: %s", rev, err, strings.TrimSpace(stderr.String()))
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return nil, nil
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n"), nil
}
```

- [ ] **Step 4: Run the tests to see them pass**

```bash
go vet ./hack/docs-drift/
go test ./hack/docs-drift/ -v 2>&1 | grep -E '^(--- |ok|FAIL)'
```

Expected: `--- PASS` for `TestDrift` (6 subtests), `TestDriftErrors` (2) and `TestPrint` (2), then `ok`.

- [ ] **Step 5: Commit**

```bash
git add hack/docs-drift
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Report drift between site/ and website/" -m "hack/docs-drift reads website/redirects.json and lists Hugo pages with
no entry, entries whose Hugo page or website page is missing, and Hugo
pages changed since the commit their rewrite started from."
```

- [ ] **Step 6: Add the make target**

In `Makefile`, add this target in the Documentation section, after `help` and before `.PHONY: generate-apiref`. Task A3 adds `docs-examples` after `generate-apiref`, so the two edits merge cleanly.

```make
.PHONY: docs-drift
docs-drift: ## 📚 Report Hugo pages that changed since website/ rewrote them
	@$(GO_CMD) run ./hack/docs-drift

```

The recipe line starts with a tab; keep the blank line before `.PHONY: generate-apiref`.

- [ ] **Step 7: Try it on this repo**

`website/` doesn't exist in this worktree, so first confirm the error, then borrow the prototype's redirects and drafts for a moment:

```bash
make docs-drift; echo "exit=$?"
```

Expected: `docs-drift: open website/redirects.json: no such file or directory`, then make's error, and `exit=2`.

```bash
test ! -e website || { echo "website/ exists; don't run this step"; exit 1; }
mkdir -p website/src/lib
cp /tmp/ome-render-proto/redirects.json website/redirects.json
cp -R /tmp/ome-render-proto/src/lib/content website/src/lib/content
go build -o /tmp/docs-drift ./hack/docs-drift
/tmp/docs-drift; echo "exit=$?"
```

Expected: `No drift: every Hugo page is mapped, and every rewrite is current.` and `exit=0`: all 84 Hugo pages have an entry, every draft's `rewrittenFrom` is null, and every website page exists.

Backdate one rewrite to see a report:

```bash
python3 - <<'EOF'
import json
path = "website/redirects.json"
rows = json.load(open(path))
for row in rows:
    if row["old"] == "concepts/inference_service.md":
        row["rewrittenFrom"] = "c6198139"
json.dump(rows, open(path, "w"), indent=2)
EOF
/tmp/docs-drift; echo "exit=$?"
rm -rf website /tmp/docs-drift
```

Expected:

```text
Hugo pages changed since they were rewritten:
  concepts/inference_service.md, rewritten from c6198139 as concepts/serving/inference-services.md:
    2656a655 2026-08-28 [Docs] Remove stale Ray references (#794)
exit=1
```

More commits can appear if `site/content/en/docs/concepts/inference_service.md` changed after this plan was written. `git status` must be clean afterwards except for the Makefile.

- [ ] **Step 8: Check and commit**

```bash
~/.local/bin/pre-commit run --files hack/docs-drift/*.go Makefile
git diff --exit-code go.mod go.sum && echo "go.mod unchanged"
git add Makefile
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Add make docs-drift" -m "Run hack/docs-drift from the repo root. CI builds the binary instead,
because go run and make replace its exit codes."
```

Expected: every hook passes or is skipped, and `go.mod unchanged`. In a sandbox, the `go mod tidy` hook can fail with "operation not permitted" on the module cache; that's the sandbox, not the change, as long as `go.mod` and `go.sum` are unchanged.

## Phase B: Pages and UI

Dispatch B1 to B4 at the same time, each in its own worktree branched from the tip of `docs/site-redesign`: the commit that adds Phases B to D to this plan (`[Docs] Plan the docs UI, pages and content`), right after the Phase A merge. Their files are disjoint; see [File ownership](#file-ownership). B1 builds the docs UI that B3's and B4's pages render in, but they depend only on the contracts in [Shared interfaces](#shared-interfaces), so none of them waits for another.

An agent's worktree starts at `main`, which has no `website/` yet. Before anything else, create your task branch from `docs/site-redesign` and check where it starts:

```bash
git switch -c docs/website-ui docs/site-redesign    # your task's branch
git log -1 --format=%s
```

Expected: `[Docs] Plan the docs UI, pages and content`.

### What Phase A left

Read this before starting any Phase B task.

- A new worktree has no `website/node_modules`. Run `pnpm install --frozen-lockfile --offline` from `website/` first; the machine's pnpm store has every package, so it takes seconds and needs no network.
- `pnpm dev` serves the site at `http://localhost:5173/ome/`. Every IA page renders through `src/routes/[section=section]/[...slug]/+page.svelte`, a placeholder B1 replaces. It prints the title, the description, and for drafts "This page hasn't been rewritten yet."
- `/ome` answers `308` with `Location: /ome/`. Link to the home page with `` `${base}/` ``, not `base || '/'`.
- `src/hooks.server.ts` answers paths that only share the base's prefix, such as `/ome.` and `/omega`, with a plain `404`. SvelteKit would otherwise render them as app pages whose relative links and client base resolve outside the site.
- `pnpm test` runs 15 files and 155 tests. The content checks (`src/lib/docs/content.test.ts`, which calls `src/lib/docs/checks.ts`) run there and again in `pnpm build`: bad front matter, an `h1` in Markdown, broken relative links and anchors, links to the old Hugo site, nav entries with no page, pages with no nav entry, and callout, tab and fence syntax errors. Read `checks.ts` to see exactly what fails.
- `redirects.json` maps every Hugo page to its new page. An entry's `rewrittenFrom` is `null` while the new page is a draft.

### Servers during Phase B

The four Phase B tasks run at the same time on one machine. Each uses its own dev server port and stops only its own server. Port 5173 belongs to the orchestrator's live server for `docs/site-redesign`, which runs from the main checkout; never stop it or use its port.

| Task | Port |
|---|---|
| B1 | 5174 |
| B2 | 5175 |
| B3 | 5176 |
| B4 | 5177 |

Start, wait for and stop the server like this, with your port in place of 5174:

```bash
cd website
pnpm dev --port 5174 --strictPort > /tmp/b1-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5174/ome/ && break; sleep 1; done
# checks go here
pkill -f 'port 5174'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"
```

The second `pkill` stops the `workerd` process Vite starts for the Cloudflare platform, which outlives Vite otherwise. Both patterns match only your own processes: the first your port, the second your worktree's path, so they work from any directory in your worktree. Don't run `pkill -f 'vite dev'`, `pkill -f 'pages dev'`, `pkill workerd` or `pnpm preview` in Phase B. They stop or collide with the other tasks' servers. C1 runs the preview build.

For visual checks, use `meta browser` (run `meta browser --help` for its commands) to open your pages and take screenshots at 1280 and 390 pixels wide, and compare them with the mockups. If it isn't available, say so in your report and rely on the HTML checks; C1 repeats the visual review.

### Task B1: Docs UI and search

**Branch:** `docs/website-ui`

**Goal:** Doc pages look like the approved doc-template mockup, with a collapsible sidebar, breadcrumbs, a toolbar, since, preview and draft states, previous and next links and a live table of contents. Search works as a ⌘K dialog on every page and as a `/search` page. The site has a 404 page, and the Writing docs page is written.

**Files:**

- Create: `website/src/lib/ui/{highlight,group-hits,active-heading,search-keys,sidebar}.ts` and a `.test.ts` for each, `website/src/lib/ui/copy-code.ts`
- Create: `website/src/lib/components/{DocsLayout,DocsSidebar,DocsToc,Breadcrumbs,DocArticle,PageNav,SearchResults,SearchButton,SearchDialog}.svelte`
- Create: `website/src/routes/search/{+page.ts,+page.svelte}`, `website/src/routes/+error.svelte`
- Replace: `website/src/routes/[section=section]/[...slug]/+page.svelte`
- Modify: `website/src/lib/components/SiteHeader.svelte`, `website/src/styles/docs.css`, `website/src/styles/search.css`, `website/src/app.css` (header rules only)
- Rewrite: `website/src/lib/content/contributing/writing-docs.md`

**How it works:** The page route already returns everything the page shows ([Doc page data](#doc-page-data-a1-returns-b1-renders)), and the renderer's markup is fixed ([Renderer markup](#renderer-markup-a1-emits-b1-styles)). B1 is components and CSS on top of those two contracts. The logic that can be tested without a DOM lives in `src/lib/ui/` as plain functions with Vitest tests; the components call them. Those helpers and their tests were validated before this plan was written (24 tests pass), so copy them as they are.

Design references:

- The mockup `/Users/simolin/go/src/github.com/ome-projects/ome/.superpowers/brainstorm/68202-1790451826/content/doc-template.html`. Its `<style>` block has every color, border, radius and weight. It's drawn at about 85% of real size inside a 1040-pixel frame, so keep the type scale and column widths already in `docs.css` (from SMG) and take everything else from the mockup.
- SMG's docs components in `/tmp/smg-docs-ref/src/lib/components/{DocsLayout,DocsSidebar,DocsToc,DocMarkdown}.svelte`. Adapt their structure and behavior; don't copy SMG's per-section routes or its nav config.

#### Target styles

These are the mockup values, translated to the real scale where it matters. "Cobalt" is `var(--ome-accent)`.

| Element | Style |
|---|---|
| Sidebar landing link | 0.875rem, weight 600 |
| Sidebar group heading | a `<button>`: 0.72rem, weight 600, uppercase, letter-spacing 0.06em, muted; a `+` after it when closed and `−` when open |
| Sidebar link | 0.875rem, line-height 1.35, ink at opacity 0.72; active: cobalt, weight 600, opacity 1 |
| Preview pill (sidebar, group heading) | 0.66rem, weight 600, `#7a4a0c` on `#f3e2c4`, radius 999px, padding 0 0.35rem |
| Breadcrumb row | flex, space-between; 0.8125rem, muted; separator `›` |
| Toolbar buttons | 1.75rem square, radius 6px, background `rgba(0,0,0,.04)`, border `1px solid rgba(0,0,0,.08)`, muted icon; hover: cobalt |
| Lead paragraph | 1.125rem, muted, line-height 1.55 |
| Page badge next to the h1 (preview, since) | 0.72rem, weight 600, radius 999px, padding 0.125rem 0.5rem; preview is `#7a4a0c` on `#f3e2c4` |
| Inline code | background `rgba(36,71,184,.07)`, radius 4px, padding 1px 4px |
| Prerequisites | background `#eef0f5`, border `1px solid rgba(36,71,184,.14)`, radius 8px; its "Before you begin" heading drops the h2 mark and border and is 0.875rem, weight 600 |
| Callouts | `border-left: 3px solid var(--c)`, `background: color-mix(in srgb, var(--c) 7%, #fff)`, radius `0 6px 6px 0`; title in `var(--c)`, weight 600; `--c` is `--ome-note`, `--ome-tip`, `--ome-warning` or `--ome-danger`, and the preview callout uses `--ome-warning` |
| Tabs | border `1px solid rgba(0,0,0,.1)`, radius 8px, clipped; label bar `rgba(0,0,0,.03)` with a bottom border; label 0.875rem, weight 500, muted; checked label cobalt with `box-shadow: inset 0 -2px 0 var(--ome-accent)` |
| Code block | block `#16181d`, text `#e6e6e6`, radius 8px, clipped. `.doc-code-bar`: flex, space-between, `#20232a`, padding `6px 10px 6px 12px`, 0.75rem Inter, `#b8bcc6`. The copy button moves into the bar: border `rgba(255,255,255,.16)`, background `rgba(255,255,255,.1)`, radius 4px, color `#eee`. `.doc-pre` loses its absolute-button padding. |
| Output block | `.doc-code--output`: background `#f6f7f9`, text `#333`, border `1px solid rgba(0,0,0,.08)`; its bar `#eceef2`, muted |
| Tables | 0.875rem; cell borders `rgba(0,0,0,.1)`; header row background `rgba(0,0,0,.02)`, weight 600; wide tables scroll horizontally instead of overflowing |
| API field tables | `table.doc-api-fields` as above, with `code` in the first two columns; `.doc-api-required` is 0.66rem, weight 600, cobalt on `rgba(36,71,184,.1)`, radius 999px, padding 0 0.35rem |
| Previous and next | a two-column grid of bordered cards (radius 8px, padding 0.75rem 1rem); a small muted "Previous" or "Next" over the title; the next card is right-aligned; a card fills its column when the other is missing |
| Landing cards | `.doc-card` keeps its border, radius and background. The title link, `.doc-card strong a`, is ink (`var(--text-on-surface)`), weight 600, with no underline, and turns cobalt on hover; the description under it stays muted. A card's title is its only link (see [Landing cards](#landing-cards-b4-writes-b1-styles)) |
| TOC | heading "On this page": 0.72rem uppercase, weight 600, letter-spacing 0.06em, muted. Links 0.8125rem with a 2px transparent left border; active: cobalt, cobalt border, weight 500 |
| Since badges | `.doc-since--unreleased` on `#f3e2c4` in `#7a4a0c`; `--released` cobalt on `rgba(36,71,184,.1)`; `--unknown` muted on `rgba(0,0,0,.05)`; inside headings they sit after the text at 0.66rem |
| Draft banner | the note callout style, with the list of replaced pages as links |
| Header Search trigger | "Search" plus a `kbd` (0.66rem monospace, border `1px solid rgba(0,0,0,.1)`, radius 4px, padding 0 4px, muted); on the dark hero header the `kbd` border and text follow the header's text color at reduced opacity |
| Search dialog and page | box border `rgba(0,0,0,.12)`, radius 10px; input row 0.9375rem with a `⌕` icon; group label 0.66rem uppercase, weight 600, muted; result row 0.875rem with a 0.75rem muted second line; active row `rgba(36,71,184,.08)`; `mark` `rgba(36,71,184,.16)` with `color: inherit` and radius 2px |

- [ ] **Step 1: Write the helper tests**

Create the five test files from the validated prototype. `testing.ts` from Task A1 isn't needed; each test builds its own fixtures.

`website/src/lib/ui/highlight.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { highlight } from './highlight';

const marked = (text: string, query: string) =>
	highlight(text, query)
		.filter((segment) => segment.match)
		.map((segment) => segment.text);

describe('highlight', () => {
	it('marks terms case-insensitively', () => {
		expect(highlight('Serve models from a PVC', 'pvc')).toEqual([
			{ text: 'Serve models from a ', match: false },
			{ text: 'PVC', match: true }
		]);
	});

	it('marks only where a term starts a word', () => {
		expect(marked('OMENative and home', 'ome')).toEqual(['OME']);
	});

	it('splits the query on punctuation, like the index', () => {
		expect(marked('kubectl ome rollout', 'kubectl-ome')).toEqual(['kubectl', 'ome']);
		expect(highlight('a (b) c', '(b)')).toEqual([
			{ text: 'a (', match: false },
			{ text: 'b', match: true },
			{ text: ') c', match: false }
		]);
	});

	it('prefers the longest term at a position', () => {
		expect(marked('rollout', 'roll rollout')).toEqual(['rollout']);
	});

	it('escapes regular expression syntax', () => {
		expect(marked('x+y', 'x+y')).toEqual(['x+y']);
	});

	it('returns the text unmarked for an empty query', () => {
		expect(highlight('Install OME', '  ')).toEqual([{ text: 'Install OME', match: false }]);
	});

	it('returns nothing for empty text', () => {
		expect(highlight('', 'ome')).toEqual([]);
	});
});
```

`website/src/lib/ui/group-hits.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import type { SearchHit } from '$lib/docs/search-client';
import { groupHits } from './group-hits';

const hit = (route: string, sectionLabel: string): SearchHit => ({
	route,
	title: route,
	sectionLabel,
	group: null,
	description: '',
	status: null
});

describe('groupHits', () => {
	it('groups by section in order of first appearance, keeping rank order', () => {
		const a = hit('guides/a', 'Guides');
		const b = hit('concepts/b', 'Concepts');
		const c = hit('guides/c', 'Guides');
		expect(groupHits([a, b, c])).toEqual([
			{ label: 'Guides', hits: [a, c] },
			{ label: 'Concepts', hits: [b] }
		]);
	});

	it('returns no groups for no hits', () => {
		expect(groupHits([])).toEqual([]);
	});
});
```

`website/src/lib/ui/active-heading.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { activeHeading } from './active-heading';

describe('activeHeading', () => {
	const tops = [100, 500, 900];

	it('returns -1 above the first heading', () => {
		expect(activeHeading(tops, 80, false)).toBe(-1);
	});

	it('returns the last heading at or above the offset', () => {
		expect(activeHeading(tops, 100, false)).toBe(0);
		expect(activeHeading(tops, 600, false)).toBe(1);
		expect(activeHeading(tops, 1000, false)).toBe(2);
	});

	it('returns the last heading at the bottom of the page', () => {
		expect(activeHeading(tops, 0, true)).toBe(2);
	});

	it('returns -1 without headings', () => {
		expect(activeHeading([], 100, true)).toBe(-1);
	});
});
```

`website/src/lib/ui/search-keys.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { isSearchShortcut, moveSelection, shortcutLabel } from './search-keys';

const key = (init: Partial<KeyboardEvent>) => ({
	key: 'k',
	metaKey: false,
	ctrlKey: false,
	altKey: false,
	shiftKey: false,
	...init
});

describe('isSearchShortcut', () => {
	it('accepts ⌘K and Ctrl+K', () => {
		expect(isSearchShortcut(key({ metaKey: true }))).toBe(true);
		expect(isSearchShortcut(key({ ctrlKey: true }))).toBe(true);
		expect(isSearchShortcut(key({ key: 'K', metaKey: true }))).toBe(true);
	});

	it('rejects other keys and modifiers', () => {
		expect(isSearchShortcut(key({}))).toBe(false);
		expect(isSearchShortcut(key({ key: 'j', metaKey: true }))).toBe(false);
		expect(isSearchShortcut(key({ metaKey: true, shiftKey: true }))).toBe(false);
		expect(isSearchShortcut(key({ ctrlKey: true, altKey: true }))).toBe(false);
	});
});

describe('moveSelection', () => {
	it('moves and wraps', () => {
		expect(moveSelection(0, 1, 3)).toBe(1);
		expect(moveSelection(2, 1, 3)).toBe(0);
		expect(moveSelection(0, -1, 3)).toBe(2);
	});

	it('starts from either end without a selection', () => {
		expect(moveSelection(-1, 1, 3)).toBe(0);
		expect(moveSelection(-1, -1, 3)).toBe(2);
	});

	it('returns -1 without items', () => {
		expect(moveSelection(0, 1, 0)).toBe(-1);
	});
});

describe('shortcutLabel', () => {
	it('uses ⌘ on Apple platforms and Ctrl elsewhere', () => {
		expect(shortcutLabel('MacIntel')).toBe('⌘K');
		expect(shortcutLabel('iPhone')).toBe('⌘K');
		expect(shortcutLabel('Win32')).toBe('Ctrl K');
		expect(shortcutLabel('Linux x86_64')).toBe('Ctrl K');
		expect(shortcutLabel('')).toBe('Ctrl K');
	});
});
```

`website/src/lib/ui/sidebar.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import type { NavLink, NavTree } from '$lib/docs/navigation';
import { closedGroups } from './sidebar';

const link = (route: string): NavLink => ({
	path: `${route}.md`,
	route,
	label: route,
	title: route,
	description: '',
	status: null
});

const tree: NavTree = {
	id: 'guides',
	label: 'Guides',
	landing: link('guides'),
	groups: [
		{ label: null, preview: false, links: [link('guides/overview')] },
		{ label: 'Deploy models', preview: false, links: [link('guides/deploy-models/pvc')] },
		{ label: 'Networking', preview: false, links: [link('guides/networking/ingress')] },
		{ label: 'Multi-cluster', preview: true, links: [link('guides/multi-cluster/setup')] }
	]
};

describe('closedGroups', () => {
	it('opens only the group holding the page', () => {
		expect(closedGroups(tree, 'guides/networking/ingress', false)).toEqual(
			new Set(['Deploy models', 'Multi-cluster'])
		);
	});

	it('opens every group on the landing page', () => {
		expect(closedGroups(tree, 'guides', false)).toEqual(new Set());
	});

	it('closes every group on the landing page in the compact layout', () => {
		expect(closedGroups(tree, 'guides', true)).toEqual(
			new Set(['Deploy models', 'Networking', 'Multi-cluster'])
		);
	});

	it('keeps the page group open in the compact layout', () => {
		expect(closedGroups(tree, 'guides/deploy-models/pvc', true)).toEqual(
			new Set(['Networking', 'Multi-cluster'])
		);
	});

	it('never closes the unlabeled group', () => {
		expect(closedGroups(tree, 'guides/overview', true)).toEqual(
			new Set(['Deploy models', 'Networking', 'Multi-cluster'])
		);
	});
});
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd website && pnpm test src/lib/ui`

Expected: 5 failed test files, each with an error like `Failed to resolve import "./highlight"`.

- [ ] **Step 3: Write the helpers**

`website/src/lib/ui/highlight.ts`:

```ts
/** A run of text that does or doesn't match the query. */
export interface Segment {
	text: string;
	match: boolean;
}

// MiniSearch's default tokenizer splits on this, so query terms split the same way.
const SEPARATOR = /[\n\r\p{Z}\p{P}]+/u;

/**
 * Splits text into runs, marking each place where a query term starts a word, as
 * the index's prefix search matches. Fuzzy matches aren't marked.
 */
export function highlight(text: string, query: string): Segment[] {
	if (text === '') return [];
	const terms = [...new Set(query.toLowerCase().split(SEPARATOR).filter(Boolean))]
		// Longer terms first, so "rollout" wins over "roll" at the same position.
		.sort((a, b) => b.length - a.length)
		.map(escapeRegExp);
	if (terms.length === 0) return [{ text, match: false }];

	const pattern = new RegExp(`(?<![\\p{L}\\p{N}])(?:${terms.join('|')})`, 'giu');
	const segments: Segment[] = [];
	let last = 0;
	for (const match of text.matchAll(pattern)) {
		if (match.index > last) segments.push({ text: text.slice(last, match.index), match: false });
		segments.push({ text: match[0], match: true });
		last = match.index + match[0].length;
	}
	if (last < text.length) segments.push({ text: text.slice(last), match: false });
	return segments;
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
```

`website/src/lib/ui/group-hits.ts`:

```ts
import type { SearchHit } from '$lib/docs/search-client';

export interface HitGroup {
	label: string;
	hits: SearchHit[];
}

/** Groups hits by section, in the order each section first appears. Hits keep their rank order. */
export function groupHits(hits: readonly SearchHit[]): HitGroup[] {
	const groups = new Map<string, SearchHit[]>();
	for (const hit of hits) {
		const group = groups.get(hit.sectionLabel);
		if (group) group.push(hit);
		else groups.set(hit.sectionLabel, [hit]);
	}
	return [...groups].map(([label, grouped]) => ({ label, hits: grouped }));
}
```

`website/src/lib/ui/active-heading.ts`:

```ts
/**
 * Returns the index of the heading being read: the last one whose top is at or
 * above offset, or the last heading once the page is scrolled to the bottom,
 * where the final headings may never reach offset. Returns -1 above the first
 * heading and when there are no headings.
 */
export function activeHeading(tops: readonly number[], offset: number, atBottom: boolean): number {
	if (tops.length === 0) return -1;
	if (atBottom) return tops.length - 1;
	let active = -1;
	for (const [index, top] of tops.entries()) {
		if (top > offset) break;
		active = index;
	}
	return active;
}
```

`website/src/lib/ui/search-keys.ts`:

```ts
type ShortcutEvent = Pick<KeyboardEvent, 'key' | 'metaKey' | 'ctrlKey' | 'altKey' | 'shiftKey'>;

/** Reports whether a keydown opens search: ⌘K or Ctrl+K. */
export function isSearchShortcut(event: ShortcutEvent): boolean {
	return (
		event.key.toLowerCase() === 'k' &&
		(event.metaKey || event.ctrlKey) &&
		!event.altKey &&
		!event.shiftKey
	);
}

/**
 * Moves a selection by one through count items, wrapping at either end. From no
 * selection (-1), down selects the first item and up the last. Returns -1 when
 * there's nothing to select.
 */
export function moveSelection(current: number, delta: 1 | -1, count: number): number {
	if (count === 0) return -1;
	if (current < 0) return delta === 1 ? 0 : count - 1;
	return (current + delta + count) % count;
}

/** Returns the shortcut's label for a platform string such as navigator.platform. */
export function shortcutLabel(platform: string): string {
	return /mac|iphone|ipad/i.test(platform) ? '⌘K' : 'Ctrl K';
}
```

`website/src/lib/ui/sidebar.ts`:

```ts
import type { NavTree } from '$lib/docs/navigation';

/**
 * Returns the labels of the sidebar groups that start closed. On a section's
 * landing page every group starts open, because the sidebar is the section's
 * map; elsewhere only the group holding the page does. In the compact layout,
 * where the sidebar sits above the article, even the landing page starts with
 * every group closed.
 */
export function closedGroups(tree: NavTree, route: string, compact: boolean): Set<string> {
	const onLanding = tree.landing?.route === route;
	const closed = new Set<string>();
	for (const group of tree.groups) {
		if (group.label === null) continue;
		const holdsPage = group.links.some((link) => link.route === route);
		if (holdsPage || (onLanding && !compact)) continue;
		closed.add(group.label);
	}
	return closed;
}
```

`website/src/lib/ui/copy-code.ts` is a Svelte action and has no unit test; Step 9 checks it in the browser:

```ts
const RESET_MS = 1500;

/**
 * Copies a code block's text when its Copy button is clicked. One delegated
 * listener serves every block, so blocks rendered by client-side navigation
 * work without rebinding.
 */
export function copyCode(node: HTMLElement) {
	const timers = new Map<HTMLButtonElement, ReturnType<typeof setTimeout>>();

	async function onClick(event: MouseEvent) {
		if (!(event.target instanceof Element)) return;
		const button = event.target.closest<HTMLButtonElement>('.doc-code-copy');
		if (!button || !node.contains(button)) return;
		const code = button.closest('.doc-code')?.querySelector('pre code')?.textContent ?? '';
		let label = 'Copied';
		try {
			await navigator.clipboard.writeText(code);
		} catch {
			label = 'Failed';
		}
		button.textContent = label;
		clearTimeout(timers.get(button));
		timers.set(
			button,
			setTimeout(() => {
				button.textContent = 'Copy';
				timers.delete(button);
			}, RESET_MS)
		);
	}

	node.addEventListener('click', onClick);
	return {
		destroy() {
			node.removeEventListener('click', onClick);
			for (const timer of timers.values()) clearTimeout(timer);
		}
	};
}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cd website && pnpm test`

Expected: `Test Files  20 passed (20)` and `Tests  170 passed (170)`.

- [ ] **Step 5: Commit**

```bash
git add website/src/lib/ui
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Add the docs UI helpers" -m "Pure functions for search highlighting and grouping, the active
table-of-contents entry, search keys and sidebar state, plus the
code copy action."
```

- [ ] **Step 6: Build the doc page components**

Every component uses runes, reads `base` from `$app/paths`, and builds URLs as `` `${base}/${link.route}` ``. Use `$app/stores` for `page`, as the existing components do.

**`DocsLayout.svelte`**. Props: `tree: NavTree`, `route: string`, `toc: TocEntry[]`, `children: Snippet`. It renders `div.docs-layout` (plus `docs-layout--with-toc` when the TOC shows), holding `<DocsSidebar {tree} {route} />` wrapped in `{#key tree.id}` so state resets between sections, then `div.docs-layout-content` with the children, then `<DocsToc {toc} />`. The TOC shows when `toc` has at least two entries.

**`DocsSidebar.svelte`**. Props: `tree: NavTree`, `route: string`.

- Markup: `nav.docs-sidebar` with `aria-label="{tree.label} pages"`. First the landing link, `a.docs-sidebar-link.docs-sidebar-link--index`, labeled with `tree.label`. Then each group. A group whose `label` is null renders its links with no heading. A labeled group renders `button.docs-sidebar-heading` with `aria-expanded` and `aria-controls` pointing at its `ul.docs-sidebar-links`, the label, and a preview pill when `group.preview` is true.
- Links: `a.docs-sidebar-link` with `class:active` and `aria-current="page"` on the current page, and a preview pill when `link.status === 'preview'`.
- State: `let closed = $state(closedGroups(tree, route, false))`. That value is the same on the server and the client, so hydration matches. In an `$effect` that runs once on mount, if `matchMedia('(max-width: 900px)').matches`, set `closed = closedGroups(tree, route, true)`. Another `$effect` reopens the group holding `route` whenever `route` changes, so following a previous or next link into a closed group opens it. Reassign a new `Set` on every change; a mutated `Set` in `$state` isn't reactive.
- Open and close groups with `transition:slide={{ duration: 180 }}`, and skip the transition when `prefers-reduced-motion` is set.

**`DocsToc.svelte`**. Props: `toc: TocEntry[]`.

- Markup: `nav.docs-toc` with `aria-label="On this page"`, the heading `p.docs-toc-heading` "On this page", and `ul.docs-toc-list` of `li.docs-toc-item` (plus `docs-toc-item--h3` for depth 3) holding `a.docs-toc-link` to `#id`. The active link gets `class:active` and `aria-current="location"`.
- Long pages: when `toc` has more than 40 entries, as the generated API reference does, list only the depth-2 entries.
- Tracking: in an `$effect` that depends on `toc`, listen to `scroll` and `resize` (passive, throttled with `requestAnimationFrame`). Each frame, read each heading's `getBoundingClientRect().top`, take `offset` as the header height (`parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--site-header-height'))`) plus 24, take `atBottom` as `window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2`, and set the active index to `activeHeading(tops, offset, atBottom)`. Remove the listeners in the cleanup.
- CSS in `docs.css`: `.doc-article :is(h2, h3) { scroll-margin-top: calc(var(--site-header-height) + 1rem); }` so anchor jumps land below the header.

**`Breadcrumbs.svelte`**. Props: `tree: NavTree`, `position: PagePosition`, `route: string`. It renders `nav.doc-breadcrumbs` with `aria-label="Breadcrumb"` and an `ol`: the section label, linked to the landing page unless this is the landing page, where it gets `aria-current="page"`; then `position.group` as plain text when it isn't null. The page title isn't repeated, because the `h1` is right below.

**`PageNav.svelte`**. Props: `position: PagePosition`. It renders nothing when both links are null. Otherwise `nav.doc-pagenav` with `aria-label="Previous and next pages"`, holding `a.doc-pagenav-link.doc-pagenav-link--prev` and `--next`, each with `span.doc-pagenav-label` ("Previous" or "Next") and `span.doc-pagenav-title` (`link.label`).

**`DocArticle.svelte`**. Props: `data: DocPageData` (import the type from `$lib/docs/page-data`). It renders `article.doc-article`:

1. `div.doc-article-head`: `<Breadcrumbs>` and `div.doc-toolbar`. The toolbar has an Edit link (`a.doc-toolbar-btn`, `aria-label="Edit this page on GitHub"`) when `data.editUrl` isn't null, and always a View source link (`aria-label="View Markdown source on GitHub"`) to `data.sourceUrl`. Both open in a new tab with `rel="noopener noreferrer"` and use SMG's icons from `DocMarkdown.svelte`.
2. `h1`, holding the title, then a `span.doc-status-badge` "Preview" when `data.page.status === 'preview'`, then `span.doc-since.doc-since--{data.since.state}` with `data.since.label` when `data.since` isn't null.
3. `p.doc-lead` with the description.
4. For preview pages, `aside.doc-admonition.doc-admonition--preview` with the title "In development" and the text "Multi-cluster routing is alpha. Fields and behavior can change between releases." Every preview page is a multi-cluster page (see the spec's Preview pages section).
5. For drafts, `aside.doc-draft` in place of the body. Its title is "This page is being rewritten". When `data.replaces` isn't empty, the text is "Until it's done, the current docs cover this topic:" followed by a list of links to each `url`, labeled with the URL without `https://`. When `replaces` is empty, the text is "This is a new page, and it hasn't been written yet."
6. Otherwise `div.doc-body` with `use:copyCode` and `{@html data.html}`. Put `<!-- eslint-disable-next-line svelte/no-at-html-tags -- rendered at build time from this repo's own Markdown -->` on the line above.
7. `<PageNav position={data.position} />`.

**`src/routes/[section=section]/[...slug]/+page.svelte`** becomes:

```svelte
<script lang="ts">
	import DocArticle from '$lib/components/DocArticle.svelte';
	import DocsLayout from '$lib/components/DocsLayout.svelte';

	let { data } = $props();
</script>

<DocsLayout tree={data.tree} route={data.page.route} toc={data.page.toc}>
	<DocArticle {data} />
</DocsLayout>
```

- [ ] **Step 7: Restyle `docs.css`**

Restyle `website/src/styles/docs.css` to [Target styles](#target-styles), keeping its section order and the SMG layout grid and breakpoints (1100 and 900 pixels). Specifically:

- Replace the single-color admonition rules, including `.doc-admonition--info`, with the four callout types plus `--preview`, each setting `--c`.
- Rebuild the code block around `.doc-code-bar`, `.doc-code-title` and `.doc-code-copy` inside the bar, and add `.doc-code--output`. Keep the syntax colors.
- Add `.doc-article-head`, `.doc-breadcrumbs`, `.doc-lead`, `.doc-status-badge`, `.doc-since` and its three states, `.doc-draft`, `.doc-pagenav*`, `table.doc-api-fields` and `.doc-api-required`, the TOC active state, and the sidebar pill and `+`/`−` indicator.
- Make the sidebar sticky under the header with its own scroll, as the TOC already is.
- Style the landing card's title link, and delete SMG's action-link rules, `.doc-card > p:last-child a` and its `:hover`: a card's last paragraph is now its description.
- Move these rules out of the `@media (max-width: 900px)` block (around lines 725 to 744) into `app.css`, inside a `@media (max-width: 900px)` block after the header rules: `.site-header-inner { align-items: center; gap: 1rem; }`, `.site-brand { max-width: min(20rem, 70vw); }`, `.site-header-right { display: none; }` and `.site-menu-toggle { display: inline-flex; }`. Delete the `.home-hero { min-height: 100vh; }` rule there; `home.css` owns the hero.

Nothing in `docs.css` may style the header or the home page after this step.

- [ ] **Step 8: Check the doc pages**

Run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build` from `website/`, and fix every failure. Then start the dev server on port 5174 (see [Servers during Phase B](#servers-during-phase-b)) and check the HTML:

```bash
B=http://localhost:5174/ome
P=$B/guides/deploy-models/serve-models-from-pvc
curl -s $B/guides | grep -o 'class="docs-sidebar-link' | wc -l
curl -s $B/guides | grep -o 'aria-expanded="true"' | wc -l
curl -s $P | grep -o 'class="docs-sidebar-link' | wc -l
curl -s $P | grep -o 'aria-expanded="true"' | wc -l
curl -s $P | grep -o 'This page is being rewritten\|class="doc-breadcrumbs\|class="doc-pagenav\|class="doc-toolbar-btn' | sort | uniq -c
```

Expected, in order: `30` sidebar links on the Guides landing page (the landing link and 29 pages) and `7` open groups, because every group starts open there; then `7` links and `1` open group on the PVC page, where only Deploy models starts open; then `class="doc-breadcrumbs`, `class="doc-pagenav` and `This page is being rewritten` once each and `class="doc-toolbar-btn` twice. The PVC page is a draft on your branch; B4 writes it.

No written page is on your branch yet, so make a scratch page to see the rest. Temporarily replace `website/src/lib/content/getting-started/introduction.md` with front matter that keeps its `title` and `description`, sets `status: preview`, `since: v1.3` and `generated: true`, and a body with one of each block in [Renderer markup](#renderer-markup-a1-emits-b1-styles): prerequisites, a titled YAML block, a Shell block, an output block, the four callouts, tabs, details, a card grid of two cards in the [landing format](#landing-cards-b4-writes-b1-styles), a table, an `h2` with `{since=v1.3}`, an `h2` with `{since=v1.0}`, a few `h3`s, and enough text to scroll. Check that the page shows the Preview badge, the page's since badge, the In development callout, one toolbar button (View source, since the page is generated), both heading badges, and a TOC that follows the scroll. Click a Copy button and check that it reads "Copied", then "Copy" again. Screenshot it at 1280 and 390 pixels wide next to the mockup, along with the Guides landing page and the PVC draft.

The since labels depend on the latest release, which the server fetches from the GitHub API. When the fetch works, the page badge reads "Unreleased: coming in v1.3" (the latest release is v1.2.2) and the heading badges read "Unreleased: coming in v1.3" and "New in v1.0". Unauthenticated requests share a limit of 60 an hour per IP, and when it runs out, the dev log shows `403 (rate limit remaining=0, …)` and every badge reads "Since v1.3" or "Since v1.0" in the muted `--unknown` style. Either result is correct; say which one you saw in your report.

Then restore the page:

```bash
git checkout -- website/src/lib/content/getting-started/introduction.md
pkill -f 'port 5174'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"
```

- [ ] **Step 9: Commit**

```bash
git add website/src
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Add the docs page layout" -m "Sidebar, breadcrumbs, toolbar, since, preview and draft states,
previous and next links and a live table of contents, restyled to
the approved doc template."
```

- [ ] **Step 10: Build search**

**`SearchResults.svelte`**. Props: `groups: HitGroup[]`, `query: string`, `active: number` (default -1), `idPrefix: string`, `onchoose?: () => void`. It renders one `div.search-group` per group, holding `p.search-group-label` and a `ul` of `li` rows. Each row has `role="option"`, `id="{idPrefix}-{i}"` and `aria-selected={i === active}`, where `i` counts across all groups, and holds an `a.search-hit` to `` `${base}/${hit.route}` `` with `class:active`. The link shows the title and, on a second line, `hit.group` and the description joined by " · ". Both lines are rendered from `highlight(text, query)`, with matches in `<mark>`. A `span.search-hit-status` shows "Preview" or "Draft" for those statuses. Clicking a link calls `onchoose`. Render the text through `{#each}` over segments, never `{@html}`.

**`SearchDialog.svelte`**. It exports `open(initialQuery = '')` for `bind:this`.

- Markup: a native `<dialog class="search-dialog" aria-label="Search the docs">` opened with `showModal()`, so it sits in the top layer above the header. Inside: a `form.search-dialog-form` with `method="GET"` and `action="{base}/search"`, holding an `input` (`type="search"`, `name="q"`, `autocomplete="off"`, `spellcheck="false"`, `placeholder="Search the docs"`, `role="combobox"`, `aria-autocomplete="list"`, `aria-controls="search-dialog-results"`, `aria-expanded` when there are hits, and `aria-activedescendant` set to the active row's id). Below it, `div#search-dialog-results` with `role="listbox"` holds `<SearchResults>` with `idPrefix="search-dialog-hit"`. A footer shows the keys (`↑` `↓` to move, `↵` to open, `esc` to close) and "See all results", linked to `/search?q=…`.
- Loading: the index loads on the first `open()` through `loadSearch(base)`. Show "Loading the search index…" until it resolves. On failure, show "Search couldn't load. Check your connection and try again." with a Retry button that calls `loadSearch(base)` again.
- Results: `search(query, 12)`, grouped by `groupHits`. When the results change, the active row resets to 0 if there are hits and -1 otherwise. With no hits for a non-empty query, show `No pages match “{query}”.`
- Keys, handled on the input: ArrowDown and ArrowUp call `moveSelection` and `preventDefault`, and scroll the active row into view with `block: 'nearest'`. Enter opens the active hit with `goto` and closes the dialog; with no active hit, the form submits to `/search`. The dialog's native `cancel` event handles Escape.
- Global shortcut: `<svelte:window onkeydown={...}>` opens the dialog when `isSearchShortcut(event)` is true, calling `preventDefault` so the browser's own ⌘K doesn't fire. When it's already open, it focuses and selects the input.
- Closing: a click on the `<dialog>` element itself (the backdrop) closes it. Choosing a result closes it. On close, focus returns to the element that had it before `open()`.
- Styles: the dialog inherits the header's white text on the home hero, because inheritance follows the DOM, not the top layer. Set `color`, `font`, `line-height` and `text-align` on `.search-dialog` explicitly. Lock page scroll while it's open with `:root:has(.search-dialog[open]) { overflow: hidden; }`.

**`SearchButton.svelte`**. Props: `onopen: () => void`, `variant: 'pill' | 'menu'`, `onclick?: () => void`. It renders `<a href="{base}/search" class="site-search-trigger">`, so without JavaScript it goes to the search page. Its click handler calls `preventDefault`, then `onclick?.()`, then `onopen()`. The `pill` variant shows "Search" and a `kbd` with `shortcutLabel(navigator.platform)`, set in an `$effect`; the server renders `⌘K`, and the effect switches it to `Ctrl K` off Apple platforms. The `menu` variant shows only "Search".

**`SiteHeader.svelte`**:

- `let searchDialog = $state<SearchDialog | null>(null);`, and render `<SearchDialog bind:this={searchDialog} />` inside `<header>` after `div.site-header-inner`. Don't put it in `.site-header-right`, which is hidden at 900 pixels and below.
- Replace the desktop Search `<li>` with `<li><SearchButton variant="pill" onopen={() => searchDialog?.open()} /></li>`.
- Replace the mobile Search `<li>` with `<li><SearchButton variant="menu" onclick={closeMenu} onopen={() => searchDialog?.open()} /></li>`.
- Change the brand link's `href={base || '/'}` to `` href="{base}/" `` (see [What Phase A left](#what-phase-a-left)).

**`app.css`**, header rules only: style `.site-search-trigger` and its `kbd` inside `.site-nav-links` so the pill keeps its height, on both the hero and the solid header, and in the mobile menu. Also make `.site-nav-links a.active` visibly current on the solid header (cobalt text), keeping the hero header as it is.

**`search.css`**: the dialog (top 12vh, width `min(40rem, calc(100vw - 2rem))`, max-height 70vh with the results scrolling, `::backdrop` `rgba(15, 20, 35, 0.35)`) and the `/search` page, using the search rows in [Target styles](#target-styles).

**`src/routes/search/+page.ts`**:

```ts
import type { PageLoad } from './$types';

export const load: PageLoad = () => ({
	title: 'Search',
	description: 'Search the OME documentation.'
});
```

**`src/routes/search/+page.svelte`**: an `h1` "Search", a GET `form` whose input starts with `$page.url.searchParams.get('q') ?? ''`, and results from `loadSearch(base)` rendered with `<SearchResults>` (`search(query, 50)`, `idPrefix="search-page-hit"`), with the loading, error and no-results states from the dialog. Typing updates the results and replaces the URL with `goto(`?q=${encodeURIComponent(query)}`, { replaceState: true, keepFocus: true, noScroll: true })`, debounced by 150 ms. The page uses the `.site-main--docs` spacing from the layout and a single column no wider than 48rem.

**`src/routes/+error.svelte`**: for 404, an `h1` "Page not found", the text "There's no page at this address. It may have moved when the docs were reorganized.", a search form like the search page's (GET to `{base}/search`) prefilled with the last path segment with hyphens turned into spaces, and links to the four header sections. For other statuses, "Something went wrong", `$page.error?.message`, and a link to the home page.

- [ ] **Step 11: Check search**

Run the four website checks again. Then, with the dev server on port 5174:

```bash
B=http://localhost:5174/ome
curl -s "$B/search?q=pvc" | grep -o '<title>[^<]*</title>\|value="pvc"'
curl -s -o /dev/null -w '%{http_code}\n' $B/guides/no-such-page
curl -s $B/guides/no-such-page | grep -o 'Page not found\|value="no such page"' | sort | uniq -c
curl -s $B/getting-started | grep -o 'class="site-search-trigger' | wc -l
```

Expected: `<title>Search · OME</title>` and `value="pvc"`; `404`; `Page not found` at least twice (the `<title>` and the `h1`) and `value="no such page"` once; and `1` trigger (the mobile menu isn't rendered until it opens).

In the browser: press ⌘K on a doc page and on the home page, type `pvc`, move with the arrow keys, press Enter, and check that the dialog closes on the PVC page. Check that Escape and a backdrop click close it and that focus returns to where it was. Open the mobile menu at 390 pixels and choose Search. Check the dialog's text is dark on the home page. Screenshot the dialog and the search page.

- [ ] **Step 12: Commit**

```bash
git add website/src
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Add search and the error page" -m "A ⌘K search dialog on every page and a /search page, both searching
the prerendered index in the browser, and a 404 page with a search
box."
```

- [ ] **Step 13: Write the Writing docs page**

Replace the draft `website/src/lib/content/contributing/writing-docs.md` with the style and authoring guide. Keep its `title` and `description`, and remove `status: draft`. It has no `redirects.json` entry, because it's new. Sections, in order:

1. **Where the docs live.** `website/src/lib/content/<section>/…`, the five sections, URLs as file paths without `.md`, `index.md` landing pages, and running `pnpm dev` from `website/`.
2. **Add or move a page.** Create the file, add it to `src/lib/config/nav.ts`, add its card to the section's landing page (see [Landing cards](#landing-cards-b4-writes-b1-styles)), and when it replaces a Hugo page, add or update its `redirects.json` entry. Explain `rewrittenFrom`: the last commit that changed the Hugo page when the new page was checked against it, from `git log -1 --abbrev=8 --format=%h HEAD -- site/content/en/docs/<old>`, and `null` while the page is a draft. Mention `make docs-drift`.
3. **Front matter.** Every key from `src/lib/markdown/frontmatter.ts` with what it does, as in the spec's Front matter section.
4. **Style.** The rules from the spec's Style section: guide titles start with a verb, concept titles are nouns, reference pages are named after the thing, all in sentence case; second person, present tense, active voice; the guide structure (Before you begin, numbered steps that each end with a check, Troubleshooting, Clean up, Next steps); complete, runnable examples with concrete names; commands and output in separate blocks; select columns when default output is too wide. Link to B4's pages as the models: Serve models from a PVC for guides, Base models for concepts, and `kubectl ome rollout` for command reference.
5. **Accuracy.** Check every field, default, flag and output against the code at the PR's base commit, not the old page. CLI output comes from the golden files in `pkg/cli/**/testdata` where they exist. Behavior that isn't in the latest release gets `since`.
6. **Syntax.** One subsection each for callouts (the four types and when to use each), details, tabs, prerequisites, card grids (the landing-card format, whose title and text must match the linked page's front matter), code blocks (languages, `title`, `output`, `check=skip` and why it needs a reason in the text), headings (`{#id}`, `{since=v1.3}` and the combination), links (relative `.md` paths with anchors; absolute paths and links to the old site fail the build), and images (`website/static/images/`, alt text required). Show each as its source in a fence and, where it helps, rendered below it. Show `{since=…}` and `{#…}` only inside code fences, because a heading in this page with the attribute would render a real badge.
7. **Versioning and status.** The three since labels ("Unreleased: coming in v1.3", "New in v1.3", and "Since v1.3" when the latest release can't be fetched), and that the label changes at request time when a release ships. A table row or a sentence can't carry a badge, so write "Since v1.3." in plain text there. `status: preview` is for the five pages that are purely about multi-cluster routing; an alpha subcommand on any other page gets `!!! note "Alpha"` under its heading, with the text "This command is alpha. Its flags and behavior can change between releases."
8. **Checks.** `pnpm lint`, `pnpm check`, `pnpm test` (which includes the content checks) and `pnpm build` from `website/`; `make docs-examples` for the YAML check; `make docs-drift`; and the pre-commit hooks.
9. **The API reference.** It's generated by `make generate-apiref` from the Go types; edit the doc comments in `pkg/apis/ome/v1beta1/`, not the page.

Write it in the style it describes. Run the four website checks; the content checks will catch broken links and syntax errors. Then open the page in the browser and check each rendered example.

- [ ] **Step 14: Commit**

```bash
git add website/src/lib/content/contributing/writing-docs.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Writing docs page" -m "The docs style guide and the authoring syntax, written in the style
it describes."
```

- [ ] **Step 15: Final checks and report**

From `website/`, run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build`. From the worktree root, run `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/site-redesign...HEAD)`. Report per Ground rule 8, and include the screenshots' paths, the test totals, and any mockup detail you couldn't match.

### Task B2: The home page

**Branch:** `docs/website-home`

**Goal:** `/ome/` matches the approved home page mockup: SMG's home page, section for section, with OME's copy, the orbit mark in the hero and the ambient shader recolored to cobalt.

**Files:**

- Copy from SMG, then modify: `website/src/lib/actions/scrollReveal.ts`, `website/src/lib/components/{HomeHero,HeroShaderBackground,HomeMetrics,HomeMetricValue,HomeWorksWith,HomeDetailPanel,HomeWhy,HomeHowItWorks,HomeFlowMark,HomeHowBranch,HomeHowLink,HomeChoosePath,SectionLabel,PlusMark,ChoosePathArrow}.svelte`
- Create: `website/src/lib/components/HeroMark.svelte`
- Modify: `website/src/routes/+page.svelte`, `website/src/styles/home.css`, `website/src/lib/components/SiteFooter.svelte` (the brand link only)

Never copy SMG's `ShiftMark`, `Logo`, `Symbol`, `FooterMark` or `HeroMark` shapes, or its `static/images/` logos. Don't edit `app.css`; B1 owns its header rules in Phase B.

**How it works:** `+page.server.ts` (Task A1) already loads the hero title and subtitle from D1, falling back to `homeDefaults` in `src/lib/config/home.ts`, and returns them as `data.content['home.hero.title']` and `data.content['home.hero.subtitle']`. `home.css` is SMG's, recolored by A1, so most components drop in with their SMG class names. The work is the copy, the three pieces that carried SMG's brand (the hero mark, the section label mark and the logos), and the How it works diagram.

Design references:

- The mockup `/Users/simolin/go/src/github.com/ome-projects/ome/.superpowers/brainstorm/68202-1790451826/content/home-page-v2.html`. Like the doc mockup, it's drawn smaller than real size inside a 980-pixel frame, so keep SMG's type scale from `home.css` and take colors, structure and copy from the mockup.
- SMG's home page: `/tmp/smg-docs-ref/src/routes/+page.svelte` and the components above.

- [ ] **Step 1: Copy the SMG components**

Run from the worktree root:

```bash
S=/tmp/smg-docs-ref/src/lib
W=website/src/lib
mkdir -p $W/actions
cp $S/actions/scrollReveal.ts $W/actions/
cp $S/components/{HomeHero,HeroShaderBackground,HomeMetrics,HomeMetricValue,HomeWorksWith,HomeDetailPanel,HomeWhy,HomeHowItWorks,HomeFlowMark,HomeHowBranch,HomeHowLink,HomeChoosePath,SectionLabel,PlusMark,ChoosePathArrow}.svelte $W/components/
grep -ln 'ShiftMark\|smg\|SMG\|Shepherd' $W/components/*.svelte $W/actions/*.ts
```

The last command lists the files that still name SMG. Each one changes in the next steps; when you're done, it must print nothing.

- [ ] **Step 2: Replace the brand pieces**

**`HeroMark.svelte`** (new file). Keep SMG's props (`class`, `width`, `active`), its three glow filters and its `<style>` behavior (idle, hover and active glows, the color transition, reduced-motion handling), but draw the orbit mark: a `viewBox="0 0 40 40"`, `height` equal to `width`, and in `currentColor` a filled core (`cx=20 cy=20 r=8`), a ring (`r=14`, no fill, `stroke-width=2`, opacity 0.8) and a satellite (`cx=31 cy=9.5 r=3`). Rename the filter ids to `ome-glow-idle`, `ome-glow-hover` and `ome-glow-active`, resize their `userSpaceOnUse` regions to the 40×40 box with the same margins in proportion, change the warm active glow `#ffc878` to `#9fb4ff`, and make the active color `var(--ome-accent)`. Drop SMG's clip path, which only fits its own shape.

**`SectionLabel.svelte`**: replace `ShiftMark` with `<span class="section-label-mark" aria-hidden="true"></span>`, a dot `markSize` pixels wide (default 6) in `currentColor`. Add its rule to `home.css`. On the dark Choose your path panel the label is `#9fb4ff`, as in the mockup.

**`HomeHero.svelte`**: the GitHub button links to `repoUrl` from `$lib/config/site`, and Get Started to `` `${base}/getting-started` ``. `<HeroMark width={48} …>` stays. Adjust `.home-hero-mark` in `home.css` for a square mark where it assumed SMG's tall one.

**`HeroShaderBackground.svelte`**: in the fragment shader, rename `orange` to `accent` and set it to `vec3(0.141, 0.278, 0.722)` (`#2447b8`), and set `mid` to `vec3(0.839, 0.855, 0.890)` (`#d6dae3`). Change nothing else in the shader.

**`SiteFooter.svelte`**: change the brand link's `href={base || '/'}` to `` href="{base}/" ``; `/ome` redirects with a 308.

- [ ] **Step 3: Write the copy**

**`+page.svelte`** composes the sections in SMG's order:

```svelte
<script lang="ts">
	import HomeChoosePath from '$lib/components/HomeChoosePath.svelte';
	import HomeDetailPanel from '$lib/components/HomeDetailPanel.svelte';
	import HomeHero from '$lib/components/HomeHero.svelte';
	import HomeMetrics from '$lib/components/HomeMetrics.svelte';
	import HomeWorksWith from '$lib/components/HomeWorksWith.svelte';

	let { data } = $props();
</script>

<HomeHero title={data.content['home.hero.title']} subtitle={data.content['home.hero.subtitle']} />
<HomeMetrics />
<HomeWorksWith />
<HomeDetailPanel />
<HomeChoosePath />
```

**`HomeMetrics.svelte`**, with `aria-label="OME at a glance"`:

| `title` | `prefix` | `value` | `suffix` | `description` |
|---|---|---|---|---|
| `PRE-CONFIGURED MODELS` | | 200 | `+` | Llama, Qwen, DeepSeek, Gemma, and more |
| `RUNTIME CONFIGS` | | 200 | `+` | Tuned for SGLang, vLLM, and TokenSpeed |
| `DEPLOYMENT MODES` | | 3 | | Deployment, LeaderWorkerSet, or OMENative |
| `KUBECTL OME` | | 17 | | Commands to inspect, explain, and act |

These are fixed in code on purpose (see the spec's Home Page section). Before committing, check they still hold: `config/models` has at least 200 model files, `config/runtimes` at least 200 runtime files, and `pkg/cli/root.go` adds 17 top-level commands. Report the counts you found.

**`HomeWorksWith.svelte`**: replace the logo images with name pills. Keep SMG's section label ("Works with") and layout, and render two rows of `a.home-works-item` links (new tab, `rel="noopener noreferrer"`), each a pill with the name and, below it, the caption:

| Row | Pill | Caption | Link |
|---|---|---|---|
| 1 | SGLang | SGLang | https://github.com/sgl-project/sglang |
| 1 | vLLM | vLLM | https://vllm.ai |
| 1 | TokenSpeed | TokenSpeed | https://github.com/lightseekorg/tokenspeed |
| 1 | SMG | SMG router | https://lightseek.org/smg |
| 2 | Hugging Face | Hugging Face | https://huggingface.co |
| 2 | Kueue | Kueue | https://kueue.sigs.k8s.io |
| 2 | LWS | LeaderWorkerSet | https://lws.sigs.k8s.io |
| 2 | KEDA | KEDA | https://keda.sh |
| 2 | Gateway API | Gateway API | https://gateway-api.sigs.k8s.io |

Pill styles from the mockup, in `home.css`: a fixed width (7rem at desktop, wrapping on small screens), height 2.625rem, `border: 1px solid` ink, radius 999px, weight 700. Row 2 pills (`--eco`) use border `#b9bfcc`, color `#3d4250` and weight 600. Hover turns the border and text cobalt. Delete the logo image rules SMG's CSS had.

**`HomeWhy.svelte`**: the section label "Why OME?", this paragraph:

> OME turns models and GPUs into production endpoints on Kubernetes. You describe what to serve. OME chooses the runtime, generates the workloads, and keeps them healthy through every change.

and five expandable items, the first open, as SMG's are:

1. **Models as Kubernetes resources.** "BaseModel and ClusterBaseModel describe what to serve. The model agent downloads weights to your nodes and reads the architecture, parameter count, and capabilities from the files."
2. **Automatic runtime selection.**
3. **GPU-aware placement.**
4. **Any serving topology.**
5. **Safe rollouts.**

Write the bodies of items 2 to 5 at the length of item 1, and check every claim against the code: runtime scoring and explicit runtimes in `pkg/runtimeselector/`; accelerator classes and the BestFit, Cheapest and MostCapable policies in `pkg/acceleratorclassselector/`; single-node Deployments, multi-node LeaderWorkerSets, OMENative and prefill-decode disaggregation in `pkg/controller/v1beta1/inferenceservice/`; revisions, canary and blue-green rollouts and `kubectl ome rollout` in the rollout controllers and `pkg/cli/`. Don't mention multi-cluster; it's alpha. Report each body with the files you checked it against.

**`HomeHowItWorks.svelte`**: keep SMG's diagram, connectors (`HomeHowLink`, `HomeHowBranch`, `HomeFlowMark`) and expand behavior, with these nodes:

| SMG node | OME node | Contents |
|---|---|---|
| Clients | You declare | InferenceService, BaseModel, ServingRuntime, AcceleratorClass |
| Gateway stack | Selection, headed "Match" | Runtime scoring, Runtime inheritance, Accelerator policy, Model version matching, Revision pinning |
| Router stack | Reconciliation, headed "Controller" | Workload generation, Canary and blue-green rollouts, Autoscaling, Gang scheduling, Gateway routes |
| Three outputs | Deployment; LeaderWorkerSet; OMENative | "default mode · HPA or KEDA"; "multi-node"; "OME-managed pods" |
| Tagline | | Any model • The right runtime • Safe rollouts |

Where SMG's diagram links a node to a docs page, link to the matching new page under `` `${base}/concepts/…` ``; find the paths in `src/lib/config/nav.ts`. Every link must resolve to a page that exists.

**`HomeChoosePath.svelte`**: the section label "Choose your path", this intro:

> Start with what you need today, whether that's a first model on a test cluster or a fleet you already run.

and four cards:

| Card text | Button | Link |
|---|---|---|
| Install OME and serve your first model. | New to OME | `${base}/getting-started` |
| Deploy models, set up networking, roll out changes, and run OME itself. | Guides | `${base}/guides` |
| How models, runtimes, and InferenceServices fit together. | Concepts | `${base}/concepts` |
| The OME API, kubectl ome commands, and matching rules. | Reference | `${base}/reference` |

SiteHeader detects this section by its `home-choose-path` class to switch the header to its dark style, so keep that class on the section.

- [ ] **Step 4: Check the page**

Run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build` from `website/`, and fix every failure. Then start the dev server on port 5175 (see [Servers during Phase B](#servers-during-phase-b)):

```bash
B=http://localhost:5175/ome
curl -s -o /dev/null -w '%{http_code}\n' $B/
curl -s $B/ | grep -o 'The Kubernetes operator for serving LLMs in production\|OME at a glance\|Why OME?\|How it works\|Choose your path\|Any model' | sort | uniq -c
curl -s $B/ | grep -o 'href="/ome/[^"#]*"' | sort -u | sed 's/href="\(.*\)"/\1/' | while read -r path; do printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:5175$path")" "$path"; done
grep -rn 'smg\|SMG\|ShiftMark\|#ffc878\|0\.702, 0\.353' website/src/lib/components/{Home*,Hero*,SectionLabel,PlusMark,ChoosePathArrow}.svelte website/src/lib/actions website/src/styles/home.css | grep -v 'SMG router\|lightseek.org/smg\|>SMG<'
pkill -f 'port 5175'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"
```

Expected: `200`; each phrase at least once; every internal link `200`; and the last `grep` printing nothing (the only SMG left is the router in Works with).

In the browser: screenshot the page at 1280 and 390 pixels wide and compare it with the mockup section by section. Click the hero mark and check that the shader turns on in cobalt and the mark glows; click it again to turn it off. Expand and collapse the Why items and the How it works diagram. Scroll to Choose your path and check that the header turns dark over it and back when you scroll up. With `prefers-reduced-motion: reduce` emulated, check that nothing animates on scroll.

- [ ] **Step 5: Commit**

```bash
git add website/src
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Build the home page" -m "SMG's home page, section for section, with OME's copy, the orbit
mark in the hero, a cobalt shader and name pills for the projects
OME works with."
```

- [ ] **Step 6: Report**

Run `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/site-redesign...HEAD)` from the worktree root. Report per Ground rule 8, and include the counts behind the metrics, the Why OME bodies with the files you checked them against, and the screenshots' paths.

### Task B3: The generated API reference

**Branch:** `docs/website-apiref`

**Goal:** `make generate-apiref` also writes `website/src/lib/content/reference/api/ome.v1beta1.md` from the Go types, in the [API reference markup](#api-reference-markup-b3-emits-b1-styles). The page covers all 14 CRD kinds and every type they use, keeps placeholder text such as `spec.<component>`, and shows a Required badge exactly where the CRD schemas require a field.

**Files:**

- Create: `hack/genref/website/markdown/pkg.tpl`, `hack/genref/website/markdown/type.tpl`, `hack/genref/website/markdown/members.tpl`
- Modify: `hack/genref/config.yaml`, `Makefile` (the `generate-apiref` recipe), `pkg/apis/ome/v1beta1/inference_service_status.go` (one comment)
- Generate: `website/src/lib/content/reference/api/ome.v1beta1.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the `reference/ome.v1beta1.md` entry, nothing else)

Don't commit anything under `site/`. The target still regenerates the Hugo page, whose committed copy is stale, and the Hugo site isn't part of this work.

**How it works:** genref ([kubernetes-sigs/reference-docs](https://github.com/kubernetes-sigs/reference-docs), v0.28.0) loads the packages listed in `config.yaml`, executes the template named `packages` from `markdown/*.tpl` in the current directory, and writes `ome.v1beta1.md` into the `-o` directory. The Hugo page comes from the stock templates in `hack/genref/markdown/`. The website's templates live in `hack/genref/website/markdown/` and read `../config.yaml`, so external links are configured once for both.

The stock templates lean on genref helpers that lose content. The templates in Step 2 work around each one; they were run against the current API while this plan was written:

| genref behavior | With the stock templates | What the new templates do |
|---|---|---|
| `GetComment` renders comments with goldmark, which treats `<component>` as raw HTML | Placeholders such as `spec.<component>` become `<!-- raw HTML omitted -->`, 28 times on the Hugo page | Print `.CommentLines` minus marker lines, HTML-escaped, except lines with a code span, where escaping would show entities literally and marked escapes the text itself |
| `IsOptional` looks only for `+optional` | About 51 fields get the wrong Required badge | Apply controller-gen's rule: required when marked `+required`, or when the field is none of `+optional`, inline and `omitempty` |
| The stock `pkg.tpl` renders only the types a kind references | Unreferenced types are missing | Render every visible type, and hide the `*List` wrappers with `hideTypePatterns` |
| `IsExported` means the type has a `+genclient` marker | `RolloutPolicy`, a CRD kind without `+genclient`, is missing | Place `RolloutPolicy` among the kinds by hand |
| `.Link` for a map or slice of pointers to a local type is an anchor that doesn't exist | Links that go nowhere | Link to the element type |
| `DisplayName` is the full package path of an external type | `k8s.io/apimachinery/pkg/apis/meta/v1.Time` | Print the bare name, such as `Time` or `[]Container`, still linked upstream |

Templates get only text/template's builtins, such as `html`, `printf`, `slice`, `index`, `len`, `eq`, `and` and `or`: there's no sprig and no arithmetic. `range $i := len $s` (Go 1.22) walks a string's byte indexes, and `printf "%.5s" (slice $s $i)` reads ahead without arithmetic. genref logs template errors and can still exit 0, so check the output file, not the exit code.

- [ ] **Step 1: Configure genref**

In `hack/genref/config.yaml`, add this block after `hiddenMemberFields`, separated by a blank line:

```yaml
# List types are only containers for API responses.
hideTypePatterns:
  - "^sigs\\.k8s\\.io/ome/pkg/apis/ome/v1beta1\\.\\w+List$"
```

and add these two entries to `externalPackages`, before the `RawExtension` entry:

```yaml
  - match: ^k8s\.io/apimachinery/pkg/util/intstr\.IntOrString$
    target: https://pkg.go.dev/k8s.io/apimachinery/pkg/util/intstr#IntOrString
  - match: ^k8s\.io/apimachinery/pkg/types\.UID$
    target: https://pkg.go.dev/k8s.io/apimachinery/pkg/types#UID
```

Without them genref logs "External link source not found" for both types.

- [ ] **Step 2: Add the templates**

Create the three files with exactly this content. `pkg.tpl` holds the front matter, the intro, the kinds and the supporting types:

```text
{{- define "packages" -}}
{{- range .packages -}}
{{- if ne .GroupName "" -}}
---
# Generated by `make generate-apiref` from the Go types in pkg/apis/ome/v1beta1.
# Don't edit this file. Edit the doc comments on the types and run the target.
title: OME API
description: Look up the fields of every type in the ome.io/v1beta1 API, such as InferenceService, BaseModel and ClusterServingRuntime, in this generated reference.
generated: true
---

Every resource on this page has `apiVersion: {{ .DisplayName }}`. The page is generated from the Go types in [`pkg/apis/ome/v1beta1`](https://github.com/ome-projects/ome/tree/main/pkg/apis/ome/v1beta1), the same types the CRDs are generated from. Fields marked **Required** must be set; every other field is optional.
{{- /*
	genref treats a type as a resource only when it has a +genclient marker.
	RolloutPolicy is a CRD kind without one, so it's placed among the kinds
	in alphabetical order by hand.
*/}}
{{- $rolloutPolicy := false }}
{{- range .VisibleTypes }}
{{- if and (not .IsExported) (eq .Name.Name "RolloutPolicy") }}{{ $rolloutPolicy = . }}{{ end }}
{{- end }}
{{- range .VisibleTypes }}
{{- if .IsExported }}
{{- if and $rolloutPolicy (lt $rolloutPolicy.Name.Name .Name.Name) }}
{{ template "kind" $rolloutPolicy }}
{{- $rolloutPolicy = false }}
{{- end }}
{{ template "kind" . }}
{{- end }}
{{- end }}
{{- if $rolloutPolicy }}
{{ template "kind" $rolloutPolicy }}
{{- end }}

## Supporting types

The resources above use these types in their fields.
{{- range .VisibleTypes }}
{{- if and (not .IsExported) (ne .Name.Name "RolloutPolicy") }}
{{ template "type" . }}
{{- end }}
{{- end }}
{{ end -}}
{{- end -}}
{{- end -}}
```

`type.tpl` renders a kind (with the `apiVersion`, `kind` and `metadata` rows every resource has) and a supporting type (with its "Used by" line, underlying type, default and allowed values), and holds the comment and marker helpers:

```text
{{- define "kind" }}
## `{{ .Name.Name }}` {#{{ .Anchor }}}
{{ template "comment" .CommentLines }}

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>apiVersion</code> <span class="doc-api-required">Required</span></td>
<td><code>string</code></td>
<td>

`{{ .APIGroup }}`

</td></tr>
<tr><td><code>kind</code> <span class="doc-api-required">Required</span></td>
<td><code>string</code></td>
<td>

`{{ .Name.Name }}`

</td></tr>
<tr><td><code>metadata</code></td>
<td><a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta"><code>ObjectMeta</code></a></td>
<td>

Standard object metadata, such as `name` and `labels`.

</td></tr>
{{- template "members" . }}
</tbody>
</table>
{{- end }}

{{- define "type" }}
### `{{ .Name.Name }}` {#{{ .Anchor }}}
{{- with .References }}

Used by {{ range $i, $ref := . }}{{ if $i }}, {{ end }}[`{{ $ref.DisplayName }}`]({{ $ref.Link }}){{ end }}.
{{- end }}
{{ template "comment" .CommentLines }}
{{- if eq .Kind "Alias" }}

Underlying type: `{{ .Underlying }}`.
{{- end }}
{{- template "markers" .CommentLines }}
{{- if .GetMembers }}

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
{{- template "members" . }}
</tbody>
</table>
{{- end }}
{{- end }}

{{- /*
	comment prints doc comment lines as Markdown without the +marker lines.
	Lines are HTML-escaped so placeholders such as spec.<component> stay
	visible, except lines with a code span: entities inside backticks would
	show literally, and marked escapes code spans itself.
*/}}
{{- define "comment" }}
{{- range . }}
{{- if or (eq . "") (ne (slice . 0 1) "+") }}
{{- $line := . }}
{{- $code := false }}
{{- range $i := len $line }}{{ if eq (index $line $i) 96 }}{{ $code = true }}{{ end }}{{ end }}
{{ if $code }}{{ $line }}{{ else }}{{ html $line }}{{ end }}
{{- end }}
{{- end }}
{{- end }}

{{- /* markers prints the defaults and allowed values that kubebuilder markers set. */}}
{{- define "markers" }}
{{- range . }}
{{- if and (ge (len .) 21) (eq (slice . 0 21) "+kubebuilder:default=") }}

Default: `{{ slice . 21 }}`.
{{- end }}
{{- if and (ge (len .) 29) (eq (slice . 0 29) "+kubebuilder:validation:Enum=") }}
{{- $values := slice . 29 }}

Allowed values: `{{ range $i := len $values }}{{ if eq (index $values $i) 59 }}`, `{{ else }}{{ printf "%c" (index $values $i) }}{{ end }}{{ end }}`.
{{- end }}
{{- end }}
{{- end }}
```

`members.tpl` renders the field rows, the Required rule and the type links:

```text
{{- /*
	members prints a table row per field. A field is required on the same
	terms controller-gen uses for the CRD schema: it's marked +required, or
	it's none of +optional, inline and omitempty.
*/}}
{{- define "members" }}
{{- range .GetMembers }}
{{- if not .Hidden }}
{{- $omitempty := printf "json:\"%s,omitempty\"" .FieldName }}
{{- $required := not (or .IsOptional .IsInline (and (ge (len .Tags) (len $omitempty)) (eq (slice .Tags 0 (len $omitempty)) $omitempty))) }}
{{- range .CommentLines }}{{ if eq . "+required" }}{{ $required = true }}{{ end }}{{ end }}
{{- if .IsInline }}
<tr><td><em>Embedded</em></td>
<td>{{ template "typelink" .GetType }}</td>
<td>
{{ template "comment" .CommentLines }}

The fields of `{{ template "shortname" .GetType }}` appear directly in this object.
{{- else }}
<tr><td><code>{{ .FieldName }}</code>{{ if $required }} <span class="doc-api-required">Required</span>{{ end }}</td>
<td>{{ template "typelink" .GetType }}</td>
<td>
{{ template "comment" .CommentLines }}
{{- template "markers" .CommentLines }}
{{- end }}

</td></tr>
{{- end }}
{{- end }}
{{- end }}

{{- /*
	typelink links a field's type. genref builds a broken anchor for a map
	or slice of pointers to a local type, so those link to the element type.
*/}}
{{- define "typelink" -}}
{{- $link := .Link }}
{{- if and .Elem (eq .Elem.Kind "Pointer") (eq .Elem.Elem.Name.Package "sigs.k8s.io/ome/pkg/apis/ome/v1beta1") }}
{{- $link = printf "#ome-io-v1beta1-%s" .Elem.Elem.Name.Name }}
{{- end }}
{{- if $link }}<a href="{{ $link }}"><code>{{ template "shortname" . }}</code></a>{{ else }}<code>{{ template "shortname" . }}</code>{{ end -}}
{{- end }}

{{- /* shortname is the type's name without its package path, such as []Container. */}}
{{- define "shortname" -}}
{{- if eq .Kind "Pointer" }}{{ template "shortname" .Elem }}
{{- else if eq .Kind "Slice" }}[]{{ template "shortname" .Elem }}
{{- else if eq .Kind "Map" }}map[{{ .Key.Name.Name }}]{{ template "shortname" .Elem }}
{{- else }}{{ .Name.Name }}{{ end -}}
{{- end }}
```

- [ ] **Step 3: Fix the status comment**

Embedded fields print their comments now, and the comment on the `duckv1.Status` embedded in `InferenceServiceStatus` breaks lines with `<br/>`, which the page would show as text. In `pkg/apis/ome/v1beta1/inference_service_status.go`, replace:

```go
	// Conditions for the InferenceService <br/>
	// - EngineReady: engine readiness condition; <br/>
	// - DecoderReady: decoder readiness condition; <br/>
	// - RouterReady: router readiness condition; <br/>
	// - IngressReady: ingress resource readiness; <br/>
	// - Ready: component aggregate on workload clusters, or placement serving
	//   readiness on a multi-cluster control-plane source; <br/>
```

with:

```go
	// Conditions for the InferenceService:
	//
	// - EngineReady: engine readiness condition.
	// - DecoderReady: decoder readiness condition.
	// - RouterReady: router readiness condition.
	// - IngressReady: ingress resource readiness.
	// - Ready: component aggregate on workload clusters, or placement serving
	//   readiness on a multi-cluster control-plane source.
```

`CRD_OPTIONS` is `crd:maxDescLen=0`, so the CRDs carry no descriptions, and the deepcopy code carries no comments. Confirm that nothing else changes. Your worktree has no `bin/`, so point make at the main checkout's tools:

```bash
M=/Users/simolin/go/src/github.com/ome-projects/ome
gofmt -l pkg/apis/ome/v1beta1
go vet ./pkg/apis/ome/v1beta1
make -o controller-gen -o yq manifests CONTROLLER_GEN=$M/bin/controller-gen YQ=$M/bin/yq > /tmp/b3-manifests.log 2>&1; echo "exit $?"
git status --short
```

Expected: no `gofmt` output, `go vet` passes, `exit 0`, and `git status` lists only your own changes. Don't run `make generate` in the sandbox: it fails on the Go module cache and leaves `go.sum` modified. If `go.sum` changes anyway, restore it with `git checkout -- go.sum`.

- [ ] **Step 4: Wire the Makefile**

In the `generate-apiref` recipe, add the second genref line after the Hugo one:

```make
generate-apiref: genref ## 📚 Generate API reference documentation
	@echo "📚 Generating API reference documentation..."
	@cd $(PROJECT_DIR)/hack/genref/ && $(GENREF) -o $(PROJECT_DIR)/site/content/en/docs/reference
	@cd $(PROJECT_DIR)/hack/genref/website && $(GENREF) -c ../config.yaml -o $(PROJECT_DIR)/website/src/lib/content/reference/api
	@echo "✅ API reference documentation generated"
```

- [ ] **Step 5: Generate the page**

```bash
make -o genref generate-apiref GENREF=/tmp/ome-tools/gobin/genref
git checkout -- site/
git status --short
head -7 website/src/lib/content/reference/api/ome.v1beta1.md
```

Expected: `git status` lists the page and your edits from Steps 1 to 4, with `hack/genref/website/` untracked, and nothing under `site/`. The page starts with the front matter from `pkg.tpl`: `generated: true` and no `status`. `-o genref` skips reinstalling genref, which needs the network; if `/tmp/ome-tools/gobin/genref` is missing, drop `-o genref` and the `GENREF` override.

- [ ] **Step 6: Check the page**

```bash
P=website/src/lib/content/reference/api/ome.v1beta1.md
diff <(grep -o '^## `[A-Za-z]*`' $P | tr -d '#` ') <(grep -h -A4 '^  names:' config/crd/full/ome.io_*.yaml | grep '^    kind:' | awk '{print $2}' | sort) && echo kinds match
grep -c '^### `' $P
grep -c 'raw HTML omitted\|<no value>\|sigs-k8s-io\|<code>sigs\.k8s\.io' $P
grep -c 'spec\.&lt;component&gt;' $P
wc -c < $P
```

Expected: `kinds match`, meaning the page's 14 kind headings, `RolloutPolicy` among them, are exactly the kinds in `config/crd/full`; `236` supporting types; `0`; `6`; and fewer than 500000 bytes, the pre-commit file size limit (about 387,000 when this plan was written). Counts that differ because the API changed after this plan are fine; say so in your report.

Then check the Required badges against the CRD schemas. Save this script outside the repo, as `/tmp/b3-check-required.py`:

```python
"""Compares the page's Required badges with the CRD schemas' required lists.

Walks each kind's schema in config/crd/full alongside the page's tables,
following type links, and prints every object whose Required fields differ.
It skips objects at InferenceService spec.<field>.<field>, where the
manifests target deletes the required lists, and required fields that come
from an embedded external type such as Container.
Usage: python3 check-required.py <page> <crd dir>
"""
import glob
import re
import sys

import yaml

page = open(sys.argv[1]).read()
types = {}
for m in re.finditer(r'^#{2,3} `(\w+)` \{#([\w-]+)\}\n(.*?)(?=^#{2,3} |\Z)', page, re.S | re.M):
    name, anchor, body = m.groups()
    rows = {}
    for r in re.finditer(
        r'<tr><td><code>(\w+)</code>( <span class="doc-api-required">Required</span>)?</td>\n'
        r'<td>(.*?)</td>', body):
        field, required, cell = r.groups()
        link = re.search(r'href="#([\w-]+)"', cell)
        rows[field] = (bool(required), link.group(1) if link else None)
    embedded = re.findall(r'<tr><td><em>Embedded</em></td>\n<td><a href="#([\w-]+)">', body)
    types[anchor] = (name, rows, embedded)


def fields(anchor):
    name, rows, embedded = types[anchor]
    merged = dict(rows)
    for e in embedded:
        if e in types:
            merged.update(fields(e))
    return merged


def element(schema):
    while True:
        if schema.get('type') == 'array' and 'items' in schema:
            schema = schema['items']
        elif isinstance(schema.get('additionalProperties'), dict) and 'properties' not in schema:
            schema = schema['additionalProperties']
        else:
            return schema


checked, problems = set(), []


def walk(anchor, schema, where):
    if anchor not in types:
        return
    rows = fields(anchor)
    props = schema.get('properties', {})
    if not re.fullmatch(r'InferenceService\.spec\.\w+\.\w+', where):
        if anchor in checked:
            return
        checked.add(anchor)
        have = {f for f, (req, _) in rows.items() if req and f in props}
        if '.' not in where:
            have -= {'apiVersion', 'kind'}
        want = set(schema.get('required', [])) & set(rows)
        if have != want:
            problems.append(f'{types[anchor][0]} ({where}): page {sorted(have)}, CRD {sorted(want)}')
    for f, (_, link) in rows.items():
        if link and f in props:
            walk(link, element(props[f]), f'{where}.{f}')


kinds = 0
for path in sorted(glob.glob(f'{sys.argv[2]}/*.yaml')):
    for doc in yaml.safe_load_all(open(path)):
        if not doc or doc.get('kind') != 'CustomResourceDefinition':
            continue
        kind = doc['spec']['names']['kind']
        anchor = f'ome-io-v1beta1-{kind}'
        if anchor not in types:
            problems.append(f'{kind}: no section on the page')
            continue
        kinds += 1
        schema = doc['spec']['versions'][0]['schema']['openAPIV3Schema']
        walk(anchor, schema, kind)

print(f'{kinds} kinds, {len(checked)} types checked, {len(problems)} mismatches')
for p in problems:
    print(p)
```

```bash
python3 /tmp/b3-check-required.py website/src/lib/content/reference/api/ome.v1beta1.md config/crd/full
```

Expected: `14 kinds, 226 types checked, 0 mismatches`. The script skips InferenceService objects at `spec.<field>.<field>` because the `manifests` target deletes their `required` lists, so on the page those follow the Go types: `consistentHash.type` and `endpointOverride.type` are Required though the API server doesn't enforce them. Any mismatch the script prints is a template bug; fix the template, not the page.

- [ ] **Step 7: Set `rewrittenFrom`**

```bash
git log -1 --abbrev=8 --format=%h HEAD -- site/content/en/docs/reference/ome.v1beta1.md
```

Expected: `2656a655`. In `website/redirects.json`, set `"rewrittenFrom": "2656a655"` in the entry whose `old` is `reference/ome.v1beta1.md`. `pnpm test` fails until you do, because the page is no longer a draft.

- [ ] **Step 8: Check the site**

Run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build` from `website/`, and fix every failure. The content checks follow every `href="#…"` in the tables, so a broken type link fails here. Then start the dev server on port 5176 (see [Servers during Phase B](#servers-during-phase-b)):

```bash
B=http://localhost:5176/ome
P=$B/reference/api/ome.v1beta1
curl -s -o /dev/null -w '%{http_code}\n' $P
curl -s $P | grep -o 'id="ome-io-v1beta1-' | wc -l
curl -s $P | grep -o '<table class="doc-api-fields">' | wc -l
curl -s $P | grep -o 'class="doc-api-required"' | wc -l
curl -s $P | grep -o 'spec\.&lt;component&gt;\|&amp;lt;' | sort | uniq -c
curl -s $B/search.json | wc -c
pkill -f 'port 5176'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"
```

Expected: `200`; `250` anchors (14 kinds and 236 types); `193` tables, since aliases and types without fields have none; `199` Required badges; `spec.&lt;component&gt;` 12 times (6 in the article and 6 in the page data SvelteKit inlines for hydration) and no `&amp;lt;`, which would mean double escaping; and a search index of about 273,000 bytes, up from about 35,000 before the page was written. Report the size. The tables render unstyled on your branch; B1 styles them and limits this page's TOC to the `h2` headings.

- [ ] **Step 9: Commit**

```bash
git add hack/genref Makefile pkg/apis/ome/v1beta1/inference_service_status.go website/src/lib/content/reference/api/ome.v1beta1.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Generate the API reference page" -m "genref now also writes the website's API reference with its own
templates. They keep placeholders such as spec.<component>, mark
fields Required on the same terms as the CRD schemas, list every
kind and type, and show external types by their short names.

The InferenceService status comment uses a Markdown list instead of
<br/> tags, which the page would show as text."
```

- [ ] **Step 10: Report**

Run `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/site-redesign...HEAD)` from the worktree root, and `git diff --exit-code go.mod go.sum`. Report per Ground rule 8, and include the kind and type counts, the check script's output, and the sizes of the page and the search index.

### Task B4: The voice-setting pages

**Branch:** `docs/website-voice`

**Goal:** Write the eight pages that set the voice for Phase D: the five section landing pages, the Serve models from a PVC guide, the Base models concept page and the `kubectl ome rollout` command reference. Every fact on them matches the code on your branch, and every example passes the YAML check.

**Files:**

- Rewrite: `website/src/lib/content/{getting-started,guides,concepts,reference,contributing}/index.md`
- Rewrite: `website/src/lib/content/guides/deploy-models/serve-models-from-pvc.md`, `website/src/lib/content/concepts/models/base-models.md`, `website/src/lib/content/reference/kubectl-ome/rollout.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 11 entries whose `new` is one of these pages, nothing else)

Keep each page's `title` and `description` as they are; the nav, the landing cards and search already use them. Remove `status: draft`. Don't edit any other page, even to fix a problem you notice; list it in your report instead.

**How it works:** Phase D's eleven batches copy the structure and voice of these pages, so they have to be right in both. The rules:

- **Voice**, from the spec's [Style](../specs/2026-09-26-docs-site-redesign-design.md#style) section. Second person, present tense, active voice: "OME mounts the claim read-only", not "The claim will be mounted". Guides follow one structure: Before you begin, numbered steps that each end with a check, Troubleshooting, Clean up, Next steps. Examples are complete and runnable, with concrete names instead of placeholders. Commands and their output go in separate blocks, `bash` for the command and `output` for what it prints; where the default output is too wide to read, select columns so the output block shows exactly what the reader sees. Use the four callout types, never `> **Note:**` blockquotes. Keep paragraphs short, lead with what the reader does or gets, and leave out filler such as "simply", "easily", "seamlessly" and "powerful". Link a term to its concept page the first time a page uses it.
- **Accuracy**, from the spec's [Accuracy](../specs/2026-09-26-docs-site-redesign-design.md#accuracy) section. The code on your branch is the truth. The Hugo pages under `site/content/en/docs/` are where the topics come from, not the facts; each page's known errors are listed in its step. Read the Go code behind every field, default, flag, state and message you write, and take CLI output only from golden files and test expectations.
- **Since.** v1.2.2, the latest release, is commit `5f1c4096`, and `git show 5f1c4096:<path>` shows a file as released. Behavior that isn't in v1.2.2 gets `since=v1.3` on its heading, or `since: v1.3` in the front matter when the whole page is new. A table row or a sentence can't carry a badge, so write "Since v1.3." in plain text there. Already checked: the `kubectl ome` CLI isn't in v1.2.2, so the rollout page gets `since: v1.3`, and neither are `spec.distribution` or the model status fields `backend`, `sourceUri` and `cache`. `pvc://` storage with its metadata Job and condition reasons, the Job's chart values under `ome.omeAgent.metadataJob`, and `local://` storage are all in v1.2.2.
- **YAML examples.** `go run ./hack/docs-examples` dry-run creates the objects in every `yaml` block under `website/src/lib/content` against the CRDs in envtest, and prints `Checked N objects: M problems.` Blocks without `apiVersion` or `kind`, such as Helm values, aren't checked. An object needs `metadata.name` or `metadata.generateName`, so mark a config file with an `apiVersion` and `kind`, such as a kubeconfig or a Kustomization, `check=skip`. The check runs no admission webhooks, so it misses the rules they enforce: check every BaseModel and ClusterBaseModel example against `ValidatePVCStorage` in `pkg/validation/basemodel.go` yourself, and any other kind against its webhook in `pkg/webhook/admission/`.
- **Commits.** One per page group, in Steps 2 to 5, each passing the checks in Ground rule 5.

B1 styles the pages in parallel, so on your branch they render unstyled. Check structure and content in the HTML, and use the browser only to catch Markdown that renders wrong.

- [ ] **Step 1: Save the tools**

Save these three scripts outside the repo. Run them from the worktree root.

`/tmp/b4-landing.py` prints a section's card grids from the nav and the pages' front matter, and checks all five landing pages. Phase D uses it too; see [Landing cards](#landing-cards-b4-writes-b1-styles).

````python
#!/usr/bin/env python3
"""Prints and checks the card grids on the five section landing pages.

Each page in a section's nav gets one card on the section's index.md, in
nav order. Cards for a labeled group sit under an h2 with the group's
label; cards for an unlabeled group come before any h2. A card is the
page's title, linked, then its description, both exactly as in the
page's front matter:

    -   **[Serve models from a PVC](deploy-models/serve-models-from-pvc.md)**

        Serve model weights that already live on a PersistentVolumeClaim ...

Run from the repository root:

    python3 landing.py cards SECTION   print SECTION's headings and card grids
    python3 landing.py check           check all five landing pages
"""

import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

CONTENT = Path('website/src/lib/content')
GRID_OPEN = '<div class="grid cards" markdown>'
CARD = re.compile(r'^-   \*\*\[(.+)\]\(([^)\s]+)\)\*\*\s*$')
H2 = re.compile(r'^## (.+?)(?:\s+\{[^}]*\})?\s*$')


def load_nav():
    script = "const m = await import('./website/src/lib/config/nav.ts'); console.log(JSON.stringify(m.nav));"
    out = subprocess.run(
        ['node', '--experimental-strip-types', '--input-type=module', '-e', script],
        check=True, capture_output=True, text=True,
    ).stdout
    return json.loads(out)


def front_matter(path):
    text = path.read_text()
    match = re.match(r'---\n(.*?)\n---\n', text, re.S)
    if not match:
        raise SystemExit(f'{path}: no front matter')
    return yaml.safe_load(match.group(1)), text[match.end():]


def expected(section):
    """Returns (heading, link, title, description) for each page, in nav order."""
    cards = []
    for group in section['groups']:
        for page in group['pages']:
            meta, _ = front_matter(CONTENT / section['id'] / page)
            cards.append((group['label'], page, meta['title'], meta['description']))
    return cards


def found(body):
    """Returns (heading, link, title, description) for each card in a landing page body."""
    cards, heading, lines = [], None, body.split('\n')
    fence, i = False, 0
    while i < len(lines):
        line = lines[i]
        if line.startswith('```'):
            fence = not fence
        elif not fence and (match := H2.match(line)):
            heading = match.group(1)
        elif not fence and line.strip() == GRID_OPEN:
            i += 1
            while i < len(lines) and lines[i].strip() != '</div>':
                if match := CARD.match(lines[i]):
                    text = []
                    i += 1
                    while i < len(lines) and not lines[i].startswith('-   ') and lines[i].strip() != '</div>':
                        if lines[i].strip() not in ('', '---'):
                            text.append(lines[i].strip())
                        i += 1
                    cards.append((heading, match.group(2), match.group(1), ' '.join(text)))
                    continue
                if lines[i].strip():
                    cards.append((heading, None, lines[i].strip(), None))
                i += 1
        i += 1
    return cards


def print_cards(nav, section_id):
    section = next((s for s in nav if s['id'] == section_id), None)
    if section is None:
        raise SystemExit(f'unknown section {section_id}; sections: {", ".join(s["id"] for s in nav)}')
    blocks = []
    for group in section['groups']:
        lines = []
        if group['label']:
            lines += [f'## {group["label"]}', '']
        lines += [GRID_OPEN, '']
        for page in group['pages']:
            meta, _ = front_matter(CONTENT / section_id / page)
            lines += [f'-   **[{meta["title"]}]({page})**', '', f'    {meta["description"]}', '']
        lines.append('</div>')
        blocks.append('\n'.join(lines))
    print('\n\n'.join(blocks))


def where(heading):
    return f'under "## {heading}"' if heading else 'before the first h2'


def check(nav):
    problems, total = [], 0
    for section in nav:
        index = CONTENT / section['id'] / 'index.md'
        _, body = front_matter(index)
        want, have = expected(section), found(body)
        total += len(want)
        for heading, link, line, _ in have:
            if link is None:
                problems.append(f'{index}: {where(heading)}, this line in a card grid isn\'t a card: {line}')
        have = [card for card in have if card[1] is not None]
        want_links = [card[1] for card in want]
        for card in have:
            if card[1] not in want_links:
                problems.append(f'{index}: a card links to {card[1]}, which isn\'t a page in this section\'s nav')
        complete = True
        for heading, link, title, description in want:
            matches = [card for card in have if card[1] == link]
            if len(matches) != 1:
                problems.append(f'{index}: {len(matches)} cards link to {link}; want 1')
                complete = False
                continue
            got = matches[0]
            if got[0] != heading:
                problems.append(f'{index}: the {link} card should be {where(heading)}, not {where(got[0])}')
            if got[2] != title:
                problems.append(f'{index}: the {link} card\'s title is "{got[2]}"; its page\'s title is "{title}"')
            if got[3] != description:
                problems.append(
                    f'{index}: the {link} card\'s text doesn\'t match its page\'s description:\n'
                    f'  card: {got[3]}\n  page: {description}'
                )
        if complete and [card[1] for card in have if card[1] in want_links] != want_links:
            problems.append(f'{index}: the cards aren\'t in nav order')
    for problem in problems:
        print(problem)
    noun = 'problem' if len(problems) == 1 else 'problems'
    print(f'{len(nav)} landing pages, {total} cards, {len(problems)} {noun}')
    return 1 if problems else 0


def main():
    args = sys.argv[1:]
    nav = load_nav()
    if args[:1] == ['cards'] and len(args) == 2:
        print_cards(nav, args[1])
        return 0
    if args == ['check']:
        return check(nav)
    print(__doc__)
    return 2


if __name__ == '__main__':
    sys.exit(main())
````

`/tmp/b4-rewritten.py` sets `rewrittenFrom` on the `redirects.json` entries of pages you've written:

```python
#!/usr/bin/env python3
"""Sets rewrittenFrom on the redirects.json entries of written pages.

Every entry whose "new" page is one of the arguments gets, as its
rewrittenFrom, the last commit that changed the entry's Hugo page. Run
from the repository root, with paths relative to website/src/lib/content:

    python3 rewritten.py guides/index.md concepts/models/base-models.md
"""

import json
import subprocess
import sys

REDIRECTS = 'website/redirects.json'


def last_commit(old):
    path = f'site/content/en/docs/{old}'
    rev = subprocess.run(
        ['git', 'log', '-1', '--abbrev=8', '--format=%h', 'HEAD', '--', path],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if not rev:
        sys.exit(f'no commit on HEAD changed {path}')
    return rev


def main(pages):
    if not pages:
        sys.exit(__doc__)
    with open(REDIRECTS) as f:
        entries = json.load(f)
    missing = sorted(set(pages) - {entry['new'] for entry in entries})
    if missing:
        sys.exit(f'no redirects.json entry has "new" set to {", ".join(missing)}')
    for entry in entries:
        if entry['new'] in pages:
            entry['rewrittenFrom'] = last_commit(entry['old'])
            print(f'{entry["old"]} -> {entry["new"]}: {entry["rewrittenFrom"]}')
    with open(REDIRECTS, 'w') as f:
        f.write(json.dumps(entries, indent='\t', ensure_ascii=False) + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])
```

`/tmp/b4-write-landing.py` writes the landing pages in Step 2:

```python
#!/usr/bin/env python3
"""Writes the five section landing pages.

Each page keeps its front matter minus "status: draft", then gets the
section's card grids from landing.py with the prose below around them.
Run from the repository root, with landing.py saved as /tmp/b4-landing.py:

    python3 write-landing.py
"""

import re
import subprocess
from pathlib import Path

CONTENT = Path('website/src/lib/content')
SECTIONS = ['getting-started', 'guides', 'concepts', 'reference', 'contributing']

BEFORE = {
    'concepts': 'New to OME? Read [How OME works](architecture/how-ome-works.md) first; the other pages build on it.\n\n',
}

AFTER = {
    'getting-started': """
## Next steps

- To learn how OME's resources fit together, read [Concepts](../concepts/index.md).
- To deploy models, roll out changes and operate OME, follow the [Guides](../guides/index.md).
- To look up a field, a command or a matching rule, see the [Reference](../reference/index.md).
- To contribute code or docs, see [Contributing](../contributing/index.md).
""",
    'contributing': """
## Report a bug or request a feature

Open an issue on GitHub with the [bug report](https://github.com/ome-projects/ome/issues/new?template=BUG_REPORT.md) or [enhancement request](https://github.com/ome-projects/ome/issues/new?template=ENHANCEMENT.md) template. For questions about using OME, use the [support request](https://github.com/ome-projects/ome/issues/new?template=SUPPORT.md) template.
""",
}

# The Multi-cluster guides are alpha; say so once, above their cards.
MULTI = '## Multi-cluster\n\n'
ALPHA = 'Multi-cluster routing is alpha. Fields and behavior can change between releases.\n\n'


def main():
    for section in SECTIONS:
        page = CONTENT / section / 'index.md'
        front = re.match(r'---\n.*?\n---\n', page.read_text(), re.S).group(0)
        front = front.replace('status: draft\n', '')
        cards = subprocess.run(
            ['python3', '/tmp/b4-landing.py', 'cards', section],
            check=True, capture_output=True, text=True,
        ).stdout
        if section == 'guides':
            cards = cards.replace(MULTI, MULTI + ALPHA)
        page.write_text(front + '\n' + BEFORE.get(section, '') + cards + AFTER.get(section, ''))
        print(f'wrote {page}')


if __name__ == '__main__':
    main()
```

Then check the first one:

```bash
python3 /tmp/b4-landing.py cards guides | head -11
python3 /tmp/b4-landing.py check | tail -1
```

Expected: the start of the Deploy models grid, then `5 landing pages, 84 cards, 84 problems`, since no landing page has cards yet:

```text
## Deploy models

<div class="grid cards" markdown>

-   **[Select accelerators](deploy-models/select-accelerators.md)**

    Pick the AcceleratorClass for each component by naming a class, or let OME choose one with the BestFit, Cheapest, MostCapable or FirstAvailable policy.

-   **[Serve models from a PVC](deploy-models/serve-models-from-pvc.md)**

    Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes.
```

- [ ] **Step 2: Write the landing pages**

```bash
python3 /tmp/b4-write-landing.py
python3 /tmp/b4-landing.py check | tail -1
```

Expected: five `wrote …` lines, then `5 landing pages, 84 cards, 0 problems`. Read the five pages. Around the cards they have:

- **Getting Started:** its five cards, then Next steps, pointing to the other four sections.
- **Guides:** 29 cards under seven `h2`s, with "Multi-cluster routing is alpha. Fields and behavior can change between releases." above the Multi-cluster cards.
- **Concepts:** a pointer to How OME works, then 18 cards under five `h2`s.
- **Reference:** 29 cards under five `h2`s.
- **Contributing:** its three cards, then how to report a bug or request a feature, with links to the three templates in `.github/ISSUE_TEMPLATE/`.

This text was reviewed with the plan, so keep it. If you would word something differently, say so in your report.

Set `rewrittenFrom` for the six entries that point at landing pages:

```bash
python3 /tmp/b4-rewritten.py getting-started/index.md guides/index.md concepts/index.md reference/index.md contributing/index.md
```

Expected:

```text
_index.md -> getting-started/index.md: 2f5da6dd
tasks/_index.md -> guides/index.md: 2f5da6dd
administration/_index.md -> guides/index.md: 8c31d399
concepts/_index.md -> concepts/index.md: 8c31d399
reference/_index.md -> reference/index.md: 2f5da6dd
developer-guide/_index.md -> contributing/index.md: 8de82852
```

Run `pnpm lint`, `pnpm check`, `pnpm test` and `pnpm build` from `website/` and the pre-commit hooks on the changed files, then commit:

```bash
git add website/src/lib/content/*/index.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the section landing pages" -m "Each landing page lists its section's pages as cards, in nav order
under the nav's group headings, with each page's title and
description. Getting Started ends with where to go next, and
Contributing with how to report a bug or request a feature."
```

- [ ] **Step 3: Write the Serve models from a PVC guide**

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/serve-models-from-pvc.md`. Its examples pass the YAML check.
- `pkg/utils/storage/storage.go`: `ParsePVCStorageURI`, for the two URI forms and what makes a URI invalid.
- `pkg/validation/basemodel.go`: `ValidatePVCStorage`, which both webhooks in `pkg/webhook/admission/basemodel/basemodel_webhook.go` call. A BaseModel's URI has no namespace, a ClusterBaseModel's must have one, and `distribution: Sharded` is rejected.
- `pkg/controller/v1beta1/basemodel/backends/pvc/`: `pvc.go` (the state table in the doc comment on `Reconcile`, and every condition it writes), `metadata_job.go` (the Job's namespace, name, command and what it writes back), `metadata_rbac.go` (the ServiceAccount and RoleBinding it creates in the claim's namespace), `backend.go` and `watches.go`.
- `pkg/apis/ome/v1beta1/model.go`: `StorageSpec`, `Distribution`, `LifeCycleState` and the `ModelConditionReasonPVC*` reasons.
- `pkg/modelagent/scout.go` and `gopher.go`: the model agent skips `pvc://` models.
- `pkg/controller/v1beta1/inferenceservice/components/pvc.go`: how serving pods mount the claim.
- `charts/ome-resources/values.yaml` (`ome.omeAgent.metadataJob`, about line 712) and `charts/ome-resources/templates/ome-controller/configmap.yaml` (how those values reach the controller).

The doc-template mockup shows this page. Its `h2`s, in order: Before you begin (the prerequisites box), The pvc:// storage URI, Step 1: Verify the PVC, Step 2: Create the model, Step 3: Watch the model become Ready, Step 4: Deploy an InferenceService, Configure the metadata Job, Troubleshooting, Clean up, Next steps. Use the mockup's names throughout: namespace `llama-demo`, claim `model-storage`, weights under `llama-3-2-1b-instruct/` on the claim, and model `llama-3-2-1b-instruct`.

- **Before you begin:** OME installed with kubectl access, and a Bound claim holding the weights in a Hugging Face layout (`config.json` plus the weight files). Say which access mode the claim needs when serving pods run on more than one node, going only as far as `components/pvc.go` and Kubernetes guarantee.
- **After the box**, the mockup's note: "Nothing is downloaded. The model agent skips pvc:// models, and serving pods mount the claim read-only." Confirm both claims in the code first.
- **The pvc:// storage URI:** both forms, `pvc://{pvc-name}/{sub-path}` and `pvc://{namespace}:{pvc-name}/{sub-path}`, which kind uses which and why, and what the sub-path must be.
- **Step 1:** show that the claim is Bound, selecting columns so the output fits.
- **Step 2:** a tabbed set with a BaseModel tab and a ClusterBaseModel tab, each a complete `yaml title="model.yaml"` block, then `kubectl apply -f model.yaml` in its own block and its output.
- **Step 3:** the watch command with custom columns, as in the mockup, and its output going from `In_Transit` to `Ready`. Say what happens meanwhile: the metadata Job, what it reads and what it records. Then show what it recorded, with a command whose output you can derive exactly from the code.
- **Step 4:** a complete InferenceService in `llama-demo` that serves the model, a check that it's ready, and a request to it. The reader needs a runtime that matches the model: name one from `config/runtimes/` or let OME select one, and say which you did and why it matches.
- **Configure the metadata Job:** a table of the `ome.omeAgent.metadataJob` values and their defaults, then a Helm values example for a claim that only mounts on some nodes, using `nodeSelector` and `tolerations`.
- **Troubleshooting:** a table with the columns State, Reason, Meaning and Fix, one row for each state and reason the PVC backend writes while waiting or on failure, then the webhook's rejections with their messages. The `distribution: Sharded` rejection is new in v1.3, so it goes under its own `h3` with `since=v1.3`, as a warning.
- **Clean up:** delete the InferenceService and the model, and say what happens to the data on the claim, the metadata Job and its RBAC objects.
- **Next steps:** links to Base models, Serve models from node-local storage and InferenceServices.

Then set `rewrittenFrom` and run the YAML check:

```bash
python3 /tmp/b4-rewritten.py guides/deploy-models/serve-models-from-pvc.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/serve-models-from-pvc.md -> guides/deploy-models/serve-models-from-pvc.md: b3f561eb`, then `Checked N objects: 0 problems.`, where N is the number of objects in the page's YAML blocks. In a sandbox, `go run` may warn that it can't write the Go stat cache; that's harmless. Run the website checks and the pre-commit hooks, then commit:

```bash
git add website/src/lib/content/guides/deploy-models/serve-models-from-pvc.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Serve models from a PVC guide" -m "The guide goes from a Bound claim to a serving InferenceService,
with the pvc:// URI rules, the metadata Job's settings and a
troubleshooting table for every state the PVC backend reports."
```

- [ ] **Step 4: Write the Base models concept page**

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/base_model.md`. Its known errors:
  - The storage section (lines 98 to 160) lists only the oci, hf, pvc and vendor schemes. `model.go` and `storage.go` support nine: oci, pvc, vendor, hf, s3, az, gs, github and local.
  - Seven examples fail the YAML check: `spec.storage.storageKey` at lines 30, 452 and 475 (the field is `key`), a BaseModel without `spec.storage` at line 352, two Secrets whose `data` isn't base64 at lines 429 and 443 (use `stringData`), and a FineTunedWeight at line 560 whose `hyperParameters` is a string instead of an object.
- `pkg/apis/ome/v1beta1/model.go`: every field of `BaseModelSpec`, `StorageSpec` and `ModelStatusSpec`, and the `LifeCycleState` values.
- `pkg/utils/storage/storage.go`: the URI formats, in `GetStorageType` and the `Parse*` functions.
- `pkg/modelagent/`: how the agent chooses nodes, downloads, labels nodes and reports per-node state.
- `pkg/controller/v1beta1/basemodel/`: the controller and its storage backends.
- `git diff 5f1c4096 HEAD -- pkg/apis/ome/v1beta1/model.go`, for what's new since v1.2.2.

A concept page explains rather than walks through steps. Outline:

- An opening paragraph, before the first `h2`, that says what the two kinds are for.
- **BaseModel and ClusterBaseModel:** namespaced and cluster-scoped, and which InferenceServices can use each. One complete example.
- **Where the weights come from:** a table of the nine URI schemes with their formats, then `key` and `parameters` for credentials, with a complete example that reads a token from a Secret. Link the PVC guide and Serve models from node-local storage.
- **How weights reach the nodes:** the model agent, `storage.path`, `nodeSelector` and `nodeAffinity`, the node labels, and the per-node counts in the status. Then `### Distribution {since=v1.3}` for PerNode and Sharded.
- **What OME learns from the model:** what the agent reads from the model's files, and which spec fields it fills in or leaves alone.
- **Model lifecycle:** the states, what moves a model between them, and what deleting a model does on the nodes.
- **How runtimes match the model:** a short paragraph linking Serving runtimes and the matching reference.
- **Fine-tuned weights:** a short paragraph linking the Fine-tuned weights page.
- **Next steps.**

Then:

```bash
python3 /tmp/b4-rewritten.py concepts/models/base-models.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/base_model.md -> concepts/models/base-models.md: bf81aa4c`, then `0 problems`. Run the website checks and the pre-commit hooks, then commit:

```bash
git add website/src/lib/content/concepts/models/base-models.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Base models concept page" -m "The page covers both kinds, all nine storage schemes, how weights
reach the nodes, what OME learns from a model and the model
lifecycle, with examples that pass the YAML check."
```

- [ ] **Step 5: Write the kubectl ome rollout page**

Sources:

- The Hugo pages, for topics: `site/content/en/docs/tasks/kubectl-ome-rollout-{explain,history,validate}.md`.
- The command itself. Build it and read every subcommand's help:

  ```bash
  CGO_ENABLED=0 go build -o /tmp/b4-kubectl-ome ./cmd/kubectl-ome
  for c in status explain history validate pause resume promote rollback repin; do /tmp/b4-kubectl-ome rollout $c --help; done
  ```

  The usage lines say `ome rollout`, the binary's own name. The page writes `kubectl ome rollout`, since kubectl runs it as a plugin.
- `pkg/cli/cmd/rollout/` (`rollout.go`, `explain.go`, `history.go`, `validate.go`, `actions.go`, `repin.go`) and `pkg/cli/exitcode/exitcode.go`.
- Output: the golden files in `pkg/cli/cmd/rollout/testdata/` (`explain`, and the `history_healthy`, `history_partial` and `history_unavailable` cases, each as JSON and YAML), and the expected strings in the tests, such as `TestRolloutCommandLocalHelpIsExact`, `TestValidateCommandLocalHelpDocumentsExitSemantics`, `TestHistoryHelpStatesRetentionBoundary` and `TestGuardedActionTimeoutHelpIsQualified`. Show output only from these. Where a command has none, show the command without output and describe its fields.

Front matter: keep `title` and `description`, and add `since: v1.3`. The body follows the CLI reference template in the spec:

- An opening that says what the command is for, a synopsis block, and a table of the nine subcommands.
- One `h2` per subcommand, named in code: `` ## `status` ``, `` ## `explain` ``, `` ## `history` ``, `` ## `validate` ``, then `` ## `pause` and `resume` ``, `` ## `promote` and `rollback` `` and `` ## `repin` ``. Each starts with a synopsis block and what the command does.
- Under each, `h3`s with explicit ids, since every subcommand has the same ones: `### Flags {#status-flags}`, `### Output fields {#status-output-fields}` for the four read commands, and `### Examples {#status-examples}`. A Flags table has the columns Flag, Default and Description, copied from the help, and is followed by "Plus the standard kubeconfig flags, such as `-n` and `--context`."
- Under the heading of each alpha subcommand (the help marks pause, resume, promote, rollback and repin "Alpha"): `!!! note "Alpha"` with "This command is alpha. Its flags and behavior can change between releases." Explain the guarded flow the actions share (the preview, `--yes`, `--dry-run`) once, and link `guarded-actions.md` for the full contract.
- **Exit codes:** a table with the columns Code, Meaning and Returned by, from `exitcode.go` and the places each subcommand returns `UnmetAssertionError` or `PreconditionError`.
- **Related guides:** the Roll out changes guides that use these commands, the guarded actions reference and Troubleshoot an InferenceService.

Then:

```bash
python3 /tmp/b4-rewritten.py reference/kubectl-ome/rollout.md
```

Expected:

```text
tasks/kubectl-ome-rollout-explain.md -> reference/kubectl-ome/rollout.md: c650bb31
tasks/kubectl-ome-rollout-history.md -> reference/kubectl-ome/rollout.md: 75957e69
tasks/kubectl-ome-rollout-validate.md -> reference/kubectl-ome/rollout.md: 652eab96
```

If the page has YAML blocks, run the YAML check too. Run the website checks and the pre-commit hooks, then commit:

```bash
git add website/src/lib/content/reference/kubectl-ome/rollout.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome rollout page" -m "A reference for every rollout subcommand, with its synopsis, flags,
output fields and examples taken from the CLI's golden files, plus
the exit codes and the guides that use the commands."
```

- [ ] **Step 6: Check the pages**

Start the dev server on port 5177 (see [Servers during Phase B](#servers-during-phase-b)), then:

```bash
B=http://localhost:5177/ome
for p in getting-started guides concepts reference contributing guides/deploy-models/serve-models-from-pvc concepts/models/base-models reference/kubectl-ome/rollout; do
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(curl -s $B/$p | grep -c 'been rewritten yet')"
done
for s in getting-started guides concepts reference contributing; do echo "$s $(curl -s $B/$s | grep -o 'class="doc-card"' | wc -l)"; done
curl -s $B/guides/deploy-models/serve-models-from-pvc | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -
curl -s $B/reference/kubectl-ome/rollout | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -
curl -s $B/reference/kubectl-ome/rollout | grep -o 'since:{[^}]*}'
curl -s $B/search.json | python3 -c 'import json, sys; pages = {e["route"]: e for e in json.load(sys.stdin)}; print("pvc://" in pages["guides/deploy-models/serve-models-from-pvc"]["body"])'
```

Expected:

- each of the eight pages followed by `200 0`: it loads and isn't a draft;
- the card counts `5`, `29`, `18`, `29` and `3`;
- `before-you-begin the-pvc-storage-uri step-1-verify-the-pvc step-2-create-the-model step-3-watch-the-model-become-ready step-4-deploy-an-inferenceservice configure-the-metadata-job troubleshooting clean-up next-steps`;
- `status explain history validate pause-and-resume promote-and-rollback repin exit-codes related-guides`;
- `since:{state:"unreleased",label:"Unreleased: coming in v1.3"}`, or `since:{state:"unknown",label:"Since v1.3"}` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw;
- `True`: the search index has the guide's prose. Code blocks aren't indexed, so this comes from inline code.

Then take screenshots at 1280 and 390 pixels wide of the PVC guide, next to the doc-template mockup, and of the Guides landing page. The pages are unstyled on your branch, so check structure, not looks: every heading, tab set, callout, table and code block renders, and no raw Markdown shows. Stop the server with `pkill -f 'port 5177'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 7: Report**

Run `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/site-redesign...HEAD)` from the worktree root, and `git diff --exit-code go.mod go.sum`. Report per Ground rule 8, and include the Step 6 output, which since label you saw, the YAML check's object count for each page, every place where the code contradicted a Hugo page (with the file and line on both sides), and anything you couldn't verify.

## Phase C: Integration

### Task C1: Merge, verify and review

**Branch:** `docs/site-redesign`, in the main checkout at `/Users/simolin/go/src/github.com/ome-projects/ome`.

**Goal:** Merge B1 to B4 into `docs/site-redesign`, prove that the merged site works as a whole, review its code, content and looks, fix what the reviews find, and remove the Phase A and B worktrees. At the end, `docs/site-redesign` is the Foundation PR, waiting for the user's go-ahead to push.

**Files:** No new files. Merge conflicts can only happen in `website/package.json` and `website/pnpm-lock.yaml`, and review fixes go in the files the findings name.

**How it works:** Each Phase B branch passed its own checks, but three things only show up together: B1's components and styles on B3's and B4's pages, links between pages written on different branches, and dependencies that both B1 and B2 added. The branches merge in the order B3, B4, B2, B1: content first, then the home page, then the UI, so the last merge runs B1's components over every written page. `pnpm test` after each merge pins a broken link on the merge that caused it; the full checks run once, at the end.

Don't push, don't open a PR and don't rebase on `main`: the user decides when. Commit fixes on `docs/site-redesign` per Ground rule 2.

- [ ] **Step 1: Check the branches**

Save this script as `/tmp/c1-branches.py`. It checks each branch's changed files against [File ownership](#file-ownership), and its `redirects.json` changes against its own pages:

```python
#!/usr/bin/env python3
"""Checks that each Phase B branch changed only the files its task owns.

For each branch, lists the files it changed since it left
docs/site-redesign and flags any outside its task's rows in the plan's
File ownership table. Where a branch changed website/redirects.json, it
also checks that the branch only set rewrittenFrom, and only on the
entries for its own pages. Run from the repository root:

    python3 branches.py
"""

import fnmatch
import json
import subprocess
import sys

BASE = 'docs/site-redesign'
CONTENT = 'website/src/lib/content/'
DEPS = ['website/package.json', 'website/pnpm-lock.yaml']
VOICE = [
    'getting-started/index.md',
    'guides/index.md',
    'concepts/index.md',
    'reference/index.md',
    'contributing/index.md',
    'guides/deploy-models/serve-models-from-pvc.md',
    'concepts/models/base-models.md',
    'reference/kubectl-ome/rollout.md',
]

# Branch: (task, the files it may change, the pages whose redirects it
# may set). A file entry ending in "/" owns everything under it, one
# with "*" is a glob, and any other entry is an exact path.
TASKS = {
    'docs/website-apiref': ('B3', [
        'hack/genref/website/',
        'hack/genref/config.yaml',
        'Makefile',
        'pkg/apis/ome/v1beta1/inference_service_status.go',
        CONTENT + 'reference/api/ome.v1beta1.md',
        'website/redirects.json',
    ], ['reference/api/ome.v1beta1.md']),
    'docs/website-voice': ('B4', [CONTENT + page for page in VOICE] + [
        'website/redirects.json',
    ], VOICE),
    'docs/website-home': ('B2', DEPS + [
        'website/src/lib/actions/',
        'website/src/lib/components/Home*.svelte',
        'website/src/lib/components/HeroMark.svelte',
        'website/src/lib/components/HeroShaderBackground.svelte',
        'website/src/lib/components/SectionLabel.svelte',
        'website/src/lib/components/PlusMark.svelte',
        'website/src/lib/components/ChoosePathArrow.svelte',
        'website/src/lib/components/SiteFooter.svelte',
        'website/src/lib/components/OrbitMark.svelte',
        'website/src/lib/config/home.ts',
        'website/src/routes/+page.svelte',
        'website/src/styles/home.css',
    ], []),
    'docs/website-ui': ('B1', DEPS + [
        'website/src/lib/ui/',
        'website/src/lib/components/Doc*.svelte',
        'website/src/lib/components/Search*.svelte',
        'website/src/lib/components/Breadcrumbs.svelte',
        'website/src/lib/components/PageNav.svelte',
        'website/src/lib/components/SiteHeader.svelte',
        'website/src/routes/search/',
        'website/src/routes/+error.svelte',
        'website/src/routes/[section=section]/[...slug]/+page.svelte',
        'website/src/styles/docs.css',
        'website/src/styles/search.css',
        'website/src/app.css',
        CONTENT + 'contributing/writing-docs.md',
    ], []),
}


def git(*args):
    return subprocess.run(['git', *args], check=True, capture_output=True, text=True).stdout


def owns(entry, path):
    if entry.endswith('/'):
        return path.startswith(entry)
    if '*' in entry:
        return fnmatch.fnmatchcase(path, entry)
    return path == entry


def redirect_changes(branch):
    """Returns the entries the branch changed, or None if it moved any."""
    base = git('merge-base', BASE, branch).strip()
    before = json.loads(git('show', f'{base}:website/redirects.json'))
    after = json.loads(git('show', f'{branch}:website/redirects.json'))
    if [(e['old'], e['new']) for e in before] != [(e['old'], e['new']) for e in after]:
        return None
    return [a for b, a in zip(before, after) if a != b]


def main():
    problems = 0
    for branch, (task, files, pages) in TASKS.items():
        if subprocess.run(['git', 'rev-parse', '--verify', '--quiet', branch],
                          capture_output=True).returncode:
            print(f'{branch} ({task}): missing')
            problems += 1
            continue
        changed = git('diff', '--name-only', f'{BASE}...{branch}').split()
        stray = [path for path in changed if not any(owns(e, path) for e in files)]
        print(f"{branch} ({task}): {len(changed)} files, {len(stray)} not {task}'s")
        for path in stray:
            print(f'  not {task}\'s: {path}')
        problems += len(stray)
        if 'website/redirects.json' not in changed:
            continue
        changes = redirect_changes(branch)
        if changes is None:
            print('  redirects.json: an entry was added, removed, moved or remapped')
            problems += 1
            continue
        for e in changes:
            ok = e['new'] in pages and e['rewrittenFrom'] is not None
            print(f"  {e['old']} -> {e['new']}: {e['rewrittenFrom']}"
                  + ('' if ok else f"  (not {task}'s to set)"))
            problems += not ok
    print(f'{problems} problems')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
```

Run it from the main checkout:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
git switch docs/site-redesign && git status --short
python3 /tmp/c1-branches.py
```

Expected: `git status` prints nothing and the script ends with `0 problems`. B3 changes 8 files and sets one entry:

```output
docs/website-apiref (B3): 8 files, 0 not B3's
  reference/ome.v1beta1.md -> reference/api/ome.v1beta1.md: 2656a655
```

B4 changes 9 files and sets these 11 entries:

```output
docs/website-voice (B4): 9 files, 0 not B4's
  _index.md -> getting-started/index.md: 2f5da6dd
  tasks/_index.md -> guides/index.md: 2f5da6dd
  administration/_index.md -> guides/index.md: 8c31d399
  tasks/run-workloads/serve-models-from-pvc.md -> guides/deploy-models/serve-models-from-pvc.md: b3f561eb
  concepts/_index.md -> concepts/index.md: 8c31d399
  concepts/base_model.md -> concepts/models/base-models.md: bf81aa4c
  reference/_index.md -> reference/index.md: 2f5da6dd
  tasks/kubectl-ome-rollout-explain.md -> reference/kubectl-ome/rollout.md: c650bb31
  tasks/kubectl-ome-rollout-history.md -> reference/kubectl-ome/rollout.md: 75957e69
  tasks/kubectl-ome-rollout-validate.md -> reference/kubectl-ome/rollout.md: 652eab96
  developer-guide/_index.md -> contributing/index.md: 8de82852
```

B2 and B1 change any number of their own files and no `redirects.json` entries. If a branch changed a file that isn't its own, read the change. Keep it if the task needed it, and say why in your report; otherwise send the branch back to its task to revert the file before you merge anything.

- [ ] **Step 2: Merge B3 and B4**

```bash
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the generated API reference" -m "Task B3 of the docs site redesign plan: genref templates that write
the website's API reference, the generated ome.v1beta1 page, and an
InferenceService status comment that renders as a list." docs/website-apiref
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm test)
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the voice-setting pages" -m "Task B4 of the docs site redesign plan: the five section landing
pages, the Serve models from a PVC guide, the Base models concept
page and the kubectl ome rollout reference." docs/website-voice
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm test)
```

Expected: both merges finish without conflicts, and both test runs pass. B3's and B4's `redirects.json` entries are several lines apart, so git merges them on its own.

- [ ] **Step 3: Merge B2 and B1**

```bash
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the home page" -m "Task B2 of the docs site redesign plan: SMG's home page with OME's
copy, the orbit mark in the hero and a cobalt shader." docs/website-home
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline && pnpm test)
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the docs UI and search" -m "Task B1 of the docs site redesign plan: the doc page layout, the
sidebar, the table of contents, since, preview and draft states,
search, the 404 page and the Writing docs page." docs/website-ui
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline && pnpm test)
```

Expected: both merges finish and both test runs pass. The only files both branches may change are `website/package.json` and `website/pnpm-lock.yaml`, so a conflict anywhere else means Step 1 missed an ownership problem: run `git merge --abort` and go back to Step 1. If `pnpm install --offline` reports a package missing from the store, rerun it without `--offline`.

If both branches added dependencies, the B1 merge stops on those two files. Resolve it like this:

```bash
git checkout --ours website/pnpm-lock.yaml
# In website/package.json, keep the lines both sides added, in alphabetical order, and delete the conflict markers.
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install && pnpm test)
git add website/package.json website/pnpm-lock.yaml
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s --no-edit --cleanup=strip
```

`pnpm install` without flags adds B1's packages to B2's lockfile, fetching what the store lacks; if the registry is unreachable, stop and report. The last command finishes the merge with the message from the `merge` command. After a conflict, git drops the sign-off from that message and appends a `# Conflicts:` list, so `-s` puts the sign-off back and `--cleanup=strip` removes the list.

The user's live server on port 5173 runs `pnpm dev` from this checkout. The merges change its dependencies, so restart it once both are in:

```bash
lsof -ti tcp:5173 | xargs kill 2>/dev/null; pkill -f '/Users/simolin/go/src/github.com/ome-projects/ome/website/node_modules/.*workerd'; sleep 2
(cd /Users/simolin/go/src/github.com/ome-projects/ome/website && export PATH=/tmp/ome-tools/bin:$PATH && nohup pnpm dev --port 5173 --strictPort > /tmp/ome-live-dev.log 2>&1 &)
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5173/ome/ && break; sleep 1; done; curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5173/ome/
```

Expected: `200`.

- [ ] **Step 4: Run the checks**

The website:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome/website
export PATH=/tmp/ome-tools/bin:$PATH
rm -f worker-configuration.d.ts && pnpm lint && pnpm test
pnpm check && pnpm build
grep -rlE 'registerLanguage|walkTokens' .svelte-kit/output/server .svelte-kit/cloudflare/_worker.js || echo "no renderer in the worker"
python3 -c 'import json; e = json.load(open(".svelte-kit/cloudflare/ome/search.json")); print(len(e), sum(x["status"] is None for x in e))'
```

Expected: all four pass, with at least 20 test files and 179 tests (A1's 15 and 155 plus B1's 5 and 24); then `no renderer in the worker`; then `89 10`: 89 pages in the search index, 10 of them written (B4's 8, the API reference and Writing docs).

The rest of the repo, from the repository root:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
export KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64"
make docs-drift
go run ./hack/docs-examples
python3 /tmp/b4-landing.py check | tail -1
go vet ./hack/docs-drift/ ./hack/docs-examples/ ./pkg/apis/ome/v1beta1/
go test ./hack/docs-drift/ ./hack/docs-examples/
make -o controller-gen -o yq manifests > /tmp/c1-manifests.log 2>&1; echo "exit $?"
git status --short
git diff --exit-code main...HEAD -- go.mod go.sum && echo "go.mod and go.sum unchanged"
```

`/tmp/b4-landing.py` is the script B4 saved in its Step 1; if it's gone, save it again from [Task B4](#task-b4-the-voice-setting-pages) Step 1.

Expected:

- `No drift: every Hugo page is mapped, and every rewrite is current.` The `rewrittenFrom` commits are the last ones to change each Hugo page on this branch, which has none of `main`'s later changes to `site/`.
- `Checked N objects: 0 problems.`, where N is the sum of the object counts B4 reported for its pages.
- `5 landing pages, 84 cards, 0 problems`.
- `go vet` prints nothing, and `go test` passes for both packages.
- `exit 0`, and `git status` prints nothing: B3's comment change doesn't reach the CRDs, because `CRD_OPTIONS` sets `maxDescLen=0`.
- `go.mod and go.sum unchanged`.

- [ ] **Step 5: Check the production build**

Serve the build from Step 4 with wrangler, as Cloudflare will:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome/website
pnpm preview > /tmp/ome-preview.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:4173/ome && break; sleep 1; done
B=http://localhost:4173/ome
for path in / /getting-started /guides /concepts /reference /contributing /guides/deploy-models/serve-models-from-pvc /concepts/models/base-models /reference/kubectl-ome/rollout /reference/api/ome.v1beta1 /contributing/writing-docs /guides/networking/configure-ingress '/search?q=pvc' /search.json /guides/nope /nope; do
  printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "$B$path")" "$path"
done
curl -s $B/getting-started | grep -o '<title>[^<]*</title>'
curl -s $B/ | grep -o 'The Kubernetes operator for serving LLMs in production' | head -1
curl -s $B/guides | grep -o 'class="doc-card"' | wc -l
curl -s $B/reference/api/ome.v1beta1 | grep -o '<li class="docs-toc-item' | wc -l
curl -s $B/reference/api/ome.v1beta1 | grep -o 'docs-toc-item--h3' | wc -l
curl -s $B/guides/deploy-models/serve-models-from-pvc | grep -o '<li class="docs-toc-item' | wc -l
curl -s $B/reference/kubectl-ome/rollout | grep -o 'class="doc-since doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sort -u
curl -s $B/guides/networking/configure-ingress | grep -o 'This page is being rewritten\|href="https://ome-projects.github.io/ome/docs/administration/ingress/"' | sort | uniq -c
curl -s $B/guides/nope | grep -o 'Page not found' | wc -l
curl -s -w ' %{http_code}\n' "$B."
```

Expected:

- `200` for every path but the last two, and `404` for those.
- `<title>Getting Started · OME</title>`, then the hero title once.
- `29` cards on the Guides landing page, one per guide.
- `15` TOC entries on the API reference, its 14 kinds and Supporting types, and `0` of them `h3`s, because the page has more than 40 headings. Then at least `10` on the PVC guide: its 10 `h2`s and any `h3`s.
- The rollout page's since badge: either `class="doc-since doc-since--unreleased">Unreleased: coming in v1.3` or, when the GitHub API rate limit has run out, `class="doc-since doc-since--unknown">Since v1.3`. Both are correct; note which one you saw.
- On the Ingress draft, `This page is being rewritten` and the link to its Hugo page once each.
- `Page not found` at least twice, in the `<title>` and the `h1`.
- `Not found 404` for `/ome.`: `src/hooks.server.ts` answers paths that only share the base's prefix with a plain 404.

Leave the server running for Step 6's visual review.

- [ ] **Step 6: Review**

Dispatch four reviewers at once, each an Opus agent in its own worktree. Every reviewer first runs `git switch --detach docs/site-redesign`, edits nothing, and reports findings as a list, most severe first. Each finding has a severity (must fix, should fix or nit), a `file:line`, what's wrong, the evidence (the code, output or screenshot that shows it) and a suggested fix.

1. **Website code** (`superpowers:code-reviewer`). Everything under `website/` except `src/lib/content/`, `redirects.json` and the lockfile, in `git diff main...docs/site-redesign`. Much of it is copied from SMG, so diff against `/tmp/smg-docs-ref` and review what OME changed or added. Check the code against this plan's [Shared interfaces](#shared-interfaces) and Tasks A1, B1 and B2. Look for SSR and hydration mismatches, `{@html}` on anything but build-time HTML, search highlighting that could inject markup, keyboard and screen reader behavior in the sidebar, TOC and search dialog, listeners that outlive their component, and SMG names, marks or colors left behind.
2. **Go, Makefile and CI** (`superpowers:code-reviewer`). `hack/docs-examples`, `hack/docs-drift`, `hack/genref/website`, the `Makefile` targets, `.github/`, `.pre-commit-config.yaml`, `AGENTS.md`, `NOTICE` and the status comment in `pkg/apis/`. Check them against Tasks A2 to A4 and B3 and the Google Go Style Guide. Confirm that `website.yml` runs every check the spec's Test Strategy lists, that the docs-drift job can't fail the workflow, and that the tests cover the failure paths.
3. **Content** (`general-purpose`). The 10 written pages: B4's 8, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. Check every field, default, flag, state, condition reason and output against the code on `docs/site-redesign`, and every `since` marker against v1.2.2 (`git show 5f1c4096:<path>`). Check the YAML against the admission webhooks in `pkg/webhook/admission/` and `pkg/validation/`, which the example check doesn't run. Check the pages against the voice rules in [Task B4](#task-b4-the-voice-setting-pages) and on the Writing docs page, and check that Writing docs describes the renderer as it is in `website/src/lib/markdown/`.
4. **Looks** (`general-purpose`). With `meta browser` (see `meta browser --help`) against the preview server at `http://localhost:4173/ome/`, screenshot at 1280 and 390 pixels wide: the home page next to the home page mockup; the Guides landing page; the PVC guide next to the doc-template mockup; the Base models page; the rollout page; the API reference; the Ingress draft; the search dialog and `/search?q=pvc`; and a 404 page. Check the interactions from B1 Steps 8 and 11 and B2 Step 4: sidebar groups, the TOC following the scroll, Copy, tabs, ⌘K search with the arrow keys, Enter and Escape, the mobile menu, the hero mark's shader, and the header over Choose your path. Report every difference from the mockups with its screenshot's path. The mockups are the two files in Ground rule 4.

- [ ] **Step 7: Fix the findings**

Check each finding against the code before acting on it; reviewers can be wrong. Fix every confirmed must-fix and should-fix finding on `docs/site-redesign`, rerun the checks from Steps 4 and 5 that cover the change, and commit each group of related fixes per Ground rule 2, for example:

```bash
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Fix review findings in the docs UI" -m "One line per fix, saying what was wrong and what changed."
```

Fix nits when they're quick. List every finding you don't fix, with the reason, for the report. Then stop the preview server. Stopping wrangler leaves its `workerd` behind, so stop whatever still holds the port:

```bash
pkill -f 'wrangler.js pages dev'; sleep 2; lsof -ti tcp:4173 | xargs kill 2>/dev/null
lsof -ti tcp:4173 || echo "port 4173 free"
```

- [ ] **Step 8: Remove the worktrees and branches**

`/tmp/ome-b4-work` and `/tmp/ome-hack-proto` are prototypes made while writing this plan; `--force` discards their uncommitted scratch changes. The rest are the agents' worktrees from Phases A to C.

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
git worktree remove --force /tmp/ome-b4-work
git worktree remove --force /tmp/ome-hack-proto
for w in .claude/worktrees/agent-*; do git worktree remove --force "$w"; done
git worktree prune
git branch -d docs/website-plumbing docs/website-drift docs/website-examples docs/website-app docs/website-apiref docs/website-voice docs/website-home docs/website-ui
git for-each-ref --format='%(refname:short)' --merged docs/site-redesign 'refs/heads/worktree-agent-*' | xargs git branch -d
git worktree list && git branch --list 'docs/*' 'worktree-agent-*'
```

Expected: `git worktree list` shows only the main checkout, and the last command lists only `docs/site-redesign`. `git branch -d` refuses a branch that isn't merged; if it does, find out why instead of forcing it. The `worktree-agent-*` branches are the ones the Agent tool creates with each worktree, at `main`; they carry no commits.

- [ ] **Step 9: Report**

Tell the user:

- that the Foundation PR is ready on `docs/site-redesign`, with `git log --oneline --first-parent main..docs/site-redesign`;
- the output of the checks in Steps 4 and 5, and the since label you saw;
- what the reviews found, what you fixed, and what you didn't and why;
- the prerequisites from the spec that must be settled before the PR is public: an Apache-2.0 LICENSE in smg-docs, and confirmation that Studio Noiich's license covers reusing the layout and styles;
- that rebasing on `main` before the PR can surface drift on the pages B3 and B4 wrote, since the nightly docs bot keeps changing `site/`; `make docs-drift` lists what to fold in.

Ask before pushing the branch or opening the PR.

## Phase D: Content

Phase D writes the 79 pages that are still drafts after Phase C, in eleven batches. Each batch is one agent in its own worktree, one branch and, later, one PR. The orchestrator creates `docs/website-content` from the tip of `docs/site-redesign` once C1 is done, and merges each batch into it after the batch passes review.

The batches run in two waves. Wave 1 writes Getting Started and Concepts, which the other pages link to. Wave 2 starts after all of wave 1 has merged, so its pages can link to headings on wave 1's pages.

| Batch | Wave | Pages | Branch | Port |
|---|---|---|---|---|
| D1 | 1 | The five Getting Started pages and How OME works | `docs/website-d1` | 5181 |
| D2 | 1 | Deployment modes, OMENative update strategies, Fine-tuned weights and the four runtime concepts | `docs/website-d2` | 5182 |
| D3 | 1 | The four serving concepts and the five rollout and traffic concepts | `docs/website-d3` | 5183 |
| D4 | 2 | Select accelerators, the two runtime selection guides and the four matching references | `docs/website-d4` | 5184 |
| D5 | 2 | The six networking guides and Traffic annotations | `docs/website-d5` | 5185 |
| D6 | 2 | The five Roll out changes guides, Canary progression and Canary metric analysis | `docs/website-d6` | 5186 |
| D7 | 2 | The two scale and migrate guides, the two multi-cluster guides and Troubleshoot an InferenceService | `docs/website-d7` | 5187 |
| D8 | 2 | The seven Operate OME guides and Labels and annotations | `docs/website-d8` | 5188 |
| D9 | 2 | The kubectl-ome overview and the accelerator, admin, autoscale, cluster, get, instance, logs and migration pages | `docs/website-d9` | 5189 |
| D10 | 2 | The placement, quota, runtime, scale, status, traffic, version and wait pages, and Guarded actions | `docs/website-d10` | 5190 |
| D11 | 2 | Serve models from node-local storage, Run benchmarks, Benchmark output storage and the two contributing pages | `docs/website-d11` | 5191 |

Each task lists its pages, the `redirects.json` entries it sets and one step per page. The check patterns in each task's check step match B1's markup as C1 merged it. If a pattern prints nothing where a count is expected, read the page's HTML before changing the page.

### Running Phase D

This section is for the orchestrator. Batch agents skip it.

- [ ] **Step 1: Create the content branch**

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
git switch docs/site-redesign && git status --short
git switch -c docs/website-content
git log -1 --format=%s
```

Expected: `git status` prints nothing, and the subject is the last commit C1 made. Every wave 1 prompt quotes it. The live server on port 5173 runs from this checkout, so from now on it shows `docs/website-content`, including each batch as it merges.

Build and count the written pages:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome/website
export PATH=/tmp/ome-tools/bin:$PATH
rm -f worker-configuration.d.ts && pnpm build > /tmp/d-build.log 2>&1 && python3 -c 'import json; e = json.load(open(".svelte-kit/cloudflare/ome/search.json")); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected: `89 10`.

- [ ] **Step 2: Save the tools**

`batches.py` checks that each batch changed only what it owns (see [What a batch owns](#what-a-batch-owns)). It lists the files the batch's branch changed, then checks the batch's `redirects.json` entries, its landing cards and its pages' front matter:

```python
#!/usr/bin/env python3
"""Checks that each Phase D batch changed only what the batch owns.

For each batch named on the command line, lists the files its branch
changed since it left docs/website-content and flags any that aren't
the batch's pages, website/redirects.json or a section landing page.
Then it checks what changed inside those files:

- redirects.json: no entry was added, removed, moved or remapped, only
  the batch's entries changed, and every one of them has a
  rewrittenFrom commit.
- Landing pages: with the batch's own cards cut out, each landing page
  is the same as where the branch started.
- Front matter: none of the batch's pages is a draft, exactly the
  preview pages have "status: preview", and every kubectl-ome page has
  "since: v1.3".

Run from the repository root:

    python3 batches.py d1 d2 d3
"""

import json
import posixpath
import re
import subprocess
import sys

import yaml

BASE = 'docs/website-content'
CONTENT = 'website/src/lib/content/'
REDIRECTS = 'website/redirects.json'
SECTIONS = ['getting-started', 'guides', 'concepts', 'reference', 'contributing']
LANDINGS = [f'{CONTENT}{section}/index.md' for section in SECTIONS]

BATCHES = {
    'd1': [
        'getting-started/introduction.md',
        'getting-started/install.md',
        'getting-started/serve-your-first-model.md',
        'getting-started/pre-configured-models.md',
        'getting-started/private-registries.md',
        'concepts/architecture/how-ome-works.md',
    ],
    'd2': [
        'concepts/architecture/deployment-modes.md',
        'concepts/architecture/omenative-update-strategies.md',
        'concepts/models/fine-tuned-weights.md',
        'concepts/runtimes/serving-runtimes.md',
        'concepts/runtimes/runtime-inheritance.md',
        'concepts/runtimes/runtime-revisions.md',
        'concepts/runtimes/accelerator-classes.md',
    ],
    'd3': [
        'concepts/serving/inference-services.md',
        'concepts/serving/gang-scheduling.md',
        'concepts/serving/autoscaler-policy.md',
        'concepts/serving/benchmarks.md',
        'concepts/rollouts-and-traffic/rollout-policy.md',
        'concepts/rollouts-and-traffic/rollout-groups.md',
        'concepts/rollouts-and-traffic/traffic-policy.md',
        'concepts/rollouts-and-traffic/traffic-map.md',
        'concepts/rollouts-and-traffic/ingress.md',
    ],
    'd4': [
        'guides/deploy-models/select-accelerators.md',
        'guides/deploy-models/reference-a-runtime-explicitly.md',
        'guides/deploy-models/troubleshoot-runtime-selection.md',
        'reference/matching/model-version-matching.md',
        'reference/matching/runtime-accelerator-class-matching.md',
        'reference/matching/runtime-deployment-mode-matching.md',
        'reference/matching/diffusion-pipeline-runtime-matching.md',
    ],
    'd5': [
        'guides/networking/configure-route-timeouts.md',
        'guides/networking/set-service-app-protocols.md',
        'guides/networking/configure-ingress.md',
        'guides/networking/gateway-host-schemes.md',
        'guides/networking/multiple-gateways.md',
        'guides/networking/namespace-gateways.md',
        'reference/api/traffic-annotations.md',
    ],
    'd6': [
        'guides/roll-out-changes/pause-and-resume-a-rollout.md',
        'guides/roll-out-changes/promote-or-roll-back-a-canary.md',
        'guides/roll-out-changes/release-a-held-revision.md',
        'guides/roll-out-changes/repin-a-drifted-rollout-plan.md',
        'guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md',
        'reference/rollouts/canary-progression.md',
        'reference/rollouts/canary-analysis.md',
    ],
    'd7': [
        'guides/scale-and-migrate/request-a-transient-scale.md',
        'guides/scale-and-migrate/request-an-instance-migration.md',
        'guides/multi-cluster/routing-health-probes.md',
        'guides/multi-cluster/drain-a-workload-cluster.md',
        'guides/troubleshoot/troubleshoot-an-inferenceservice.md',
    ],
    'd8': [
        'guides/operate-ome/configure-the-controller.md',
        'guides/operate-ome/model-agent.md',
        'guides/operate-ome/ome-scheduler.md',
        'guides/operate-ome/accelerator-quota.md',
        'guides/operate-ome/shared-hf-artifacts.md',
        'guides/operate-ome/metrics.md',
        'guides/operate-ome/alerting.md',
        'reference/api/labels-and-annotations.md',
    ],
    'd9': [f'reference/kubectl-ome/{name}.md' for name in [
        'overview', 'accelerator', 'admin', 'autoscale', 'cluster',
        'get', 'instance', 'logs', 'migration',
    ]],
    'd10': [f'reference/kubectl-ome/{name}.md' for name in [
        'placement', 'quota', 'runtime', 'scale', 'status',
        'traffic', 'version', 'wait', 'guarded-actions',
    ]],
    'd11': [
        'guides/deploy-models/serve-models-from-local-storage.md',
        'guides/deploy-models/run-benchmarks.md',
        'reference/storage/benchmark-output-storage.md',
        'contributing/development-setup.md',
        'contributing/pull-requests-and-oeps.md',
    ],
}

PREVIEW = {
    'guides/multi-cluster/routing-health-probes.md',
    'guides/multi-cluster/drain-a-workload-cluster.md',
    'concepts/rollouts-and-traffic/traffic-map.md',
    'reference/kubectl-ome/cluster.md',
    'reference/kubectl-ome/placement.md',
}

CARD = re.compile(r'^-   \*\*\[.+\]\(([^)\s]+)\)\*\*\s*$')


def git(*args):
    return subprocess.run(['git', *args], check=True, capture_output=True, text=True).stdout


def branch_exists(branch):
    return subprocess.run(['git', 'rev-parse', '--verify', '--quiet', branch],
                          capture_output=True).returncode == 0


def check_redirects(base, branch, pages):
    """Returns the problems in the branch's redirects.json changes."""
    before = json.loads(git('show', f'{base}:{REDIRECTS}'))
    after = json.loads(git('show', f'{branch}:{REDIRECTS}'))
    if [(e['old'], e['new']) for e in before] != [(e['old'], e['new']) for e in after]:
        return ['redirects.json: an entry was added, removed, moved or remapped']
    problems = []
    for b, a in zip(before, after):
        mine = a['new'] in pages
        if mine:
            print(f"  {a['old']} -> {a['new']}: {a['rewrittenFrom'] or 'null'}")
        if mine and a['rewrittenFrom'] is None:
            problems.append(f"redirects.json: {a['old']} -> {a['new']} has no rewrittenFrom")
        elif not mine and a != b:
            problems.append(f"redirects.json: {a['old']} -> {a['new']} isn't this batch's")
    return problems


def without_own_cards(text, section, pages):
    """Returns a landing page's lines without the cards for pages.

    A card runs from its "-   **[Title](link)**" line to the line
    before the next card or the grid's closing </div>.
    """
    kept, skipping = [], False
    for line in text.splitlines():
        card = CARD.match(line)
        if card or line.startswith('</div>'):
            target = posixpath.normpath(posixpath.join(section, card.group(1))) if card else None
            skipping = target in pages
        if not skipping:
            kept.append(line)
    return kept


def check_landing(base, branch, path, pages):
    section = path[len(CONTENT):].split('/')[0]
    before = without_own_cards(git('show', f'{base}:{path}'), section, pages)
    after = without_own_cards(git('show', f'{branch}:{path}'), section, pages)
    if before != after:
        return [f'{path}: changed outside this batch\'s cards']
    return []


def check_front_matter(branch, pages):
    problems = []
    for page in pages:
        text = git('show', f'{branch}:{CONTENT}{page}')
        match = re.match(r'---\n(.*?)\n---\n', text, re.S)
        front = yaml.safe_load(match.group(1)) if match else None
        if not isinstance(front, dict):
            problems.append(f'{page}: no front matter')
            continue
        status = front.get('status')
        if status == 'draft':
            problems.append(f'{page}: still a draft')
        elif (status == 'preview') != (page in PREVIEW):
            want = 'status: preview' if page in PREVIEW else 'no status'
            got = f'status: {status}' if status else 'no status'
            problems.append(f'{page}: want {want}, got {got}')
        since = front.get('since')
        if page.startswith('reference/kubectl-ome/') and since != 'v1.3':
            got = f'since: {since}' if since else 'no since'
            problems.append(f'{page}: want since: v1.3, got {got}')
    return problems


def check(batch):
    task, branch = batch.upper(), f'docs/website-{batch}'
    if not branch_exists(branch):
        print(f'{branch} ({task}): missing')
        return 1
    pages = set(BATCHES[batch])
    base = git('merge-base', BASE, branch).strip()
    changed = git('diff', '--name-only', f'{base}...{branch}').split()
    owned = {CONTENT + page for page in pages} | {REDIRECTS} | set(LANDINGS)
    problems = [f"not {task}'s: {path}" for path in changed if path not in owned]
    print(f"{branch} ({task}): {len(changed)} files, {len(problems)} not {task}'s")
    if REDIRECTS in changed:
        problems += check_redirects(base, branch, pages)
    else:
        problems.append('redirects.json: unchanged, so no page has a rewrittenFrom')
    for path in LANDINGS:
        if path in changed:
            problems += check_landing(base, branch, path, pages)
    problems += check_front_matter(branch, sorted(pages))
    for problem in problems:
        print(f'  {problem}')
    return len(problems)


def main(batches):
    unknown = [b for b in batches if b not in BATCHES]
    if not batches or unknown:
        sys.exit(__doc__ if not unknown else f'unknown batches: {" ".join(unknown)}')
    problems = sum(check(batch) for batch in batches)
    print(f'{problems} problems')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
```

Save it, with `landing.py` and `rewritten.py` from [Task B4](#task-b4-the-voice-setting-pages) Step 1, straight from this plan:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d-{name}.py', 'w').write(code)
EOF
python3 /tmp/d-batches.py d1 d2 d3
```

Expected: `docs/website-d1 (D1): missing`, the same for D2 and D3, then `3 problems`.

- [ ] **Step 3: Start wave 1**

Send three Agent calls in one message, one each for D1, D2 and D3, each with `model: "opus"` and `isolation: "worktree"`. Each gets this prompt, with its batch number in place of N and the subject from Step 1 in place of SUBJECT:

```text
You are doing Task DN of the OME docs site redesign plan. Your worktree
starts at main. First run:

  git switch -c docs/website-dN docs/website-content
  git log -1 --format=%s

The subject must be "SUBJECT". If it isn't, stop and report.

Then read these sections of
docs/superpowers/plans/2026-09-26-docs-site-redesign.md: "How this plan
runs", "Phase D: Content" up to "Running Phase D", "What a batch owns",
"Servers during Phase D", "Writing rules", "Writing a page" and
"Task DN". Read the Style and Accuracy sections of
docs/superpowers/specs/2026-09-26-docs-site-redesign-design.md.

Do Task DN step by step, and report as its last step says.
```

- [ ] **Step 4: Review each batch**

When a batch reports, start a reviewer for it: an Opus `general-purpose` agent in its own worktree, with this prompt:

```text
Review Task DN of the OME docs site redesign plan. Your worktree starts
at main. Run `git switch --detach docs/website-dN`. Edit nothing, commit
nothing and start no servers.

Read "Writing rules" and "Task DN" in
docs/superpowers/plans/2026-09-26-docs-site-redesign.md. For every page
the task wrote, check each field, default, flag, state, condition,
reason, message and output against the code on this commit, and each
since marker against v1.2.2 (`git show 5f1c4096:<path>`). Check the YAML
examples against the admission webhooks in pkg/webhook/admission/ and
pkg/validation/, which the example check doesn't run. Check the pages
against the Writing rules and against the outline in their step.

Report findings as a list, most severe first. Each has a severity (must
fix, should fix or nit), a file:line, what's wrong, the evidence (the
code or output that shows it, with its file:line) and a suggested fix.
```

Check each finding against the code yourself; reviewers can be wrong. Send the confirmed must-fix and should-fix findings, and any quick nits, to the batch's agent with SendMessage. The agent fixes them in new commits, reruns its task's last two steps and reports again. A batch whose agent has finished gets the findings in a new Agent call in the same worktree path, with the findings and "Fix these on docs/website-dN, then rerun the last two steps of Task DN and report."

- [ ] **Step 5: Merge each batch**

When a batch's fixes are in, check it and merge it:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
python3 /tmp/d-batches.py dN
```

Expected: the batch's changed files with `0 not DN's`, its entries each with a commit, and `0 problems`. Then run the batch's command from this list, and the website's tests:

```bash
# Merge commands for the Phase D batches. Run the one for the batch
# being merged, from the main checkout on docs/website-content.

# D1: Getting Started and How OME works
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the Getting Started pages" -m "Task D1 of the docs site redesign plan: the Introduction, Install OME,
Serve your first model, Pre-configured models and Install from a private
registry pages, and the new How OME works concept page." docs/website-d1

# D2: Deployment modes, fine-tuned weights and runtimes
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the runtime and model concepts" -m "Task D2 of the docs site redesign plan: Deployment modes, OMENative
update strategies, Fine-tuned weights, Serving runtimes, Runtime
inheritance, Runtime revisions and Accelerator classes." docs/website-d2

# D3: Serving, rollout and traffic concepts
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the serving and traffic concepts" -m "Task D3 of the docs site redesign plan: InferenceService, Gang
scheduling, Autoscaler policy, Benchmarks, Rollout policy, Rollout
groups, Traffic policy, Traffic map and Ingress." docs/website-d3

# D4: Runtime selection guides and matching references
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the runtime selection pages" -m "Task D4 of the docs site redesign plan: Select accelerators, Reference a
runtime explicitly, Troubleshoot runtime selection and the four matching
references." docs/website-d4

# D5: Networking guides and traffic annotations
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the networking pages" -m "Task D5 of the docs site redesign plan: the six networking guides, from
route timeouts to per-namespace gateways, and the Traffic annotations
reference." docs/website-d5

# D6: Roll out changes guides and canary references
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the rollout pages" -m "Task D6 of the docs site redesign plan: the five Roll out changes guides
and the Canary progression and Canary metric analysis references." docs/website-d6

# D7: Scale, migration and multi-cluster guides
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the scale and multi-cluster guides" -m "Task D7 of the docs site redesign plan: Request a transient scale,
Request an instance migration, the two multi-cluster guides and the new
Troubleshoot an InferenceService guide." docs/website-d7

# D8: Operator guides and labels reference
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the operator guides" -m "Task D8 of the docs site redesign plan: the seven Operate OME guides,
from the controller's flags to alerting, and the Labels and annotations
reference." docs/website-d8

# D9: kubectl-ome overview and first command references
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the first kubectl ome pages" -m "Task D9 of the docs site redesign plan: the kubectl-ome overview and the
accelerator, admin, autoscale, cluster, get, instance, logs and
migration command references." docs/website-d9

# D10: Remaining kubectl ome command references
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the remaining kubectl ome pages" -m "Task D10 of the docs site redesign plan: the placement, quota, runtime,
scale, status, traffic, version and wait command references and the
Guarded actions contract." docs/website-d10

# D11: Benchmark, storage and contributing pages
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" merge --no-ff --signoff -m "[Docs] Merge the benchmark and contributing pages" -m "Task D11 of the docs site redesign plan: Serve models from node-local
storage, Run benchmarks, Benchmark output storage, Set up a development
environment and the new Pull requests and OEPs page." docs/website-d11
```

```bash
(cd /Users/simolin/go/src/github.com/ome-projects/ome/website && PATH=/tmp/ome-tools/bin:$PATH pnpm test)
```

Expected: the merge finishes and the tests pass. Batches in a wave change disjoint lines of `redirects.json`, since each entry spans five lines, so git merges the file on its own. Landing pages can conflict when two batches changed cards next to each other. Keep both sides' cards, run `python3 /tmp/d-landing.py check | tail -1`, expect `5 landing pages, 84 cards, 0 problems`, then finish the merge with `git add website/src/lib/content/*/index.md` and `git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s --no-edit --cleanup=strip`.

Then load the batch's pages from the live server, which is now serving them:

```bash
for p in $(python3 -c 'import runpy, sys; print(" ".join(p[:-3] for p in runpy.run_path("/tmp/d-batches.py")["BATCHES"][sys.argv[1]]))' dN); do
  printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:5173/ome/$p)" "$p"
done
```

Expected: `200` for every page.

- [ ] **Step 6: Check wave 1**

After D1, D2 and D3 have merged:

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome/website
export PATH=/tmp/ome-tools/bin:$PATH
rm -f worker-configuration.d.ts && pnpm lint && pnpm test && pnpm check && pnpm build
python3 -c 'import json; e = json.load(open(".svelte-kit/cloudflare/ome/search.json")); print(len(e), sum(x["status"] != "draft" for x in e))'
cd ..
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
make docs-drift
python3 /tmp/d-landing.py check | tail -1
python3 /tmp/d-batches.py d4 d5 d6 d7 d8 d9 d10 d11 | tail -1
git log -1 --format=%s
```

Expected: the four website checks pass; `89 32`; `Checked N objects: 0 problems.`; `No drift: every Hugo page is mapped, and every rewrite is current.`; `5 landing pages, 84 cards, 0 problems`; `8 problems`, one per wave 2 branch that doesn't exist yet; and the subject of the last merge, which every wave 2 prompt quotes.

- [ ] **Step 7: Start wave 2**

Send eight Agent calls in one message, for D4 to D11, with the prompt from Step 3 and the subject from Step 6.

- [ ] **Step 8: Review and merge wave 2**

Review and merge each batch as it reports, as in Steps 4 and 5.

- [ ] **Step 9: Run the final checks**

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome/website
export PATH=/tmp/ome-tools/bin:$PATH
rm -f worker-configuration.d.ts && pnpm lint && pnpm test && pnpm check && pnpm build
python3 -c 'import json; e = json.load(open(".svelte-kit/cloudflare/ome/search.json")); print(len(e), sum(x["status"] != "draft" for x in e))'
python3 -c 'import json; print("\n".join(e["route"] for e in json.load(open(".svelte-kit/cloudflare/ome/search.json"))))' > /tmp/d-routes.txt
pnpm preview > /tmp/ome-preview.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:4173/ome && break; sleep 1; done
while read -r r; do
  printf '%s %s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:4173/ome/$r")" "$(curl -s "http://localhost:4173/ome/$r" | grep -c 'This page is being rewritten')" "$r"
done < /tmp/d-routes.txt | grep -v '^200 0 ' || echo "all pages load and none is a draft"
pkill -f 'wrangler.js pages dev'; sleep 2; lsof -ti tcp:4173 | xargs kill 2>/dev/null
cd ..
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
make docs-drift
python3 /tmp/d-landing.py check | tail -1
git grep -l 'status: draft' -- website/src/lib/content || echo "no drafts"
git status --short
```

Expected: the four website checks pass; `89 89`; `all pages load and none is a draft`; `Checked N objects: 0 problems.`; `No drift: every Hugo page is mapped, and every rewrite is current.`; `5 landing pages, 84 cards, 0 problems`; `no drafts`; and `git status` prints nothing.

- [ ] **Step 10: Remove the worktrees**

```bash
cd /Users/simolin/go/src/github.com/ome-projects/ome
for w in .claude/worktrees/agent-*; do git worktree remove --force "$w"; done
git worktree prune
git for-each-ref --format='%(refname:short)' --merged docs/website-content 'refs/heads/worktree-agent-*' | xargs git branch -d
git worktree list && git branch --list 'docs/*' 'worktree-agent-*'
```

Expected: `git worktree list` shows only the main checkout, and the branches are `docs/site-redesign`, `docs/website-content` and the eleven `docs/website-dN` branches. Keep the batch branches: they're the heads of the content PRs.

- [ ] **Step 11: Report**

Tell the user:

- that every page is written, with the Step 9 output and the since label the batches saw;
- the PRs, in the order they can merge: the Foundation PR from `docs/site-redesign`, then D1 to D3, then D4 to D11. Each batch branch holds only its own commits on top of `docs/website-content` as its wave started. Once the PR before it has merged into `main`, rebase a batch with `git rebase --onto main <base> docs/website-dN`, where `<base>` is `git merge-base M^1 M^2` for the batch's merge commit `M` on `docs/website-content`. Wave 2 PRs link to headings on wave 1's pages, so they wait for wave 1's PRs;
- what the reviews found, what was fixed, and what wasn't and why;
- every contradiction the batches reported between the code and the Hugo pages, and every problem they found on pages they don't own;
- that `hack/krew/ome.yaml` line 22 links the old `https://ome-projects.github.io/ome/docs/tasks/kubectl-ome/` page, which should move to the new overview once the site is live;
- the prerequisites and findings carried over from C1's report.

Ask before pushing any branch or opening any PR.

### What a batch owns

A batch owns:

- its pages, listed in its task;
- in `website/redirects.json`, the `rewrittenFrom` of each entry whose `new` is one of its pages, which only `rewritten.py` writes;
- on the section landing pages, its own pages' cards, and only to match a `title` or `description` it changed.

Nothing else: not the nav, not another batch's pages, not the pages Phases B and C wrote, not the website's code, not `hack/`, not `site/` and no Go code. If you find a problem outside your batch, such as a wrong fact on another page, a broken example in `site/` or a bug in the code, list it in your report instead of fixing it. The orchestrator runs `batches.py` before merging, and it fails on any change outside these.

### Servers during Phase D

Up to eight batches run at the same time on one machine. Start a dev server only in your task's check step, on your batch's port from the [Phase D table](#phase-d-content), and stop it before the step ends. Port 5173 belongs to the orchestrator's live server; never stop it or use its port. Don't run `pnpm preview` or use port 4173; the orchestrator runs the preview build in its final checks.

Start, wait for and stop the server like this, with your port in place of 5181 and your batch in place of d1. Each task's check step has these lines already filled in:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5181 --strictPort > /tmp/d1-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5181/ome/ && break; sleep 1; done
# checks go here
pkill -f 'port 5181'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"
```

The second `pkill` stops the `workerd` process Vite starts, which outlives Vite otherwise. Both patterns match only your own processes. Don't run `pkill -f 'vite dev'`, `pkill -f 'pages dev'` or `pkill workerd`: they stop the other batches' servers and the live server.

For visual checks, use `meta browser` (run `meta browser --help` for its commands) to take screenshots at 1280 and 390 pixels wide. If it isn't available, say so in your report and rely on the HTML checks.

### Writing rules

These apply to every Phase D page. The pages B4 wrote are the models: read `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md` and `reference/kubectl-ome/rollout.md` under `website/src/lib/content/` before you write anything, and `contributing/writing-docs.md` for the authoring syntax.

- **Voice.** Second person, present tense, active voice: "OME mounts the claim read-only", not "The claim will be mounted". Keep paragraphs short, lead with what the reader does or gets, and leave out filler such as "simply", "easily", "seamlessly" and "powerful". Link a term to its concept page the first time a page uses it. Titles and headings use sentence case.
- **Page types.** A guide has Before you begin (the prerequisites box), numbered steps that each end with a check, Troubleshooting, Clean up and Next steps; leave out a section that has nothing to say, such as Clean up when the guide creates nothing. A concept page has an opening paragraph before the first `h2` that says what the thing is for, `h2`s by topic, and Next steps. A reference page has an opening paragraph, then facts in tables where they fit, and ends with Related pages; `kubectl ome` pages follow the [CLI page format](#cli-page-format) below.
- **Examples.** Complete and runnable, with concrete names instead of placeholders. Commands and their output go in separate blocks, `bash` for the command and `output` for what it prints. Where the default output is too wide to read, select columns so the output block shows exactly what the reader sees. Use the four callout types, never `> **Note:**` blockquotes. No images.
- **Accuracy.** The code on your branch is the truth. The Hugo pages under `site/content/en/docs/` are where the topics come from, not the facts; each page's step lists the errors already known in its Hugo page. Read the Go code behind every field, default, flag, state, condition, reason, event and message you write.
  - Take CLI output only from golden files and test expectations, and help text only from `--help` on a binary you built. Never copy the "Captured Moirai examples" in `cmd/kubectl-ome/README.md` (lines 674 to 917): they come from an internal cluster.
  - Annotation and label keys are often built from constants, such as `constants.OMEAPIGroupName + "/deploymentMode"`, so grep for the part after the slash as well as the full key.
  - Don't write counts that go stale, such as how many models a catalog has; show a command that lists them instead.
  - When the code contradicts the Hugo page, write what the code does, and note the contradiction for your report with the file and line on both sides.
- **Since.** v1.2.2, the latest release, is commit `5f1c4096`. `git show 5f1c4096:<path>` shows a file as released, and `git diff 5f1c4096 HEAD -- <paths>` shows what changed since. Behavior that isn't in v1.2.2 gets `since=v1.3` on its heading, or `since: v1.3` in the front matter when the whole page is new. A page with `since: v1.3` in its front matter puts no badges on its headings. A table row or a sentence can't carry a badge, so write "Since v1.3." in plain text there. Behavior that changed since v1.2.2 gets the badge and one sentence on what v1.2.2 did, so a reader on v1.2.2 isn't misled. Each page's step says what's already known; when the code says otherwise, follow the code and say so in your report.
- **Preview pages.** The five multi-cluster pages, `guides/multi-cluster/routing-health-probes.md`, `guides/multi-cluster/drain-a-workload-cluster.md`, `concepts/rollouts-and-traffic/traffic-map.md`, `reference/kubectl-ome/cluster.md` and `reference/kubectl-ome/placement.md`, get `status: preview` in place of `status: draft`. The page template shows the "In development" callout on them, so don't add another. No page presents multi-cluster as finished: the manager's multi-cluster reconcilers are alpha and off by default (`cmd/manager/main.go:257-258`).
- **Alpha.** Under the heading of each subcommand whose help says "Alpha", put `!!! note "Alpha"` with "This command is alpha. Its flags and behavior can change between releases." A page about an alpha API or a feature behind a flag says so in its opening paragraph, and says how to turn it on.
- **YAML.** Every block with `apiVersion` and `kind` goes through `go run ./hack/docs-examples`, which fails an object without `metadata.name` or `metadata.generateName` and dry-run creates the rest against the CRDs in envtest. The check runs no admission webhooks, so check each example against its kind's webhook in `pkg/webhook/admission/` and the rules in `pkg/validation/` yourself. An example meant to be rejected uses `yaml check=skip`, and the text says it's rejected and quotes the message. So does a config file with an `apiVersion` and `kind` that isn't applied to a cluster, such as a kubeconfig or a Kustomization. Never use `check=skip` to hide a broken example.
- **Links.** Link other pages with relative paths to their `.md` files, such as `../models/base-models.md#distribution`. Link to a heading only on a page that's already written on your branch; a draft has no headings yet, and the content check fails on anchors into drafts. Link to GitHub only for files that `git ls-files` lists, as `https://github.com/ome-projects/ome/blob/main/<path>`, or `tree/main/<path>` for a directory. Never link to the old site at `ome-projects.github.io/ome/docs`.
- **Front matter.** Keep each page's `title` and `description` unless they're wrong. If you change one, update the page's card on its section landing page to match, run `python3 /tmp/dN-landing.py check | tail -1`, and say so in your report. Remove `status: draft`, and add `since: v1.3` where the page's step says to.
- **Commits.** One per page, after the page's checks pass, per Ground rule 2, with the message in the page's step.

#### CLI page format

Every `reference/kubectl-ome/` page except the overview and Guarded actions follows B4's `rollout.md`:

- Front matter: `since: v1.3`, since the CLI isn't in v1.2.2.
- An opening that says what the command is for, a synopsis block, and for commands with subcommands a table of them.
- For commands with subcommands, one `h2` per subcommand, named in code, such as `` ## `status` ``. Each starts with a synopsis block and what the subcommand does. Under it, `h3`s with explicit ids, since every subcommand has the same ones: `### Flags {#status-flags}`, `### Output fields {#status-output-fields}` for subcommands that print a report, and `### Examples {#status-examples}`.
- For commands without subcommands, the `h2`s Flags, Output fields (when the command prints a report) and Examples, plus the sections the page's step names.
- A Flags table has the columns Flag, Default and Description, copied from `--help`, and is followed by "Plus the standard kubeconfig flags, such as `-n` and `--context`."
- `## Exit codes`: a table with the columns Code, Meaning and Returned by, from `pkg/cli/exitcode/exitcode.go` and the places the command returns each code.
- `## Related guides`: the guides that use the command, and Troubleshoot an InferenceService.

Build the binary once and read every command's help from it:

```bash
CGO_ENABLED=0 go build -o /tmp/dN-kubectl-ome ./cmd/kubectl-ome
/tmp/dN-kubectl-ome rollout status --help
```

The usage lines say `ome`, the binary's own name. The pages write `kubectl ome`, since kubectl runs it as a plugin. Reference pages describe commands; they don't walk through troubleshooting sessions. Those sequences belong in D7's Troubleshoot an InferenceService guide, which the reference pages link without an anchor.

### Writing a page

Each page step follows the same loop:

1. **Research.** Read the page's sources. Run `git show 5f1c4096:<path>` on the code behind each section to see whether v1.2.2 had it, and `git diff 5f1c4096 HEAD -- <paths>` to see what changed.
2. **Write.** Follow the page's outline: its `h2`s, in the order and with the names given, since the check step compares the heading ids. Add `h3`s where they help. If the code shows that the outline is wrong, such as a section about behavior that doesn't exist, change the outline, update the ids in your check step and say why in your report.
3. **Set `rewrittenFrom`.** Run `python3 /tmp/dN-rewritten.py <page>`, with the page's path relative to `website/src/lib/content`. It prints one line per Hugo page the page replaces, which must match the step's expected lines. New pages have no entry, and their steps skip this.
4. **Check the examples.** Run the YAML check from the repository root, and check the examples against the webhooks by hand:

   ```bash
   KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
   ```

   Expected: `Checked N objects: 0 problems.` In a sandbox, `go run` may warn that it can't write the Go stat cache; that's harmless.
5. **Run the website checks** from `website/`: `rm -f worker-configuration.d.ts && pnpm lint && pnpm test && pnpm check && pnpm build`.
6. **Run the pre-commit hooks** on the changed files: `SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files <files>`.
7. **Commit** the page and `website/redirects.json` with the step's message.

### Task D1: Getting Started and How OME works

**Branch:** `docs/website-d1`

**Goal:** Rewrite the five Getting Started pages and write the new How OME works page, so a new user can go from an empty cluster to a model answering requests.

**Files:**

- Rewrite: `website/src/lib/content/getting-started/introduction.md`
- Rewrite: `website/src/lib/content/getting-started/install.md`
- Rewrite: `website/src/lib/content/getting-started/serve-your-first-model.md`
- Rewrite: `website/src/lib/content/getting-started/pre-configured-models.md`
- Rewrite: `website/src/lib/content/getting-started/private-registries.md`
- Rewrite: `website/src/lib/content/concepts/architecture/how-ome-works.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 5 entries whose `new` is one of these pages, nothing else)
- Modify: `website/src/lib/content/getting-started/index.md` (the cards of Serve your first model and Pre-configured models and runtimes, whose description changes)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1 runs D1, D2 and D3 at the same time, so D2 and D3's pages are drafts on your branch: link them without anchors. You can link headings on the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`.

Getting Started is what new users read first. Keep these pages short and concrete, and leave depth to the concept pages you link. Where a table row covers something v1.2.2 doesn't have, such as an optional chart or an alpha kind, write "Since v1.3." in the row.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d1`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d1-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d1-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d1-landing.py check | tail -1
python3 /tmp/d1-batches.py d1
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d1 (D1): 0 files, 0 not D1's
  redirects.json: unchanged, so no page has a rewrittenFrom
  concepts/architecture/how-ome-works.md: still a draft
  getting-started/install.md: still a draft
  getting-started/introduction.md: still a draft
  getting-started/pre-configured-models.md: still a draft
  getting-started/private-registries.md: still a draft
  getting-started/serve-your-first-model.md: still a draft
7 problems
```

- [ ] **Step 3: Write the Introduction page**

`website/src/lib/content/getting-started/introduction.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/overview/_index.md`. Its known errors:
  - Line 70 presents multi-region support across Kubernetes clusters as a finished feature. The manager's multi-cluster reconcilers are alpha and off by default (`cmd/manager/main.go:257-258`).
  - Lines 24, 60 and 63 claim TPU support, GPU sharing and spot instances. Nothing in the code supports them; leave them out.
- `README.md`: the project summary and feature list, which you check against the code like the Hugo page.
- `cmd/manager/main.go`: the controllers and webhooks the manager starts, and the flags that turn features on, including the ones marked alpha.
- `pkg/apis/ome/v1beta1/`: the kinds OME defines. `git show 5f1c4096:pkg/apis/ome/v1beta1` lists the ones v1.2.2 had.
- `pkg/runtimeselector/` and `pkg/acceleratorclassselector/`: runtime selection and accelerator selection, the two things OME decides for the user.

Outline, `h2`s in order:

- **What OME does**: one short paragraph per capability the code has, each linking its concept page: models and where weights come from, serving runtimes and runtime selection, accelerator classes, InferenceServices and deployment modes, and rollouts and traffic. Say "Since v1.3." for capabilities v1.2.2 lacks.
- **How the pieces fit**: a few sentences from a model and a runtime to serving pods, linking How OME works for the details.
- **Alpha features**: a table of the features that are alpha or behind a manager flag, with what each does and how to turn it on. Multi-cluster is one of them, and is off by default.
- **Next steps**: Install OME, Serve your first model and How OME works.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-rewritten.py getting-started/introduction.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `overview/_index.md -> getting-started/introduction.md: 73626f86`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/getting-started/introduction.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Introduction page" -m "The introduction describes what OME does as the code has it, links each
capability to its concept page and lists the alpha features and how to
turn them on, instead of presenting multi-cluster as finished."
```

- [ ] **Step 4: Write the Install OME page**

`website/src/lib/content/getting-started/install.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/installation/_index.md`. Its known errors:
  - Lines 29 to 31 list the deployment modes as RawDeployment, MultiNode and PDDisaggregated and leave out OMENative. See `pkg/apis/ome/v1beta1/inference_service.go:22-33` and `pkg/constants/constants.go:521-528`: PDDisaggregated is a shape, not a mode. Link Deployment modes instead of listing them.
  - Line 37 asks for Kubernetes 1.27 or newer; `README.md:56` says 1.28.
- `README.md` (lines 56 to 110): the Kubernetes version, and the `helm upgrade --install` commands for the `ome-crd` and `ome` releases from `oci://ghcr.io/moirai-internal/charts/`, the source install, and the optional charts.
- `charts/ome-crd/` and `charts/ome-resources/`: what each chart installs. `charts/ome-resources/templates/ome-controller/deployment.yaml` names the controller Deployment `ome-controller-manager`, and `charts/ome-resources/templates/model-agent-daemonset/daemonset.yaml` names the DaemonSet `ome-model-agent-daemonset`.
- `charts/ome-resources/templates/ome-controller/certificate.yaml`: the webhook certificate that needs cert-manager. Confirm that nothing else provides it before you make cert-manager Step 1.
- `charts/ome-resources/templates/ome-controller/default-runtime.yaml`: the chart also installs a ClusterServingRuntime named `default-runtime`, new since v1.2.2. It has no `supportedModelFormats`, so runtime selection never picks it. Say what it is in one sentence, as far as the code shows.
- `charts/ome-scheduler/Chart.yaml`, `charts/ome-quota-manager/Chart.yaml`, `charts/ome-alfred/Chart.yaml` and `charts/ome-serving/Chart.yaml`: the optional charts and what each needs, such as the scheduler's `kubeVersion`.
- The `dep-crds` target in `Makefile`: the third-party CRDs OME can use (KEDA, Gateway API, Kueue and scheduler-plugins), and which features need each.
- `config/default/` and the `install` target in `Makefile`: the manifest install that the last section moves to Helm.

Before you write "Since v1.3." in the optional components table, check each chart and CRD with `git show 5f1c4096:charts` and `git show 5f1c4096:charts/ome-crd/templates`.

Outline, `h2`s in order:

- **Before you begin**: Kubernetes 1.28 or newer, kubectl, Helm and cluster-admin rights.
- **Optional components**: a table of the third-party components and optional charts, what each enables and which page covers it.
- **Step 1: Install cert-manager**: the install command and a check that its pods are running.
- **Step 2: Install the CRDs**: the `ome-crd` release, and a check that lists the OME CRDs.
- **Step 3: Install OME**: the `ome` release from the `ome-resources` chart.
- **Step 4: Verify the installation**: the pods of `ome-controller-manager` and `ome-model-agent-daemonset`, with output that selects the columns you can derive from the chart.
- **Install from source**: the local chart install from a checkout.
- **Troubleshooting**: what fails when cert-manager or the CRDs are missing, and how it shows.
- **Uninstall**: both releases in the right order, and what deleting the CRDs deletes.
- **Move a manifest install to the Helm charts**: what the Hugo page covers here, checked against `config/default/`.
- **Next steps**: Serve your first model and Pre-configured models.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-rewritten.py getting-started/install.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `installation/_index.md -> getting-started/install.md: 8fcac889`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/getting-started/install.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Install OME guide" -m "The guide installs cert-manager, the ome-crd chart and the ome release
in order, checks each step, lists the optional components and covers
uninstalling and moving from a manifest install."
```

- [ ] **Step 5: Write the Serve your first model page**

`website/src/lib/content/getting-started/serve-your-first-model.md` is a guide. Front matter: remove `status: draft`. Change its `description` to "Create a ClusterBaseModel, a ClusterServingRuntime and an InferenceService for a small model, then send your first request to it.", since the guide now applies the runtime and names it, and change its card on the Getting Started landing page (`website/src/lib/content/getting-started/index.md`) to match.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/deploy-inference-service.md`. Its known errors:
  - Line 33 shows an `ome-model-controller` pod, which doesn't exist, and the controller at 2/2. The chart deploys `ome-controller-manager` (`charts/ome-resources/templates/ome-controller/deployment.yaml:5`) and `ome-model-agent-daemonset` (`charts/ome-resources/templates/model-agent-daemonset/daemonset.yaml:5`).
  - Line 19 says the images come from `ghcr.io/sgl-project`; `charts/ome-resources/values.yaml:7` sets the hub to `ghcr.io/moirai-internal`.
  - It serves a gated Llama model, which needs a Hugging Face token. This guide uses Qwen3-0.6B, which isn't gated.
- `config/models/Qwen/Qwen3-0.6B.yaml`: the ClusterBaseModel `qwen3-0-6b`.
- `config/runtimes/srt/Qwen/qwen3-0-6b-rt.yaml`: the ClusterServingRuntime `srt-qwen3-0-6b`, its image and the resources it asks for.
- `config/samples/isvc/Qwen/qwen3-0-6b.yaml`: the sample Namespace and InferenceService.
- `pkg/runtimeselector/`: why a runtime with `autoSelect: false` is used only when a service names it.
- `pkg/modelagent/` and `pkg/apis/ome/v1beta1/model.go`: how the agent downloads the model, where on the node it writes, and the model states.
- `pkg/apis/ome/v1beta1/inference_service_status.go` and `pkg/controller/v1beta1/inferenceservice/status/`: the conditions and URL the service reports.
- `pkg/controller/v1beta1/inferenceservice/components/engine.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/`: the Service the request goes to, and its port.

`srt-qwen3-0-6b` has `autoSelect: false`, so OME never picks it on its own: Step 4 applies it from the catalog and Step 5 names it in the InferenceService. Check `config/runtimes/srt/Qwen/Qwen3-Embedding-0.6B-rt.yaml` as well. It sets `autoSelect: true` for `Qwen3ForCausalLM` from 0.5B to 1B, and its `metadata.name` is the same as `qwen3-embedding-0-6b-rt.yaml`'s, so a reader who applies the whole catalog can get an embedding runtime for this model. Report it; the catalog is outside your batch.

The catalog model sets `storage.path` to `/raid/models/Qwen/Qwen3-0.6B`. Check where the model agent can write on a node with the chart's defaults, and give the reader a path that works there. Say what the runtime needs from a node, such as a GPU, in Before you begin.

Outline, `h2`s in order:

- **Before you begin**: OME installed (link Install OME), kubectl access, and a node that meets the runtime's resource requests.
- **Step 1: Check that OME is running**: the two workloads, with output you can derive from the chart.
- **Step 2: Create the model**: a complete ClusterBaseModel for Qwen3-0.6B and `kubectl apply`.
- **Step 3: Watch the model become Ready**: the watch command with selected columns, and what the model agent does meanwhile.
- **Step 4: Create the runtime**: apply `srt-qwen3-0-6b` and say why the service has to name it.
- **Step 5: Create the InferenceService**: a complete InferenceService that names the model and the runtime.
- **Step 6: Watch the InferenceService become Ready**: the ready condition and the pods behind it.
- **Step 7: Send a request**: port-forward to the Service and send a chat completion request, then show the shape of the response.
- **Troubleshooting**: a table of what the reader sees when the model doesn't download, no node fits the runtime, or the pod doesn't start, and where to look for each.
- **Clean up**: delete the InferenceService, the runtime and the model, and say what happens to the files on the node.
- **Next steps**: Pre-configured models, Serving runtimes and InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-rewritten.py getting-started/serve-your-first-model.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/deploy-inference-service.md -> getting-started/serve-your-first-model.md: 1ee89e68`, then `Checked N objects: 0 problems.` Then `python3 /tmp/d1-landing.py check | tail -1` prints `5 landing pages, 84 cards, 0 problems`. Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/getting-started/serve-your-first-model.md website/redirects.json website/src/lib/content/getting-started/index.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Serve your first model guide" -m "The guide serves Qwen3-0.6B, which needs no Hugging Face token, with the
catalog runtime named explicitly, and checks the model, the service and
a first request step by step."
```

- [ ] **Step 6: Write the Pre-configured models and runtimes page**

`website/src/lib/content/getting-started/pre-configured-models.md` is a guide. Front matter: remove `status: draft`. Change its `description` to "Install the ome-serving Helm chart to deploy ready-made ClusterBaseModels, SGLang runtimes and InferenceServices from OME's model catalog.", since a count of models goes stale, and change its card on the Getting Started landing page (`website/src/lib/content/getting-started/index.md`) to match.

Sources:

- The Hugo page, for topics: `site/content/en/docs/installation/ome-serving.md`. Its known errors:
  - Lines 6 and 13 say the chart has over 160 models. `charts/ome-serving/values.yaml` has more, and its "All 165 models" comment on line 2 is stale too. Show a command that lists the models instead of a count, and report the comment.
- `charts/ome-serving/values.yaml` and `charts/ome-serving/templates/` (`basemodel.yaml`, `clusterbasemodel.yaml`, `clusterservingruntime.yaml`, `inferenceservice.yaml` and `_helpers.tpl`): every per-model option and what it renders.
- `charts/ome-serving/Chart.yaml` and the `ome-serving` install in `README.md`.
- `config/models/` and `config/runtimes/`: where the catalog comes from.
- `git diff 5f1c4096 HEAD -- charts/ome-serving`: which options are new since v1.2.2.

The chart's `routerImage` is on `fra.ocir.io`. Check whether readers can pull it, say so where PD mode needs it, and report it.

Outline, `h2`s in order:

- **Before you begin**: OME installed, and Helm.
- **Step 1: Pick models from the catalog**: a command that lists the model keys in the chart's values, with no counts.
- **Step 2: Install the chart**: enable two small models in a values file and install the release.
- **Step 3: Check the resources**: the models, runtimes and InferenceServices the release created.
- **Per-model options**: an `h3` each for scope, storage, installing only the model or only the runtime, runtime options, replicas, and PD mode.
- **Add a model outside the catalog**: the values a new entry needs.
- **Troubleshooting**: what the reader sees when a runtime doesn't match or an image can't be pulled.
- **Uninstall**: what uninstalling the release deletes, and what it leaves.
- **Next steps**: Serving runtimes and Install from a private registry.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-rewritten.py getting-started/pre-configured-models.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `installation/ome-serving.md -> getting-started/pre-configured-models.md: 042043ae`, then `Checked N objects: 0 problems.` Then `python3 /tmp/d1-landing.py check | tail -1` prints `5 landing pages, 84 cards, 0 problems`. Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/getting-started/pre-configured-models.md website/redirects.json website/src/lib/content/getting-started/index.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Pre-configured models guide" -m "The guide installs the ome-serving chart for chosen catalog models,
documents every per-model option and lists models with a command instead
of a count that goes stale."
```

- [ ] **Step 7: Write the Install from a private registry page**

`website/src/lib/content/getting-started/private-registries.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/installation/private-registries.md`. It has no known errors, but check every fact anyway.
- `charts/ome-resources/values.yaml`: the global hub, each image's repository and tag, per-image overrides such as the model agent's (about line 773) and the pull secrets (about line 6).
- `charts/ome-resources/templates/ome-controller/deployment.yaml` and `charts/ome-resources/templates/model-agent-daemonset/daemonset.yaml`: how the templates build each image reference.
- `charts/ome-resources/templates/ome-controller/configmap.yaml` and `pkg/controller/v1beta1/controllerconfig/configmap.go`: the images the controller puts into pods it creates, such as init containers and the metadata Job.
- `pkg/webhook/admission/pod/model_init_injector.go`, `pkg/webhook/admission/pod/fine_tuned_adapter_injector.go` and `pkg/webhook/admission/pod/serving_sidecar_injector.go`: the containers the pod webhook adds, and where their images come from.
- `charts/ome-serving/values.yaml`: the runtime images, and a `routerImage` on `fra.ocir.io`.
- `git diff 5f1c4096 HEAD -- charts/ome-resources/values.yaml charts/ome-resources/templates`: v1.2.2 had only part of the global registry settings. Mark what's new.

Outline, `h2`s in order:

- **Before you begin**: a registry the cluster can pull from, and the images copied into it.
- **How image references are built**: hub, repository and tag, and which setting wins.
- **Step 1: Point every image at your mirror**: a values file and the upgrade command.
- **Step 2: Supply pull credentials**: the Secret and the values that reference it.
- **Step 3: Check the rendered images**: a `helm template` or `kubectl get` command that lists every image, with output that selects columns.
- **Override a single image**: one image from a different registry.
- **Images OME pulls at runtime**: a table of the images the controller and webhooks add to pods, and the setting behind each.
- **Troubleshooting**: ImagePullBackOff on each workload and where the reference came from.
- **Next steps**: Install OME and Pre-configured models.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-rewritten.py getting-started/private-registries.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `installation/private-registries.md -> getting-started/private-registries.md: ed162ebd`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/getting-started/private-registries.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the private registry guide" -m "The guide points every OME image at a mirror, supplies pull credentials
and lists the images OME adds to pods at runtime, with the setting
behind each."
```

- [ ] **Step 8: Write the How OME works page**

`website/src/lib/content/concepts/architecture/how-ome-works.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `cmd/manager/main.go`: the controllers and webhooks the manager runs.
- `cmd/model-agent/main.go` and `pkg/modelagent/`: the node agent.
- `cmd/ome-agent/`: the agent that runs in Jobs and init containers.
- `pkg/controller/v1beta1/`: one directory per controller.
- `pkg/webhook/admission/`: the admission webhooks, including the pod mutator in `pkg/webhook/admission/pod/mutator.go`.
- `pkg/controller/v1beta1/inferenceservice/controller.go` and `pkg/controller/v1beta1/inferenceservice/components/`: from an InferenceService to workloads.
- `pkg/apis/ome/v1beta1/`: the kinds; `git show 5f1c4096:pkg/apis/ome/v1beta1` lists the ones v1.2.2 had.
- `AGENTS.md`: the repository's architecture summary.

Keep to what a new user needs to picture the system, and link each concept page for depth. Don't describe multi-cluster as finished.

Outline, `h2`s in order:

- **Components**: the manager with its controllers and webhooks, the model agent and the OME agent, and what each is responsible for.
- **Resources**: a table of the kinds, their scope and what each is for, with "Since v1.3." for alpha kinds v1.2.2 lacks.
- **From InferenceService to pods**: runtime selection, accelerator selection, the deployment mode and the workloads the controller creates.
- **How weights reach the nodes**: the model agent, the node labels and the init container, linking Base models.
- **Next steps**: Deployment modes, Serving runtimes and InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/architecture/how-ome-works.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the How OME works page" -m "A new concept page on the manager, the model agent and the OME agent,
the kinds OME defines, and how an InferenceService becomes serving pods."
```

- [ ] **Step 9: Check the pages**

Start the dev server on port 5181, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5181 --strictPort > /tmp/d1-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5181/ome/ && break; sleep 1; done
B=http://localhost:5181/ome
for p in getting-started/introduction getting-started/install getting-started/serve-your-first-model getting-started/pre-configured-models getting-started/private-registries concepts/architecture/how-ome-works; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in getting-started/introduction getting-started/install getting-started/serve-your-first-model getting-started/pre-configured-models getting-started/private-registries concepts/architecture/how-ome-works; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in getting-started/introduction getting-started/install getting-started/serve-your-first-model getting-started/pre-configured-models getting-started/private-registries concepts/architecture/how-ome-works; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
getting-started/introduction 200 0 0 0
getting-started/install 200 0 0 0
getting-started/serve-your-first-model 200 0 0 0
getting-started/pre-configured-models 200 0 0 0
getting-started/private-registries 200 0 0 0
concepts/architecture/how-ome-works 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
getting-started/introduction: what-ome-does how-the-pieces-fit alpha-features next-steps
getting-started/install: before-you-begin optional-components step-1-install-cert-manager step-2-install-the-crds step-3-install-ome step-4-verify-the-installation install-from-source troubleshooting uninstall move-a-manifest-install-to-the-helm-charts next-steps
getting-started/serve-your-first-model: before-you-begin step-1-check-that-ome-is-running step-2-create-the-model step-3-watch-the-model-become-ready step-4-create-the-runtime step-5-create-the-inferenceservice step-6-watch-the-inferenceservice-become-ready step-7-send-a-request troubleshooting clean-up next-steps
getting-started/pre-configured-models: before-you-begin step-1-pick-models-from-the-catalog step-2-install-the-chart step-3-check-the-resources per-model-options add-a-model-outside-the-catalog troubleshooting uninstall next-steps
getting-started/private-registries: before-you-begin how-image-references-are-built step-1-point-every-image-at-your-mirror step-2-supply-pull-credentials step-3-check-the-rendered-images override-a-single-image images-ome-pulls-at-runtime troubleshooting next-steps
concepts/architecture/how-ome-works: components resources from-inferenceservice-to-pods how-weights-reach-the-nodes next-steps
```

Then the since labels: nothing, since your pages have no badges. Last, `89 16`: the 16 written pages include your 6.

Take screenshots of Install OME, Serve your first model and How OME works at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5181'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 10: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d1-batches.py d1
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d1 (D1): 8 files, 0 not D1's
  overview/_index.md -> getting-started/introduction.md: 73626f86
  installation/_index.md -> getting-started/install.md: 8fcac889
  tasks/run-workloads/deploy-inference-service.md -> getting-started/serve-your-first-model.md: 1ee89e68
  installation/ome-serving.md -> getting-started/pre-configured-models.md: 042043ae
  installation/private-registries.md -> getting-started/private-registries.md: ed162ebd
0 problems
```

with one more file for each other landing page you changed. The file count includes the landing page your pages' new descriptions change. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 9 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D2: Deployment modes, fine-tuned weights and runtimes

**Branch:** `docs/website-d2`

**Goal:** Rewrite the two architecture pages, Fine-tuned weights and the four runtime concept pages.

**Files:**

- Rewrite: `website/src/lib/content/concepts/architecture/deployment-modes.md`
- Rewrite: `website/src/lib/content/concepts/architecture/omenative-update-strategies.md`
- Rewrite: `website/src/lib/content/concepts/models/fine-tuned-weights.md`
- Rewrite: `website/src/lib/content/concepts/runtimes/serving-runtimes.md`
- Rewrite: `website/src/lib/content/concepts/runtimes/runtime-inheritance.md`
- Rewrite: `website/src/lib/content/concepts/runtimes/runtime-revisions.md`
- Rewrite: `website/src/lib/content/concepts/runtimes/accelerator-classes.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1 runs D1, D2 and D3 at the same time, so D1 and D3's pages are drafts on your branch: link them without anchors. You can link headings on the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d2`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d2-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d2-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d2-landing.py check | tail -1
python3 /tmp/d2-batches.py d2
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d2 (D2): 0 files, 0 not D2's
  redirects.json: unchanged, so no page has a rewrittenFrom
  concepts/architecture/deployment-modes.md: still a draft
  concepts/architecture/omenative-update-strategies.md: still a draft
  concepts/models/fine-tuned-weights.md: still a draft
  concepts/runtimes/accelerator-classes.md: still a draft
  concepts/runtimes/runtime-inheritance.md: still a draft
  concepts/runtimes/runtime-revisions.md: still a draft
  concepts/runtimes/serving-runtimes.md: still a draft
8 problems
```

- [ ] **Step 3: Write the Deployment modes and OMENative page**

`website/src/lib/content/concepts/architecture/deployment-modes.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/omenative.md`. Its known errors:
  - The InferenceService at line 105 leaves out `runner.name` in `spec.engine.leader` and `spec.engine.worker`, which the CRD requires.
- `pkg/apis/ome/v1beta1/inference_service.go`: the doc comment on `spec.deploymentMode` (about line 10) and its enum.
- `pkg/constants/constants.go`: the mode values and `IsValid` (about line 515), and the `DeploymentMode` annotation key built from `OMEAPIGroupName` (about line 79).
- `pkg/controller/v1beta1/inferenceservice/utils/deployment.go`: the resolution order. `git show 5f1c4096:pkg/controller/v1beta1/inferenceservice/utils/deployment.go` shows that v1.2.2 had no spec field and resolved leader and worker to MultiNode.
- `pkg/controller/v1beta1/controllerconfig/configmap.go`: the operator-level default mode.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/` and `pkg/controller/v1beta1/workload/`: what OMENative creates and manages.
- `pkg/apis/ome/v1beta1/lifecycle_types.go`, `pkg/apis/ome/v1beta1/inference_service_status.go` and `pkg/controller/v1beta1/controllerconfig/omenativestatus.go`: what OMENative reports.

Leader and worker resolve to OMENative now, and resolved to MultiNode in v1.2.2. Say that in one sentence under How OME resolves the mode, with "Since v1.3." in the text.

Outline, `h2`s in order:

- **The deployment modes**: a table of RawDeployment, MultiNode, OMENative ("Since v1.3.") and the PD-disaggregated shape, with what each creates. Mention VirtualDeployment only as far as the code still acts on it.
- **How OME resolves the mode**: the per-component annotation, `spec.deploymentMode`, the leader and worker shape, the operator default and the fallback, in order.
- **The service-level mode** (with `{since=v1.3}`): `spec.deploymentMode`, its two accepted values, and why MultiNode and PDDisaggregated aren't accepted there.
- **OMENative** (with `{since=v1.3}`): what OMENative manages directly instead of Deployments and LeaderWorkerSets, including multi-node instances.
- **Opt in to OMENative** (with `{since=v1.3}`): a complete InferenceService that sets `spec.deploymentMode`, and the annotation for one component.
- **Observe OMENative in status** (with `{since=v1.3}`): the status fields and conditions OMENative writes.
- **Next steps**: OMENative update strategies, InferenceService and Gang scheduling.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/architecture/deployment-modes.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/omenative.md -> concepts/architecture/deployment-modes.md: b29d19db`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/architecture/deployment-modes.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Deployment modes page" -m "The page covers every deployment mode, the order OME resolves it in and
the new spec.deploymentMode field, and marks OMENative and the leader
and worker change as new since v1.2.2."
```

- [ ] **Step 4: Write the OMENative update strategies page**

`website/src/lib/content/concepts/architecture/omenative-update-strategies.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/omenative-update-strategies.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/lifecycle_types.go`: `UpdateStrategy`, its four types (about line 618), `InPlaceUpdateStrategy` and `Partition`.
- `pkg/validation/lifecycle.go`: what admission accepts.
- `pkg/controller/v1beta1/workload/revision/revision.go` and `pkg/controller/v1beta1/workload/drain/drain.go`: how each strategy replaces pods.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/`: how a rollout advances, and what happens when the strategy changes mid-rollout.
- `pkg/controller/v1beta1/controllerconfig/rollout.go`: the defaults.

Outline, `h2`s in order:

- **The four strategies**: an `h3` for each of SurgeThenDrain, RecreatePod, InPlaceIfPossible and InPlaceOnly: what it does to pods and capacity, and when to choose it.
- **Pacing with rollingUpdate**: how `partition` holds instances back, and the surge and unavailability limits.
- **Where the defaults come from**: the field, the runtime and the operator config, in order.
- **Changing the strategy mid-rollout**: what the controller does with a rollout already in progress.
- **Next steps**: Deployment modes, Rollout groups and Pause and resume a rollout.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/architecture/omenative-update-strategies.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/omenative-update-strategies.md -> concepts/architecture/omenative-update-strategies.md: ef61f471`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/architecture/omenative-update-strategies.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the OMENative update strategies page" -m "The page explains the four OMENative update strategies, pacing with
partition, where the defaults come from and what happens when the
strategy changes mid-rollout."
```

- [ ] **Step 5: Write the Fine-tuned weights page**

`website/src/lib/content/concepts/models/fine-tuned-weights.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/fine_tuned_weight.md`. Its known errors:
  - The FineTunedWeight at line 110 uses `spec.storage.storageKey`; the field is `spec.storage.key`.
- `pkg/apis/ome/v1beta1/model.go`: `FineTunedWeightSpec` (about line 383) and `FineTunedWeight` (about line 640).
- `pkg/controller/v1beta1/inferenceservice/components/engine.go`, `pkg/controller/v1beta1/inferenceservice/components/base.go` and `pkg/controller/v1beta1/inferenceservice/utils/utils.go`: how a service uses fine-tuned weights.
- `pkg/webhook/admission/pod/fine_tuned_adapter_injector.go`: how the adapter reaches the serving pod.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/modelconfig/modelconfig_reconciler.go`: what the controller records about the weights.
- `cmd/ome-agent/`: the agent that fetches the adapter.

Check whether anything writes a FineTunedWeight's status. If nothing does, say what the reader can observe instead, and rename Status and lifecycle to match.

Outline, `h2`s in order:

- **The FineTunedWeight resource**: what it is, how it relates to its base model, and one complete example.
- **Fields**: a table of the spec fields.
- **Where the weights come from**: the storage URI and credentials, linking Base models for the schemes.
- **Use fine-tuned weights in an InferenceService**: a complete service and what the pod gets.
- **Status and lifecycle**: what the reader can observe, and what deleting the resource does.
- **Next steps**: Base models and InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/models/fine-tuned-weights.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/fine_tuned_weight.md -> concepts/models/fine-tuned-weights.md: 8c31d399`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/models/fine-tuned-weights.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Fine-tuned weights page" -m "The page covers the FineTunedWeight fields, where the weights come from
and how an InferenceService uses them, with an example that uses
storage.key."
```

- [ ] **Step 6: Write the Serving runtimes page**

`website/src/lib/content/concepts/runtimes/serving-runtimes.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/serving_runtime.md`. Its known errors:
  - Five ClusterServingRuntimes fail the YAML check. Lines 20, 380 and 418 leave out `spec.engineConfig.runner.name`, and lines 146 and 197 leave out both it and `spec.supportedModelFormats[].modelFramework`. The CRD requires both.
- `pkg/apis/ome/v1beta1/servingruntime_types.go`: every field. `git diff 5f1c4096 HEAD -- pkg/apis/ome/v1beta1/servingruntime_types.go` shows that `scalingPolicy`, `modelCacheProviders`, `inheritanceChain` and `conditions` are new, and that the v1.2.2 `workers` spec with its `size` is gone.
- `pkg/runtimeselector/`: how OME matches and scores runtimes.
- `pkg/webhook/admission/servingruntime/servingruntime_webhook.go` and `pkg/validation/servingruntime.go`: what admission checks.
- `pkg/controller/v1beta1/servingruntime/status.go`: the status the runtime controller writes.
- `config/runtimes/`: the catalog, with SGLang runtimes under `srt/` and vLLM runtimes under `vllm/`.

Outline, `h2`s in order:

- **ServingRuntime and ClusterServingRuntime**: the two scopes and which services can use each.
- **Anatomy of a runtime**: formats, the engine, decoder and router configs, and model size ranges, with one complete example. Add `### Scaling policy {since=v1.3}` and `### Model cache providers {since=v1.3}`; the first says in one sentence that v1.2.2 used `workers` and `size`.
- **Per-accelerator configuration**: overrides per accelerator class.
- **How OME selects a runtime**: a short summary linking the matching references and Troubleshoot runtime selection.
- **The runtime catalog**: what `config/runtimes/` holds and how to apply part of it.
- **Status** (with `{since=v1.3}`): `inheritanceChain` and the conditions.
- **Next steps**: Runtime inheritance, Runtime revisions and Accelerator classes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/runtimes/serving-runtimes.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/serving_runtime.md -> concepts/runtimes/serving-runtimes.md: 1ee89e68`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/runtimes/serving-runtimes.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Serving runtimes page" -m "The page covers both runtime kinds, every part of a runtime spec, per-
accelerator overrides, the catalog and the new status fields, with
examples that pass the YAML check."
```

- [ ] **Step 7: Write the Runtime inheritance page**

`website/src/lib/content/concepts/runtimes/runtime-inheritance.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/runtime_inheritance.md`. Its known errors:
  - The ClusterServingRuntime at line 26 leaves out `spec.engineConfig.runner.name`, and the one at line 54 leaves out both it and `spec.supportedModelFormats[].modelFramework`. The CRD requires both, even on a profile meant only as a parent; check `pkg/webhook/admission/servingruntime/inheritance_validation.go` for what else a parent needs.
- `pkg/constants/constants.go`: `RuntimeInheritFromAnnotationKey` (about line 138).
- `pkg/runtimeinheritance/`: `resolve.go` (how parent names resolve), `merge.go` (what merges and how), `descendants.go` and `from_client.go`.
- `pkg/webhook/admission/servingruntime/inheritance_validation.go`: what admission checks, including chain limits.
- `pkg/controller/v1beta1/servingruntime/inheritance_controller.go`, `pkg/controller/v1beta1/servingruntime/status.go` and `pkg/controller/v1beta1/servingruntime/watches.go`: chain health in status, and when children are revisited.
- `charts/ome-resources/templates/ome-controller/default-runtime.yaml`: a runtime the chart installs with no model formats. Say whether it's meant as a parent only if the code or chart says so.

Outline, `h2`s in order:

- **Define a profile and inherit from it**: a complete parent and child that pass the YAML check.
- **What merges**: field by field, from `merge.go`.
- **Chain limits**: depth and cycles.
- **How parent names resolve**: namespaced and cluster-scoped parents.
- **What admission checks**: each rejection and its message.
- **Chain health in status**: `inheritanceChain` and the conditions.
- **When the merged spec applies**: when a parent change reaches running services, linking Runtime revisions.
- **Next steps**: Serving runtimes and Runtime revisions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/runtimes/runtime-inheritance.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/runtime_inheritance.md -> concepts/runtimes/runtime-inheritance.md: 08869f00`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/runtimes/runtime-inheritance.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Runtime inheritance page" -m "The page covers the inherit-from annotation, what merges, chain limits,
how parent names resolve, what admission checks and the chain status,
with examples that pass the YAML check."
```

- [ ] **Step 8: Write the Runtime revisions and pinning page**

`website/src/lib/content/concepts/runtimes/runtime-revisions.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/runtime-revision.md`. It has no known errors, but check every fact anyway.
- `pkg/runtimerevision/`: `hash.go`, `name.go` and `store.go`, for how revisions are named and stored.
- `pkg/controller/v1beta1/runtimerevision/plan.go` and `pkg/controller/v1beta1/runtimerevision/gc_controller.go`: pinning and garbage collection.
- `pkg/controller/v1beta1/inferenceservice/pinning.go`: how a service pins a revision.
- `pkg/webhook/admission/runtimerevision/webhook.go`: what admission protects.
- `pkg/cli/cmd/runtime/sync.go` and `pkg/cli/mutate/`: `kubectl ome runtime sync`, which is new since v1.2.2.
- `charts/ome-resources/templates/ome-controller/rbac/role.yaml`: the RBAC around revisions.

Outline, `h2`s in order:

- **Live and pinned runtimes**: what a service runs when the runtime changes.
- **How pinning works**: the revision objects, their names and what records the pin.
- **Roll forward to the latest runtime**: with `### kubectl ome runtime sync {since=v1.3}` for the command.
- **Pin a specific revision**: how and when.
- **Garbage collection**: what the collector keeps and deletes, and the flags that tune it.
- **RBAC**: who can read and change revisions.
- **Next steps**: Serving runtimes and kubectl ome runtime.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/runtimes/runtime-revisions.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/runtime-revision.md -> concepts/runtimes/runtime-revisions.md: c9ada638`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/runtimes/runtime-revisions.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Runtime revisions page" -m "The page explains live and pinned runtimes, how revisions are named and
stored, rolling forward, pinning, garbage collection and the RBAC around
revisions."
```

- [ ] **Step 9: Write the Accelerator classes page**

`website/src/lib/content/concepts/runtimes/accelerator-classes.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/accelerator_class.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/accelerator_class.go`: every field and the status.
- `pkg/controller/v1beta1/acceleratorclass/controller.go`: how the controller finds matching nodes and what it writes.
- `pkg/acceleratorclassselector/`: how a service gets a class, and the policies.
- `pkg/runtimeselector/`: accelerator-class matching between runtimes and services.

Outline, `h2`s in order:

- **The AcceleratorClass resource**: what it describes and why it's cluster-scoped.
- **Write an AcceleratorClass**: a complete example for a common GPU.
- **How OME finds matching nodes**: the discovery rules.
- **Status fields**: a table.
- **Why a class reports zero nodes**: the causes and how to tell them apart.
- **Delete a class**: what happens to services that use it.
- **How services choose a class**: a short summary linking Select accelerators and Runtime accelerator-class matching.
- **Next steps**: Select accelerators and Serving runtimes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-rewritten.py concepts/runtimes/accelerator-classes.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/accelerator_class.md -> concepts/runtimes/accelerator-classes.md: 5bff284e`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/runtimes/accelerator-classes.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Accelerator classes page" -m "The page covers the AcceleratorClass fields, how OME finds matching
nodes, the status, why a class can report zero nodes and how services
choose a class."
```

- [ ] **Step 10: Check the pages**

Start the dev server on port 5182, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5182 --strictPort > /tmp/d2-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5182/ome/ && break; sleep 1; done
B=http://localhost:5182/ome
for p in concepts/architecture/deployment-modes concepts/architecture/omenative-update-strategies concepts/models/fine-tuned-weights concepts/runtimes/serving-runtimes concepts/runtimes/runtime-inheritance concepts/runtimes/runtime-revisions concepts/runtimes/accelerator-classes; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in concepts/architecture/deployment-modes concepts/architecture/omenative-update-strategies concepts/models/fine-tuned-weights concepts/runtimes/serving-runtimes concepts/runtimes/runtime-inheritance concepts/runtimes/runtime-revisions concepts/runtimes/accelerator-classes; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in concepts/architecture/deployment-modes concepts/architecture/omenative-update-strategies concepts/models/fine-tuned-weights concepts/runtimes/serving-runtimes concepts/runtimes/runtime-inheritance concepts/runtimes/runtime-revisions concepts/runtimes/accelerator-classes; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
concepts/architecture/deployment-modes 200 0 0 4
concepts/architecture/omenative-update-strategies 200 0 0 1
concepts/models/fine-tuned-weights 200 0 0 0
concepts/runtimes/serving-runtimes 200 0 0 3
concepts/runtimes/runtime-inheritance 200 0 0 1
concepts/runtimes/runtime-revisions 200 0 0 1
concepts/runtimes/accelerator-classes 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
concepts/architecture/deployment-modes: the-deployment-modes how-ome-resolves-the-mode the-service-level-mode omenative opt-in-to-omenative observe-omenative-in-status next-steps
concepts/architecture/omenative-update-strategies: the-four-strategies pacing-with-rollingupdate where-the-defaults-come-from changing-the-strategy-mid-rollout next-steps
concepts/models/fine-tuned-weights: the-finetunedweight-resource fields where-the-weights-come-from use-fine-tuned-weights-in-an-inferenceservice status-and-lifecycle next-steps
concepts/runtimes/serving-runtimes: servingruntime-and-clusterservingruntime anatomy-of-a-runtime per-accelerator-configuration how-ome-selects-a-runtime the-runtime-catalog status next-steps
concepts/runtimes/runtime-inheritance: define-a-profile-and-inherit-from-it what-merges chain-limits how-parent-names-resolve what-admission-checks chain-health-in-status when-the-merged-spec-applies next-steps
concepts/runtimes/runtime-revisions: live-and-pinned-runtimes how-pinning-works roll-forward-to-the-latest-runtime pin-a-specific-revision garbage-collection rbac next-steps
concepts/runtimes/accelerator-classes: the-acceleratorclass-resource write-an-acceleratorclass how-ome-finds-matching-nodes status-fields why-a-class-reports-zero-nodes delete-a-class how-services-choose-a-class next-steps
```

Then the since labels: one line, `10 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `10 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 17`: the 17 written pages include your 7.

Take screenshots of Deployment modes, OMENative update strategies and Serving runtimes at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5182'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 11: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d2-batches.py d2
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d2 (D2): 8 files, 0 not D2's
  concepts/omenative.md -> concepts/architecture/deployment-modes.md: b29d19db
  concepts/omenative-update-strategies.md -> concepts/architecture/omenative-update-strategies.md: ef61f471
  concepts/fine_tuned_weight.md -> concepts/models/fine-tuned-weights.md: 8c31d399
  concepts/serving_runtime.md -> concepts/runtimes/serving-runtimes.md: 1ee89e68
  concepts/runtime_inheritance.md -> concepts/runtimes/runtime-inheritance.md: 08869f00
  concepts/runtime-revision.md -> concepts/runtimes/runtime-revisions.md: c9ada638
  concepts/accelerator_class.md -> concepts/runtimes/accelerator-classes.md: 5bff284e
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 10 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D3: Serving, rollout and traffic concepts

**Branch:** `docs/website-d3`

**Goal:** Rewrite the four serving concept pages and the five rollout and traffic concept pages.

**Files:**

- Rewrite: `website/src/lib/content/concepts/serving/inference-services.md`
- Rewrite: `website/src/lib/content/concepts/serving/gang-scheduling.md`
- Rewrite: `website/src/lib/content/concepts/serving/autoscaler-policy.md`
- Rewrite: `website/src/lib/content/concepts/serving/benchmarks.md`
- Rewrite: `website/src/lib/content/concepts/rollouts-and-traffic/rollout-policy.md`
- Rewrite: `website/src/lib/content/concepts/rollouts-and-traffic/rollout-groups.md`
- Rewrite: `website/src/lib/content/concepts/rollouts-and-traffic/traffic-policy.md`
- Rewrite: `website/src/lib/content/concepts/rollouts-and-traffic/traffic-map.md`
- Rewrite: `website/src/lib/content/concepts/rollouts-and-traffic/ingress.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 9 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1 runs D1, D2 and D3 at the same time, so D1 and D2's pages are drafts on your branch: link them without anchors. You can link headings on the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`.

Six of these pages are about features v1.2.2 doesn't have: their steps say `since: v1.3`. Several are alpha or behind a manager flag; each says so in its opening paragraph and says how to turn it on.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d3`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d3-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d3-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d3-landing.py check | tail -1
python3 /tmp/d3-batches.py d3
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d3 (D3): 0 files, 0 not D3's
  redirects.json: unchanged, so no page has a rewrittenFrom
  concepts/rollouts-and-traffic/ingress.md: still a draft
  concepts/rollouts-and-traffic/rollout-groups.md: still a draft
  concepts/rollouts-and-traffic/rollout-policy.md: still a draft
  concepts/rollouts-and-traffic/traffic-map.md: still a draft
  concepts/rollouts-and-traffic/traffic-policy.md: still a draft
  concepts/serving/autoscaler-policy.md: still a draft
  concepts/serving/benchmarks.md: still a draft
  concepts/serving/gang-scheduling.md: still a draft
  concepts/serving/inference-services.md: still a draft
10 problems
```

- [ ] **Step 3: Write the InferenceService page**

`website/src/lib/content/concepts/serving/inference-services.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/inference_service.md`. Its known errors:
  - Lines 215 to 224 say leader and worker select MultiNode (LeaderWorkerSet), and that the decoder supports only RawDeployment or MultiNode. In `pkg/controller/v1beta1/inferenceservice/utils/deployment.go:10-40`, leader and worker resolve to OMENative, and MultiNode comes only from the annotation or the operator default.
  - The InferenceService at line 243 leaves out `runner.name` in `spec.engine.leader` and `spec.engine.worker`, which the CRD requires.
  - The InferenceService at line 499 uses `spec.engine.kedaConfig`, which was removed after v1.2.2 in commit 3361e051, together with `scaleMetric` and `scaleTarget`.
- `pkg/apis/ome/v1beta1/inference_service.go` and `pkg/apis/ome/v1beta1/component.go`: every field. `git diff 5f1c4096 HEAD --` on both shows the new fields (`deploymentMode`, `overlays`, `placement`, `rollout`, `routing`, `scalingPolicy`, `topologySpread`, `traffic`, `autoscaler`, `autoscalerPolicyRef`, `lifecycle` and `servicePortAppProtocols`) and the removed `kedaConfig`, `scaleMetric` and `scaleTarget`.
- `pkg/apis/ome/v1beta1/inference_service_status.go` and `pkg/controller/v1beta1/inferenceservice/status/`: the status.
- `pkg/controller/v1beta1/inferenceservice/controller.go` and `pkg/controller/v1beta1/inferenceservice/components/`: the engine, decoder and router.
- `pkg/webhook/admission/isvc/inference_service_validation.go` and `pkg/validation/isvc.go`: what admission checks.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler/`: how scaling works now.

Outline, `h2`s in order:

- **A minimal InferenceService**: a complete example with a model and nothing else.
- **Components**: the engine, decoder and router, and when each exists.
- **Model and runtime references**: how the service names its model and, optionally, its runtime.
- **How OME picks the deployment mode**: a short summary linking Deployment modes.
- **Multi-node serving**: leader and worker, with a complete example that passes the YAML check.
- **Accelerator selection**: a short summary linking Select accelerators.
- **Scaling** (with `{since=v1.3}`): `scalingPolicy`, `autoscaler` and `autoscalerPolicyRef`, and one sentence that v1.2.2 used `kedaConfig`, `scaleTarget` and `scaleMetric`, which are gone.
- **Rollouts and traffic** (with `{since=v1.3}`): `rollout`, `traffic` and `routing` in brief, linking their concept pages.
- **Status**: the conditions, URLs and per-component status.
- **Next steps**: Deployment modes, Autoscaler policy and Rollout groups.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/serving/inference-services.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/inference_service.md -> concepts/serving/inference-services.md: 2656a655`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/serving/inference-services.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the InferenceService page" -m "The page covers the InferenceService components, references, multi-node
serving, scaling, rollouts and status, with the kedaConfig removal and
the OMENative default for leader and worker."
```

- [ ] **Step 4: Write the Gang scheduling page**

`website/src/lib/content/concepts/serving/gang-scheduling.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/gang_scheduling.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/workload/podgroup/podgroup.go` and `pkg/controller/v1beta1/workload/podgroup/observe.go`: when OME creates a PodGroup and what it watches.
- `pkg/controller/v1beta1/workload/gang/gang.go` and `pkg/controller/v1beta1/workload/gang/inventory.go`: the gang itself.
- `pkg/controller/v1beta1/controllerconfig/configmap.go`: `GangScheduleTimeout` and its default.
- `charts/ome-scheduler/`: the gang-aware scheduler OME ships.

The PodGroup kind comes from scheduler-plugins. The YAML check skips kinds whose CRDs aren't installed, so a PodGroup example isn't checked: check it against the CRD by hand.

Outline, `h2`s in order:

- **One PodGroup per multi-pod instance**: when OME creates it, its name and its `minMember`.
- **When the PodGroup CRD is missing**: what OME does and reports.
- **Bring a gang-aware scheduler**: the OME scheduler or another, and how pods name it.
- **Bound the gang schedule timeout**: the setting, its default and what happens at the timeout.
- **Next steps**: Deployment modes and Use the OME scheduler.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/serving/gang-scheduling.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/gang_scheduling.md -> concepts/serving/gang-scheduling.md: 582ab9b2`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/serving/gang-scheduling.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Gang scheduling page" -m "The page explains when OME creates a PodGroup, what happens without the
CRD, which schedulers work and how the gang schedule timeout bounds a
stuck instance."
```

- [ ] **Step 5: Write the Autoscaler policy page**

`website/src/lib/content/concepts/serving/autoscaler-policy.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/autoscaler_policy.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/autoscalerpolicy_types.go`, `pkg/apis/ome/v1beta1/autoscaler.go` and `pkg/apis/ome/v1beta1/autoscaler_status.go`.
- `pkg/controller/v1beta1/controllerconfig/autoscalerpolicy.go`, `pkg/controller/v1beta1/controllerconfig/metricproviders.go` and `cmd/manager/main.go`: how to turn the feature on, and the metric providers.
- `pkg/autoscalerpolicy/render/`: how a policy renders into autoscaler objects, and what it validates.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler/`: attaching, resolving and precedence between inline settings and the policy.
- `pkg/controller/v1beta1/autoscalerpolicy/status_controller.go`: conditions and status.
- `pkg/webhook/admission/autoscalerpolicy/validator.go` and `pkg/validation/autoscalerpolicy.go`: what admission checks, and what happens on delete.

Outline, `h2`s in order:

- **Turn on the feature**: the flag or setting and its default.
- **Define a policy**: a complete example.
- **Metric providers**: the providers and their settings.
- **Attach a policy to an InferenceService**: `autoscalerPolicyRef` with a complete example.
- **Inline settings win**: the precedence rules.
- **Fail-closed behavior**: what happens when the policy is missing or invalid.
- **Conditions and status**: on the policy and on the service.
- **Delete a policy**: what happens to services that reference it.
- **Next steps**: InferenceService and kubectl ome autoscale.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/serving/autoscaler-policy.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/autoscaler_policy.md -> concepts/serving/autoscaler-policy.md: 1838140a`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/serving/autoscaler-policy.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Autoscaler policy page" -m "The page covers turning the feature on, defining and attaching a policy,
metric providers, precedence, fail-closed behavior, status and deletion."
```

- [ ] **Step 6: Write the Benchmarks page**

`website/src/lib/content/concepts/serving/benchmarks.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/benchmark.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/benchmark_job.go`: every field and the status.
- `pkg/controller/v1beta1/benchmark/controller.go`, `pkg/controller/v1beta1/benchmark/reconcilers/job/job.go` and `pkg/controller/v1beta1/benchmark/utils/utils.go`: the Job OME creates and how it runs.
- `pkg/webhook/admission/benchmark/benchmark_webhook.go`: what admission checks.

Outline, `h2`s in order:

- **The BenchmarkJob resource**: what it measures, with a complete example.
- **Endpoints**: an InferenceService or an external endpoint.
- **Traffic scenarios**: the scenario syntax and what each shape means.
- **Where results go**: the output locations, linking Benchmark output storage.
- **How a benchmark runs**: the Job, its pod and its lifetime.
- **Status**: the states and fields.
- **Next steps**: Run benchmarks and Benchmark output storage.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/serving/benchmarks.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/benchmark.md -> concepts/serving/benchmarks.md: 8de82852`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/serving/benchmarks.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Benchmarks page" -m "The page covers the BenchmarkJob fields, endpoints, traffic scenarios,
where results go, how the Job runs and the status it reports."
```

- [ ] **Step 7: Write the Rollout policy page**

`website/src/lib/content/concepts/rollouts-and-traffic/rollout-policy.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/rollout_policy.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/rolloutpolicy_types.go`.
- `pkg/rolloutpolicy/compose.go` and `pkg/rolloutpolicy/digest.go`: how the policy combines with inline settings, and when the reference is resolved.
- `pkg/controller/v1beta1/rolloutpolicy/status_controller.go`: conditions and status.
- `pkg/webhook/admission/rolloutpolicy/validator.go`, `pkg/validation/rolloutpolicy.go` and `pkg/validation/rolloutpolicyref.go`: what admission checks.
- `pkg/controller/v1beta1/controllerconfig/rollout.go` and `cmd/manager/main.go`: how to turn the feature on.

Outline, `h2`s in order:

- **Turn on the feature**: the flag or setting and its default.
- **Define a policy**: a complete example.
- **Attach a policy to a rollout group**: the reference, with a complete service.
- **When the reference is resolved**: and what a later policy change does to a rollout in progress.
- **Inline settings win**: the precedence rules.
- **Fail-closed parking**: what happens when the policy is missing or invalid.
- **Conditions and status**: on the policy and on the service.
- **Delete a policy**: what happens to groups that reference it.
- **Next steps**: Rollout groups and Canary progression.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/rollouts-and-traffic/rollout-policy.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/rollout_policy.md -> concepts/rollouts-and-traffic/rollout-policy.md: 8c392f08`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/rollouts-and-traffic/rollout-policy.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Rollout policy page" -m "The page covers turning the feature on, defining and attaching a policy,
when references resolve, precedence, fail-closed parking, status and
deletion."
```

- [ ] **Step 8: Write the Rollout groups page**

`website/src/lib/content/concepts/rollouts-and-traffic/rollout-groups.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/rollout_groups.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/rollout_types.go`: groups, `groupOrdering` and canary settings.
- `pkg/rollout/rollout.go` and `pkg/rollout/canary_unit.go`.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/`: how groups roll.
- `pkg/validation/canary.go` and `pkg/webhook/admission/isvc/inference_service_validation.go`: what admission rejects.

Outline, `h2`s in order:

- **Anatomy of a group**: a complete example with two groups.
- **Roll components one at a time**: the order and what gates the next group.
- **What groupOrdering promises**: and what it doesn't.
- **Canary groups roll whole units**: what a unit is.
- **What admission rejects**: each rejection and its message.
- **Next steps**: Rollout policy, Canary progression and Pause and resume a rollout.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/rollouts-and-traffic/rollout-groups.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/rollout_groups.md -> concepts/rollouts-and-traffic/rollout-groups.md: fe19dbe3`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/rollouts-and-traffic/rollout-groups.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Rollout groups page" -m "The page explains rollout groups, the order components roll in, what
groupOrdering promises, canary units and what admission rejects."
```

- [ ] **Step 9: Write the Traffic policy page**

`website/src/lib/content/concepts/rollouts-and-traffic/traffic-policy.md` is a concept page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/traffic_policy.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/traffic_types.go` and `pkg/apis/ome/v1beta1/traffic_status_types.go`.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/`: `intent.go`, `reconciler.go`, `translator.go`, `unsupported.go`, the translators under `translators/` (Envoy Gateway, Istio and the no-op) and `status/status.go`.
- `pkg/constants/traffic_capabilities.go` and `pkg/validation/traffic_capabilities.go`: what each translator supports.
- `pkg/validation/traffic.go`: what admission checks, and per-component overrides.

Outline, `h2`s in order:

- **How OME applies the policy**: intent, translation and the objects each translator writes.
- **Choose a load-balancing algorithm**: the algorithms, with an example.
- **Session affinity with ConsistentHash**: the hash sources, with an example.
- **Endpoint override**: what it does and when to use it.
- **Defaults when you declare nothing**: what OME sets.
- **Translator support**: a table of features by translator.
- **Read the traffic status**: the fields under `status.traffic`.
- **Conflicts and per-component overrides**: hand-written policies and component-level settings.
- **Next steps**: Traffic annotations and kubectl ome traffic.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/rollouts-and-traffic/traffic-policy.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/traffic_policy.md -> concepts/rollouts-and-traffic/traffic-policy.md: 13b2fd1b`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/rollouts-and-traffic/traffic-policy.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Traffic policy page" -m "The page covers how OME translates a traffic policy for each gateway,
load balancing, session affinity, endpoint override, defaults,
translator support and the traffic status."
```

- [ ] **Step 10: Write the Traffic map page**

`website/src/lib/content/concepts/rollouts-and-traffic/traffic-map.md` is a concept page. Front matter: replace `status: draft` with `status: preview`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/traffic_map.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/trafficmap_types.go`.
- `pkg/controller/v1beta1/placement/trafficmap_source.go` and `pkg/controller/v1beta1/placement/routing/`: where the map comes from and how weights are computed (`weight.go`, `capacity.go`, `prober.go`, `publisher.go`).
- `pkg/controller/v1beta1/placement/endpoint/trafficmap_publisher.go` and `pkg/controller/v1beta1/placement/endpoint/gatewayapi_trafficmap.go`: how the map reaches the gateway.
- `cmd/manager/main.go:257-258`: the multi-cluster reconcilers are alpha and off by default.

Outline, `h2`s in order:

- **Where it comes from**: which controller writes it and from what.
- **Read the routing table**: the fields, with an example read from the types.
- **Why a cluster's weight is zero**: each cause.
- **Conditions**: a table.
- **Staleness checks**: how consumers tell the map is out of date.
- **Next steps**: Configure routing health probes and Drain a workload cluster.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/rollouts-and-traffic/traffic-map.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/traffic_map.md -> concepts/rollouts-and-traffic/traffic-map.md: 3257b0b2`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/rollouts-and-traffic/traffic-map.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Traffic map page" -m "The page explains where the traffic map comes from, how to read its
routing table, why a weight is zero, its conditions and its staleness
checks, as an in-development feature."
```

- [ ] **Step 11: Write the Ingress and external access page**

`website/src/lib/content/concepts/rollouts-and-traffic/ingress.md` is a concept page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/concepts/ingress.md`. Its known errors:
  - It uses the annotation `ome.io/deployment-mode` (around lines 30 and 55); the key is `ome.io/deploymentMode` (`pkg/constants/constants.go:79`).
  - It calls the decoder a post-processing step with `/v1/decoder/` endpoints (lines 93 to 107 and 175 to 186). The decoder is the decode half of PD-disaggregated serving (`pkg/apis/ome/v1beta1/inference_service.go:44-50`).
  - Five InferenceServices fail the YAML check: lines 25, 50, 204, 217 and 242 use `spec.engine.model`, and some also use `spec.engine.parallelism` and `spec.engine.resources`, none of which exist.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/`: `reconciler.go`, the builders (`httproute_builder.go`, `ingress_builder.go`, `backendtrafficpolicy_builder.go` and `endpoints.go`), the strategies and the domain and path services.
- `pkg/controller/v1beta1/controllerconfig/configmap.go`: `IngressConfig` and its defaults.
- `pkg/constants/constants.go`: the ingress annotations.

Outline, `h2`s in order:

- **What OME creates for external access**: HTTPRoutes or Ingresses, and what decides.
- **Routing to components**: which paths reach the router, engine and decoder.
- **Hostnames and URLs**: how OME builds them, and the status fields that report them.
- **Send requests**: a request through the gateway.
- **Cluster-local services**: services with no external route.
- **Per-service overrides**: the annotations.
- **Security**: what OME does and doesn't do about TLS and authentication.
- **Next steps**: Configure ingress and Choose a Gateway API host scheme.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-rewritten.py concepts/rollouts-and-traffic/ingress.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `concepts/ingress.md -> concepts/rollouts-and-traffic/ingress.md: c6198139`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/concepts/rollouts-and-traffic/ingress.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Ingress concept page" -m "The page explains what OME creates for external access, how requests
reach each component, how hostnames are built and the per-service
overrides, with examples that pass the YAML check."
```

- [ ] **Step 12: Check the pages**

Start the dev server on port 5183, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5183 --strictPort > /tmp/d3-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5183/ome/ && break; sleep 1; done
B=http://localhost:5183/ome
for p in concepts/serving/inference-services concepts/serving/gang-scheduling concepts/serving/autoscaler-policy concepts/serving/benchmarks concepts/rollouts-and-traffic/rollout-policy concepts/rollouts-and-traffic/rollout-groups concepts/rollouts-and-traffic/traffic-policy concepts/rollouts-and-traffic/traffic-map concepts/rollouts-and-traffic/ingress; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in concepts/serving/inference-services concepts/serving/gang-scheduling concepts/serving/autoscaler-policy concepts/serving/benchmarks concepts/rollouts-and-traffic/rollout-policy concepts/rollouts-and-traffic/rollout-groups concepts/rollouts-and-traffic/traffic-policy concepts/rollouts-and-traffic/traffic-map concepts/rollouts-and-traffic/ingress; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in concepts/serving/inference-services concepts/serving/gang-scheduling concepts/serving/autoscaler-policy concepts/serving/benchmarks concepts/rollouts-and-traffic/rollout-policy concepts/rollouts-and-traffic/rollout-groups concepts/rollouts-and-traffic/traffic-policy concepts/rollouts-and-traffic/traffic-map concepts/rollouts-and-traffic/ingress; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
concepts/serving/inference-services 200 0 0 2
concepts/serving/gang-scheduling 200 0 0 1
concepts/serving/autoscaler-policy 200 0 0 1
concepts/serving/benchmarks 200 0 0 0
concepts/rollouts-and-traffic/rollout-policy 200 0 0 1
concepts/rollouts-and-traffic/rollout-groups 200 0 0 1
concepts/rollouts-and-traffic/traffic-policy 200 0 0 1
concepts/rollouts-and-traffic/traffic-map 200 0 1 1
concepts/rollouts-and-traffic/ingress 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
concepts/serving/inference-services: a-minimal-inferenceservice components model-and-runtime-references how-ome-picks-the-deployment-mode multi-node-serving accelerator-selection scaling rollouts-and-traffic status next-steps
concepts/serving/gang-scheduling: one-podgroup-per-multi-pod-instance when-the-podgroup-crd-is-missing bring-a-gang-aware-scheduler bound-the-gang-schedule-timeout next-steps
concepts/serving/autoscaler-policy: turn-on-the-feature define-a-policy metric-providers attach-a-policy-to-an-inferenceservice inline-settings-win fail-closed-behavior conditions-and-status delete-a-policy next-steps
concepts/serving/benchmarks: the-benchmarkjob-resource endpoints traffic-scenarios where-results-go how-a-benchmark-runs status next-steps
concepts/rollouts-and-traffic/rollout-policy: turn-on-the-feature define-a-policy attach-a-policy-to-a-rollout-group when-the-reference-is-resolved inline-settings-win fail-closed-parking conditions-and-status delete-a-policy next-steps
concepts/rollouts-and-traffic/rollout-groups: anatomy-of-a-group roll-components-one-at-a-time what-groupordering-promises canary-groups-roll-whole-units what-admission-rejects next-steps
concepts/rollouts-and-traffic/traffic-policy: how-ome-applies-the-policy choose-a-load-balancing-algorithm session-affinity-with-consistenthash endpoint-override defaults-when-you-declare-nothing translator-support read-the-traffic-status conflicts-and-per-component-overrides next-steps
concepts/rollouts-and-traffic/traffic-map: where-it-comes-from read-the-routing-table why-a-clusters-weight-is-zero conditions staleness-checks next-steps
concepts/rollouts-and-traffic/ingress: what-ome-creates-for-external-access routing-to-components hostnames-and-urls send-requests cluster-local-services per-service-overrides security next-steps
```

Then the since labels: one line, `8 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `8 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 19`: the 19 written pages include your 9.

Take screenshots of InferenceService, Traffic policy and Traffic map at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5183'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 13: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d3-batches.py d3
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d3 (D3): 10 files, 0 not D3's
  concepts/inference_service.md -> concepts/serving/inference-services.md: 2656a655
  concepts/gang_scheduling.md -> concepts/serving/gang-scheduling.md: 582ab9b2
  concepts/autoscaler_policy.md -> concepts/serving/autoscaler-policy.md: 1838140a
  concepts/benchmark.md -> concepts/serving/benchmarks.md: 8de82852
  concepts/rollout_policy.md -> concepts/rollouts-and-traffic/rollout-policy.md: 8c392f08
  concepts/rollout_groups.md -> concepts/rollouts-and-traffic/rollout-groups.md: fe19dbe3
  concepts/traffic_policy.md -> concepts/rollouts-and-traffic/traffic-policy.md: 13b2fd1b
  concepts/traffic_map.md -> concepts/rollouts-and-traffic/traffic-map.md: 3257b0b2
  concepts/ingress.md -> concepts/rollouts-and-traffic/ingress.md: c6198139
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 12 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D4: Runtime selection guides and matching references

**Branch:** `docs/website-d4`

**Goal:** Rewrite the three runtime selection guides and the four matching references, so a reader can steer runtime and accelerator selection and diagnose a failed one.

**Files:**

- Rewrite: `website/src/lib/content/guides/deploy-models/select-accelerators.md`
- Rewrite: `website/src/lib/content/guides/deploy-models/reference-a-runtime-explicitly.md`
- Rewrite: `website/src/lib/content/guides/deploy-models/troubleshoot-runtime-selection.md`
- Rewrite: `website/src/lib/content/reference/matching/model-version-matching.md`
- Rewrite: `website/src/lib/content/reference/matching/runtime-accelerator-class-matching.md`
- Rewrite: `website/src/lib/content/reference/matching/runtime-deployment-mode-matching.md`
- Rewrite: `website/src/lib/content/reference/matching/diffusion-pipeline-runtime-matching.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

`pkg/runtimeselector/` changed a lot since v1.2.2: `git diff --stat 5f1c4096 HEAD -- pkg/runtimeselector` shows how much. Read `pkg/runtimeselector/matcher.go` and `pkg/runtimeselector/errors.go` once for the whole batch, and quote every exclusion reason and error message exactly as the code builds it.

The four references are lookup pages: each states one rule and names the function that implements it. The guides link them instead of repeating the rules.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d4`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d4-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d4-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d4-landing.py check | tail -1
python3 /tmp/d4-batches.py d4
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d4 (D4): 0 files, 0 not D4's
  redirects.json: unchanged, so no page has a rewrittenFrom
  guides/deploy-models/reference-a-runtime-explicitly.md: still a draft
  guides/deploy-models/select-accelerators.md: still a draft
  guides/deploy-models/troubleshoot-runtime-selection.md: still a draft
  reference/matching/diffusion-pipeline-runtime-matching.md: still a draft
  reference/matching/model-version-matching.md: still a draft
  reference/matching/runtime-accelerator-class-matching.md: still a draft
  reference/matching/runtime-deployment-mode-matching.md: still a draft
8 problems
```

- [ ] **Step 3: Write the Select accelerators page**

`website/src/lib/content/guides/deploy-models/select-accelerators.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/select-accelerators.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/inference_service.go`: `acceleratorSelector` on the spec (about line 68), its types and policies (about lines 155 to 226), and `acceleratorOverride` on a component (about lines 307 and 403).
- `pkg/acceleratorclassselector/selector.go` and `pkg/acceleratorclassselector/policy_helpers.go`: `GetAcceleratorClass` (about line 48), where a component's class comes from, and how each policy ranks the candidates (`getAcceleratorClassByPolicy`, about line 113). `pkg/acceleratorclassselector/README.md` summarizes it; check it against the code.
- `pkg/apis/ome/v1beta1/servingruntime_types.go`: the runtime's `acceleratorRequirements` (about line 244), which lists its candidate classes, and the per-class `acceleratorConfig` (about line 66).
- `pkg/apis/ome/v1beta1/accelerator_class.go`: the class fields the policies compare.
- `pkg/controller/v1beta1/inferenceservice/controller.go` (about lines 660 to 720): selection for each component, and the `AcceleratorClassError` it reports.
- `pkg/controller/v1beta1/inferenceservice/components/base.go` and `pkg/controller/v1beta1/inferenceservice/utils/merging.go`: how the class shapes the pod, such as its node affinity, its resources and the runtime's per-class config.
- `pkg/apis/ome/v1beta1/inference_service_status.go`: where the selected class is reported.
- `pkg/constants/constants.go:32-35` and `pkg/runtimeselector/matcher.go` (about line 234): the `ome.io/accelerator-class` annotation. The runtime matcher reads it and `pkg/acceleratorclassselector/` doesn't, so it steers which runtime matches, not which class a component gets. Check both before you say what it does.
- `pkg/cli/cmd/accelerator/explain.go` and `pkg/cli/acceleratorprojection/project.go`: `kubectl ome accelerator explain`, which is new since v1.2.2.

Everything on this page except `kubectl ome accelerator explain` existed in v1.2.2, so the page has no since badges. Where Step 4 uses the command, write "Since v1.3." after it, and give the `kubectl get` command that reads the same status field for v1.2.2.

Outline, `h2`s in order:

- **Before you begin**: OME installed, at least one AcceleratorClass that matches nodes (link Accelerator classes), and a runtime that lists classes.
- **How selection works**: where a component's class comes from, in the order `pkg/acceleratorclassselector/selector.go` checks the sources, such as `acceleratorOverride`, `acceleratorSelector` and the runtime's candidates, and how the policy breaks ties. Link Runtime accelerator-class matching for how a class affects which runtime matches.
- **Step 1: List the AcceleratorClasses**: a `kubectl get` command that shows each class and how many nodes match it, with selected columns.
- **Step 2: Check the runtime's candidates**: read `acceleratorRequirements` from the runtime the service uses.
- **Step 3: Pin a class or choose a policy**: an `h3` for pinning one class and one for choosing a policy, each with a complete InferenceService, and a table of the policies and what each prefers.
- **Step 4: Check the selected class**: the status field that reports it, read with `kubectl get`, and `kubectl ome accelerator explain` for why OME chose it.
- **Override the class for one component**: `acceleratorOverride` on the engine or decoder, with an example.
- **How the class shapes the pod**: what the class adds to the pod, such as node affinity and resources, and the runtime's per-class overrides.
- **Troubleshooting**: `AcceleratorClassError` and the other failures, with the message for each and where it shows.
- **Clean up**: delete the example services.
- **Next steps**: Accelerator classes, Runtime accelerator-class matching and kubectl ome accelerator.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py guides/deploy-models/select-accelerators.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/select-accelerators.md -> guides/deploy-models/select-accelerators.md: d2ab568f`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/deploy-models/select-accelerators.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Select accelerators guide" -m "The guide lists the accelerator classes and a runtime's candidates, pins
a class or picks a policy, checks the selected class and overrides it
for one component."
```

- [ ] **Step 4: Write the Reference a runtime explicitly page**

`website/src/lib/content/guides/deploy-models/reference-a-runtime-explicitly.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/reference-a-runtime-explicitly.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/inference_service.go`: `spec.runtime` (about line 62) and `ServingRuntimeRef` (about lines 520 to 548), with `name`, `kind` (default `ClusterServingRuntime`), `apiGroup`, `autoSync` (default true) and `revision`.
- `pkg/runtimeselector/selector.go`: `GetRuntime` (about line 229), which resolves the name, and `ValidateRuntime` (about line 123).
- `pkg/runtimeselector/errors.go`: the errors a named runtime can produce, such as `RuntimeNotFoundError` and `RuntimeDisabledError`.
- `pkg/webhook/admission/isvc/inference_service_validation.go` (about lines 583 to 640): what admission checks for a named runtime, which mismatches are warnings and which are errors, and the pin checks in `validateRuntimePin`.
- `pkg/controller/v1beta1/inferenceservice/controller.go`: the `RuntimeCompatibilityAdvisory` event (about lines 456 to 490) and what happens when the named runtime is missing (about lines 1100 to 1135).
- `pkg/apis/ome/v1beta1/inference_service_status.go` (about lines 290 to 296): the `RuntimeReady` condition, which is new since v1.2.2.

Quote each admission warning and error exactly. A runtime that doesn't declare support for the model is only a warning, since the name was explicit. A sharded model on a runtime without a model cache provider is an error, and that check is new since v1.2.2: write "Since v1.3." in its row.

Check what `autoSync` and `revision` do before you mention them, and link Runtime revisions for pinning instead of covering it here.

Outline, `h2`s in order:

- **Before you begin**: OME installed, a model, and the runtime you want, applied.
- **Step 1: Find a compatible runtime**: list the runtimes and the formats and sizes each declares, and link Troubleshoot runtime selection for why OME wouldn't pick one on its own.
- **Step 2: Name the runtime in the InferenceService**: a complete InferenceService with `spec.runtime.name`, and `kind` for a namespaced ServingRuntime.
- **Step 3: Check the runtime OME used**: the status field or condition that reports it, read with `kubectl get`.
- **How OME resolves the name**: the lookup by kind and namespace, and what happens when the runtime is disabled.
- **What OME checks when you apply**: a table of each admission warning and error, with its exact text.
- **If the runtime goes missing**: the `RuntimeNotFound` event, what happens to running pods and how the service recovers. Add `### The RuntimeReady condition {since=v1.3}` for the condition that reports it.
- **Format mismatches at reconcile time**: the `RuntimeCompatibilityAdvisory` event, and what it means for a runtime named explicitly.
- **Clean up**: delete the example service.
- **Next steps**: Troubleshoot runtime selection, Serving runtimes and Runtime revisions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py guides/deploy-models/reference-a-runtime-explicitly.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/reference-a-runtime-explicitly.md -> guides/deploy-models/reference-a-runtime-explicitly.md: 5a4e8a86`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/deploy-models/reference-a-runtime-explicitly.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the explicit runtime guide" -m "The guide names a runtime in an InferenceService, checks which runtime
OME used and covers name resolution, the admission warnings and errors,
a missing runtime and the RuntimeReady condition."
```

- [ ] **Step 5: Write the Troubleshoot runtime selection page**

`website/src/lib/content/guides/deploy-models/troubleshoot-runtime-selection.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/troubleshoot-runtime-selection.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/inferenceservice/controller.go` (about lines 1100 to 1135): what the controller does when no runtime matches, and the `RuntimeNotFound` event.
- `pkg/apis/ome/v1beta1/inference_service_status.go` (about lines 290 to 296): the `RuntimeReady` condition. v1.2.2 had only the event.
- `pkg/runtimeselector/errors.go`: `NoRuntimeFoundError` (about line 31), which lists each excluded runtime as "name (reason)".
- `pkg/runtimeselector/matcher.go`: every exclusion reason, starting at `getFormatMismatchReason` (about line 312).
- `pkg/runtimeselector/model_cache.go`: the model-cache-provider rule, which is new since v1.2.2.
- `pkg/runtimeselector/selector.go` and `pkg/runtimeselector/README.md`: how candidates are gathered and scored, and what `autoSelect` does.
- `pkg/webhook/admission/isvc/inference_service_validation.go` (about line 624): the admission error when no runtime supports the model.

The `RuntimeReady` condition is new since v1.2.2, so Step 1 has a since badge and says that on v1.2.2 the event in Step 2 is all there is.

Outline, `h2`s in order:

- **Before you begin**: kubectl access to the service's namespace.
- **How a selection failure shows up**: what the reader sees on the service, in its events and in its pods when no runtime matches.
- **Step 1: Read the RuntimeReady condition** (with `{since=v1.3}`): the command, and the condition's reasons and messages.
- **Step 2: Read the RuntimeNotFound event**: the command, and the event text with its list of excluded runtimes.
- **Step 3: Read the exclusion reasons**: a table of every reason, what it means and how to fix it. The model-cache-provider row says "Since v1.3."
- **Step 4: Check autoSelect**: why a matching runtime with `autoSelect: false` is skipped, and the two fixes: set it, or name the runtime (link Reference a runtime explicitly).
- **Step 5: Verify recovery**: what the service shows once a runtime matches.
- **Next steps**: the matching references and Serving runtimes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py guides/deploy-models/troubleshoot-runtime-selection.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/troubleshoot-runtime-selection.md -> guides/deploy-models/troubleshoot-runtime-selection.md: b9d20451`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/deploy-models/troubleshoot-runtime-selection.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write Troubleshoot runtime selection" -m "The guide reads the RuntimeReady condition and the RuntimeNotFound
event, explains every exclusion reason and autoSelect, and checks that
the service recovers."
```

- [ ] **Step 6: Write the Model version matching page**

`website/src/lib/content/reference/matching/model-version-matching.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/model-version-matching.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/model.go`: the `version` and `operator` fields on `ModelFormat` (about line 22) and `ModelFrameworkSpec` (about line 42), and `RuntimeSelectorOperator` (about line 94).
- `pkg/apis/ome/v1beta1/servingruntime_types.go`: where a runtime declares versions.
- `pkg/runtimeselector/matcher.go`: `compareVersionsWithOperator` (about line 500) and its callers.
- `pkg/modelver/util.go`: how versions are parsed and compared.

Outline, `h2`s in order:

- **Where versions and operators are declared**: on the model and on the runtime, for the format and the framework.
- **When versions are compared**: which side's operator applies, and what an unset version or operator means.
- **Accepted version formats**: what `pkg/modelver/util.go` parses, with examples.
- **Operators**: a table of each operator and what it matches.
- **When ordering comparisons are refused**: versions that can't be ordered, and what the matcher does with them.
- **Examples**: model and runtime pairs, and whether each matches.
- **Related pages**: Troubleshoot runtime selection and Serving runtimes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py reference/matching/model-version-matching.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/model-version-matching.md -> reference/matching/model-version-matching.md: 07c83415`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/matching/model-version-matching.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Model version matching page" -m "The reference covers where versions and operators are declared, when
they are compared, the accepted formats, each operator and when an
ordering comparison is refused."
```

- [ ] **Step 7: Write the Runtime accelerator-class matching page**

`website/src/lib/content/reference/matching/runtime-accelerator-class-matching.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/runtime-accelerator-class-matching.md`. Its known errors:
  - The ClusterServingRuntime at line 47 leaves out `spec.supportedModelFormats[].modelFramework`, which the CRD requires.
- `pkg/runtimeselector/matcher.go`: `compareAcceleratorClass` (about line 226), and where it reads the service's class (about line 234).
- `pkg/apis/ome/v1beta1/servingruntime_types.go`: `acceleratorRequirements` (about line 244).
- `pkg/apis/ome/v1beta1/inference_service.go`: `acceleratorSelector` and `acceleratorOverride`.
- `pkg/constants/constants.go:32-35`: the `ome.io/accelerator-class` annotation. The matcher and the controller's watch mapper (`pkg/controller/v1beta1/inferenceservice/controller.go`, about line 1775) read it; `pkg/acceleratorclassselector/` doesn't.
- `pkg/acceleratorclassselector/selector.go`: how the class a component gets is chosen, separately from matching.
- `pkg/controller/v1beta1/inferenceservice/controller.go` (about line 456): where a mismatch shows for a runtime named explicitly.

Outline, `h2`s in order:

- **Where a service names a class**: the annotation and the selector fields, and which of them the matcher reads.
- **The matching rule**: from `compareAcceleratorClass`, including what happens when the runtime lists no classes or the service names none.
- **Declare both sides**: a complete ClusterServingRuntime and InferenceService that pass the YAML check.
- **Where a rejection shows up**: the exclusion reason on auto-selection, and the warning or event for a runtime named explicitly.
- **Matching versus the class a component gets**: why a runtime can match and the component still get a different class, linking Select accelerators.
- **Related pages**: Select accelerators, Accelerator classes and Troubleshoot runtime selection.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py reference/matching/runtime-accelerator-class-matching.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/runtime-accelerator-class-matching.md -> reference/matching/runtime-accelerator-class-matching.md: 30eca893`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/matching/runtime-accelerator-class-matching.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the accelerator-class matching page" -m "The reference explains where a service names an accelerator class, the
matching rule, where a rejection shows up and how matching differs from
the class a component gets."
```

- [ ] **Step 8: Write the Runtime deployment-mode matching page**

`website/src/lib/content/reference/matching/runtime-deployment-mode-matching.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/runtime-deployment-mode-matching.md`. Its known errors:
  - The ClusterServingRuntime at line 47 leaves out `spec.supportedModelFormats[].modelFramework`, which the CRD requires.
- `pkg/runtimeselector/matcher.go` (about lines 663 to 790): how the runtime's mode and the service's mode are compared.
- `pkg/constants/constants.go`: the `DeploymentMode` annotation key (about line 79) and `IsValid` (about lines 521 to 528).
- `pkg/apis/ome/v1beta1/inference_service.go`: `spec.deploymentMode`, which is new since v1.2.2.
- `pkg/apis/ome/v1beta1/servingruntime_types.go`: where a runtime declares its mode. Check whether anything besides the annotation does.
- `pkg/controller/v1beta1/inferenceservice/utils/deployment.go`: how a component's mode is resolved, which is separate from matching.

A runtime is rejected only when both sides declare a valid mode and the modes differ, and the decoder counts only when `spec.decoder` is set; confirm both in `pkg/runtimeselector/matcher.go`. OMENative and `spec.deploymentMode` are new since v1.2.2: write "Since v1.3." where they appear.

Outline, `h2`s in order:

- **The matching rule**: when a runtime's mode rejects it, and when the modes aren't compared at all.
- **Valid mode values**: a table of the values `IsValid` accepts, with "Since v1.3." for OMENative.
- **Declare a mode on a runtime**: a complete ClusterServingRuntime with the annotation, which passes the YAML check.
- **Declare a mode on an InferenceService**: the annotation, and `spec.deploymentMode` ("Since v1.3.").
- **Where a rejection shows up**: the exclusion reason and its exact text.
- **Matching versus the mode a component runs**: why matching can pass and a component still run in another mode, linking Deployment modes.
- **Related pages**: Deployment modes and Troubleshoot runtime selection.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py reference/matching/runtime-deployment-mode-matching.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/runtime-deployment-mode-matching.md -> reference/matching/runtime-deployment-mode-matching.md: 60c70f45`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/matching/runtime-deployment-mode-matching.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the deployment-mode matching page" -m "The reference explains when a runtime's deployment mode rejects it, the
valid values, how each side declares a mode and how matching differs
from the mode a component runs."
```

- [ ] **Step 9: Write the Diffusion pipeline runtime matching page**

`website/src/lib/content/reference/matching/diffusion-pipeline-runtime-matching.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/diffusion-pipeline-runtime-matching.md`. It has no known errors, but check every fact anyway.
- `pkg/runtimeselector/matcher.go`: `compareDiffusionPipeline` (about line 411).
- `pkg/apis/ome/v1beta1/servingruntime_types.go` and `pkg/apis/ome/v1beta1/model.go`: where a runtime declares the pipelines it supports, and where a model records its pipeline.
- `pkg/modelconfig/diffusion.go`, `pkg/modelparser/config_parser.go` and `pkg/modelparser/model_metadata.go`: how the model agent reads a diffusion model's pipeline.
- `pkg/validation/compatibility.go`: what admission checks.

Outline, `h2`s in order:

- **Where the model metadata comes from**: which files the parser reads, and which model field it fills.
- **Declare supported pipelines in a runtime**: a complete ClusterServingRuntime that passes the YAML check.
- **Matching rules**: from `compareDiffusionPipeline`, including a model with no recorded pipeline and a runtime that lists none.
- **Troubleshooting**: the exclusion reason, and how to read the pipeline a model recorded.
- **Related pages**: Troubleshoot runtime selection and Serving runtimes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-rewritten.py reference/matching/diffusion-pipeline-runtime-matching.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/diffusion-pipeline-runtime-matching.md -> reference/matching/diffusion-pipeline-runtime-matching.md: 5b3b7c0f`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/matching/diffusion-pipeline-runtime-matching.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the diffusion pipeline matching page" -m "The reference explains where a diffusion model's pipeline comes from,
how a runtime declares the pipelines it supports, the matching rules and
how to troubleshoot a mismatch."
```

- [ ] **Step 10: Check the pages**

Start the dev server on port 5184, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5184 --strictPort > /tmp/d4-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5184/ome/ && break; sleep 1; done
B=http://localhost:5184/ome
for p in guides/deploy-models/select-accelerators guides/deploy-models/reference-a-runtime-explicitly guides/deploy-models/troubleshoot-runtime-selection reference/matching/model-version-matching reference/matching/runtime-accelerator-class-matching reference/matching/runtime-deployment-mode-matching reference/matching/diffusion-pipeline-runtime-matching; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/deploy-models/select-accelerators guides/deploy-models/reference-a-runtime-explicitly guides/deploy-models/troubleshoot-runtime-selection reference/matching/model-version-matching reference/matching/runtime-accelerator-class-matching reference/matching/runtime-deployment-mode-matching reference/matching/diffusion-pipeline-runtime-matching; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/deploy-models/select-accelerators guides/deploy-models/reference-a-runtime-explicitly guides/deploy-models/troubleshoot-runtime-selection reference/matching/model-version-matching reference/matching/runtime-accelerator-class-matching reference/matching/runtime-deployment-mode-matching reference/matching/diffusion-pipeline-runtime-matching; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/deploy-models/select-accelerators 200 0 0 0
guides/deploy-models/reference-a-runtime-explicitly 200 0 0 1
guides/deploy-models/troubleshoot-runtime-selection 200 0 0 1
reference/matching/model-version-matching 200 0 0 0
reference/matching/runtime-accelerator-class-matching 200 0 0 0
reference/matching/runtime-deployment-mode-matching 200 0 0 0
reference/matching/diffusion-pipeline-runtime-matching 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/deploy-models/select-accelerators: before-you-begin how-selection-works step-1-list-the-acceleratorclasses step-2-check-the-runtimes-candidates step-3-pin-a-class-or-choose-a-policy step-4-check-the-selected-class override-the-class-for-one-component how-the-class-shapes-the-pod troubleshooting clean-up next-steps
guides/deploy-models/reference-a-runtime-explicitly: before-you-begin step-1-find-a-compatible-runtime step-2-name-the-runtime-in-the-inferenceservice step-3-check-the-runtime-ome-used how-ome-resolves-the-name what-ome-checks-when-you-apply if-the-runtime-goes-missing format-mismatches-at-reconcile-time clean-up next-steps
guides/deploy-models/troubleshoot-runtime-selection: before-you-begin how-a-selection-failure-shows-up step-1-read-the-runtimeready-condition step-2-read-the-runtimenotfound-event step-3-read-the-exclusion-reasons step-4-check-autoselect step-5-verify-recovery next-steps
reference/matching/model-version-matching: where-versions-and-operators-are-declared when-versions-are-compared accepted-version-formats operators when-ordering-comparisons-are-refused examples related-pages
reference/matching/runtime-accelerator-class-matching: where-a-service-names-a-class the-matching-rule declare-both-sides where-a-rejection-shows-up matching-versus-the-class-a-component-gets related-pages
reference/matching/runtime-deployment-mode-matching: the-matching-rule valid-mode-values declare-a-mode-on-a-runtime declare-a-mode-on-an-inferenceservice where-a-rejection-shows-up matching-versus-the-mode-a-component-runs related-pages
reference/matching/diffusion-pipeline-runtime-matching: where-the-model-metadata-comes-from declare-supported-pipelines-in-a-runtime matching-rules troubleshooting related-pages
```

Then the since labels: one line, `2 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `2 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 39`: the 39 written pages include your 7.

Take screenshots of Select accelerators, Troubleshoot runtime selection and Runtime deployment-mode matching at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5184'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 11: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d4-batches.py d4
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d4 (D4): 8 files, 0 not D4's
  tasks/run-workloads/select-accelerators.md -> guides/deploy-models/select-accelerators.md: d2ab568f
  tasks/run-workloads/reference-a-runtime-explicitly.md -> guides/deploy-models/reference-a-runtime-explicitly.md: 5a4e8a86
  tasks/run-workloads/troubleshoot-runtime-selection.md -> guides/deploy-models/troubleshoot-runtime-selection.md: b9d20451
  reference/model-version-matching.md -> reference/matching/model-version-matching.md: 07c83415
  reference/runtime-accelerator-class-matching.md -> reference/matching/runtime-accelerator-class-matching.md: 30eca893
  reference/runtime-deployment-mode-matching.md -> reference/matching/runtime-deployment-mode-matching.md: 60c70f45
  reference/diffusion-pipeline-runtime-matching.md -> reference/matching/diffusion-pipeline-runtime-matching.md: 5b3b7c0f
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 10 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D5: Networking guides and traffic annotations

**Branch:** `docs/website-d5`

**Goal:** Rewrite the six networking guides and the Traffic annotations reference, so a reader can expose services through Ingress or Gateway API and tune their routes.

**Files:**

- Rewrite: `website/src/lib/content/guides/networking/configure-route-timeouts.md`
- Rewrite: `website/src/lib/content/guides/networking/set-service-app-protocols.md`
- Rewrite: `website/src/lib/content/guides/networking/configure-ingress.md`
- Rewrite: `website/src/lib/content/guides/networking/gateway-host-schemes.md`
- Rewrite: `website/src/lib/content/guides/networking/multiple-gateways.md`
- Rewrite: `website/src/lib/content/guides/networking/namespace-gateways.md`
- Rewrite: `website/src/lib/content/reference/api/traffic-annotations.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

The ingress settings live in the `ingress` key of the `inferenceservice-config` ConfigMap, which the chart renders from `ome.controller.ingressGateway` in `charts/ome-resources/values.yaml` through `charts/ome-resources/templates/ome-controller/configmap.yaml`. Show both: the Helm values the reader sets, and the ConfigMap key they become.

v1.2.2 had the settings `ingressGateway`, `ingressService`, `omeIngressGateway`, `ingressDomain`, `ingressClassName`, `additionalIngressDomains`, `domainTemplate`, `urlScheme`, `pathTemplate`, `disableIstioVirtualHost`, `disableIngressCreation` and `enableGatewayAPI`, and the per-service annotations for the domain, domain template, additional domains, URL scheme, path template, Istio virtual host and ingress creation. Every other setting and annotation on these pages is new, including `ome.io/ingress-gateway`. Check each with `git show 5f1c4096:pkg/controller/v1beta1/controllerconfig/configmap.go` and `git show 5f1c4096:pkg/constants/constants.go`.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d5`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d5-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d5-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d5-landing.py check | tail -1
python3 /tmp/d5-batches.py d5
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d5 (D5): 0 files, 0 not D5's
  redirects.json: unchanged, so no page has a rewrittenFrom
  guides/networking/configure-ingress.md: still a draft
  guides/networking/configure-route-timeouts.md: still a draft
  guides/networking/gateway-host-schemes.md: still a draft
  guides/networking/multiple-gateways.md: still a draft
  guides/networking/namespace-gateways.md: still a draft
  guides/networking/set-service-app-protocols.md: still a draft
  reference/api/traffic-annotations.md: still a draft
8 problems
```

- [ ] **Step 3: Write the Configure route timeouts page**

`website/src/lib/content/guides/networking/configure-route-timeouts.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/configure-route-timeouts.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/controllerconfig/configmap.go` (about lines 204 to 211): `defaultRouteTimeoutSeconds`. Its comment says a value of zero or less means no route timeout; check that against the builder.
- `charts/ome-resources/values.yaml` (about lines 670 to 679) and `charts/ome-resources/templates/ome-controller/configmap.yaml` (about lines 32 and 33): the Helm value, and how it renders.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`: `defaultTimeout` (about lines 109 to 123), which turns the setting and a component's `timeoutSeconds` into the route's timeout.
- `pkg/apis/ome/v1beta1/component.go`: the per-component `timeoutSeconds` (about line 18), which v1.2.2 had.
- `git show 5f1c4096:pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`: v1.2.2 set a fixed 60-second `DefaultTimeout` (about line 29).
- `pkg/constants/traffic_annotations.go` (about lines 27 to 33): the connection-timeout annotations, which set a different timeout.

The chart's default of 0 renders a route timeout of `0s`, which turns the timeout off. A null value leaves the field out, so Envoy Gateway's 15-second default applies. v1.2.2 always set 60 seconds. Check all three in the builder, and say them plainly. The `configmap.go` comment contradicts the builder: report it.

Outline, `h2`s in order:

- **Before you begin**: OME installed with Gateway API routing (link Configure ingress), and Helm.
- **How OME resolves the timeout** (with `{since=v1.3}`): a component's `timeoutSeconds`, then `defaultRouteTimeoutSeconds`, then the gateway's own default, with what 0 and null do, and one sentence that v1.2.2 always set 60 seconds.
- **Step 1: Set a cluster-wide default** (with `{since=v1.3}`): the Helm value, the upgrade command and the ConfigMap key it becomes.
- **Step 2: Override the timeout for one service**: `timeoutSeconds` on a component, in a complete InferenceService.
- **Step 3: Verify the route**: a `kubectl get httproute` command that prints the timeout.
- **Turn the timeout off**: which value does it, and why long-running streams might need it.
- **Connection-timeout annotations are different** (with `{since=v1.3}`): the traffic annotations that set connection timeouts, and how they differ from the route timeout, linking Traffic annotations.
- **Next steps**: Configure ingress and Traffic annotations.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/configure-route-timeouts.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/configure-route-timeouts.md -> guides/networking/configure-route-timeouts.md: 2ebbce39`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/configure-route-timeouts.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Configure route timeouts guide" -m "The guide sets a cluster-wide route timeout and a per-service override,
checks the HTTPRoute, explains what 0 and an unset value do, and how the
connection-timeout annotations differ."
```

- [ ] **Step 4: Write the Set appProtocol on services page**

`website/src/lib/content/guides/networking/set-service-app-protocols.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/set-service-app-protocols.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/component.go` (about lines 27 to 30): `servicePortAppProtocols` on a component.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/service/service_reconciler.go` (about lines 99 to 160): `buildService` and `buildServicePorts`, which take the ports from the first container, keep their names, name the one port after the container when it declares none, and set `appProtocol` from the map.
- `pkg/apis/ome/v1beta1/servingruntime_types.go` and `pkg/controller/v1beta1/inferenceservice/utils/merging.go`: the same field on a runtime, and how it merges with the service's.
- `config/runtimes/`: the port names the catalog runtimes use.

A key that matches no port name is ignored without an error. Check that, and the merge between runtime and service, before you write them down.

Outline, `h2`s in order:

- **Before you begin**: an InferenceService, and a gateway or mesh that reads `appProtocol`.
- **How OME names Service ports**: from the first container's ports, or after the container when it declares none.
- **Step 1: Find the port name**: a command that reads the port names from the Service.
- **Step 2: Set servicePortAppProtocols**: a complete InferenceService that maps a port name to a protocol.
- **Step 3: Check the Service**: a command that prints each port's name and `appProtocol`.
- **Set a default in the ServingRuntime**: the runtime field, and which side wins.
- **Troubleshooting**: a key that matches no port, and a protocol the gateway doesn't read.
- **Next steps**: Configure ingress and Serving runtimes.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/set-service-app-protocols.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/set-service-app-protocols.md -> guides/networking/set-service-app-protocols.md: 6e72729e`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/set-service-app-protocols.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Set appProtocol on services guide" -m "The guide finds a Service port name, sets servicePortAppProtocols on a
component, checks the Service, and covers runtime defaults and keys that
match no port."
```

- [ ] **Step 5: Write the Configure ingress page**

`website/src/lib/content/guides/networking/configure-ingress.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/ingress.md`. Its known errors:
  - The Gateway API example at lines 111 to 117 sets only `ingressClassName`, but HTTPRoutes attach through `omeIngressGateway` (`pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go:170,210`). `ingressClassName` feeds only the Kubernetes Ingress (`pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/ingress_builder.go:120`).
  - The mode table at lines 152 and 153 leaves out OMENative.
- `pkg/controller/v1beta1/controllerconfig/configmap.go` (about lines 164 to 260): `IngressConfig`, every setting and its default.
- `charts/ome-resources/values.yaml` (about lines 600 to 680) and `charts/ome-resources/templates/ome-controller/configmap.yaml`: the Helm values, and how they render.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/reconciler.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/factory/reconciler_factory.go`: which strategy runs.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/strategies/gateway_api_strategy.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/strategies/raw_ingress_strategy.go`: what each strategy creates.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/ingress_builder.go`: the objects themselves.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services/domain_service.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services/path_service.go`: hostnames and paths.
- `pkg/constants/constants.go` (about lines 183 to 197) and `pkg/controller/v1beta1/inferenceservice/utils/annotations.go` (about lines 98 to 140): the per-service annotations, and how they're read.

Where a row in the configuration reference or the overrides table covers a setting or annotation v1.2.2 lacks, write "Since v1.3." in the row.

Outline, `h2`s in order:

- **Before you begin**: OME installed, and either an Ingress controller or a Gateway API implementation with a Gateway.
- **Choose an ingress type**: Kubernetes Ingress or Gateway API HTTPRoutes, what each needs and which setting picks it.
- **Step 1: Configure the ingress settings**: a values file for each type, the upgrade command and the ConfigMap it renders.
- **Step 2: Deploy and check the generated routes**: an InferenceService, the Ingress or HTTPRoute OME creates for it, and the URL in its status.
- **Configuration reference**: a table of every ingress setting, with its Helm value, its default and its effect.
- **Per-service overrides**: a table of the ingress annotations, linking the guides that cover them.
- **Turn off ingress creation**: cluster-wide and for one service.
- **Behavior by deployment mode**: what each mode gets, from the code rather than the Hugo table.
- **Troubleshooting**: no route created, a route with no hostname, and requests that don't reach the service.
- **Next steps**: Choose a Gateway API host scheme, Use multiple gateways and Configure route timeouts.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/configure-ingress.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/ingress.md -> guides/networking/configure-ingress.md: c6198139`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/configure-ingress.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Configure ingress guide" -m "The guide chooses between Ingress and Gateway API, configures the
settings through Helm, checks the generated routes and documents every
setting and per-service override."
```

- [ ] **Step 6: Write the Choose a Gateway API host scheme page**

`website/src/lib/content/guides/networking/gateway-host-schemes.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/gateway-host-schemes.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/controllerconfig/configmap.go` (about lines 185 to 200): `perISVCSubdomain` and `sharedHostPrefix`.
- `charts/ome-resources/values.yaml` (about lines 636 and 641): the Helm values and their defaults.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`: `gatewayHostnameForDomain`, which builds the hostname for each scheme.
- `pkg/constants/constants.go` (about lines 194 to 197) and `pkg/controller/v1beta1/inferenceservice/utils/annotations.go`: the per-service overrides. The shared-host-prefix annotation takes effect when it's present, and an empty prefix gives the bare domain; check both.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/strategies/gateway_api_strategy.go`: the URL the service reports.
- `git show 5f1c4096:pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`: how v1.2.2 named hosts.

Upgrading from v1.2.2 can change a service's hostname, depending on the default scheme. Once you've confirmed it in both versions of the builder, put it in a warning callout near the top of the page, with an example hostname before and after.

Outline, `h2`s in order:

- **Before you begin**: OME with Gateway API routing (link Configure ingress), and a DNS name for the gateway.
- **Shared host with a path prefix**: the hostname and path a service gets, and the DNS and TLS it needs.
- **Per-service subdomains**: the hostname a service gets, and the wildcard DNS and certificate it needs.
- **Switch schemes cluster-wide**: the Helm values and the upgrade command.
- **Override the scheme for one service**: the annotations, in a complete InferenceService.
- **Check which scheme is active**: the HTTPRoute's hostnames and the URL in the service's status.
- **Next steps**: Configure ingress and Use multiple gateways.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/gateway-host-schemes.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/gateway-host-schemes.md -> guides/networking/gateway-host-schemes.md: f4057609`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/gateway-host-schemes.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Gateway API host scheme guide" -m "The guide compares the shared-host and per-service-subdomain schemes,
switches schemes cluster-wide and per service, checks which is active
and warns that upgrading can change hostnames."
```

- [ ] **Step 7: Write the Use multiple gateways page**

`website/src/lib/content/guides/networking/multiple-gateways.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/multiple-gateways.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/controllerconfig/configmap.go`: `additionalIngressGateways` and `IngressGatewaySpec`.
- `charts/ome-resources/values.yaml` and `charts/ome-resources/templates/ome-controller/configmap.yaml`: the Helm values.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go`: `resolvedGateways` (about line 168), which decides the gateways a route attaches to, and the hostname for each gateway (about lines 205 to 212).
- `pkg/apis/ome/v1beta1/inference_service_status.go` (about lines 27 to 36) and `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/strategies/gateway_api_strategy.go` (about lines 143 to 186): `status.addresses`, one for each gateway.
- `pkg/constants/constants.go` (about line 197) and `pkg/controller/v1beta1/inferenceservice/utils/annotations.go` (about lines 100 to 140): the per-service gateway annotations.

A malformed `ome.io/ingress-additional-gateways` value is ignored without an error. The comment at `pkg/controller/v1beta1/inferenceservice/utils/annotations.go:128` says a webhook validates it, but none does. Put a note on the page, and report the comment.

Outline, `h2`s in order:

- **Before you begin**: OME with Gateway API routing, and two or more Gateways.
- **Step 1: List the gateways in the Helm values**: the primary gateway and `additionalIngressGateways`, and the upgrade command.
- **Step 2: Check the generated routes**: the `parentRefs` and hostnames on the HTTPRoute.
- **Step 3: Read the gateway addresses**: `status.addresses`, with one entry for each gateway.
- **Override the gateways for one service**: the annotations, their format and a complete InferenceService.
- **Troubleshooting**: a route missing from a gateway, and a malformed annotation.
- **Next steps**: Use per-namespace gateways and Choose a Gateway API host scheme.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/multiple-gateways.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/multiple-gateways.md -> guides/networking/multiple-gateways.md: 4affd2cf`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/multiple-gateways.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Use multiple gateways guide" -m "The guide attaches routes to more than one Gateway, checks the routes
and status.addresses, overrides the gateways for one service and notes
that a malformed annotation is ignored."
```

- [ ] **Step 8: Write the Use per-namespace gateways page**

`website/src/lib/content/guides/networking/namespace-gateways.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/namespace-gateways.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/controllerconfig/configmap.go` (from about line 212): `namespaceIngressGateways`.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/httproute_builder.go` (about lines 168 to 181): `resolvedGateways`, and the order in which it applies the namespace entry, the per-service annotations and the cluster default.
- `pkg/controller/v1beta1/inferenceservice/utils/annotations.go`: the per-service annotations.
- `charts/ome-resources/values.yaml`: the Helm value.

Outline, `h2`s in order:

- **Before you begin**: OME with Gateway API routing, and a Gateway for each tenant namespace.
- **How a gateway entry is written**: the fields of an entry, with an example.
- **Which gateway wins**: the order in which `resolvedGateways` applies the per-service annotations, the namespace entry and the cluster default.
- **Step 1: Map namespaces to gateways**: the Helm values and the upgrade command.
- **Step 2: Check the attachment**: the HTTPRoute's `parentRefs` for a service in a mapped namespace.
- **Per-service annotations**: how they combine with a namespace entry, such as additional gateways.
- **Next steps**: Use multiple gateways and Configure ingress.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py guides/networking/namespace-gateways.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/namespace-gateways.md -> guides/networking/namespace-gateways.md: 5847ff91`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/networking/namespace-gateways.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the per-namespace gateways guide" -m "The guide maps namespaces to Gateways, explains which gateway a route
attaches to, checks the attachment and covers how per-service
annotations combine with a namespace entry."
```

- [ ] **Step 9: Write the Traffic annotations page**

`website/src/lib/content/reference/api/traffic-annotations.md` is a reference page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/traffic-annotations.md`. It has no known errors, but check every fact anyway.
- `pkg/constants/traffic_annotations.go`: every annotation, its value format and its default.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translator.go`, `pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/unsupported.go` and the translators in `pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/` (`envoygateway/translator.go`, `istio/translator.go` and `noop.go`): what each gateway gets.
- `pkg/validation/traffic_annotations.go`: what admission rejects.
- `pkg/constants/traffic_capabilities.go` and `pkg/validation/traffic_capabilities.go`: which keys each translator supports.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders/backendtrafficpolicy_builder.go`: the BackendTrafficPolicy for Envoy Gateway.

`pkg/constants/traffic_annotations.go` also holds the annotations behind the rollout actions, such as pause, promote, rollback and repin. Cover only the annotations that configure traffic, and say in one sentence that the rest are in Labels and annotations.

Outline, `h2`s in order:

- **How annotations become gateway configuration**: the path from an annotation to the objects the translator writes.
- **Circuit breaker annotations**: a table of each key, its value format, its default and its effect.
- **Retry annotations**: the same table.
- **Timeout annotations**: the same table, and how they differ from the route timeout (link Configure route timeouts).
- **Pass-through prefixes**: the prefixes whose keys OME passes to the gateway unchanged.
- **Admission validation**: what the webhook rejects, with each message.
- **When the translator can't honor a key**: what happens on a gateway that doesn't support it, and where that shows.
- **Conflicting hand-written policies**: what OME does when a policy it didn't write targets the same route.
- **Example**: a complete InferenceService with an annotation from each group, which passes the YAML check.
- **Related pages**: Traffic policy, Configure route timeouts and Labels and annotations.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-rewritten.py reference/api/traffic-annotations.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/traffic-annotations.md -> reference/api/traffic-annotations.md: ce9c565e`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/api/traffic-annotations.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Traffic annotations reference" -m "The reference lists every circuit breaker, retry and timeout annotation
with its format and default, and covers pass-through prefixes, admission
checks, unsupported keys and conflicting policies."
```

- [ ] **Step 10: Check the pages**

Start the dev server on port 5185, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5185 --strictPort > /tmp/d5-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5185/ome/ && break; sleep 1; done
B=http://localhost:5185/ome
for p in guides/networking/configure-route-timeouts guides/networking/set-service-app-protocols guides/networking/configure-ingress guides/networking/gateway-host-schemes guides/networking/multiple-gateways guides/networking/namespace-gateways reference/api/traffic-annotations; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/networking/configure-route-timeouts guides/networking/set-service-app-protocols guides/networking/configure-ingress guides/networking/gateway-host-schemes guides/networking/multiple-gateways guides/networking/namespace-gateways reference/api/traffic-annotations; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/networking/configure-route-timeouts guides/networking/set-service-app-protocols guides/networking/configure-ingress guides/networking/gateway-host-schemes guides/networking/multiple-gateways guides/networking/namespace-gateways reference/api/traffic-annotations; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/networking/configure-route-timeouts 200 0 0 3
guides/networking/set-service-app-protocols 200 0 0 1
guides/networking/configure-ingress 200 0 0 0
guides/networking/gateway-host-schemes 200 0 0 1
guides/networking/multiple-gateways 200 0 0 1
guides/networking/namespace-gateways 200 0 0 1
reference/api/traffic-annotations 200 0 0 1
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/networking/configure-route-timeouts: before-you-begin how-ome-resolves-the-timeout step-1-set-a-cluster-wide-default step-2-override-the-timeout-for-one-service step-3-verify-the-route turn-the-timeout-off connection-timeout-annotations-are-different next-steps
guides/networking/set-service-app-protocols: before-you-begin how-ome-names-service-ports step-1-find-the-port-name step-2-set-serviceportappprotocols step-3-check-the-service set-a-default-in-the-servingruntime troubleshooting next-steps
guides/networking/configure-ingress: before-you-begin choose-an-ingress-type step-1-configure-the-ingress-settings step-2-deploy-and-check-the-generated-routes configuration-reference per-service-overrides turn-off-ingress-creation behavior-by-deployment-mode troubleshooting next-steps
guides/networking/gateway-host-schemes: before-you-begin shared-host-with-a-path-prefix per-service-subdomains switch-schemes-cluster-wide override-the-scheme-for-one-service check-which-scheme-is-active next-steps
guides/networking/multiple-gateways: before-you-begin step-1-list-the-gateways-in-the-helm-values step-2-check-the-generated-routes step-3-read-the-gateway-addresses override-the-gateways-for-one-service troubleshooting next-steps
guides/networking/namespace-gateways: before-you-begin how-a-gateway-entry-is-written which-gateway-wins step-1-map-namespaces-to-gateways step-2-check-the-attachment per-service-annotations next-steps
reference/api/traffic-annotations: how-annotations-become-gateway-configuration circuit-breaker-annotations retry-annotations timeout-annotations pass-through-prefixes admission-validation when-the-translator-cant-honor-a-key conflicting-hand-written-policies example related-pages
```

Then the since labels: one line, `8 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `8 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 39`: the 39 written pages include your 7.

Take screenshots of Configure route timeouts, Configure ingress and Traffic annotations at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5185'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 11: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d5-batches.py d5
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d5 (D5): 8 files, 0 not D5's
  tasks/run-workloads/configure-route-timeouts.md -> guides/networking/configure-route-timeouts.md: 2ebbce39
  tasks/run-workloads/set-service-app-protocols.md -> guides/networking/set-service-app-protocols.md: 6e72729e
  administration/ingress.md -> guides/networking/configure-ingress.md: c6198139
  administration/gateway-host-schemes.md -> guides/networking/gateway-host-schemes.md: f4057609
  administration/multiple-gateways.md -> guides/networking/multiple-gateways.md: 4affd2cf
  administration/namespace-gateways.md -> guides/networking/namespace-gateways.md: 5847ff91
  reference/traffic-annotations.md -> reference/api/traffic-annotations.md: ce9c565e
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 10 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D6: Roll out changes guides and canary references

**Branch:** `docs/website-d6`

**Goal:** Rewrite the five Roll out changes guides and the two canary references, so a reader can steer an OMENative rollout safely.

**Files:**

- Rewrite: `website/src/lib/content/guides/roll-out-changes/pause-and-resume-a-rollout.md`
- Rewrite: `website/src/lib/content/guides/roll-out-changes/promote-or-roll-back-a-canary.md`
- Rewrite: `website/src/lib/content/guides/roll-out-changes/release-a-held-revision.md`
- Rewrite: `website/src/lib/content/guides/roll-out-changes/repin-a-drifted-rollout-plan.md`
- Rewrite: `website/src/lib/content/guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md`
- Rewrite: `website/src/lib/content/reference/rollouts/canary-progression.md`
- Rewrite: `website/src/lib/content/reference/rollouts/canary-analysis.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Every page is about features v1.2.2 doesn't have: each step says `since: v1.3`. The four action guides use alpha commands that work only on OMENative components. Each says so in its opening paragraph and links Opt in to OMENative on Deployment modes. The minReadySeconds guide says which deployment modes honor the field.

Take flags from `--help` on `/tmp/d6-kubectl-ome`, and output from the golden files in `pkg/cli/cmd/rollout/testdata/` and the tests beside each command. Link Guarded actions and the kubectl ome pages, which D9 and D10 write, without anchors.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d6`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d6-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d6-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d6-landing.py check | tail -1
python3 /tmp/d6-batches.py d6
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d6 (D6): 0 files, 0 not D6's
  redirects.json: unchanged, so no page has a rewrittenFrom
  guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md: still a draft
  guides/roll-out-changes/pause-and-resume-a-rollout.md: still a draft
  guides/roll-out-changes/promote-or-roll-back-a-canary.md: still a draft
  guides/roll-out-changes/release-a-held-revision.md: still a draft
  guides/roll-out-changes/repin-a-drifted-rollout-plan.md: still a draft
  reference/rollouts/canary-analysis.md: still a draft
  reference/rollouts/canary-progression.md: still a draft
8 problems
```

- [ ] **Step 3: Write the Pause and resume a rollout page**

`website/src/lib/content/guides/roll-out-changes/pause-and-resume-a-rollout.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/pause-and-resume-a-rollout.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/rollout/actions.go`: the `pause` and `resume` commands and their flags, including `--discard-pending-actions` on `resume`.
- `pkg/cli/mutate/rollout.go` and `pkg/cli/mutate/preview.go`: the checks before the write, the dry-run preview and the annotation written.
- `pkg/constants/traffic_annotations.go` (about line 99): the rollout-paused annotation.
- `pkg/controller/v1beta1/workload/revision/revision.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/`: what the controller holds while the rollout is paused.
- `pkg/cli/report/v1alpha1/rollout_action.go` and `pkg/cli/report/v1alpha1/action_result.go`: the fields of the result.

Check in the controller what a pause holds and what keeps running, such as autoscaling and pod restarts, before you write it down.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService with a rollout in progress, kubectl-ome, and the RBAC that Guarded actions lists.
- **What a pause holds**: what stops and what keeps running while the rollout is paused.
- **Step 1: Pause the rollout**: `kubectl ome rollout pause`, with `--dry-run` first and then for real.
- **Step 2: Check the hold**: `kubectl ome rollout status`, and the annotation on the service.
- **Step 3: Resume the rollout**: `kubectl ome rollout resume`.
- **Discard pending actions on resume**: what `--discard-pending-actions` discards, and when to use it.
- **How the guard works**: what the command checks before it writes, and what it refuses.
- **Troubleshooting**: each refusal, with its message and exit code.
- **Next steps**: Promote or roll back a canary, kubectl ome rollout and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py guides/roll-out-changes/pause-and-resume-a-rollout.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/pause-and-resume-a-rollout.md -> guides/roll-out-changes/pause-and-resume-a-rollout.md: 11220a85`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/roll-out-changes/pause-and-resume-a-rollout.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Pause and resume a rollout guide" -m "The guide pauses an OMENative rollout, checks the hold, resumes it and
covers discarding pending actions, the guard the command applies and
each refusal."
```

- [ ] **Step 4: Write the Promote or roll back a canary page**

`website/src/lib/content/guides/roll-out-changes/promote-or-roll-back-a-canary.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/promote-or-rollback-a-canary.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/rollout/actions.go`: `promote` and `rollback`, including `--override-analysis` on `promote`.
- `pkg/cli/mutate/canary.go` and `pkg/cli/mutate/rollout.go`: the checks before the write, and the annotations written.
- `pkg/constants/traffic_annotations.go` (about lines 78 and 84): the promote and rollback annotations.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/promotion.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/reconciler.go`: what the controller does with each.
- `pkg/validation/traffic_annotations.go`: what admission checks on the annotations.
- `pkg/cli/canaryevidence/evidence.go`: the canary state the command reads.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService with a canary in progress, kubectl-ome, and the RBAC that Guarded actions lists.
- **How a canary step gates**: manual gates and analysis gates, linking Canary progression and Canary metric analysis.
- **Step 1: Check the canary's state**: `kubectl ome rollout status` output for a canary waiting at a gate.
- **Step 2: Promote a manually gated step**: `kubectl ome rollout promote`, with a dry run first.
- **Step 3: Override an analysis gate**: `--override-analysis`, and when the command refuses it.
- **Roll back the canary**: `kubectl ome rollout rollback`, and what happens to the new revision's pods and traffic.
- **What acceptance means**: the result says the request was recorded, not that the controller acted; what to watch next.
- **Troubleshooting**: each refusal, with its message and exit code.
- **Next steps**: Canary progression, Canary metric analysis and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py guides/roll-out-changes/promote-or-roll-back-a-canary.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/promote-or-rollback-a-canary.md -> guides/roll-out-changes/promote-or-roll-back-a-canary.md: 0e6c56f7`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/roll-out-changes/promote-or-roll-back-a-canary.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the promote or roll back guide" -m "The guide checks a canary's state, promotes a manually gated step,
overrides an analysis gate, rolls the canary back and explains what
acceptance means."
```

- [ ] **Step 5: Write the Release a held revision page**

`website/src/lib/content/guides/roll-out-changes/release-a-held-revision.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/release-a-held-revision.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/instance/release_held.go`: the command, `--revision` and `--component`, and its long help.
- `pkg/cli/mutate/held_release.go`, `pkg/cli/mutate/held_release_runtime.go` and `pkg/cli/mutate/held_release_source.go`: the eligibility checks and the request written.
- `pkg/constants/constants.go` (about line 211): the release-held annotation.
- `pkg/controller/v1beta1/inferencereplica/release_held.go`: how the controller accepts the request.
- `pkg/controller/v1beta1/workload/revision/revision.go`: what makes a revision Held.
- `pkg/cli/cmd/instance/retry_blocks.go` and `pkg/cli/waitheld/held.go`: finding held revisions, and waiting for the release.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService with a Held revision, kubectl-ome, and the RBAC that Guarded actions lists.
- **Step 1: Find the held revision**: `kubectl ome instance retry-blocks` or `kubectl ome instance status`, and what makes a revision Held.
- **Step 2: Preview with a dry run**: `kubectl ome instance release-held --dry-run` and its preview.
- **Step 3: Submit the release**: the command with `--revision` and `--component`, and the confirmation prompt.
- **Step 4: Watch the outcome**: `kubectl ome wait` for the held revision, and what the instance shows afterwards.
- **Eligibility**: a table of the checks, and the refusal for each.
- **What acceptance means**: the request is recorded on the InferenceReplica, and the controller decides.
- **Troubleshooting**: each refusal, with its message and exit code.
- **Release by annotation**: the annotation the command writes, for readers without the plugin, and what the command checks that a hand-written annotation skips.
- **Next steps**: kubectl ome instance, kubectl ome wait and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py guides/roll-out-changes/release-a-held-revision.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/release-a-held-revision.md -> guides/roll-out-changes/release-a-held-revision.md: 497eefe0`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/roll-out-changes/release-a-held-revision.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Release a held revision guide" -m "The guide finds a held revision, previews and submits its release,
watches the outcome and covers eligibility, what acceptance means and
the annotation the command writes."
```

- [ ] **Step 6: Write the Repin a drifted rollout plan page**

`website/src/lib/content/guides/roll-out-changes/repin-a-drifted-rollout-plan.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/repin-a-drifted-rollout-plan.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/rollout/repin.go`: the command and its flags.
- `pkg/cli/mutate/rollout_repin.go`: the checks before the write, and the digest it writes (`currentRepinDigest`, and the portable `rp1:` pattern at about line 41).
- `pkg/constants/traffic_annotations.go` (about lines 105 to 115): the repin annotation. Its value is the render digest or `now`, a repin can only hold or tighten the plan, and the controller clears the annotation once it acts.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/`: what the controller does with a repin.
- `pkg/cli/rolloutprojection/validate.go` and `pkg/cli/cmd/rollout/validate.go`: `kubectl ome rollout validate`, which reports drift.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService whose rollout plan drifted, kubectl-ome, and the RBAC that Guarded actions lists.
- **Step 1: Confirm the drift**: `kubectl ome rollout validate`, and what drift means.
- **Step 2: Preview and confirm the repin**: `kubectl ome rollout repin --dry-run`, then for real.
- **Step 3: Check the result**: the rollout status after the controller acts, and the annotation gone.
- **What the command checks**: each check and its refusal.
- **Why the value is a digest**: how the digest pins the plan the reader saw, and when `now` is accepted.
- **What the controller does next**: hold or tighten, and clearing the annotation.
- **Troubleshooting**: each refusal, with its message and exit code.
- **Repin by annotation**: the annotation and its value, for readers without the plugin.
- **Next steps**: kubectl ome rollout and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py guides/roll-out-changes/repin-a-drifted-rollout-plan.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/repin-a-drifted-rollout-plan.md -> guides/roll-out-changes/repin-a-drifted-rollout-plan.md: bb8f5b55`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/roll-out-changes/repin-a-drifted-rollout-plan.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Repin a drifted rollout guide" -m "The guide confirms drift, previews and submits a repin, checks the
result and explains the digest, what the command checks and what the
controller does next."
```

- [ ] **Step 7: Write the Pace rollouts with minReadySeconds page**

`website/src/lib/content/guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/lifecycle_types.go` (about line 574): `minReadySeconds`.
- `pkg/controller/v1beta1/controllerconfig/configmap.go` (about lines 282 to 288, validated at about line 1645): the cluster-wide default.
- `pkg/controller/v1beta1/inferenceservice/specdefaults/specdefaults.go` (about lines 43 and 162): the order in which the service, the runtime and the cluster default apply.
- `charts/ome-resources/values.yaml` (about lines 290 to 298): the Helm value, commented out by default.
- `pkg/apis/ome/v1beta1/inferencereplica_types.go` (about line 86): where the value lands on each instance.

Check whether RawDeployment and MultiNode pass the field to their workloads, or only OMENative reads it, and say which modes honor it.

Outline, `h2`s in order:

- **Before you begin**: an InferenceService in a deployment mode that honors the field.
- **Ready versus available**: what `minReadySeconds` delays, and how a rollout counts available instances.
- **How OME resolves the value**: the service, the runtime and the cluster default, in the order `specdefaults.go` applies them.
- **Step 1: Set it on an InferenceService**: a complete InferenceService with the field.
- **Step 2: Check the workload**: where the value shows on the instances, and how the rollout slows.
- **Set a default in the ServingRuntime**: the runtime field.
- **Set a cluster-wide default**: the Helm value and the ConfigMap key.
- **Troubleshooting**: a value admission rejects, and a value that doesn't seem to apply.
- **Clean up**: remove the field.
- **Next steps**: OMENative update strategies and Rollout groups.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md -> guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md: 17750ee2`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the minReadySeconds pacing guide" -m "The guide explains ready versus available, how OME resolves
minReadySeconds from the service, the runtime and the cluster default,
and how to set it and check it."
```

- [ ] **Step 8: Write the Canary progression page**

`website/src/lib/content/reference/rollouts/canary-progression.md` is a reference page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/canary-progression.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/rollout_types.go`: the canary fields.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/`: `capacity.go` (new-revision capacity), `traffic.go` (the split), `reconciler.go` and `promotion.go` (how a step advances) and `effective.go`.
- `pkg/constants/traffic_annotations.go` (about line 58): the ready-timeout annotation.
- `pkg/validation/canary.go`: what admission rejects.

Outline, `h2`s in order:

- **Example**: a complete InferenceService with a canary, which passes the YAML check.
- **New-revision capacity**: how many new-revision instances each step runs.
- **Traffic split**: how each step sets the weights, and on which gateways.
- **How a step advances**: the gates, pauses and analysis that move a step on.
- **scaleDownDelaySeconds**: what it delays, and its default.
- **readyTimeout**: the field, the annotation, and what happens when the timeout passes.
- **What admission rejects**: each rejection and its message.
- **Related pages**: Canary metric analysis, Promote or roll back a canary and Rollout groups.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py reference/rollouts/canary-progression.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/canary-progression.md -> reference/rollouts/canary-progression.md: 07a98de4`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/rollouts/canary-progression.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Canary progression reference" -m "The reference covers new-revision capacity, the traffic split, how a
step advances, scaleDownDelaySeconds, readyTimeout and what admission
rejects, with an example that passes the YAML check."
```

- [ ] **Step 9: Write the Canary metric analysis page**

`website/src/lib/content/reference/rollouts/canary-analysis.md` is a reference page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/canary-analysis.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis/`: `evaluator.go` (verdicts), `querier.go` (queries) and `template.go` (template variables).
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis_sampler.go` and `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/sampler.go`: when samples are taken.
- `pkg/apis/ome/v1beta1/rollout_types.go`: the analysis fields, including `onInconclusive`.
- `pkg/validation/canary.go`: what admission checks.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/status.go`: the analysis in status.

Outline, `h2`s in order:

- **Where the metrics come from**: the metric provider, and how OME reaches it.
- **When queries run**: the sampling schedule.
- **Query template variables**: a table of each variable and what it expands to.
- **How a sample is judged**: the thresholds and the verdicts.
- **What each verdict does**: pass, fail and inconclusive.
- **onInconclusive**: the choices and the default.
- **Analysis in status**: the fields a reader checks.
- **What admission checks**: each rejection and its message.
- **Related pages**: Canary progression and Promote or roll back a canary.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-rewritten.py reference/rollouts/canary-analysis.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/canary-analysis.md -> reference/rollouts/canary-analysis.md: d4cdc674`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/rollouts/canary-analysis.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Canary metric analysis reference" -m "The reference explains where analysis metrics come from, when queries
run, the template variables, how samples are judged, what each verdict
does and what admission checks."
```

- [ ] **Step 10: Check the pages**

Start the dev server on port 5186, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5186 --strictPort > /tmp/d6-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5186/ome/ && break; sleep 1; done
B=http://localhost:5186/ome
for p in guides/roll-out-changes/pause-and-resume-a-rollout guides/roll-out-changes/promote-or-roll-back-a-canary guides/roll-out-changes/release-a-held-revision guides/roll-out-changes/repin-a-drifted-rollout-plan guides/roll-out-changes/pace-rollouts-with-min-ready-seconds reference/rollouts/canary-progression reference/rollouts/canary-analysis; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/roll-out-changes/pause-and-resume-a-rollout guides/roll-out-changes/promote-or-roll-back-a-canary guides/roll-out-changes/release-a-held-revision guides/roll-out-changes/repin-a-drifted-rollout-plan guides/roll-out-changes/pace-rollouts-with-min-ready-seconds reference/rollouts/canary-progression reference/rollouts/canary-analysis; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/roll-out-changes/pause-and-resume-a-rollout guides/roll-out-changes/promote-or-roll-back-a-canary guides/roll-out-changes/release-a-held-revision guides/roll-out-changes/repin-a-drifted-rollout-plan guides/roll-out-changes/pace-rollouts-with-min-ready-seconds reference/rollouts/canary-progression reference/rollouts/canary-analysis; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/roll-out-changes/pause-and-resume-a-rollout 200 0 0 1
guides/roll-out-changes/promote-or-roll-back-a-canary 200 0 0 1
guides/roll-out-changes/release-a-held-revision 200 0 0 1
guides/roll-out-changes/repin-a-drifted-rollout-plan 200 0 0 1
guides/roll-out-changes/pace-rollouts-with-min-ready-seconds 200 0 0 1
reference/rollouts/canary-progression 200 0 0 1
reference/rollouts/canary-analysis 200 0 0 1
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/roll-out-changes/pause-and-resume-a-rollout: before-you-begin what-a-pause-holds step-1-pause-the-rollout step-2-check-the-hold step-3-resume-the-rollout discard-pending-actions-on-resume how-the-guard-works troubleshooting next-steps
guides/roll-out-changes/promote-or-roll-back-a-canary: before-you-begin how-a-canary-step-gates step-1-check-the-canarys-state step-2-promote-a-manually-gated-step step-3-override-an-analysis-gate roll-back-the-canary what-acceptance-means troubleshooting next-steps
guides/roll-out-changes/release-a-held-revision: before-you-begin step-1-find-the-held-revision step-2-preview-with-a-dry-run step-3-submit-the-release step-4-watch-the-outcome eligibility what-acceptance-means troubleshooting release-by-annotation next-steps
guides/roll-out-changes/repin-a-drifted-rollout-plan: before-you-begin step-1-confirm-the-drift step-2-preview-and-confirm-the-repin step-3-check-the-result what-the-command-checks why-the-value-is-a-digest what-the-controller-does-next troubleshooting repin-by-annotation next-steps
guides/roll-out-changes/pace-rollouts-with-min-ready-seconds: before-you-begin ready-versus-available how-ome-resolves-the-value step-1-set-it-on-an-inferenceservice step-2-check-the-workload set-a-default-in-the-servingruntime set-a-cluster-wide-default troubleshooting clean-up next-steps
reference/rollouts/canary-progression: example new-revision-capacity traffic-split how-a-step-advances scaledowndelayseconds readytimeout what-admission-rejects related-pages
reference/rollouts/canary-analysis: where-the-metrics-come-from when-queries-run query-template-variables how-a-sample-is-judged what-each-verdict-does oninconclusive analysis-in-status what-admission-checks related-pages
```

Then the since labels: one line, `7 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `7 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 39`: the 39 written pages include your 7.

Take screenshots of Pause and resume a rollout, Promote or roll back a canary and Canary progression at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5186'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 11: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d6-batches.py d6
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d6 (D6): 8 files, 0 not D6's
  tasks/pause-and-resume-a-rollout.md -> guides/roll-out-changes/pause-and-resume-a-rollout.md: 11220a85
  tasks/promote-or-rollback-a-canary.md -> guides/roll-out-changes/promote-or-roll-back-a-canary.md: 0e6c56f7
  tasks/release-a-held-revision.md -> guides/roll-out-changes/release-a-held-revision.md: 497eefe0
  tasks/repin-a-drifted-rollout-plan.md -> guides/roll-out-changes/repin-a-drifted-rollout-plan.md: bb8f5b55
  tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md -> guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md: 17750ee2
  reference/canary-progression.md -> reference/rollouts/canary-progression.md: 07a98de4
  reference/canary-analysis.md -> reference/rollouts/canary-analysis.md: d4cdc674
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 10 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D7: Scale, migration and multi-cluster guides

**Branch:** `docs/website-d7`

**Goal:** Rewrite the scale, migration and multi-cluster guides and write the new Troubleshoot an InferenceService guide, so a reader can act on running instances and find what's wrong with a service.

**Files:**

- Rewrite: `website/src/lib/content/guides/scale-and-migrate/request-a-transient-scale.md`
- Rewrite: `website/src/lib/content/guides/scale-and-migrate/request-an-instance-migration.md`
- Rewrite: `website/src/lib/content/guides/multi-cluster/routing-health-probes.md`
- Rewrite: `website/src/lib/content/guides/multi-cluster/drain-a-workload-cluster.md`
- Rewrite: `website/src/lib/content/guides/troubleshoot/troubleshoot-an-inferenceservice.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 4 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Every page is about features v1.2.2 doesn't have: each step says `since: v1.3`. Scale and migration use alpha commands that work only on OMENative components; each guide says so in its opening paragraph and links Opt in to OMENative on Deployment modes. The two multi-cluster guides are preview pages. The multi-cluster reconcilers are alpha and off by default (`cmd/manager/main.go:257-258`), so each says how to turn them on.

Take flags from `--help` on `/tmp/d7-kubectl-ome`, and output from the golden files and tests beside each command. Link Guarded actions and the kubectl ome pages, which D9 and D10 write, without anchors.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d7`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d7-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d7-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d7-landing.py check | tail -1
python3 /tmp/d7-batches.py d7
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d7 (D7): 0 files, 0 not D7's
  redirects.json: unchanged, so no page has a rewrittenFrom
  guides/multi-cluster/drain-a-workload-cluster.md: still a draft
  guides/multi-cluster/routing-health-probes.md: still a draft
  guides/scale-and-migrate/request-a-transient-scale.md: still a draft
  guides/scale-and-migrate/request-an-instance-migration.md: still a draft
  guides/troubleshoot/troubleshoot-an-inferenceservice.md: still a draft
6 problems
```

- [ ] **Step 3: Write the Request a transient scale page**

`website/src/lib/content/guides/scale-and-migrate/request-a-transient-scale.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/request-a-transient-scale.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/scale/scale.go` and `pkg/cli/cmd/scale/collect.go`: the command, its flags (`--replicas`, `--component`, `--override-autoscaler`, `--ome-namespace`, `--dry-run`, `--yes` and `--output`) and what it reads before it writes.
- `pkg/cli/mutate/scale.go`, `pkg/cli/mutate/scale_preview.go`, `pkg/cli/mutate/scale_evidence.go` and `pkg/cli/mutate/scale_pinned.go`: `PrepareScale`, the ownership and bounds checks (`ErrScaleOwnership` and `ErrScaleBounds`) and the preview.
- `pkg/cli/transport/inference_replica_scale.go`: what the request writes.
- `pkg/apis/ome/v1beta1/inferencereplica_types.go`: the fields the request changes, and the status that reports the outcome.
- `pkg/cli/cmd/wait/scale_current.go`: waiting for the scale to land with `kubectl ome wait`.
- `pkg/cli/report/v1alpha1/scale_action.go` and `pkg/cli/report/v1alpha1/action_result.go`: the fields of the result.

Check how long a transient scale lasts, and what ends it, such as the autoscaler or the next change to the spec, before you write it down.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService, kubectl-ome, and the RBAC that Guarded actions lists.
- **Scaling alongside an autoscaler**: what "transient" means, what ends the request, and when `--override-autoscaler` is needed.
- **Step 1: Preview with a dry run**: `kubectl ome scale --dry-run` and its preview.
- **Step 2: Submit the request**: the command with `--replicas` and `--component`, and the confirmation prompt.
- **Step 3: Check the outcome**: `kubectl ome wait` for the scale, and the instances afterwards.
- **Eligibility**: a table of the checks the command makes, and the refusal for each.
- **What acceptance means**: the result says the request was recorded, not that pods are running; what to watch next.
- **Troubleshooting**: each refusal, with its message and exit code.
- **Next steps**: kubectl ome scale, Guarded actions and Autoscaler policy.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d7-rewritten.py guides/scale-and-migrate/request-a-transient-scale.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/request-a-transient-scale.md -> guides/scale-and-migrate/request-a-transient-scale.md: 73eef4e8`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/scale-and-migrate/request-a-transient-scale.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Request a transient scale guide" -m "The guide previews and submits a transient OMENative scale, checks the
outcome, and covers scaling alongside an autoscaler, eligibility, what
acceptance means and each refusal."
```

- [ ] **Step 4: Write the Request an instance migration page**

`website/src/lib/content/guides/scale-and-migrate/request-an-instance-migration.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/request-an-instance-migration.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/migration/start.go`: the command and its flags (`--instance`, `--component`, `--from-node`, `--hint-node`, `--reason`, `--request-id`, `--requested-by`, `--ome-namespace`, `--dry-run`, `--yes` and `--output`). Its long help says it requests the migration by annotation, never by spec, status or scale.
- `pkg/cli/mutate/migration.go`, `pkg/cli/mutate/migration_preview.go` and `pkg/cli/mutate/migration_evidence.go`: the checks before the write, and the annotation written (about line 280 of `migration.go`).
- `pkg/controller/v1beta1/inferencereplica/migration_accept.go`: how the controller accepts or rejects the request.
- `pkg/alfred/engine/dispatch_request.go` (about line 19): Alfred can send the same request. Check it before you say so.
- `pkg/cli/cmd/migration/status.go`, `pkg/cli/cmd/migration/history.go` and `pkg/cli/waitmigration/`: following the request.

Outline, `h2`s in order:

- **Before you begin**: an OMENative InferenceService, kubectl-ome, and the RBAC that Guarded actions lists.
- **How a migration request travels**: the annotation on the InferenceReplica, what the controller does with it, and who else writes it.
- **Step 1: Preview with a dry run**: `kubectl ome migration start --dry-run` and its preview.
- **Step 2: Request the migration**: the command with `--instance` and `--component`, and the optional node, reason and request ID flags.
- **Step 3: Follow the request**: `kubectl ome migration status` and `kubectl ome migration history`, and waiting with `kubectl ome wait`.
- **Troubleshooting**: each refusal, with its message and exit code, and a request the controller rejects.
- **Next steps**: kubectl ome migration and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d7-rewritten.py guides/scale-and-migrate/request-an-instance-migration.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/request-an-instance-migration.md -> guides/scale-and-migrate/request-an-instance-migration.md: f9cf89d9`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/scale-and-migrate/request-an-instance-migration.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the instance migration guide" -m "The guide explains how a migration request reaches the controller,
previews and submits one for an OMENative instance and follows it with
the migration status and history commands."
```

- [ ] **Step 5: Write the Configure routing health probes page**

`website/src/lib/content/guides/multi-cluster/routing-health-probes.md` is a guide. Front matter: replace `status: draft` with `status: preview`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/routing-health-probes.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/routing_types.go`: `RoutingSpec` (about line 11), `RoutingProbeSpec` (about line 50) and `AllFailedPolicy` (about lines 104 to 123).
- `pkg/controller/v1beta1/controllerconfig/multicluster.go`: the operator-level probe defaults.
- `pkg/controller/v1beta1/placement/routing/`: `config.go` (how a service's settings merge with the defaults), `prober.go` (one probe attempt), `weight.go` and `resolver.go` (thresholds, hysteresis and weights) and `controller.go`.
- `pkg/validation/routing.go`: what admission checks.
- `charts/ome-resources/multicluster-options.md` and `cmd/manager/multicluster.go`: how to turn on the multi-cluster reconcilers and set the defaults through Helm.

Outline, `h2`s in order:

- **Before you begin**: a control-plane OME with the multi-cluster reconcilers turned on, and workload clusters that serve the InferenceService.
- **How OME judges a probe attempt**: what a probe sends, and what counts as a success or a failure.
- **Thresholds and hysteresis**: how many results change a cluster's state, and why a state doesn't flap.
- **Step 1: Set operator-level defaults**: the Helm values and what they render.
- **Step 2: Override the probe for one service**: `spec.routing` in a complete InferenceService.
- **Step 3: Check the probe state**: where the result shows, such as the TrafficMap.
- **When every cluster fails**: `allFailedPolicy`, its choices and its default.
- **Next steps**: Traffic map and Drain a workload cluster.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d7-rewritten.py guides/multi-cluster/routing-health-probes.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/routing-health-probes.md -> guides/multi-cluster/routing-health-probes.md: 7f0b7bd1`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/multi-cluster/routing-health-probes.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the routing health probes guide" -m "The guide explains how OME probes each workload cluster, thresholds and
hysteresis, operator defaults and per-service overrides, the probe state
and what happens when every cluster fails."
```

- [ ] **Step 6: Write the Drain a workload cluster page**

`website/src/lib/content/guides/multi-cluster/drain-a-workload-cluster.md` is a guide. Front matter: replace `status: draft` with `status: preview`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/drain-traffic-from-a-workload-cluster.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/traffic/actions.go`: `drain`, with `--workload-cluster`, `--id`, `--reason`, `--dry-run`, `--yes` and `--output`, and `undrain`, with only `--id`, `--dry-run`, `--yes` and `--output`.
- `pkg/cli/mutate/traffic_drain.go`: the checks, and the write.
- `pkg/trafficdrain/annotation.go`: the `ome.io/traffic-drain` annotation on the InferenceService, a JSON object keyed by drain ID with a cluster and a reason for each, so each drain is removed on its own. A present value is parsed strictly: admission rejects a malformed one (`pkg/validation/traffic_annotations.go:194`), and the routing controller never reads it as no drain (`pkg/controller/v1beta1/placement/routing/controller.go:102`).
- `pkg/apis/ome/v1beta1/trafficmap_types.go` (about line 63): how the TrafficMap reports a drained cluster, with the `TrafficDrain` reason.
- `pkg/controller/v1beta1/placement/routing/weight.go`: what a drain does to the weights.

Outline, `h2`s in order:

- **Before you begin**: a control-plane OME with the multi-cluster reconcilers turned on, kubectl-ome, and the RBAC that Guarded actions lists.
- **How a drain is recorded**: the annotation, one entry for each drain ID, and why several drains can hold the same cluster.
- **Step 1: Drain the cluster**: `kubectl ome traffic drain` with `--workload-cluster`, `--id` and `--reason`, with a dry run first.
- **Step 2: Check the drain**: the TrafficMap weight and reason, and `kubectl ome traffic status`.
- **Step 3: Undrain the cluster**: `kubectl ome traffic undrain` with the same ID.
- **What acceptance means**: the annotation is written, and the controller moves the traffic.
- **Troubleshooting**: each refusal, with its message and exit code, and a drain that doesn't move traffic.
- **Next steps**: Traffic map, kubectl ome traffic and Guarded actions.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d7-rewritten.py guides/multi-cluster/drain-a-workload-cluster.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/drain-traffic-from-a-workload-cluster.md -> guides/multi-cluster/drain-a-workload-cluster.md: dd6a16f5`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/multi-cluster/drain-a-workload-cluster.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Drain a workload cluster guide" -m "The guide drains one workload cluster from an InferenceService's
traffic, checks the TrafficMap, undrains it and explains how drains are
recorded and what acceptance means."
```

- [ ] **Step 7: Write the Troubleshoot an InferenceService page**

`website/src/lib/content/guides/troubleshoot/troubleshoot-an-inferenceservice.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/status/`: `kubectl ome status` (`status.go`, `gather.go`, `project.go` and `render.go`), with golden output in `pkg/cli/cmd/status/testdata/`.
- `pkg/apis/ome/v1beta1/inference_service_status.go` and `pkg/controller/v1beta1/inferenceservice/status/`: the conditions, and what sets each.
- `pkg/apis/ome/v1beta1/model.go`: the model states.
- `pkg/cli/cmd/runtime/explain.go`, `pkg/cli/cmd/rollout/explain.go`, `pkg/cli/cmd/autoscale/explain.go` and `pkg/cli/cmd/traffic/explain.go`: the explain command for each area.
- `pkg/cli/cmd/logs/logs.go`: `kubectl ome logs`, and how it selects pods.

This guide ties together the read commands. Keep each step to the command, what healthy output looks like and where to go next, and link the guide or reference that covers the fix.

Outline, `h2`s in order:

- **Before you begin**: kubectl-ome, and read access to the service's namespace.
- **Symptoms and where to start**: a table from common symptoms, such as no URL, pods pending or requests failing, to the step that covers each.
- **Step 1: Get the overall status**: `kubectl ome status`, and how to read each section.
- **Step 2: Check the model**: the model's state, and what to do when it isn't Ready.
- **Step 3: Check the runtime**: `kubectl ome runtime explain`, linking Troubleshoot runtime selection.
- **Step 4: Check the rollout**: `kubectl ome rollout explain` for a rollout that doesn't progress.
- **Step 5: Check autoscaling**: `kubectl ome autoscale explain`.
- **Step 6: Check traffic**: `kubectl ome traffic explain` for requests that don't arrive.
- **Step 7: Read the logs**: `kubectl ome logs` for the component's pods.
- **Next steps**: kubectl-ome overview and install, and Troubleshoot runtime selection.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/troubleshoot/troubleshoot-an-inferenceservice.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write Troubleshoot an InferenceService" -m "A new guide that finds what is wrong with an InferenceService, starting
from kubectl ome status and checking the model, runtime, rollout,
autoscaling, traffic and logs in turn."
```

- [ ] **Step 8: Check the pages**

Start the dev server on port 5187, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5187 --strictPort > /tmp/d7-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5187/ome/ && break; sleep 1; done
B=http://localhost:5187/ome
for p in guides/scale-and-migrate/request-a-transient-scale guides/scale-and-migrate/request-an-instance-migration guides/multi-cluster/routing-health-probes guides/multi-cluster/drain-a-workload-cluster guides/troubleshoot/troubleshoot-an-inferenceservice; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/scale-and-migrate/request-a-transient-scale guides/scale-and-migrate/request-an-instance-migration guides/multi-cluster/routing-health-probes guides/multi-cluster/drain-a-workload-cluster guides/troubleshoot/troubleshoot-an-inferenceservice; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/scale-and-migrate/request-a-transient-scale guides/scale-and-migrate/request-an-instance-migration guides/multi-cluster/routing-health-probes guides/multi-cluster/drain-a-workload-cluster guides/troubleshoot/troubleshoot-an-inferenceservice; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/scale-and-migrate/request-a-transient-scale 200 0 0 1
guides/scale-and-migrate/request-an-instance-migration 200 0 0 1
guides/multi-cluster/routing-health-probes 200 0 1 1
guides/multi-cluster/drain-a-workload-cluster 200 0 1 1
guides/troubleshoot/troubleshoot-an-inferenceservice 200 0 0 1
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/scale-and-migrate/request-a-transient-scale: before-you-begin scaling-alongside-an-autoscaler step-1-preview-with-a-dry-run step-2-submit-the-request step-3-check-the-outcome eligibility what-acceptance-means troubleshooting next-steps
guides/scale-and-migrate/request-an-instance-migration: before-you-begin how-a-migration-request-travels step-1-preview-with-a-dry-run step-2-request-the-migration step-3-follow-the-request troubleshooting next-steps
guides/multi-cluster/routing-health-probes: before-you-begin how-ome-judges-a-probe-attempt thresholds-and-hysteresis step-1-set-operator-level-defaults step-2-override-the-probe-for-one-service step-3-check-the-probe-state when-every-cluster-fails next-steps
guides/multi-cluster/drain-a-workload-cluster: before-you-begin how-a-drain-is-recorded step-1-drain-the-cluster step-2-check-the-drain step-3-undrain-the-cluster what-acceptance-means troubleshooting next-steps
guides/troubleshoot/troubleshoot-an-inferenceservice: before-you-begin symptoms-and-where-to-start step-1-get-the-overall-status step-2-check-the-model step-3-check-the-runtime step-4-check-the-rollout step-5-check-autoscaling step-6-check-traffic step-7-read-the-logs next-steps
```

Then the since labels: one line, `5 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `5 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 37`: the 37 written pages include your 5.

Take screenshots of Request a transient scale, Configure routing health probes and Troubleshoot an InferenceService at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5187'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 9: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d7-batches.py d7
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d7 (D7): 6 files, 0 not D7's
  tasks/request-a-transient-scale.md -> guides/scale-and-migrate/request-a-transient-scale.md: 73eef4e8
  tasks/request-an-instance-migration.md -> guides/scale-and-migrate/request-an-instance-migration.md: f9cf89d9
  administration/routing-health-probes.md -> guides/multi-cluster/routing-health-probes.md: 7f0b7bd1
  tasks/drain-traffic-from-a-workload-cluster.md -> guides/multi-cluster/drain-a-workload-cluster.md: dd6a16f5
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 8 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D8: Operator guides and labels reference

**Branch:** `docs/website-d8`

**Goal:** Rewrite the seven Operate OME guides and the Labels and annotations reference, so an operator can configure the controller and the model agent, add the optional scheduler and quota manager, and set up metrics and alerting.

**Files:**

- Rewrite: `website/src/lib/content/guides/operate-ome/configure-the-controller.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/model-agent.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/ome-scheduler.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/accelerator-quota.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/shared-hf-artifacts.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/metrics.md`
- Rewrite: `website/src/lib/content/guides/operate-ome/alerting.md`
- Rewrite: `website/src/lib/content/reference/api/labels-and-annotations.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 8 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Operators change OME through the `ome-resources` chart, so each setting on these pages shows its Helm value and what it renders, such as a flag in `charts/ome-resources/templates/ome-controller/deployment.yaml`. In `charts/ome-resources/values.yaml`, `ome.controller` starts at about line 253, `modelAgent` at 727, `serviceMonitor` at 843, `prometheusRule` at 868, `podMonitor` at 931 and `prometheus` at 945.

Much of this is new since v1.2.2: the `ome-scheduler` and `ome-quota-manager` charts, the bundled Prometheus and its alerts, shared Hugging Face artifacts and most of the controller's tuning flags. Check each with `git show 5f1c4096:<path>`.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d8`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d8-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d8-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d8-landing.py check | tail -1
python3 /tmp/d8-batches.py d8
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d8 (D8): 0 files, 0 not D8's
  redirects.json: unchanged, so no page has a rewrittenFrom
  guides/operate-ome/accelerator-quota.md: still a draft
  guides/operate-ome/alerting.md: still a draft
  guides/operate-ome/configure-the-controller.md: still a draft
  guides/operate-ome/metrics.md: still a draft
  guides/operate-ome/model-agent.md: still a draft
  guides/operate-ome/ome-scheduler.md: still a draft
  guides/operate-ome/shared-hf-artifacts.md: still a draft
  reference/api/labels-and-annotations.md: still a draft
9 problems
```

- [ ] **Step 3: Write the Configure the controller page**

`website/src/lib/content/guides/operate-ome/configure-the-controller.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/controller-configuration.md`. It has no known errors, but check every fact anyway.
- `cmd/manager/main.go` (about lines 180 to 290): every flag, its default and its help text. `git show 5f1c4096:cmd/manager/main.go` shows which flags v1.2.2 had.
- `pkg/leaderelection/timing.go`: the three `--leader-elect-*` lease flags, which are new since v1.2.2.
- `charts/ome-resources/templates/ome-controller/deployment.yaml` (about lines 62 to 103): the args the chart always passes, `--metrics-bind-address=:8080`, `--leader-elect`, `--webhook` and `--zap-encoder=console`, and the ones it passes only when a value is set.
- `charts/ome-resources/values.yaml` (about lines 330 to 400): the values under `ome.controller` that become flags, such as `inferenceServiceMaxConcurrentReconciles`, `kubeAPIQPS`, `leaderElection`, `configCacheTTL` and `quotaAcceleratorResources`.
- `pkg/controller/v1beta1/runtimerevision/gc_controller.go` and `pkg/controller/v1beta1/runtimerevision/plan.go`: what the garbage collection flags keep and delete.

The chart has no value for the garbage collection flags or `--enable-inferencereplica-controller`. Find out how an operator changes them, such as by patching the Deployment, and whether `helm upgrade` undoes that, and say both on the page.

In the flag tables, write "Since v1.3." in the row of each flag v1.2.2 lacks. The multi-cluster flags are alpha and off by default (`cmd/manager/main.go:257-258`): give them their own table, and link the multi-cluster guides.

Outline, `h2`s in order:

- **Before you begin**: OME installed with the `ome-resources` chart, Helm, and kubectl access to the `ome` namespace.
- **Step 1: Set a flag**: a values file that sets one flag, the upgrade command and the arg it renders on the manager Deployment.
- **Step 2: Check the running flags**: a command that prints the manager container's args.
- **Flags**: tables by area, with the columns Flag, Default, Helm value and Description.
- **Tune leader election**: what the lease settings do, and when to change them. Add `### Lease timing {since=v1.3}` for the three lease flags; the chart passes them only when all three values are set.
- **Tune reconcile throughput** (with `{since=v1.3}`): the concurrency and client QPS flags, and an `h3` on how to tell that the manager is throttled.
- **Tune runtime-revision garbage collection**: what the flags keep and delete, and how to change them without a Helm value.
- **Next steps**: Run the model agent, Collect metrics and Set accelerator quotas.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/configure-the-controller.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/controller-configuration.md -> guides/operate-ome/configure-the-controller.md: 248a7463`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/configure-the-controller.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Configure the controller guide" -m "The guide sets a manager flag through Helm, checks the running flags and
documents every flag, with leader election, reconcile throughput and
runtime-revision garbage collection."
```

- [ ] **Step 4: Write the Run the model agent page**

`website/src/lib/content/guides/operate-ome/model-agent.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/model-agent.md`. Its known errors:
  - The flag tables (about lines 120 to 160) and the examples (about lines 458 and 562) use ten flags that don't exist: `--hf-max-workers`, `--hf-max-retries`, `--hf-retry-interval`, `--temp-dir`, `--cleanup-temp`, `--log-format`, `--metrics-port`, `--config-map-sync-interval`, `--model-watch-resync-period` and `--max-concurrent-reconciles`.
  - They leave out three real flags: `--model-verification-concurrency`, `--num-high-priority-worker` and `--same-path-wait-timeout`. The real flags are in `cmd/model-agent/main.go:78-90`.
- `cmd/model-agent/main.go`: the flags (about lines 78 to 90), and how Viper maps each to an environment variable (`SetEnvKeyReplacer` and `AutomaticEnv`).
- `charts/ome-resources/templates/model-agent-daemonset/`: the DaemonSet, its RBAC, its ConfigMap and the PodMonitor, which is new since v1.2.2.
- `charts/ome-resources/values.yaml`: `modelAgent` (about line 727) and `podMonitor` (about line 931).
- `pkg/modelagent/scout.go`, `pkg/modelagent/gopher.go` and `pkg/modelagent/gopher_task_queue.go`: how the agent finds models, queues their downloads and verifies them.
- `pkg/modelagent/node_label_reconciler.go` and `pkg/modelagent/configmap_reconciler.go`: the node labels and the per-node ConfigMap that report where a model is ready.
- `pkg/modelagent/healthz.go`, `pkg/modelagent/metrics.go` and `pkg/modelagent/verification_limiter.go`: the health checks, the metrics and the limit on concurrent verifications.
- `pkg/modelagent/hf_revision.go` and `pkg/modelagent/hf_snapshot_validation.go`: Hugging Face revision pinning and snapshot validation.

Of the real flags, only `--model-verification-concurrency` is new since v1.2.2: write "Since v1.3." in its row. Write it in plain text, too, where the page covers the PodMonitor, Hugging Face revision pinning, snapshot validation and shared artifacts, and link Share Hugging Face artifacts for the last.

Outline, `h2`s in order:

- **Before you begin**: OME installed with the `ome-resources` chart, and Helm.
- **What the agent does**: finding models, the download queue, verification, and the node labels and ConfigMap that report where each model is ready.
- **Step 1: Check the agent on each node**: the DaemonSet, and one ready pod per node.
- **Step 2: Change a setting**: the Helm value, the upgrade command and the flag it renders.
- **Step 3: Check where a model is ready**: the node label and the ConfigMap entry for a model.
- **Flags**: a table of every real flag, with its default, its environment variable and its Helm value.
- **Download behavior**: workers, retries, the high-priority queue and waiting on a path another download holds.
- **Health checks and metrics**: the probes, the metrics port and the PodMonitor.
- **Troubleshooting**: a model stuck downloading, a failed verification and a node that never gets the label.
- **Next steps**: Share Hugging Face artifacts, Base models and Collect metrics.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/model-agent.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/model-agent.md -> guides/operate-ome/model-agent.md: 8de82852`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/model-agent.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Run the model agent guide" -m "The guide checks the model agent on each node, changes a setting through
Helm, finds where a model is ready and documents the real flags,
download behavior, health checks and metrics."
```

- [ ] **Step 5: Write the Use the OME scheduler page**

`website/src/lib/content/guides/operate-ome/ome-scheduler.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/ome-scheduler.md`. It has no known errors, but check every fact anyway.
- `charts/ome-scheduler/`: `Chart.yaml` and its `kubeVersion`, `values.yaml`, `values.schema.json`, `README.md`, the templates and `charts/ome-scheduler/tests/render_test.sh`.
- `scheduler/README.md`, `scheduler/cmd/ome-scheduler/main.go` and `scheduler/go.mod`: what the scheduler is, and the Kubernetes and scheduler-plugins versions it builds against.
- `scheduler/pkg/plugins/gangpack/`: `config.go` and `plugin.go` (the plugin args), `gang.go` (the `scheduling.x-k8s.io/pod-group` label), `permit.go`, `score.go` and `metrics.go`.
- `charts/ome-scheduler/values.yaml` (about lines 55 to 75): the plugin args the chart renders, such as `podGroupTopologyKeyAnnotation`, which defaults to `ome.io/topology-key`, and `unsupportedPlacementGroupLabel`, which defaults to `ome.io/placement-group`. A pod with that label fails closed until partner-gang placement exists.
- `scheduler/pkg/topology/domains.go` and `scheduler/pkg/placement/pins.go`: accelerator domains, and the domain each gang is pinned to.
- `pkg/controller/v1beta1/workload/podgroup/podgroup.go`, `pkg/controller/v1beta1/workload/gang/gang.go` and `pkg/controller/v1beta1/workload/types/events.go`: the PodGroups OME creates for OMENative components, and the `MaybeNoGangScheduler` warning event.
- `pkg/apis/ome/v1beta1/podspec.go` and `pkg/apis/ome/v1beta1/servingruntime_types.go`: `schedulerName`.

The scheduler is alpha and runs only on Kubernetes 1.35: check `kubeVersion` in `charts/ome-scheduler/Chart.yaml` and the Kubernetes version in `scheduler/go.mod`, and say both in the opening paragraph.

The YAML check skips PodGroup objects, since their CRD isn't in `config/crd/full`. Check each PodGroup field by hand against the scheduler-plugins version in `scheduler/go.mod`.

Outline, `h2`s in order:

- **Before you begin**: Kubernetes 1.35, Helm, and the PodGroup CRD from scheduler-plugins.
- **How gang packing works**: what a gang is, how the plugin picks and pins an accelerator domain, and what Permit waits for.
- **Step 1: Install the chart**: the Helm command, and a check that the scheduler pods are running.
- **Step 2: Opt a workload in**: `schedulerName`, the `scheduling.x-k8s.io/pod-group` label, and a PodGroup with the `ome.io/topology-key` annotation, `minMember` and `scheduleTimeoutSeconds`. Say that OME creates its own PodGroups for OMENative components.
- **Step 3: Check the placement**: which nodes the gang landed on, and the scheduler's events.
- **Tune packing**: the fallback topology key, the permit timeout and standalone domain packing.
- **Availability**: the default two replicas with leader election, the anti-affinity and the PodDisruptionBudget, and running one replica on a small cluster.
- **Metrics and debugging**: the scheduler's metrics, its logs and the `MaybeNoGangScheduler` event.
- **Key values**: a table of the chart values an operator changes most.
- **Clean up**: uninstall the chart, and what happens to pods that still name the scheduler.
- **Next steps**: Deployment modes and OMENative, and Set accelerator quotas.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/ome-scheduler.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/ome-scheduler.md -> guides/operate-ome/ome-scheduler.md: b7fa8323`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/ome-scheduler.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Use the OME scheduler guide" -m "The guide installs the alpha ome-scheduler on Kubernetes 1.35, opts a
workload into gang packing, checks the placement and covers tuning,
availability, metrics and the key values."
```

- [ ] **Step 6: Write the Set accelerator quotas page**

`website/src/lib/content/guides/operate-ome/accelerator-quota.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/accelerator-quota.md`. It has no known errors, but check every fact anyway.
- `charts/ome-quota-manager/`: `values.yaml` (`mode` is required, and `crd.install`), `README.md`, the templates and `charts/ome-quota-manager/tests/render_test.sh`.
- `cmd/ome-quota-manager/main.go`: the flags, and what each mode runs.
- `pkg/apis/ome/v1beta1/acceleratorquota_types.go`: the AcceleratorQuota fields and conditions.
- `pkg/controller/v1beta1/acceleratorquota/`: `controller.go`, `materialize.go` (the Kueue objects), `project.go`, `capacity.go`, `capacitycheck.go`, `usage.go` and `metrics.go`.
- `pkg/quota/tree/` and `pkg/quota/backend/kueue/`: the quota tree, and how it becomes Kueue objects.
- `pkg/controller/v1beta1/workload/holds/quota.go`: how a workload waits for quota.
- `cmd/manager/main.go` (about line 280): `--accelerator-resources`, which the chart sets from `ome.controller.quotaAcceleratorResources`.
- `pkg/cli/cmd/quota/`, `pkg/cli/quotacollection/`, `pkg/cli/quotastatusprojection/` and `pkg/cli/quotatreeprojection/`: `kubectl ome quota`.

Cover only single-cluster `mode: workload`, and give management mode one sentence: it's for multi-cluster, which is in development. The YAML check skips the Kueue objects, since their CRDs aren't in `config/crd/full`: check them by hand against the Kueue version in `go.mod`.

Outline, `h2`s in order:

- **Before you begin**: OME installed, Kueue installed, and Helm.
- **How quotas work**: the AcceleratorQuota tree, the budgets it declares and the Kueue objects OME renders from it.
- **Step 1: Install ome-quota-manager**: the Helm command with `mode: workload`, and a check that it's running.
- **Step 2: Write the quota tree**: a complete AcceleratorQuota tree under the reserved root. Say that borrowed overage can't currently be reclaimed.
- **Step 3: Check what Kueue gets**: the Kueue objects OME created from the tree, read with `kubectl get`.
- **Match the accelerator resources**: which resource names count as accelerators, and the Helm value that sets `--accelerator-resources`.
- **Conditions**: a table of the AcceleratorQuota conditions, their reasons and what each means.
- **Inspect with kubectl ome quota**: `status`, `tree` and `validate`, linking kubectl ome quota.
- **Metrics**: the quota manager's metrics, and how they're scraped.
- **RBAC**: what the quota manager's ServiceAccount can do, and what a user needs to write AcceleratorQuotas.
- **Clean up**: delete the tree, uninstall the chart, and what happens to the Kueue objects.
- **Next steps**: kubectl ome quota and Use the OME scheduler.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/accelerator-quota.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/accelerator-quota.md -> guides/operate-ome/accelerator-quota.md: fdc83ca9`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/accelerator-quota.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Set accelerator quotas guide" -m "The guide installs ome-quota-manager in workload mode, writes an
AcceleratorQuota tree, checks what Kueue gets and covers resource
matching, conditions, inspection, metrics and RBAC."
```

- [ ] **Step 7: Write the Share Hugging Face artifacts page**

`website/src/lib/content/guides/operate-ome/shared-hf-artifacts.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/shared-hf-artifacts.md`. It has no known errors, but check every fact anyway.
- `pkg/apis/ome/v1beta1/model.go` (about lines 148 to 161): `downloadPolicy`, with `AlwaysDownload` and `ReuseIfExists`.
- `pkg/modelagent/gopher_artifact_routing.go` and `pkg/modelagent/gopher_hf_artifact.go`: when the agent shares a snapshot, and when it keeps a private copy.
- `pkg/modelagent/hf_artifact_*.go`: the shared store, from identity and download to pending deletion, repair and recovery at startup.
- `pkg/modelagent/artifact_object_filter.go` and `pkg/modelagent/gopher_hf_source.go`: OCI mirrors of Hugging Face snapshots.
- `pkg/modelagent/hf_revision.go` and `pkg/modelagent/hf_snapshot_validation.go`: the revision a model pins, and how a snapshot is validated before it's shared.

`ReuseIfExists` existed in v1.2.2 for OCI models; sharing Hugging Face snapshots is new. Say so in one plain sentence in The downloadPolicy field.

Outline, `h2`s in order:

- **Before you begin**: OME with the model agent running, and two models that use the same Hugging Face repository and revision.
- **The downloadPolicy field**: its values and its default, and what `ReuseIfExists` changes.
- **Step 1: Share a Hugging Face snapshot**: a complete BaseModel and ClusterBaseModel with `downloadPolicy: ReuseIfExists`.
- **Step 2: Check the shared copy on the node**: where the shared copy lives, and how to see that both models use it.
- **Share OCI mirrors of Hugging Face snapshots**: when an OCI model counts as the same snapshot.
- **Reference-counted deletion**: what happens when one of the models is deleted, and when the copy goes away.
- **When the agent falls back to a private copy**: each case, and how to tell it happened.
- **Clean up**: delete the models, and check that the copy is removed.
- **Next steps**: Run the model agent and Base models.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/shared-hf-artifacts.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/shared-hf-artifacts.md -> guides/operate-ome/shared-hf-artifacts.md: ca69dc7d`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/shared-hf-artifacts.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Share Hugging Face artifacts guide" -m "The guide shares one Hugging Face snapshot between models with
downloadPolicy ReuseIfExists, checks the shared copy and covers OCI
mirrors, reference-counted deletion and fallbacks."
```

- [ ] **Step 8: Write the Collect metrics page**

`website/src/lib/content/guides/operate-ome/metrics.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/metrics.md`. It has no known errors, but check every fact anyway.
- `charts/ome-resources/templates/prometheus/`: the Prometheus Deployment, its scrape config in `configmap.yaml`, and its PVC, RBAC, Service and ServiceAccount.
- `charts/ome-resources/values.yaml`: `prometheus` (about line 945), `serviceMonitor` (about line 843) and the comments on the Prometheus server address (about lines 190 to 238).
- `charts/ome-resources/templates/ome-controller/configmap.yaml` (about line 53) and `pkg/controller/v1beta1/controllerconfig/configmap.go` (about lines 480 to 490): the bundled server address the controller uses, which `spec.rollout.canary.prometheus.serverAddress` overrides.
- `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis/querier.go`: how canary analysis queries Prometheus.

Check what stops working when `prometheus.enabled` is false, and say it in Use your own Prometheus.

Outline, `h2`s in order:

- **Before you begin**: OME installed with the `ome-resources` chart, and Helm.
- **What needs it**: KEDA autoscaling on Prometheus metrics and canary analysis, and what each queries.
- **What the chart installs**: the objects the chart renders for Prometheus, and their default retention and storage.
- **What it scrapes**: the scrape jobs in its config, and the ServiceMonitor for the controller.
- **Step 1: Check the targets**: port-forward to the Prometheus Service and list its active targets.
- **Step 2: Tune retention, storage and scope**: the Helm values, and the upgrade command.
- **Use your own Prometheus**: turn the bundled one off, point the controller and canary analysis at yours, and what stops working.
- **Key values**: a table of the `prometheus` and `serviceMonitor` values.
- **Next steps**: Set up alerting and Canary metric analysis.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/metrics.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/metrics.md -> guides/operate-ome/metrics.md: 23c15a51`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/metrics.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Collect metrics guide" -m "The guide explains what the bundled Prometheus is for, what it installs
and scrapes, checks its targets, tunes retention and storage, and
switches OME to your own Prometheus."
```

- [ ] **Step 9: Write the Set up alerting page**

`website/src/lib/content/guides/operate-ome/alerting.md` is a guide. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/administration/alerting.md`. It has no known errors, but check every fact anyway.
- `charts/ome-resources/templates/prometheus/prometheusrule.yaml`: the five alerts, `OMEControllerDown` (about line 43), `OMEWebhookFailing` (about line 66), `OMEServiceUnavailable` (about line 97), `OMEMassRolloutFailure` (about line 127) and `OMEReconcileStalled` (about line 167).
- `charts/ome-resources/values.yaml` (about line 868): `prometheusRule`, off by default, with the pack-wide `additionalLabels`, `interval`, `severity`, `runbookUrlBase` and `selectors`, and each rule's own settings.

A PrometheusRule needs the prometheus-operator CRDs, and the bundled Prometheus may not read PrometheusRule objects at all. Check both, and say which Prometheus evaluates the rules.

Outline, `h2`s in order:

- **Before you begin**: OME installed with the `ome-resources` chart, Helm, and a Prometheus that loads PrometheusRule objects.
- **Step 1: Turn on the alerts**: the Helm values and the upgrade command.
- **Step 2: Check the rules**: the PrometheusRule object, and the rules loaded in your Prometheus.
- **The five alerts**: for each, what it fires on, how long it must hold, what it means and what to check first.
- **Pack-wide settings**: the values that apply to every alert, such as `additionalLabels`, `severity`, `runbookUrlBase` and `selectors`.
- **Default values**: a table of every `prometheusRule` value and its default.
- **Next steps**: Collect metrics and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py guides/operate-ome/alerting.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `administration/alerting.md -> guides/operate-ome/alerting.md: 24480b91`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/operate-ome/alerting.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Set up alerting guide" -m "The guide turns on the five alerts in the ome-resources chart, checks
the PrometheusRule and documents each alert, the pack-wide settings and
the default values."
```

- [ ] **Step 10: Write the Labels and annotations page**

`website/src/lib/content/reference/api/labels-and-annotations.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/labels-and-annotations.md`. It has no known errors, but check every fact anyway.
- `pkg/constants/constants.go`: most keys. Many are built from `OMEAPIGroupName`, so grep the part after the slash too.
- `pkg/constants/traffic_annotations.go` and `pkg/trafficdrain/annotation.go`: the traffic and rollout-action annotations, which are new since v1.2.2.
- `git grep -n '"ome.io/' -- pkg cmd`: keys written as literals.
- `git show 5f1c4096:pkg/constants/constants.go`: the keys v1.2.2 had.

Group the keys by the resource they go on, and say whether OME reads each, sets it, or both. Leave out keys that nothing reads or sets, and list them in your report. Write "Since v1.3." in the row of each key v1.2.2 lacks.

The traffic annotations get one row that links Traffic annotations, which D5 writes in the same wave, without an anchor.

Outline, `h2`s in order:

- **Annotations**: tables by resource, with the columns Key, Values, Read or set by and Description.
- **Labels**: the same tables, including the node labels the model agent sets.
- **Special values**: the autoscaler classes, scale metrics and priority classes the Hugo page lists, checked against the code.
- **Related pages**: Traffic annotations, Configure ingress and Run the model agent.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-rewritten.py reference/api/labels-and-annotations.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/labels-and-annotations.md -> reference/api/labels-and-annotations.md: 4ed42091`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/api/labels-and-annotations.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Labels and annotations page" -m "The reference lists the labels and annotations OME reads and sets,
grouped by the resource they go on, with their values and which
component reads or sets each."
```

- [ ] **Step 11: Check the pages**

Start the dev server on port 5188, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5188 --strictPort > /tmp/d8-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5188/ome/ && break; sleep 1; done
B=http://localhost:5188/ome
for p in guides/operate-ome/configure-the-controller guides/operate-ome/model-agent guides/operate-ome/ome-scheduler guides/operate-ome/accelerator-quota guides/operate-ome/shared-hf-artifacts guides/operate-ome/metrics guides/operate-ome/alerting reference/api/labels-and-annotations; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/operate-ome/configure-the-controller guides/operate-ome/model-agent guides/operate-ome/ome-scheduler guides/operate-ome/accelerator-quota guides/operate-ome/shared-hf-artifacts guides/operate-ome/metrics guides/operate-ome/alerting reference/api/labels-and-annotations; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/operate-ome/configure-the-controller guides/operate-ome/model-agent guides/operate-ome/ome-scheduler guides/operate-ome/accelerator-quota guides/operate-ome/shared-hf-artifacts guides/operate-ome/metrics guides/operate-ome/alerting reference/api/labels-and-annotations; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/operate-ome/configure-the-controller 200 0 0 2
guides/operate-ome/model-agent 200 0 0 0
guides/operate-ome/ome-scheduler 200 0 0 1
guides/operate-ome/accelerator-quota 200 0 0 1
guides/operate-ome/shared-hf-artifacts 200 0 0 1
guides/operate-ome/metrics 200 0 0 1
guides/operate-ome/alerting 200 0 0 1
reference/api/labels-and-annotations 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/operate-ome/configure-the-controller: before-you-begin step-1-set-a-flag step-2-check-the-running-flags flags tune-leader-election tune-reconcile-throughput tune-runtime-revision-garbage-collection next-steps
guides/operate-ome/model-agent: before-you-begin what-the-agent-does step-1-check-the-agent-on-each-node step-2-change-a-setting step-3-check-where-a-model-is-ready flags download-behavior health-checks-and-metrics troubleshooting next-steps
guides/operate-ome/ome-scheduler: before-you-begin how-gang-packing-works step-1-install-the-chart step-2-opt-a-workload-in step-3-check-the-placement tune-packing availability metrics-and-debugging key-values clean-up next-steps
guides/operate-ome/accelerator-quota: before-you-begin how-quotas-work step-1-install-ome-quota-manager step-2-write-the-quota-tree step-3-check-what-kueue-gets match-the-accelerator-resources conditions inspect-with-kubectl-ome-quota metrics rbac clean-up next-steps
guides/operate-ome/shared-hf-artifacts: before-you-begin the-downloadpolicy-field step-1-share-a-hugging-face-snapshot step-2-check-the-shared-copy-on-the-node share-oci-mirrors-of-hugging-face-snapshots reference-counted-deletion when-the-agent-falls-back-to-a-private-copy clean-up next-steps
guides/operate-ome/metrics: before-you-begin what-needs-it what-the-chart-installs what-it-scrapes step-1-check-the-targets step-2-tune-retention-storage-and-scope use-your-own-prometheus key-values next-steps
guides/operate-ome/alerting: before-you-begin step-1-turn-on-the-alerts step-2-check-the-rules the-five-alerts pack-wide-settings default-values next-steps
reference/api/labels-and-annotations: annotations labels special-values related-pages
```

Then the since labels: one line, `7 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `7 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 40`: the 40 written pages include your 8.

Take screenshots of Configure the controller, Set accelerator quotas and Share Hugging Face artifacts at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5188'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 12: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d8-batches.py d8
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d8 (D8): 9 files, 0 not D8's
  administration/controller-configuration.md -> guides/operate-ome/configure-the-controller.md: 248a7463
  administration/model-agent.md -> guides/operate-ome/model-agent.md: 8de82852
  administration/ome-scheduler.md -> guides/operate-ome/ome-scheduler.md: b7fa8323
  administration/accelerator-quota.md -> guides/operate-ome/accelerator-quota.md: fdc83ca9
  administration/shared-hf-artifacts.md -> guides/operate-ome/shared-hf-artifacts.md: ca69dc7d
  administration/metrics.md -> guides/operate-ome/metrics.md: 23c15a51
  administration/alerting.md -> guides/operate-ome/alerting.md: 24480b91
  reference/labels-and-annotations.md -> reference/api/labels-and-annotations.md: 4ed42091
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 11 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D9: kubectl-ome overview and first command references

**Branch:** `docs/website-d9`

**Goal:** Write the kubectl-ome overview and the accelerator, admin, autoscale, cluster, get, instance, logs and migration command references, so a reader can install the plugin and look up any of these commands.

**Files:**

- Rewrite: `website/src/lib/content/reference/kubectl-ome/overview.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/accelerator.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/admin.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/autoscale.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/cluster.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/get.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/instance.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/logs.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/migration.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Modify: `website/src/lib/content/reference/index.md` (the card of kubectl-ome overview and install, whose description changes)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Every page covers a CLI v1.2.2 doesn't have, so each gets `since: v1.3` and no heading badges. Take flags from `--help` on `/tmp/d9-kubectl-ome`, including each family's own global flags other than the kubeconfig ones, and output only from golden files and tests, such as `pkg/cli/cmd/admin/testdata/doctor/`, `pkg/cli/cmd/cluster/testdata/` and `pkg/cli/cmd/get/testdata/`.

Write the overview first, so the other pages can link its headings. RBAC lives only in the overview's Required RBAC section: move the Hugo pages' RBAC facts there, and link it from each page's opening. The alpha subcommands are `instance release-held`, `migration start` and `cluster status`; each gets the Alpha note from [Writing rules](#writing-rules). Instances are OMENative: link the Opt in to OMENative heading on Deployment modes and OMENative.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d9`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d9-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d9-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d9-landing.py check | tail -1
python3 /tmp/d9-batches.py d9
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d9 (D9): 0 files, 0 not D9's
  redirects.json: unchanged, so no page has a rewrittenFrom
  reference/kubectl-ome/accelerator.md: still a draft
  reference/kubectl-ome/accelerator.md: want since: v1.3, got no since
  reference/kubectl-ome/admin.md: still a draft
  reference/kubectl-ome/admin.md: want since: v1.3, got no since
  reference/kubectl-ome/autoscale.md: still a draft
  reference/kubectl-ome/autoscale.md: want since: v1.3, got no since
  reference/kubectl-ome/cluster.md: still a draft
  reference/kubectl-ome/cluster.md: want since: v1.3, got no since
  reference/kubectl-ome/get.md: still a draft
  reference/kubectl-ome/get.md: want since: v1.3, got no since
  reference/kubectl-ome/instance.md: still a draft
  reference/kubectl-ome/instance.md: want since: v1.3, got no since
  reference/kubectl-ome/logs.md: still a draft
  reference/kubectl-ome/logs.md: want since: v1.3, got no since
  reference/kubectl-ome/migration.md: still a draft
  reference/kubectl-ome/migration.md: want since: v1.3, got no since
  reference/kubectl-ome/overview.md: still a draft
  reference/kubectl-ome/overview.md: want since: v1.3, got no since
19 problems
```

- [ ] **Step 3: Write the kubectl-ome overview and install page**

`website/src/lib/content/reference/kubectl-ome/overview.md` is a reference page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges. Change its `description` to "Build and install the kubectl-ome plugin, and see its command families, exit codes and the RBAC that read and action commands need.", since no release ships the plugin yet: v1.2.2, the latest, has no krew manifest or kubectl-ome archives, and change its card on the Reference landing page (`website/src/lib/content/reference/index.md`) to match.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome.md`. Its known errors:
  - The install commands at lines 13 to 30 fetch `ome.yaml` from a GitHub release, but v1.2.2, the latest release, ships no krew manifest and no kubectl-ome archives. The `cli-artifacts` job in `.github/workflows/release.yaml` (about line 356) adds them from the next release, and line 11 says `kubectl krew install ome` works only once the plugin is accepted into the krew index.
- `cmd/kubectl-ome/README.md`: building (about line 36), the command families (about line 101), exit codes (about line 513) and output formats (about line 567). Never copy lines 674 to 917.
- `Makefile` (about lines 362 to 373): `make kubectl-ome`, and how it stamps the version.
- `hack/krew/ome.yaml` and `.github/workflows/release.yaml` (about lines 356 to 400): the krew manifest, and the job that publishes it. Line 22 of the manifest links the old site: report it.
- `README.md:115`: the krew command in the README, which fails until a release ships the plugin.
- `pkg/cli/root.go`: the command families and the global flags.
- `pkg/cli/exitcode/exitcode.go` and `pkg/cli/printers/printers.go`: the exit codes and the output formats.
- `charts/ome-resources/templates/ome-controller/rbac/supplemental_view_role.yaml`: the read role the chart ships.
- The RBAC sections of the other Hugo pages, whose facts move here, besides this page's own at line 86:
  - `site/content/en/docs/tasks/kubectl-ome-accelerator-explain.md:192`
  - `site/content/en/docs/tasks/kubectl-ome-doctor.md:139`
  - `site/content/en/docs/tasks/kubectl-ome-migration.md:167`
  - `site/content/en/docs/tasks/kubectl-ome-rollout-validate.md:168`
  - `site/content/en/docs/tasks/kubectl-ome-traffic-status.md:242`

Check every RBAC rule against the reads in `pkg/cli/factory/` and in each command's code, such as the ConfigMap reads behind `admin recommendations`, `migration history` and `migration start`, and the HPA and KEDA reads behind `autoscale status`.

Outline, `h2`s in order:

- **Install**: an `h3` for building from source with `make kubectl-ome` or `go install`, and an `h3` for krew, which works from the next release. Check the install with `kubectl ome version`.
- **Commands**: a table of the command families, each linking its page.
- **Global flags**: the kubeconfig flags every command takes, such as `-n` and `--context`, from `--help`.
- **Output formats**: what `-o` accepts, and which commands take it.
- **Exit codes**: a table of every code in `pkg/cli/exitcode/exitcode.go` and what it means.
- **Required RBAC**: a ClusterRole with the rules every read command needs, a table of the extra rules some commands need, and one sentence linking Guarded actions for the rule action commands add.
- **Related guides**: Troubleshoot an InferenceService, and the guides that use the commands.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/overview.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome.md -> reference/kubectl-ome/overview.md: 12554876`, then `Checked N objects: 0 problems.` Then `python3 /tmp/d9-landing.py check | tail -1` prints `5 landing pages, 84 cards, 0 problems`. Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/overview.md website/redirects.json website/src/lib/content/reference/index.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl-ome overview page" -m "The overview builds and installs the plugin, lists its command families,
global flags, output formats and exit codes, and gathers the RBAC every
read command needs."
```

- [ ] **Step 4: Write the kubectl ome accelerator page**

`website/src/lib/content/reference/kubectl-ome/accelerator.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-accelerator-explain.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/accelerator/accelerator.go` and `pkg/cli/cmd/accelerator/explain.go`: the command and its flags.
- `pkg/cli/acceleratorprojection/project.go` and `pkg/cli/effective/accelerator.go`: what it compares, and where each value comes from.
- `pkg/cli/report/v1alpha1/accelerator_explain.go`: the report fields.

Outline, `h2`s in order:

- **`explain`**: the accelerator selection an InferenceService declares, compared with the class and resources each component reports.
- **Exit codes**.
- **Related guides**: Select accelerators and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/accelerator.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-accelerator-explain.md -> reference/kubectl-ome/accelerator.md: f2b7476a`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/accelerator.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome accelerator page" -m "The command reference covers accelerator explain: its flags, the fields
it reports for each component, examples and exit codes."
```

- [ ] **Step 5: Write the kubectl ome admin page**

`website/src/lib/content/reference/kubectl-ome/admin.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-doctor.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/admin/admin.go` and `pkg/cli/cmd/admin/doctor.go`: `doctor` and `recommendations`, and the family's own global flags, `--ome-namespace`, `--alfred-namespace`, `--alfred-config-name` and `--alfred-config-key`.
- `pkg/cli/doctorcollection/`, `pkg/cli/doctorprojection/` and `pkg/cli/alfredrecommendations/`: what each subcommand reads and checks.
- `pkg/cli/report/v1alpha1/doctor.go` and `pkg/cli/report/v1alpha1/alfred_recommendations.go`: the report fields.
- `pkg/cli/cmd/admin/testdata/doctor/`: golden output for a complete install, an incomplete one and one with violations, in each output format.
- `charts/ome-alfred/` and `oeps/0008-alfred-gpu-cluster-caretaker/`: Alfred, whose OEP is provisional.

Alfred is alpha: say so where `recommendations` starts, and say what the command prints when Alfred isn't installed.

Outline, `h2`s in order:

- **`doctor`**: what it checks about the installation, and what a violation means.
- **`recommendations`**: Alfred's advisory recommendations, and where the command reads them.
- **Exit codes**.
- **Related guides**: Configure the controller and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/admin.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-doctor.md -> reference/kubectl-ome/admin.md: bc44ee28`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/admin.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome admin page" -m "The command reference covers admin doctor and admin recommendations:
their flags, the fields they report, examples and exit codes."
```

- [ ] **Step 6: Write the kubectl ome autoscale page**

`website/src/lib/content/reference/kubectl-ome/autoscale.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-autoscale-status.md`. It has no known errors, but check every fact anyway.
- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-autoscale-explain.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/autoscale/autoscale.go`, `pkg/cli/cmd/autoscale/status.go`, `pkg/cli/cmd/autoscale/status_live.go`, `pkg/cli/cmd/autoscale/status_scaler_reader.go` and `pkg/cli/cmd/autoscale/explain.go`: the commands and their flags.
- `pkg/cli/autoscaleprojection/` and `pkg/cli/effective/autoscale.go`: how each report is built, and which layer supplies the autoscaler.
- `pkg/cli/report/v1alpha1/autoscale_status.go` and `pkg/cli/report/v1alpha1/autoscale_explain.go`: the report fields.

Outline, `h2`s in order:

- **`status`**: the ISSUES rows, `--live-scale`, `--live-scaler` and where each value comes from.
- **`explain`**: the effective layer, how the evidence is matched, and a table of the WHY codes.
- **Exit codes**.
- **Related guides**: Autoscaler policy and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/autoscale.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected:

```text
tasks/kubectl-ome-autoscale-status.md -> reference/kubectl-ome/autoscale.md: ccadb15f
tasks/kubectl-ome-autoscale-explain.md -> reference/kubectl-ome/autoscale.md: cd28d69a
```

then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/autoscale.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome autoscale page" -m "The command reference covers autoscale status and autoscale explain:
their flags, the fields they report, the WHY codes, examples and exit
codes."
```

- [ ] **Step 7: Write the kubectl ome cluster page**

`website/src/lib/content/reference/kubectl-ome/cluster.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: replace `status: draft` with `status: preview`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/cluster/cluster.go`: the command and its flags.
- `pkg/cli/cmd/cluster/cluster_test.go` and `pkg/cli/cmd/cluster/testdata/synthetic_workloadcluster.json`: the output, from the test expectations, since the command has no golden file.
- `pkg/cli/clusterstatus/` and `pkg/cli/report/v1alpha1/cluster_status.go`: how the report is built, and its fields.
- `pkg/apis/ome/v1beta1/workloadcluster_types.go`: the WorkloadCluster status the command reads.

WorkloadCluster is alpha, and the multi-cluster reconcilers are off by default (`cmd/manager/main.go:257-258`): the opening says so, and how to turn them on.

Outline, `h2`s in order:

- **`status`**: the Alpha note, and what the command reports for one cluster or a bounded list.
- **Exit codes**.
- **Related guides**: Configure routing health probes, Drain a workload cluster and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/cluster.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome cluster page" -m "The command reference covers the alpha cluster status command: its
flags, the WorkloadCluster fields it reports, examples and exit codes."
```

- [ ] **Step 8: Write the kubectl ome get page**

`website/src/lib/content/reference/kubectl-ome/get.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/get/get.go`, `pkg/cli/cmd/get/registry.go` and `pkg/cli/cmd/get/autoscalerpolicy.go`: the resources `get` knows, their columns and the merged views.
- `pkg/cli/printers/printers.go` and `pkg/cli/printers/table.go`: the output formats.
- `pkg/cli/cmd/get/testdata/isvc_list.golden`, `pkg/cli/cmd/get/testdata/models_merged.golden` and the tests in `pkg/cli/cmd/get/`: the output.

Outline, `h2`s in order:

- **Resources**: a table of the resources `get` lists, the names it accepts for each, and which views merge the namespaced and cluster-scoped kinds.
- **Flags**.
- **Output fields**: the columns for each resource.
- **Examples**.
- **Exit codes**.
- **Related guides**: Serve your first model and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/get.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome get page" -m "The command reference covers get: the resources it lists, the merged
model and runtime views, its flags, columns, examples and exit codes."
```

- [ ] **Step 9: Write the kubectl ome instance page**

`website/src/lib/content/reference/kubectl-ome/instance.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/instance/`: `list`, `status`, `retry-blocks` and `release-held`, and their flags.
- `pkg/cli/instancecollection/`, `pkg/cli/instanceprojection/`, `pkg/cli/instancestatusprojection/` and `pkg/cli/retryblockprojection/`: how each report is built.
- `pkg/cli/mutate/held_release.go`, `pkg/cli/mutate/held_release_runtime.go` and `pkg/cli/mutate/held_release_source.go`: the checks `release-held` makes before it writes.
- `pkg/cli/report/v1alpha1/instance_list.go`, `pkg/cli/report/v1alpha1/instance_status.go` and `pkg/cli/report/v1alpha1/instance_retry_blocks.go`: the report fields.

Outline, `h2`s in order:

- **`list`**: the logical instances of an InferenceService.
- **`status`**: one instance in detail.
- **`retry-blocks`**: the retry blocks on an instance, and what each means.
- **`release-held`**: the Alpha note, and links to Release a held revision and Guarded actions.
- **Exit codes**.
- **Related guides**: Release a held revision and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/instance.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome instance page" -m "The command reference covers instance list, status, retry-blocks and the
alpha release-held action: their flags, fields, examples and exit codes."
```

- [ ] **Step 10: Write the kubectl ome logs page**

`website/src/lib/content/reference/kubectl-ome/logs.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-logs.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/logs/logs.go`: the command, its flags and how it selects pods.
- `pkg/cli/cmd/logs/consume.go` and `pkg/cli/cmd/logs/multiplex.go`: how it streams from several pods at once.
- The tests in `pkg/cli/cmd/logs/`: the flag combinations it rejects, and their messages.

Outline, `h2`s in order:

- **Select the pods**: by component, OMENative instance or revision, and what happens when nothing matches.
- **Flags**.
- **Examples**.
- **Validation errors**: a table of the flag combinations the command rejects, with each message.
- **Exit codes**.
- **Related guides**: Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/logs.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-logs.md -> reference/kubectl-ome/logs.md: df178c73`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/logs.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome logs page" -m "The command reference covers logs: how it selects pods, its flags,
examples, the flag combinations it rejects and its exit codes."
```

- [ ] **Step 11: Write the kubectl ome migration page**

`website/src/lib/content/reference/kubectl-ome/migration.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-migration.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/migration/`: `status`, `history` and `start`, and their flags.
- `pkg/cli/migrationcollection/`, `pkg/cli/migrationprojection/`, `pkg/cli/migrationhistorycollection/` and `pkg/cli/migrationhistoryprojection/`: the live evidence, and the audit ConfigMap `history` reads.
- `pkg/cli/mutate/migration.go`, `pkg/cli/mutate/migration_preview.go` and `pkg/cli/mutate/migration_evidence.go`: the checks `start` makes before it writes.
- `pkg/cli/report/v1alpha1/migration_status.go` and `pkg/cli/report/v1alpha1/migration_history.go`: the report fields.

Outline, `h2`s in order:

- **`status`**: live migration evidence for an InferenceService.
- **`history`**: past migrations, from the audit ConfigMap.
- **`start`**: the Alpha note, and links to Request an instance migration and Guarded actions.
- **Exit codes**.
- **Related guides**: Request an instance migration and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-rewritten.py reference/kubectl-ome/migration.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-migration.md -> reference/kubectl-ome/migration.md: 2cebb0fc`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/migration.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome migration page" -m "The command reference covers migration status, history and the alpha
start action: their flags, the fields they report, examples and exit
codes."
```

- [ ] **Step 12: Check the pages**

Start the dev server on port 5189, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5189 --strictPort > /tmp/d9-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5189/ome/ && break; sleep 1; done
B=http://localhost:5189/ome
for p in reference/kubectl-ome/overview reference/kubectl-ome/accelerator reference/kubectl-ome/admin reference/kubectl-ome/autoscale reference/kubectl-ome/cluster reference/kubectl-ome/get reference/kubectl-ome/instance reference/kubectl-ome/logs reference/kubectl-ome/migration; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in reference/kubectl-ome/overview reference/kubectl-ome/accelerator reference/kubectl-ome/admin reference/kubectl-ome/autoscale reference/kubectl-ome/cluster reference/kubectl-ome/get reference/kubectl-ome/instance reference/kubectl-ome/logs reference/kubectl-ome/migration; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in reference/kubectl-ome/overview reference/kubectl-ome/accelerator reference/kubectl-ome/admin reference/kubectl-ome/autoscale reference/kubectl-ome/cluster reference/kubectl-ome/get reference/kubectl-ome/instance reference/kubectl-ome/logs reference/kubectl-ome/migration; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
reference/kubectl-ome/overview 200 0 0 1
reference/kubectl-ome/accelerator 200 0 0 1
reference/kubectl-ome/admin 200 0 0 1
reference/kubectl-ome/autoscale 200 0 0 1
reference/kubectl-ome/cluster 200 0 1 1
reference/kubectl-ome/get 200 0 0 1
reference/kubectl-ome/instance 200 0 0 1
reference/kubectl-ome/logs 200 0 0 1
reference/kubectl-ome/migration 200 0 0 1
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
reference/kubectl-ome/overview: install commands global-flags output-formats exit-codes required-rbac related-guides
reference/kubectl-ome/accelerator: explain exit-codes related-guides
reference/kubectl-ome/admin: doctor recommendations exit-codes related-guides
reference/kubectl-ome/autoscale: status explain exit-codes related-guides
reference/kubectl-ome/cluster: status exit-codes related-guides
reference/kubectl-ome/get: resources flags output-fields examples exit-codes related-guides
reference/kubectl-ome/instance: list status retry-blocks release-held exit-codes related-guides
reference/kubectl-ome/logs: select-the-pods flags examples validation-errors exit-codes related-guides
reference/kubectl-ome/migration: status history start exit-codes related-guides
```

Then the since labels: one line, `9 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `9 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 41`: the 41 written pages include your 9.

Take screenshots of kubectl-ome overview and install, kubectl ome get and kubectl ome instance at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5189'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 13: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d9-batches.py d9
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d9 (D9): 11 files, 0 not D9's
  tasks/kubectl-ome.md -> reference/kubectl-ome/overview.md: 12554876
  tasks/kubectl-ome-accelerator-explain.md -> reference/kubectl-ome/accelerator.md: f2b7476a
  tasks/kubectl-ome-doctor.md -> reference/kubectl-ome/admin.md: bc44ee28
  tasks/kubectl-ome-autoscale-status.md -> reference/kubectl-ome/autoscale.md: ccadb15f
  tasks/kubectl-ome-autoscale-explain.md -> reference/kubectl-ome/autoscale.md: cd28d69a
  tasks/kubectl-ome-logs.md -> reference/kubectl-ome/logs.md: df178c73
  tasks/kubectl-ome-migration.md -> reference/kubectl-ome/migration.md: 2cebb0fc
0 problems
```

with one more file for each other landing page you changed. The file count includes the landing page your pages' new descriptions change. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 12 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D10: Remaining kubectl ome command references

**Branch:** `docs/website-d10`

**Goal:** Write the placement, quota, runtime, scale, status, traffic, version and wait command references and the Guarded actions contract, so a reader can look up any of these commands and what every mutating command guarantees.

**Files:**

- Rewrite: `website/src/lib/content/reference/kubectl-ome/placement.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/quota.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/runtime.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/scale.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/status.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/traffic.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/version.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/wait.md`
- Rewrite: `website/src/lib/content/reference/kubectl-ome/guarded-actions.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 7 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Every page gets `since: v1.3` and no heading badges. Take flags from `--help` on `/tmp/d10-kubectl-ome`, including each family's own global flags other than the kubeconfig ones, and output only from golden files and tests, such as `pkg/cli/cmd/status/testdata/` and `pkg/cli/trafficprojection/testdata/explain/`.

RBAC lives in the overview's Required RBAC section, which D9 writes in the same wave: link the overview without an anchor. Guarded actions keeps the rule action commands add. The alpha commands are `scale`, `runtime sync`, `traffic drain` and `traffic undrain`, each with the Alpha note; `scale` has no subcommands, so its note goes right after the opening paragraph.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d10`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d10-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d10-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d10-landing.py check | tail -1
python3 /tmp/d10-batches.py d10
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d10 (D10): 0 files, 0 not D10's
  redirects.json: unchanged, so no page has a rewrittenFrom
  reference/kubectl-ome/guarded-actions.md: still a draft
  reference/kubectl-ome/guarded-actions.md: want since: v1.3, got no since
  reference/kubectl-ome/placement.md: still a draft
  reference/kubectl-ome/placement.md: want since: v1.3, got no since
  reference/kubectl-ome/quota.md: still a draft
  reference/kubectl-ome/quota.md: want since: v1.3, got no since
  reference/kubectl-ome/runtime.md: still a draft
  reference/kubectl-ome/runtime.md: want since: v1.3, got no since
  reference/kubectl-ome/scale.md: still a draft
  reference/kubectl-ome/scale.md: want since: v1.3, got no since
  reference/kubectl-ome/status.md: still a draft
  reference/kubectl-ome/status.md: want since: v1.3, got no since
  reference/kubectl-ome/traffic.md: still a draft
  reference/kubectl-ome/traffic.md: want since: v1.3, got no since
  reference/kubectl-ome/version.md: still a draft
  reference/kubectl-ome/version.md: want since: v1.3, got no since
  reference/kubectl-ome/wait.md: still a draft
  reference/kubectl-ome/wait.md: want since: v1.3, got no since
19 problems
```

- [ ] **Step 3: Write the kubectl ome placement page**

`website/src/lib/content/reference/kubectl-ome/placement.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: replace `status: draft` with `status: preview`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/placement/placement.go`: `status`, `explain` and `endpoint`, and their flags.
- `pkg/cli/cmd/placement/placement_test.go`: the output, from the test expectations.
- `pkg/cli/placementcollection/` and `pkg/cli/placementprojection/`: how each report is built.
- `pkg/apis/ome/v1beta1/placement_types.go` and `pkg/apis/ome/v1beta1/workloadcluster_types.go`: the fields the command reads.
- `pkg/cli/report/v1alpha1/placement.go`: the report fields.

Multi-cluster placement is alpha, and its reconcilers are off by default (`cmd/manager/main.go:257-258`): the opening says so, and how to turn them on.

Outline, `h2`s in order:

- **`status`**: the placement the controller reports for an InferenceService.
- **`explain`**: the selector and registry evidence behind it.
- **`endpoint`**: the routing origins.
- **Exit codes**.
- **Related guides**: Configure routing health probes, Drain a workload cluster and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/placement.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome placement page" -m "The command reference covers the alpha placement status, explain and
endpoint commands: their flags, the fields they report, examples and
exit codes."
```

- [ ] **Step 4: Write the kubectl ome quota page**

`website/src/lib/content/reference/kubectl-ome/quota.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/quota/diagnostics.go` and `pkg/cli/cmd/quota/tree.go`: `status`, `tree` and `validate`, and their flags.
- `pkg/cli/quotacollection/`, `pkg/cli/quotastatusprojection/` and `pkg/cli/quotatreeprojection/`: how each report is built.
- `pkg/quota/tree/violation.go`: the violations `validate` reports.
- `pkg/cli/report/v1alpha1/quota_status.go` and `pkg/cli/report/v1alpha1/quota_tree.go`: the report fields.

Outline, `h2`s in order:

- **`status`**: the reported budgets and materialization.
- **`tree`**: the declared AcceleratorQuota tree.
- **`validate`**: the checks, and exit code 2 when the topology is invalid, including an empty one.
- **Exit codes**.
- **Related guides**: Set accelerator quotas and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/quota.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome quota page" -m "The command reference covers quota status, tree and validate: their
flags, the fields they report, examples and the exit code validate
returns."
```

- [ ] **Step 5: Write the kubectl ome runtime page**

`website/src/lib/content/reference/kubectl-ome/runtime.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-runtime-effective.md`. It has no known errors, but check every fact anyway.
- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-runtime-tree.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/runtime/`: `explain`, `effective`, `tree`, `history` and `sync`, and their flags. `explain` has no `--output`; it takes `--isvc`, `--model`, `--ome-namespace` and `--with-effective`.
- `pkg/cli/effective/`: the effective runtime, layer by layer.
- `pkg/cli/runtimecollection/`, `pkg/cli/runtimeprojection/`, `pkg/cli/runtimetreeprojection/`, `pkg/cli/runtimegraph/` and `pkg/cli/runtimeusage/`: how each report is built.
- `pkg/cli/mutate/runtime_sync.go`, `pkg/cli/mutate/runtime_sync_preview.go` and `pkg/cli/mutate/runtime_sync_replicas.go`: the checks `sync` makes before it writes.
- `pkg/cli/report/v1alpha1/runtime_common.go`, `pkg/cli/report/v1alpha1/runtime_effective.go`, `pkg/cli/report/v1alpha1/runtime_history.go` and `pkg/cli/report/v1alpha1/runtime_tree.go`: the report fields.

Outline, `h2`s in order:

- **`explain`**: which runtimes match a model, and why the others don't.
- **`effective`**: the runtime an InferenceService ends up with, layer by layer.
- **`tree`**: the inheritance tree behind it.
- **`history`**: the runtime revisions behind an InferenceService.
- **`sync`**: the Alpha note, and links to Runtime revisions and pinning and Guarded actions.
- **Exit codes**.
- **Related guides**: Troubleshoot runtime selection, Runtime revisions and pinning, and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-rewritten.py reference/kubectl-ome/runtime.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected:

```text
tasks/kubectl-ome-runtime-effective.md -> reference/kubectl-ome/runtime.md: 4fffe042
tasks/kubectl-ome-runtime-tree.md -> reference/kubectl-ome/runtime.md: b42bddd6
```

then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/runtime.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome runtime page" -m "The command reference covers runtime explain, effective, tree, history
and the alpha sync action: their flags, fields, examples and exit codes."
```

- [ ] **Step 6: Write the kubectl ome scale page**

`website/src/lib/content/reference/kubectl-ome/scale.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/scale/scale.go` and `pkg/cli/cmd/scale/collect.go`: the command and its flags, `--replicas`, `--component`, `--override-autoscaler`, `--ome-namespace`, `--dry-run`, `--yes` and `--output`.
- `pkg/cli/mutate/scale.go`, `pkg/cli/mutate/scale_preview.go`, `pkg/cli/mutate/scale_evidence.go` and `pkg/cli/mutate/scale_pinned.go`: the checks before the write, and the preview.
- `pkg/cli/transport/inference_replica_scale.go`: the write, one guarded JSON Patch to the InferenceReplica's `/scale` subresource, never a PUT and never the parent InferenceService.
- `pkg/cli/report/v1alpha1/scale_action.go` and `pkg/cli/report/v1alpha1/action_result.go`: the result fields.
- The tests in `pkg/cli/cmd/scale/`: the output.

Outline, `h2`s in order:

- **What the request changes**: the InferenceReplica's scale subresource, what the controller does with it and what ends the request.
- **Flags**.
- **Output fields**: the ActionResult fields for a scale.
- **Examples**: a dry run, then the request.
- **Exit codes**.
- **Related guides**: Request a transient scale, Guarded actions and Troubleshoot an InferenceService.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/scale.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome scale page" -m "The command reference covers the alpha scale action: what the request
changes, its flags, the ActionResult fields, examples and exit codes."
```

- [ ] **Step 7: Write the kubectl ome status page**

`website/src/lib/content/reference/kubectl-ome/status.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-status.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/status/status.go`, `pkg/cli/cmd/status/gather.go`, `pkg/cli/cmd/status/gather_integrations.go`, `pkg/cli/cmd/status/placement_integration.go`, `pkg/cli/cmd/status/project.go` and `pkg/cli/cmd/status/render.go`: what the command gathers, how far each read goes and how it renders.
- `pkg/cli/report/v1alpha1/status.go`, `pkg/cli/report/v1alpha1/status_autoscale.go`, `pkg/cli/report/v1alpha1/status_integrations.go` and `pkg/cli/report/v1alpha1/status_placement.go`: the report fields.
- `pkg/cli/cmd/status/testdata/status_notready.golden` and `pkg/cli/cmd/status/testdata/status_unlabeled.golden`: the output.

Outline, `h2`s in order:

- **Flags**.
- **Output fields**: each section of the report, and where its values come from.
- **Observation bounds**: how many pods, events and objects the command reads, and what it prints when it stops early.
- **Examples**.
- **When to use a detail command**: a table from each section to the command that explains it, such as `runtime explain` or `traffic explain`.
- **Exit codes**.
- **Related guides**: Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-rewritten.py reference/kubectl-ome/status.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-status.md -> reference/kubectl-ome/status.md: 1de07d77`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/status.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome status page" -m "The command reference covers status: its flags, each section of the
report, how far it reads, examples, the detail commands and exit codes."
```

- [ ] **Step 8: Write the kubectl ome traffic page**

`website/src/lib/content/reference/kubectl-ome/traffic.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-traffic-explain.md`. It has no known errors, but check every fact anyway.
- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-traffic-status.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/traffic/traffic.go`, `pkg/cli/cmd/traffic/status.go`, `pkg/cli/cmd/traffic/explain.go` and `pkg/cli/cmd/traffic/actions.go`: the commands and their flags. `drain` takes `--workload-cluster`, `--id`, `--reason`, `--dry-run`, `--yes` and `--output`; `undrain` takes only `--id`, `--dry-run`, `--yes` and `--output`.
- `pkg/cli/trafficprojection/` and `pkg/cli/trafficprojection/testdata/explain/README.md`, with its cases: how each report is built, and the output.
- `pkg/cli/mutate/traffic_drain.go` and `pkg/trafficdrain/annotation.go`: the checks before a drain, and the annotation it writes.
- `pkg/cli/report/v1alpha1/traffic_status.go`, `pkg/cli/report/v1alpha1/traffic_explain.go` and `pkg/cli/report/v1alpha1/traffic_action.go`: the report fields.

Outline, `h2`s in order:

- **`status`**: the routes, weights and canary split the controller reports.
- **`explain`**: the declared traffic behavior, checked against what the controller reports.
- **`drain` and `undrain`**: the Alpha note, both commands and their flags, and a link to Drain a workload cluster.
- **Exit codes**.
- **Related guides**: Drain a workload cluster, Traffic policy and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-rewritten.py reference/kubectl-ome/traffic.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected:

```text
tasks/kubectl-ome-traffic-explain.md -> reference/kubectl-ome/traffic.md: a30a5e94
tasks/kubectl-ome-traffic-status.md -> reference/kubectl-ome/traffic.md: 6228e18f
```

then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/traffic.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome traffic page" -m "The command reference covers traffic status, explain and the alpha drain
and undrain actions: their flags, fields, examples and exit codes."
```

- [ ] **Step 9: Write the kubectl ome version page**

`website/src/lib/content/reference/kubectl-ome/version.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `pkg/cli/cmd/version/version.go` and `pkg/cli/cmd/version/version_test.go`: what it prints, and how it reads the operator version from the manager image of the `ome-controller-manager` Deployment.
- `Makefile`: how a build stamps the plugin version.

Outline, `h2`s in order:

- **What it prints**: the plugin version, the operator version and where it comes from, and what it prints when it can't read the Deployment.
- **Flags**: only `--ome-namespace`.
- **Examples**.
- **Exit codes**.
- **Related guides**: kubectl-ome overview and install.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/version.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome version page" -m "The command reference covers version: the plugin and operator versions
it prints, where the operator version comes from, and its exit codes."
```

- [ ] **Step 10: Write the kubectl ome wait page**

`website/src/lib/content/reference/kubectl-ome/wait.md` is a command reference in the [CLI page format](#cli-page-format). Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/kubectl-ome-wait.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/cmd/wait/wait.go`: the predicates (about line 32), the flags and the help examples (about line 143).
- `pkg/cli/cmd/wait/held_revision.go`, `pkg/cli/cmd/wait/ready_replicas.go`, `pkg/cli/cmd/wait/runtime_sync.go` and `pkg/cli/cmd/wait/scale_current.go`: the predicates that need more than a condition.
- `pkg/cli/waitengine/`, `pkg/cli/waitpredicate/` and `pkg/cli/waitsource/`: how `wait` reads, how often, and when it gives up.
- `pkg/cli/waitheld/`, `pkg/cli/waitmigration/`, `pkg/cli/waitrollout/`, `pkg/cli/waitruntime/` and `pkg/cli/waitscale/`: what each predicate checks.
- `pkg/cli/report/v1alpha1/wait_*.go`: the report fields.

Outline, `h2`s in order:

- **Predicates**: a table of every predicate, what it waits for and the flags it needs.
- **How wait observes**: how it reads the object, how often, how it times out and what happens when the object goes away.
- **Flags**.
- **Output fields**.
- **Examples**.
- **What a match does not mean**: what each predicate proves and what it doesn't, so scripts don't read more into a match.
- **Exit codes**.
- **Related guides**: Release a held revision, Request a transient scale and Troubleshoot an InferenceService.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-rewritten.py reference/kubectl-ome/wait.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/kubectl-ome-wait.md -> reference/kubectl-ome/wait.md: eb27c26f`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/wait.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the kubectl ome wait page" -m "The command reference covers wait: every predicate, how it observes, its
flags, fields, examples, what a match does not mean and exit codes."
```

- [ ] **Step 11: Write the Guarded actions page**

`website/src/lib/content/reference/kubectl-ome/guarded-actions.md` is a reference page. Front matter: remove `status: draft`, and add `since: v1.3`, since v1.2.2 has none of it, so its headings carry no since badges.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/kubectl-ome-guarded-actions.md`. It has no known errors, but check every fact anyway.
- `pkg/cli/mutate/preview.go`, `pkg/cli/mutate/confirmation.go`, `pkg/cli/mutate/bounds.go`, `pkg/cli/mutate/target.go` and `pkg/cli/mutate/patch_error.go`: the shared sequence, the dry-run modes, the prompt and how a failed patch is reported.
- `pkg/cli/actionbounds/bounds.go`: the timeouts and response bounds.
- `pkg/cli/report/v1alpha1/action_result.go` and `pkg/cli/exitcode/exitcode.go`: the ActionResult fields and the exit codes.
- `pkg/cli/transport/`, `pkg/cli/factory/action_reads.go` and `pkg/cli/factory/action_runtime.go`: how an action reads and writes.
- `pkg/cli/cmd/rollout/actions.go`, `pkg/cli/cmd/rollout/repin.go`, `pkg/cli/cmd/traffic/actions.go`, `pkg/cli/cmd/migration/start.go`, `pkg/cli/cmd/scale/scale.go`, `pkg/cli/cmd/runtime/sync.go` and `pkg/cli/cmd/instance/release_held.go`: every guarded command.
- `oeps/0011.1-kubectl-ome-operations/README.md`: the design, which is provisional. Where it and the code differ, the code wins.

Outline, `h2`s in order:

- **Guarded commands**: a table of every guarded command, what it writes and where.
- **The shared sequence**: the steps every guarded command runs, in order, from reading the target to reporting the result.
- **Dry-run modes**: what each `--dry-run` value does.
- **The confirmation prompt**: when it appears, what it shows and how `--yes` skips it.
- **The ActionResult**: a table of its fields.
- **Exit codes**: which code each outcome returns.
- **Timeouts and response bounds**: from `pkg/cli/actionbounds/bounds.go`.
- **Required RBAC**: a ClusterRole with the rule action commands add on top of the read role on kubectl-ome overview and install.
- **Related pages**: kubectl-ome overview and install, and the guides that use each command.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-rewritten.py reference/kubectl-ome/guarded-actions.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/kubectl-ome-guarded-actions.md -> reference/kubectl-ome/guarded-actions.md: 6c277aac`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/kubectl-ome/guarded-actions.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Guarded actions reference" -m "The reference covers the contract every mutating command follows: the
shared sequence, dry-run modes, the prompt, the ActionResult, exit
codes, bounds and RBAC."
```

- [ ] **Step 12: Check the pages**

Start the dev server on port 5190, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5190 --strictPort > /tmp/d10-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5190/ome/ && break; sleep 1; done
B=http://localhost:5190/ome
for p in reference/kubectl-ome/placement reference/kubectl-ome/quota reference/kubectl-ome/runtime reference/kubectl-ome/scale reference/kubectl-ome/status reference/kubectl-ome/traffic reference/kubectl-ome/version reference/kubectl-ome/wait reference/kubectl-ome/guarded-actions; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in reference/kubectl-ome/placement reference/kubectl-ome/quota reference/kubectl-ome/runtime reference/kubectl-ome/scale reference/kubectl-ome/status reference/kubectl-ome/traffic reference/kubectl-ome/version reference/kubectl-ome/wait reference/kubectl-ome/guarded-actions; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in reference/kubectl-ome/placement reference/kubectl-ome/quota reference/kubectl-ome/runtime reference/kubectl-ome/scale reference/kubectl-ome/status reference/kubectl-ome/traffic reference/kubectl-ome/version reference/kubectl-ome/wait reference/kubectl-ome/guarded-actions; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
reference/kubectl-ome/placement 200 0 1 1
reference/kubectl-ome/quota 200 0 0 1
reference/kubectl-ome/runtime 200 0 0 1
reference/kubectl-ome/scale 200 0 0 1
reference/kubectl-ome/status 200 0 0 1
reference/kubectl-ome/traffic 200 0 0 1
reference/kubectl-ome/version 200 0 0 1
reference/kubectl-ome/wait 200 0 0 1
reference/kubectl-ome/guarded-actions 200 0 0 1
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
reference/kubectl-ome/placement: status explain endpoint exit-codes related-guides
reference/kubectl-ome/quota: status tree validate exit-codes related-guides
reference/kubectl-ome/runtime: explain effective tree history sync exit-codes related-guides
reference/kubectl-ome/scale: what-the-request-changes flags output-fields examples exit-codes related-guides
reference/kubectl-ome/status: flags output-fields observation-bounds examples when-to-use-a-detail-command exit-codes related-guides
reference/kubectl-ome/traffic: status explain drain-and-undrain exit-codes related-guides
reference/kubectl-ome/version: what-it-prints flags examples exit-codes related-guides
reference/kubectl-ome/wait: predicates how-wait-observes flags output-fields examples what-a-match-does-not-mean exit-codes related-guides
reference/kubectl-ome/guarded-actions: guarded-commands the-shared-sequence dry-run-modes the-confirmation-prompt the-actionresult exit-codes timeouts-and-response-bounds required-rbac related-pages
```

Then the since labels: one line, `9 Unreleased: coming in v1.3` with the count padded by `uniq -c`, or `9 Since v1.3` when the GitHub API limit has run out (see Task B1 Step 8); say which you saw. Last, `89 41`: the 41 written pages include your 9.

Take screenshots of kubectl ome status, kubectl ome wait and Guarded actions at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5190'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 13: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d10-batches.py d10
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d10 (D10): 10 files, 0 not D10's
  tasks/kubectl-ome-runtime-effective.md -> reference/kubectl-ome/runtime.md: 4fffe042
  tasks/kubectl-ome-runtime-tree.md -> reference/kubectl-ome/runtime.md: b42bddd6
  tasks/kubectl-ome-status.md -> reference/kubectl-ome/status.md: 1de07d77
  tasks/kubectl-ome-traffic-explain.md -> reference/kubectl-ome/traffic.md: a30a5e94
  tasks/kubectl-ome-traffic-status.md -> reference/kubectl-ome/traffic.md: 6228e18f
  tasks/kubectl-ome-wait.md -> reference/kubectl-ome/wait.md: eb27c26f
  reference/kubectl-ome-guarded-actions.md -> reference/kubectl-ome/guarded-actions.md: 6c277aac
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 12 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

### Task D11: Benchmark, storage and contributing pages

**Branch:** `docs/website-d11`

**Goal:** Rewrite Serve models from node-local storage, Run benchmarks, Benchmark output storage and Set up a development environment, and write the new Pull requests and OEPs page.

**Files:**

- Rewrite: `website/src/lib/content/guides/deploy-models/serve-models-from-local-storage.md`
- Rewrite: `website/src/lib/content/guides/deploy-models/run-benchmarks.md`
- Rewrite: `website/src/lib/content/reference/storage/benchmark-output-storage.md`
- Rewrite: `website/src/lib/content/contributing/development-setup.md`
- Rewrite: `website/src/lib/content/contributing/pull-requests-and-oeps.md`
- Modify: `website/redirects.json` (the `rewrittenFrom` of the 4 entries whose `new` is one of these pages, nothing else)
- Maybe modify: a section landing page, only to match a `title` or `description` you change

Keep each page's `title` and `description` unless its step changes them or you find them wrong. Follow [Writing rules](#writing-rules) and [Writing a page](#writing-a-page) for every page.

Wave 1's pages are written on your branch, so you can link their headings, as well as those of the pages Phases B and C wrote: the five section landing pages, `guides/deploy-models/serve-models-from-pvc.md`, `concepts/models/base-models.md`, `reference/kubectl-ome/rollout.md`, `reference/api/ome.v1beta1.md` and `contributing/writing-docs.md`. The other wave 2 batches' pages are drafts on your branch: link them without anchors.

Most of what these pages cover existed in v1.2.2, so they have no since badges. The steps name the few sentences that need "Since v1.3." in plain text.

- [ ] **Step 1: Set up the worktree**

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
(cd website && export PATH=/tmp/ome-tools/bin:$PATH && pnpm install --frozen-lockfile --offline)
```

Expected: `docs/website-d11`, then pnpm installs every package from its store without downloading anything.

- [ ] **Step 2: Save the tools**

Save `landing.py`, `rewritten.py` and `batches.py` straight from this plan, build the CLI, and check where the branch starts:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 - <<'EOF'
import re
plan = open('docs/superpowers/plans/2026-09-26-docs-site-redesign.md').read()
fences = re.findall(r'^(`{3,})python\n(.*?)^\1$', plan, re.M | re.S)
for name, marker in [('landing', 'Prints and checks the card grids'), ('rewritten', 'Sets rewrittenFrom on the redirects.json'), ('batches', 'Checks that each Phase D batch changed only what the batch owns')]:
    [code] = [body for _, body in fences if marker in body]
    open(f'/tmp/d11-{name}.py', 'w').write(code)
EOF
CGO_ENABLED=0 go build -o /tmp/d11-kubectl-ome ./cmd/kubectl-ome
python3 /tmp/d11-landing.py check | tail -1
python3 /tmp/d11-batches.py d11
```

Expected: `5 landing pages, 84 cards, 0 problems`, then:

```text
docs/website-d11 (D11): 0 files, 0 not D11's
  redirects.json: unchanged, so no page has a rewrittenFrom
  contributing/development-setup.md: still a draft
  contributing/pull-requests-and-oeps.md: still a draft
  guides/deploy-models/run-benchmarks.md: still a draft
  guides/deploy-models/serve-models-from-local-storage.md: still a draft
  reference/storage/benchmark-output-storage.md: still a draft
6 problems
```

- [ ] **Step 3: Write the Serve models from node-local storage page**

`website/src/lib/content/guides/deploy-models/serve-models-from-local-storage.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/serve-models-from-local-storage.md`. It has no known errors, but check every fact anyway.
- `pkg/utils/storage/storage.go`: the `local://` prefix (about line 28), the parse errors (about lines 520 to 526) and `ValidateLocalStorageURI` (about line 536).
- `pkg/apis/ome/v1beta1/model.go` (about lines 102 to 146): `storage.storageUri`, `storage.path`, `storage.nodeSelector` and `storage.nodeAffinity`.
- `pkg/modelagent/gopher.go`: `processLocalStorageModel`, which validates the files in place (about line 578), and the delete path, which never removes local files (about line 694).
- `pkg/controller/v1beta1/inferenceservice/components/base.go` (about lines 500 to 520): `UpdatePodSpecVolumes`, which mounts the model on the pod.

The agent checks `storage.path` when it's set, and the URI's path otherwise, but the pod's host path volume comes only from `storage.path` (`UpdatePodSpecVolumes`, about line 512 of `base.go`). Without it, a model can be Ready while its pods get no model volume. Confirm both, put the rule in a warning callout under Always set the storage path, and report it.

Outline, `h2`s in order:

- **Before you begin**: OME installed, and model weights on the nodes' disks.
- **The local:// storage URI**: its format, what the parser rejects and how the agent validates the files in place.
- **Always set the storage path**: why the pods get no model volume without `storage.path`.
- **Step 1: Stage the weights on the nodes**: the same path on every node, and `storage.nodeSelector` when only some nodes have them.
- **Step 2: Create the model**: a complete ClusterBaseModel with a `local://` URI and `storage.path`.
- **Step 3: Watch the model become Ready**: the model's state, and the node labels that say where it's ready.
- **Step 4: Deploy an InferenceService**: a complete InferenceService, and a check that its pods mount the path read-only.
- **Troubleshooting**: a model that stays not ready, a URI the parser rejects and pods without the model volume.
- **Clean up**: delete the service and the model; OME leaves the files on the nodes.
- **Next steps**: Serve models from a PVC and Base models.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d11-rewritten.py guides/deploy-models/serve-models-from-local-storage.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/serve-models-from-local-storage.md -> guides/deploy-models/serve-models-from-local-storage.md: 97efce19`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/deploy-models/serve-models-from-local-storage.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the node-local storage guide" -m "The guide stages weights on the nodes, creates a model with a local://
URI and storage.path, deploys an InferenceService and explains why the
storage path is required."
```

- [ ] **Step 4: Write the Run benchmarks page**

`website/src/lib/content/guides/deploy-models/run-benchmarks.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/tasks/run-workloads/run-benchmarks.md`. Its known errors:
  - The BenchmarkJob at line 223 uses `spec.endpoints` (line 227) and `spec.comparisonMetrics` (line 244), which don't exist.
- `pkg/apis/ome/v1beta1/benchmark_job.go`: every BenchmarkJob field, its default and its validation.
- `pkg/controller/v1beta1/benchmark/controller.go` and `pkg/controller/v1beta1/benchmark/reconcilers/job/job.go`: the Job the controller creates, and how it sets the status.
- `pkg/controller/v1beta1/benchmark/utils/utils.go`: the benchmark tool's arguments, including `resolveServedModelName` (about line 124), which reads the served model name from the engine.
- `pkg/webhook/admission/benchmark/benchmark_webhook.go`: what admission rejects.
- `config/configmap/benchmarkjob.yaml`: the controller's benchmark settings.
- `config/samples/benchmark/`: sample BenchmarkJobs; check each against the types before you use it.

v1.2.2 always passed `vllm-model` as the served model name (`git show 5f1c4096:pkg/controller/v1beta1/benchmark/utils/utils.go`, about line 71); the controller now reads it from the engine. Write "Since v1.3." in the sentence that says so.

Outline, `h2`s in order:

- **Before you begin**: a Ready InferenceService, and storage for the results (link Benchmark output storage).
- **Step 1: Check the endpoint**: the InferenceService is Ready and answers a request.
- **Step 2: Create a benchmark**: a complete BenchmarkJob that targets the InferenceService.
- **Step 3: Watch it run**: the BenchmarkJob's status, and its Job's pod.
- **Step 4: Read the results**: where they land in storage, and what each file holds.
- **Choose traffic scenarios**: the scenario formats the tool accepts, and what each models.
- **Benchmark an external API**: an endpoint outside the cluster, and the credentials it needs.
- **Troubleshooting**: admission rejections, a Job that fails and results that never arrive.
- **Clean up**: delete the BenchmarkJob, and what happens to its Job and results.
- **Next steps**: Benchmark output storage and Serve your first model.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d11-rewritten.py guides/deploy-models/run-benchmarks.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `tasks/run-workloads/run-benchmarks.md -> guides/deploy-models/run-benchmarks.md: 51ef8126`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/guides/deploy-models/run-benchmarks.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Run benchmarks guide" -m "The guide checks the endpoint, creates a BenchmarkJob, watches it run,
reads the results and covers traffic scenarios, external APIs and
troubleshooting."
```

- [ ] **Step 5: Write the Benchmark output storage page**

`website/src/lib/content/reference/storage/benchmark-output-storage.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/reference/benchmark-output-storage.md`. It has no known errors, but check every fact anyway.
- `pkg/controller/v1beta1/benchmark/utils/utils.go`: `BuildStorageArgs` (about line 256), with a branch for each provider: OCI (about line 282), PVC (about line 304), S3 (about line 312), Azure (about line 342), GCS (about line 372) and GitHub (about line 394).
- `pkg/utils/storage/storage.go`: how each URI is parsed, including the PVC path check that rejects `..`, which is new since v1.2.2.
- `pkg/apis/ome/v1beta1/benchmark_job.go`: `outputLocation`.

Credentials go on the benchmark pod's command line. Check that in `BuildStorageArgs`, and put it first on the page, in a warning callout. Write "Since v1.3." in the sentence about the PVC `..` check.

Outline, `h2`s in order:

- **Credentials appear on the pod command line**: which parameters end up in the pod spec, and who can read them there.
- **OCI Object Storage**: the URI format and a table of its parameters, with an example.
- **AWS S3**: the same.
- **Azure Blob Storage**: the same.
- **Google Cloud Storage**: the same.
- **GitHub Releases**: the same.
- **Persistent volume claims**: the same, and the paths it rejects.
- **Related pages**: Run benchmarks and Serve models from node-local storage.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d11-rewritten.py reference/storage/benchmark-output-storage.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `reference/benchmark-output-storage.md -> reference/storage/benchmark-output-storage.md: 1b935aa8`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/reference/storage/benchmark-output-storage.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Benchmark output storage page" -m "The reference lists the storage URI format and parameters for each
provider a BenchmarkJob can write to, and warns that credentials appear
on the pod command line."
```

- [ ] **Step 6: Write the Set up a development environment page**

`website/src/lib/content/contributing/development-setup.md` is a guide. Front matter: remove `status: draft`.

Sources:

- The Hugo page, for topics: `site/content/en/docs/developer-guide/contributing.md`. Its known errors:
  - Line 29 says "Go 1.22.0+", and so does `CONTRIBUTING.md:130`, but `go.mod:3` is `go 1.26.0`.
- `Makefile`: the build, test, lint and deploy targets.
- `go.mod`: the Go version.
- `CONTRIBUTING.md` and `AGENTS.md`: the documented workflow, including `make test-no-xet` for machines without a Rust toolchain.
- `.pre-commit-config.yaml`: the hooks.

Keep the nvkind section only if everything it references still exists. The Hugo page's Pull Request Process and OEP sections (about lines 261 to 349) move to Pull requests and OEPs: link it instead of repeating them.

Outline, `h2`s in order:

- **Before you begin**: Go, Docker, kubectl and Helm, Rust for `pkg/xet`, and Node for the website, with the versions the repo needs.
- **Step 1: Clone the repository**: the clone, and installing the pre-commit hooks.
- **Step 2: Build**: the make targets for each binary.
- **Step 3: Run the tests**: `make test`, `make test-no-xet` and running one test with envtest.
- **Step 4: Deploy your build to a cluster**: build and load the images, and install the charts with them.
- **Change the API**: `make generate` and `make manifests` after any change to `pkg/apis/`.
- **IDE setup**: a VS Code or Cursor `launch.json` for `cmd/manager/main.go` and a GoLand run configuration, checked against the current flags.
- **Troubleshooting**: the common failures, such as a missing Rust toolchain or missing envtest binaries.
- **Next steps**: Pull requests and OEPs, and Writing docs.

Then set `rewrittenFrom` and run the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d11-rewritten.py contributing/development-setup.md
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `developer-guide/contributing.md -> contributing/development-setup.md: 777c54b7`, then `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/contributing/development-setup.md website/redirects.json
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the development setup guide" -m "The guide installs the tools OME needs, builds and tests it, deploys a
local build, and covers API changes, IDE setup and troubleshooting."
```

- [ ] **Step 7: Write the Pull requests and OEPs page**

`website/src/lib/content/contributing/pull-requests-and-oeps.md` is a reference page. Front matter: remove `status: draft`.

Sources:

- This page is new: it has no Hugo page and no `redirects.json` entry.
- `site/content/en/docs/developer-guide/contributing.md` (about lines 261 to 349): the Hugo page's pull request and OEP sections, for topics.
- `CONTRIBUTING.md` (about lines 16 to 114): the documented process.
- `.github/PULL_REQUEST_TEMPLATE.md`: the template.
- `.github/workflows/pr-validation.yml`, `.github/workflows/website.yml`, `.github/workflows/alfred-simulator.yml`, `.github/workflows/claude-code-review.yml` and `.github/workflows/sync-dependabot.yaml`: the checks that run on a pull request, and when.
- `.github/workflows/pr-labeler.yml`, `.github/labeler.yml` and `.github/labels.yml`: the labels a pull request gets.
- `.github/CODEOWNERS`: who reviews what.
- `.pre-commit-config.yaml`: the hooks to run before pushing.
- `oeps/NNNN-template/README.md`, `oeps/NNNN-template/oep.yaml` and `oeps/*/oep.yaml`: the OEP template, and the statuses OEPs use.
- `AGENTS.md`: the title prefixes, the commit title and body lengths and the DCO sign-off.

Only `AGENTS.md` asks for the DCO sign-off, and no workflow checks titles or sign-offs. Document the conventions without saying a workflow enforces them, and report the gap.

Outline, `h2`s in order:

- **Before you open a pull request**: rebase on `main`, run the hooks and the tests, and add a test that fails without the fix for a bug fix.
- **Title prefixes**: a table of the prefixes and when to use each.
- **Commit messages**: title and body lengths, and the DCO sign-off.
- **The pull request template**: what each section asks for.
- **Checks that run on your pull request**: a table of each workflow, what it checks and when it runs.
- **Review and merge**: code owners, labels and how a pull request merges.
- **When you need an OEP**: which changes need one, from `CONTRIBUTING.md`.
- **Write an OEP**: copy the template, fill in `oep.yaml`, and the statuses an OEP moves through.
- **Related pages**: Set up a development environment and Writing docs.

This page has no `redirects.json` entry, so run only the YAML check:

```bash
cd "$(git rev-parse --show-toplevel)"
KUBEBUILDER_ASSETS="$HOME/Library/Application Support/io.kubebuilder.envtest/k8s/1.30.3-darwin-arm64" go run ./hack/docs-examples
```

Expected: `Checked N objects: 0 problems.` Run the website checks and the pre-commit hooks, then commit:

```bash
cd "$(git rev-parse --show-toplevel)"
git add website/src/lib/content/contributing/pull-requests-and-oeps.md
git -c user.name="Simo Lin" -c user.email="25425177+slin1237@users.noreply.github.com" commit -s -m "[Docs] Write the Pull requests and OEPs page" -m "A new reference for contributors: title prefixes, commit messages, the
pull request template, the checks that run, review and merge, and when
and how to write an OEP."
```

- [ ] **Step 8: Check the pages**

Start the dev server on port 5191, as [Servers during Phase D](#servers-during-phase-d) says, and check what each page renders:

```bash
cd "$(git rev-parse --show-toplevel)/website"
export PATH=/tmp/ome-tools/bin:$PATH
pnpm dev --port 5191 --strictPort > /tmp/d11-dev.log 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null http://localhost:5191/ome/ && break; sleep 1; done
B=http://localhost:5191/ome
for p in guides/deploy-models/serve-models-from-local-storage guides/deploy-models/run-benchmarks reference/storage/benchmark-output-storage contributing/development-setup contributing/pull-requests-and-oeps; do
  h=$(curl -s $B/$p)
  echo "$p $(curl -s -o /dev/null -w '%{http_code}' $B/$p) $(grep -c 'This page is being rewritten' <<<"$h") $(grep -o 'class="[^"]*doc-admonition--preview' <<<"$h" | wc -l | tr -d ' ') $(grep -oE 'class="[^"]*doc-since--(unreleased|released|unknown)' <<<"$h" | wc -l | tr -d ' ')"
done
for p in guides/deploy-models/serve-models-from-local-storage guides/deploy-models/run-benchmarks reference/storage/benchmark-output-storage contributing/development-setup contributing/pull-requests-and-oeps; do echo "$p: $(curl -s $B/$p | grep -o '<h2 id="[^"]*"' | cut -d'"' -f2 | paste -sd' ' -)"; done
for p in guides/deploy-models/serve-models-from-local-storage guides/deploy-models/run-benchmarks reference/storage/benchmark-output-storage contributing/development-setup contributing/pull-requests-and-oeps; do curl -s $B/$p; done | grep -o 'class="[^"]*doc-since--[a-z]*[^"]*"[^>]*>[^<]*' | sed 's/.*>//' | sort | uniq -c
curl -s $B/search.json | python3 -c 'import json, sys; e = json.load(sys.stdin); print(len(e), sum(x["status"] != "draft" for x in e))'
```

Expected, first, each page with its status code, draft count, preview callout count and since badge count:

```text
guides/deploy-models/serve-models-from-local-storage 200 0 0 0
guides/deploy-models/run-benchmarks 200 0 0 0
reference/storage/benchmark-output-storage 200 0 0 0
contributing/development-setup 200 0 0 0
contributing/pull-requests-and-oeps 200 0 0 0
```

A page with `since: v1.3` in its front matter has one badge, in its header, and the others have one for each heading marked `since=v1.3`. If the code led you to badge more or fewer headings than the outline marks, the count changes by as many; say why in your report. Then each page's `h2` ids, matching its outline:

```text
guides/deploy-models/serve-models-from-local-storage: before-you-begin the-local-storage-uri always-set-the-storage-path step-1-stage-the-weights-on-the-nodes step-2-create-the-model step-3-watch-the-model-become-ready step-4-deploy-an-inferenceservice troubleshooting clean-up next-steps
guides/deploy-models/run-benchmarks: before-you-begin step-1-check-the-endpoint step-2-create-a-benchmark step-3-watch-it-run step-4-read-the-results choose-traffic-scenarios benchmark-an-external-api troubleshooting clean-up next-steps
reference/storage/benchmark-output-storage: credentials-appear-on-the-pod-command-line oci-object-storage aws-s3 azure-blob-storage google-cloud-storage github-releases persistent-volume-claims related-pages
contributing/development-setup: before-you-begin step-1-clone-the-repository step-2-build step-3-run-the-tests step-4-deploy-your-build-to-a-cluster change-the-api ide-setup troubleshooting next-steps
contributing/pull-requests-and-oeps: before-you-open-a-pull-request title-prefixes commit-messages the-pull-request-template checks-that-run-on-your-pull-request review-and-merge when-you-need-an-oep write-an-oep related-pages
```

Then the since labels: nothing, since your pages have no badges. Last, `89 37`: the 37 written pages include your 5.

Take screenshots of Run benchmarks, Set up a development environment and Pull requests and OEPs at 1280 and 390 pixels wide, as [Servers during Phase D](#servers-during-phase-d) says. On each, check that headings, tabs, callouts, tables and code blocks render; that wide tables and code blocks scroll inside their frames at 390 pixels instead of widening the page; that no raw Markdown shows, such as `!!!`, `===` or `{#`; and that the sidebar marks the page as current and the table of contents lists its headings. Fix what's wrong in your pages and rerun the checks. Report problems in the site itself, such as its styles, instead of fixing them.

Stop the server with `pkill -f 'port 5191'; pkill -f "$(git rev-parse --show-toplevel)/website/node_modules/.*workerd"`.

- [ ] **Step 9: Report**

```bash
cd "$(git rev-parse --show-toplevel)"
python3 /tmp/d11-batches.py d11
SKIP=go-mod-tidy ~/.local/bin/pre-commit run --files $(git diff --name-only docs/website-content...HEAD)
git status --short
git log --format=%s docs/website-content..HEAD | awk 'length > 52'
```

Expected:

```text
docs/website-d11 (D11): 6 files, 0 not D11's
  tasks/run-workloads/serve-models-from-local-storage.md -> guides/deploy-models/serve-models-from-local-storage.md: 97efce19
  tasks/run-workloads/run-benchmarks.md -> guides/deploy-models/run-benchmarks.md: 51ef8126
  reference/benchmark-output-storage.md -> reference/storage/benchmark-output-storage.md: 1b935aa8
  developer-guide/contributing.md -> contributing/development-setup.md: 777c54b7
0 problems
```

with one more file for each other landing page you changed. Then the hooks pass, `git status` prints nothing, and `awk` prints nothing, since no commit subject is longer than 52 characters.

Report per Ground rule 8, with: the Step 8 output; which since label you saw; the YAML check's object count for each page; every contradiction between the code and a Hugo page, with the file and line on both sides; each `title` or `description` you changed, and why; each outline you changed, and why; problems you found outside your batch; and anything you couldn't verify.

## Appendix A: Renderer and registry source

The prototype's `src/lib/**` files, as Task A1 Step 4 copies them from `/tmp/ome-render-proto`. `nav.ts` is in Appendix B.

### `website/src/lib/config/site.ts`

```ts
/**
 * Site identity and repository location. Everything that names the repo,
 * branch or content path reads from here, so the Edit links and the GitHub
 * badge follow the site if it moves to its own repo.
 */
export const site = {
	name: 'OME',
	longName: 'Open Model Engine',
	description:
		'OME is a Kubernetes operator for serving large language models. Declare a model, and OME picks the runtime, places it on the right GPUs, and rolls out every change safely.',
	url: 'https://lightseek.org/ome',
	repo: 'ome-projects/ome',
	branch: 'main',
	contentDir: 'website/src/lib/content',
	legacyDocsUrl: 'https://ome-projects.github.io/ome/docs'
} as const;

export const repoUrl = `https://github.com/${site.repo}`;

/** GitHub editor URL for a content path such as `guides/index.md`. */
export function editUrl(path: string): string {
	return `${repoUrl}/edit/${site.branch}/${site.contentDir}/${path}`;
}

/** Raw Markdown URL for a content path. */
export function sourceUrl(path: string): string {
	return `https://raw.githubusercontent.com/${site.repo}/${site.branch}/${site.contentDir}/${path}`;
}
```

### `website/src/lib/docs/checks.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { checkLinks, checkNav, checkRedirects, checkSearchIndex } from './checks';
import { SECTION_IDS } from './paths';
import { testPage } from './testing';
import type { NavSection, SearchEntry } from './types';

const landings = SECTION_IDS.map((id) => testPage(`${id}/index.md`));
const emptyNav = (): NavSection[] => SECTION_IDS.map((id) => ({ id, label: id, groups: [] }));

describe('checkNav', () => {
	it('passes when every page is listed once', () => {
		const nav = emptyNav();
		nav[1].groups.push({ label: 'Deploy models', pages: ['deploy-models/a.md'] });
		expect(checkNav(nav, [...landings, testPage('guides/deploy-models/a.md')])).toEqual([]);
	});

	it('reports every problem', () => {
		const nav = emptyNav().filter((section) => section.id !== 'contributing');
		nav[1].groups.push(
			{ label: 'A', pages: ['deploy-models/a.md', 'deploy-models/gone.md', 'index.md'] },
			{ label: 'B', pages: ['deploy-models/a.md'] }
		);
		const pages = [
			...landings.filter((page) => page.section !== 'reference'),
			testPage('guides/deploy-models/a.md'),
			testPage('guides/networking/unlisted.md')
		];
		expect(checkNav(nav, pages)).toEqual([
			'section "reference" has no landing page reference/index.md',
			'nav.ts must list section "contributing" once, not 0 times',
			'nav.ts lists guides/deploy-models/gone.md, which does not exist',
			'nav.ts lists guides/index.md; section landing pages are implicit and groups have no index page',
			'guides/deploy-models/a.md is listed 2 times in nav.ts',
			'guides/networking/unlisted.md is not in nav.ts'
		]);
	});
});

describe('checkLinks', () => {
	it('checks targets and anchors', () => {
		const pages = [
			testPage('guides/a.md', {
				links: [
					{ route: 'concepts/b', anchor: 'storage' },
					{ route: 'concepts/b', anchor: 'nope' },
					{ route: 'concepts/b', anchor: null },
					{ route: 'concepts/missing', anchor: null },
					{ route: 'concepts/draft', anchor: 'x' },
					{ route: 'guides/a', anchor: 'self' }
				],
				anchors: ['self']
			}),
			testPage('concepts/b.md', { anchors: ['storage'] }),
			testPage('concepts/draft.md', { status: 'draft' })
		];
		expect(checkLinks(pages)).toEqual([
			'guides/a.md: the link to /concepts/b#nope: concepts/b.md has no id "nope"',
			'guides/a.md: the link to /concepts/missing does not match any page',
			'guides/a.md: the link to /concepts/draft#x points into concepts/draft.md, a draft with no headings yet; link to the page without an anchor'
		]);
	});
});

describe('checkRedirects', () => {
	const pages = [
		testPage('concepts/written.md'),
		testPage('concepts/draft.md', { status: 'draft' })
	];

	it('passes a consistent table', () => {
		expect(
			checkRedirects(
				[
					{
						old: 'concepts/a.md',
						new: 'concepts/written.md',
						rewrittenFrom: '0123456789abcdef0123456789abcdef01234567'
					},
					{ old: 'concepts/b.md', new: 'concepts/draft.md', rewrittenFrom: null }
				],
				pages
			)
		).toEqual([]);
	});

	it('reports every problem', () => {
		expect(
			checkRedirects(
				[
					{ old: 'concepts/a', new: 'concepts/draft.md', rewrittenFrom: null },
					{ old: 'concepts/b.md', new: 'concepts/gone.md', rewrittenFrom: null },
					{ old: 'concepts/b.md', new: 'concepts/written.md', rewrittenFrom: null },
					{ old: 'concepts/c.md', new: 'concepts/draft.md', rewrittenFrom: 'abc1234' },
					{ old: 'concepts/d.md', new: 'concepts/written.md', rewrittenFrom: 'HEAD' }
				],
				pages
			)
		).toEqual([
			'redirects.json entry "concepts/a": "old" must be a Hugo page path ending in .md',
			'redirects.json entry "concepts/b.md": "new" page concepts/gone.md does not exist',
			'redirects.json entry "concepts/b.md": "old" appears more than once',
			'redirects.json entry "concepts/b.md": concepts/written.md is written, so set "rewrittenFrom" to the last commit that changed the old page (git log -1 --format=%H -- site/content/en/docs/concepts/b.md)',
			'redirects.json entry "concepts/c.md": concepts/draft.md is a draft, so "rewrittenFrom" must be null',
			'redirects.json entry "concepts/d.md": "rewrittenFrom" must be a commit SHA'
		]);
	});
});

describe('checkSearchIndex', () => {
	it('wants one entry per page', () => {
		const entry = (route: string) => ({ route }) as SearchEntry;
		const pages = [testPage('guides/a.md'), testPage('guides/b.md')];
		expect(checkSearchIndex([entry('guides/a'), entry('guides/b')], pages)).toEqual([]);
		expect(
			checkSearchIndex([entry('guides/a'), entry('guides/a'), entry('guides/x')], pages)
		).toEqual([
			'search entry /guides/a appears more than once',
			'search entry /guides/x does not match any page',
			'guides/b.md is missing from the search index'
		]);
	});
});
```

### `website/src/lib/docs/checks.ts`

```ts
import { SECTION_IDS } from './paths';
import type { RedirectEntry } from './redirects';
import type { NavSection, PageMeta, SearchEntry } from './types';

// Each check returns every problem it finds, so one test run reports them all.

const COMMIT = /^[0-9a-f]{7,40}$/;

/** Every section has a landing page, every nav entry exists, and every other page is listed once. */
export function checkNav(nav: readonly NavSection[], pages: readonly PageMeta[]): string[] {
	const errors: string[] = [];
	const paths = new Set(pages.map((page) => page.path));
	const listed = new Map<string, number>();

	for (const id of SECTION_IDS) {
		const count = nav.filter((section) => section.id === id).length;
		if (count !== 1) errors.push(`nav.ts must list section "${id}" once, not ${count} times`);
		if (!paths.has(`${id}/index.md`))
			errors.push(`section "${id}" has no landing page ${id}/index.md`);
	}
	for (const section of nav) {
		for (const group of section.groups) {
			for (const page of group.pages) {
				const path = `${section.id}/${page}`;
				if (page === 'index.md' || page.endsWith('/index.md')) {
					errors.push(
						`nav.ts lists ${path}; section landing pages are implicit and groups have no index page`
					);
				} else if (!paths.has(path)) {
					errors.push(`nav.ts lists ${path}, which does not exist`);
				}
				listed.set(path, (listed.get(path) ?? 0) + 1);
			}
		}
	}
	for (const page of pages) {
		if (page.path === `${page.section}/index.md`) continue;
		const count = listed.get(page.path) ?? 0;
		if (count === 0) errors.push(`${page.path} is not in nav.ts`);
		if (count > 1) errors.push(`${page.path} is listed ${count} times in nav.ts`);
	}
	return errors;
}

/** Every internal link reaches a page, and every anchor exists on its target. */
export function checkLinks(pages: readonly PageMeta[]): string[] {
	const errors: string[] = [];
	const byRoute = new Map(pages.map((page) => [page.route, page]));
	for (const page of pages) {
		for (const { route, anchor } of page.links) {
			const shown = `/${route}${anchor ? `#${anchor}` : ''}`;
			const target = byRoute.get(route);
			if (!target) {
				errors.push(`${page.path}: the link to ${shown} does not match any page`);
			} else if (anchor && !target.anchors.includes(anchor)) {
				errors.push(
					target.status === 'draft'
						? `${page.path}: the link to ${shown} points into ${target.path}, a draft with no headings yet; link to the page without an anchor`
						: `${page.path}: the link to ${shown}: ${target.path} has no id "${anchor}"`
				);
			}
		}
	}
	return errors;
}

/**
 * Old paths are unique, new paths exist, and `rewrittenFrom` is set exactly
 * when the new page is written.
 */
export function checkRedirects(
	entries: readonly RedirectEntry[],
	pages: readonly PageMeta[]
): string[] {
	const errors: string[] = [];
	const byPath = new Map(pages.map((page) => [page.path, page]));
	const seen = new Set<string>();
	for (const entry of entries) {
		const where = `redirects.json entry "${entry.old}"`;
		if (!entry.old.endsWith('.md'))
			errors.push(`${where}: "old" must be a Hugo page path ending in .md`);
		if (seen.has(entry.old)) errors.push(`${where}: "old" appears more than once`);
		seen.add(entry.old);

		const page = byPath.get(entry.new);
		if (!page) {
			errors.push(`${where}: "new" page ${entry.new} does not exist`);
		} else if (entry.rewrittenFrom === null && page.status !== 'draft') {
			errors.push(
				`${where}: ${entry.new} is written, so set "rewrittenFrom" to the last commit that changed the old page (git log -1 --format=%H -- site/content/en/docs/${entry.old})`
			);
		} else if (entry.rewrittenFrom !== null && page.status === 'draft') {
			errors.push(`${where}: ${entry.new} is a draft, so "rewrittenFrom" must be null`);
		}
		if (entry.rewrittenFrom !== null && !COMMIT.test(entry.rewrittenFrom)) {
			errors.push(`${where}: "rewrittenFrom" must be a commit SHA`);
		}
	}
	return errors;
}

/** The search index has exactly one entry per page. */
export function checkSearchIndex(
	entries: readonly SearchEntry[],
	pages: readonly PageMeta[]
): string[] {
	const errors: string[] = [];
	const routes = new Set(pages.map((page) => page.route));
	const seen = new Set<string>();
	for (const entry of entries) {
		if (!routes.has(entry.route))
			errors.push(`search entry /${entry.route} does not match any page`);
		if (seen.has(entry.route)) errors.push(`search entry /${entry.route} appears more than once`);
		seen.add(entry.route);
	}
	for (const page of pages) {
		if (!seen.has(page.route)) errors.push(`${page.path} is missing from the search index`);
	}
	return errors;
}
```

### `website/src/lib/docs/content.test.ts`

```ts
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { nav } from '$lib/config/nav';
import { renderPage, type RenderedPage } from '$lib/markdown/render';
import { checkLinks, checkNav, checkRedirects, checkSearchIndex } from './checks';
import { createRegistry } from './registry';
import { redirects } from './redirects';
import { buildSearchIndex } from './search';

// Renders every page the way the build does and runs every content check,
// so `pnpm test` reports all content errors at once.

const contentDir = fileURLToPath(new URL('../content', import.meta.url));
const staticDir = fileURLToPath(new URL('../../../static', import.meta.url));

function markdownFiles(dir: string): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		const full = join(dir, entry.name);
		if (entry.isDirectory()) return markdownFiles(full);
		return entry.name.endsWith('.md') ? [full] : [];
	});
}

const rendered = new Map<string, RenderedPage>();
const renderErrors: string[] = [];
for (const file of markdownFiles(contentDir)) {
	const path = relative(contentDir, file).split(sep).join('/');
	try {
		rendered.set(
			path,
			renderPage(readFileSync(file, 'utf8'), {
				path,
				basePath: '/ome',
				assetExists: (publicPath) => existsSync(join(staticDir, publicPath))
			})
		);
	} catch (error) {
		renderErrors.push((error as Error).message);
	}
}
const registry = createRegistry(
	[...rendered.values()].map((page) => page.meta),
	async (path) => rendered.get(path)!.html
);

describe('content', () => {
	it('renders every page', () => {
		expect(renderErrors).toEqual([]);
	});

	it('lists every page in nav.ts', () => {
		expect(checkNav(nav, registry.pages)).toEqual([]);
	});

	it('resolves every internal link and anchor', () => {
		expect(checkLinks(registry.pages)).toEqual([]);
	});

	it('keeps redirects.json consistent', () => {
		expect(checkRedirects(redirects, registry.pages)).toEqual([]);
	});

	it('indexes every page for search', async () => {
		const entries = await buildSearchIndex(
			registry.pages,
			nav,
			async (path) => rendered.get(path)!.searchText
		);
		expect(checkSearchIndex(entries, registry.pages)).toEqual([]);
	});
});
```

### `website/src/lib/docs/navigation.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { buildNavTree, pagePosition } from './navigation';
import { testPage } from './testing';
import type { NavSection } from './types';

const section: NavSection = {
	id: 'guides',
	label: 'Guides',
	groups: [
		{ label: 'Deploy models', pages: ['deploy-models/a.md', 'deploy-models/missing.md'] },
		{ label: 'Networking', pages: ['networking/b.md'] },
		{ label: 'Empty', pages: ['empty/missing.md'] },
		{ label: 'Multi-cluster', preview: true, pages: ['multi-cluster/c.md'] }
	]
};
const pages = new Map(
	[
		'guides/index.md',
		'guides/deploy-models/a.md',
		'guides/networking/b.md',
		'guides/multi-cluster/c.md'
	].map((path) => [
		path,
		testPage(path, { navLabel: `Label ${path}`, status: path.includes('/b') ? 'draft' : null })
	])
);
const tree = buildNavTree(section, (path) => pages.get(path));

describe('buildNavTree', () => {
	it('resolves pages and drops missing pages and empty groups', () => {
		expect(tree.landing?.route).toBe('guides');
		expect(
			tree.groups.map((group) => [
				group.label,
				group.preview,
				group.links.map((link) => link.route)
			])
		).toEqual([
			['Deploy models', false, ['guides/deploy-models/a']],
			['Networking', false, ['guides/networking/b']],
			['Multi-cluster', true, ['guides/multi-cluster/c']]
		]);
		expect(tree.groups[1].links[0]).toMatchObject({
			label: 'Label guides/networking/b.md',
			status: 'draft'
		});
	});
});

describe('pagePosition', () => {
	it('walks from the landing page through each group', () => {
		expect(pagePosition(tree, 'guides')).toMatchObject({
			group: null,
			prev: null,
			next: { route: 'guides/deploy-models/a' }
		});
		expect(pagePosition(tree, 'guides/networking/b')).toMatchObject({
			group: 'Networking',
			prev: { route: 'guides/deploy-models/a' },
			next: { route: 'guides/multi-cluster/c' }
		});
		expect(pagePosition(tree, 'guides/multi-cluster/c').next).toBeNull();
		expect(pagePosition(tree, 'guides/unknown')).toEqual({ group: null, prev: null, next: null });
	});
});
```

### `website/src/lib/docs/navigation.ts`

```ts
import type { NavSection, PageMeta, PageStatus, SectionId } from './types';

export interface NavLink {
	path: string;
	route: string;
	label: string;
	title: string;
	description: string;
	status: PageStatus | null;
}

export interface NavTreeGroup {
	label: string | null;
	preview: boolean;
	links: NavLink[];
}

/** A section's sidebar, with pages resolved from the registry. */
export interface NavTree {
	id: SectionId;
	label: string;
	landing: NavLink | null;
	groups: NavTreeGroup[];
}

export interface PagePosition {
	/** Label of the group the page is in; null for the landing page and ungrouped pages. */
	group: string | null;
	prev: NavLink | null;
	next: NavLink | null;
}

function toLink(page: PageMeta): NavLink {
	return {
		path: page.path,
		route: page.route,
		label: page.navLabel,
		title: page.title,
		description: page.description,
		status: page.status
	};
}

/**
 * Resolves a section's nav entries against the registry. Missing pages are
 * skipped here and reported by checkNav, so a bad entry never breaks the
 * sidebar at request time.
 */
export function buildNavTree(
	section: NavSection,
	lookup: (path: string) => PageMeta | undefined
): NavTree {
	const landing = lookup(`${section.id}/index.md`);
	const groups = section.groups
		.map((group) => ({
			label: group.label,
			preview: group.preview ?? false,
			links: group.pages
				.map((page) => lookup(`${section.id}/${page}`))
				.filter((page): page is PageMeta => page !== undefined)
				.map(toLink)
		}))
		.filter((group) => group.links.length > 0);
	return {
		id: section.id,
		label: section.label,
		landing: landing ? toLink(landing) : null,
		groups
	};
}

/** Finds a page in its section's reading order: the landing page, then each group in turn. */
export function pagePosition(tree: NavTree, route: string): PagePosition {
	const order = [
		...(tree.landing ? [{ link: tree.landing, group: null }] : []),
		...tree.groups.flatMap((group) => group.links.map((link) => ({ link, group: group.label })))
	];
	const index = order.findIndex((entry) => entry.link.route === route);
	if (index === -1) return { group: null, prev: null, next: null };
	return {
		group: order[index].group,
		prev: order[index - 1]?.link ?? null,
		next: order[index + 1]?.link ?? null
	};
}
```

### `website/src/lib/docs/paths.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { isSectionId, pathToRoute, routeHref, sectionOf } from './paths';

describe('pathToRoute', () => {
	it.each([
		['guides/index.md', 'guides'],
		['guides/deploy-models/run-benchmarks.md', 'guides/deploy-models/run-benchmarks'],
		['concepts/models/index.md', 'concepts/models'],
		['reference/api/ome.v1beta1.md', 'reference/api/ome.v1beta1'],
		['guides/reindex.md', 'guides/reindex']
	])('serves %s at %s', (path, route) => {
		expect(pathToRoute(path)).toBe(route);
	});

	it('rejects a root index page and non-Markdown files', () => {
		expect(() => pathToRoute('index.md')).toThrow(/content root has no index page/);
		expect(() => pathToRoute('guides/diagram.svg')).toThrow(/must end in \.md/);
	});
});

describe('sectionOf', () => {
	it('returns the first directory', () => {
		expect(sectionOf('getting-started/install.md')).toBe('getting-started');
		expect(isSectionId('guides')).toBe(true);
		expect(isSectionId('blog')).toBe(false);
	});

	it('rejects pages outside a section', () => {
		expect(() => sectionOf('install.md')).toThrow(/section directory/);
		expect(() => sectionOf('blog/post.md')).toThrow(/section directory/);
	});
});

describe('routeHref', () => {
	it('prefixes the base path and adds the anchor', () => {
		expect(routeHref('/ome', 'guides')).toBe('/ome/guides');
		expect(routeHref('/ome', 'concepts/models/base-models', 'storage')).toBe(
			'/ome/concepts/models/base-models#storage'
		);
		expect(routeHref('', 'guides')).toBe('/guides');
	});
});
```

### `website/src/lib/docs/paths.ts`

```ts
import type { SectionId } from './types.ts';

export const SECTION_IDS: readonly SectionId[] = [
	'getting-started',
	'guides',
	'concepts',
	'reference',
	'contributing'
];

export function isSectionId(value: string): value is SectionId {
	return (SECTION_IDS as readonly string[]).includes(value);
}

/**
 * Maps a content path to its route: `guides/x/y.md` is served at
 * `guides/x/y`, and `guides/index.md` at `guides`.
 */
export function pathToRoute(path: string): string {
	if (!path.endsWith('.md')) throw new Error(`${path}: content pages must end in .md`);
	const stem = path.slice(0, -'.md'.length);
	if (stem === 'index') throw new Error(`${path}: the content root has no index page`);
	return stem.endsWith('/index') ? stem.slice(0, -'/index'.length) : stem;
}

/** Returns the section a content path belongs to, or throws if it is outside every section. */
export function sectionOf(path: string): SectionId {
	const [first, ...rest] = path.split('/');
	if (rest.length === 0 || !isSectionId(first)) {
		throw new Error(`${path}: pages must live in a section directory (${SECTION_IDS.join(', ')})`);
	}
	return first;
}

/** Builds the URL of a route under the base path, with an optional anchor. */
export function routeHref(base: string, route: string, anchor: string | null = null): string {
	return `${base}/${route}${anchor ? `#${anchor}` : ''}`;
}
```

### `website/src/lib/docs/redirects.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { legacyUrl, parseRedirects, replacedPages } from './redirects';

describe('parseRedirects', () => {
	it('accepts well-formed entries', () => {
		const entries = [
			{
				old: 'tasks/kubectl-ome-rollout-explain.md',
				new: 'reference/kubectl-ome/rollout.md',
				rewrittenFrom: null
			},
			{
				old: 'tasks/kubectl-ome-rollout-history.md',
				new: 'reference/kubectl-ome/rollout.md',
				rewrittenFrom: 'abc1234'
			}
		];
		expect(parseRedirects(entries)).toEqual(entries);
		expect(replacedPages(entries, 'reference/kubectl-ome/rollout.md')).toEqual([
			'tasks/kubectl-ome-rollout-explain.md',
			'tasks/kubectl-ome-rollout-history.md'
		]);
		expect(replacedPages(entries, 'guides/index.md')).toEqual([]);
	});

	it.each([
		['a non-array', {}, /must be an array/],
		['a non-object entry', ['x'], /\[0\] must be an object/],
		[
			'an unknown key',
			[{ old: 'a.md', new: 'b.md', rewrittenFrom: null, note: 'x' }],
			/unknown key "note"/
		],
		['a missing path', [{ old: 'a.md', rewrittenFrom: null }], /needs string "old" and "new"/],
		['a missing rewrittenFrom', [{ old: 'a.md', new: 'b.md' }], /needs "rewrittenFrom"/]
	])('rejects %s', (_name, value, message) => {
		expect(() => parseRedirects(value)).toThrow(message);
	});
});

describe('legacyUrl', () => {
	it.each([
		['_index.md', 'https://ome-projects.github.io/ome/docs/'],
		['concepts/_index.md', 'https://ome-projects.github.io/ome/docs/concepts/'],
		['concepts/base_model.md', 'https://ome-projects.github.io/ome/docs/concepts/base_model/'],
		[
			'tasks/run-workloads/select-accelerators.md',
			'https://ome-projects.github.io/ome/docs/tasks/run-workloads/select-accelerators/'
		]
	])('%s -> %s', (old, url) => {
		expect(legacyUrl(old)).toBe(url);
	});
});
```

### `website/src/lib/docs/redirects.ts`

```ts
import table from '../../../redirects.json';
import { site } from '../config/site';

/** One row of website/redirects.json, which maps each Hugo page to the page that replaces it. */
export interface RedirectEntry {
	/** Hugo page, relative to site/content/en/docs, such as `concepts/base_model.md`. */
	old: string;
	/** Replacement, relative to src/lib/content, such as `concepts/models/base-models.md`. */
	new: string;
	/** Last commit that changed the old page when the new page was written; null while it is a draft. */
	rewrittenFrom: string | null;
}

const KEYS = new Set(['old', 'new', 'rewrittenFrom']);

/** Checks the shape of the redirects table. `checkRedirects` checks its contents. */
export function parseRedirects(value: unknown): RedirectEntry[] {
	if (!Array.isArray(value)) throw new Error('redirects.json must be an array');
	return value.map((entry: unknown, index) => {
		const where = `redirects.json[${index}]`;
		if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
			throw new Error(`${where} must be an object`);
		}
		const record = entry as Record<string, unknown>;
		for (const key of Object.keys(record)) {
			if (!KEYS.has(key)) throw new Error(`${where} has unknown key "${key}"`);
		}
		const { old, new: replacement, rewrittenFrom } = record;
		if (typeof old !== 'string' || typeof replacement !== 'string') {
			throw new Error(`${where} needs string "old" and "new" paths`);
		}
		if (rewrittenFrom !== null && typeof rewrittenFrom !== 'string') {
			throw new Error(
				`${where} needs "rewrittenFrom": a commit SHA, or null while the page is a draft`
			);
		}
		return { old, new: replacement, rewrittenFrom };
	});
}

export const redirects: readonly RedirectEntry[] = parseRedirects(table);

/** Hugo pages that a new page replaces, in table order. */
export function replacedPages(entries: readonly RedirectEntry[], path: string): string[] {
	return entries.filter((entry) => entry.new === path).map((entry) => entry.old);
}

/** URL of a Hugo page: `concepts/_index.md` was served at `.../docs/concepts/`. */
export function legacyUrl(old: string): string {
	const stem = old.replace(/(?:^|\/)_index\.md$/, '').replace(/\.md$/, '');
	return `${site.legacyDocsUrl}/${stem ? `${stem}/` : ''}`;
}
```

### `website/src/lib/docs/registry.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { createRegistry } from './registry';
import { testPage } from './testing';

describe('createRegistry', () => {
	const pages = [
		testPage('concepts/models/index.md'),
		testPage('concepts/index.md'),
		testPage('concepts/models/base-models.md')
	];
	const registry = createRegistry(pages, async (path) => `<p>${path}</p>`);

	it('sorts pages by path', () => {
		expect(registry.pages.map((page) => page.path)).toEqual([
			'concepts/index.md',
			'concepts/models/base-models.md',
			'concepts/models/index.md'
		]);
	});

	it('looks up a section index and a nested index separately', () => {
		expect(registry.getPage('concepts')?.path).toBe('concepts/index.md');
		expect(registry.getPage('concepts/models')?.path).toBe('concepts/models/index.md');
		expect(registry.getPage('concepts/models/base-models')?.path).toBe(
			'concepts/models/base-models.md'
		);
		expect(registry.getPage('concepts/model')).toBeUndefined();
		expect(registry.getPage('concepts/')).toBeUndefined();
		expect(registry.getPageByPath('concepts/index.md')?.route).toBe('concepts');
	});

	it('loads HTML for known pages only', async () => {
		await expect(registry.loadHtml('concepts/index.md')).resolves.toBe('<p>concepts/index.md</p>');
		await expect(registry.loadHtml('concepts/missing.md')).rejects.toThrow(/unknown page/);
	});

	it('rejects two pages served at one route', () => {
		expect(() =>
			createRegistry(
				[testPage('guides/x.md'), { ...testPage('guides/x/index.md'), route: 'guides/x' }],
				async () => ''
			)
		).toThrow('guides/x.md and guides/x/index.md are both served at /guides/x');
	});
});
```

### `website/src/lib/docs/registry.ts`

```ts
import type { PageMeta } from './types';

export interface Registry {
	/** Every page, sorted by content path. */
	readonly pages: readonly PageMeta[];
	/** Exact lookup by route, such as `guides` or `guides/deploy-models/run-benchmarks`. */
	getPage(route: string): PageMeta | undefined;
	/** Exact lookup by content path, such as `guides/index.md`. */
	getPageByPath(path: string): PageMeta | undefined;
	loadHtml(path: string): Promise<string>;
}

export function createRegistry(
	metas: Iterable<PageMeta>,
	loadHtml: (path: string) => Promise<string>
): Registry {
	const pages = [...metas].sort((a, b) => a.path.localeCompare(b.path));
	const byRoute = new Map<string, PageMeta>();
	const byPath = new Map<string, PageMeta>();

	for (const page of pages) {
		const existing = byRoute.get(page.route);
		if (existing) {
			throw new Error(`${existing.path} and ${page.path} are both served at /${page.route}`);
		}
		byRoute.set(page.route, page);
		byPath.set(page.path, page);
	}

	return {
		pages,
		getPage: (route) => byRoute.get(route),
		getPageByPath: (path) => byPath.get(path),
		loadHtml: async (path) => {
			if (!byPath.has(path)) throw new Error(`unknown page ${path}`);
			return loadHtml(path);
		}
	};
}
```

### `website/src/lib/docs/search-client.test.ts`

```ts
import { describe, expect, it, vi } from 'vitest';
import { createSearch, loadSearch } from './search-client';
import type { SearchEntry } from './types';

const entry = (route: string, title: string, body: string, headings = ''): SearchEntry => ({
	id: route,
	route,
	title,
	section: 'guides',
	sectionLabel: 'Guides',
	group: null,
	description: '',
	headings,
	body,
	status: null
});

const entries = [
	entry('guides/pause', 'Pause and resume a rollout', 'Stop a rollout partway through.'),
	entry('guides/pvc', 'Serve models from a PVC', 'Mount a persistent volume claim.'),
	entry('concepts/storage', 'Model storage', 'Where weights live.', 'Persistent volumes')
];
const search = createSearch(entries);

describe('createSearch', () => {
	it('ranks title matches above body matches', () => {
		expect(search('rollout')[0].route).toBe('guides/pause');
	});

	it('matches prefixes and typos', () => {
		expect(
			search('persis')
				.map((hit) => hit.route)
				.sort()
		).toEqual(['concepts/storage', 'guides/pvc']);
		expect(search('rolout')[0].route).toBe('guides/pause');
	});

	it('requires every term, then falls back to any term', () => {
		expect(search('persistent claim').map((hit) => hit.route)).toEqual(['guides/pvc']);
		expect(
			search('pause claim')
				.map((hit) => hit.route)
				.sort()
		).toEqual(['guides/pause', 'guides/pvc']);
	});

	it('returns stored fields only, and nothing for a blank query', () => {
		expect(search('weights')[0]).toEqual({
			route: 'concepts/storage',
			title: 'Model storage',
			sectionLabel: 'Guides',
			group: null,
			description: '',
			status: null
		});
		expect(search('   ')).toEqual([]);
	});
});

describe('loadSearch', () => {
	it('fetches once, and retries after a failure', async () => {
		const failing = vi.fn(async () => new Response('', { status: 503 }));
		await expect(loadSearch('/ome', failing)).rejects.toThrow(/503/);

		const working = vi.fn(async () => Response.json(entries));
		const loaded = await loadSearch('/ome', working);
		await loadSearch('/ome', working);
		expect(working).toHaveBeenCalledTimes(1);
		expect(working).toHaveBeenCalledWith('/ome/search.json');
		expect(loaded('pvc')[0].route).toBe('guides/pvc');
	});
});
```

### `website/src/lib/docs/search-client.ts`

```ts
import MiniSearch from 'minisearch';
import type { SearchEntry } from './types';

export type SearchHit = Pick<
	SearchEntry,
	'route' | 'title' | 'sectionLabel' | 'group' | 'description' | 'status'
>;

export type Search = (query: string, limit?: number) => SearchHit[];

/** Indexes entries for ranked search: all terms first, any term if that finds nothing. */
export function createSearch(entries: readonly SearchEntry[]): Search {
	const index = new MiniSearch<SearchEntry>({
		fields: ['title', 'headings', 'description', 'body'],
		storeFields: ['route', 'title', 'sectionLabel', 'group', 'description', 'status'],
		searchOptions: { boost: { title: 5, headings: 2, description: 2 }, prefix: true, fuzzy: 0.2 }
	});
	index.addAll(entries);

	return (query, limit = 20) => {
		const text = query.trim();
		if (text === '') return [];
		let results = index.search(text, { combineWith: 'AND' });
		if (results.length === 0) results = index.search(text, { combineWith: 'OR' });
		return results.slice(0, limit).map((result) => ({
			route: result.route,
			title: result.title,
			sectionLabel: result.sectionLabel,
			group: result.group,
			description: result.description,
			status: result.status
		}));
	};
}

let loading: Promise<Search> | null = null;

/** Fetches and indexes /search.json once per page load. A failed fetch is retried on the next call. */
export function loadSearch(base: string, fetcher: typeof fetch = fetch): Promise<Search> {
	loading ??= fetcher(`${base}/search.json`)
		.then((response) => {
			if (!response.ok) throw new Error(`search index request failed: ${response.status}`);
			return response.json() as Promise<SearchEntry[]>;
		})
		.then(createSearch)
		.catch((error: unknown) => {
			loading = null;
			throw error;
		});
	return loading;
}
```

### `website/src/lib/docs/search.test.ts`

````ts
import { describe, expect, it } from 'vitest';
import { renderPage } from '../markdown/render';
import { buildSearchIndex } from './search';
import type { NavSection } from './types';

const render = (path: string, source: string) =>
	renderPage(source, { path, basePath: '/ome', assetExists: () => false });

describe('buildSearchIndex', () => {
	it('indexes prose and headings without code, and drafts by title and description', async () => {
		const written = render(
			'guides/deploy-models/pvc.md',
			'---\ntitle: Serve models from a PVC\ndescription: Mount weights from a volume.\n---\n\n## Create the claim\n\nApply the manifest.\n\n```yaml\nkind: PersistentVolumeClaim\n```\n'
		);
		const draft = render(
			'guides/index.md',
			'---\ntitle: Guides\ndescription: Task guides.\nstatus: draft\n---\n'
		);
		const texts = new Map([
			[written.meta.path, written.searchText],
			[draft.meta.path, 'not loaded for drafts']
		]);
		const nav: NavSection[] = [
			{
				id: 'guides',
				label: 'Guides',
				groups: [{ label: 'Deploy models', pages: ['deploy-models/pvc.md'] }]
			}
		];

		const entries = await buildSearchIndex(
			[written.meta, draft.meta],
			nav,
			async (path) => texts.get(path)!
		);

		expect(entries).toEqual([
			{
				id: 'guides/deploy-models/pvc',
				route: 'guides/deploy-models/pvc',
				title: 'Serve models from a PVC',
				section: 'guides',
				sectionLabel: 'Guides',
				group: 'Deploy models',
				description: 'Mount weights from a volume.',
				headings: 'Create the claim',
				body: 'Create the claim Apply the manifest.',
				status: null
			},
			{
				id: 'guides',
				route: 'guides',
				title: 'Guides',
				section: 'guides',
				sectionLabel: 'Guides',
				group: null,
				description: 'Task guides.',
				headings: '',
				body: '',
				status: 'draft'
			}
		]);
	});
});
````

### `website/src/lib/docs/search.ts`

```ts
import type { NavSection, PageMeta, SearchEntry } from './types';

/**
 * Builds the search index served at /search.json. Drafts are indexed by title
 * and description only.
 */
export async function buildSearchIndex(
	pages: readonly PageMeta[],
	nav: readonly NavSection[],
	loadText: (path: string) => Promise<string>
): Promise<SearchEntry[]> {
	const sectionLabels = new Map(nav.map((section) => [section.id, section.label]));
	const groups = new Map<string, string | null>();
	for (const section of nav) {
		for (const group of section.groups) {
			for (const page of group.pages) groups.set(`${section.id}/${page}`, group.label);
		}
	}

	return Promise.all(
		pages.map(async (page): Promise<SearchEntry> => {
			const draft = page.status === 'draft';
			return {
				id: page.route,
				route: page.route,
				title: page.title,
				section: page.section,
				sectionLabel: sectionLabels.get(page.section) ?? page.section,
				group: groups.get(page.path) ?? null,
				description: page.description,
				headings: draft ? '' : page.toc.map((entry) => entry.text).join(' '),
				body: draft ? '' : await loadText(page.path),
				status: page.status
			};
		})
	);
}
```

### `website/src/lib/docs/since.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { applySinceLabels, sinceLabel, sinceState } from './since';

describe('sinceState', () => {
	it.each([
		['v1.3', 'v1.2.2', 'unreleased'],
		['v2.0', 'v1.9.0', 'unreleased'],
		['v1.2', 'v1.2.2', 'released'],
		['v1.1', 'v1.2.2', 'released'],
		['v1.3', '1.3.0', 'released'],
		['v1.3', null, 'unknown'],
		['v1.3', 'nightly', 'unknown']
	] as const)('%s against %s is %s', (since, latest, state) => {
		expect(sinceState(since, latest)).toBe(state);
	});

	it('rejects malformed markers', () => {
		expect(() => sinceState('1.3', 'v1.2.2')).toThrow(/invalid since value/);
	});
});

describe('labels', () => {
	it('words each state', () => {
		expect(sinceLabel('v1.3', 'v1.2.2')).toBe('Unreleased: coming in v1.3');
		expect(sinceLabel('v1.3', 'v1.3.0')).toBe('New in v1.3');
		expect(sinceLabel('v1.3', null)).toBe('Since v1.3');
	});

	it('rewrites rendered badges', () => {
		const html =
			'<h2 id="a">A<span class="doc-since" data-since="v1.3">Since v1.3</span></h2><h2 id="b">B<span class="doc-since" data-since="v1.1">Since v1.1</span></h2>';
		expect(applySinceLabels(html, 'v1.2.2')).toBe(
			'<h2 id="a">A<span class="doc-since doc-since--unreleased" data-since="v1.3">Unreleased: coming in v1.3</span></h2><h2 id="b">B<span class="doc-since doc-since--released" data-since="v1.1">New in v1.1</span></h2>'
		);
		expect(applySinceLabels('<p>No badges.</p>', 'v1.2.2')).toBe('<p>No badges.</p>');
	});
});
```

### `website/src/lib/docs/since.ts`

```ts
const SINCE = /^v(\d+)\.(\d+)$/;
const RELEASE = /^v?(\d+)\.(\d+)/;

export type SinceState = 'unreleased' | 'released' | 'unknown';

/** Compares a `vMAJOR.MINOR` marker with the latest release tag, such as `v1.2.2`. */
export function sinceState(since: string, latestRelease: string | null): SinceState {
	const target = SINCE.exec(since);
	if (!target) throw new Error(`invalid since value "${since}"; use vMAJOR.MINOR`);
	const latest = latestRelease ? RELEASE.exec(latestRelease) : null;
	if (!latest) return 'unknown';
	const [major, minor] = [Number(target[1]), Number(target[2])];
	const [latestMajor, latestMinor] = [Number(latest[1]), Number(latest[2])];
	const newer = major > latestMajor || (major === latestMajor && minor > latestMinor);
	return newer ? 'unreleased' : 'released';
}

export function sinceLabel(since: string, latestRelease: string | null): string {
	switch (sinceState(since, latestRelease)) {
		case 'unreleased':
			return `Unreleased: coming in ${since}`;
		case 'released':
			return `New in ${since}`;
		default:
			return `Since ${since}`;
	}
}

const SINCE_BADGE = /<span class="doc-since" data-since="(v\d+\.\d+)">[^<]*<\/span>/g;

/**
 * Rewrites the heading badges the renderer emits, labeling each against the
 * latest release. Runs per request, so no redeploy is needed when a release
 * ships.
 */
export function applySinceLabels(html: string, latestRelease: string | null): string {
	return html.replace(SINCE_BADGE, (_match, since: string) => {
		const state = sinceState(since, latestRelease);
		const label = sinceLabel(since, latestRelease);
		return `<span class="doc-since doc-since--${state}" data-since="${since}">${label}</span>`;
	});
}
```

### `website/src/lib/docs/testing.ts`

```ts
import { pathToRoute, sectionOf } from './paths';
import type { PageMeta } from './types';

/** Builds page metadata for tests. */
export function testPage(path: string, overrides: Partial<PageMeta> = {}): PageMeta {
	return {
		path,
		route: pathToRoute(path),
		section: sectionOf(path),
		title: path,
		navLabel: path,
		description: `About ${path}.`,
		status: null,
		since: null,
		generated: false,
		toc: [],
		anchors: [],
		links: [],
		...overrides
	};
}
```

### `website/src/lib/docs/types.ts`

```ts
export type SectionId = 'getting-started' | 'guides' | 'concepts' | 'reference' | 'contributing';

export type PageStatus = 'draft' | 'preview';

export interface TocEntry {
	depth: 2 | 3;
	id: string;
	text: string;
}

/** An internal link found in a rendered page. `route` has no base path. */
export interface LinkRef {
	route: string;
	anchor: string | null;
}

/** Everything known about a page without loading its HTML. */
export interface PageMeta {
	/** Content path relative to src/lib/content, such as `guides/index.md`. */
	path: string;
	/** URL path without the base path or a leading slash, such as `guides`. */
	route: string;
	section: SectionId;
	title: string;
	/** Sidebar label: front matter `navLabel`, else `title`. */
	navLabel: string;
	description: string;
	status: PageStatus | null;
	since: string | null;
	generated: boolean;
	/** h2 and h3 headings in document order. Empty for drafts. */
	toc: TocEntry[];
	/** Every id in the rendered HTML. */
	anchors: string[];
	links: LinkRef[];
}

export interface NavGroup {
	/** Group heading in the sidebar; null for the ungrouped pages of a section. */
	label: string | null;
	/** Marks a group whose pages are all preview features. */
	preview?: boolean;
	/** Page paths relative to the section directory, in order. `index.md` is implicit. */
	pages: string[];
}

export interface NavSection {
	id: SectionId;
	label: string;
	groups: NavGroup[];
}

export interface SearchEntry {
	id: string;
	route: string;
	title: string;
	section: SectionId;
	sectionLabel: string;
	group: string | null;
	description: string;
	/** h2 and h3 heading text, joined. Empty for drafts. */
	headings: string;
	/** Body text without code blocks. Empty for drafts. */
	body: string;
	status: PageStatus | null;
}
```

### `website/src/lib/markdown/blocks.ts`

```ts
export const CALLOUT_TYPES = ['note', 'tip', 'warning', 'danger'] as const;
export type CalloutType = (typeof CALLOUT_TYPES)[number];

export type DocBlock =
	| { kind: 'prerequisites'; body: string }
	| { kind: 'cards'; items: string[] }
	| { kind: 'admonition'; type: CalloutType; title: string | null; body: string }
	| { kind: 'details'; type: CalloutType; title: string; open: boolean; body: string }
	| { kind: 'tabs'; tabs: { title: string; body: string }[] };

const PREREQUISITES_OPEN = /^<div class="prerequisites" markdown>\s*$/;
const CARDS_OPEN = /^<div class="grid cards" markdown>\s*$/;
const DIV_CLOSE = /^<\/div>\s*$/;
const ADMONITION = /^!!! (\w+)(?: "([^"]*)")?\s*$/;
const DETAILS = /^\?\?\?(\+)? (\w+)(?: "([^"]*)")?\s*$/;
const TAB = /^=== "([^"]+)"\s*$/;
/** Block syntax left over after extraction: indented, or not in a form the renderer accepts. */
const STRAY =
	/^[ \t]*(?:!!!|\?\?\?\+?)(?:\s|$)|^[ \t]*=== |^[ \t]*<div class="(?:prerequisites|grid cards)"/m;

export const BLOCK_PLACEHOLDER = /^<!--DOCBLOCK:(\d+)-->\s*$/;

/**
 * Replaces the block syntax at one nesting level of masked Markdown with
 * placeholders, and returns the blocks with their bodies dedented. Bodies
 * are rendered later, one level down.
 */
export function extractBlocks(text: string, path: string): { text: string; blocks: DocBlock[] } {
	const lines = text.split('\n');
	const out: string[] = [];
	const blocks: DocBlock[] = [];
	const place = (block: DocBlock) => {
		blocks.push(block);
		out.push('', `<!--DOCBLOCK:${blocks.length - 1}-->`, '');
	};

	let i = 0;
	while (i < lines.length) {
		const line = lines[i];
		let match: RegExpExecArray | null;

		if (PREREQUISITES_OPEN.test(line) || CARDS_OPEN.test(line)) {
			let end = i + 1;
			while (end < lines.length && !DIV_CLOSE.test(lines[end])) end++;
			if (end === lines.length) throw new Error(`${path}: "${line.trim()}" has no closing </div>`);
			const body = trimBlankLines(lines.slice(i + 1, end)).join('\n');
			if (PREREQUISITES_OPEN.test(line)) place({ kind: 'prerequisites', body });
			else place({ kind: 'cards', items: splitCards(body) });
			i = end + 1;
		} else if ((match = ADMONITION.exec(line))) {
			const type = calloutType(match[1], line, path);
			const { body, next } = collectIndented(lines, i + 1, line, path);
			const title = match[2] === undefined ? defaultTitle(type) : match[2] || null;
			place({ kind: 'admonition', type, title, body });
			i = next;
		} else if ((match = DETAILS.exec(line))) {
			const type = calloutType(match[2], line, path);
			const { body, next } = collectIndented(lines, i + 1, line, path);
			place({
				kind: 'details',
				type,
				title: match[3] || defaultTitle(type),
				open: !!match[1],
				body
			});
			i = next;
		} else if (TAB.test(line)) {
			const tabs: { title: string; body: string }[] = [];
			let j = i;
			for (;;) {
				const header = lines[j];
				const { body, next } = collectIndented(lines, j + 1, header, path);
				tabs.push({ title: TAB.exec(header)![1], body });
				let k = next;
				while (k < lines.length && lines[k].trim() === '') k++;
				if (k < lines.length && TAB.test(lines[k])) {
					j = k;
					continue;
				}
				j = next;
				break;
			}
			place({ kind: 'tabs', tabs });
			i = j;
		} else {
			out.push(line);
			i++;
		}
	}

	const result = out.join('\n');
	const stray = STRAY.exec(result);
	if (stray) {
		throw new Error(
			`${path}: "${stray[0].trim()}" is not valid here; callouts, details and tabs start at column 0 and look like !!! note "Title", ??? tip "Title" or === "Tab"`
		);
	}
	return { text: result, blocks };
}

function calloutType(value: string, line: string, path: string): CalloutType {
	if ((CALLOUT_TYPES as readonly string[]).includes(value)) return value as CalloutType;
	throw new Error(
		`${path}: "${line.trim()}" uses an unknown type; use ${CALLOUT_TYPES.join(', ')}`
	);
}

function defaultTitle(type: CalloutType): string {
	return type[0].toUpperCase() + type.slice(1);
}

/** Collects the lines indented by four spaces after a block header, stopping at the first line that isn't. */
function collectIndented(lines: string[], start: number, header: string, path: string) {
	let end = start;
	for (let j = start; j < lines.length; j++) {
		if (lines[j].trim() === '') continue;
		if (!lines[j].startsWith('    ')) break;
		end = j + 1;
	}
	const body = trimBlankLines(
		lines.slice(start, end).map((line) => (line.trim() === '' ? '' : line.slice(4)))
	).join('\n');
	if (body === '') {
		throw new Error(`${path}: "${header.trim()}" has no body; indent its content by four spaces`);
	}
	return { body, next: end };
}

function splitCards(body: string): string[] {
	return body
		.split(/\n(?=-\s)/)
		.map((item) =>
			trimBlankLines(
				item
					.replace(/^-\s+/, '')
					.replace(/\n\s+---\s*\n/g, '\n\n')
					.split('\n')
					.map((line) => line.replace(/^ {1,4}/, ''))
			).join('\n')
		)
		.filter(Boolean);
}

function trimBlankLines(lines: string[]): string[] {
	let start = 0;
	let end = lines.length;
	while (start < end && lines[start].trim() === '') start++;
	while (end > start && lines[end - 1].trim() === '') end--;
	return lines.slice(start, end);
}
```

### `website/src/lib/markdown/fences.ts`

```ts
const FENCE_OPEN = /^(\s*)(`{3,}|~{3,})(.*)$/;
const TOKEN = /\u0000F(\d+)\u0000/g;

export interface MaskedText {
	text: string;
	/** Puts the hidden code back into any fragment of `text`. */
	restore(fragment: string): string;
}

/**
 * Hides the contents of fenced code blocks, so callout, tab and card syntax
 * inside code is never interpreted. Fence lines stay. Each non-blank inner
 * line keeps its leading whitespace, so indented bodies still dedent, and the
 * rest of the line becomes a token.
 */
export function maskFences(body: string, path: string, firstLine = 1): MaskedText {
	const stored: string[] = [];
	const lines = body.split('\n');
	const out: string[] = [];

	for (let i = 0; i < lines.length; i++) {
		const open = FENCE_OPEN.exec(lines[i]);
		// A backtick fence's info string cannot contain a backtick.
		if (!open || (open[2][0] === '`' && open[3].includes('`'))) {
			out.push(lines[i]);
			continue;
		}

		const marker = open[2];
		const closer = new RegExp(`^\\s*\\${marker[0]}{${marker.length},}\\s*$`);
		out.push(lines[i]);

		let j = i + 1;
		for (; j < lines.length && !closer.test(lines[j]); j++) {
			const line = lines[j];
			if (line.trim() === '') {
				out.push(line);
				continue;
			}
			const indent = /^\s*/.exec(line)![0];
			stored.push(line.slice(indent.length));
			out.push(`${indent}\u0000F${stored.length - 1}\u0000`);
		}
		if (j === lines.length) {
			throw new Error(`${path}: the code fence on line ${firstLine + i} is never closed`);
		}
		out.push(lines[j]);
		i = j;
	}

	return {
		text: out.join('\n'),
		restore: (fragment) => fragment.replace(TOKEN, (_match, n: string) => stored[Number(n)])
	};
}
```

### `website/src/lib/markdown/frontmatter.ts`

```ts
import { parse } from 'yaml';
import type { PageStatus } from '../docs/types.ts';

export interface FrontMatter {
	title: string;
	navLabel: string | null;
	description: string;
	status: PageStatus | null;
	since: string | null;
	generated: boolean;
}

export interface ParsedPage {
	data: FrontMatter;
	body: string;
	/** 1-based line number of the first body line in the file. */
	bodyLine: number;
}

export const SINCE_PATTERN = /^v\d+\.\d+$/;

const KEYS = new Set(['title', 'navLabel', 'description', 'status', 'since', 'generated']);

/** Splits a page into validated front matter and its Markdown body. */
export function parseFrontMatter(source: string, path: string): ParsedPage {
	const text = source.replace(/\r\n?/g, '\n');
	const match = /^---\n([\s\S]*?)\n---(?:\n|$)/.exec(text);
	if (!match) {
		throw new Error(`${path}: missing front matter; start the file with a --- block`);
	}

	let raw: unknown;
	try {
		raw = parse(match[1]);
	} catch (error) {
		throw new Error(`${path}: invalid front matter: ${(error as Error).message}`);
	}
	if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
		throw new Error(`${path}: front matter must be a YAML mapping`);
	}

	const record = raw as Record<string, unknown>;
	for (const key of Object.keys(record)) {
		if (!KEYS.has(key)) throw new Error(`${path}: unknown front matter key "${key}"`);
	}

	const data: FrontMatter = {
		title: requiredString(record, 'title', path),
		navLabel: optionalString(record, 'navLabel', path),
		description: requiredString(record, 'description', path),
		status: status(record.status, path),
		since: since(record.since, path),
		generated: generated(record.generated, path)
	};

	return {
		data,
		body: text.slice(match[0].length),
		bodyLine: match[0].split('\n').length
	};
}

function requiredString(record: Record<string, unknown>, key: string, path: string): string {
	const value = record[key];
	if (value === undefined) throw new Error(`${path}: front matter needs "${key}"`);
	if (typeof value !== 'string' || value.trim() === '') {
		throw new Error(`${path}: front matter "${key}" must be a non-empty string`);
	}
	return value.trim();
}

function optionalString(record: Record<string, unknown>, key: string, path: string) {
	return record[key] === undefined ? null : requiredString(record, key, path);
}

function status(value: unknown, path: string): PageStatus | null {
	if (value === undefined) return null;
	if (value === 'draft' || value === 'preview') return value;
	throw new Error(`${path}: front matter "status" must be draft or preview`);
}

function since(value: unknown, path: string): string | null {
	if (value === undefined) return null;
	if (typeof value === 'string' && SINCE_PATTERN.test(value)) return value;
	throw new Error(`${path}: front matter "since" must look like v1.3`);
}

function generated(value: unknown, path: string): boolean {
	if (value === undefined) return false;
	if (typeof value === 'boolean') return value;
	throw new Error(`${path}: front matter "generated" must be true or false`);
}
```

### `website/src/lib/markdown/highlight.ts`

```ts
import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import diff from 'highlight.js/lib/languages/diff';
import dockerfile from 'highlight.js/lib/languages/dockerfile';
import go from 'highlight.js/lib/languages/go';
import ini from 'highlight.js/lib/languages/ini';
import json from 'highlight.js/lib/languages/json';
import makefile from 'highlight.js/lib/languages/makefile';
import markdown from 'highlight.js/lib/languages/markdown';
import python from 'highlight.js/lib/languages/python';
import yaml from 'highlight.js/lib/languages/yaml';
import { escapeHtml } from './html.ts';

hljs.registerLanguage('bash', bash);
hljs.registerLanguage('diff', diff);
hljs.registerLanguage('dockerfile', dockerfile);
hljs.registerLanguage('go', go);
hljs.registerLanguage('ini', ini);
hljs.registerLanguage('json', json);
hljs.registerLanguage('makefile', makefile);
hljs.registerLanguage('markdown', markdown);
hljs.registerLanguage('python', python);
hljs.registerLanguage('yaml', yaml);

export interface Language {
	/** Class suffix on the code element: `language-${name}`. */
	name: string;
	/** highlight.js grammar, or null for unhighlighted text. */
	grammar: string | null;
	/** Shown in the code bar when the block has no title. */
	label: string;
}

const lang = (name: string, grammar: string | null, label: string): Language => ({
	name,
	grammar,
	label
});

/** Every language a code fence may name, by the name used in the fence. */
export const LANGUAGES: Record<string, Language> = {
	bash: lang('bash', 'bash', 'Shell'),
	sh: lang('bash', 'bash', 'Shell'),
	shell: lang('bash', 'bash', 'Shell'),
	go: lang('go', 'go', 'Go'),
	json: lang('json', 'json', 'JSON'),
	yaml: lang('yaml', 'yaml', 'YAML'),
	yml: lang('yaml', 'yaml', 'YAML'),
	python: lang('python', 'python', 'Python'),
	dockerfile: lang('dockerfile', 'dockerfile', 'Dockerfile'),
	makefile: lang('makefile', 'makefile', 'Makefile'),
	diff: lang('diff', 'diff', 'Diff'),
	ini: lang('ini', 'ini', 'INI'),
	toml: lang('toml', 'ini', 'TOML'),
	markdown: lang('markdown', 'markdown', 'Markdown'),
	md: lang('markdown', 'markdown', 'Markdown'),
	text: lang('text', null, 'Text'),
	plaintext: lang('text', null, 'Text'),
	txt: lang('text', null, 'Text'),
	output: lang('output', null, 'Output')
};

export function highlight(code: string, language: Language): string {
	if (!language.grammar) return escapeHtml(code);
	return hljs.highlight(code, { language: language.grammar, ignoreIllegals: true }).value;
}
```

### `website/src/lib/markdown/html.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { blockHtmlToText, decodeEntities, escapeHtml, htmlToText } from './html';

describe('html helpers', () => {
	it('escapes and decodes', () => {
		expect(escapeHtml(`<a href="x">'&'</a>`)).toBe(
			'&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;'
		);
		expect(decodeEntities('&lt;&#39;&#x41;&amp;amp;&bogus;&#0;')).toBe("<'A&amp;&bogus;&#0;");
	});

	it('extracts text', () => {
		expect(htmlToText('<code>spec</code>&nbsp;and  <em>more</em>')).toBe('spec and more');
		expect(blockHtmlToText('<p>One</p><p>Two</p><ul><li>a</li><li>b</li></ul>')).toBe(
			'One Two a b'
		);
	});
});
```

### `website/src/lib/markdown/html.ts`

```ts
const ESCAPES: Record<string, string> = {
	'&': '&amp;',
	'<': '&lt;',
	'>': '&gt;',
	'"': '&quot;',
	"'": '&#39;'
};

export function escapeHtml(value: string): string {
	return value.replace(/[&<>"']/g, (char) => ESCAPES[char]);
}

const NAMED_ENTITIES: Record<string, string> = {
	amp: '&',
	lt: '<',
	gt: '>',
	quot: '"',
	apos: "'",
	nbsp: ' '
};

export function decodeEntities(value: string): string {
	return value.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, code: string) => {
		if (code[0] !== '#') return NAMED_ENTITIES[code.toLowerCase()] ?? match;
		const hex = code[1] === 'x' || code[1] === 'X';
		const point = hex ? parseInt(code.slice(2), 16) : Number(code.slice(1));
		return point > 0 && point <= 0x10ffff ? String.fromCodePoint(point) : match;
	});
}

/** Text of an inline HTML fragment: tags removed, entities decoded, whitespace collapsed. */
export function htmlToText(html: string): string {
	return decodeEntities(html.replace(/<[^>]*>/g, ''))
		.replace(/\s+/g, ' ')
		.trim();
}

const BLOCK_BOUNDARY =
	/<\/(?:p|div|li|h[1-6]|td|th|tr|pre|aside|details|summary|blockquote|table|ul|ol)>|<br\s*\/?>/g;

/** Text of a block HTML fragment, keeping words in adjacent blocks apart. */
export function blockHtmlToText(html: string): string {
	return htmlToText(html.replace(BLOCK_BOUNDARY, ' '));
}
```

### `website/src/lib/markdown/links.ts`

```ts
import { posix } from 'node:path';
import { site } from '../config/site.ts';
import { pathToRoute, routeHref } from '../docs/paths.ts';

const LEGACY_HOST = new URL(site.legacyDocsUrl).host;
const SCHEME = /^([a-z][a-z0-9+.-]*):/i;

export interface LinkContext {
	/** Content path of the page being rendered. */
	path: string;
	basePath: string;
}

/**
 * Checks a Markdown link and returns the href to emit. Relative `.md` links
 * resolve against the current page and become site URLs; external links pass
 * through; everything else is an error.
 */
export function resolveLink(href: string, { path, basePath }: LinkContext): string {
	if (href.startsWith('#')) return href;

	const scheme = SCHEME.exec(href)?.[1].toLowerCase();
	if (scheme === 'mailto') return href;
	if (scheme === 'http' || scheme === 'https') {
		if (new URL(href).host === LEGACY_HOST) {
			throw new Error(
				`${path}: "${href}" links to the old Hugo site; link to the new page with a relative .md path`
			);
		}
		if (href === site.url || href.startsWith(`${site.url}/`)) {
			throw new Error(
				`${path}: "${href}" is an absolute link to this site; use a relative .md path`
			);
		}
		return encodeURI(href).replace(/%25/g, '%');
	}
	if (scheme) throw new Error(`${path}: unsupported link "${href}"`);
	if (href.startsWith('/')) {
		throw new Error(
			`${path}: "${href}" is an absolute path; use a relative .md path such as ../concepts/index.md`
		);
	}

	const hash = href.indexOf('#');
	const target = hash === -1 ? href : href.slice(0, hash);
	const anchor = hash === -1 ? null : href.slice(hash + 1) || null;
	if (!target.endsWith('.md')) {
		throw new Error(
			`${path}: "${href}" must point to a .md page; link to other repository files with a full GitHub URL`
		);
	}
	const resolved = posix.normalize(posix.join(posix.dirname(path), target));
	if (resolved === '..' || resolved.startsWith('../')) {
		throw new Error(
			`${path}: "${href}" leaves src/lib/content; link to repository files with a full GitHub URL`
		);
	}
	return routeHref(basePath, pathToRoute(resolved), anchor);
}

export interface ImageContext extends LinkContext {
	/** Whether a path such as `/images/x.svg` exists in the static directory. */
	assetExists: (publicPath: string) => boolean;
}

/** Checks a Markdown image source and returns the src to emit. */
export function resolveImage(src: string, { path, basePath, assetExists }: ImageContext): string {
	if (/^https?:\/\//i.test(src)) return src;
	if (!src.startsWith('/images/')) {
		throw new Error(
			`${path}: image "${src}" must be a path under /images/ (website/static/images) or an https URL`
		);
	}
	if (!assetExists(src))
		throw new Error(`${path}: image "${src}" does not exist in website/static`);
	return `${basePath}${src}`;
}
```

### `website/src/lib/markdown/plugin.test.ts`

```ts
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { omeMarkdown } from './plugin';

const root = mkdtempSync(join(tmpdir(), 'ome-markdown-'));
const contentDir = join(root, 'content');
const staticDir = join(root, 'static');
const plugin = omeMarkdown({ basePath: '/ome', contentDir, staticDir });
const addWatchFile = vi.fn();
const load = (id: string) =>
	(plugin.load as (this: unknown, id: string) => string | null).call({ addWatchFile }, id);
const exported = (code: string | null) =>
	JSON.parse(code!.replace(/^export default /, '').replace(/;$/, ''));

beforeAll(() => {
	mkdirSync(join(contentDir, 'guides'), { recursive: true });
	mkdirSync(join(staticDir, 'images'), { recursive: true });
	writeFileSync(join(staticDir, 'images', 'flow.svg'), '<svg/>');
	writeFileSync(
		join(contentDir, 'guides', 'page.md'),
		'---\ntitle: Page\ndescription: A page.\n---\n\n## Hello\n\n![Flow](/images/flow.svg)\n'
	);
	writeFileSync(join(contentDir, 'guides', 'broken.md'), '---\ntitle: Broken\n---\n');
	writeFileSync(join(root, 'outside.md'), '---\ntitle: Outside\ndescription: X.\n---\n');
});

describe('omeMarkdown', () => {
	it('exports metadata, HTML and search text', () => {
		const file = join(contentDir, 'guides', 'page.md');
		expect(exported(load(`${file}?meta`))).toMatchObject({
			path: 'guides/page.md',
			route: 'guides/page'
		});
		expect(exported(load(`${file}?html`))).toContain(
			'<img src="/ome/images/flow.svg" alt="Flow" loading="lazy">'
		);
		expect(exported(load(`${file}?search`))).toBe('Hello');
		expect(addWatchFile).toHaveBeenCalledWith(file);
	});

	it('ignores other imports', () => {
		expect(load(join(contentDir, 'guides', 'page.md'))).toBeNull();
		expect(load(`${join(contentDir, 'guides', 'page.md')}?raw`)).toBeNull();
		expect(load(`${join(root, 'outside.md')}?meta`)).toBeNull();
		expect(load(`${join(contentDir, 'guides', 'x.ts')}?meta`)).toBeNull();
	});

	it('fails on renderer errors', () => {
		expect(() => load(`${join(contentDir, 'guides', 'broken.md')}?meta`)).toThrow(
			'guides/broken.md: front matter needs "description"'
		);
	});
});
```

### `website/src/lib/markdown/plugin.ts`

```ts
import { existsSync, readFileSync } from 'node:fs';
import { isAbsolute, join, relative, sep } from 'node:path';
import type { Plugin } from 'vite';
import { renderPage, type RenderedPage } from './render.ts';

// vite.config.ts imports this module, and Vite's native config loader needs
// file extensions, so relative imports in the renderer's modules keep `.ts`.

const KINDS = ['meta', 'html', 'search'] as const;

export interface OmeMarkdownOptions {
	/** Site base path, such as `/ome`. */
	basePath: string;
	/** Absolute path of src/lib/content. */
	contentDir: string;
	/** Absolute path of static/. */
	staticDir: string;
}

/**
 * Renders content pages at build time. Importing `page.md?meta`, `?html` or
 * `?search` yields the page's metadata, HTML or search text; any renderer
 * error fails the build with the page path.
 */
export function omeMarkdown({ basePath, contentDir, staticDir }: OmeMarkdownOptions): Plugin {
	const cache = new Map<string, { source: string; page: RenderedPage }>();

	return {
		name: 'ome-markdown',
		enforce: 'pre',
		load(id) {
			const [file, query = ''] = id.split('?', 2);
			const params = new URLSearchParams(query);
			const kind = KINDS.find((k) => params.has(k));
			if (!kind || !file.endsWith('.md')) return null;
			const rel = relative(contentDir, file);
			if (rel.startsWith('..') || isAbsolute(rel)) return null;

			this.addWatchFile(file);
			const source = readFileSync(file, 'utf8');
			const path = rel.split(sep).join('/');
			let entry = cache.get(path);
			if (entry?.source !== source) {
				const page = renderPage(source, {
					path,
					basePath,
					assetExists: (publicPath) => existsSync(join(staticDir, publicPath))
				});
				entry = { source, page };
				cache.set(path, entry);
			}

			const value =
				kind === 'meta'
					? entry.page.meta
					: kind === 'html'
						? entry.page.html
						: entry.page.searchText;
			return `export default ${JSON.stringify(value)};`;
		}
	};
}
```

### `website/src/lib/markdown/render.test.ts`

`````ts
import { describe, expect, it } from 'vitest';
import { renderPage, type RenderOptions } from './render';

const options = (path = 'guides/deploy-models/example.md'): RenderOptions => ({
	path,
	basePath: '/ome',
	assetExists: (publicPath) => publicPath === '/images/diagram.svg'
});

const page = (body: string, frontMatter = 'title: Example\ndescription: An example page.') =>
	`---\n${frontMatter}\n---\n\n${body}`;

const render = (body: string, path?: string) => renderPage(page(body), options(path));

describe('front matter', () => {
	it('fills page metadata', () => {
		const { meta } = renderPage(
			page(
				'Body.',
				'title: Serve models from a PVC\nnavLabel: Serve from a PVC\ndescription: Mount weights.\nstatus: preview\nsince: v1.3'
			),
			options()
		);
		expect(meta).toMatchObject({
			path: 'guides/deploy-models/example.md',
			route: 'guides/deploy-models/example',
			section: 'guides',
			title: 'Serve models from a PVC',
			navLabel: 'Serve from a PVC',
			description: 'Mount weights.',
			status: 'preview',
			since: 'v1.3',
			generated: false
		});
	});

	it('uses the title as the nav label by default', () => {
		expect(render('Body.').meta.navLabel).toBe('Example');
	});

	it.each([
		['no front matter', 'Body only.', /missing front matter/],
		[
			'an unknown key',
			page('x', 'title: A\ndescription: B\nweight: 3'),
			/unknown front matter key "weight"/
		],
		['no title', page('x', 'description: B'), /needs "title"/],
		['no description', page('x', 'title: A'), /needs "description"/],
		[
			'an empty title',
			page('x', 'title: ""\ndescription: B'),
			/"title" must be a non-empty string/
		],
		[
			'an unknown status',
			page('x', 'title: A\ndescription: B\nstatus: beta'),
			/"status" must be draft or preview/
		],
		[
			'a bad since',
			page('x', 'title: A\ndescription: B\nsince: 1.3'),
			/"since" must look like v1.3/
		],
		[
			'a patch since',
			page('x', 'title: A\ndescription: B\nsince: v1.3.1'),
			/"since" must look like v1.3/
		],
		[
			'a string generated',
			page('x', 'title: A\ndescription: B\ngenerated: "yes"'),
			/"generated" must be true or false/
		],
		['a list', '---\n- a\n---\n', /must be a YAML mapping/],
		['invalid YAML', page('x', 'title: A: B\ndescription: C'), /invalid front matter/]
	])('rejects %s', (_name, source, message) => {
		expect(() => renderPage(source, options())).toThrow(message);
	});

	it('serves index.md at its directory', () => {
		expect(render('Body.', 'guides/index.md').meta.route).toBe('guides');
	});

	it('rejects pages outside a section', () => {
		expect(() => render('Body.', 'faq.md')).toThrow(/section directory/);
		expect(() => render('Body.', 'blog/post.md')).toThrow(/section directory/);
	});
});

describe('drafts', () => {
	it('renders no body', () => {
		const { meta, html, searchText } = renderPage(
			page('', 'title: A\ndescription: B\nstatus: draft'),
			options()
		);
		expect(html).toBe('');
		expect(searchText).toBe('');
		expect(meta.toc).toEqual([]);
		expect(meta.status).toBe('draft');
	});

	it('rejects a draft with a body', () => {
		expect(() =>
			renderPage(page('Text.', 'title: A\ndescription: B\nstatus: draft'), options())
		).toThrow(/draft pages have no body/);
	});
});

describe('headings', () => {
	it('slugifies text and numbers duplicates', () => {
		const { meta } = render('## Install the chart\n\n## Verify\n\n### Verify\n\n## Verify');
		expect(meta.toc.map((entry) => entry.id)).toEqual([
			'install-the-chart',
			'verify',
			'verify-1',
			'verify-2'
		]);
	});

	it('uses inline text for the id', () => {
		expect(render('## `kubectl ome rollout explain`').meta.toc[0]).toEqual({
			depth: 2,
			id: 'kubectl-ome-rollout-explain',
			text: 'kubectl ome rollout explain'
		});
	});

	it('accepts an explicit id', () => {
		const { html, meta } = render('## Storage options {#storage}');
		expect(html).toContain(
			'<h2 id="storage">Storage options<a class="doc-headerlink" href="#storage"'
		);
		expect(meta.toc[0]).toEqual({ depth: 2, id: 'storage', text: 'Storage options' });
	});

	it('adds a since badge', () => {
		const { html, meta } = render('## Pause a rollout {#pause since=v1.3}');
		expect(html).toContain(
			'<h2 id="pause">Pause a rollout<span class="doc-since" data-since="v1.3">Since v1.3</span><a class="doc-headerlink"'
		);
		expect(meta.toc[0].text).toBe('Pause a rollout');
	});

	it('avoids ids already claimed explicitly', () => {
		const { meta } = render('## Setup {#verify}\n\n## Verify');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['verify', 'verify-1']);
	});

	it('rejects duplicate explicit ids', () => {
		expect(() => render('## A {#same}\n\n## B {#same}')).toThrow(/duplicate heading id "same"/);
	});

	it('rejects unknown attributes', () => {
		expect(() => render('## A {.wide}')).toThrow(/invalid attribute "\.wide"/);
		expect(() => render('## A {since=1.3}')).toThrow(/invalid attribute "since=1.3"/);
	});

	it('rejects h1', () => {
		expect(() => render('# Title')).toThrow(/sections start at ##/);
	});

	it('keeps h4 out of the table of contents', () => {
		const { html, meta } = render('## A\n\n#### Deep');
		expect(html).toContain('<h4 id="deep">');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['a']);
	});

	it('assigns ids in document order across blocks', () => {
		const { meta } = render(
			'## Verify\n\n=== "Helm"\n\n    ### Verify\n\n=== "Kustomize"\n\n    ### Verify\n\n## Verify'
		);
		expect(meta.toc.map((entry) => entry.id)).toEqual([
			'verify',
			'verify-1',
			'verify-2',
			'verify-3'
		]);
	});
});

describe('code blocks', () => {
	it('highlights with a language label', () => {
		const { html } = render('```yaml\nkind: BaseModel\n```');
		expect(html).toContain(
			'<div class="doc-code"><div class="doc-code-bar"><span class="doc-code-title">YAML</span>'
		);
		expect(html).toContain(
			'<button type="button" class="doc-code-copy" aria-label="Copy to clipboard">Copy</button>'
		);
		expect(html).toContain('<code class="hljs language-yaml"><span class="hljs-attr">kind:</span>');
	});

	it('shows a title', () => {
		const { html } = render('```yaml title="model.yaml"\nkind: BaseModel\n```');
		expect(html).toContain('<span class="doc-code-title">model.yaml</span>');
	});

	it('renders output without a copy button', () => {
		const { html } = render('```output\nNAME   READY\nllama  True\n```');
		expect(html).toBe(
			'<div class="doc-code doc-code--output"><div class="doc-code-bar"><span class="doc-code-title">Output</span></div><pre class="doc-pre"><code class="language-output">NAME   READY\nllama  True</code></pre></div>\n'
		);
	});

	it('maps aliases', () => {
		expect(render('```sh\nls\n```').html).toContain('language-bash');
		expect(render('```toml\na = 1\n```').html).toContain(
			'<span class="doc-code-title">TOML</span>'
		);
		expect(render('```text\n<b>\n```').html).toContain(
			'<code class="language-text">&lt;b&gt;</code>'
		);
	});

	it('accepts check=skip on YAML only', () => {
		expect(render('```yaml check=skip\nkind: X\n```').html).toContain('language-yaml');
		expect(() => render('```bash check=skip\nls\n```')).toThrow(
			/unsupported code block attribute "check=skip"/
		);
	});

	it.each([
		['no language', '```\nls\n```', /need a language/],
		['an indented block', 'Text.\n\n    indented code', /need a language/],
		['an unknown language', '```rust\nfn main() {}\n```', /unsupported code block language "rust"/],
		['an unknown attribute', '```yaml linenums="1"\na: 1\n```', /unsupported code block attribute/],
		['an unquoted title', '```yaml title=model.yaml\na: 1\n```', /unsupported code block attribute/]
	])('rejects %s', (_name, body, message) => {
		expect(() => render(body)).toThrow(message);
	});

	it('reports an unclosed fence with its line', () => {
		expect(() => render('Text.\n\n```yaml\na: 1')).toThrow(/code fence on line 8 is never closed/);
	});

	it('keeps block syntax inside code literal', () => {
		const { html } = render('```markdown\n!!! note\n    Body\n=== "Tab"\n```');
		expect(html).not.toContain('doc-admonition');
		expect(html).not.toContain('doc-tabbed-set');
		expect(html).toContain('!!! note');
	});

	it('supports longer fences around shorter ones', () => {
		const { html } = render('````markdown\n```yaml title="a.yaml"\nx: 1\n```\n````');
		expect(html).toContain('```yaml title=&quot;a.yaml&quot;');
		expect(html.match(/class="doc-code"/g)).toHaveLength(1);
	});
});

describe('callouts', () => {
	it.each(['note', 'tip', 'warning', 'danger'])('renders %s with a default title', (type) => {
		const { html } = render(`!!! ${type}\n    Body text.`);
		const title = type[0].toUpperCase() + type.slice(1);
		expect(html).toBe(
			`<aside class="doc-admonition doc-admonition--${type}"><p class="doc-admonition-title">${title}</p><p>Body text.</p>\n</aside>\n`
		);
	});

	it('renders a custom title with inline Markdown', () => {
		expect(render('!!! warning "Do not use `latest`"\n    Body.').html).toContain(
			'<p class="doc-admonition-title">Do not use <code>latest</code></p>'
		);
	});

	it('omits an empty title', () => {
		expect(render('!!! note ""\n    Body.').html).not.toContain('doc-admonition-title');
	});

	it('rejects other types', () => {
		expect(() => render('!!! info\n    Body.')).toThrow(
			/unknown type; use note, tip, warning, danger/
		);
	});

	it('rejects a callout without an indented body', () => {
		expect(() => render('!!! note\nBody.')).toThrow(/has no body/);
	});

	it('renders code and lists inside', () => {
		const { html } = render(
			'!!! tip\n    Run:\n\n    ```bash\n    kubectl get pods\n    ```\n\n    - one\n    - two\n\nAfter.'
		);
		expect(html).toContain('<aside class="doc-admonition doc-admonition--tip">');
		expect(html).toContain('<code class="hljs language-bash">kubectl get pods</code>');
		expect(html).toContain('<li>one</li>');
		expect(html).toMatch(/<\/aside>\n<p>After.<\/p>/);
	});

	it('rejects indented callout syntax', () => {
		expect(() => render('1. Step\n\n    !!! note\n        Body.')).toThrow(/start at column 0/);
	});

	it('rejects malformed callout syntax', () => {
		expect(() => render('!!! note Title without quotes\n    Body.')).toThrow(/not valid here/);
	});
});

describe('details', () => {
	it('renders collapsed and open details', () => {
		expect(render('??? note "More"\n    Hidden.').html).toBe(
			'<details class="doc-details doc-details--note"><summary>More</summary><p>Hidden.</p>\n</details>\n'
		);
		expect(render('???+ tip\n    Shown.').html).toContain(
			'<details class="doc-details doc-details--tip" open><summary>Tip</summary>'
		);
	});
});

describe('tabs', () => {
	it('groups consecutive tabs into one set', () => {
		const { html, meta } = render(
			'=== "Helm"\n\n    ```bash\n    helm install ome\n    ```\n\n=== "Kustomize"\n\n    Apply it.\n\nAfter.'
		);
		expect(html).toContain(
			'<div class="doc-tabbed-set"><input type="radio" class="doc-tab-input" name="doc-tab-set-1" id="doc-tab-1-0" checked><input type="radio" class="doc-tab-input" name="doc-tab-set-1" id="doc-tab-1-1"><div class="doc-tab-labels"><label class="doc-tab-label" for="doc-tab-1-0">Helm</label><label class="doc-tab-label" for="doc-tab-1-1">Kustomize</label></div><div class="doc-tab-panels"><div class="doc-tab-panel"><div class="doc-code">'
		);
		expect(html).toContain(
			'<div class="doc-tab-panel"><p>Apply it.</p>\n</div></div></div>\n<p>After.</p>'
		);
		expect(meta.anchors).toEqual(expect.arrayContaining(['doc-tab-1-0', 'doc-tab-1-1']));
	});

	it('numbers tab sets per page', () => {
		const { html } = render('=== "A"\n    a\n\nText.\n\n=== "B"\n    b');
		expect(html).toContain('name="doc-tab-set-1"');
		expect(html).toContain('name="doc-tab-set-2"');
	});

	it('nests callouts in tabs', () => {
		const { html } = render('=== "A"\n\n    !!! warning\n        Careful.');
		expect(html).toContain(
			'<div class="doc-tab-panel"><aside class="doc-admonition doc-admonition--warning">'
		);
	});
});

describe('prerequisites and cards', () => {
	it('renders a Before you begin box in the table of contents', () => {
		const { html, meta } = render(
			'<div class="prerequisites" markdown>\n\n- A cluster\n- `kubectl`\n\n</div>\n\n## Step one'
		);
		expect(html).toContain(
			'<aside class="doc-prerequisites"><h2 id="before-you-begin">Before you begin<a class="doc-headerlink" href="#before-you-begin"'
		);
		expect(html).toContain('<li><code>kubectl</code></li>');
		expect(meta.toc.map((entry) => entry.id)).toEqual(['before-you-begin', 'step-one']);
	});

	it('rejects an unclosed box', () => {
		expect(() => render('<div class="prerequisites" markdown>\n\n- A')).toThrow(
			/no closing <\/div>/
		);
	});

	it('renders card grids', () => {
		const { html } = render(
			'<div class="grid cards" markdown>\n\n-   **Install**\n\n    ---\n\n    Set up OME.\n\n-   **Serve**\n\n    Deploy a model.\n\n</div>'
		);
		expect(html).toBe(
			'<div class="doc-grid"><div class="doc-card"><p><strong>Install</strong></p>\n<p>Set up OME.</p>\n</div><div class="doc-card"><p><strong>Serve</strong></p>\n<p>Deploy a model.</p>\n</div></div>\n'
		);
	});
});

describe('links', () => {
	it('resolves relative .md links to site URLs', () => {
		const { html, meta } = render(
			'See [base models](../../concepts/models/base-models.md#storage), [guides](../index.md) and [a sibling](run-benchmarks.md).'
		);
		expect(html).toContain('<a href="/ome/concepts/models/base-models#storage">base models</a>');
		expect(html).toContain('<a href="/ome/guides">guides</a>');
		expect(html).toContain('<a href="/ome/guides/deploy-models/run-benchmarks">a sibling</a>');
		expect(meta.links).toEqual([
			{ route: 'concepts/models/base-models', anchor: 'storage' },
			{ route: 'guides', anchor: null },
			{ route: 'guides/deploy-models/run-benchmarks', anchor: null }
		]);
	});

	it('records same-page anchors but not headerlinks', () => {
		const { meta } = render('## Setup\n\nJump to [verify](#verify).');
		expect(meta.links).toEqual([{ route: 'guides/deploy-models/example', anchor: 'verify' }]);
	});

	it('passes external links through', () => {
		const { html, meta } = render(
			'[Kueue](https://kueue.sigs.k8s.io/docs/) and <https://github.com/ome-projects/ome/blob/main/Makefile> and [mail](mailto:a@b.c)'
		);
		expect(html).toContain('<a href="https://kueue.sigs.k8s.io/docs/">Kueue</a>');
		expect(html).toContain('href="https://github.com/ome-projects/ome/blob/main/Makefile"');
		expect(html).toContain('href="mailto:a@b.c"');
		expect(meta.links).toEqual([]);
	});

	it.each([
		['a Hugo link', '[old](https://ome-projects.github.io/ome/docs/concepts/)', /old Hugo site/],
		[
			'an absolute self link',
			'[x](https://lightseek.org/ome/guides)',
			/absolute link to this site/
		],
		['an absolute path', '[x](/ome/guides)', /absolute path/],
		['a link out of the content', '[x](../../../../Makefile.md)', /leaves src\/lib\/content/],
		['a relative non-page link', '[x](../../../config/runtimes/)', /must point to a \.md page/],
		['a javascript link', '[x](javascript:alert(1))', /unsupported link/]
	])('rejects %s', (_name, body, message) => {
		expect(() => render(body)).toThrow(message);
	});

	it('resolves images in static/images', () => {
		expect(render('![Diagram](/images/diagram.svg)').html).toContain(
			'<img src="/ome/images/diagram.svg" alt="Diagram" loading="lazy">'
		);
		expect(() => render('![x](/images/missing.svg)')).toThrow(/does not exist/);
		expect(() => render('![x](diagram.svg)')).toThrow(/under \/images\//);
	});
});

describe('search text', () => {
	it('keeps prose and headings but drops code and badges', () => {
		const { searchText } = render(
			'## Install {since=v1.3}\n\nRun the `helm` command.\n\n```bash\nhelm install secret-flag\n```\n\n- one\n- two'
		);
		expect(searchText).toBe('Install Run the helm command. one two');
	});
});

describe('content patterns', () => {
	it('renders GFM tables', () => {
		const { html } = render('| Field | Type |\n| --- | --- |\n| `name` | string |');
		expect(html).toContain('<table>');
		expect(html).toContain('<td><code>name</code></td>');
	});

	it('renders fences inside list items', () => {
		const { html } = render(
			'1. Apply it:\n\n   ```bash\n   kubectl apply -f model.yaml\n   ```\n\n2. Check it.'
		);
		expect(html).toContain('<li><p>Apply it:</p>\n<div class="doc-code">');
		expect(html).toContain('<code class="hljs language-bash">kubectl apply -f model.yaml</code>');
		expect(html).toContain('<li><p>Check it.</p>');
	});

	it('renders Markdown inside HTML table cells, as the API reference does', () => {
		const { html, meta } = render(
			[
				'## `BaseModel` {#ome-io-v1beta1-BaseModel}',
				'',
				'<table class="doc-api-fields">',
				'<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>',
				'<tbody>',
				'<tr><td><code>spec</code> <span class="doc-api-required">Required</span></td>',
				'<td><a href="#ome-io-v1beta1-BaseModelSpec"><code>BaseModelSpec</code></a></td>',
				'<td>',
				'',
				'Where the weights live. See [storage](#ome-io-v1beta1-BaseModel) and:',
				'',
				'- `hf://org/model`',
				'',
				'</td>',
				'</tr>',
				'</tbody>',
				'</table>'
			].join('\n')
		);
		expect(html).toContain(
			'<h2 id="ome-io-v1beta1-BaseModel"><code>BaseModel</code><a class="doc-headerlink"'
		);
		expect(html).toContain(
			'<td><p>Where the weights live. See <a href="#ome-io-v1beta1-BaseModel">storage</a> and:</p>'
		);
		expect(html).toContain('<li><code>hf://org/model</code></li>');
		expect(meta.links).toEqual([
			{ route: 'guides/deploy-models/example', anchor: 'ome-io-v1beta1-BaseModelSpec' },
			{ route: 'guides/deploy-models/example', anchor: 'ome-io-v1beta1-BaseModel' }
		]);
	});
});
`````

### `website/src/lib/markdown/render.ts`

```ts
import { Marked, type Tokens } from 'marked';
import { pathToRoute, sectionOf } from '../docs/paths.ts';
import type { LinkRef, PageMeta, TocEntry } from '../docs/types.ts';
import { BLOCK_PLACEHOLDER, extractBlocks, type DocBlock } from './blocks.ts';
import { maskFences } from './fences.ts';
import { parseFrontMatter, SINCE_PATTERN } from './frontmatter.ts';
import { highlight, LANGUAGES, type Language } from './highlight.ts';
import { blockHtmlToText, escapeHtml, htmlToText } from './html.ts';
import { resolveImage, resolveLink, type ImageContext } from './links.ts';
import { createSlugger } from './slug.ts';

export interface RenderOptions {
	/** Content path relative to src/lib/content, with forward slashes. */
	path: string;
	/** Site base path, such as `/ome`. */
	basePath: string;
	/** Whether a path such as `/images/x.svg` exists in the static directory. */
	assetExists: (publicPath: string) => boolean;
}

export interface RenderedPage {
	meta: PageMeta;
	html: string;
	/** Body text without code blocks, for the search index. */
	searchText: string;
}

const HEADING_ID = /^[A-Za-z0-9][\w.-]*$/;
const HEADING_ATTRIBUTES = /\s*\{([^{}]*)\}\s*$/;
const TOC_HEADING = /<h([23]) id="([^"]+)">([\s\S]*?)<\/h\1>/g;
const SINCE_BADGE = /<span class="doc-since[^"]*"[^>]*>[^<]*<\/span>/g;
const HEADERLINK = /<a class="doc-headerlink"[^>]*>[^<]*<\/a>/g;
const CODE_BLOCK = /<div class="doc-code[^"]*">[\s\S]*?<\/pre><\/div>/g;

const headerlink = (id: string) =>
	`<a class="doc-headerlink" href="#${id}" aria-label="Permanent link">¶</a>`;

/** Renders one Markdown page into its metadata, HTML and search text. */
export function renderPage(source: string, options: RenderOptions): RenderedPage {
	const { path } = options;
	const { data, body, bodyLine } = parseFrontMatter(source, path);
	const section = sectionOf(path);
	const route = pathToRoute(path);

	if (data.status === 'draft' && body.trim() !== '') {
		throw new Error(
			`${path}: draft pages have no body; remove "status: draft" once the page is written`
		);
	}
	const html = data.status === 'draft' ? '' : renderBody(body, bodyLine, options);

	const meta: PageMeta = {
		path,
		route,
		section,
		title: data.title,
		navLabel: data.navLabel ?? data.title,
		description: data.description,
		status: data.status,
		since: data.since,
		generated: data.generated,
		toc: extractToc(html),
		anchors: [...new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]))],
		links: extractLinks(html, route, options.basePath)
	};

	return { meta, html, searchText: searchText(html) };
}

function renderBody(body: string, firstLine: number, options: RenderOptions): string {
	const { path } = options;
	if (body.includes('\u0000')) throw new Error(`${path}: contains a NUL character`);

	const masked = maskFences(body, path, firstLine);
	const slugger = createSlugger(path);
	const imageContext: ImageContext = options;
	const levels: DocBlock[][] = [];
	let tabSets = 0;

	const marked = new Marked({
		gfm: true,
		renderer: {
			heading({ tokens, depth }) {
				let inner = this.parser.parseInline(tokens);
				if (depth === 1) {
					throw new Error(
						`${path}: "# ${htmlToText(inner)}": the title comes from front matter, so sections start at ##`
					);
				}
				let id: string | null = null;
				let since: string | null = null;
				const attributes = HEADING_ATTRIBUTES.exec(inner);
				if (attributes) {
					inner = inner.slice(0, attributes.index);
					for (const part of attributes[1].trim().split(/\s+/).filter(Boolean)) {
						if (part.startsWith('#') && HEADING_ID.test(part.slice(1))) id = part.slice(1);
						else if (part.startsWith('since=') && SINCE_PATTERN.test(part.slice(6)))
							since = part.slice(6);
						else {
							throw new Error(
								`${path}: heading "${htmlToText(inner)}" has an invalid attribute "${part}"; use {#id} and {since=v1.3}`
							);
						}
					}
				}
				if (id) slugger.claim(id);
				else id = slugger.slug(htmlToText(inner));
				const badge = since
					? `<span class="doc-since" data-since="${since}">Since ${since}</span>`
					: '';
				return `<h${depth} id="${id}">${inner}${badge}${headerlink(id)}</h${depth}>\n`;
			},
			code({ text, lang }) {
				const { language, title } = parseInfo(lang ?? '', path);
				const code = highlight(text, language);
				if (language.name === 'output') {
					return `<div class="doc-code doc-code--output"><div class="doc-code-bar"><span class="doc-code-title">${escapeHtml(title ?? language.label)}</span></div><pre class="doc-pre"><code class="language-output">${code}</code></pre></div>\n`;
				}
				const hljsClass = language.grammar ? 'hljs ' : '';
				return `<div class="doc-code"><div class="doc-code-bar"><span class="doc-code-title">${escapeHtml(title ?? language.label)}</span><button type="button" class="doc-code-copy" aria-label="Copy to clipboard">Copy</button></div><pre class="doc-pre"><code class="${hljsClass}language-${language.name}">${code}</code></pre></div>\n`;
			},
			link({ href, title, tokens }) {
				const text = this.parser.parseInline(tokens);
				const resolved = resolveLink(href, options);
				const titleAttribute = title ? ` title="${escapeHtml(title)}"` : '';
				return `<a href="${escapeHtml(resolved)}"${titleAttribute}>${text}</a>`;
			},
			image({ href, title, tokens, text }) {
				const alt = tokens ? htmlToText(this.parser.parseInline(tokens)) : text;
				const src = resolveImage(href, imageContext);
				const titleAttribute = title ? ` title="${escapeHtml(title)}"` : '';
				return `<img src="${escapeHtml(src)}" alt="${escapeHtml(alt)}"${titleAttribute} loading="lazy">`;
			},
			html(token: Tokens.HTML | Tokens.Tag) {
				const placeholder = BLOCK_PLACEHOLDER.exec(token.text);
				if (!placeholder) return token.text;
				const block = levels[levels.length - 1][Number(placeholder[1])];
				// Rendering a block runs nested parses, which rebind this.parser.
				const parser = this.parser;
				try {
					return renderBlock(block);
				} finally {
					this.parser = parser;
				}
			}
		}
	});

	const inline = (text: string) => marked.parseInline(text, { async: false }) as string;

	// Renders one nesting level: extract its blocks, restore its code, parse,
	// and render each block where its placeholder sits, in document order.
	const renderLevel = (text: string): string => {
		const { text: withPlaceholders, blocks } = extractBlocks(text, path);
		levels.push(blocks);
		try {
			return marked.parse(masked.restore(withPlaceholders), { async: false }) as string;
		} finally {
			levels.pop();
		}
	};

	const renderBlock = (block: DocBlock): string => {
		switch (block.kind) {
			case 'prerequisites':
				slugger.claim('before-you-begin');
				return `<aside class="doc-prerequisites"><h2 id="before-you-begin">Before you begin${headerlink('before-you-begin')}</h2>${renderLevel(block.body)}</aside>\n`;
			case 'cards':
				return `<div class="doc-grid">${block.items.map((item) => `<div class="doc-card">${renderLevel(item)}</div>`).join('')}</div>\n`;
			case 'admonition': {
				const title = block.title
					? `<p class="doc-admonition-title">${inline(block.title)}</p>`
					: '';
				return `<aside class="doc-admonition doc-admonition--${block.type}">${title}${renderLevel(block.body)}</aside>\n`;
			}
			case 'details':
				return `<details class="doc-details doc-details--${block.type}"${block.open ? ' open' : ''}><summary>${inline(block.title)}</summary>${renderLevel(block.body)}</details>\n`;
			case 'tabs': {
				const set = ++tabSets;
				const inputs = block.tabs
					.map(
						(_, i) =>
							`<input type="radio" class="doc-tab-input" name="doc-tab-set-${set}" id="doc-tab-${set}-${i}"${i === 0 ? ' checked' : ''}>`
					)
					.join('');
				const labels = block.tabs
					.map(
						(tab, i) =>
							`<label class="doc-tab-label" for="doc-tab-${set}-${i}">${escapeHtml(tab.title)}</label>`
					)
					.join('');
				const panels = block.tabs
					.map((tab) => `<div class="doc-tab-panel">${renderLevel(tab.body)}</div>`)
					.join('');
				return `<div class="doc-tabbed-set">${inputs}<div class="doc-tab-labels">${labels}</div><div class="doc-tab-panels">${panels}</div></div>\n`;
			}
		}
	};

	return renderLevel(masked.text);
}

/** Splits a fence info string such as `yaml title="model.yaml"` into a language and attributes. */
function parseInfo(info: string, path: string): { language: Language; title: string | null } {
	const trimmed = info.trim();
	const name = trimmed.split(/\s+/)[0];
	if (!name)
		throw new Error(`${path}: code blocks need a language, such as \`\`\`yaml or \`\`\`bash`);
	const language = LANGUAGES[name.toLowerCase()];
	if (!language) {
		throw new Error(
			`${path}: unsupported code block language "${name}"; use one of ${Object.keys(LANGUAGES).join(', ')}`
		);
	}

	let title: string | null = null;
	const attributes = trimmed.slice(name.length);
	const attribute = /\s*([\w-]+)=(?:"([^"]*)"|(\S+))/y;
	let position = 0;
	while (attributes.slice(position).trim() !== '') {
		attribute.lastIndex = position;
		const match = attribute.exec(attributes);
		if (!match) throw new Error(`${path}: cannot parse code block attributes in \`\`\`${trimmed}`);
		position = attribute.lastIndex;
		const [whole, key, quoted, bare] = match;
		if (key === 'title' && quoted) title = quoted;
		else if (key === 'check' && bare === 'skip' && language.name === 'yaml') continue;
		else
			throw new Error(
				`${path}: unsupported code block attribute "${whole.trim()}" in \`\`\`${trimmed}`
			);
	}
	return { language, title };
}

function extractToc(html: string): TocEntry[] {
	return [...html.matchAll(TOC_HEADING)].map(([, depth, id, inner]) => ({
		depth: Number(depth) as 2 | 3,
		id,
		text: htmlToText(inner.replace(SINCE_BADGE, '').replace(HEADERLINK, ''))
	}));
}

function extractLinks(html: string, route: string, basePath: string): LinkRef[] {
	const links = new Map<string, LinkRef>();
	for (const [, raw] of html.replace(HEADERLINK, '').matchAll(/\shref="([^"]*)"/g)) {
		const href = raw.replaceAll('&amp;', '&');
		let target: string;
		if (href.startsWith('#')) target = `${route}${href}`;
		else if (href.startsWith(`${basePath}/`)) target = href.slice(basePath.length + 1);
		else continue;
		const hash = target.indexOf('#');
		const link: LinkRef =
			hash === -1
				? { route: target, anchor: null }
				: { route: target.slice(0, hash), anchor: target.slice(hash + 1) || null };
		links.set(`${link.route}#${link.anchor ?? ''}`, link);
	}
	return [...links.values()];
}

function searchText(html: string): string {
	return blockHtmlToText(
		html.replace(CODE_BLOCK, ' ').replace(SINCE_BADGE, ' ').replace(HEADERLINK, '')
	);
}
```

### `website/src/lib/markdown/slug.test.ts`

```ts
import { describe, expect, it } from 'vitest';
import { createSlugger, slugify } from './slug';

describe('slugify', () => {
	it.each([
		['Install the chart', 'install-the-chart'],
		['  kubectl ome rollout  ', 'kubectl-ome-rollout'],
		['What is OME?', 'what-is-ome'],
		['spec.modelRef', 'specmodelref'],
		['GPU/NPU support', 'gpunpu-support'],
		['snake_case stays', 'snake_case-stays']
	])('%s -> %s', (text, id) => {
		expect(slugify(text)).toBe(id);
	});
});

describe('createSlugger', () => {
	it('numbers repeats and skips claimed ids', () => {
		const slugger = createSlugger('page.md');
		slugger.claim('verify-1');
		expect(['Verify', 'Verify', 'Verify', '???'].map((text) => slugger.slug(text))).toEqual([
			'verify',
			'verify-2',
			'verify-3',
			'section'
		]);
		expect(() => slugger.claim('verify')).toThrow('page.md: duplicate heading id "verify"');
	});
});
```

### `website/src/lib/markdown/slug.ts`

```ts
/** Heading text to id: lowercase, punctuation dropped, spaces to hyphens. Matches SMG. */
export function slugify(text: string): string {
	return text
		.trim()
		.toLowerCase()
		.replace(/[^\w\s-]/g, '')
		.replace(/\s+/g, '-');
}

export interface Slugger {
	/** Claims an explicit id. Throws if the page already uses it. */
	claim(id: string): void;
	/** Returns a unique id for heading text, adding -1, -2 for repeats. */
	slug(text: string): string;
}

export function createSlugger(path: string): Slugger {
	const used = new Set<string>();
	const counts = new Map<string, number>();

	return {
		claim(id) {
			if (used.has(id)) throw new Error(`${path}: duplicate heading id "${id}"`);
			used.add(id);
		},
		slug(text) {
			const base = slugify(text) || 'section';
			let count = counts.get(base) ?? 0;
			let id = base;
			while (used.has(id)) {
				count += 1;
				id = `${base}-${count}`;
			}
			counts.set(base, count);
			used.add(id);
			return id;
		}
	};
}
```

## Appendix B: Navigation, redirects and page data

Task A1 copies `nav.ts` and `redirects.json` into `website/`. `pages.tsv` and `descriptions.tsv` feed the draft generator in Appendix C: each `pages.tsv` row is the content path, the title (empty for section landings), `preview` or empty, and the Hugo pages it replaces (`-` for new pages).

### `website/src/lib/config/nav.ts`

```ts
import type { NavSection } from '$lib/docs/types';

/** Sidebar order for each section. Each section's index.md is its landing page and is not listed. */
export const nav: NavSection[] = [
	{
		id: 'getting-started',
		label: 'Getting Started',
		groups: [
			{
				label: null,
				pages: [
					'introduction.md',
					'install.md',
					'serve-your-first-model.md',
					'pre-configured-models.md',
					'private-registries.md'
				]
			}
		]
	},
	{
		id: 'guides',
		label: 'Guides',
		groups: [
			{
				label: 'Deploy models',
				pages: [
					'deploy-models/select-accelerators.md',
					'deploy-models/serve-models-from-pvc.md',
					'deploy-models/serve-models-from-local-storage.md',
					'deploy-models/reference-a-runtime-explicitly.md',
					'deploy-models/troubleshoot-runtime-selection.md',
					'deploy-models/run-benchmarks.md'
				]
			},
			{
				label: 'Networking',
				pages: [
					'networking/configure-route-timeouts.md',
					'networking/set-service-app-protocols.md',
					'networking/configure-ingress.md',
					'networking/gateway-host-schemes.md',
					'networking/multiple-gateways.md',
					'networking/namespace-gateways.md'
				]
			},
			{
				label: 'Roll out changes',
				pages: [
					'roll-out-changes/pause-and-resume-a-rollout.md',
					'roll-out-changes/promote-or-roll-back-a-canary.md',
					'roll-out-changes/release-a-held-revision.md',
					'roll-out-changes/repin-a-drifted-rollout-plan.md',
					'roll-out-changes/pace-rollouts-with-min-ready-seconds.md'
				]
			},
			{
				label: 'Scale and migrate',
				pages: [
					'scale-and-migrate/request-a-transient-scale.md',
					'scale-and-migrate/request-an-instance-migration.md'
				]
			},
			{
				label: 'Multi-cluster',
				preview: true,
				pages: [
					'multi-cluster/routing-health-probes.md',
					'multi-cluster/drain-a-workload-cluster.md'
				]
			},
			{
				label: 'Operate OME',
				pages: [
					'operate-ome/configure-the-controller.md',
					'operate-ome/model-agent.md',
					'operate-ome/ome-scheduler.md',
					'operate-ome/accelerator-quota.md',
					'operate-ome/shared-hf-artifacts.md',
					'operate-ome/metrics.md',
					'operate-ome/alerting.md'
				]
			},
			{
				label: 'Troubleshoot',
				pages: ['troubleshoot/troubleshoot-an-inferenceservice.md']
			}
		]
	},
	{
		id: 'concepts',
		label: 'Concepts',
		groups: [
			{
				label: 'Architecture',
				pages: [
					'architecture/how-ome-works.md',
					'architecture/deployment-modes.md',
					'architecture/omenative-update-strategies.md'
				]
			},
			{
				label: 'Models',
				pages: ['models/base-models.md', 'models/fine-tuned-weights.md']
			},
			{
				label: 'Runtimes',
				pages: [
					'runtimes/serving-runtimes.md',
					'runtimes/runtime-inheritance.md',
					'runtimes/runtime-revisions.md',
					'runtimes/accelerator-classes.md'
				]
			},
			{
				label: 'Serving',
				pages: [
					'serving/inference-services.md',
					'serving/gang-scheduling.md',
					'serving/autoscaler-policy.md',
					'serving/benchmarks.md'
				]
			},
			{
				label: 'Rollouts and traffic',
				pages: [
					'rollouts-and-traffic/rollout-policy.md',
					'rollouts-and-traffic/rollout-groups.md',
					'rollouts-and-traffic/traffic-policy.md',
					'rollouts-and-traffic/traffic-map.md',
					'rollouts-and-traffic/ingress.md'
				]
			}
		]
	},
	{
		id: 'reference',
		label: 'Reference',
		groups: [
			{
				label: 'API',
				pages: ['api/ome.v1beta1.md', 'api/labels-and-annotations.md', 'api/traffic-annotations.md']
			},
			{
				label: 'kubectl ome',
				pages: [
					'kubectl-ome/overview.md',
					'kubectl-ome/accelerator.md',
					'kubectl-ome/admin.md',
					'kubectl-ome/autoscale.md',
					'kubectl-ome/cluster.md',
					'kubectl-ome/get.md',
					'kubectl-ome/instance.md',
					'kubectl-ome/logs.md',
					'kubectl-ome/migration.md',
					'kubectl-ome/placement.md',
					'kubectl-ome/quota.md',
					'kubectl-ome/rollout.md',
					'kubectl-ome/runtime.md',
					'kubectl-ome/scale.md',
					'kubectl-ome/status.md',
					'kubectl-ome/traffic.md',
					'kubectl-ome/version.md',
					'kubectl-ome/wait.md',
					'kubectl-ome/guarded-actions.md'
				]
			},
			{
				label: 'Matching',
				pages: [
					'matching/model-version-matching.md',
					'matching/runtime-accelerator-class-matching.md',
					'matching/runtime-deployment-mode-matching.md',
					'matching/diffusion-pipeline-runtime-matching.md'
				]
			},
			{
				label: 'Rollouts',
				pages: ['rollouts/canary-progression.md', 'rollouts/canary-analysis.md']
			},
			{
				label: 'Storage',
				pages: ['storage/benchmark-output-storage.md']
			}
		]
	},
	{
		id: 'contributing',
		label: 'Contributing',
		groups: [
			{
				label: null,
				pages: ['development-setup.md', 'pull-requests-and-oeps.md', 'writing-docs.md']
			}
		]
	}
];
```

### `website/redirects.json`

```json
[
	{
		"old": "_index.md",
		"new": "getting-started/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "overview/_index.md",
		"new": "getting-started/introduction.md",
		"rewrittenFrom": null
	},
	{
		"old": "installation/_index.md",
		"new": "getting-started/install.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/deploy-inference-service.md",
		"new": "getting-started/serve-your-first-model.md",
		"rewrittenFrom": null
	},
	{
		"old": "installation/ome-serving.md",
		"new": "getting-started/pre-configured-models.md",
		"rewrittenFrom": null
	},
	{
		"old": "installation/private-registries.md",
		"new": "getting-started/private-registries.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/_index.md",
		"new": "guides/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/_index.md",
		"new": "guides/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/select-accelerators.md",
		"new": "guides/deploy-models/select-accelerators.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/serve-models-from-pvc.md",
		"new": "guides/deploy-models/serve-models-from-pvc.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/serve-models-from-local-storage.md",
		"new": "guides/deploy-models/serve-models-from-local-storage.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/reference-a-runtime-explicitly.md",
		"new": "guides/deploy-models/reference-a-runtime-explicitly.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/troubleshoot-runtime-selection.md",
		"new": "guides/deploy-models/troubleshoot-runtime-selection.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/run-benchmarks.md",
		"new": "guides/deploy-models/run-benchmarks.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/configure-route-timeouts.md",
		"new": "guides/networking/configure-route-timeouts.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/set-service-app-protocols.md",
		"new": "guides/networking/set-service-app-protocols.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/ingress.md",
		"new": "guides/networking/configure-ingress.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/gateway-host-schemes.md",
		"new": "guides/networking/gateway-host-schemes.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/multiple-gateways.md",
		"new": "guides/networking/multiple-gateways.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/namespace-gateways.md",
		"new": "guides/networking/namespace-gateways.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/pause-and-resume-a-rollout.md",
		"new": "guides/roll-out-changes/pause-and-resume-a-rollout.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/promote-or-rollback-a-canary.md",
		"new": "guides/roll-out-changes/promote-or-roll-back-a-canary.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/release-a-held-revision.md",
		"new": "guides/roll-out-changes/release-a-held-revision.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/repin-a-drifted-rollout-plan.md",
		"new": "guides/roll-out-changes/repin-a-drifted-rollout-plan.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md",
		"new": "guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/request-a-transient-scale.md",
		"new": "guides/scale-and-migrate/request-a-transient-scale.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/request-an-instance-migration.md",
		"new": "guides/scale-and-migrate/request-an-instance-migration.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/routing-health-probes.md",
		"new": "guides/multi-cluster/routing-health-probes.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/drain-traffic-from-a-workload-cluster.md",
		"new": "guides/multi-cluster/drain-a-workload-cluster.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/controller-configuration.md",
		"new": "guides/operate-ome/configure-the-controller.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/model-agent.md",
		"new": "guides/operate-ome/model-agent.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/ome-scheduler.md",
		"new": "guides/operate-ome/ome-scheduler.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/accelerator-quota.md",
		"new": "guides/operate-ome/accelerator-quota.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/shared-hf-artifacts.md",
		"new": "guides/operate-ome/shared-hf-artifacts.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/metrics.md",
		"new": "guides/operate-ome/metrics.md",
		"rewrittenFrom": null
	},
	{
		"old": "administration/alerting.md",
		"new": "guides/operate-ome/alerting.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/_index.md",
		"new": "concepts/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/omenative.md",
		"new": "concepts/architecture/deployment-modes.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/omenative-update-strategies.md",
		"new": "concepts/architecture/omenative-update-strategies.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/base_model.md",
		"new": "concepts/models/base-models.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/fine_tuned_weight.md",
		"new": "concepts/models/fine-tuned-weights.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/serving_runtime.md",
		"new": "concepts/runtimes/serving-runtimes.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/runtime_inheritance.md",
		"new": "concepts/runtimes/runtime-inheritance.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/runtime-revision.md",
		"new": "concepts/runtimes/runtime-revisions.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/accelerator_class.md",
		"new": "concepts/runtimes/accelerator-classes.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/inference_service.md",
		"new": "concepts/serving/inference-services.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/gang_scheduling.md",
		"new": "concepts/serving/gang-scheduling.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/autoscaler_policy.md",
		"new": "concepts/serving/autoscaler-policy.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/benchmark.md",
		"new": "concepts/serving/benchmarks.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/rollout_policy.md",
		"new": "concepts/rollouts-and-traffic/rollout-policy.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/rollout_groups.md",
		"new": "concepts/rollouts-and-traffic/rollout-groups.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/traffic_policy.md",
		"new": "concepts/rollouts-and-traffic/traffic-policy.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/traffic_map.md",
		"new": "concepts/rollouts-and-traffic/traffic-map.md",
		"rewrittenFrom": null
	},
	{
		"old": "concepts/ingress.md",
		"new": "concepts/rollouts-and-traffic/ingress.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/_index.md",
		"new": "reference/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/ome.v1beta1.md",
		"new": "reference/api/ome.v1beta1.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/labels-and-annotations.md",
		"new": "reference/api/labels-and-annotations.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/traffic-annotations.md",
		"new": "reference/api/traffic-annotations.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome.md",
		"new": "reference/kubectl-ome/overview.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-accelerator-explain.md",
		"new": "reference/kubectl-ome/accelerator.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-doctor.md",
		"new": "reference/kubectl-ome/admin.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-autoscale-status.md",
		"new": "reference/kubectl-ome/autoscale.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-autoscale-explain.md",
		"new": "reference/kubectl-ome/autoscale.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-logs.md",
		"new": "reference/kubectl-ome/logs.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-migration.md",
		"new": "reference/kubectl-ome/migration.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-rollout-explain.md",
		"new": "reference/kubectl-ome/rollout.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-rollout-history.md",
		"new": "reference/kubectl-ome/rollout.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-rollout-validate.md",
		"new": "reference/kubectl-ome/rollout.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-runtime-effective.md",
		"new": "reference/kubectl-ome/runtime.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-runtime-tree.md",
		"new": "reference/kubectl-ome/runtime.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-status.md",
		"new": "reference/kubectl-ome/status.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-traffic-explain.md",
		"new": "reference/kubectl-ome/traffic.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-traffic-status.md",
		"new": "reference/kubectl-ome/traffic.md",
		"rewrittenFrom": null
	},
	{
		"old": "tasks/kubectl-ome-wait.md",
		"new": "reference/kubectl-ome/wait.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/kubectl-ome-guarded-actions.md",
		"new": "reference/kubectl-ome/guarded-actions.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/model-version-matching.md",
		"new": "reference/matching/model-version-matching.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/runtime-accelerator-class-matching.md",
		"new": "reference/matching/runtime-accelerator-class-matching.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/runtime-deployment-mode-matching.md",
		"new": "reference/matching/runtime-deployment-mode-matching.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/diffusion-pipeline-runtime-matching.md",
		"new": "reference/matching/diffusion-pipeline-runtime-matching.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/canary-progression.md",
		"new": "reference/rollouts/canary-progression.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/canary-analysis.md",
		"new": "reference/rollouts/canary-analysis.md",
		"rewrittenFrom": null
	},
	{
		"old": "reference/benchmark-output-storage.md",
		"new": "reference/storage/benchmark-output-storage.md",
		"rewrittenFrom": null
	},
	{
		"old": "developer-guide/_index.md",
		"new": "contributing/index.md",
		"rewrittenFrom": null
	},
	{
		"old": "developer-guide/contributing.md",
		"new": "contributing/development-setup.md",
		"rewrittenFrom": null
	}
]
```

### `/tmp/plan-parts/pages.tsv`

Tab-separated.

```text
getting-started/index.md			_index.md
getting-started/introduction.md	Introduction		overview/_index.md
getting-started/install.md	Install OME		installation/_index.md
getting-started/serve-your-first-model.md	Serve your first model		tasks/run-workloads/deploy-inference-service.md
getting-started/pre-configured-models.md	Pre-configured models and runtimes		installation/ome-serving.md
getting-started/private-registries.md	Install from a private registry		installation/private-registries.md
guides/index.md			tasks/_index.md,administration/_index.md
guides/deploy-models/select-accelerators.md	Select accelerators		tasks/run-workloads/select-accelerators.md
guides/deploy-models/serve-models-from-pvc.md	Serve models from a PVC		tasks/run-workloads/serve-models-from-pvc.md
guides/deploy-models/serve-models-from-local-storage.md	Serve models from node-local storage		tasks/run-workloads/serve-models-from-local-storage.md
guides/deploy-models/reference-a-runtime-explicitly.md	Reference a runtime explicitly		tasks/run-workloads/reference-a-runtime-explicitly.md
guides/deploy-models/troubleshoot-runtime-selection.md	Troubleshoot runtime selection		tasks/run-workloads/troubleshoot-runtime-selection.md
guides/deploy-models/run-benchmarks.md	Run benchmarks		tasks/run-workloads/run-benchmarks.md
guides/networking/configure-route-timeouts.md	Configure route timeouts		tasks/run-workloads/configure-route-timeouts.md
guides/networking/set-service-app-protocols.md	Set appProtocol on services		tasks/run-workloads/set-service-app-protocols.md
guides/networking/configure-ingress.md	Configure ingress		administration/ingress.md
guides/networking/gateway-host-schemes.md	Choose a Gateway API host scheme		administration/gateway-host-schemes.md
guides/networking/multiple-gateways.md	Use multiple gateways		administration/multiple-gateways.md
guides/networking/namespace-gateways.md	Use per-namespace gateways		administration/namespace-gateways.md
guides/roll-out-changes/pause-and-resume-a-rollout.md	Pause and resume a rollout		tasks/pause-and-resume-a-rollout.md
guides/roll-out-changes/promote-or-roll-back-a-canary.md	Promote or roll back a canary		tasks/promote-or-rollback-a-canary.md
guides/roll-out-changes/release-a-held-revision.md	Release a held revision		tasks/release-a-held-revision.md
guides/roll-out-changes/repin-a-drifted-rollout-plan.md	Repin a drifted rollout plan		tasks/repin-a-drifted-rollout-plan.md
guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md	Pace rollouts with minReadySeconds		tasks/run-workloads/pace-rollouts-with-min-ready-seconds.md
guides/scale-and-migrate/request-a-transient-scale.md	Request a transient scale		tasks/request-a-transient-scale.md
guides/scale-and-migrate/request-an-instance-migration.md	Request an instance migration		tasks/request-an-instance-migration.md
guides/multi-cluster/routing-health-probes.md	Configure routing health probes	preview	administration/routing-health-probes.md
guides/multi-cluster/drain-a-workload-cluster.md	Drain a workload cluster	preview	tasks/drain-traffic-from-a-workload-cluster.md
guides/operate-ome/configure-the-controller.md	Configure the controller		administration/controller-configuration.md
guides/operate-ome/model-agent.md	Run the model agent		administration/model-agent.md
guides/operate-ome/ome-scheduler.md	Use the OME scheduler		administration/ome-scheduler.md
guides/operate-ome/accelerator-quota.md	Set accelerator quotas		administration/accelerator-quota.md
guides/operate-ome/shared-hf-artifacts.md	Share Hugging Face artifacts		administration/shared-hf-artifacts.md
guides/operate-ome/metrics.md	Collect metrics		administration/metrics.md
guides/operate-ome/alerting.md	Set up alerting		administration/alerting.md
guides/troubleshoot/troubleshoot-an-inferenceservice.md	Troubleshoot an InferenceService		-
concepts/index.md			concepts/_index.md
concepts/architecture/how-ome-works.md	How OME works		-
concepts/architecture/deployment-modes.md	Deployment modes and OMENative		concepts/omenative.md
concepts/architecture/omenative-update-strategies.md	OMENative update strategies		concepts/omenative-update-strategies.md
concepts/models/base-models.md	Base models		concepts/base_model.md
concepts/models/fine-tuned-weights.md	Fine-tuned weights		concepts/fine_tuned_weight.md
concepts/runtimes/serving-runtimes.md	Serving runtimes		concepts/serving_runtime.md
concepts/runtimes/runtime-inheritance.md	Runtime inheritance		concepts/runtime_inheritance.md
concepts/runtimes/runtime-revisions.md	Runtime revisions and pinning		concepts/runtime-revision.md
concepts/runtimes/accelerator-classes.md	Accelerator classes		concepts/accelerator_class.md
concepts/serving/inference-services.md	InferenceService		concepts/inference_service.md
concepts/serving/gang-scheduling.md	Gang scheduling		concepts/gang_scheduling.md
concepts/serving/autoscaler-policy.md	Autoscaler policy		concepts/autoscaler_policy.md
concepts/serving/benchmarks.md	Benchmarks		concepts/benchmark.md
concepts/rollouts-and-traffic/rollout-policy.md	Rollout policy		concepts/rollout_policy.md
concepts/rollouts-and-traffic/rollout-groups.md	Rollout groups		concepts/rollout_groups.md
concepts/rollouts-and-traffic/traffic-policy.md	Traffic policy		concepts/traffic_policy.md
concepts/rollouts-and-traffic/traffic-map.md	Traffic map	preview	concepts/traffic_map.md
concepts/rollouts-and-traffic/ingress.md	Ingress and external access		concepts/ingress.md
reference/index.md			reference/_index.md
reference/api/ome.v1beta1.md	OME API		reference/ome.v1beta1.md
reference/api/labels-and-annotations.md	Labels and annotations		reference/labels-and-annotations.md
reference/api/traffic-annotations.md	Traffic annotations		reference/traffic-annotations.md
reference/kubectl-ome/overview.md	kubectl-ome overview and install		tasks/kubectl-ome.md
reference/kubectl-ome/accelerator.md	kubectl ome accelerator		tasks/kubectl-ome-accelerator-explain.md
reference/kubectl-ome/admin.md	kubectl ome admin		tasks/kubectl-ome-doctor.md
reference/kubectl-ome/autoscale.md	kubectl ome autoscale		tasks/kubectl-ome-autoscale-status.md,tasks/kubectl-ome-autoscale-explain.md
reference/kubectl-ome/cluster.md	kubectl ome cluster	preview	-
reference/kubectl-ome/get.md	kubectl ome get		-
reference/kubectl-ome/instance.md	kubectl ome instance		-
reference/kubectl-ome/logs.md	kubectl ome logs		tasks/kubectl-ome-logs.md
reference/kubectl-ome/migration.md	kubectl ome migration		tasks/kubectl-ome-migration.md
reference/kubectl-ome/placement.md	kubectl ome placement	preview	-
reference/kubectl-ome/quota.md	kubectl ome quota		-
reference/kubectl-ome/rollout.md	kubectl ome rollout		tasks/kubectl-ome-rollout-explain.md,tasks/kubectl-ome-rollout-history.md,tasks/kubectl-ome-rollout-validate.md
reference/kubectl-ome/runtime.md	kubectl ome runtime		tasks/kubectl-ome-runtime-effective.md,tasks/kubectl-ome-runtime-tree.md
reference/kubectl-ome/scale.md	kubectl ome scale		-
reference/kubectl-ome/status.md	kubectl ome status		tasks/kubectl-ome-status.md
reference/kubectl-ome/traffic.md	kubectl ome traffic		tasks/kubectl-ome-traffic-explain.md,tasks/kubectl-ome-traffic-status.md
reference/kubectl-ome/version.md	kubectl ome version		-
reference/kubectl-ome/wait.md	kubectl ome wait		tasks/kubectl-ome-wait.md
reference/kubectl-ome/guarded-actions.md	Guarded actions		reference/kubectl-ome-guarded-actions.md
reference/matching/model-version-matching.md	Model version matching		reference/model-version-matching.md
reference/matching/runtime-accelerator-class-matching.md	Runtime accelerator-class matching		reference/runtime-accelerator-class-matching.md
reference/matching/runtime-deployment-mode-matching.md	Runtime deployment-mode matching		reference/runtime-deployment-mode-matching.md
reference/matching/diffusion-pipeline-runtime-matching.md	Diffusion pipeline runtime matching		reference/diffusion-pipeline-runtime-matching.md
reference/rollouts/canary-progression.md	Canary progression		reference/canary-progression.md
reference/rollouts/canary-analysis.md	Canary metric analysis		reference/canary-analysis.md
reference/storage/benchmark-output-storage.md	Benchmark output storage		reference/benchmark-output-storage.md
contributing/index.md			developer-guide/_index.md
contributing/development-setup.md	Set up a development environment		developer-guide/contributing.md
contributing/pull-requests-and-oeps.md	Pull requests and OEPs		-
contributing/writing-docs.md	Writing docs		-
```

### `/tmp/plan-parts/descriptions.tsv`

Tab-separated.

```text
getting-started/index.md	Start here if you are new to OME: install the operator, serve your first model, and add pre-configured models and runtimes.
getting-started/introduction.md	OME is a Kubernetes operator for serving large language models: it manages models and runtimes as resources, matches them, and runs the workloads.
getting-started/install.md	Install cert-manager, then the ome-crd and ome-resources Helm charts, and check that the OME controller and model agent are running.
getting-started/serve-your-first-model.md	Create a ClusterBaseModel and an InferenceService, let OME pick a serving runtime, and send your first request to the model.
getting-started/pre-configured-models.md	Install the ome-serving Helm chart to deploy ready-made ClusterBaseModels, SGLang runtimes and InferenceServices from a catalog of over 200 models.
getting-started/private-registries.md	Pull every OME image from your own or a mirrored registry by setting global.hub, and supply pull credentials with imagePullSecrets.
guides/index.md	Follow these step-by-step guides to deploy models, set up networking, roll out changes, scale workloads, and operate and troubleshoot OME.
guides/deploy-models/select-accelerators.md	Pick the AcceleratorClass for each component by naming a class, or let OME choose one with the BestFit, Cheapest, MostCapable or FirstAvailable policy.
guides/deploy-models/serve-models-from-pvc.md	Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes.
guides/deploy-models/serve-models-from-local-storage.md	Serve model weights already on your nodes' disks with a local:// storage URI, which OME validates in place and mounts read-only.
guides/deploy-models/reference-a-runtime-explicitly.md	Name a ServingRuntime or ClusterServingRuntime on an InferenceService to skip auto-selection, and see how the name is resolved and checked.
guides/deploy-models/troubleshoot-runtime-selection.md	Find out why OME cannot auto-select a runtime for your model by reading the RuntimeReady condition and the RuntimeNotFound event.
guides/deploy-models/run-benchmarks.md	Run a BenchmarkJob against an InferenceService with realistic traffic scenarios, and collect its throughput and latency results from storage.
guides/networking/configure-route-timeouts.md	Set the Gateway API request timeout on OME's HTTPRoutes per component or cluster-wide, so Envoy Gateway's 15-second default doesn't cut off inference.
guides/networking/set-service-app-protocols.md	Set Kubernetes appProtocol values on the Service ports OME generates, so gateways and service meshes use the right protocol for your model server.
guides/networking/configure-ingress.md	Configure how OME exposes InferenceServices through Kubernetes Ingress or Gateway API with the ingress block of the inferenceservice-config ConfigMap.
guides/networking/gateway-host-schemes.md	Choose between one shared hostname with per-service path prefixes and a subdomain per InferenceService for the HTTPRoutes OME generates.
guides/networking/multiple-gateways.md	Attach OME's HTTPRoutes to more than one Gateway, such as an internal and an external one, and read each gateway's endpoints in status.
guides/networking/namespace-gateways.md	Route each namespace's InferenceServices through that namespace's own Gateway, and override the gateway for a single service with annotations.
guides/roll-out-changes/pause-and-resume-a-rollout.md	Pause in-flight rollout work on an OMENative InferenceService with the alpha kubectl ome rollout pause action, and continue it with resume.
guides/roll-out-changes/promote-or-roll-back-a-canary.md	Advance a gated canary step with kubectl ome rollout promote, or abort the canary back to the stable revisions with rollback; both are alpha.
guides/roll-out-changes/release-a-held-revision.md	Let the controller retry a revision it has stopped retrying by releasing its Held retry block with the alpha kubectl ome instance release-held action.
guides/roll-out-changes/repin-a-drifted-rollout-plan.md	Apply an edit to spec.rollout or a RolloutPolicy to the active rollout run, keeping its progress, with the alpha kubectl ome rollout repin action.
guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md	Make a new OMENative pod stay Ready for a warm-up window before it counts as Available, so a rollout waits before draining the old pod.
guides/scale-and-migrate/request-a-transient-scale.md	Send one guarded, transient replica request to an OMENative component with the alpha kubectl ome scale command; reconciliation can overwrite it.
guides/scale-and-migrate/request-an-instance-migration.md	Ask OME to move one OMENative instance off its current node with the alpha kubectl ome migration start command, which records the request as an annotation.
guides/multi-cluster/routing-health-probes.md	Configure the alpha end-to-end probe that sets a workload cluster's TrafficMap weight to zero after repeated failures, cluster-wide or per service.
guides/multi-cluster/drain-a-workload-cluster.md	Ask alpha multi-cluster routing to hold a workload cluster at zero traffic weight with kubectl ome traffic drain, and withdraw the request with undrain.
guides/operate-ome/configure-the-controller.md	Tune the OME controller manager's command-line flags, including reconcile concurrency, leader election and runtime-revision garbage collection.
guides/operate-ome/model-agent.md	Configure the model agent DaemonSet that downloads models to each node, parses their metadata, and labels the nodes where each model is ready.
guides/operate-ome/ome-scheduler.md	Install the alpha ome-scheduler as a second scheduler and opt workloads into topology-packed gang placement; it requires Kubernetes 1.35.
guides/operate-ome/accelerator-quota.md	Install the ome-quota-manager and author an AcceleratorQuota tree that OME renders into Kueue GPU budgets on a single cluster.
guides/operate-ome/shared-hf-artifacts.md	Keep one downloaded copy of a Hugging Face snapshot per node and share it across BaseModels and ClusterBaseModels with downloadPolicy ReuseIfExists.
guides/operate-ome/metrics.md	Tune or turn off the short-retention Prometheus that the ome-resources chart deploys for KEDA autoscaling and canary analysis.
guides/operate-ome/alerting.md	Enable the ome-resources chart's five P0 PrometheusRule alerts, which are off by default, and tune their thresholds and selectors.
guides/troubleshoot/troubleshoot-an-inferenceservice.md	Find out why an InferenceService is not ready or serving by running kubectl-ome commands in order, from status through runtime and rollout to logs.
concepts/index.md	Start here to understand how OME works, from its architecture and deployment modes to models, runtimes, serving, rollouts and traffic.
concepts/architecture/how-ome-works.md	The OME manager, the model agent on each node and OME's custom resources work together to turn a model and a runtime into serving pods.
concepts/architecture/deployment-modes.md	OME runs each InferenceService component in RawDeployment, MultiNode or OMENative mode; in OMENative, an InferenceReplica manages the pods directly.
concepts/architecture/omenative-update-strategies.md	OMENative replaces an Instance's pods with SurgeThenDrain, RecreatePod, InPlaceIfPossible or InPlaceOnly, paced by partition, maxSurge and maxUnavailable.
concepts/models/base-models.md	BaseModel and ClusterBaseModel tell OME where a model's weights live and which nodes get them; OME parses the model's architecture, size and capabilities.
concepts/models/fine-tuned-weights.md	A FineTunedWeight, such as a LoRA adapter, stores weights trained from a base model so an InferenceService can serve them on top of that model.
concepts/runtimes/serving-runtimes.md	A ServingRuntime or ClusterServingRuntime defines the pods that serve a model, and the formats, sizes and protocols it supports for auto-selection.
concepts/runtimes/runtime-inheritance.md	The ome.io/inherit-from annotation lets a runtime inherit a parent runtime's spec, so shared settings live in one reusable profile.
concepts/runtimes/runtime-revisions.md	Runtime pinning ties an InferenceService to an immutable snapshot of its runtime, so runtime edits roll out only when you roll forward or back.
concepts/runtimes/accelerator-classes.md	An AcceleratorClass catalogs one type of GPU: how nodes are matched to it, how InferenceServices select it, and how to read its status.
concepts/serving/inference-services.md	An InferenceService ties a model to a serving runtime and declares the engine, decoder and router components that OME turns into workloads.
concepts/serving/gang-scheduling.md	OME gang-schedules each multi-pod OMENative Instance with a scheduler-plugins PodGroup, so its leader and workers are placed together or not at all.
concepts/serving/autoscaler-policy.md	An AutoscalerPolicy is an alpha, reusable KEDA or HPA template that InferenceService components attach to by reference instead of inlining.
concepts/serving/benchmarks.md	A BenchmarkJob runs genai-bench against an InferenceService or URL with the traffic scenarios you choose, and stores the results.
concepts/rollouts-and-traffic/rollout-policy.md	A RolloutPolicy is an alpha, reusable canary, blue-green or rolling-update progression that InferenceService rollout groups attach by reference.
concepts/rollouts-and-traffic/rollout-groups.md	Rollout groups declare which OMENative components of an InferenceService roll out together, one after another, or concurrently.
concepts/rollouts-and-traffic/traffic-policy.md	The spec.traffic block sets an InferenceService's load-balancing algorithm and session affinity; OME applies it at the gateway and reports it in status.
concepts/rollouts-and-traffic/traffic-map.md	A TrafficMap is the alpha, controller-written routing table of a multi-cluster InferenceService: per-cluster endpoints, weights and conditions.
concepts/rollouts-and-traffic/ingress.md	OME exposes each InferenceService outside the cluster through a Kubernetes Ingress or Gateway API HTTPRoutes, at a hostname built from your config.
reference/index.md	Look up the OME API, kubectl-ome commands, runtime matching and rollout rules, labels, annotations and storage formats when you need exact details.
reference/api/ome.v1beta1.md	Look up the fields of every type in the ome.io/v1beta1 API, such as InferenceService, BaseModel and ClusterServingRuntime, in this generated reference.
reference/api/labels-and-annotations.md	OME reads and sets these labels and annotations on InferenceServices, models, runtimes, nodes and the resources it generates.
reference/api/traffic-annotations.md	These ome.io annotations tune circuit breaking, retries and timeouts for an InferenceService's backends, with pass-through prefixes for Envoy Gateway and Istio.
reference/kubectl-ome/overview.md	Install the kubectl-ome plugin with krew, and see its command families, exit codes and the RBAC that read and action commands need.
reference/kubectl-ome/accelerator.md	Compare the accelerator selection declared on an InferenceService with the AcceleratorClass and resource requests reported for each component.
reference/kubectl-ome/admin.md	Check that a cluster's OME installation is discoverable and consistent with admin doctor, and read Alfred's advisory recommendations.
reference/kubectl-ome/autoscale.md	Show the autoscaling state the controller reports for each InferenceService component, and explain which layer supplies its autoscaler.
reference/kubectl-ome/cluster.md	Show the alpha WorkloadCluster status that the controller reports in the current context, for one cluster or a bounded list.
reference/kubectl-ome/get.md	List OME resources with model-centric columns, including merged views of BaseModels with ClusterBaseModels and ServingRuntimes with ClusterServingRuntimes.
reference/kubectl-ome/instance.md	List the logical OMENative instances of an InferenceService, inspect one instance and its retry blocks, and release a held revision (alpha).
reference/kubectl-ome/logs.md	Stream logs from the pods behind an InferenceService, narrowed to one component, OMENative instance or revision.
reference/kubectl-ome/migration.md	Show live and historical OMENative migration evidence for an InferenceService, and request one guarded migration with the alpha start action.
reference/kubectl-ome/placement.md	Inspect the alpha multi-cluster placement of an InferenceService: reported status, selector and registry evidence, and routing origins.
reference/kubectl-ome/quota.md	Show the declared AcceleratorQuota tree, reported budgets and materialization, and validate the quota topology with a scriptable exit code.
reference/kubectl-ome/rollout.md	Show rollout progress, the pinned plan and bounded history for an InferenceService, validate its configuration, and run the alpha rollout actions.
reference/kubectl-ome/runtime.md	Explain which runtimes match a model, and show the effective runtime, revision history and inheritance tree behind an InferenceService.
reference/kubectl-ome/scale.md	Request a transient replica count for one OMENative component through the InferenceReplica scale subresource, as an alpha guarded action.
reference/kubectl-ome/status.md	Show the full readiness story of an InferenceService in one report: conditions, pods, model, runtime, rollout, autoscaling, traffic and warning events.
reference/kubectl-ome/traffic.md	Show the traffic routes, weights and canary split the controller reports for an InferenceService, check declared behavior, and drain a cluster (alpha).
reference/kubectl-ome/version.md	Print the kubectl-ome version and the OME operator version read from the manager image of the ome-controller-manager Deployment.
reference/kubectl-ome/wait.md	Block until an InferenceService reaches a condition, rollout outcome, migration result, replica state or runtime sync, and exit with a scriptable code.
reference/kubectl-ome/guarded-actions.md	Every mutating kubectl-ome command follows one safety contract: dry-run modes, confirmation, a guarded patch, ActionResult output and exit codes.
reference/matching/model-version-matching.md	Runtime selection compares the version and operator in each supportedModelFormats entry with the model's modelFormat and modelFramework versions.
reference/matching/runtime-accelerator-class-matching.md	Auto-selection rejects a runtime that does not list every AcceleratorClass your InferenceService names; an explicitly named runtime only gets a warning.
reference/matching/runtime-deployment-mode-matching.md	Auto-selection rejects a runtime only when it and your InferenceService both declare a deployment mode for a component and the two differ.
reference/matching/diffusion-pipeline-runtime-matching.md	The diffusionPipeline field on a runtime's supportedModelFormats limits auto-selection to the diffusers pipelines and components it can serve.
reference/rollouts/canary-progression.md	Canary steps set new-revision capacity and traffic independently, advance through immediate, timed, manual or analysis gates, and are bounded by readyTimeout.
reference/rollouts/canary-analysis.md	Metric-gated canary steps sample Prometheus each interval, roll back after failureLimit failing samples, and hold or roll back when metrics can't be read.
reference/storage/benchmark-output-storage.md	A BenchmarkJob's outputLocation accepts these storageUri formats and parameters for OCI, S3, Azure Blob, Google Cloud Storage, GitHub Releases and PVCs.
contributing/index.md	Start here to contribute to OME: set up a development environment, open pull requests and enhancement proposals, and write docs.
contributing/development-setup.md	Install the tools OME needs, build and test it locally, and deploy your own build to a Kubernetes cluster for development.
contributing/pull-requests-and-oeps.md	Open pull requests with the right title prefix, signed-off commits and tests, and propose major features or API changes as OME Enhancement Proposals.
contributing/writing-docs.md	Follow the OME docs style guide for titles, voice and guide structure, and use the site's front matter, callouts and code block syntax.
```

## Appendix C: Draft generator

### `/tmp/plan-parts/make-drafts.py`

```python
#!/usr/bin/env python3
"""Writes a draft page for every row of pages.tsv that has no file yet.

Usage: make-drafts.py PAGES_TSV DESCRIPTIONS_TSV CONTENT_DIR

pages.tsv rows are: content path, title (empty for section landings),
"preview" or empty, and the replaced Hugo pages ("-" for new pages).
descriptions.tsv rows are: content path, description. Every page needs
exactly one description.
"""

import json
import pathlib
import re
import sys

LANDING_TITLES = {
    "getting-started": "Getting Started",
    "guides": "Guides",
    "concepts": "Concepts",
    "reference": "Reference",
    "contributing": "Contributing",
}

# Values that YAML reads back unchanged without quotes.
PLAIN = re.compile(r"[A-Za-z0-9][^:#'\"\\]*")


def scalar(value: str) -> str:
    """Returns value as a YAML scalar, quoting it only when needed."""
    if PLAIN.fullmatch(value) and not value.endswith(" "):
        return value
    return json.dumps(value, ensure_ascii=False)


def read_tsv(path: str, columns: int) -> list[list[str]]:
    rows = []
    text = pathlib.Path(path).read_text(encoding="utf-8")
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        fields = line.split("\t")
        if len(fields) != columns:
            sys.exit(f"{path}:{number}: want {columns} tab-separated fields, got {len(fields)}")
        rows.append(fields)
    return rows


def main() -> None:
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    pages_tsv, descriptions_tsv, content_dir = sys.argv[1:]

    pages = read_tsv(pages_tsv, 4)
    descriptions: dict[str, str] = {}
    for path, description in read_tsv(descriptions_tsv, 2):
        if path in descriptions:
            sys.exit(f"{descriptions_tsv}: {path} is listed twice")
        descriptions[path] = description.strip()

    paths = [row[0] for row in pages]
    missing = sorted(set(paths) - set(descriptions))
    extra = sorted(set(descriptions) - set(paths))
    if missing or extra:
        sys.exit(f"{descriptions_tsv}: missing {missing}, extra {extra}")

    written = 0
    for path, title, _preview, _replaces in pages:
        target = pathlib.Path(content_dir, path)
        if target.exists():
            continue
        title = title or LANDING_TITLES[path.split("/")[0]]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            "---\n"
            f"title: {scalar(title)}\n"
            f"description: {scalar(descriptions[path])}\n"
            "status: draft\n"
            "---\n",
            encoding="utf-8",
        )
        written += 1
    print(f"wrote {written} drafts; {len(pages) - written} pages already existed")


if __name__ == "__main__":
    main()
```
