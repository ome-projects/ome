---
title: Writing docs
description: "Follow the OME docs style guide for titles, voice and guide structure, and use the site's front matter, callouts and code block syntax."
---

Every page in these docs is a Markdown file in the OME repository. This page covers where the files live, how to add a page, the house style and the syntax the site accepts. The build checks the syntax, links and navigation, so most mistakes fail `pnpm test` with the file and the problem.

## Where the docs live

Pages live in `website/src/lib/content/`, one directory per section:

| Directory | What it holds |
|---|---|
| `getting-started/` | Installing OME and serving a first model |
| `guides/` | Step-by-step guides, one task each |
| `concepts/` | How OME works |
| `reference/` | Exact details: the API, `kubectl ome` commands, matching rules and formats |
| `contributing/` | Working on OME and its docs |

Each section's `index.md` is its landing page. The pages in a nav group live in a directory named after the group, such as `guides/deploy-models/` for Deploy models. A page's URL is its path under `/ome/`, without `.md`:

| File | URL |
|---|---|
| `guides/index.md` | `/ome/guides` |
| `guides/deploy-models/serve-models-from-pvc.md` | `/ome/guides/deploy-models/serve-models-from-pvc` |

To preview the site, install Node 22 or newer and pnpm 10, then start the dev server from `website/`:

```bash
cd website
pnpm install
pnpm dev
```

Then open `http://localhost:5173/ome`. The browser reloads when you save a page.

## Add or move a page

To add a page:

1. Create the file in the section's directory, or in the group's directory, and start it with [front matter](#front-matter).
2. Add its path to the group's `pages` in `website/src/lib/config/nav.ts`, such as `'deploy-models/serve-models-from-pvc.md'` under Deploy models. The order of `pages` sets the order of the sidebar and of the previous and next links. `pnpm test` fails when a page isn't listed, or is listed twice. Don't list an `index.md`: a section's landing page needs no entry, and a group's directory can't have one.
3. Add its card to the section's landing page, in the same position as in `nav.ts`. See [Card grids](#card-grids).
4. If the page replaces a page of the old Hugo site, add or update its entry in `website/redirects.json`.

To move a page, change its path in `nav.ts`, its card, the `new` path in its `redirects.json` entry and every link to it. `pnpm test` lists the links that no longer resolve.

`website/redirects.json` maps each Hugo page to the page that replaces it, with an entry like this one:

```json
{
	"old": "tasks/run-workloads/serve-models-from-pvc.md",
	"new": "guides/deploy-models/serve-models-from-pvc.md",
	"rewrittenFrom": "b3f561eb"
}
```

- `old` is the Hugo page's path under `site/content/en/docs/`.
- `new` is the new page's path under `website/src/lib/content/`. When a Hugo page is split across several pages, `new` names the main one. At launch, each entry becomes a redirect.
- `rewrittenFrom` is the 8-character hash of the last commit that changed the Hugo page when you checked the new page against it. It's `null` while the new page is a draft. `pnpm test` fails when a written page's entry has `null` or a draft's entry has a commit.

Indent the file with tabs: `pnpm lint` fails on spaces, and `pnpm format` fixes them.

To get the commit, run this from the repository root, with the Hugo page's path:

```bash
git log -1 --abbrev=8 --format=%h HEAD -- site/content/en/docs/tasks/run-workloads/serve-models-from-pvc.md
```

It prints the commit's 8-character hash:

```output
b3f561eb
```

The Hugo site stays live until launch, and the nightly docs job keeps changing it. `make docs-drift` reports each Hugo page that changed after its `rewrittenFrom` commit, with the commits that changed it. It skips pages that set `generated: true`, such as [the API reference](#the-api-reference), because the tool that writes them keeps them current. It also reports Hugo pages with no entry, and entries whose pages don't exist. Fold the changes into the new page, then move `rewrittenFrom` forward. See [Checks](#checks).

## Front matter

Every page starts with front matter: YAML between two `---` lines.

```markdown title="traffic-map.md"
---
title: Traffic map
description: "A TrafficMap is the alpha, controller-written routing table of a multi-cluster InferenceService: per-cluster endpoints, weights and conditions."
status: preview
since: v1.3
---
```

| Key | Required | What it does |
|---|---|---|
| `title` | Yes | The page's heading and browser title. The sidebar and the previous and next links use it too, unless `navLabel` is set. |
| `description` | Yes | One sentence that says what the reader does or learns. It's the lead under the title, the page's meta description and its text in search results, and the page's card on the landing page repeats it. |
| `navLabel` | No | A shorter label for the sidebar and the previous and next links, such as "Serve from a PVC". |
| `status` | No | `draft` or `preview`. A draft is front matter only, and the page shows a banner that links to the Hugo pages it replaces. Remove `status: draft` when you write the page. For `preview`, see [Preview pages](#preview-pages). |
| `since` | No | The release that adds what the whole page covers, such as `v1.3`. It puts a since badge next to the title. |
| `generated` | No | `true` on pages a tool writes. They show View source and no Edit button. See [The API reference](#the-api-reference). |

An unknown key, a missing `title` or `description`, or a value the table doesn't allow fails the build. Put a value in double quotes when it contains a colon followed by a space, like the description above. Without the quotes, the YAML doesn't parse.

## Style

Three pages set the model for the rest: [Serve models from a PVC](../guides/deploy-models/serve-models-from-pvc.md) for guides, [Base models](../concepts/models/base-models.md) for concepts and [`kubectl ome rollout`](../reference/kubectl-ome/rollout.md) for command references. When this page doesn't answer a question, do what they do.

### Titles

Guide titles start with a verb, such as "Serve models from a PVC". Concept titles are nouns, such as "Base models". Reference pages are named after what they describe, such as `kubectl ome rollout`.

Titles and headings use sentence case: capitalize the first word and proper nouns only. The one exception is the name of the Getting Started section. Don't copy its capitals into other titles.

### Voice

Write in the second person, present tense and active voice: "OME mounts the claim read-only", not "The claim will be mounted read-only".

- Keep paragraphs short. When a paragraph lists several things, make it a list.
- Lead with what the reader does or gets, then explain.
- Leave out filler such as "simply", "easily", "seamlessly" and "powerful".
- Link a term to its concept page the first time a page uses it, such as [BaseModel](../concepts/models/base-models.md).
- Put notes and warnings in [callouts](#callouts), not in `> **Note:**` blockquotes.

### Page structure

Leave out any section that has nothing to say, such as Clean up when a guide creates nothing.

A guide opens with a paragraph that says what the reader does and gets. Then come these sections, in this order:

1. A Before you begin box that lists what the reader needs. See [Prerequisites](#prerequisites).
2. Optionally, a `##` section of background that the steps rely on, such as "The `pvc://` storage URI" in [Serve models from a PVC](../guides/deploy-models/serve-models-from-pvc.md).
3. Numbered steps, each a `##` heading such as "Step 1: Verify the PVC". Each step ends with a check: a command, and the output that shows the step worked.
4. Optionally, a `##` section on settings the steps don't need, such as "Configure the metadata Job".
5. Troubleshooting, with a `###` heading for each symptom: what the reader sees, and how to fix it.
6. Clean up: how to delete what the guide created.
7. Next steps: where to go from here.

A concept page explains how something works. Its opening paragraph says what the thing is for, then comes a `##` section per topic, then Next steps.

A reference page holds exact details. After its opening paragraph, put the facts in tables where they fit, and end with Related pages. Pages for `kubectl ome` commands follow the [command reference format](#command-reference-pages).

### Command reference pages

Every page under `reference/kubectl-ome/`, except the [overview](../reference/kubectl-ome/overview.md) and [Guarded actions](../reference/kubectl-ome/guarded-actions.md), follows the format of [`kubectl ome rollout`](../reference/kubectl-ome/rollout.md):

- **Front matter.** Set `since: v1.3`, since the CLI isn't in v1.2.2. The page's headings carry no since badges.
- **Opening.** A paragraph on what the command is for, the synopsis in a `text` block, and for a command with subcommands, a table of them.
- **Subcommands.** Each subcommand gets a `##` heading named in code, such as `` ## `status` ``, that starts with its synopsis and what it does. Two subcommands that share their flags and behavior can share a heading, such as `` ## `pause` and `resume` ``.
- **Sections.** Under a subcommand, the `###` headings get explicit ids, since every subcommand has the same ones:
    - A read subcommand, which prints a report, gets Flags, Output fields and Examples: `### Flags {#status-flags}`, `### Output fields {#status-output-fields}` and `### Examples {#status-examples}`.
    - An action subcommand, which changes a resource, gets Flags, Refusals and Examples. Explain the flow that the actions share once, in a How the actions run section under the first action.
- **Alpha.** An alpha subcommand gets an Alpha note under its heading. See [Alpha features](#alpha-features).
- **No subcommands.** A command without subcommands uses the `##` headings Flags, Output fields when it prints a report, and Examples.
- **Exit codes.** A `## Exit codes` table with the columns Code, Meaning and Returned by, from `pkg/cli/exitcode/exitcode.go` and the places where the command returns each code.
- **Related guides.** The page ends with `## Related guides`: only the guides that use the command, then [Troubleshoot an InferenceService](../guides/troubleshoot/troubleshoot-an-inferenceservice.md), linked without an anchor. Link other reference pages, such as Guarded actions, from the body.

A Flags table has the columns Flag, Default and Description, copied as printed from the `--help` of a binary you just built (see [Accuracy](#accuracy)). Follow it with this sentence, word for word:

```markdown
Plus the standard kubeconfig flags, such as `-n` and `--context`.
```

For output fields and examples:

- Describe table output from the report's `Table()` method in `pkg/cli/report/`: its columns, any `hint` row, the width it cuts long values to, and what `-o wide` adds, since it isn't always a superset of the default.
- Give the full syntax of a cell that combines values, and list every value a field can take, from the code that defines them.
- List a state only when the command can print it: trace it from the resource to the output, through every code path that sets it.
- Say whether a value is recorded on the resource or worked out by the command, and mark optional API fields as optional.
- A statement about output formats that covers the whole page must hold for every subcommand, actions included.
- Don't copy another command's output or messages. Each action has its own table and messages.

For actions:

- Build the Refusals table from every error the command returns before it changes anything, in the order it checks them. List every condition that returns each error, and the subcommands it applies to.
- Say which failures leave the outcome unknown and which are plain API errors. After a failed change, tell readers to check the resource's status before they run the command again.
- When an action sets an annotation, say who removes it and when, what it blocks in the meantime, and the `kubectl annotate` command that clears it.

Reference pages describe commands. They don't walk through troubleshooting sessions: those belong in the Troubleshoot an InferenceService guide.

### Examples

Examples are complete and runnable, with concrete names: `-n llama-demo`, not `-n <namespace>`. Use the same names throughout a page.

Put a command and its output in separate blocks: the command in a `bash` block, and what it prints in an `output` block. Every command gets an output block, so readers can check that they got the same. When a command prints nothing, say so. On contributing pages like this one, you can leave out long output that changes with the code, such as a test run's log, a command's help or a file's contents.

When a command's default output is too wide to read, select columns, so the output block shows exactly what the reader sees:

```bash
kubectl get pvc model-storage -n llama-demo -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,CAPACITY:.status.capacity.storage
```

```output
NAME            STATUS   CAPACITY
model-storage   Bound    100Gi
```

## Accuracy

The code is the source of truth. Check every field, default, flag, state, condition, reason, event, message and output you write against the code at the commit your pull request is based on. The Hugo pages show what a topic needs to cover, but they can be out of date, and so can doc comments: check behavior in the code that implements it.

- Before you say what OME does, find the default, such as a `+kubebuilder:default` marker, and the code that acts on it. Write "by default" for behavior that can be turned off, and "when" for behavior that depends on a condition. When the code makes something certain, write "always".
- Quote an output line, such as an event or a status message, only after you trace the code that prints it, at the version you name.
- Take `kubectl ome` output from the golden files in `pkg/cli/**/testdata`, such as `pkg/cli/cmd/rollout/testdata/history_healthy.yaml`, or from the expected output in the command's tests. When neither exists, work the output out from the code that prints it, and say so in your pull request.
- Never copy the "Captured Moirai examples" in `cmd/kubectl-ome/README.md`. They come from an internal cluster, not from the code.
- Take flags and defaults from the `--help` of a binary you just built. Its usage lines say `ome rollout`, but pages write `kubectl ome rollout`, the way readers run it.
- Label and annotation keys are built from constants, such as `OMEAPIGroupName + "/deploymentMode"` in `pkg/constants/constants.go`, so search the code for the part after the slash.
- Don't write counts that change with the code, such as how many runtimes OME ships. Show a command that lists them instead.
- YAML examples also go through `make docs-examples`. See [Checks](#checks) for what it can't catch.
- Mark anything that isn't in the latest release with `since`. See [Versioning and status](#versioning-and-status).

To read a command's help, build the plugin from the repository root:

```bash
make kubectl-ome
bin/kubectl-ome rollout history --help
```

To see a file as released, or what changed since a release, use the release's tag:

```bash
git show v1.2.2:pkg/apis/ome/v1beta1/model.go
git diff v1.2.2 HEAD -- pkg/apis/ome/v1beta1/
```

## Syntax

Pages are GitHub Flavored Markdown, plus the blocks in this section. A mistake in any of them fails the build with the file and the problem. Callouts, details and tabs start at the beginning of a line, so they can't go in list items, and their content is indented by four spaces. They can nest: a callout inside a tab is indented four more spaces.

### Callouts

A callout sets text apart from the steps around it. It starts with `!!!`, the type and an optional title in double quotes, and its content is indented by four spaces. There are four types:

```markdown
!!! note
    Use a note for background the reader can skip, such as why a default is what it is.

!!! tip
    Use a tip for a better way to do something, such as a flag that saves a step.

!!! warning "Restart required"
    Use a warning when a step can fail or surprise the reader, such as a change that only takes effect after the pods restart. A title that names the problem, like this one, helps readers who skim.

!!! danger
    Use danger only when a step can lose data or interrupt serving, such as deleting the only copy of a model's weights. Say what's lost and how to avoid it.
```

They render as:

!!! note
    Use a note for background the reader can skip, such as why a default is what it is.

!!! tip
    Use a tip for a better way to do something, such as a flag that saves a step.

!!! warning "Restart required"
    Use a warning when a step can fail or surprise the reader, such as a change that only takes effect after the pods restart. A title that names the problem, like this one, helps readers who skim.

!!! danger
    Use danger only when a step can lose data or interrupt serving, such as deleting the only copy of a model's weights. Say what's lost and how to avoid it.

A callout without a title is titled with its type: Note, Tip, Warning or Danger. For no title at all, write `!!! note ""`. A title can use inline Markdown, such as code, but can't contain a double quote. Any type but these four fails the build.

### Details

Details are callouts that start closed, for content most readers can skip. They start with `???` in place of `!!!`, or `???+` to start open, and take the same four types:

```markdown
??? note "When to use details"
    Use details for long output, background or another way to do a step. They stay closed until the reader opens them.
```

It renders as:

??? note "When to use details"
    Use details for long output, background or another way to do a step. They stay closed until the reader opens them.

Titles work as they do for callouts: without one, details are titled with their type, and `""` leaves the summary empty. Give details a title that says what's inside, so readers can tell whether to open them.

### Tabs

Tabs show alternatives, such as the same step for two kinds of resource. Each tab starts with `===` and its title in double quotes, and its content is indented by four spaces. Tabs in a row form one set:

````markdown
=== "BaseModel"

    ```bash
    kubectl get basemodel llama-3-2-1b-instruct -n llama-demo
    ```

=== "ClusterBaseModel"

    ```bash
    kubectl get clusterbasemodel llama-3-2-1b-instruct
    ```
````

It renders as:

=== "BaseModel"

    ```bash
    kubectl get basemodel llama-3-2-1b-instruct -n llama-demo
    ```

=== "ClusterBaseModel"

    ```bash
    kubectl get clusterbasemodel llama-3-2-1b-instruct
    ```

A tab's title is plain text. Blank lines don't end a set, so put a paragraph between two sets of tabs.

### Prerequisites

A guide's Before you begin box lists what the reader needs first. Wrap a list in these two lines, exactly:

```markdown
<div class="prerequisites" markdown>

- OME installed, and `kubectl` access to the cluster.
- A Bound PersistentVolumeClaim that holds the model's weights.

</div>
```

The box adds its own Before you begin heading, which the table of contents lists, so don't write one. A page has one box at most. [Serve models from a PVC](../guides/deploy-models/serve-models-from-pvc.md) shows it.

### Card grids

Each section's landing page has a card for every page in the section's nav, in nav order. Cards for a labeled group go under a `##` heading with the group's label, and cards for unlabeled groups come before the first heading. A card is the page's title, linked, then its description, both exactly as in the page's front matter:

```markdown
## Deploy models

<div class="grid cards" markdown>

-   **[Serve models from a PVC](deploy-models/serve-models-from-pvc.md)**

    Serve model weights that already live on a PersistentVolumeClaim by pointing a BaseModel at a pvc:// URI, with no download to nodes.

</div>
```

Start each card with `-` and three spaces, and indent its description by four. The title is the card's only link. No check compares cards with `nav.ts` or the front matter, so when you add, move or rename a page, or change its description, change its card too. The [Guides](../guides/index.md) landing page shows cards.

### Code blocks

A code block names its language: `bash`, `go`, `json`, `yaml`, `python`, `dockerfile`, `makefile`, `diff`, `ini`, `toml`, `markdown`, `text` or `output`. The build also takes `sh` and `shell` for `bash`, `yml` for `yaml`, `md` for `markdown`, and `plaintext` and `txt` for `text`. Any other language, or none, fails it.

- `title="model.yaml"` after the language shows a file name in the block's bar, in place of the language. The value must be in double quotes. An `output` block takes a title too.
- `output` is for what a command prints. It renders in a lighter block, with no Copy button.
- `check=skip`, on a `yaml` block only, keeps the block out of the [YAML check](#checks).

````markdown
```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: pvc://model-storage/llama-3-2-1b-instruct
```

```bash
kubectl apply -f model.yaml
```

```output
basemodel.ome.io/llama-3-2-1b-instruct created
```
````

It renders as:

```yaml title="model.yaml"
apiVersion: ome.io/v1beta1
kind: BaseModel
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  storage:
    storageUri: pvc://model-storage/llama-3-2-1b-instruct
```

```bash
kubectl apply -f model.yaml
```

```output
basemodel.ome.io/llama-3-2-1b-instruct created
```

To show a code block inside a code block, as this page does, fence the outer one with four backticks.

Use `check=skip` only for an example that's meant to be rejected, or for a config file with `apiVersion` and `kind` that you don't apply to a cluster, such as a kubeconfig or a Kustomization. The check would fail the config file for having no `metadata.name`. For a rejected example, say in the text that it's rejected, and quote the message, so no one copies it:

````markdown
A ClusterBaseModel's `pvc://` URI must name the claim's namespace, so OME rejects this one with `ClusterBaseModel PVC URI must specify a namespace (format: pvc://{namespace}:{pvc-name}/{sub-path}), got "pvc://model-storage/llama-3-2-1b-instruct"`:

```yaml check=skip
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-2-1b-instruct
spec:
  storage:
    storageUri: pvc://model-storage/llama-3-2-1b-instruct
```
````

Never use `check=skip` to hide an example that fails the check. Fix the example.

### Headings

Sections start at `##`. The title comes from the front matter, so a `#` heading fails the build.

The table of contents lists the `##` and `###` headings. On a page with more than 40 of them, it lists only the `##` headings, and a page with fewer than 2 has no table of contents.

A heading's id is its text in lowercase, with punctuation dropped and spaces turned into hyphens: "Step 1: Verify the PVC" becomes `step-1-verify-the-pvc`. A repeated heading gets `-1`, `-2` and so on. To set the id, or to mark a heading as new, end it with attributes in braces:

```markdown
### Flags {#status-flags}

### Distribution {since=v1.3}

### A new section {#new-section since=v1.3}
```

- The first heading's id is `status-flags`. Set an id when the same heading text appears more than once on a page, as each subcommand's Flags heading does on [`kubectl ome rollout`](../reference/kubectl-ome/rollout.md). An id starts with a letter or digit, followed by letters, digits, hyphens, underscores or dots.
- The second heading gets a since badge, and keeps the id of its text: link to it with `#distribution`. See [Versioning and status](#versioning-and-status).
- The third sets an id and gets a badge, with a space between the two.

A duplicate id, or any other attribute, fails the build.

### Links

Link to a page with its relative `.md` path, and to a heading with `#` and its id. Page links also work when you browse the files on GitHub. From a page in `guides/deploy-models/`:

```markdown
[BaseModel](../../concepts/models/base-models.md)
[the BaseModel type](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-BaseModel)
[Step 2](#step-2-create-the-model)
```

!!! tip
    To get a heading's id, hover over the heading and copy its ¶ link.

Link to other files in the repository with a full GitHub URL: `https://github.com/ome-projects/ome/blob/main/` and the file's path, or `tree/main/` for a directory. The build doesn't check these URLs, so link only to files that `git ls-files` lists. Other web links and `mailto:` links pass as written.

The build fails on:

- an absolute path, such as `/ome/guides`;
- a link to the old site at `ome-projects.github.io`;
- a full URL of a page on this site;
- a relative link to anything but a `.md` page;
- a path that leaves `src/lib/content`;
- any scheme but `https:`, `http:` and `mailto:`.

`pnpm test` also fails when a linked page or id doesn't exist, and on an anchor into a draft, which has no headings yet.

Raw HTML, such as an `<a>` or `<img>` tag, skips most of these checks, so write links and images in Markdown.

### Images

Prefer text, since screenshots go stale. Add an image only when text can't do the job, such as a diagram. Put it in `website/static/images/`, point to it with a path under `/images/`, and give it alt text that says what it shows:

```markdown
![A multi-pod Instance: one leader pod and two worker pods](/images/multi-pod-instance.svg)
```

An external image URL, any other path, or a file that doesn't exist fails the build. Alt text is required, but the build can't check it, so reviewers do.

## Versioning and status

The docs track `main`, so they describe features before a release ships them. Mark what isn't in the latest release with `since`:

- A page that's new as a whole gets `since` in its front matter, and no badges on its headings.
- A new section gets `since` on its heading. See [Headings](#headings).
- A table row or a sentence can't carry a badge, so write "Since v1.3." there in plain text.

The badge compares the version with the latest release on GitHub:

| Label | When |
|---|---|
| Unreleased: coming in v1.3 | v1.3 is newer than the latest release. |
| New in v1.3 | v1.3 is the latest release, or older. |
| Since v1.3 | The site can't fetch the latest release. |

The site compares the versions on each request, so the labels change when a release ships, without a rebuild.

### Changed behavior

When `main` changes how something works, also say what the latest release does, so readers on it aren't misled. In a paragraph, use this form: "Since v1.3, …. On v1.2.2, …." [How weights reach the nodes](../concepts/models/base-models.md#how-weights-reach-the-nodes) on the Base models page shows it. When a whole section changed, put `since` on its heading, and add one sentence on what the latest release does.

### Preview pages

`status: preview` is only for pages that are purely about multi-cluster routing, which is alpha:

- [Configure routing health probes](../guides/multi-cluster/routing-health-probes.md)
- [Drain a workload cluster](../guides/multi-cluster/drain-a-workload-cluster.md)
- [Traffic map](../concepts/rollouts-and-traffic/traffic-map.md)
- [`kubectl ome cluster`](../reference/kubectl-ome/cluster.md)
- [`kubectl ome placement`](../reference/kubectl-ome/placement.md)
- [Weight traffic for heterogeneous clusters](../guides/multi-cluster/weight-traffic-for-heterogeneous-clusters.md)
- [Publish a global endpoint](../guides/multi-cluster/publish-a-global-endpoint.md)
- [Tune routing capacity polling](../guides/multi-cluster/tune-routing-capacity-polling.md)

A preview page shows a Preview badge and an In development callout, so don't write your own. Pages that mention InferenceReplica don't get the preview marking, and don't call it alpha: InferenceReplica backs single-cluster [OMENative](../concepts/omenative/overview.md), which isn't alpha. Only the multi-cluster routing built on it is.

In the sidebar, a preview page has a preview pill. When every page in a group is a preview, set `preview: true` on the group in `nav.ts`, as the Multi-cluster group does, and the pill moves to the group's name.

### Alpha features

A command is alpha when its `--help` says "Alpha". On any page but a preview page, put this note under the heading of each alpha subcommand, such as `rollout repin`:

```markdown
!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.
```

When two alpha subcommands share a heading, the note is plural:

```markdown
!!! note "Alpha"
    These commands are alpha. Their flags and behavior can change between releases.
```

A page about an alpha API, or about a feature behind a flag, says so in its opening paragraph, and says how to turn the feature on.

## Checks

Run the site's checks from `website/`:

```bash
pnpm lint
pnpm check
pnpm test
pnpm build
```

- `pnpm lint` runs Prettier and ESLint. Prettier skips the pages in `src/lib/content/`, and `pnpm format` fixes what it reports elsewhere.
- `pnpm check` type-checks the code.
- `pnpm test` runs the unit tests and the content checks. The content checks render every page, then check `nav.ts`, every link and anchor, `redirects.json` and the search index. They report every broken page at once, so run `pnpm test` first when you edit pages.
- `pnpm build` builds the site. It stops at the first page that doesn't render, with the file and the problem.

`make docs-examples`, from the repository root, checks the YAML examples. It's a Go program, so it needs Go. It starts a local Kubernetes API server with OME's CRDs and dry-run creates every object in a `yaml` block:

- It catches unknown fields, wrong types, bad enum values, missing required fields and failed CEL rules.
- It doesn't run OME's admission webhooks, so check what `pkg/webhook/admission/` and `pkg/validation/` enforce by hand.
- It skips blocks marked `check=skip`, and documents without `apiVersion` or `kind`, such as Helm values.
- It skips objects whose CRDs it doesn't have, and lists them.
- It reports an object without `metadata.name` or `metadata.generateName` as a problem.

It prints each problem, then a count of objects and problems.

`make docs-drift`, also from the repository root, reports Hugo pages that changed since they were rewritten (see [Add or move a page](#add-or-move-a-page)). When there's nothing to fold in, it prints:

```bash
make docs-drift
```

```output
No drift: every Hugo page is mapped, and every rewrite is current.
```

The repository's pre-commit hooks trim trailing whitespace, fix the final newline and run codespell, among other checks. `helm-lint` and `helm-template` run every time, even when no chart changed, so they fail when Helm isn't installed: install Helm, or skip them. From the repository root, run the hooks on the files you changed:

```bash
SKIP=helm-lint,helm-template pre-commit run --files website/src/lib/content/guides/deploy-models/serve-models-from-pvc.md
```

It prints a line for each hook, ending in Passed, Failed or Skipped. Work on a branch: the `no-commit-to-branch` hook fails on `main`.

To run the hooks on every commit, install them with `pip install pre-commit && pre-commit install`. Without Helm, set `SKIP` the same way when you commit.

The PR Validation workflow runs the hooks, the Helm ones included, on every pull request into `main` or a `release-*` branch. On a pull request that changes `website/`, the CRDs, the API types or the programs behind these checks, the Website workflow runs the site checks, the YAML check, the API reference check and the drift report. The drift report is information only: it doesn't fail the workflow. To open the pull request, see [Pull requests and OEPs](pull-requests-and-oeps.md).

## The API reference

The [OME API](../reference/api/ome.v1beta1.md) reference is generated from the Go types in `pkg/apis/ome/v1beta1/`, so don't edit the page. Edit the doc comments on the types and fields, then regenerate it from the repository root:

```bash
make generate-apiref
```

It writes two copies and prints the path of each: the website page, in `website/src/lib/content/reference/api/`, and the Hugo copy, at `site/content/en/docs/reference/ome.v1beta1.md`. The Hugo copy stays out of date until launch, so commit only the website page, and undo the change to the Hugo copy:

```bash
git restore site/content/en/docs/reference/ome.v1beta1.md
```

It prints nothing.

The page sets `generated: true`, so it shows View source and no Edit button. Its templates are in `hack/genref/website/markdown/`. When you change the types, also run `make manifests` and `make generate`. The Website workflow regenerates the page and fails if it differs from the one you committed.
