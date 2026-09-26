# Nightly documentation updates

`.github/workflows/nightly-docs.yml` runs daily at **09:23 UTC** and supports
manual dispatch. All jobs use **ome-runner-cpu** and **claude-fable-5**.
It updates the documentation source in this repository's `site/content/en/docs/`;
the existing release-driven Pages workflow publishes the website separately.

## Scope and lifecycle

1. Build the unchanged base site first to catch runner/dependency failures before
   calling a model. Partition the full first-parent code/configuration history
   into eight focused scans: CLI observation, CLI actions, model storage,
   runtimes/accelerators, workloads/rollouts, networking/traffic,
   autoscaling/quota, and operations. Each scan has its own 100-turn budget
   (80 turns for discovery, reserving headroom for the result). Commits touching
   several areas can appear in several scans; otherwise-unassigned commits go
   to operations. No eligible commit is dropped by the partition.
   There is no date cutoff or persisted success cursor: older gaps and failed or
   deferred work remain eligible. Discovery is model-guided, not an exhaustive
   guarantee that every gap will be found in a single run.
2. Combine the eight validated results in round-robin order and select
   **at most 100 independent concerns per run**, not 100 per scan. Each concern answers
   one concrete user question or corrects one stale claim and cites a source
   commit. Sharing a subsystem or source commit never justifies bundling fixes.
3. Give each concern its own fresh checkout and allowlist of handwritten
   Markdown files. Each PR must have **fewer than 1,000 added plus deleted lines
   (999 maximum)**, with **no file-count limit**. Generated API
   reference files, code, configuration, file deletion, and symlinks are blocked.
4. Defer duplicate concern keys, area/concern identities, identical normalized
   questions, and overlapping files across scans; defer files touched by any
   open PR. Conflicts do not discard other useful proposals from a scan.
   Examine human PRs too. Stable source/area/concern markers and branch names
   deduplicate retries and remember closed-unmerged proposals as declined.
   Semantic duplicate detection across different source commits/slugs also
   relies on the planners reading existing PRs and honoring their separate
   responsibilities; matching keys/questions is not a semantic equivalence proof.
5. Transfer only a JSON bundle of documentation text to a **separate publisher
   job with a fresh checkout**. Revalidate the bundle with pristine guards before
   writing allowed documentation paths. No writer scripts, Git metadata, hooks,
   configuration, or review verdicts cross this boundary.
6. Independently review each diff with read-only Fable tools for accuracy and a
   single concern, build the production Hugo site, recheck live PRs, sign off
   one commit, and open one PR. Git commands disable hooks, including pre-push.
   Nothing is merged automatically. Empty or failed edits publish no PR.
   An explicit accuracy/scope rejection is recorded in the job summary and skips
   publication; it is an expected filter outcome. Malformed review output, model
   failures, scope violations, and build/publication errors still fail the run.
   `fail-fast: false` lets other concerns finish when one fails. The publisher
   skips concerns whose writer failed to produce an artifact.

Writers and independent reviewers each have a 120-turn ceiling, with a prompt
target of 60 turns for investigation/editing or evidence gathering. This leaves
headroom for tool batches and the final result; exceeding the ceiling still
fails the job rather than bypassing review.

The writer has a read-only GitHub token and cannot publish. The planner and
publisher's reviewer have only Read, Glob, and Grep tools (no shell, editing, or
agent tools); source diffs are prepared by the workflow. The publisher does not
trust any checks run inside the writer's checkout. PR titles must be a nonempty,
single printable line, validated before any branch is pushed.

Every job first checks out the immutable workflow `github.sha` and preserves its
automation tools outside the source checkout. The planner job pins the default
branch's source SHA once, before calling the model; every job then checks out
that same source SHA. A manual run from a fix branch therefore tests that
branch's tooling while all generated PRs contain only documentation changes
against the pinned default branch. The model cannot choose either revision.
Run-level concurrency prevents overlapping nightlies. Discovery, writing, and
publication each allow four concurrent jobs. This increases total model work
and may increase model cost; the 100-PR ceiling is not a daily output target.
Publisher overlap checks batch open-PR file lists with GraphQL and fall back to
fully paginated REST for PRs changing more than 100 files.

Models select numbered entries from a trusted per-scan commit index; the
workflow resolves them to full source SHAs, avoiding hash transcription errors.
Each scan reports inspected source commits and remaining work. These are model
self-reports, not a tool-level audit; inspecting a commit subject does not count.
The `nightly-docs-discovery-report` artifact (14-day retention) contains every
scan, the selected plan, and reasons for deferring proposals. Job summaries show
eligible commits, reported inspected commits and candidate counts by scan.
A missing/invalid scan fails planning rather than silently treating it as an
empty result. This makes incomplete discovery visible and retryable.
Existing PR branches are never force-pushed or overwritten. If a previous run
pushed a branch but failed to open its PR, an exact retry can reuse that tree;
otherwise the job fails for maintainer inspection instead of overwriting it.
To reconsider a deliberately declined concern, a maintainer must explicitly
reopen/rework it; the nightly does not silently recreate it.

## Setup

- Runner pods must expose `ANTHROPIC_API_KEY` with access to `claude-fable-5`,
  and support the Linux/Go/Node build used by the Pages workflow. The nightly
  downloads Hugo Extended 0.157.0 for Linux amd64/arm64 and verifies its pinned
  SHA-256 digest; a C compiler is not required on the runner. Builds use Node 22
  (required by postcss-cli 12) and pin Docsy 0.14.3, compatible with this Hugo,
  only in the isolated build copy. Dependency resolution and generated files
  never enter the documentation PR. The Pages deployment workflow is separate.
  Use ephemeral, single-job runner pods so jobs do not share mutable host state.
- Enable **Allow GitHub Actions to create and approve pull requests** in the
  repository's Actions settings (organization policy must allow it).
- Publication uses the scoped `GITHUB_TOKEN` (`contents: write` and
  `pull-requests: write`); no additional PAT or GitHub App is required.
  Token-generated events do not reliably run follow-up CI without approval.
  The nightly therefore performs its own scope, review and Hugo checks. The
  maintenance worker below validates its actual PR head independently; it does
  not bypass any other pending or required CI. An installation token can be
  added separately if normal event-driven CI without approval is desired.
- Schedules become active only after the workflow is on the default branch.
  Maintainers can manually dispatch from a trusted workflow branch to validate
  fixes before opening a PR. These runs perform the full review/build/publication
  pipeline and open documentation PRs targeting the default branch. Use
  `gh workflow run nightly-docs.yml --repo ome-projects/ome --ref BRANCH`.
  The workflow does not change runner or repository settings.

## Local validation

```bash
python3 -m pip install -r hack/nightly-docs/maintenance-requirements.txt
python3 -m unittest discover -s hack/nightly-docs -p '*_test.py'
actionlint .github/workflows/nightly-docs.yml
```

Register `ome-runner-cpu` as a self-hosted label in your local actionlint config.
Tests use temporary Git repositories and mocks; they never call a model or
publish a branch/PR. Prompt files are separate so policy can be reviewed without
reading workflow syntax. Publication uses the standard library; example validation uses the pinned
PyYAML and jsonschema dependencies. The pre-commit hook installs these in its
own Python environment.


## Maintenance of existing documentation PRs

`docs-pr-maintenance.yml` reconciles up to 100 eligible open PRs on each sweep,
with four concurrent workers on `ome-runner-cpu`. Sweeps run at minutes 11 and
41 every hour; issue comments and completion of the nightly, PR validation or
code review workflow also wake it. Submitted reviews and inline replies are
picked up by the sweep, avoiding privileged execution from a PR merge ref.
Schedules/events use default-branch workflow code. Manual dispatch can use a
trusted implementation branch. No PR-controlled scripts or Git metadata are
executed. Runner pods must be ephemeral and isolated between jobs.

Eligibility requires the original `github-actions[bot]` author, a same-repository
branch matching the original concern marker, an open non-draft PR targeting
main, and only added/modified handwritten documentation. Labels alone do not
confer eligibility. A replacement publisher identity needs an explicit update
to this guard. The source commit must belong to current main's history.

Each activation does one repair round:

1. Pin the PR head, current main and review feedback. Read unresolved threads,
   review/issue comments, failed CI check summaries and previous repair findings.
   Read main's live Git ref because the PR API's `base.sha` can lag updates.
   Ignore maintenance bookkeeping and CodeRabbit's informational skip notices.
   Overlay only the PR's Markdown on trusted main; changes to the same pages on
   main require human conflict resolution.
2. A fresh **claude-fable-5**, `xhigh`, 120-turn worker fixes the original concern
   using read/edit tools and a read-only GitHub token. It cannot push or merge.
3. A separate job imports only documentation text, repeats the full-PR scope,
   whitespace and **999 changed-line maximum** checks, validates YAML examples
   against current OME CRD schemas, and catches incorrect `/docs/` prefixes.
   There is no file-count limit; repairs stay within the original PR's paths.
4. A second, independent **claude-fable-5** review checks the whole PR, behavior
   claims, feedback and semantic correctness against implementation and tests.
   Build the production Hugo site, then check internal docs links and anchors
   from the rendered pages. Fenced YAML is data: shell heredocs, CEL rules and
   admission webhooks are not executed. These checks do not prove every example
   can run against a live Kubernetes cluster.
5. Append a signed-off repair commit to the **same branch**, including current
   main when necessary, using a normal push. The committed tree must exactly
   match the validated tree. Concurrent changes invalidate publication; no
   force-push is used. A technically valid, single-concern partial repair may be
   published with a **failing** quality check so another round can address the
   remaining accuracy findings. Rejected/malformed scope cannot be published.
6. Record `Docs maintenance` on the actual PR head and update one bot status
   comment. Verified bot-only threads may be resolved; human discussions never
   are. New head/base/feedback invalidates a cached successful result. The same
   source/feedback does not repeatedly consume model turns after validation.

Three unsuccessful rounds exhaust the durable per-PR budget, including worker
failures and cancellation after reservation. Successful validation resets the
counter. A `needs-human` status remains visible until a maintainer dispatches
`force=true`; a new automatic commit does not reset failed attempts. One PR's
failure does not cancel other matrix workers. Artifacts retain context, final
patch, validation findings and verdict for 14 days. A successful workflow can
still report a rejected repair; inspect its verdict and the PR-head check.

### Merge policy and controls

- Set `DOCS_MAINTENANCE_ENABLED=false` to stop new maintenance work.
- **Merging is off by default.** Opt in separately with repository variable
  `DOCS_MAINTENANCE_MERGE=true`. No repository/organization setting is changed by
  the workflow. Dry runs never write a branch, check, comment, thread or merge.
- Even when enabled, normal squash merge requires a fresh successful maintenance
  check for the current head/main, all reported checks successful/neutral/skipped,
  no unresolved threads, GitHub's `APPROVED` review decision and `CLEAN` merge
  state. Repository approval/CODEOWNER rules still apply. The worker neither
  approves itself nor invokes an administrative bypass. Approval-required CI
  must be released by a maintainer; the bot will wait.
- Merge operations are serialized and conditional on the PR head SHA. GitHub's
  merge API does not atomically pin main's SHA; strict required checks or a merge
  queue are needed for an atomic base-freshness guarantee under external merges.
  This workflow rechecks main immediately before the merge request.
- The workflow can drive a PR to a validated, mergeable state, but cannot promise
  a merge when approval, CI, conflicts or unresolved human feedback block it.

### Test on a branch before submitting changes

The existing nightly dispatcher has a maintenance-only route, allowing the new
reusable workflows to run before their first merge to main:

```bash
gh workflow run nightly-docs.yml --repo ome-projects/ome --ref BRANCH \
  -f maintenance_pr=1047 -f maintenance_apply=false
```

Inspect `docs-maintenance-evidence-1047/result.json` and the final diff, then test
publication with `maintenance_apply=true`. Keep the merge variable unset during
pilots. `maintenance_feedback` supplies specific additional feedback;
`maintenance_force=true` explicitly resumes an exhausted PR. After installation,
`docs-pr-maintenance.yml` also supports direct dispatch with `pr_number`, `apply`,
`feedback` and `force`. A dispatch without a PR number scans all eligible PRs;
manual dispatch defaults to dry-run mode.
