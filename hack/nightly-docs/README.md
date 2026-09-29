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
   autoscaling/quota, and operations. Each scan has a 200-turn hard ceiling
   and an 80-turn investigation target, leaving headroom for tool batches and
   structured output. The ceiling is not a target for more investigation. Commits touching
   several areas can appear in several scans; otherwise-unassigned commits go
   to operations. No eligible commit is dropped by the partition.
   There is no date cutoff or persisted success cursor: older gaps and failed or
   deferred work remain eligible. Discovery is model-guided, not an exhaustive
   guarantee that every gap will be found in a single run.
   Supply an inventory of authored page titles/headings. Planners must read
   existing candidate pages, identify canonical pages needing correction, and
   justify each new page. A new page cannot substitute for fixing an existing
   false claim, including claims left behind by a merged related documentation PR.
2. Combine the available validated results in round-robin order and select
   **at most 100 independent concerns per run**, not 100 per scan. Each concern answers
   one concrete user question or corrects one stale claim and cites a source
   commit. Sharing a subsystem or source commit never justifies bundling fixes.
3. Give each concern its own fresh checkout and allowlist of handwritten
   Markdown files. Each PR must have **fewer than 1,000 added plus deleted lines
   (999 maximum)**, with **no file-count limit**. Generated API
   reference files, code, configuration, file deletion, and symlinks are blocked.
4. Defer duplicate concern keys, area/concern identities, identical normalized
   questions, and overlapping files across scans. Open nightly documentation
   PRs reserve their files; human PR file lists do not. Review their actual
   documentation diffs on the proposed paths instead: unrelated edits or changes
   already integrated into main need not block a canonical-page fix. A genuinely
   duplicate, competing, or ambiguous human change is rejected by review. Age,
   draft status, and PR size alone never decide whether a human change matters.
   Conflicts do not discard other useful proposals from a scan. Stable source/area/concern markers and branch names
   deduplicate retries and remember closed-unmerged proposals as declined.
   Semantic duplicate detection across different source commits/slugs also
   relies on the planners reading existing PRs and honoring their separate
   responsibilities; matching keys/questions is not a semantic equivalence proof.
5. Transfer only a JSON bundle of documentation text to a **separate publisher
   job with a fresh checkout**. Revalidate the bundle with pristine guards before
   writing allowed documentation paths. No writer scripts, Git metadata, hooks,
   configuration, or review verdicts cross this boundary.
6. Independently review each diff with read-only Fable tools for accuracy and a
   single concern, appropriate page placement, consistency of related docs, and
   absence of competing human PR work. Capture human PR head SHAs and doc-only
   diffs using each branch's merge base with pinned main; never execute PR code.
   Build the production Hugo site, recheck live PRs, sign off
   one commit, and open one PR. Git commands disable hooks, including pre-push.
   Nothing is merged automatically. Empty or failed edits publish no PR.
   Any new/changed overlapping human PR observed by the final recheck invalidates
   the review before publication. This check cannot be atomic with other
   contributors opening or updating PRs; normal PR review and merge checks remain
   necessary after publication.
   An explicit accuracy/scope/placement/overlap rejection is recorded in the job summary and skips
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
scan, the selected plan, and reasons for deferring proposals. Concerns deferred
for a shared canonical page or the PR cap are saved as `queued_concerns`. The
next run paginates default-branch runs from the last 14 days to find the newest
eligible retained plan and supplies that queue for fresh evaluation. Only runs
that requested all eight scans with the default 100-concern cap and dry-run
disabled may seed the queue, including incomplete production runs;
branch pilots and limited/dry runs on main cannot replace it. Deferred records
use the full source/area/concern key, so equal slugs in different areas do not
queue selected work by accident.
This is a best-effort queue within artifact retention; full-history discovery
still covers older work when evidence expires. Job summaries show
eligible commits, reported inspected commits and candidate counts by scan.
A failed scan does not discard successful scans: planning and downstream jobs
explicitly tolerate failed discovery dependencies. Each received artifact must
still pass source, baseline, placement, and scope validation; invalid or duplicate
artifacts fail planning. Missing artifacts are recorded as `missing_shards` beside
`expected_shards` and `complete` in the report, never treated as inspected empty
results. A separate coverage job fails when scans are missing, so the run remains
visibly incomplete even while validated concerns proceed through normal review,
build, and publication guards. No scans means no writing/publication jobs, but a
report still records the missing coverage and retained queue.
Incomplete reports give prior unselected pending concerns priority over newly
deferred work, deduplicate them, and remove already recorded PR identities.
The pending queue holds at most 100 concerns. Any excess concerns are explicitly
listed by key in `queue_overflow` and the job summary; they are not carried as
pending evidence, but remain eligible through full-history discovery. Prior
pending work therefore cannot be silently displaced by newly deferred concerns.
The next nightly rechecks all code history, including failed subsystems and the
retained pending concerns. A complete scan can retire old concerns it no longer
proposes. This recovery remains bounded by the 100-item queue and artifact
retention; it is not a guarantee of exhaustive discovery or an immediate retry.
Existing PR branches are never force-pushed or overwritten. If a previous run
pushed a branch but failed to open its PR, an exact retry can reuse that tree;
otherwise the job fails for maintainer inspection instead of overwriting it.
To reconsider a deliberately declined concern, a maintainer must explicitly
reopen/rework it; the nightly does not silently recreate it.

## Branch validation without opening documentation PRs

A targeted manual run can exercise discovery, writing, independent placement and
human-overlap review, and the production site build without pushing branches:

```bash
gh workflow run nightly-docs.yml --repo ome-projects/ome \
  --ref codex/your-fix-branch -f dry_run=true \
  -f discovery_shard=runtime-accelerators -f max_prs=2
```

`discovery_shard` accepts one of the eight scan names above (CLI observation is
`cli-observe`, CLI actions `cli-actions`, model storage `model-storage`, runtimes
`runtime-accelerators`, workloads `workload-rollouts`, networking
`networking-traffic`, autoscaling `autoscaling-quota`, or `operations`). Empty
runs all scans. `max_prs` is 1–100; scheduled runs retain the 100-PR ceiling.
`dry_run` defaults to false. It runs all publication guards but skips branch
creation, commits, pushes and PR creation. It still fetches an existing concern
branch to verify that its tree and parent match the proposed retry. Inspect `nightly-docs-validation-*`
artifacts for the item, full diff, pinned human overlap evidence, and explicit
review verdict. A successful run with a rejected verdict is not a successful
repair: require an accepted verdict, a useful existing-page diff, and a passing
build when evaluating a pilot.

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
with four concurrent workers on `ome-runner-cpu`. Sweeps run every two hours,
at minute 11 of even-numbered UTC hours. Issue comments and completion of the
nightly, PR validation or code review workflow also wake it. Submitted reviews and inline replies are
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
   Only OWNER, MEMBER and COLLABORATOR feedback and authenticated Claude/CodeRabbit
   bot feedback can invalidate the model cache. Outsider comments cannot start
   model rounds; all unresolved threads still block merging, and a human reply
   of any association protects a thread from automatic resolution.
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
4. A second, independent **claude-fable-5** review in a read-only job checks the whole PR, behavior
   claims, feedback and semantic correctness against implementation and tests.
   A separate publisher revalidates the data and requires both accuracy and scope
   approval. Its write token is scoped to API/publication steps, with no model
   running in that job. Known live credentials and recognizable token literals
   are rejected before artifact upload and public reporting. Non-model setup
   does not copy the runner API key into the GitHub environment file.
   Build the production Hugo site, then check internal docs links and anchors
   from the rendered pages. Fenced YAML is data: shell heredocs, CEL rules and
   admission webhooks are not executed. These checks do not prove every example
   can run against a live Kubernetes cluster.
   Before building, a main advance may be carried forward only if it adds
   unrelated regular handwritten Markdown pages. Source, schemas, templates and
   every existing page must remain byte-for-byte identical. The semantic review
   can then be reused while build/link validation uses the refreshed base; both
   revisions are recorded in the evidence. Any other main change requires a
   fresh review, and a subsequent base move still blocks publication.
5. Append a signed-off repair commit to the **same branch**, including current
   main when necessary, using a normal push. The committed tree must exactly
   match the validated tree. Concurrent changes invalidate publication; no
   force-push is used. Rejected accuracy, rejected scope, failed deterministic
   checks or malformed verdicts cannot publish a repair. Their findings remain
   in bounded retry state and artifacts for the next round. The publisher waits
   briefly for its new head to propagate, rejecting any competing head or base.
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
manual dispatch defaults to dry-run mode. `feedback` (at most 2,000 characters)
and `force` require an explicit PR number; they cannot fan out across a sweep.
A PR with malformed feedback is skipped with a diagnostic during a sweep, while
an explicit dispatch fails loudly. Scope/conflict rejections during preparation
are marked `needs-human` rather than repeatedly launching workers.
