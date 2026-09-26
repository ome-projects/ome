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
  GitHub does not trigger normal push/PR workflows for this token's events,
  so the nightly performs its own scope, review, and Hugo checks before opening
  the PR. Maintainers can trigger any further desired CI manually.
- Schedules become active only after the workflow is on the default branch.
  Maintainers can manually dispatch from a trusted workflow branch to validate
  fixes before opening a PR. These runs perform the full review/build/publication
  pipeline and open documentation PRs targeting the default branch. Use
  `gh workflow run nightly-docs.yml --repo ome-projects/ome --ref BRANCH`.
  The workflow does not change runner or repository settings.

## Local validation

```bash
python3 -m unittest discover -s hack/nightly-docs -p '*_test.py'
actionlint .github/workflows/nightly-docs.yml
```

Register `ome-runner-cpu` as a self-hosted label in your local actionlint config.
Tests use temporary Git repositories and mocks; they never call a model or
publish a branch/PR. Prompt files are separate so policy can be reviewed without
reading workflow syntax. The Python helper uses only the standard library.
