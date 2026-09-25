# Nightly documentation updates

`.github/workflows/nightly-docs.yml` runs daily at **09:23 UTC** and supports
manual dispatch. All jobs use **ome-runner-cpu** and **claude-fable-5**.
It updates the documentation source in this repository's `site/content/en/docs/`;
the existing release-driven Pages workflow publishes the website separately.

## Scope and lifecycle

1. Inspect the full first-parent history of code/configuration changes on the
   default branch and compare candidate changes with current code and docs.
   There is no date cutoff or persisted success cursor: older gaps and failed or
   deferred work remain eligible. Discovery is model-guided, not an exhaustive
   guarantee that every gap will be found in a single run.
2. Select **at most four independent concerns per run**. Each concern answers
   one concrete user question or corrects one stale claim and cites a source
   commit. Sharing a subsystem or source commit never justifies bundling fixes.
3. Give each concern its own fresh checkout and allowlist of **at most three
   handwritten Markdown files and 300 added plus deleted lines**. Generated API
   reference files, code, configuration, file deletion, and symlinks are blocked.
4. Reject overlapping files within a plan; defer files touched by any open PR.
   Examine human PRs too. Stable source/area/concern markers and branch names
   deduplicate retries and remember closed-unmerged proposals as declined.
   Semantic duplicate detection across different source commits/slugs also
   relies on the planner reading existing PRs; it is not a text-matching proof.
5. Independently review each diff for accuracy and a single concern, build the
   production Hugo site, recheck live PRs, sign off one commit, and open one PR.
   Nothing is merged automatically. Empty or failed edits publish no PR.
   `fail-fast: false` lets other concerns finish when one fails.

Every PR starts from the planner's default-branch snapshot, not from another
documentation branch. Run-level concurrency prevents overlapping nightlies.
Existing PR branches are never force-pushed or overwritten. If a previous run
pushed a branch but failed to open its PR, an exact retry can reuse that tree;
otherwise the job fails for maintainer inspection instead of overwriting it.
To reconsider a deliberately declined concern, a maintainer must explicitly
reopen/rework it; the nightly does not silently recreate it.

## Setup

- Runner pods must expose `ANTHROPIC_API_KEY` with access to `claude-fable-5`,
  and support the same Linux/Go/Node/Hugo build used by the Pages workflow.
- Enable **Allow GitHub Actions to create and approve pull requests** in the
  repository's Actions settings (organization policy must allow it).
- Publication uses the scoped `GITHUB_TOKEN` (`contents: write` and
  `pull-requests: write`); no additional PAT or GitHub App is required.
  GitHub does not trigger normal push/PR workflows for this token's events,
  so the nightly performs its own scope, review, and Hugo checks before opening
  the PR. Maintainers can trigger any further desired CI manually.
- Schedules become active only after the workflow is on the default branch.
  Manual runs also inspect and check out the default branch, not an arbitrary
  dispatch branch. The workflow does not change runner or repository settings.

## Local validation

```bash
python3 -m unittest discover -s hack/nightly-docs -p '*_test.py'
actionlint .github/workflows/nightly-docs.yml
```

Register `ome-runner-cpu` as a self-hosted label in your local actionlint config.
Tests use temporary Git repositories and mocks; they never call a model or
publish a branch/PR. Prompt files are separate so policy can be reviewed without
reading workflow syntax. The Python helper uses only the standard library.
