---
title: Pull requests and OEPs
description: Open pull requests with the right title prefix, signed-off commits and tests, and propose major features or breaking API changes as OME Enhancement Proposals.
---

To get a change into OME, open a pull request against `main` with a prefixed title, signed-off commits and tests. A substantial change, such as a new CRD or a breaking API change, starts as an [OME Enhancement Proposal](#when-you-need-an-oep) (OEP), which you review as a pull request first. To install the tools, and build and test OME, see [Set up a development environment](development-setup.md).

## Open a pull request {#before-you-open-a-pull-request}

1. Create a branch named `<type>/<description>` or `<username>/<description>`, in lowercase, as in `alex-kim/fix-pvc-claim-name`. Keep the pull request to one change.
2. Add tests, and update the docs that describe your change. A bug fix needs a test that fails without the fix. Go code follows the [Google Go Style Guide](https://google.github.io/styleguide/go/).
3. After a change to the API types in `pkg/apis/`, regenerate the code and manifests, and commit the files they change or create:

   ```bash
   make manifests
   make generate
   ```

   They print the steps they run. [Change the API](development-setup.md#change-the-api) says what they regenerate. When you change the types or their doc comments, also run `make generate-apiref`, as [The API reference](writing-docs.md#the-api-reference) describes.

4. Run the tests, the coverage check and the linter from the repository root, and commit the files that `make test` changes:

   ```bash
   make test
   make coverage
   make ci-lint
   ```

   They print what they find, and stop with an error when a check or a test fails. `make test` needs a Rust toolchain: without one, see [Run the tests](development-setup.md#step-3-run-the-tests). For a docs change, run the checks in [Check a docs change](development-setup.md#check-a-docs-change).

5. Commit with a sign-off and a [prefixed title](#title-prefixes), as [Commit messages](#commit-messages) describes. The [pre-commit hooks](development-setup.md#step-1-clone-the-repository) check your commits, and [A pre-commit hook fails](development-setup.md#a-pre-commit-hook-fails) covers the usual failures.
6. Rebase on the latest `main` of `ome-projects/ome`, which is `origin` in your clone. Then fork `ome-projects/ome` on GitHub, add your fork as a remote, and push the branch to it. Put your GitHub username in place of `alex-kim`:

   ```bash
   git fetch origin
   git rebase origin/main
   git remote add fork https://github.com/alex-kim/ome.git
   git push -u fork alex-kim/fix-pvc-claim-name
   ```

   Git replays your commits on the latest `main`, and `git remote add` prints nothing. GitHub's reply to the first push includes a link to open a pull request.

7. Open the pull request against `main`, and fill in the [template](#the-pull-request-template).

## Title prefixes

Start the pull request's title with one of these prefixes and a short summary, such as `[Docs] Fix the claim name in the PVC guide`:

| Prefix | For |
|---|---|
| `[Bugfix]` | Bug fixes. |
| `[Core]` | Core controller changes, including build changes, version upgrades and changes across all controllers. |
| `[API]` | All OME API changes. |
| `[Helm]` | Changes to the Helm charts. |
| `[Docs]` | Documentation changes. |
| `[CI/Tests]` | Unit tests, integration tests and CI. |
| `[Misc]` | Changes that fit none of the other prefixes. Use it sparingly. |
| `[OEP]` | OME Enhancement Proposals. See [Write an OEP](#write-an-oep). |

Use one prefix per title. Most pull requests land on `main` as one commit whose title ends with the pull request's number, such as `[CI/Tests] Retry dev image tool installs (#898)`. The 52-character limit in [Commit messages](#commit-messages) applies to commit titles, and a pull request's title can be longer.

## Commit messages

Keep a commit title to 52 characters or fewer, start it with a [prefix](#title-prefixes), wrap the body at 72 characters, and sign off every commit.

### Sign off every commit

The `Signed-off-by:` trailer is your sign-off of the [Developer Certificate of Origin](https://developercertificate.org/). With it, you certify that you have the right to submit the change under the project's open source license. `git commit -s` adds it, with the name and email address from your Git `user.name` and `user.email`:

```bash
git commit -s -m "[Docs] Fix the claim name in the PVC guide"
```

Git prints the commit's short hash and title. To see the trailer, print the last commit's message:

```bash
git log -1 --format=%B
```

```output
[Docs] Fix the claim name in the PVC guide

Signed-off-by: Alex Kim <alex.kim@example.com>
```

Your name and email address in the sign-off become part of the repository's public history. To keep your address private, set `user.email` to your GitHub noreply address.

To sign off the last commit after you made it:

```bash
git commit --amend --signoff --no-edit
```

Git rewrites the commit with the trailer. To sign off every commit on your branch, rebase it on the latest `main` of `ome-projects/ome` with `--signoff`. Here, `origin` is `ome-projects/ome`:

```bash
git fetch origin
git rebase --signoff origin/main
```

Git replays your commits on the latest `main`, adding the trailer to each. After either command, push the branch with `git push --force-with-lease`.

## The pull request template

When you open a pull request, GitHub fills in its description from [`.github/PULL_REQUEST_TEMPLATE.md`](https://github.com/ome-projects/ome/blob/main/.github/PULL_REQUEST_TEMPLATE.md). Fill in all four sections:

| Section | What to write |
|---|---|
| What this PR does | A short description of the change. |
| Why we need it | The motivation, or a link to the issue. Complete the `Fixes #` line, as in `Fixes #123`, and GitHub closes the issue when the pull request merges. |
| How to test | The steps that verify the change, or `N/A` for a docs or configuration change. |
| Checklist | Check what applies: tests added or updated, docs updated, and `make test` passing locally. |

When [Claude Code Review](#checks-that-run-on-your-pull-request) runs, it comments if a heading is missing or one of the first three sections is empty. It checks again at your next push, not when you edit the description.

## Checks that run on your pull request

PR Validation runs on pull requests into `main` or a `release-*` branch, and again when you push. When a job fails, run its command from the repository root, fix what it reports, and push again:

| Job | Run it locally |
|---|---|
| Pre-commit Checks | `pre-commit run --all-files`. It runs the hooks on all files, the Helm hooks included, and prints Passed, Failed or Skipped for each hook. |
| Test and Build | `make test`, then `make coverage`, which fails when the average coverage is below the floor that `COVER_MIN` sets in the Makefile. |
| Lint | `make ci-lint` |
| Generated Code Drift | `make manifests generate`, then commit what changes. See [Change the API](development-setup.md#change-the-api). |
| CLI cross-compile (CGO_ENABLED=0) | `make kubectl-ome-cross`, which prints the platforms as it builds them. |

Other checks run only on some pull requests:

| Check | Runs on |
|---|---|
| Docker Build Validation | Pull requests that change `dockerfiles/`, the root Makefiles, `pkg/xet/`, `scheduler/`, `go.mod`, `go.sum`, or the PR validation or dev image workflow. It builds the images for amd64. |
| Docker Multi-Arch Build Validation | Pull requests with the `test-multiarch` label, from the push after you add it. It builds the manager and model agent images for amd64 and arm64. |
| Website | Pull requests that change `website/`, `pkg/apis/`, `config/crd/full/`, the docs tooling in `hack/`, `go.mod`, `go.sum` or the root Makefiles. It runs the checks in [Check a docs change](development-setup.md#check-a-docs-change). It also fails when `make generate-apiref` changes the API reference. |
| Alfred Scheduler Simulator | Pull requests that change the scheduling code or simulator of [Alfred](../concepts/scheduling/alfred.md), `scheduler/`, `go.mod` or `go.sum`. Alfred is alpha. |
| Claude Code Review | Pull requests from branches in `ome-projects/ome`, not from forks, that change more than docs pages. It checks the [description](#the-pull-request-template), then comments on the code, and marks its comments Important, Nit or Pre-existing. |

## Review and merge

Every pull request needs a code review, including pull requests from project members. GitHub asks the code owners of the files you change, listed in [`.github/CODEOWNERS`](https://github.com/ome-projects/ome/blob/main/.github/CODEOWNERS), for a review. Answer every comment. When one is unclear, or you disagree, discuss it in the thread.

After the merge, the PR Push Validation workflow tests and builds `main` again, and scans it with Trivy. The integration tests in `tests/` don't run in CI.

## When you need an OEP

An OEP is a design document for a substantial change. You need one for:

- a significant architectural change;
- a major feature, such as a new CRD;
- a breaking API change;
- a change that affects several components;
- a change to core behavior or interfaces.

OEPs live in [`oeps/`](https://github.com/ome-projects/ome/tree/main/oeps), one directory each, named with the number and a short name, as in `oeps/0011-kubectl-ome-plugin/`. The directory holds the proposal in `README.md` and the metadata in `oep.yaml`.

Before you build on an OEP's design, check its `status` in `oep.yaml`. A `provisional` design can still change, and a status can trail the code, so check the code too. To list the status of all OEPs, run this from the repository root:

```bash
grep '^status:' oeps/*/oep.yaml
```

It prints each OEP's `oep.yaml` path and status, and the template's line lists the possible statuses.

## Write an OEP

### Create the OEP

Find the next free number:

```bash
ls oeps
```

It lists the OEP directories and the template, `NNNN-template`. Take the next number after the highest one, in `oeps/` or in an open pull request labeled `oep`. An OEP that builds on another can take a decimal number: `0011.1-kubectl-ome-operations` builds on `0011-kubectl-ome-plugin`.

Create a branch, then copy the template to a directory named with the number and a short, hyphenated name:

```bash
git switch -c oep/0012-example-proposal
cp -r oeps/NNNN-template oeps/0012-example-proposal
```

Git prints `Switched to a new branch 'oep/0012-example-proposal'`, and `cp` prints nothing.

### Fill in the proposal

Fill in `README.md` and `oep.yaml`. The template's comments and sample values show what the sections and fields need. The Test Plan, under Design Details, is required when the OEP targets a release. Edit the table of contents by hand, because the `hack/update-toc.sh` script that the template names doesn't exist. Most OEPs start with `status: provisional`. Quote a number with a decimal part, as in `oep-number: "0011.1"`.

### Review

Open a pull request whose title starts with `[OEP]`. The Auto Label PRs workflow labels it `oep`, and GitHub asks the code owner of `oeps/` for a review. Reviewers give their first feedback within a week, on the design's feasibility, impact and fit with the project's goals. Once the required reviewers approve, the OEP merges and implementation starts. Reference its number in the pull requests that implement it.

## Related pages

- [Set up a development environment](development-setup.md): install the tools OME needs, build and test it, and deploy your own build.
- [Writing docs](writing-docs.md): the docs style guide, the site's syntax and its checks.
