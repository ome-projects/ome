#!/usr/bin/env python3
"""Bound and publish one-concern documentation updates (standard library only)."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile


DOC_ROOT = "site/content/en/docs/"
GENERATED = DOC_ROOT + "reference/ome.v1beta1.md"
MAX_FILES = 3
MAX_LINES = 300
MAX_PRS = 4
CODE_PATHS = ["cmd", "pkg", "internal", "charts", "config", "scheduler", "hack",
              "dockerfiles", "Makefile", "Makefile-deps.mk", "go.mod"]
SLUG = r"[a-z0-9]+(?:-[a-z0-9]+)*"
MARKER = "<!-- nightly-docs:"


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def git(*args):
    return run("git", *args)


def pages(endpoint):
    chunks = json.loads(run("gh", "api", endpoint, "--paginate", "--slurp"))
    return [item for chunk in chunks for item in chunk]


def doc_path(path):
    p = PurePosixPath(path)
    return (path.startswith(DOC_ROOT) and path.endswith(".md")
            and ".." not in p.parts and str(p) == path and path != GENERATED)


def validate_item(item):
    for key in ("area", "concern"):
        if not re.fullmatch(SLUG, item[key]) or len(item[key]) > 64:
            raise ValueError(f"Invalid {key} slug")
    if not re.fullmatch(r"[0-9a-f]{40}", item["source_sha"]):
        raise ValueError("Expected a full source commit SHA")
    if not item["title"].startswith("[Docs] ") or len(item["title"]) > 120:
        raise ValueError("Expected a concise [Docs] title")
    for key in ("question", "evidence"):
        if not isinstance(item[key], str) or not item[key].strip():
            raise ValueError(f"Missing {key}")
    paths = item["doc_paths"]
    if not 1 <= len(paths) <= MAX_FILES or len(set(paths)) != len(paths):
        raise ValueError("Expected one to three distinct documentation files")
    if not all(doc_path(path) for path in paths):
        raise ValueError("Only handwritten documentation Markdown is allowed")
    key = f'{item["source_sha"]}:{item["area"]}:{item["concern"]}'
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return {**item, "key": key, "branch": f'codex/nightly-docs-{digest}'}


def existing_prs(repo):
    # All states are needed to remember declined proposals and merged fixes.
    result = []
    for pr in pages(f"repos/{repo}/pulls?state=all&per_page=100"):
        body = pr.get("body") or ""
        if pr["state"] != "open" and MARKER not in body:
            continue
        files = []
        if pr["state"] == "open":
            files = [f["filename"] for f in pages(
                f'repos/{repo}/pulls/{pr["number"]}/files?per_page=100')]
        result.append({"number": pr["number"], "title": pr["title"],
                       "body": body, "state": pr["state"],
                       "merged": bool(pr["merged_at"]),
                       "branch": pr["head"]["ref"], "files": files})
    return result


def covered(item, prs):
    marker = f'{MARKER}{item["key"]} -->'
    for pr in prs:
        if marker in pr["body"] or pr["branch"] == item["branch"]:
            return True
        if pr["state"] == "open" and set(item["doc_paths"]) & set(pr["files"]):
            return True
    return False


def prepare(repo, output):
    # Full history, no moving date cutoff or success cursor: failures and capped
    # work stay eligible on the next night, including the initial docs backlog.
    history = git("log", "--first-parent", "--format=%H %cs %s", "HEAD", "--", *CODE_PATHS)
    context = {"base_sha": git("rev-parse", "HEAD"), "max_prs": MAX_PRS,
               "code_history": history.splitlines(), "existing_prs": existing_prs(repo)}
    Path(output).write_text(json.dumps(context, indent=2) + "\n")


def plan(raw, context):
    proposed = json.loads(raw)["concerns"]
    if len(proposed) > MAX_PRS:
        raise ValueError("Plan exceeds the nightly PR limit")
    candidates = {line.split()[0] for line in context["code_history"]}
    selected, occupied, keys = [], set(), set()
    for proposal in proposed:
        item = validate_item(proposal)
        if item["source_sha"] not in candidates:
            raise ValueError("Source commit is not in the supplied default-branch history")
        if item["key"] in keys or occupied.intersection(item["doc_paths"]):
            raise ValueError("Planned concerns duplicate or overlap each other")
        keys.add(item["key"])
        occupied.update(item["doc_paths"])
        if not covered(item, context["existing_prs"]):
            selected.append(item)
    return selected


def validate_diff(item, base):
    if git("rev-parse", "HEAD") != base:
        raise ValueError("The writer must not commit or switch branches")
    # Include added files but never silently ignore edits outside the allowlist.
    changed = set(filter(None, git("diff", "--name-only", "HEAD").splitlines()))
    changed.update(filter(None, git("ls-files", "--others", "--exclude-standard").splitlines()))
    if not changed:
        return False
    if not changed <= set(item["doc_paths"]) or len(changed) > MAX_FILES:
        raise ValueError("Changes exceed the planned documentation file allowlist")
    for path in changed:
        p = Path(path)
        if not p.is_file() or any(parent.is_symlink() for parent in (p, *p.parents)):
            raise ValueError("Deleted files and symbolic links are not allowed")
        if p.stat().st_mode & 0o111:
            raise ValueError("Documentation must not be executable")
    subprocess.run(["git", "add", "--", *sorted(changed)], check=True)
    total = 0
    for line in git("diff", "--cached", "--numstat", base).splitlines():
        added, removed, _ = line.split("\t", 2)
        if not added.isdigit() or not removed.isdigit():
            raise ValueError("Binary changes are not allowed")
        total += int(added) + int(removed)
    if total > MAX_LINES:
        raise ValueError(f"Documentation diff exceeds {MAX_LINES} changed lines")
    subprocess.run(["git", "diff", "--cached", "--check", base], check=True)
    return total > 0


def review_passes(raw):
    verdict = json.loads(raw)
    if verdict.get("single_concern") is not True or verdict.get("accurate") is not True:
        raise ValueError("Documentation review rejected the change: " + str(verdict.get("reason")))


def publish(item, repo, base, base_branch):
    # Refresh against live PRs immediately before publishing to cover human PRs
    # opened while the model/build ran and retries after partial publication.
    if covered(item, existing_prs(repo)):
        print("Concern already covered or files reserved by another PR; skipping.")
        return
    if not validate_diff(item, base):
        print("No documentation gap to publish.")
        return
    # Never overwrite an existing branch, even after a prior push/PR API failure.
    # In that case reuse it only if its exact tree and parent match this run.
    branch = item["branch"]
    tree = git("write-tree")
    remote = git("ls-remote", "--heads", "origin", f"refs/heads/{branch}")
    if remote:
        subprocess.run(["git", "fetch", "origin", f"refs/heads/{branch}"], check=True)
        if git("rev-parse", "FETCH_HEAD^{tree}") != tree or git("rev-parse", "FETCH_HEAD^") != base:
            raise ValueError(f"Existing branch {branch} differs; inspect it before retrying")
    else:
        subprocess.run(["git", "switch", "-c", branch], check=True)
        # The publisher, rather than the model, owns commit metadata and DCO.
        git("config", "user.name", "github-actions[bot]")
        git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        message = f'[Docs] Update {item["concern"]}'[:52]
        subprocess.run(["git", "commit", "-s", "-m", message], check=True)
        subprocess.run(["git", "push", "origin", f"HEAD:refs/heads/{branch}"], check=True)
    body = f'''{MARKER}{item["key"]} -->
## What this PR does

{item["question"]}

## Why we need it

Source change: https://github.com/{repo}/commit/{item["source_sha"]}

{item["evidence"]}

Scope: **{item["area"]} / {item["concern"]}**. Other concerns are deferred.

## How to test

- Passed the documentation path and size guard (at most {MAX_FILES} files, {MAX_LINES} changed lines).
- Passed an independent accuracy and single-concern review.
- Passed `git diff --check` and the production Hugo build.

## Checklist

- [ ] Tests added/updated (if applicable)
- [x] Docs updated (if applicable)
- [ ] `make test` passes locally (not run; documentation only)
'''
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md") as f:
        f.write(body)
        f.flush()
        url = run("gh", "pr", "create", "--repo", repo, "--base", base_branch,
                  "--head", branch, "--title", item["title"], "--body-file", f.name)
    print(url)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f'- {item["title"]}: {url}\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["context", "plan", "check", "publish"])
    args = parser.parse_args()
    repo = os.environ["GITHUB_REPOSITORY"]
    if args.command == "context":
        prepare(repo, os.environ["NIGHTLY_CONTEXT"])
    elif args.command == "plan":
        context = json.loads(Path(os.environ["NIGHTLY_CONTEXT"]).read_text())
        items = plan(os.environ["PLAN_JSON"], context)
        with open(os.environ["GITHUB_OUTPUT"], "a") as out:
            out.write("matrix=" + json.dumps({"include": items}) + "\n")
            out.write(f"count={len(items)}\nbase_sha={context['base_sha']}\n")
    else:
        item = validate_item(json.loads(os.environ["ITEM_JSON"]))
        base = os.environ["BASE_SHA"]
        if args.command == "check":
            changed = validate_diff(item, base)
            with open(os.environ["GITHUB_OUTPUT"], "a") as out:
                out.write(f"changed={str(changed).lower()}\n")
        else:
            review_passes(os.environ["REVIEW_JSON"])
            publish(item, repo, base, os.environ["BASE_BRANCH"])


if __name__ == "__main__":
    main()
