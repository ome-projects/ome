"""Bound and publish one-concern documentation updates (standard library only)."""

import argparse
import hashlib
import html
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile


DOC_ROOT = "site/content/en/docs/"
GENERATED = DOC_ROOT + "reference/ome.v1beta1.md"
MAX_LINES = 1000
MAX_PRS = 100
CODE_PATHS = ["cmd", "pkg", "internal", "charts", "config", "scheduler", "hack",
              "dockerfiles", "Makefile", "Makefile-deps.mk", "go.mod"]
SLUG = r"[a-z0-9]+(?:-[a-z0-9]+)*"
MARKER = "<!-- nightly-docs:"


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def git(*args):
    return run("git", "-c", "core.hooksPath=/dev/null", *args)


def mutate_git(*args):
    subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args], check=True)


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
    title = item["title"]
    if (not title.startswith("[Docs] ") or not title[7:].strip()
            or len(title) > 120 or not title.isprintable()):
        raise ValueError("Expected a concise single-line [Docs] title")
    for key in ("question", "evidence"):
        if not isinstance(item[key], str) or not item[key].strip():
            raise ValueError(f"Missing {key}")
    paths = item["doc_paths"]
    if not paths or len(set(paths)) != len(paths):
        raise ValueError("Expected one or more distinct documentation files")
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
    sources = Path(output).parent / "nightly-docs-sources"
    sources.mkdir(exist_ok=True)
    for line in history.splitlines():
        sha = line.split()[0]
        with (sources / f"{sha}.patch").open("w") as patch:
            subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "show",
                            "--first-parent", "--no-ext-diff", "--no-textconv",
                            sha, "--", *CODE_PATHS], stdout=patch, check=True)
    context = {"base_sha": git("rev-parse", "HEAD"), "max_prs": MAX_PRS,
               "code_history": history.splitlines(), "source_diffs": str(sources),
               "existing_prs": existing_prs(repo)}
    Path(output).write_text(json.dumps(context, indent=2) + "\n")


def plan(raw, context):
    proposed = json.loads(raw)["concerns"]
    if len(proposed) > MAX_PRS:
        raise ValueError("Plan exceeds the nightly PR limit")
    candidates = {line.split()[0] for line in context["code_history"]}
    selected, occupied, keys = [], set(), set()
    for proposal in proposed:
        # The workflow owns the repository's presentation prefix. Keep all
        # content, length, and printable-character validation below unchanged.
        if not proposal["title"].startswith("[Docs] "):
            proposal = {**proposal, "title": "[Docs] " + proposal["title"]}
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
    if not changed <= set(item["doc_paths"]):
        raise ValueError("Changes exceed the planned documentation file allowlist")
    for path in changed:
        p = Path(path)
        if not p.is_file() or any(parent.is_symlink() for parent in (p, *p.parents)):
            raise ValueError("Deleted files and symbolic links are not allowed")
        if p.stat().st_mode & 0o111:
            raise ValueError("Documentation must not be executable")
    mutate_git("add", "--", *sorted(changed))
    total = 0
    for line in git("diff", "--cached", "--numstat", base).splitlines():
        added, removed, _ = line.split("\t", 2)
        if not added.isdigit() or not removed.isdigit():
            raise ValueError("Binary changes are not allowed")
        total += int(added) + int(removed)
    if total >= MAX_LINES:
        raise ValueError(f"Documentation diff must be under {MAX_LINES} changed lines")
    mutate_git("diff", "--cached", "--check", base)
    return total > 0


def export_bundle(item, base, output):
    """Writer output is untrusted data; never transfer its scripts or .git."""
    validate_diff(item, base)
    changed = git("diff", "--cached", "--name-only", base).splitlines()
    payload = {"base_sha": base, "key": item["key"],
               "files": {path: Path(path).read_text() for path in changed}}
    Path(output).write_text(json.dumps(payload) + "\n")


def import_bundle(item, base, raw):
    """Revalidate writer output using the publisher's pristine default-branch code."""
    if len(raw.encode()) > 10 * 1024 * 1024:
        raise ValueError("Documentation bundle exceeds 10 MiB")
    payload = json.loads(raw)
    if payload["base_sha"] != base or payload["key"] != item["key"]:
        raise ValueError("Bundle does not match this concern and base")
    files = payload["files"]
    if not isinstance(files, dict) or not set(files) <= set(item["doc_paths"]):
        raise ValueError("Bundle contains paths outside the documentation allowlist")
    # Validate the entire payload before writing anything. Neither hooks nor
    # executable scripts/configuration from the writer are ever imported.
    for path, content in files.items():
        p = Path(path)
        if (not doc_path(path) or not isinstance(content, str) or "\x00" in content
                or any(parent.is_symlink() for parent in (p, *p.parents))):
            raise ValueError("Invalid documentation bundle entry")
    for path, content in files.items():
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    return validate_diff(item, base)


def review_verdict(raw):
    verdict = json.loads(raw)
    if (not isinstance(verdict, dict)
            or type(verdict.get("single_concern")) is not bool
            or type(verdict.get("accurate")) is not bool
            or not isinstance(verdict.get("reason"), str)
            or not verdict["reason"].strip() or len(verdict["reason"]) > 10000):
        raise ValueError("Malformed documentation review")
    return verdict


def record_review(raw):
    verdict = review_verdict(raw)
    accepted = verdict["single_concern"] and verdict["accurate"]
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"accepted={str(accepted).lower()}\n")
    if not accepted:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write("Documentation proposal rejected; no PR created.\n\n<pre>"
                          + html.escape(verdict["reason"]) + "</pre>\n")


def review_passes(raw):
    verdict = review_verdict(raw)
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
        mutate_git("fetch", "origin", f"refs/heads/{branch}")
        if git("rev-parse", "FETCH_HEAD^{tree}") != tree or git("rev-parse", "FETCH_HEAD^") != base:
            raise ValueError(f"Existing branch {branch} differs; inspect it before retrying")
    else:
        mutate_git("switch", "-c", branch)
        # The publisher, rather than the model, owns commit metadata and DCO.
        git("config", "user.name", "github-actions[bot]")
        git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        message = f'[Docs] Update {item["concern"]}'[:52]
        mutate_git("commit", "-s", "-m", message)
        mutate_git("push", "origin", f"HEAD:refs/heads/{branch}")
    body = f'''{MARKER}{item["key"]} -->
## What this PR does

{item["question"]}

## Why we need it

Source change: https://github.com/{repo}/commit/{item["source_sha"]}

{item["evidence"]}

Scope: **{item["area"]} / {item["concern"]}**. Other concerns are deferred.

## How to test

- Passed the documentation path and size guard (under {MAX_LINES} added plus deleted lines; no file-count limit).
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
    parser.add_argument("command", choices=["context", "plan", "evidence", "check", "export", "import", "review", "publish"])
    args = parser.parse_args()
    repo = os.environ["GITHUB_REPOSITORY"]
    if args.command == "context":
        prepare(repo, os.environ["NIGHTLY_CONTEXT"])
    elif args.command == "plan":
        context = json.loads(Path(os.environ["NIGHTLY_CONTEXT"]).read_text())
        items = plan(os.environ["PLAN_JSON"], context)
        with open(os.environ["GITHUB_OUTPUT"], "a") as out:
            out.write("matrix=" + json.dumps({"include": items}) + "\n")
            out.write(f"count={len(items)}\n")
    elif args.command == "review":
        record_review(os.environ["REVIEW_JSON"])
    else:
        item = validate_item(json.loads(os.environ["ITEM_JSON"]))
        base = os.environ["BASE_SHA"]
        if args.command == "evidence":
            path = Path(os.environ["NIGHTLY_ITEM"])
            path.write_text(json.dumps(item) + "\n")
            patch = git("show", "--first-parent", "--no-ext-diff", "--no-textconv", item["source_sha"])
            path.with_name("nightly-docs-source.patch").write_text(patch + "\n")
        elif args.command in ("check", "import"):
            if args.command == "import":
                changed = import_bundle(item, base, Path(os.environ["BUNDLE_PATH"]).read_text())
            else:
                changed = validate_diff(item, base)
            with open(os.environ["GITHUB_OUTPUT"], "a") as out:
                out.write(f"changed={str(changed).lower()}\n")
        elif args.command == "export":
            export_bundle(item, base, os.environ["BUNDLE_PATH"])
        else:
            review_passes(os.environ["REVIEW_JSON"])
            publish(item, repo, base, os.environ["BASE_BRANCH"])


if __name__ == "__main__":
    main()
