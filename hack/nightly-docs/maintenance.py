"""Reconcile existing nightly documentation PRs without bypassing review policy."""

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import html
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import nightly_docs as docs

STATE = "<!-- docs-maintenance-state:"
CHECK = "Docs maintenance"
BOTS = {"claude[bot]", "coderabbitai[bot]"}
MAX_ATTEMPTS = 3


def api(endpoint, method="GET", payload=None):
    """Use structured input; no PR-controlled text is interpolated into shell."""
    args = ["gh", "api", endpoint, "--method", method]
    if payload is not None:
        args += ["--input", "-"]
    output = subprocess.check_output(args, input=json.dumps(payload) if payload is not None else None,
                                     text=True)
    return json.loads(output) if output.strip() else None


def repo():
    """The workflow is deliberately restricted to this repository."""
    value = os.environ["GITHUB_REPOSITORY"]
    if value != "ome-projects/ome":
        raise ValueError("Unsupported repository")
    return value


def eligible(pr):
    """Authenticate the original publisher, repository, branch and concern marker."""
    body = pr.get("body") or ""
    match = re.match(r"<!-- nightly-docs:([0-9a-f]{40}):(" + docs.SLUG + r"):(" + docs.SLUG + r") -->", body)
    if (not match or pr["state"] != "open" or pr.get("draft")
            or pr["user"]["login"] != "github-actions[bot]"
            or pr["head"]["repo"] is None
            or pr["head"]["repo"]["full_name"] != repo()
            or pr["base"]["repo"]["full_name"] != repo()
            or pr["base"]["ref"] != "main"):
        raise ValueError("Not an eligible, open, same-repository nightly docs PR")
    key = ":".join(match.groups())
    expected = "codex/nightly-docs-" + hashlib.sha256(key.encode()).hexdigest()[:16]
    if pr["head"]["ref"] != expected:
        raise ValueError("PR branch does not match its original concern")
    return match.groups()


def decode_state(comments):
    """Only our bot's exact state marker can carry retry bookkeeping."""
    matches = [c for c in comments if c["user"]["login"] == "github-actions[bot]"
               and c["body"].startswith(STATE)]
    if len(matches) > 1:
        raise ValueError("Multiple maintenance state comments require inspection")
    if not matches:
        return {}, None
    comment = matches[0]
    encoded = comment["body"][len(STATE):].split(" -->", 1)[0]
    state = json.loads(base64.b64decode(encoded, validate=True))
    if type(state.get("attempts")) is not int or not 0 <= state["attempts"] <= MAX_ATTEMPTS:
        raise ValueError("Invalid maintenance attempt counter")
    return state, comment["id"]


def feedback(pr):
    """Read all feedback, including unresolved threads, with bounded graph pages."""
    number = pr["number"]
    comments = docs.pages(f"repos/{repo()}/issues/{number}/comments?per_page=100")
    state, state_id = decode_state(comments)
    reviews = docs.pages(f"repos/{repo()}/pulls/{number}/reviews?per_page=100")
    owner, name = repo().split("/")
    threads, cursor = [], None
    query = """query($owner:String!,$name:String!,$number:Int!,$cursor:String){
      repository(owner:$owner,name:$name){pullRequest(number:$number){
        reviewThreads(first:100,after:$cursor){pageInfo{hasNextPage endCursor}
          nodes{id isResolved isOutdated path line comments(first:100){
            pageInfo{hasNextPage} nodes{author{login} body url}}}}
      }}}"""
    while True:
        data = api("graphql", "POST", {"query": query, "variables": {
            "owner": owner, "name": name, "number": number, "cursor": cursor}})
        connection = data["data"]["repository"]["pullRequest"]["reviewThreads"]
        for thread in connection["nodes"]:
            if thread["isResolved"]:
                continue
            if thread["comments"]["pageInfo"]["hasNextPage"]:
                raise ValueError("A review thread exceeds 100 comments; human triage required")
            threads.append({**thread, "comments": thread["comments"]["nodes"],
                            "number": len(threads) + 1})
        if not connection["pageInfo"]["hasNextPage"]:
            break
        cursor = connection["pageInfo"]["endCursor"]
    failed_checks = [{"name": c["name"], "conclusion": c["conclusion"],
                      "url": c["details_url"], "output": c["output"]}
                     for c in check_runs(pr["head"]["sha"])
                     if c["name"] != CHECK and c["conclusion"] in
                     {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}]
    details = {"comments": [{"id": c["id"], "author": c["user"]["login"], "body": c["body"]}
                            for c in comments if not (c["user"]["login"] == "github-actions[bot]"
                                                      and c["body"].startswith(STATE))],
               "reviews": [{"id": r["id"], "author": r["user"]["login"], "body": r["body"],
                            "state": r["state"], "commit": r["commit_id"]}
                           for r in reviews if r["body"] or r["state"] == "CHANGES_REQUESTED"],
               "threads": threads, "failed_checks": failed_checks}
    if len(json.dumps(details).encode()) > 2 * 1024 * 1024:
        raise ValueError("Feedback exceeds 2 MiB; human triage required")
    return details, state, state_id


def check_runs(head):
    """Read every check page at this immutable PR commit."""
    raw = json.loads(docs.run("gh", "api", f"repos/{repo()}/commits/{head}/check-runs?per_page=100",
                              "--paginate", "--slurp"))
    return [check for page in raw for check in page["check_runs"]]


def signature(pr, details, extra=""):
    """A cached review is invalidated by content, base, or substantive feedback."""
    return hashlib.sha256(json.dumps([pr["head"]["sha"], pr["base"]["sha"],
                                     details, extra], sort_keys=True).encode()).hexdigest()


def decision(state, digest, force=False):
    """Bound unsuccessful rounds without resetting the budget on our own push."""
    if force:
        return "work"
    if state.get("phase") == "ready" and state.get("signature") == digest:
        return "cached"
    if state.get("attempts", 0) >= MAX_ATTEMPTS:
        return "needs-human"
    return "work"


def state_body(state):
    """Expose a single readable status with machine-readable retry history."""
    encoded = base64.b64encode(json.dumps(state).encode()).decode()
    return (f"{STATE}{encoded} -->\n"
            f"Documentation maintenance: **{state['phase']}**. "
            f"Unsuccessful-round budget used: {state['attempts']}/{MAX_ATTEMPTS}.\n\n"
            f"Head: `{state['head']}`; reviewed main: `{state['base']}`.\n\n"
            f"<pre>{html.escape(state.get('reason', ''))}</pre>\n\n"
            f"[Workflow evidence]({state['run_url']})\n\n"
            "Human review threads and CODEOWNER approval remain under repository policy.")


def save_state(ctx, state):
    """Update one status comment; never post repeated feedback chatter."""
    body = {"body": state_body(state)}
    if ctx.get("state_id"):
        api(f"repos/{repo()}/issues/comments/{ctx['state_id']}", "PATCH", body)
    else:
        result = api(f"repos/{repo()}/issues/{ctx['number']}/comments", "POST", body)
        ctx["state_id"] = result["id"]


def output(**values):
    """Write compact workflow outputs, never multiline model-controlled values."""
    with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
        for key, value in values.items():
            stream.write(f"{key}={json.dumps(value) if not isinstance(value, str) else value}\n")


def select(number, force, merge):
    """Reconcile all eligible PRs on each wake so replaced queued events are safe."""
    prs = ([api(f"repos/{repo()}/pulls/{int(number)}")] if number else
           docs.pages(f"repos/{repo()}/pulls?state=open&per_page=100"))
    prs = list({pr["number"]: pr for pr in prs}.values())

    def candidate(pr):
        try:
            eligible(pr)
        except ValueError:
            if number:
                raise
            return None
        details, state, _ = feedback(pr)
        extra = os.getenv("EXTRA_FEEDBACK") or state.get("extra_feedback", "")
        action = decision(state, signature(pr, details, extra), force)
        if action == "needs-human" or (action == "cached" and not merge):
            return None
        return {"number": pr["number"]}

    with ThreadPoolExecutor(max_workers=4) as pool:
        selected = [item for item in pool.map(candidate, prs) if item is not None]
    # Oldest PRs first; 100 is the existing creation cap, not a file limit.
    selected.sort(key=lambda item: item["number"])
    selected = selected[:100]
    output(matrix={"include": selected}, count=str(len(selected)))


def context(pr):
    """Verify the full PR diff, and overlay data on trusted current main."""
    source, area, concern = eligible(pr)
    base, head = pr["base"]["sha"], pr["head"]["sha"]
    if any(not re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("Expected immutable commits")
    docs.mutate_git("fetch", "--no-tags", "origin", base, head)
    docs.git("merge-base", "--is-ancestor", source, base)
    fork = docs.git("merge-base", base, head)
    files = {}
    for line in docs.git("diff", "--name-status", fork, head).splitlines():
        status, path = line.split("\t", 1)
        if status not in {"A", "M"} or not docs.doc_path(path):
            raise ValueError("The entire PR must contain only added/modified authored docs")
        if docs.git("ls-tree", head, "--", path).split()[0] != "100644":
            raise ValueError("Symlinks and executable documentation are forbidden")
        files[path] = subprocess.check_output(
            ["git", "-c", "core.hooksPath=/dev/null", "show", f"{head}:{path}"], text=True)
    if not files:
        raise ValueError("No documentation changes remain")
    advanced = set(docs.git("diff", "--name-only", fork, base).splitlines())
    if advanced.intersection(files):
        raise ValueError("Main changed the same documentation; human conflict resolution required")
    details, state, state_id = feedback(pr)
    extra = os.getenv("EXTRA_FEEDBACK") or state.get("extra_feedback", "")
    item = docs.validate_item({"area": area, "concern": concern, "source_sha": source,
                              "title": pr["title"], "question": pr["body"],
                              "evidence": "Original PR body and current source", "doc_paths": sorted(files)})
    return {"number": pr["number"], "head": head, "base": base, "item": item,
            "files": files, "feedback": details, "state": state, "state_id": state_id,
            "extra_feedback": extra, "signature": signature(pr, details, extra),
            "tools_sha": os.environ["GITHUB_SHA"],
            "run_url": f"https://github.com/{repo()}/actions/runs/{os.environ['GITHUB_RUN_ID']}"}


def restore(ctx, bundle=None):
    """Import only validated markdown into a pristine trusted main checkout."""
    docs.mutate_git("checkout", "--detach", ctx["base"])
    payload = bundle or json.dumps({"base_sha": ctx["base"], "key": ctx["item"]["key"],
                                   "files": ctx["files"]})
    return docs.import_bundle(ctx["item"], ctx["base"], payload)


def prepare(number, directory, apply, force):
    """Reserve one bounded attempt under the workflow's per-PR concurrency lock."""
    import maintenance_checks as checks
    ctx = context(api(f"repos/{repo()}/pulls/{number}"))
    action = decision(ctx["state"], ctx["signature"], force)
    directory.mkdir(parents=True, exist_ok=True)
    ctx["action"] = action
    if action == "work":
        restore(ctx)
        ctx["findings"] = checks.document_findings(ctx["files"], Path.cwd())
        attempts = 1 if force else ctx["state"].get("attempts", 0) + 1
        ctx["attempts"] = attempts
        if apply:
            save_state(ctx, {"phase": "working", "attempts": attempts, "head": ctx["head"],
                             "base": ctx["base"], "run_url": ctx["run_url"],
                             "reason": "Repair/validation in progress; no merge authorization implied."})
    (directory / "context.json").write_text(json.dumps(ctx, indent=2))
    output(work=str(action == "work").lower(), cached=str(action == "cached").lower(), base=ctx["base"])


def live_match(ctx):
    """Reject stale writers rather than overwrite a new commit or ignore feedback."""
    pr = api(f"repos/{repo()}/pulls/{ctx['number']}")
    eligible(pr)
    details, _, _ = feedback(pr)
    if (pr["head"]["sha"] != ctx["head"] or pr["base"]["sha"] != ctx["base"]
            or signature(pr, details, ctx["extra_feedback"]) != ctx["signature"]):
        raise ValueError("PR head, main, or feedback changed; discard this stale attempt")
    return pr


def publish_repair(ctx):
    """Append a normal commit, carrying current main, with no force push."""
    live_match(ctx)
    reviewed_tree = docs.git("write-tree")
    final = {path: Path(path).read_text() if Path(path).exists() else None
             for path in ctx["item"]["doc_paths"]}
    docs.mutate_git("reset", "--hard", ctx["base"])
    docs.mutate_git("checkout", "-B", "docs-maintenance-work", ctx["head"])
    docs.git("config", "user.name", "github-actions[bot]")
    docs.git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    docs.mutate_git("merge", "--no-ff", "--no-commit", ctx["base"])
    for path, content in final.items():
        if content is None:
            if docs.git("ls-tree", ctx["base"], "--", path):
                raise ValueError("Refusing to delete a file from main")
            docs.mutate_git("rm", "--ignore-unmatch", "--", path)
        else:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(content)
            docs.mutate_git("add", "--", path)
    if docs.git("write-tree") != reviewed_tree:
        raise ValueError("Publication tree differs from the validated tree")
    if reviewed_tree == docs.git("rev-parse", "HEAD^{tree}"):
        return ctx["head"]
    docs.mutate_git("commit", "-s", "-m", f"[Docs] Address feedback for #{ctx['number']}")
    # The normal push is the final compare-and-swap: a competing commit rejects it.
    live_match(ctx)
    docs.mutate_git("push", "origin", f"HEAD:refs/heads/{ctx['item']['branch']}")
    return docs.git("rev-parse", "HEAD")


def checked_threads(verdict, ctx):
    """Resolve only explicitly verified bot-only threads; never human discussions."""
    numbers = verdict.get("addressed_threads", [])
    threads = ctx["feedback"]["threads"]
    if (not isinstance(numbers, list) or any(type(n) is not int or not 1 <= n <= len(threads)
                                           for n in numbers) or len(numbers) != len(set(numbers))):
        raise ValueError("Invalid addressed-thread report")
    return [threads[n - 1]["id"] for n in numbers
            if threads[n - 1]["comments"] and all(c.get("author") and c["author"]["login"] in BOTS
                                                 for c in threads[n - 1]["comments"])]


def record_check(ctx, head, accepted, reason):
    """Attach the verdict to the actual PR commit, not the dispatcher commit."""
    api(f"repos/{repo()}/check-runs", "POST", {
        "name": CHECK, "head_sha": head, "status": "completed",
        "conclusion": "success" if accepted else "failure", "details_url": ctx["run_url"],
        "external_id": f"docs-maintenance:{ctx['number']}:{ctx['base']}",
        "output": {"title": "Validated documentation" if accepted else "Documentation needs repair",
                   "summary": reason[:60000]}})


def finish(ctx, directory, apply):
    """Publish bounded progress and record an honest success/failure verdict."""
    raw = os.getenv("REVIEW_JSON", "")
    verdict = docs.review_verdict(raw) if raw else {
        "single_concern": False, "accurate": False, "reason": "Review did not complete."}
    threads = checked_threads(verdict, ctx)
    findings_path = directory / "checks.json"
    findings = json.loads(findings_path.read_text()) if findings_path.exists() else ["Checks did not complete."]
    technical = os.getenv("BUILD_OK") == "true" and os.getenv("CHECKS_OK") == "true" and not findings
    accepted = technical and verdict["single_concern"] and verdict["accurate"]
    reason = "\n".join(findings + [verdict["reason"]])
    if not technical:
        reason += "\nProduction build or deterministic validation did not pass."
    head = ctx["head"]
    if apply:
        live_match(ctx)
        # A sound, single-concern partial repair can remain in an OPEN PR. Its
        # failing quality check and stored reviewer findings prohibit merging.
        if technical and verdict["single_concern"]:
            head = publish_repair(ctx)
        record_check(ctx, head, accepted, reason)
        expected_details = json.loads(json.dumps(ctx["feedback"]))
        # Checks on the old commit are evidence for the repair, not failures on
        # its successor. Any newly arriving result invalidates the saved digest.
        if head != ctx["head"]:
            expected_details["failed_checks"] = []
        if accepted:
            current = api(f"repos/{repo()}/pulls/{ctx['number']}")
            fresh, _, _ = feedback(current)
            original_threads = {t["id"]: t for t in ctx["feedback"]["threads"]}
            fresh_threads = {t["id"]: t for t in fresh["threads"]}
            for thread in threads:
                if fresh_threads.get(thread) != original_threads[thread]:
                    continue
                api("graphql", "POST", {"query": "mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id}}}",
                                        "variables": {"id": thread}})
                expected_details["threads"] = [t for t in expected_details["threads"] if t["id"] != thread]
            for index, thread in enumerate(expected_details["threads"], 1):
                thread["number"] = index
        current = api(f"repos/{repo()}/pulls/{ctx['number']}")
        details, _, _ = feedback(current)
        if current["head"]["sha"] != head or current["base"]["sha"] != ctx["base"]:
            raise ValueError("PR changed after publication; a fresh review is required")
        attempts = 0 if accepted else ctx["attempts"]
        state = {"phase": "ready" if accepted else ("needs-human" if attempts >= MAX_ATTEMPTS else "needs-repair"),
                 "attempts": attempts, "head": head, "base": ctx["base"], "reason": reason,
                 "signature": signature(current, expected_details, ctx["extra_feedback"]),
                 "extra_feedback": ctx["extra_feedback"], "run_url": ctx["run_url"]}
        save_state(ctx, state)
    result = {"number": ctx["number"], "applied": apply, "accepted": accepted, "head": head,
              "base": ctx["base"], "reason": reason, "resolved_bot_threads": threads if accepted else []}
    (directory / "result.json").write_text(json.dumps(result, indent=2))
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"PR #{ctx['number']}: {'validated' if accepted else 'needs repair'}; "
                      f"{'changes published' if apply else 'dry run, no repository writes'}.\n\n"
                      f"<pre>{html.escape(reason)}</pre>\n")


def failure(ctx):
    """An interrupted model/build still consumes the reserved failed-round budget."""
    current = api(f"repos/{repo()}/pulls/{ctx['number']}")
    if current["state"] != "open":
        return
    state = {"phase": "needs-human" if ctx["attempts"] >= MAX_ATTEMPTS else "needs-repair",
             "attempts": ctx["attempts"], "head": current["head"]["sha"], "base": current["base"]["sha"],
             "extra_feedback": ctx["extra_feedback"], "run_url": ctx["run_url"],
             "reason": "A worker or publication step failed. Inspect the linked workflow before retrying; no success verdict was recorded."}
    save_state(ctx, state)
    record_check(ctx, current["head"]["sha"], False, state["reason"])


def merge_blockers(pr, state, digest, info, checks):
    """A normal merge must satisfy both our exact-revision gate and GitHub policy."""
    blockers = []
    if decision(state, digest) != "cached" or state.get("head") != pr["head"]["sha"] or state.get("base") != pr["base"]["sha"]:
        blockers.append("A fresh successful documentation review is required")
    if info["reviewDecision"] != "APPROVED":
        blockers.append("Required approval (including CODEOWNER approval) is missing")
    if info["mergeStateStatus"] != "CLEAN":
        blockers.append("GitHub reports the PR is not cleanly mergeable")
    if info["unresolved"]:
        blockers.append("Unresolved review threads remain")
    passing = {"SUCCESS", "SKIPPED", "NEUTRAL"}
    for check in info["checks"]:
        if check.get("status") == "COMPLETED":
            good = check.get("conclusion") in passing
        else:
            good = check.get("state") == "SUCCESS"
        if not good:
            blockers.append("A check is pending or unsuccessful")
            break
    expected = f"docs-maintenance:{pr['number']}:{pr['base']['sha']}"
    matching = [c for c in checks if c["name"] == CHECK and c["app"]["slug"] == "github-actions"]
    latest = max(matching, key=lambda c: c["id"]) if matching else None
    if not latest or latest["external_id"] != expected or latest["conclusion"] != "success":
        blockers.append("Missing current trusted Docs maintenance check")
    return blockers


def merge(number, enabled):
    """Opt-in squash merge; no admin bypass, auto-approval, or policy mutations."""
    pr = api(f"repos/{repo()}/pulls/{number}")
    eligible(pr)
    details, state, _ = feedback(pr)
    digest = signature(pr, details, state.get("extra_feedback", ""))
    info = json.loads(docs.run("gh", "pr", "view", str(number), "--repo", repo(),
                              "--json", "reviewDecision,mergeStateStatus,statusCheckRollup"))
    info["unresolved"] = bool(details["threads"])
    info["checks"] = info.pop("statusCheckRollup")
    checks = check_runs(pr["head"]["sha"])
    blockers = merge_blockers(pr, state, digest, info, checks)
    if not enabled:
        blockers.insert(0, "Automatic merge is disabled (DOCS_MAINTENANCE_MERGE is not true)")
    if blockers:
        message = "Not merged: " + "; ".join(blockers)
    else:
        latest = api(f"repos/{repo()}/pulls/{number}")
        if latest["head"]["sha"] != pr["head"]["sha"] or latest["base"]["sha"] != pr["base"]["sha"]:
            raise ValueError("PR or main moved before merge")
        result = api(f"repos/{repo()}/pulls/{number}/merge", "PUT", {
            "sha": pr["head"]["sha"], "merge_method": "squash"})
        if not result["merged"]:
            raise ValueError("GitHub refused the merge: " + result["message"])
        message = "Squash merged " + result["sha"]
    print(message)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"PR #{number}: {html.escape(message)}\n")


def main():
    """Expose narrowly scoped commands to trusted workflow steps."""
    directory = Path(os.environ.get("MAINTENANCE_DIR", os.environ.get("RUNNER_TEMP", "/tmp") + "/docs-maintenance"))
    command = sys.argv[1]
    number = int(os.getenv("PR_NUMBER") or "0")
    apply = os.getenv("APPLY") == "true"
    force = os.getenv("FORCE") == "true"
    if command == "select":
        select(number, force, os.getenv("ALLOW_MERGE") == "true")
        return
    if command == "prepare":
        prepare(number, directory, apply, force)
        return
    if command == "merge":
        merge(number, os.getenv("ALLOW_MERGE") == "true")
        return
    ctx = json.loads((directory / "context.json").read_text())
    if command == "restore":
        restore(ctx)
    elif command == "export":
        docs.export_bundle(ctx["item"], ctx["base"], directory / "bundle.json")
    elif command == "import":
        changed = restore(ctx, (directory / "bundle.json").read_text())
        (directory / "full-pr.patch").write_text(docs.git("diff", "--cached", ctx["base"]))
        output(changed=str(changed).lower())
    elif command == "check":
        import maintenance_checks as checks
        files = {path: Path(path).read_text() for path in ctx["item"]["doc_paths"] if Path(path).is_file()}
        findings = checks.write_report(files, Path.cwd(), directory / "checks.json",
                                       Path(os.environ["PUBLIC_DIR"]) if os.getenv("PUBLIC_DIR") else None)
        if findings:
            raise ValueError("\n".join(findings))
    elif command == "finish":
        finish(ctx, directory, apply)
    elif command == "failure":
        failure(ctx)
    else:
        raise ValueError("Unknown maintenance command")


if __name__ == "__main__":
    main()
