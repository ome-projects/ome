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
import time

import nightly_docs as docs

STATE = "<!-- docs-maintenance-state:"
CHECK = "Docs maintenance"
BOTS = {"claude", "coderabbitai"}
TRUSTED_ASSOCIATIONS = {'OWNER', 'MEMBER', 'COLLABORATOR'}
MAX_ATTEMPTS = 3


def reject_secrets(text):
    """Reject known live credentials and recognizable token literals without echoing them."""
    values = [os.getenv(name, '') for name in ('ANTHROPIC_API_KEY', 'GH_TOKEN', 'GITHUB_TOKEN')]
    if (any(len(value) >= 8 and value in text for value in values)
            or re.search(r'(?:sk-ant-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]{20,}', text)):
        raise ValueError('Credential-like content detected; refusing public output')


def trusted_feedback(author, association):
    """Only maintainers and authenticated review bots can spend model budget."""
    return bool(author) and (association in TRUSTED_ASSOCIATIONS or (
        (author.get('__typename') or author.get('type')) == 'Bot'
        and author['login'].removesuffix('[bot]') in BOTS))


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


def current_base():
    """PR base.sha can lag branch updates; read the live main ref explicitly."""
    return api(f"repos/{repo()}/git/ref/heads/main")["object"]["sha"]


def get_pr(number):
    """Keep PR identity metadata, but pin source freshness to the actual branch."""
    pr = api(f"repos/{repo()}/pulls/{number}")
    pr["base"]["sha"] = current_base()
    return pr


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


def substantive_comment(comment):
    """Bookkeeping and bot skip notices are not new review feedback."""
    author, body = comment["user"]["login"], comment["body"]
    if not trusted_feedback(comment['user'], comment.get('author_association')):
        return False
    if author == "github-actions[bot]" and body.startswith(STATE):
        return False
    if (author == "coderabbitai[bot]"
            and body.startswith("<!-- This is an auto-generated comment: summarize by coderabbit.ai -->\n"
                                "<!-- This is an auto-generated comment: skip review by coderabbit.ai -->")):
        return False
    return True


def feedback(pr):
    """Read all feedback, including unresolved threads, with bounded graph pages."""
    number = pr["number"]
    comments = docs.pages(f"repos/{repo()}/issues/{number}/comments?per_page=100")
    state, state_id = decode_state(comments)
    reviews = docs.pages(f"repos/{repo()}/pulls/{number}/reviews?per_page=100")
    owner, name = repo().split("/")
    threads, cursor, unresolved, protected = [], None, [], []
    query = """query($owner:String!,$name:String!,$number:Int!,$cursor:String){
      repository(owner:$owner,name:$name){pullRequest(number:$number){
        reviewThreads(first:100,after:$cursor){pageInfo{hasNextPage endCursor}
          nodes{id isResolved isOutdated path line comments(first:100){
            pageInfo{hasNextPage} nodes{author{__typename login} authorAssociation body url}}}}
      }}}"""
    while True:
        data = api("graphql", "POST", {"query": query, "variables": {
            "owner": owner, "name": name, "number": number, "cursor": cursor}})
        connection = data["data"]["repository"]["pullRequest"]["reviewThreads"]
        for thread in connection["nodes"]:
            if thread["isResolved"]:
                continue
            unresolved.append(thread['id'])
            if thread["comments"]["pageInfo"]["hasNextPage"]:
                raise ValueError("A review thread exceeds 100 comments; human triage required")
            comments_in_thread = thread['comments']['nodes']
            accepted_comments = [c for c in comments_in_thread
                                 if trusted_feedback(c.get('author'), c.get('authorAssociation'))]
            if len(accepted_comments) != len(comments_in_thread):
                protected.append(thread['id'])
            if accepted_comments:
                threads.append({**thread, "comments": accepted_comments, "number": len(threads) + 1})
        if not connection["pageInfo"]["hasNextPage"]:
            break
        cursor = connection["pageInfo"]["endCursor"]
    failed_checks = [{"name": c["name"], "conclusion": c["conclusion"],
                      "url": c["details_url"], "output": c["output"]}
                     for c in check_runs(pr["head"]["sha"])
                     if c["name"] != CHECK and c["conclusion"] in
                     {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}]
    details = {"comments": [{"id": c["id"], "author": c["user"]["login"], "body": c["body"]}
                            for c in comments if substantive_comment(c)],
               "reviews": [{"id": r["id"], "author": r["user"]["login"], "body": r["body"],
                            "state": r["state"], "commit": r["commit_id"]}
                           for r in reviews if trusted_feedback(r['user'], r.get('author_association'))
                           and (r["body"] or r["state"] == "CHANGES_REQUESTED")],
               "threads": threads, "failed_checks": failed_checks,
               "unresolved_threads": unresolved, "protected_threads": protected}
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
    substantive = {key: value for key, value in details.items()
                   if key not in {'unresolved_threads', 'protected_threads'}}
    return hashlib.sha256(json.dumps([pr["head"]["sha"], pr["base"]["sha"],
                                     substantive, extra], sort_keys=True).encode()).hexdigest()


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
    state = dict(state)
    for key, limit in [('reason', 4000), ('extra_feedback', 2000)]:
        if key in state:
            state[key] = state[key][:limit]
    reject_secrets(json.dumps(state))
    encoded = base64.b64encode(json.dumps(state, ensure_ascii=False).encode()).decode()
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


def apply_mode(inputs, event):
    """Honor reusable-call inputs even when the caller runs an automatic sweep."""
    if 'apply' in inputs:
        if type(inputs['apply']) is not bool:
            raise ValueError('apply must be a boolean')
        return inputs['apply']
    return event in {'schedule', 'issue_comment', 'workflow_run'}


def select(number, force, merge):
    """Reconcile all eligible PRs on each wake so replaced queued events are safe."""
    if not number and (force or os.getenv('EXTRA_FEEDBACK')):
        raise ValueError('feedback and force require an explicit PR number')
    if len(os.getenv('EXTRA_FEEDBACK', '')) > 2000:
        raise ValueError('Additional feedback is limited to 2000 characters')
    prs = ([get_pr(int(number))] if number else
           docs.pages(f"repos/{repo()}/pulls?state=open&per_page=100"))
    base = current_base()
    for pr in prs:
        pr["base"]["sha"] = base
    prs = list({pr["number"]: pr for pr in prs}.values())

    def candidate(pr):
        if pr['state'] != 'open':
            print(f"Skipping closed PR #{pr['number']}")
            return None
        try:
            eligible(pr)
            details, state, _ = feedback(pr)
        except ValueError as error:
            if number:
                raise
            print(f"Skipping PR #{pr['number']}: {type(error).__name__}; inspect it with an explicit dispatch")
            return None
        extra = os.getenv("EXTRA_FEEDBACK") or state.get("extra_feedback", "")
        action = decision(state, signature(pr, details, extra), force)
        if action == "needs-human" or (action == "cached" and not merge):
            return None
        return {"number": pr["number"], "priority": 0 if action == "work" else 1}

    with ThreadPoolExecutor(max_workers=4) as pool:
        selected = [item for item in pool.map(candidate, prs) if item is not None]
    # Repairs precede cached merge checks so older PRs waiting for approval do
    # not indefinitely occupy all 100 slots. Oldest first within each class.
    selected.sort(key=lambda item: (item["priority"], item["number"]))
    selected = [{"number": item["number"]} for item in selected[:100]]
    output(matrix={"include": selected}, count=str(len(selected)))


def context(pr):
    """Verify the full PR diff, and overlay data on trusted current main."""
    source, area, concern = eligible(pr)
    base, head = pr["base"]["sha"], pr["head"]["sha"]
    if any(not re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("Expected immutable commits")
    docs.mutate_git("fetch", "--no-tags", "origin", base, head)
    try:
        docs.git("merge-base", "--is-ancestor", source, base)
    except subprocess.CalledProcessError as error:
        if error.returncode == 1:
            raise ValueError('The source commit is not an ancestor of main') from error
        raise
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
    pr = get_pr(number)
    eligible(pr)
    try:
        ctx = context(pr)
        reject_secrets(json.dumps(ctx))
        restore(ctx)
    except ValueError as error:
        if apply:
            _, state_id = decode_state(docs.pages(f"repos/{repo()}/issues/{number}/comments?per_page=100"))
            rejected = {'number': number, 'state_id': state_id, 'base': pr['base']['sha'],
                        'run_url': f"https://github.com/{repo()}/actions/runs/{os.environ['GITHUB_RUN_ID']}"}
            save_state(rejected, {'phase': 'needs-human', 'attempts': MAX_ATTEMPTS,
                                  'head': pr['head']['sha'], 'base': pr['base']['sha'],
                                  'reason': str(error), 'run_url': rejected['run_url']})
            record_check(rejected, pr['head']['sha'], False, str(error))
        raise
    action = decision(ctx["state"], ctx["signature"], force)
    directory.mkdir(parents=True, exist_ok=True)
    ctx["action"] = action
    if action == "work":
        ctx["findings"] = checks.document_findings(ctx["files"], Path.cwd())
        attempts = 1 if force else ctx["state"].get("attempts", 0) + 1
        ctx["attempts"] = attempts
        if apply:
            save_state(ctx, {"phase": "working", "attempts": attempts, "head": ctx["head"],
                             "base": ctx["base"], "run_url": ctx["run_url"],
                             "extra_feedback": ctx["extra_feedback"],
                             "reason": "Repair/validation in progress; no merge authorization implied."})
    reject_secrets(json.dumps(ctx))
    (directory / "context.json").write_text(json.dumps(ctx, indent=2))
    output(work=str(action == "work").lower(), cached=str(action == "cached").lower(), base=ctx["base"])


def live_match(ctx):
    """Reject stale writers rather than overwrite a new commit or ignore feedback."""
    pr = get_pr(ctx['number'])
    eligible(pr)
    details, _, _ = feedback(pr)
    if (pr["head"]["sha"] != ctx["head"] or pr["base"]["sha"] != ctx["base"]
            or signature(pr, details, ctx["extra_feedback"]) != ctx["signature"]):
        raise ValueError("PR head, main, or feedback changed; discard this stale attempt")
    return pr


def refresh_base(ctx, directory):
    """Carry a reviewed patch across unrelated new pages, never source edits."""
    pr = get_pr(ctx['number'])
    eligible(pr)
    details, _, _ = feedback(pr)
    previous = {**pr, 'base': {**pr['base'], 'sha': ctx['base']}}
    if (pr['head']['sha'] != ctx['head']
            or signature(previous, details, ctx['extra_feedback']) != ctx['signature']):
        raise ValueError('PR head or feedback changed during review; discard this stale attempt')
    base = pr['base']['sha']
    if base == ctx['base']:
        return
    docs.mutate_git('fetch', '--no-tags', 'origin', base)
    docs.git('merge-base', '--is-ancestor', ctx['base'], base)
    for line in docs.git('diff', '--name-status', ctx['base'], base).splitlines():
        status, path = line.split('\t', 1)
        if (status != 'A' or not docs.doc_path(path) or path in ctx['item']['doc_paths']
                or docs.git('ls-tree', base, '--', path).split()[0] != '100644'):
            raise ValueError('Main changed source, existing docs, or overlapping paths; a fresh review is required')
    # Executable source, schemas, templates and every existing page are byte-for-
    # byte unchanged. Reuse the semantic verdict, then rebuild and recheck links
    # against the new site. Publication still rejects any subsequent base move.
    docs.export_bundle(ctx['item'], ctx['base'], directory / 'bundle.json')
    payload = json.loads((directory / 'bundle.json').read_text())
    docs.mutate_git('reset', '--hard', ctx['base'])
    ctx['review_base'] = ctx['base']
    ctx['base'] = base
    ctx['signature'] = signature(pr, details, ctx['extra_feedback'])
    payload['base_sha'] = base
    restore(ctx, json.dumps(payload))
    (directory / 'bundle.json').write_text(json.dumps(payload))
    (directory / 'context.json').write_text(json.dumps(ctx, indent=2))
    (directory / 'full-pr.patch').write_text(docs.git('diff', '--cached', base))
    print(f"Refreshed {ctx['review_base']} -> {base}: only unrelated new documentation pages")


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
            if threads[n - 1]['id'] not in ctx['feedback'].get('protected_threads', [])
            and threads[n - 1]["comments"] and all(
                c.get("author") and c["author"].get("__typename") == "Bot"
                and c["author"]["login"].removesuffix("[bot]") in BOTS
                for c in threads[n - 1]["comments"])]


def record_check(ctx, head, accepted, reason):
    """Attach the verdict to the actual PR commit, not the dispatcher commit."""
    reject_secrets(reason)
    api(f"repos/{repo()}/check-runs", "POST", {
        "name": CHECK, "head_sha": head, "status": "completed",
        "conclusion": "success" if accepted else "failure", "details_url": ctx["run_url"],
        "external_id": f"docs-maintenance:{ctx['number']}:{ctx['base']}",
        "output": {"title": "Validated documentation" if accepted else "Documentation needs repair",
                   "summary": reason[:60000]}})


def finish(ctx, directory, apply):
    """Publish bounded progress and record an honest success/failure verdict."""
    raw = (directory / 'review.json').read_text() if (directory / 'review.json').exists() else os.getenv("REVIEW_JSON", "")
    reject_secrets(raw)
    verdict = docs.review_verdict(raw) if raw else {
        "single_concern": False, "accurate": False, "reason": "Review did not complete."}
    threads = checked_threads(verdict, ctx)
    findings_path = directory / "checks.json"
    findings = json.loads(findings_path.read_text()) if findings_path.exists() else ["Checks did not complete."]
    technical = os.getenv("BUILD_OK") == "true" and os.getenv("CHECKS_OK") == "true" and not findings
    accepted = technical and verdict["single_concern"] and verdict["accurate"]
    resolved = []
    reason = "\n".join(findings + [verdict["reason"]])
    if not technical:
        reason += "\nProduction build or deterministic validation did not pass."
    head = ctx["head"]
    reject_secrets(reason)
    if apply:
        if accepted:
            head = publish_repair(ctx)
        else:
            live_match(ctx)
        current = published_pr(ctx, head)
        record_check(ctx, head, accepted, reason)
        expected_details = json.loads(json.dumps(ctx["feedback"]))
        # Checks on the old commit are evidence for the repair, not failures on
        # its successor. Any newly arriving result invalidates the saved digest.
        if head != ctx["head"]:
            expected_details["failed_checks"] = []
        if accepted and threads:
            fresh, _, _ = feedback(current)
            original_threads = {t["id"]: t for t in ctx["feedback"]["threads"]}
            fresh_threads = {t["id"]: t for t in fresh["threads"]}
            for thread in threads:
                if (fresh_threads.get(thread) != original_threads[thread]
                        or thread in fresh.get('protected_threads', [])):
                    continue
                api("graphql", "POST", {"query": "mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id}}}",
                                        "variables": {"id": thread}})
                resolved.append(thread)
                expected_details["threads"] = [t for t in expected_details["threads"] if t["id"] != thread]
            for index, thread in enumerate(expected_details["threads"], 1):
                thread["number"] = index
        current = published_pr(ctx, head)
        attempts = 0 if accepted else ctx["attempts"]
        state = {"phase": "ready" if accepted else ("needs-human" if attempts >= MAX_ATTEMPTS else "needs-repair"),
                 "attempts": attempts, "head": head, "base": ctx["base"], "reason": reason,
                 "signature": signature(current, expected_details, ctx["extra_feedback"]),
                 "extra_feedback": ctx["extra_feedback"], "run_url": ctx["run_url"]}
        save_state(ctx, state)
    result = {"number": ctx["number"], "applied": apply, "published": head != ctx['head'],
              "accepted": accepted, "head": head,
              "base": ctx["base"], "review_base": ctx.get("review_base", ctx["base"]),
              "reason": reason, "resolved_bot_threads": resolved}
    (directory / "result.json").write_text(json.dumps(result, indent=2))
    publication = ('dry run, no repository writes' if not apply else
                   ('repair published' if head != ctx['head'] else 'no repair published'))
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"PR #{ctx['number']}: {'validated' if accepted else 'needs repair'}; "
                      f"{publication}.\n\n"
                      f"<pre>{html.escape(reason)}</pre>\n")


def published_pr(ctx, head):
    """Tolerate propagation of our push, but never accept a competing head/base."""
    for attempt in range(12):
        current = get_pr(ctx['number'])
        eligible(current)
        if current['base']['sha'] != ctx['base']:
            raise ValueError('Main changed after publication; a fresh review is required')
        if current['head']['sha'] == head:
            return current
        if current['head']['sha'] != ctx['head'] or attempt == 11:
            raise ValueError('PR changed after publication or our push did not propagate')
        time.sleep(2)


def failure(ctx):
    """An interrupted model/build still consumes the reserved failed-round budget."""
    current = get_pr(ctx['number'])
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
    pr = get_pr(number)
    eligible(pr)
    details, state, _ = feedback(pr)
    digest = signature(pr, details, state.get("extra_feedback", ""))
    info = json.loads(docs.run("gh", "pr", "view", str(number), "--repo", repo(),
                              "--json", "reviewDecision,mergeStateStatus,statusCheckRollup"))
    info["unresolved"] = bool(details.get('unresolved_threads', details['threads']))
    info["checks"] = info.pop("statusCheckRollup")
    checks = check_runs(pr["head"]["sha"])
    blockers = merge_blockers(pr, state, digest, info, checks)
    if not enabled:
        blockers.insert(0, "Automatic merge is disabled (DOCS_MAINTENANCE_MERGE is not true)")
    if blockers:
        message = "Not merged: " + "; ".join(blockers)
    else:
        latest = get_pr(number)
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
    if command == 'mode':
        output(apply=str(apply_mode(json.loads(os.environ['INPUTS_JSON']), os.environ['RUN_EVENT'])).lower())
        return
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
        for path in ctx['item']['doc_paths']:
            if Path(path).is_file():
                reject_secrets(Path(path).read_text())
        docs.export_bundle(ctx["item"], ctx["base"], directory / "bundle.json")
        reject_secrets((directory / 'bundle.json').read_text())
    elif command == "import":
        reject_secrets((directory / 'bundle.json').read_text())
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
    elif command == "refresh":
        refresh_base(ctx, directory)
    elif command == "finish":
        finish(ctx, directory, apply)
    elif command == 'review-export':
        raw = os.getenv('REVIEW_JSON', '')
        verdict = docs.review_verdict(raw) if raw else {
            'single_concern': False, 'accurate': False, 'reason': 'Review did not complete.', 'addressed_threads': []}
        reject_secrets(json.dumps(verdict))
        (directory / 'review.json').write_text(json.dumps(verdict))
    elif command == 'review-gate':
        raw = (directory / 'review.json').read_text()
        reject_secrets(raw)
        verdict = docs.review_verdict(raw)
        output(accepted=str(verdict['single_concern'] and verdict['accurate']).lower())
    elif command == 'scan':
        for path in directory.rglob('*'):
            if path.is_file():
                reject_secrets(path.read_text())
    elif command == "failure":
        failure(ctx)
    else:
        raise ValueError("Unknown maintenance command")


if __name__ == "__main__":
    main()
