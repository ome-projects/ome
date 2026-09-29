"""Partition discovery and fairly combine independently validated scan results."""

from datetime import datetime, timedelta, timezone
from itertools import zip_longest
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import urlencode

import nightly_docs as docs


# Ownership is about the user question, not where a shared API type happens to
# live. A commit may appear in several scans; the complete history stays eligible.
SHARDS = [
    ("cli-observe", "Read-only CLI commands, reports, wait predicates, logs and diagnostics; not mutating commands.",
     ["pkg/cli/cmd/get", "pkg/cli/cmd/status", "pkg/cli/cmd/wait", "pkg/cli/cmd/logs", "pkg/cli/report", "pkg/cli/root.go"]),
    ("cli-actions", "CLI mutations: runtime sync, scale, rollout, migration, instance, traffic and admin actions; not read-only reports or controller internals.",
     ["pkg/cli/cmd/runtime", "pkg/cli/cmd/scale", "pkg/cli/cmd/rollout", "pkg/cli/cmd/migration", "pkg/cli/cmd/instance", "pkg/cli/cmd/traffic", "pkg/cli/cmd/admin", "pkg/cli/mutate"]),
    ("model-storage", "Model storage, downloads, metadata, adapters and model-agent behavior; not runtime selection or CLI commands.",
     ["pkg/modelagent", "internal/ome-agent", "pkg/controller/v1beta1/basemodel", "pkg/apis/ome/v1beta1/model.go", "pkg/utils/storage", "config/models", "cmd/model-agent", "cmd/ome-agent"]),
    ("runtime-accelerators", "Runtime definitions, selection, pinning, inheritance and accelerator classes; not CLI commands or rollout orchestration.",
     ["pkg/runtimeselector", "pkg/acceleratorclassselector", "pkg/controller/v1beta1/runtimerevision", "pkg/controller/v1beta1/servingruntime", "pkg/controller/v1beta1/acceleratorclass", "pkg/apis/ome/v1beta1/servingruntime_types.go", "pkg/apis/ome/v1beta1/accelerator_class.go", "config/runtimes"]),
    ("workload-rollouts", "Workload generation, replicas, deployment modes, rollout policies and lifecycle; not CLI commands, routing, or unfinished multi-cluster promises.",
     ["pkg/controller/v1beta1/inferencereplica", "pkg/controller/v1beta1/rolloutpolicy", "pkg/controller/v1beta1/inferenceservice", "pkg/apis/ome/v1beta1/inference_service.go", "pkg/apis/ome/v1beta1/inferencereplica_types.go", "pkg/apis/ome/v1beta1/rollout_types.go", "pkg/apis/ome/v1beta1/rolloutpolicy_types.go"]),
    ("networking-traffic", "Service exposure, ingress, routing, traffic maps and network behavior; not CLI command syntax or rollout orchestration.",
     ["pkg/controller/v1beta1/inferenceservice/reconcilers/ingress", "pkg/controller/v1beta1/inferenceservice/reconcilers/service", "pkg/controller/v1beta1/inferenceservice/reconcilers/traffic", "pkg/apis/ome/v1beta1/traffic_types.go", "pkg/apis/ome/v1beta1/trafficmap_types.go", "pkg/apis/ome/v1beta1/routing_types.go"]),
    ("autoscaling-quota", "Autoscaling, reusable scaler policies, metric providers, quota and scheduling; not CLI command syntax.",
     ["pkg/controller/v1beta1/autoscalerpolicy", "pkg/controller/v1beta1/acceleratorquota", "pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler", "pkg/quota", "pkg/apis/ome/v1beta1/autoscalerpolicy_types.go", "pkg/apis/ome/v1beta1/autoscaler.go", "pkg/apis/ome/v1beta1/acceleratorquota_types.go", "scheduler", "charts/ome-quota-manager"]),
    ("operations", "Installation, upgrades, controller configuration, RBAC, observability, benchmarking and remaining operator-facing gaps outside the other scan responsibilities.",
     ["cmd/manager", "charts", "pkg/leaderelection", "pkg/controller/v1beta1/controllerconfig", "pkg/controller/v1beta1/benchmark", "pkg/apis/ome/v1beta1/benchmark_job.go", "config/rbac", "dockerfiles", "Makefile", "Makefile-deps.mk"]),
]


def partition(context):
    history = {line.split()[0]: line for line in context["code_history"]}
    assignments = {}
    for slug, _, paths in SHARDS:
        assignments[slug] = set(docs.git("log", "--first-parent", "--format=%H",
                                       context["base_sha"], "--", *paths).splitlines()) & history.keys()
    # No path falls through the cracks. Operational discovery also receives
    # commits outside the named subsystems, including future directories.
    assignments["operations"].update(history.keys() - set().union(*assignments.values()))
    return {slug: [line for sha, line in history.items() if sha in assignments[slug]]
            for slug, _, _ in SHARDS}


def resolve_scan(raw, history):
    """Resolve small model-selected IDs through the trusted source index."""
    result = json.loads(raw)
    commits = [line.split()[0] for line in history]

    def resolve(number):
        if type(number) is not int or number < 1 or number > len(commits):
            raise ValueError("Source commit ID is outside this scan's index")
        return commits[number - 1]

    concerns = []
    for proposal in result["concerns"]:
        item = dict(proposal)
        if "source_sha" in item:
            raise ValueError("The model must select a commit ID, not supply a hash")
        item["source_sha"] = resolve(item.pop("source_commit"))
        concerns.append(item)
    return json.dumps({"concerns": concerns,
                       "inspected_commits": [resolve(number) for number in result["inspected_commits"]],
                       "remaining_work": result["remaining_work"]})


def validate_scan(raw, context, slug, history):
    result = json.loads(raw)
    inspected = result["inspected_commits"]
    candidates = {line.split()[0] for line in history}
    if (not isinstance(inspected, list) or any(not isinstance(sha, str) for sha in inspected)
            or len(inspected) != len(set(inspected)) or not set(inspected) <= candidates):
        raise ValueError("Invalid inspected-commit report")
    if not isinstance(result["remaining_work"], str) or not result["remaining_work"].strip():
        raise ValueError("Missing remaining-work report")
    concerns = result["concerns"]
    # Validate each independently. Competing proposals are deferred centrally,
    # not an operational failure that discards a whole scan's useful results.
    for item in concerns:
        docs.plan(json.dumps({"concerns": [item]}), {**context, "code_history": history})
        if item["source_sha"] not in inspected:
            raise ValueError("Concern source was not reported as inspected")
    if len(concerns) > docs.MAX_PRS:
        raise ValueError("Scan exceeds proposal limit")
    return {"shard": slug, "base_sha": context["base_sha"], "concerns": concerns,
            "inspected_commits": inspected, "remaining_work": result["remaining_work"]}


def combine(scans, context, allow_partial=False):
    expected = context.get("selected_shards", [slug for slug, _, _ in SHARDS])
    by_slug = {scan["shard"]: scan for scan in scans}
    if len(by_slug) != len(scans) or not set(by_slug) <= set(expected):
        raise ValueError("Unknown or duplicate discovery scans")
    if not allow_partial and set(by_slug) != set(expected):
        raise ValueError("Missing discovery scans")
    assignments = partition(context)
    for slug, scan in by_slug.items():
        if scan["base_sha"] != context["base_sha"]:
            raise ValueError("Discovery baseline mismatch")
        validate_scan(json.dumps(scan), context, slug, assignments[slug])
    selected, occupied, keys, identities, questions, deferred = [], set(), set(), set(), set(), []
    # Round robin prevents the first large subsystem from consuming the cap.
    for row in zip_longest(*(by_slug[slug]["concerns"] for slug in expected if slug in by_slug)):
        for proposal in row:
            if proposal is None:
                continue
            item = docs.plan(json.dumps({"concerns": [proposal]}), context)
            if not item:
                key = f'{proposal["source_sha"]}:{proposal["area"]}:{proposal["concern"]}'
                deferred.append((key, "existing PR"))
                continue
            item = item[0]
            identity = (item["area"], item["concern"])
            question = " ".join(item["question"].lower().split())
            if item["key"] in keys or identity in identities or question in questions:
                reason = "duplicate concern"
            elif occupied.intersection(item["doc_paths"]):
                reason = "overlapping documentation files"
            elif len(selected) >= context.get("max_prs", docs.MAX_PRS):
                reason = "PR cap"
            else:
                selected.append(item)
                keys.add(item["key"])
                identities.add(identity)
                questions.add(question)
                occupied.update(item["doc_paths"])
                continue
            deferred.append((item["key"], reason))
    return selected, deferred


def deferred_queue(scans, deferred, prs):
    """Queue file-blocked concerns without reviving merged or declined work."""
    queued = {key for key, reason in deferred
              if reason in {'overlapping documentation files', 'PR cap', 'existing PR'}}
    queued_concerns = []
    for scan in scans:
        for proposal in scan['concerns']:
            title = proposal['title']
            item = docs.validate_item({**proposal, 'title': title if title.startswith('[Docs] ') else '[Docs] ' + title})
            if item['key'] not in queued:
                continue
            # Keep file-blocked work, but never queue an already-open,
            # merged, or deliberately declined instance of this concern.
            if any(f"{docs.MARKER}{item['key']} -->" in pr['body'] or item['branch'] == pr['branch']
                   for pr in prs):
                continue
            queued_concerns.append(item)
    return queued_concerns


def build_report(scans, context):
    """Keep validated partial work while explicitly reporting missing coverage."""
    selected, deferred = combine(scans, context, allow_partial=True)
    expected = context.get('selected_shards', [name for name, _, _ in SHARDS])
    received = {scan['shard'] for scan in scans}
    missing = [name for name in expected if name not in received]
    queued = deferred_queue(scans, deferred, context['existing_prs'])
    if missing:
        selected_keys = {item['key'] for item in selected}
        queued.extend(item for item in context.get('pending_concerns', [])
                      if docs.validate_item(item)['key'] not in selected_keys)
    return {'base_sha': context['base_sha'], 'scans': scans, 'selected': selected,
            'deferred': deferred, 'queued_concerns': pending_from_report({'queued_concerns': queued}, context),
            'expected_shards': expected, 'missing_shards': missing, 'complete': not missing,
            'doc_inventory': context.get('doc_inventory', []),
            'dry_run': context.get('dry_run', False), 'max_prs': context.get('max_prs', docs.MAX_PRS)}


def pending_from_report(report, context):
    """Carry deferred concerns as evidence; revalidate against current docs/history."""
    history = {line.split()[0] for line in context['code_history']}
    result, seen = [], set()
    for proposal in report.get('queued_concerns', []):
        item = docs.validate_item(proposal)
        identity = (item['area'], item['concern'])
        if any(f"{docs.MARKER}{item['key']} -->" in pr['body'] or item['branch'] == pr['branch']
               for pr in context.get('existing_prs', [])):
            continue
        if item['source_sha'] in history and identity not in seen:
            seen.add(identity)
            result.append(item)
    return result[:docs.MAX_PRS]


def previous_pending(repo, branch, context):
    """Read the latest retained main-branch plan, never another branch's pilot."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).date().isoformat()
    query = urlencode({'branch': branch, 'per_page': 100, 'created': '>=' + cutoff})
    run_pages = json.loads(docs.run('gh', 'api',
        f'repos/{repo}/actions/workflows/nightly-docs.yml/runs?{query}', '--paginate', '--slurp'))
    for run in (run for page in run_pages for run in page['workflow_runs']):
        if (run['status'] != 'completed' or run['head_branch'] != branch
                or run['head_repository']['full_name'] != repo):
            continue
        pages = json.loads(docs.run('gh', 'api',
            f"repos/{repo}/actions/runs/{run['id']}/artifacts?per_page=100", '--paginate', '--slurp'))
        artifacts = [artifact for page in pages for artifact in page['artifacts']]
        if not any(a['name'] == 'nightly-docs-discovery-report' and not a['expired'] for a in artifacts):
            continue
        with tempfile.TemporaryDirectory() as directory:
            docs.run('gh', 'run', 'download', str(run['id']), '--repo', repo,
                     '--name', 'nightly-docs-discovery-report', '--dir', directory)
            report = json.loads(Path(directory, 'nightly-docs-discovery-report.json').read_text())
        # A full production attempt may carry a partial recovery report.
        # Filtered/dry-run pilots still cannot replace the production queue.
        if (report.get('dry_run') is not False or report.get('max_prs') != docs.MAX_PRS
                or set(report.get('expected_shards', [scan['shard'] for scan in report.get('scans', [])]))
                != {name for name, _, _ in SHARDS}):
            continue
        return pending_from_report(report, context)
    return []


def main():
    command = sys.argv[1]
    root = Path(os.environ["RUNNER_TEMP"]) / "nightly-docs-context"
    context = json.loads((root / "context.json").read_text())
    context["source_diffs"] = str(root / "nightly-docs-sources")
    if command == "partition":
        selected_shard = os.getenv('DISCOVERY_SHARD', '')
        names = [slug for slug, _, _ in SHARDS]
        if selected_shard and selected_shard not in names:
            raise ValueError('Unknown discovery shard')
        context['selected_shards'] = [selected_shard] if selected_shard else names
        (root / 'context.json').write_text(json.dumps(context, indent=2))
        assignments = partition(context)
        (root / "assignments.json").write_text(json.dumps(assignments))
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("matrix=" + json.dumps({"include": [{"shard": slug} for slug in context["selected_shards"]]}) + "\n")
    elif command == "context":
        slug = os.environ["SHARD"]
        assignments = json.loads((root / "assignments.json").read_text())
        focus = next(focus for name, focus, _ in SHARDS if name == slug)
        context.update(code_history=[f"{i}: {line}" for i, line in enumerate(assignments[slug], 1)],
                       shard=slug, focus=focus,
                       scan_responsibilities={name: focus for name, focus, _ in SHARDS})
        Path(os.environ["NIGHTLY_CONTEXT"]).write_text(json.dumps(context, indent=2))
    elif command == "scan":
        slug = os.environ["SHARD"]
        assignments = json.loads((root / "assignments.json").read_text())
        raw = resolve_scan(os.environ["PLAN_JSON"], assignments[slug])
        scan = validate_scan(raw, context, slug, assignments[slug])
        Path(os.environ["SCAN_OUTPUT"]).write_text(json.dumps(scan))
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"{slug}: {len(assignments[slug])} eligible commits; "
                          f"{len(scan['inspected_commits'])} self-reported inspected; "
                          f"{len(scan['concerns'])} proposed concerns.\n")
    elif command == "combine":
        scans = [json.loads(path.read_text()) for path in Path(os.environ["SCAN_DIR"]).glob("*.json")]
        report = build_report(scans, context)
        selected, deferred = report['selected'], report['deferred']
        Path(os.environ["REPORT_OUTPUT"]).write_text(json.dumps(report, indent=2))
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("matrix=" + json.dumps({"include": selected}) + "\n")
            output.write(f"count={len(selected)}\n")
            output.write(f"complete={str(report['complete']).lower()}\n")
            output.write("missing=" + ", ".join(report['missing_shards']) + "\n")
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"Selected {len(selected)} independent concerns; deferred {len(deferred)}.\n\n")
            if report['missing_shards']:
                summary.write("**Incomplete discovery**: no validated result from "
                              + ", ".join(report['missing_shards'])
                              + ". Successful scans continue; missing scans remain eligible next run.\n\n")
            summary.write("| Scan | Eligible commits | Reported inspected | Proposals |\n| --- | ---: | ---: | ---: |\n")
            assignments = partition(context)
            for scan in scans:
                summary.write(f"| {scan['shard']} | {len(assignments[scan['shard']])} | "
                              f"{len(scan['inspected_commits'])} | {len(scan['concerns'])} |\n")
    else:
        raise ValueError("Unknown discovery command")


if __name__ == "__main__":
    main()
