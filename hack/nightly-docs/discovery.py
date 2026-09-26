"""Partition discovery and fairly combine independently validated scan results."""

from itertools import zip_longest
import json
import os
from pathlib import Path
import sys

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


def combine(scans, context):
    expected = [slug for slug, _, _ in SHARDS]
    by_slug = {scan["shard"]: scan for scan in scans}
    if len(by_slug) != len(scans) or set(by_slug) != set(expected):
        raise ValueError("Missing or duplicate discovery scans")
    assignments = partition(context)
    for slug, scan in by_slug.items():
        if scan["base_sha"] != context["base_sha"]:
            raise ValueError("Discovery baseline mismatch")
        validate_scan(json.dumps(scan), context, slug, assignments[slug])
    selected, occupied, keys, identities, questions, deferred = [], set(), set(), set(), set(), []
    # Round robin prevents the first large subsystem from consuming the cap.
    for row in zip_longest(*(by_slug[slug]["concerns"] for slug in expected)):
        for proposal in row:
            if proposal is None:
                continue
            item = docs.plan(json.dumps({"concerns": [proposal]}), context)
            if not item:
                deferred.append((proposal["concern"], "existing PR"))
                continue
            item = item[0]
            identity = (item["area"], item["concern"])
            question = " ".join(item["question"].lower().split())
            if item["key"] in keys or identity in identities or question in questions:
                reason = "duplicate concern"
            elif occupied.intersection(item["doc_paths"]):
                reason = "overlapping documentation files"
            elif len(selected) >= docs.MAX_PRS:
                reason = "100-PR cap"
            else:
                selected.append(item)
                keys.add(item["key"])
                identities.add(identity)
                questions.add(question)
                occupied.update(item["doc_paths"])
                continue
            deferred.append((item["concern"], reason))
    return selected, deferred


def main():
    command = sys.argv[1]
    root = Path(os.environ["RUNNER_TEMP"]) / "nightly-docs-context"
    context = json.loads((root / "context.json").read_text())
    context["source_diffs"] = str(root / "nightly-docs-sources")
    if command == "partition":
        assignments = partition(context)
        (root / "assignments.json").write_text(json.dumps(assignments))
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("matrix=" + json.dumps({"include": [{"shard": slug} for slug, _, _ in SHARDS]}) + "\n")
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
        selected, deferred = combine(scans, context)
        report = {"base_sha": context["base_sha"], "scans": scans, "selected": selected,
                  "deferred": deferred}
        Path(os.environ["REPORT_OUTPUT"]).write_text(json.dumps(report, indent=2))
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("matrix=" + json.dumps({"include": selected}) + "\n")
            output.write(f"count={len(selected)}\n")
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"Selected {len(selected)} independent concerns; deferred {len(deferred)}.\n\n")
            summary.write("| Scan | Eligible commits | Reported inspected | Proposals |\n| --- | ---: | ---: | ---: |\n")
            assignments = partition(context)
            for scan in scans:
                summary.write(f"| {scan['shard']} | {len(assignments[scan['shard']])} | "
                              f"{len(scan['inspected_commits'])} | {len(scan['concerns'])} |\n")
    else:
        raise ValueError("Unknown discovery command")


if __name__ == "__main__":
    main()
