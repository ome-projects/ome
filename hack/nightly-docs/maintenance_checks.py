"""Deterministic checks for authored docs; examples are data, never commands."""

import json
from pathlib import Path
import re


def kubernetes_schema(schema):
    """Adapt Kubernetes' nullable and IntOrString extensions for Draft 7."""
    if isinstance(schema, list):
        return [kubernetes_schema(value) for value in schema]
    if not isinstance(schema, dict):
        return schema
    result = {key: kubernetes_schema(value) for key, value in schema.items()}
    if result.get("x-kubernetes-int-or-string"):
        result.pop("type", None)
        result["anyOf"] = [{"type": "integer"}, {"type": "string"}]
    if result.pop("nullable", False):
        result = {"anyOf": [result, {"type": "null"}]}
    return result


def schema_catalog(root):
    """Load exactly the checked-out full CRD schemas, not a remote catalog."""
    import yaml
    catalog = {}
    for path in (root / "config/crd/full").glob("*.yaml"):
        for crd in yaml.safe_load_all(path.read_text()):
            if not crd or crd.get("kind") != "CustomResourceDefinition":
                continue
            spec = crd["spec"]
            for version in spec["versions"]:
                catalog[(spec["group"] + "/" + version["name"], spec["names"]["kind"])] = (
                    kubernetes_schema(version["schema"]["openAPIV3Schema"]))
    return catalog


def document_findings(files, root):
    """Check links' deployment prefix and complete fenced OME manifests."""
    import yaml
    from jsonschema import Draft7Validator
    catalog = schema_catalog(root)
    findings = []
    for path, content in files.items():
        for match in re.finditer(r'(?:\]\(|href=["\'])(/(?:ome/)?docs/[^\s)"\']*)', content):
            findings.append(f"{path}: internal link {match[1]} uses a legacy docs URL; use the website /ome/<section>/ route")
        # Shell heredocs are deliberately not executed or claimed as validated.
        for match in re.finditer(r"^```ya?ml[^\n]*\n(.*?)^```", content, re.M | re.S):
            try:
                documents = list(yaml.safe_load_all(match[1]))
            except yaml.YAMLError as error:
                findings.append(f"{path}: invalid YAML example: {error}")
                continue
            for document in documents:
                if not isinstance(document, dict):
                    continue
                key = (document.get("apiVersion"), document.get("kind"))
                if key not in catalog:
                    continue
                for error in Draft7Validator(catalog[key]).iter_errors(document):
                    location = ".".join(str(part) for part in error.absolute_path)
                    findings.append(f"{path}: {key[1]} example {location}: {error.message}")
    return findings


def write_report(files, root, output):
    """Persist example findings; the website content tests validate rendered links."""
    findings = document_findings(files, root)
    Path(output).write_text(json.dumps(findings, indent=2))
    return findings
