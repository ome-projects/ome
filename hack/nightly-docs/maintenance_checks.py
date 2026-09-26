"""Deterministic checks for authored docs; examples are data, never commands."""

from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


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
        for match in re.finditer(r'(?:\]\(|href=["\'])(/docs/[^\s)"\']*)', content):
            findings.append(f"{path}: internal link {match[1]} omits the deployed /ome/ prefix")
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


class Page(HTMLParser):
    """Collect local links and anchors without executing rendered content."""

    def __init__(self, text):
        super().__init__()
        self.links, self.ids = [], set()
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("id"):
            self.ids.add(attrs["id"])
        if tag == "a" and attrs.get("name"):
            self.ids.add(attrs["name"])
        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])


def rendered_findings(paths, public):
    """Check rendered docs links/anchors on edited pages, including relative URLs."""
    from urllib.parse import urljoin
    findings = []
    for path in paths:
        relative = path.removeprefix("site/content/en/").removesuffix(".md")
        relative = relative.removesuffix("/_index")
        page = public / relative / "index.html"
        if not page.is_file():
            findings.append(f"{path}: expected rendered page {relative}/index.html is missing")
            continue
        for link in Page(page.read_text()).links:
            url = urlsplit(urljoin(f"https://ome-projects.github.io/ome/{relative}/", link))
            if url.netloc != "ome-projects.github.io" or not url.path.startswith("/ome/docs/"):
                continue
            target = public / unquote(url.path.removeprefix("/ome/"))
            if target.is_dir():
                target /= "index.html"
            if not target.is_file():
                findings.append(f"{path}: broken rendered link {link}")
            elif url.fragment and unquote(url.fragment) not in Page(target.read_text()).ids:
                findings.append(f"{path}: missing rendered anchor {link}")
    return sorted(set(findings))


def write_report(files, root, output, public=None):
    """Persist all findings so a failed validation is usable by the next repair."""
    findings = document_findings(files, root)
    if public:
        findings.extend(rendered_findings(files, public))
    Path(output).write_text(json.dumps(findings, indent=2))
    return findings
