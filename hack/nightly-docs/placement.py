"""Choose canonical documentation homes and review actual human PR overlap."""

import json
from pathlib import Path
import re

import nightly_docs as docs


def inventory(base):
    """Index authored pages at the pinned source revision, including headings."""
    result = []
    for path in docs.git('ls-tree', '-r', '--name-only', base, '--', docs.DOC_ROOT).splitlines():
        if not docs.doc_path(path):
            continue
        content = docs.git('show', f'{base}:{path}')
        title = re.search(r'^title:\s*(.+)$', content, re.MULTILINE)
        result.append({'path': path, 'title': title.group(1).strip('"\'') if title else path,
                       'headings': re.findall(r'^#{1,6}\s+(.+)$', content, re.MULTILINE)})
    return result


def validate(item, pages):
    """Require an auditable existing-page search and justification for new pages."""
    known = {page['path'] for page in pages}
    decision = item.get('placement', {})
    for key in ['examined_pages', 'canonical_pages']:
        paths = decision.get(key)
        if (not isinstance(paths, list) or any(not isinstance(p, str) for p in paths)
                or len(paths) != len(set(paths)) or not set(paths) <= known):
            raise ValueError(f'Invalid placement {key}; use existing authored pages')
    if known and not decision['examined_pages']:
        raise ValueError('Inspect existing documentation before choosing a page')
    canonical = set(decision['canonical_pages'])
    if not canonical <= set(decision['examined_pages']) or not canonical <= set(item['doc_paths']):
        raise ValueError('Canonical pages needing correction must be examined and included in doc_paths')
    reason = decision.get('new_page_reason')
    if not isinstance(reason, str) or len(reason) > 4000:
        raise ValueError('Invalid new-page justification')
    if set(item['doc_paths']) - known:
        if not reason.strip():
            raise ValueError('New pages require an explanation of why existing pages cannot host the concern')
    elif not canonical:
        raise ValueError('An existing-page update must identify its canonical pages')


def automated(pr):
    """Nightly PRs retain whole-file reservations to serialize concern updates."""
    return pr['body'].startswith(docs.MARKER) and pr['branch'].startswith('codex/nightly-docs-')


def overlaps(item, prs):
    """Human file overlap is evidence to inspect, not automatic coverage."""
    paths = set(item['doc_paths'])
    return sorted([{'number': p['number'], 'head_sha': p['head_sha'],
                    'title': p['title'], 'body': p['body'],
                    'paths': sorted(paths.intersection(p['files']))}
                   for p in prs if p['state'] == 'open' and not automated(p)
                   and paths.intersection(p['files'])], key=lambda p: p['number'])


def capture(item, repo, base, destination):
    """Save pinned, doc-only human PR diffs for independent semantic review."""
    validate(item, inventory(base))
    snapshot = overlaps(item, docs.existing_prs(repo))
    evidence = []
    for pr in snapshot:
        number, head = pr['number'], pr['head_sha']
        if type(number) is not int or number < 1 or not re.fullmatch(r'[0-9a-f]{40}', head):
            raise ValueError('Invalid GitHub PR identity')
        docs.mutate_git('fetch', '--no-tags', 'origin', f'refs/pull/{number}/head')
        if docs.git('rev-parse', 'FETCH_HEAD') != head:
            raise ValueError('Human PR changed while collecting overlap evidence; retry')
        fork = docs.git('merge-base', base, head)
        patch = docs.git('diff', '--no-ext-diff', '--no-textconv', fork, head, '--', *pr['paths'])
        evidence.append({**pr, 'merge_base': fork, 'documentation_patch': patch})
    payload = json.dumps({'snapshot': snapshot, 'evidence': evidence}, indent=2)
    if len(payload.encode()) > 2 * 1024 * 1024:
        raise ValueError('Human PR overlap evidence exceeds 2 MiB; defer for human review')
    Path(destination).write_text(payload)


def verify_snapshot(item, prs, path):
    """New or edited overlapping human PRs invalidate the independent review."""
    reviewed = json.loads(Path(path).read_text())['snapshot']
    if overlaps(item, prs) != reviewed:
        raise ValueError('Overlapping human PRs changed after review; retry before publication')
