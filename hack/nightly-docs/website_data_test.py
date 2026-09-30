"""Website migration and the non-executable metadata boundary."""
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import discovery
import maintenance
import nightly_docs as docs
import placement
import website_data as data
import nightly_docs_test as fixtures
from nightly_docs_test import proposal, pr

ROOT = Path(__file__).resolve().parents[2]
NAV = (ROOT / data.NAV).read_text()


class WebsiteDataTests(unittest.TestCase):
    def test_real_navigation_and_redirects_are_data(self):
        self.assertEqual({section['id'] for section in data.navigation(NAV)}, data.SECTIONS)
        data.validate(data.REDIRECTS, (ROOT / data.REDIRECTS).read_text())

    def test_executable_navigation_is_rejected(self):
        for text in [NAV + 'process.exit();', NAV.replace("'guides'", 'process.exit()'),
                     NAV.replace("'guides'", '`guides`'), NAV.replace('groups: [', 'groups: [...extra,'),
                     NAV.replace("'guides'", "'guides', __proto__: {}"),
                     NAV.replace('export const', 'run(); export const'),
                     NAV.replace('groups: [', 'groups: [// comment\r process.exit(),'),
                     NAV.replace('groups: [', 'groups: [// comment\u2028 process.exit(),'),
                     NAV.replace('/** Sidebar', '/* x */ run(); /* Sidebar'),
                     NAV.replace('label: null', 'get label() { return null }'),
                     NAV.replace("'introduction.md'", "'../secret.md'")]:
            with self.subTest(text=text[:100]), self.assertRaises((ValueError, SyntaxError)):
                data.validate(data.NAV, text)

    def test_redirect_paths_and_schema_are_bounded(self):
        for row in [{'old': '../x.md', 'new': 'guides/x.md', 'rewrittenFrom': None},
                    {'old': 'x.md', 'new': 'guides/x.md', 'rewrittenFrom': 'invented'},
                    {'old': 'x.md', 'new': 'guides/x.md', 'rewrittenFrom': None, 'run': 'x'}]:
            with self.assertRaises(ValueError):
                data.validate(data.REDIRECTS, json.dumps([row]))

    def test_only_website_pages_and_metadata_are_eligible(self):
        for path in ['site/content/en/docs/tasks/x.md', docs.GENERATED,
                     docs.DOC_ROOT + 'reference/api/serving-runtime.md', 'website/package.json',
                     docs.DOC_ROOT + 'guides/../x.md']:
            with self.subTest(path=path), self.assertRaises(ValueError):
                docs.validate_item(proposal(doc_paths=[path]))
        with self.assertRaises(ValueError):
            docs.validate_item(proposal(doc_paths=[data.NAV]))
        docs.validate_item(proposal(doc_paths=[docs.DOC_ROOT + 'guides/x.md', data.NAV, data.REDIRECTS]))

    def test_metadata_is_not_a_new_page(self):
        page = docs.DOC_ROOT + 'guides/x.md'
        item = docs.validate_item(proposal(doc_paths=[page, data.REDIRECTS], placement={
            'examined_pages': [page], 'canonical_pages': [page], 'new_page_reason': ''}))
        placement.validate(item, [{'path': page}])

    def test_old_hugo_prs_and_queues_do_not_cover_website(self):
        item = docs.validate_item(proposal())
        old = pr(item, state='closed', body='<!-- nightly-docs:' + item['key'] + ' -->',
                 branch='codex/nightly-docs-' + hashlib.sha256(item['key'].encode()).hexdigest()[:16])
        self.assertFalse(docs.covered(item, [old]))
        self.assertEqual(discovery.pending_from_report({'queued_concerns': ['legacy']}, {}), [])
        from maintenance_test import pull
        legacy = pull()
        legacy['body'] = old['body']
        with patch.object(maintenance, 'repo', return_value='ome-projects/ome'), self.assertRaises(ValueError):
            maintenance.eligible(legacy)


class WebsiteBundleTests(unittest.TestCase):
    setUp = fixtures.GitGuardTests.setUp
    cleanup = fixtures.GitGuardTests.cleanup
    git = fixtures.GitGuardTests.git
    bundle = fixtures.GitGuardTests.bundle

    def test_navigation_rejected_before_any_payload_is_written(self):
        self.item['doc_paths'].append(data.NAV)
        raw = self.bundle({str(self.path): 'Changed\n', data.NAV: NAV + '\nprocess.exit();'})
        with self.assertRaises(ValueError):
            docs.import_bundle(self.item, self.base, raw)
        self.assertEqual(self.path.read_text(), 'Original documentation.\n')
        self.assertFalse(Path(data.NAV).exists())

    def test_navigation_is_included_in_diff_size(self):
        self.item['doc_paths'].append(data.NAV)
        huge = NAV.replace('/** Sidebar', '/** ' + '\n'.join(['data'] * 1000) + '\nSidebar')
        with self.assertRaisesRegex(ValueError, 'under 1000'):
            docs.import_bundle(self.item, self.base, self.bundle({str(self.path): 'Changed\n', data.NAV: huge}))

    def test_metadata_only_patch_cannot_publish(self):
        self.item['doc_paths'].append(data.NAV)
        with self.assertRaisesRegex(ValueError, 'authored page'):
            docs.import_bundle(self.item, self.base, self.bundle({data.NAV: NAV}))
