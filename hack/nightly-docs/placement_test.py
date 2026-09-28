"""Regression coverage for canonical-page placement and human PR conflicts."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import discovery
import nightly_docs as docs
import placement
import nightly_docs_test as fixtures
from nightly_docs_test import proposal, pr


class PlacementTests(unittest.TestCase):
    def setUp(self):
        self.path = docs.DOC_ROOT + 'concepts/serving_runtime.md'
        self.item = docs.validate_item(proposal(doc_paths=[self.path], placement={
            'examined_pages': [self.path], 'canonical_pages': [self.path], 'new_page_reason': ''}))
        self.pages = [{'path': self.path, 'title': 'ServingRuntime', 'headings': ['Model size']}]

    def test_existing_page_correction_is_eligible_despite_old_broad_draft(self):
        human = pr(self.item, branch='old/refactor', body='Split controllers', head_sha='b' * 40,
                   draft=True, created_at='2025-12-03')
        context = {'code_history': ['a' * 40 + ' old change'], 'doc_inventory': self.pages,
                   'existing_prs': [human]}
        self.assertEqual(len(docs.plan(json.dumps({'concerns': [self.item]}), context)), 1)
        self.assertEqual(placement.overlaps(self.item, [human])[0]['number'], 1)
        automatic = pr(self.item, body=docs.MARKER + 'another-concern -->', branch='codex/nightly-docs-another')
        self.assertTrue(docs.covered(self.item, [automatic]))

    def test_new_page_cannot_replace_a_canonical_correction_or_omit_justification(self):
        new = docs.DOC_ROOT + 'reference/model-size.md'
        item = {**self.item, 'doc_paths': [new]}
        with self.assertRaisesRegex(ValueError, 'Canonical pages'):
            placement.validate(item, self.pages)
        item['doc_paths'].append(self.path)
        with self.assertRaisesRegex(ValueError, 'New pages require'):
            placement.validate(item, self.pages)
        item['placement'] = {**self.item['placement'], 'new_page_reason': 'A distinct worked example with a canonical correction.'}
        placement.validate(item, self.pages)

    def test_existing_search_must_reference_real_pages(self):
        for pages in [[], ['nonexistent.md']]:
            item = {**self.item, 'placement': {**self.item['placement'], 'examined_pages': pages}}
            with self.assertRaises(ValueError):
                placement.validate(item, self.pages)

    def test_same_page_concerns_are_deferred_instead_of_relocated(self):
        shard = discovery.SHARDS[0][0]
        context = {'base_sha': 'c' * 40, 'code_history': ['a' * 40 + ' old'], 'doc_inventory': self.pages,
                   'existing_prs': [], 'selected_shards': [shard], 'max_prs': 100}
        second = {**self.item, 'concern': 'another-claim', 'question': 'Another question'}
        scans = [{'shard': shard, 'base_sha': context['base_sha'], 'concerns': [self.item, second],
                  'inspected_commits': ['a' * 40], 'remaining_work': 'None'}]
        with patch.object(discovery, 'partition', return_value={shard: context['code_history']}):
            selected, deferred = discovery.combine(scans, context)
        self.assertEqual(len(selected), 1)
        self.assertEqual(deferred, [('another-claim', 'overlapping documentation files')])
        self.assertEqual(selected[0]['doc_paths'], [self.path])

    def test_every_editorial_and_conflict_verdict_is_required(self):
        good = dict(single_concern=True, accurate=True, placement_appropriate=True,
                    related_docs_consistent=True, no_competing_pr=True, reason='Verified')
        docs.review_passes(json.dumps(good))
        for key in ['placement_appropriate', 'related_docs_consistent', 'no_competing_pr']:
            for value in [False, 'true', None]:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    docs.review_passes(json.dumps({**good, key: value}))
        # Existing maintenance verdicts intentionally retain their own schema.
        docs.review_verdict(json.dumps({'single_concern': True, 'accurate': True, 'reason': 'Verified'}))

    def test_file_blocked_queue_survives_but_declined_concerns_do_not(self):
        other = pr(self.item, body=docs.MARKER + 'another-concern -->', branch='codex/nightly-docs-another')
        scans = [{'concerns': [self.item]}]
        deferred = [(self.item['concern'], 'existing PR')]
        self.assertEqual(discovery.deferred_queue(scans, deferred, [other]), [self.item])
        self.assertEqual(discovery.deferred_queue(scans, deferred, [pr(self.item, state='closed')]), [])

    def test_queue_rechecks_current_history_and_deduplicates(self):
        report = {'queued_concerns': [self.item, self.item, {**self.item, 'source_sha': 'd' * 40}]}
        context = {'code_history': ['a' * 40 + ' old change']}
        self.assertEqual(discovery.pending_from_report(report, context), [self.item])

    def test_queue_never_reads_branch_pilot_artifacts(self):
        runs = {'workflow_runs': [
            {'id': 1, 'status': 'completed', 'head_branch': 'codex/pilot', 'head_repository': {'full_name': 'o/r'}},
            {'id': 2, 'status': 'in_progress', 'head_branch': 'main', 'head_repository': {'full_name': 'o/r'}}]}
        with patch.object(docs, 'run', return_value=json.dumps(runs)), patch.object(docs, 'pages') as pages:
            self.assertEqual(discovery.previous_pending('o/r', 'main', {}), [])
            pages.assert_not_called()

    def test_live_human_overlap_must_match_the_reviewed_snapshot(self):
        human = pr(self.item, body='Manual change', branch='human/fix', head_sha='b' * 40)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'overlaps.json'
            path.write_text(json.dumps({'snapshot': placement.overlaps(self.item, [human])}))
            placement.verify_snapshot(self.item, [human], path)
            for current in [[], [{**human, 'head_sha': 'c' * 40}], [human, {**human, 'number': 2}],
                            [{**human, 'body': 'Changed purpose'}]]:
                with self.subTest(current=current), self.assertRaisesRegex(ValueError, 'changed after review'):
                    placement.verify_snapshot(self.item, current, path)


class PlacementGitTests(unittest.TestCase):
    setUp = fixtures.GitGuardTests.setUp
    cleanup = fixtures.GitGuardTests.cleanup
    git = fixtures.GitGuardTests.git

    def test_writer_cannot_add_a_page_and_skip_the_planned_canonical_fix(self):
        new = self.path.parent / 'new-reference.md'
        new.write_text('Correct information on a new page.\n')
        item = {**self.item, 'doc_paths': [str(self.path), str(new)],
                'placement': {'canonical_pages': [str(self.path)]}}
        with self.assertRaisesRegex(ValueError, 'canonical-page correction unchanged'):
            docs.validate_diff(item, self.base)

    def test_inventory_uses_pinned_pages_not_writer_edits(self):
        self.path.write_text('title: Model-generated title\n# New heading\n')
        inventory = placement.inventory(self.base)
        self.assertEqual([p['path'] for p in inventory], [str(self.path)])
        self.assertNotIn('New heading', inventory[0]['headings'])

    def test_overlap_evidence_contains_only_selected_documentation(self):
        self.path.write_text('Human documentation change.\n')
        Path('source.go').write_text('Secret unrelated source change.\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'human change')
        head = self.git('rev-parse', 'HEAD')
        human = pr(self.item, body='Human PR', branch='human/fix', head_sha=head)
        item = {**self.item, 'placement': {'examined_pages': [str(self.path)],
                'canonical_pages': [str(self.path)], 'new_page_reason': ''}}
        self.git('fetch', '.', 'HEAD')
        with tempfile.TemporaryDirectory() as directory, patch.object(docs, 'existing_prs', return_value=[human]), \
                patch.object(docs, 'mutate_git'):
            dest = Path(directory) / 'overlaps.json'
            placement.capture(item, 'owner/repo', self.base, dest)
            content = dest.read_text()
            self.assertIn('Human documentation change', content)
            self.assertNotIn('Secret unrelated source', content)
            self.assertNotIn('source.go', content)

    def test_dry_run_validates_but_does_not_push_or_open_pr(self):
        self.path.write_text('Corrected canonical documentation.\n')
        with patch.dict(os.environ, {'DRY_RUN': 'true'}), patch.object(docs, 'existing_prs', return_value=[]), \
                patch.object(docs, 'mutate_git', wraps=docs.mutate_git) as mutate, patch.object(docs, 'run', wraps=docs.run) as run:
            docs.publish(self.item, 'owner/repo', self.base, 'main')
        self.assertFalse(any(call.args[0] in {"push", "commit", "switch"} for call in mutate.call_args_list))
        self.assertFalse(any(call.args[:3] == ('gh', 'pr', 'create') for call in run.call_args_list))
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.base)
