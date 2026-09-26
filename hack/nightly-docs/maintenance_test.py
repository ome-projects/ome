"""Exercise maintenance policy and real Git publication without remote writes."""

import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import maintenance as m
import maintenance_checks as checks
from nightly_docs_test import proposal


def pull():
    item = m.docs.validate_item(proposal())
    return {"number": 7, "state": "open", "draft": False, "user": {"login": "github-actions[bot]"},
            "title": item["title"], "body": f"<!-- nightly-docs:{item['key']} -->",
            "head": {"sha": "b" * 40, "ref": item["branch"], "repo": {"full_name": "ome-projects/ome"}},
            "base": {"sha": "a" * 40, "ref": "main", "repo": {"full_name": "ome-projects/ome"}}}


class PolicyTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {"GITHUB_REPOSITORY": "ome-projects/ome"})
        env.start()
        self.addCleanup(env.stop)
        base = patch.object(m, 'current_base', return_value='a' * 40)
        base.start()
        self.addCleanup(base.stop)

    def test_identity_is_checked_independently_of_label(self):
        self.assertEqual(m.eligible(pull())[0], "a" * 40)
        for field, value in [("state", "closed"), ("draft", True), ("body", "docs")]:
            pr = pull()
            pr[field] = value
            with self.assertRaises(ValueError):
                m.eligible(pr)
        for who, value in [("user", {"login": "someone"}),
                           ("head", {"sha": "b" * 40, "ref": "some-branch", "repo": {"full_name": "ome-projects/ome"}}),
                           ("head", {"sha": "b" * 40, "ref": pull()["head"]["ref"], "repo": {"full_name": "fork/ome"}})]:
            pr = pull()
            pr[who] = value
            with self.assertRaises(ValueError):
                m.eligible(pr)

    def test_budget_survives_own_push_and_force_is_explicit(self):
        state = {"phase": "needs-repair", "attempts": 3, "signature": "old"}
        self.assertEqual(m.decision(state, "new-head"), "needs-human")
        self.assertEqual(m.decision(state, "new-head", True), "work")
        state = {"phase": "ready", "attempts": 0, "signature": "old"}
        self.assertEqual(m.decision(state, "old"), "cached")
        self.assertEqual(m.decision(state, "new-feedback"), "work")

    def test_waiting_approvals_cannot_starve_repairs(self):
        prs = [{**pull(), "number": number} for number in range(1, 102)]

        def feedback(pr):
            state = {"phase": "ready", "signature": "same", "attempts": 0} if pr['number'] <= 100 else {}
            return {}, state, None

        with patch.object(m.docs, 'pages', return_value=prs), \
                patch.object(m, 'feedback', side_effect=feedback), \
                patch.object(m, 'signature', return_value='same'), patch.object(m, 'output') as output:
            m.select(0, False, True)
        selected = output.call_args.kwargs['matrix']['include']
        self.assertEqual(len(selected), 100)
        self.assertEqual(selected[0], {'number': 101})

    def test_closed_pr_dispatch_is_a_noop(self):
        pr = {**pull(), 'state': 'closed'}
        with patch.object(m, 'get_pr', return_value=pr), \
                patch.object(m, 'feedback') as feedback, patch.object(m, 'output') as output:
            m.select(7, False, False)
        feedback.assert_not_called()
        self.assertEqual(output.call_args.kwargs, {'matrix': {'include': []}, 'count': '0'})

    def test_cache_pins_head_base_and_feedback(self):
        pr = pull()
        original = m.signature(pr, {"threads": []})
        for side in ["base", "head"]:
            changed = copy.deepcopy(pr)
            changed[side]["sha"] = "c" * 40
            self.assertNotEqual(m.signature(changed, {"threads": []}), original)
        self.assertNotEqual(m.signature(pr, {"threads": ["fix this"]}), original)

    def test_state_requires_bot_and_valid_budget(self):
        state = {"attempts": 2, "phase": "working", "head": "x", "base": "y", "run_url": "url"}
        comment = {"id": 99, "user": {"login": "github-actions[bot]"}, "body": m.state_body(state)}
        self.assertEqual(m.decode_state([comment]), (state, 99))
        with self.assertRaises(ValueError):
            m.decode_state([comment, comment])
        comment["user"]["login"] = "untrusted"
        self.assertEqual(m.decode_state([comment]), ({}, None))

    def test_bot_skip_notice_is_not_a_repair_request(self):
        comment = {'user': {'login': 'coderabbitai[bot]', 'type': 'Bot'},
                   'body': '<!-- This is an auto-generated comment: summarize by coderabbit.ai -->\n'
                           '<!-- This is an auto-generated comment: skip review by coderabbit.ai -->\nReview skipped'}
        self.assertFalse(m.substantive_comment(comment))
        comment['user']['login'] = 'maintainer'
        comment['author_association'] = 'COLLABORATOR'
        self.assertTrue(m.substantive_comment(comment))
        comment['user']['login'] = 'coderabbitai[bot]'
        comment['body'] = 'Fix this incorrect example'
        self.assertTrue(m.substantive_comment(comment))

    def test_untrusted_feedback_does_not_invalidate_a_successful_review(self):
        outsider = {'login': 'claude', '__typename': 'User'}
        self.assertFalse(m.trusted_feedback(outsider, 'NONE'))
        self.assertTrue(m.trusted_feedback(outsider, 'COLLABORATOR'))
        self.assertTrue(m.trusted_feedback({'login': 'claude', '__typename': 'Bot'}, 'NONE'))
        details = {'threads': [], 'unresolved_threads': [], 'protected_threads': []}
        self.assertEqual(m.signature(pull(), details), m.signature(pull(), {
            **details, 'unresolved_threads': ['external-thread'], 'protected_threads': ['T']}))

    def test_feedback_filters_outsiders_but_preserves_merge_blockers(self):
        bot = {'author': {'__typename': 'Bot', 'login': 'claude'}, 'authorAssociation': 'NONE', 'body': 'Fix link'}
        outsider = {'author': {'__typename': 'User', 'login': 'visitor'}, 'authorAssociation': 'NONE', 'body': 'Untrusted text'}
        threads = [{'id': 'T', 'isResolved': False, 'comments': {'pageInfo': {'hasNextPage': False}, 'nodes': [bot, outsider]}},
                   {'id': 'external', 'isResolved': False, 'comments': {'pageInfo': {'hasNextPage': False}, 'nodes': [outsider]}}]
        response = {'data': {'repository': {'pullRequest': {'reviewThreads': {
            'nodes': threads, 'pageInfo': {'hasNextPage': False}}}}}}
        comments = [{'id': 1, 'user': {'login': 'visitor', 'type': 'User'}, 'author_association': 'NONE', 'body': 'Spend budget'}]
        reviews = [{'id': 2, 'user': {'login': 'visitor', 'type': 'User'}, 'author_association': 'NONE',
                    'body': 'External review', 'state': 'CHANGES_REQUESTED', 'commit_id': 'b' * 40}]
        with patch.object(m.docs, 'pages', side_effect=[comments, reviews]), \
                patch.object(m, 'api', return_value=response), patch.object(m, 'check_runs', return_value=[]):
            details, _, _ = m.feedback(pull())
        self.assertEqual(details['comments'], [])
        self.assertEqual(details['reviews'], [])
        self.assertEqual(details['threads'][0]['comments'], [bot])
        self.assertEqual(details['unresolved_threads'], ['T', 'external'])
        self.assertEqual(m.checked_threads({'addressed_threads': [1]}, {'feedback': details}), [])

    def test_model_jobs_are_read_only_and_publisher_scopes_credentials(self):
        import yaml
        root = Path(__file__).resolve().parents[2]
        workflow = yaml.load((root / '.github/workflows/docs-pr-worker.yml').read_text(), Loader=yaml.BaseLoader)
        for name in ['repair', 'review']:
            self.assertEqual(workflow['jobs'][name]['permissions']['contents'], 'read')
            self.assertEqual(workflow['jobs'][name]['permissions']['pull-requests'], 'read')
        publisher = workflow['jobs']['validate-publish']
        self.assertNotIn('GH_TOKEN', publisher.get('env', {}))
        for step in publisher['steps']:
            self.assertFalse(step.get('uses', '').startswith('anthropics/'))
            if 'GH_TOKEN' in step.get('env', {}):
                self.assertIn(step['name'], ['Refresh only unrelated new documentation on main',
                                             'Publish guarded repair and record the actual PR-head check'])

    def test_reusable_sweeps_honor_apply_false_for_every_caller_event(self):
        for event in ['schedule', 'issue_comment', 'workflow_run', 'workflow_dispatch', 'push']:
            for number in ['', '1072']:
                self.assertFalse(m.apply_mode({'apply': False, 'pr_number': number}, event))
                self.assertTrue(m.apply_mode({'apply': True, 'pr_number': number}, event))
            self.assertEqual(m.apply_mode({}, event), event in {'schedule', 'issue_comment', 'workflow_run'})
        with self.assertRaises(ValueError):
            m.apply_mode({'apply': 'false'}, 'schedule')

    def test_bulk_dispatch_rejects_single_pr_options(self):
        with patch.dict(os.environ, {'EXTRA_FEEDBACK': 'Fix one thing'}), self.assertRaisesRegex(ValueError, 'PR number'):
            m.select(0, False, False)
        with patch.dict(os.environ, {'EXTRA_FEEDBACK': ''}), self.assertRaisesRegex(ValueError, 'PR number'):
            m.select(0, True, False)

    def test_bad_feedback_does_not_abort_other_prs(self):
        prs = [{**pull(), 'number': 1}, {**pull(), 'number': 2}]
        def feedback(pr):
            if pr['number'] == 1:
                raise ValueError('Feedback requires triage')
            return {}, {}, None
        with patch.object(m.docs, 'pages', return_value=prs), \
                patch.object(m, 'feedback', side_effect=feedback), patch.object(m, 'output') as output:
            m.select(0, False, False)
        self.assertEqual(output.call_args.kwargs['matrix']['include'], [{'number': 2}])

    def test_secret_content_and_oversized_status_are_handled(self):
        for text in ['sk-ant-' + 'x' * 40, 'ghs_' + 'x' * 40, 'prefix private-test-key suffix']:
            with patch.dict(os.environ, {'ANTHROPIC_API_KEY': 'private-test-key'}), self.assertRaisesRegex(ValueError, 'Credential'):
                m.reject_secrets(text)
        state = {'phase': 'needs-repair', 'attempts': 1, 'head': 'a', 'base': 'b', 'run_url': 'url',
                 'reason': '<&😀' * 100000, 'extra_feedback': '😀' * 100000}
        self.assertLess(len(m.state_body(state).encode()), 65536)

    def test_publication_waits_only_for_our_own_head(self):
        ctx = {'number': 7, 'head': 'b' * 40, 'base': 'a' * 40}
        old, new = pull(), pull()
        new['head']['sha'] = 'c' * 40
        with patch.object(m, 'get_pr', side_effect=[old, old, new]), patch.object(m.time, 'sleep') as sleep:
            self.assertEqual(m.published_pr(ctx, 'c' * 40), new)
            self.assertEqual(sleep.call_count, 2)
        with patch.object(m, 'get_pr', return_value=new), patch.object(m.time, 'sleep') as sleep:
            with self.assertRaises(ValueError):
                m.published_pr(ctx, 'd' * 40)
            sleep.assert_not_called()

    def test_prepare_scope_rejection_is_terminal_without_model_work(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'GITHUB_RUN_ID': '1'}), \
                patch.object(m, 'get_pr', return_value=pull()), \
                patch.object(m, 'context', side_effect=ValueError('Scope violation')), \
                patch.object(m.docs, 'pages', return_value=[]), patch.object(m, 'save_state') as save, \
                patch.object(m, 'record_check') as record, self.assertRaisesRegex(ValueError, 'Scope'):
            m.prepare(7, Path(directory), True, False)
        self.assertEqual(save.call_args.args[1]['phase'], 'needs-human')
        self.assertEqual(save.call_args.args[1]['attempts'], 3)
        self.assertFalse(record.call_args.args[2])

    def test_inaccurate_review_cannot_publish_even_valid_single_concern(self):
        ctx = {'number': 7, 'head': 'b' * 40, 'base': 'a' * 40, 'feedback': {'threads': []},
               'attempts': 1, 'extra_feedback': '', 'run_url': 'url'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'checks.json').write_text('[]')
            with patch.dict(os.environ, {'BUILD_OK': 'true', 'CHECKS_OK': 'true',
                    'GITHUB_STEP_SUMMARY': str(root / 'summary'),
                    'REVIEW_JSON': json.dumps({'single_concern': True, 'accurate': False, 'reason': 'Wrong behavior'})}), \
                    patch.object(m, 'live_match'), patch.object(m, 'published_pr', return_value=pull()), \
                    patch.object(m, 'publish_repair') as publish, patch.object(m, 'record_check') as record, \
                    patch.object(m, 'save_state'):
                m.finish(ctx, root, True)
            publish.assert_not_called()
            self.assertFalse(record.call_args.args[2])
            self.assertFalse(json.loads((root / 'result.json').read_text())['published'])
            self.assertIn('no repair published', (root / 'summary').read_text())
    def test_human_threads_are_never_resolved(self):
        bot = {"author": {"__typename": "Bot", "login": "claude"}, "body": "fix link"}
        human = {"author": {"__typename": "User", "login": "claude"}, "body": "also clarify"}
        ctx = {"feedback": {"threads": [{"id": "bot", "comments": [bot]},
                                         {"id": "human", "comments": [bot, human]}]}}
        self.assertEqual(m.checked_threads({"addressed_threads": [1, 2]}, ctx), ["bot"])
        for numbers in [[True], [3], [1, 1], "1"]:
            with self.assertRaises(ValueError):
                m.checked_threads({"addressed_threads": numbers}, ctx)

    def test_stale_writer_cannot_publish(self):
        pr = pull()
        details = {"threads": []}
        ctx = {"number": 7, "head": pr["head"]["sha"], "base": pr["base"]["sha"],
               "signature": m.signature(pr, details), "extra_feedback": ""}
        with patch.object(m, "api", return_value=pr), patch.object(m, "feedback", return_value=(details, {}, None)):
            self.assertEqual(m.live_match(ctx), pr)
            pr["head"]["sha"] = "c" * 40
            with self.assertRaisesRegex(ValueError, "stale"):
                m.live_match(ctx)

    def test_merge_requires_current_check_approval_and_all_ci(self):
        pr = pull()
        state = {"phase": "ready", "attempts": 0, "signature": "digest",
                 "head": pr["head"]["sha"], "base": pr["base"]["sha"]}
        info = {"reviewDecision": "APPROVED", "mergeStateStatus": "CLEAN", "unresolved": False,
                "checks": [{"status": "COMPLETED", "conclusion": "SUCCESS"}]}
        runs = [{"id": 1, "name": m.CHECK, "app": {"slug": "github-actions"},
                 "external_id": f"docs-maintenance:7:{pr['base']['sha']}", "conclusion": "success"}]
        self.assertEqual(m.merge_blockers(pr, state, "digest", info, runs), [])
        for key, value in [("reviewDecision", "REVIEW_REQUIRED"), ("mergeStateStatus", "BLOCKED"),
                           ("unresolved", True), ("checks", [{"status": "IN_PROGRESS"}]),
                           ("checks", [{"status": "COMPLETED", "conclusion": "FAILURE"}])]:
            self.assertTrue(m.merge_blockers(pr, state, "digest", {**info, key: value}, runs))
        self.assertTrue(m.merge_blockers(pr, state, "stale", info, runs))
        self.assertTrue(m.merge_blockers(pr, state, "digest", info, []))
        runs[0]["external_id"] = "stale-base"
        self.assertTrue(m.merge_blockers(pr, state, "digest", info, runs))

    def test_disabled_merge_never_calls_mutation_even_when_ready(self):
        pr = pull()
        info = {"reviewDecision": "APPROVED", "mergeStateStatus": "CLEAN", "statusCheckRollup": []}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "GITHUB_STEP_SUMMARY": str(Path(directory) / "summary")}), \
                patch.object(m, "api", return_value=pr) as api, \
                patch.object(m, "feedback", return_value=({"threads": []}, {}, None)), \
                patch.object(m.docs, "run", return_value=json.dumps(info)), \
                patch.object(m, "check_runs", return_value=[]), \
                patch.object(m, "merge_blockers", return_value=[]):
            m.merge(7, False)
            self.assertEqual(api.call_count, 1)
            self.assertEqual(api.call_args.args, ('repos/ome-projects/ome/pulls/7',))

    def test_feedback_arriving_during_publication_is_not_marked_reviewed(self):
        thread = {"id": "T", "number": 1, "comments": [{"author": {"__typename": "Bot", "login": "claude"}}]}
        original = {"threads": [thread], "comments": [], "failed_checks": []}
        fresh = copy.deepcopy(original)
        fresh['threads'][0]['comments'].append({'author': {'__typename': 'User', 'login': 'reviewer'},
                                                'body': 'New concern after publication'})
        pr = pull()
        ctx = {"number": 7, "head": "old", "base": pr['base']['sha'], "feedback": original,
               "attempts": 1, "extra_feedback": "", "run_url": "https://example.test/run"}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'checks.json').write_text('[]')
            with patch.dict(os.environ, {"BUILD_OK": "true", "CHECKS_OK": "true",
                    "REVIEW_JSON": json.dumps({"accurate": True, "single_concern": True, "reason": "Verified",
                                                "addressed_threads": [1]}),
                    "GITHUB_STEP_SUMMARY": str(root / 'summary')}), \
                    patch.object(m, 'live_match'), patch.object(m, 'publish_repair', return_value=pr['head']['sha']), \
                    patch.object(m, 'record_check'), patch.object(m, 'api', return_value=pr), \
                    patch.object(m, 'feedback', return_value=(fresh, {}, None)), \
                    patch.object(m, 'save_state') as save:
                m.finish(ctx, root, True)
            state = save.call_args.args[1]
            self.assertEqual(m.decision(state, m.signature(pr, fresh)), 'work')
            self.assertEqual(json.loads((root / 'result.json').read_text())['resolved_bot_threads'], [])

    def test_enabled_merge_uses_normal_squash_and_expected_head(self):
        pr = pull()
        details = {'threads': []}
        state = {'phase': 'ready', 'attempts': 0, 'head': pr['head']['sha'],
                 'base': pr['base']['sha'], 'signature': m.signature(pr, details)}
        info = {'reviewDecision': 'APPROVED', 'mergeStateStatus': 'CLEAN', 'statusCheckRollup': []}
        runs = [{'id': 1, 'name': m.CHECK, 'app': {'slug': 'github-actions'},
                 'external_id': f"docs-maintenance:7:{pr['base']['sha']}", 'conclusion': 'success'}]
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                'GITHUB_STEP_SUMMARY': str(Path(directory) / 'summary')}), \
                patch.object(m, 'api', side_effect=[pr, pr, {'merged': True, 'sha': 'd' * 40}]) as api, \
                patch.object(m, 'feedback', return_value=(details, state, None)), \
                patch.object(m.docs, 'run', return_value=json.dumps(info)), \
                patch.object(m, 'check_runs', return_value=runs):
            m.merge(7, True)
        self.assertEqual(api.call_args.args, ('repos/ome-projects/ome/pulls/7/merge', 'PUT',
                                             {'sha': pr['head']['sha'], 'merge_method': 'squash'}))


class FreshnessTests(unittest.TestCase):
    def test_stale_pr_base_cannot_hide_a_main_update(self):
        pr = pull()
        ctx = {"number": 7, "head": pr['head']['sha'], "base": pr['base']['sha'],
               "signature": m.signature(pr, {}), "extra_feedback": ""}
        with patch.object(m, 'repo', return_value='ome-projects/ome'), \
                patch.object(m, 'api', side_effect=[pr, {'object': {'sha': 'c' * 40}}]), \
                patch.object(m, 'feedback', return_value=({}, {}, None)):
            with self.assertRaisesRegex(ValueError, 'stale'):
                m.live_match(ctx)


class ExampleTests(unittest.TestCase):
    def test_manifest_schema_and_prefix(self):
        schema = {("ome.io/v1", "ServingRuntime"): {"type": "object", "required": ["spec"],
                    "properties": {"spec": {"type": "object", "required": ["runner"],
                       "properties": {"runner": {"type": "object", "required": ["name"]}}}}}}
        text = '[bad](/docs/tasks/)\n```yaml\napiVersion: ome.io/v1\nkind: ServingRuntime\nspec:\n  runner: {}\n```\n'
        with patch.object(checks, "schema_catalog", return_value=schema):
            findings = checks.document_findings({"guide.md": text}, Path.cwd())
            self.assertEqual(len(findings), 2)
            fixed = text.replace('/docs/', '/ome/docs/').replace('runner: {}', 'runner: {name: model}')
            self.assertEqual(checks.document_findings({"guide.md": fixed}, Path.cwd()), [])

    def test_nullable_int_or_string(self):
        from jsonschema import Draft7Validator
        validator = Draft7Validator(checks.kubernetes_schema({"x-kubernetes-int-or-string": True, "nullable": True}))
        for value in [None, 3, "3"]:
            self.assertTrue(validator.is_valid(value))
        self.assertFalse(validator.is_valid([]))

    def test_rendered_relative_links_and_fragments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = root / 'docs/tasks/guide/index.html'
            page.parent.mkdir(parents=True)
            page.write_text('<a href="../other/#exists">good</a><a href="../other/#missing">bad</a>')
            target = root / 'docs/tasks/other/index.html'
            target.parent.mkdir(parents=True)
            target.write_text('<h2 id="exists">Title</h2>')
            findings = checks.rendered_findings(['site/content/en/docs/tasks/guide.md'], root)
            self.assertEqual(len(findings), 1)
            self.assertIn('missing', findings[0])


class PublicationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.original = os.getcwd()
        self.addCleanup(os.chdir, self.original)
        self.root = Path(directory.name)
        self.remote = self.root / 'remote.git'
        subprocess.run(['git', 'init', '--bare', '-q', str(self.remote)], check=True)
        working = self.root / 'working'
        working.mkdir()
        os.chdir(working)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@users.noreply.github.com')
        self.git('remote', 'add', 'origin', str(self.remote))
        self.path = Path(proposal()['doc_paths'][0])
        self.path.parent.mkdir(parents=True)
        self.path.write_text('Original\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD')
        self.item = m.docs.validate_item(proposal(source_sha=self.base))
        self.git('checkout', '-qb', self.item['branch'])
        self.path.write_text('Unfixed PR\n')
        self.git('commit', '-qam', 'original PR')
        self.head = self.git('rev-parse', 'HEAD')
        self.git('push', '-q', 'origin', 'HEAD')
        self.git('checkout', '--detach', self.base)
        Path('source.go').write_text('package newer\n')
        self.git('add', 'source.go')
        self.git('commit', '-qm', 'advance main')
        self.base = self.git('rev-parse', 'HEAD')
        self.ctx = {'head': self.head, 'base': self.base, 'item': self.item, 'number': 7}

    def git(self, *args):
        return subprocess.check_output(['git', *args], text=True, stderr=subprocess.DEVNULL).strip()

    def test_publication_preserves_history_and_exact_reviewed_tree(self):
        self.path.write_text('Fixed PR\n\nSecond paragraph.\n')
        m.docs.validate_diff(self.item, self.base)
        tree = self.git('write-tree')
        with patch.object(m, 'live_match'):
            result = m.publish_repair(self.ctx)
        self.assertEqual(self.git('rev-parse', result + '^{tree}'), tree)
        self.git('merge-base', '--is-ancestor', self.head, result)
        self.git('merge-base', '--is-ancestor', self.base, result)
        self.assertIn('Signed-off-by:', self.git('show', '-s', '--format=%B'))
        self.assertEqual(self.path.read_text(), 'Fixed PR\n\nSecond paragraph.\n')

    def test_competing_commit_rejects_push(self):
        self.git('checkout', '--detach', self.head)
        self.path.write_text('Human repair\n')
        self.git('commit', '-qam', 'human repair')
        self.git('push', '-q', 'origin', 'HEAD:refs/heads/' + self.item['branch'])
        self.git('checkout', '--detach', self.base)
        self.path.write_text('Stale repair\n')
        m.docs.validate_diff(self.item, self.base)
        with patch.object(m, 'live_match'), self.assertRaises(subprocess.CalledProcessError):
            m.publish_repair(self.ctx)

    def test_incoming_code_and_executable_docs_are_rejected(self):
        for path, mode in [('code.py', 0o644), (str(self.path), 0o755)]:
            self.git('checkout', '--detach', self.head)
            Path(path).write_text('Untrusted content\n')
            Path(path).chmod(mode)
            self.git('add', path)
            self.git('commit', '-qm', 'invalid PR edit')
            pr = pull()
            pr['body'] = f"<!-- nightly-docs:{self.item['key']} -->"
            pr['head'].update(sha=self.git('rev-parse', 'HEAD'), ref=self.item['branch'])
            pr['base']['sha'] = self.base
            with patch.dict(os.environ, {'GITHUB_REPOSITORY': 'ome-projects/ome'}), \
                    patch.object(m.docs, 'mutate_git'), self.assertRaises(ValueError):
                m.context(pr)

    def refresh_context(self, base):
        pr = pull()
        pr['body'] = f"<!-- nightly-docs:{self.item['key']} -->"
        pr['head'].update(sha=self.head, ref=self.item['branch'])
        pr['base']['sha'] = base
        details = {'threads': [], 'comments': [], 'failed_checks': []}
        previous = {**pr, 'base': {**pr['base'], 'sha': self.base}}
        self.ctx.update(extra_feedback='', feedback=details, signature=m.signature(previous, details))
        return pr, details

    def test_refresh_allows_only_unrelated_new_pages_and_preserves_patch(self):
        added = self.path.parent / 'unrelated.md'
        added.write_text('Another concern\n')
        self.git('add', str(added))
        self.git('commit', '-qm', 'merge unrelated new doc')
        base = self.git('rev-parse', 'HEAD')
        self.git('push', '-q', 'origin', 'HEAD:refs/heads/main')
        self.git('checkout', '--detach', self.base)
        self.path.write_text('Reviewed repair\n')
        directory = self.root / 'evidence'
        directory.mkdir()
        pr, details = self.refresh_context(base)
        with patch.dict(os.environ, {'GITHUB_REPOSITORY': 'ome-projects/ome'}), \
                patch.object(m, 'get_pr', return_value=pr), \
                patch.object(m, 'feedback', return_value=(details, {}, None)):
            m.refresh_base(self.ctx, directory)
        self.assertEqual(self.ctx['review_base'], self.base)
        self.assertEqual(self.ctx['base'], base)
        self.assertEqual(self.git('rev-parse', 'HEAD'), base)
        self.assertEqual(self.path.read_text(), 'Reviewed repair\n')
        self.assertEqual(added.read_text(), 'Another concern\n')
        self.assertEqual(self.git('diff', '--cached', '--name-only', base), str(self.path))

    def test_refresh_rejects_source_or_existing_page_changes(self):
        for path in [Path('source.go'), self.path]:
            self.git('reset', '--hard', self.base)
            path.write_text('Changed on main\n')
            self.git('add', str(path))
            self.git('commit', '-qm', 'main changed existing content')
            base = self.git('rev-parse', 'HEAD')
            self.git('push', '-q', 'origin', 'HEAD:refs/heads/' + ('source' if path.suffix == '.go' else 'docs'))
            self.git('checkout', '--detach', self.base)
            self.path.write_text('Reviewed repair\n')
            pr, details = self.refresh_context(base)
            with patch.dict(os.environ, {'GITHUB_REPOSITORY': 'ome-projects/ome'}), \
                    patch.object(m, 'get_pr', return_value=pr), \
                    patch.object(m, 'feedback', return_value=(details, {}, None)), \
                    self.assertRaisesRegex(ValueError, 'fresh review'):
                m.refresh_base(self.ctx, self.root)
