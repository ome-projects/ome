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

    def test_human_threads_are_never_resolved(self):
        bot = {"author": {"login": "claude[bot]"}, "body": "fix link"}
        human = {"author": {"login": "maintainer"}, "body": "also clarify"}
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
