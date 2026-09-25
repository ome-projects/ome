import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import nightly_docs as docs


def proposal(**changes):
    item = {"area": "rollouts", "concern": "wait-timeout", "source_sha": "a" * 40,
            "title": "[Docs] Explain rollout wait timeout", "question": "How long does rollout wait?",
            "evidence": "pkg/controller: timeout default differs from the guide.",
            "doc_paths": [docs.DOC_ROOT + "tasks/rollouts.md"]}
    return {**item, **changes}


def pr(item, state="open", **changes):
    return {"number": 1, "title": item["title"],
            "body": f'{docs.MARKER}{item["key"]} -->', "state": state,
            "merged": False, "branch": item["branch"], "files": item["doc_paths"], **changes}


class PlanningTests(unittest.TestCase):
    def context(self, prs=None):
        return {"code_history": ["a" * 40 + " 2020-01-01 old code change"],
                "existing_prs": prs or []}

    def plan(self, items, context=None):
        return docs.plan(json.dumps({"concerns": items}), context or self.context())

    def test_old_backlog_is_eligible_and_key_is_stable(self):
        first = self.plan([proposal()])[0]
        renamed = self.plan([proposal(title="[Docs] Another title")])[0]
        self.assertEqual(first["branch"], renamed["branch"])
        self.assertEqual(len(self.plan([proposal()])), 1)

    def test_empty_plan_is_valid(self):
        self.assertEqual(self.plan([]), [])

    def test_accepts_one_hundred_independent_concerns(self):
        items = [proposal(concern=f"concern-{i}",
                          doc_paths=[docs.DOC_ROOT + f"tasks/concern-{i}.md"])
                 for i in range(100)]
        self.assertEqual(len(self.plan(items)), 100)

    def test_rejects_more_than_one_hundred_concerns(self):
        with self.assertRaisesRegex(ValueError, "nightly PR limit"):
            self.plan([proposal()] * 101)

    def test_rejects_unknown_source(self):
        with self.assertRaisesRegex(ValueError, "history"):
            self.plan([proposal(source_sha="b" * 40)])

    def test_same_commit_can_have_separate_nonoverlapping_concerns(self):
        second = proposal(concern="rollback", doc_paths=[docs.DOC_ROOT + "tasks/rollback.md"])
        items = self.plan([proposal(), second])
        self.assertEqual(len(items), 2)
        self.assertNotEqual(items[0]["branch"], items[1]["branch"])

    def test_no_two_items_can_edit_same_page(self):
        with self.assertRaisesRegex(ValueError, "overlap"):
            self.plan([proposal(), proposal(concern="rollback")])

    def test_open_merged_and_declined_proposals_are_not_recreated(self):
        item = docs.validate_item(proposal())
        for state, merged in [("open", False), ("closed", False), ("closed", True)]:
            with self.subTest(state=state, merged=merged):
                self.assertEqual(self.plan([proposal()], self.context([pr(item, state, merged=merged)])), [])

    def test_open_human_pr_reserves_paths_without_marker(self):
        item = docs.validate_item(proposal())
        human = pr(item, body="Manual docs fix", branch="human/fix")
        self.assertEqual(self.plan([proposal()], self.context([human])), [])
        human["state"] = "closed"
        self.assertEqual(len(self.plan([proposal()], self.context([human]))), 1)

    def test_disallowed_paths_and_slugs(self):
        for path in ["README.md", docs.GENERATED, docs.DOC_ROOT + "../outside.md",
                     docs.DOC_ROOT + "tasks/x.yaml", "/tmp/test.md"]:
            with self.subTest(path=path), self.assertRaises(ValueError):
                docs.validate_item(proposal(doc_paths=[path]))
        for slug in ["../bad", "rollout;cmd", "UPPER", "a" * 65]:
            with self.subTest(slug=slug), self.assertRaises(ValueError):
                docs.validate_item(proposal(concern=slug))

    def test_title_must_be_one_printable_nonempty_line(self):
        for title in ["[Docs] ", "[Docs]   ", "[Docs] x\ny", "[Docs] x\r", "[Docs] x\t",
                      "[Docs] x\x00", "[Docs] x\x1b", "[Docs] " + "x" * 114]:
            with self.subTest(title=title), self.assertRaisesRegex(ValueError, "single-line"):
                docs.validate_item(proposal(title=title))
        docs.validate_item(proposal(title="[Docs] Explain café model names"))

    def test_fail_closed_on_review_rejection_or_malformed_output(self):
        for value in [{}, {"single_concern": True, "accurate": False},
                      {"single_concern": "true", "accurate": True}]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                docs.review_passes(json.dumps(value))
        docs.review_passes('{"single_concern":true,"accurate":true,"reason":"verified"}')


class GitGuardTests(unittest.TestCase):
    def setUp(self):
        self.original = os.getcwd()
        self.temp = tempfile.TemporaryDirectory()
        os.chdir(self.temp.name)
        self.addCleanup(self.cleanup)
        self.git("init", "-q")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@users.noreply.github.com")
        self.path = Path(proposal()["doc_paths"][0])
        self.path.parent.mkdir(parents=True)
        self.path.write_text("Original documentation.\n")
        Path("source.go").write_text("package example\n")
        self.git("add", ".")
        self.git("commit", "-qm", "baseline")
        self.base = self.git("rev-parse", "HEAD")
        self.item = docs.validate_item(proposal(source_sha=self.base))

    def cleanup(self):
        os.chdir(self.original)
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()

    def test_noop_does_not_publish(self):
        self.assertFalse(docs.validate_diff(self.item, self.base))

    def test_valid_edit_and_new_page_are_staged(self):
        self.path.write_text("Updated documentation.\n")
        new = self.path.parent / "example.md"
        new.write_text("A related example.\n")
        self.item["doc_paths"].append(str(new))
        self.assertTrue(docs.validate_diff(self.item, self.base))
        self.assertEqual(set(self.git("diff", "--cached", "--name-only").splitlines()),
                         {str(self.path), str(new)})

    def test_many_documentation_files_for_one_concern_are_allowed(self):
        paths = []
        for i in range(20):
            path = self.path.parent / f"related-{i}.md"
            path.write_text("Related documentation.\n")
            paths.append(str(path))
        item = docs.validate_item(proposal(source_sha=self.base, doc_paths=paths))
        self.assertTrue(docs.validate_diff(item, self.base))
        self.assertEqual(len(self.git("diff", "--cached", "--name-only").splitlines()), 20)

    def test_catches_code_changes_even_if_staged(self):
        self.path.write_text("Updated documentation.\n")
        Path("source.go").write_text("package changed\n")
        self.git("add", "source.go")
        with self.assertRaisesRegex(ValueError, "allowlist"):
            docs.validate_diff(self.item, self.base)

    def test_catches_untracked_files_outside_plan(self):
        Path("unexpected.md").write_text("Unrelated\n")
        with self.assertRaisesRegex(ValueError, "allowlist"):
            docs.validate_diff(self.item, self.base)

    def test_rejects_deletions_symlinks_and_executable_docs(self):
        self.path.unlink()
        with self.assertRaisesRegex(ValueError, "Deleted"):
            docs.validate_diff(self.item, self.base)
        self.path.symlink_to(Path.cwd() / "source.go")
        with self.assertRaisesRegex(ValueError, "symbolic"):
            docs.validate_diff(self.item, self.base)
        self.path.unlink()
        self.path.write_text("Updated\n")
        self.path.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "executable"):
            docs.validate_diff(self.item, self.base)

    def test_strictly_under_one_thousand_changed_lines_boundary(self):
        self.path.write_text("\n".join(str(i) for i in range(998)) + "\n")
        self.assertTrue(docs.validate_diff(self.item, self.base)) # 998 additions + 1 deletion
        self.path.write_text("\n".join(str(i) for i in range(999)) + "\n")
        with self.assertRaisesRegex(ValueError, "under 1000 changed lines"):
            docs.validate_diff(self.item, self.base)

    def test_writer_cannot_hide_edits_in_commit(self):
        self.path.write_text("Updated\n")
        self.git("add", ".")
        self.git("commit", "-qm", "unexpected commit")
        with self.assertRaisesRegex(ValueError, "must not commit"):
            docs.validate_diff(self.item, self.base)

    def test_publish_rechecks_open_prs_before_any_push(self):
        self.path.write_text("Updated\n")
        with patch.object(docs, "existing_prs", return_value=[pr(self.item)]), \
                patch.object(docs, "run") as remote:
            docs.publish(self.item, "test/repo", self.base, "main")
            remote.assert_not_called()

    def test_rejected_scope_never_pushes(self):
        self.origin()
        Path("source.go").write_text("package changed\n")
        with patch.object(docs, "existing_prs", return_value=[]):
            with self.assertRaisesRegex(ValueError, "allowlist"):
                docs.publish(self.item, "test/repo", self.base, "main")
        self.assertEqual(self.git("ls-remote", "--heads", "origin"), "")

    def bundle(self, files, **changes):
        return json.dumps({"base_sha": self.base, "key": self.item["key"], "files": files, **changes})

    def test_bundle_roundtrip_revalidates_in_clean_tree(self):
        self.path.write_text("Updated documentation.\n")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "bundle.json"
            docs.export_bundle(self.item, self.base, output)
            self.git("reset", "--hard", self.base)
            self.assertTrue(docs.import_bundle(self.item, self.base, output.read_text()))
        self.assertEqual(self.path.read_text(), "Updated documentation.\n")

    def test_planner_gets_source_patches_without_shell_tools(self):
        Path("pkg").mkdir()
        Path("pkg/example.go").write_text("package example\n")
        self.git("add", "pkg/example.go")
        self.git("commit", "-qm", "Add example")
        source = self.git("rev-parse", "HEAD")
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(docs, "existing_prs", return_value=[]):
            output = Path(directory) / "context.json"
            docs.prepare("test/repo", output)
            context = json.loads(output.read_text())
            self.assertEqual(context["base_sha"], source)
            self.assertEqual(context["max_prs"], 100)
            self.assertTrue(any(line.startswith(source) for line in context["code_history"]))
            patch_file = Path(context["source_diffs"]) / f"{source}.patch"
            self.assertIn("+package example", patch_file.read_text())

    def test_bundle_cannot_replace_guard_or_install_git_hook(self):
        for path in ["hack/nightly-docs/nightly_docs.py", ".git/hooks/pre-push",
                     ".git/config", docs.DOC_ROOT + "../../../../.git/config"]:
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "allowlist"):
                docs.import_bundle(self.item, self.base, self.bundle({str(self.path): "Changed\n", path: "payload"}))
            self.assertEqual(self.path.read_text(), "Original documentation.\n")

    def test_bundle_cannot_reuse_other_concern_or_base(self):
        for changes in [{"base_sha": "b" * 40}, {"key": "other-concern"}]:
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "does not match"):
                docs.import_bundle(self.item, self.base, self.bundle({}, **changes))

    def test_bundle_cannot_bypass_line_guard_or_supply_binary(self):
        with self.assertRaisesRegex(ValueError, "under 1000"):
            docs.import_bundle(self.item, self.base, self.bundle({str(self.path): "new\n" * 999}))
        with self.assertRaisesRegex(ValueError, "Invalid"):
            docs.import_bundle(self.item, self.base, self.bundle({str(self.path): "\x00"}))

    def test_git_publication_never_executes_hooks(self):
        self.origin()
        marker = Path(self.temp.name) / "hook-ran"
        for name in ["pre-commit", "post-checkout", "pre-push"]:
            hook = Path(".git/hooks") / name
            hook.write_text(f'#!/bin/sh\ntouch "{marker}"\nexit 1\n')
            hook.chmod(0o755)
        self.path.write_text("Updated documentation.\n")
        calls = []
        with patch.object(docs, "existing_prs", return_value=[]), \
                patch.object(docs, "run", side_effect=self.publisher_commands(calls)):
            docs.publish(self.item, "test/repo", self.base, "main")
        self.assertFalse(marker.exists())
        self.assertEqual(len(calls), 1)

    def origin(self):
        remote = tempfile.TemporaryDirectory()
        self.addCleanup(remote.cleanup)
        subprocess.run(["git", "init", "--bare", "-q", remote.name], check=True)
        self.git("remote", "add", "origin", remote.name)

    def publisher_commands(self, calls):
        original = docs.run

        def fake_gh(*args):
            if args[:3] == ("gh", "pr", "create"):
                body = Path(args[args.index("--body-file") + 1]).read_text()
                calls.append((args, body))
                return "https://github.com/test/repo/pull/1"
            return original(*args)

        return fake_gh

    def test_publish_makes_one_signed_off_docs_commit_and_one_pr(self):
        self.origin()
        self.path.write_text("Updated documentation.\n")
        calls = []
        with patch.object(docs, "existing_prs", return_value=[]), \
                patch.object(docs, "run", side_effect=self.publisher_commands(calls)):
            docs.publish(self.item, "test/repo", self.base, "main")
        self.assertEqual(self.git("rev-parse", "HEAD^"), self.base)
        self.assertEqual(self.git("diff", "--name-only", self.base), str(self.path))
        self.assertIn("Signed-off-by: github-actions[bot]", self.git("log", "-1", "--format=%B"))
        self.assertEqual(len(calls), 1)
        self.assertIn(self.item["key"], calls[0][1])
        self.assertIn(self.item["source_sha"], calls[0][1])

    def test_retry_recovers_matching_branch_without_rewriting_it(self):
        self.origin()
        self.path.write_text("Updated documentation.\n")
        calls = []
        with patch.object(docs, "existing_prs", return_value=[]), \
                patch.object(docs, "run", side_effect=self.publisher_commands(calls)):
            docs.publish(self.item, "test/repo", self.base, "main")
        published = self.git("rev-parse", "HEAD")
        self.git("switch", "--detach", self.base)
        self.path.write_text("Updated documentation.\n")
        calls.clear()
        with patch.object(docs, "existing_prs", return_value=[]), \
                patch.object(docs, "run", side_effect=self.publisher_commands(calls)):
            docs.publish(self.item, "test/repo", self.base, "main")
        self.assertEqual(len(calls), 1)
        self.assertTrue(self.git("ls-remote", "origin", self.item["branch"]).startswith(published))
        self.assertEqual(self.git("rev-parse", "HEAD"), self.base)

    def test_retry_never_overwrites_a_different_existing_branch(self):
        self.origin()
        self.git("push", "origin", f'HEAD:refs/heads/{self.item["branch"]}')
        self.path.write_text("Updated documentation.\n")
        calls = []
        with patch.object(docs, "existing_prs", return_value=[]), \
                patch.object(docs, "run", side_effect=self.publisher_commands(calls)):
            with self.assertRaisesRegex(ValueError, "differs"):
                docs.publish(self.item, "test/repo", self.base, "main")
        self.assertEqual(calls, [])
        self.assertTrue(self.git("ls-remote", "origin", self.item["branch"]).startswith(self.base))


if __name__ == "__main__":
    unittest.main()
