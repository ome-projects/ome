import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import prepare_source


class PrepareSourceTests(unittest.TestCase):
    def test_branch_tools_survive_checkout_and_doc_commit_has_only_docs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            repo.mkdir()

            def git(*args):
                return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null", *args],
                                               cwd=repo, text=True).strip()

            git("init", "-q")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            scripts = repo / "hack/nightly-docs"
            scripts.mkdir(parents=True)
            script = scripts / "prepare_source.py"
            script.write_text(Path(prepare_source.__file__).read_text())
            (scripts / "version").write_text("main tooling")
            (repo / "doc.md").write_text("main docs")
            git("add", ".")
            git("commit", "-qm", "main")
            base = git("rev-parse", "HEAD")
            git("switch", "-qc", "fix")
            (scripts / "version").write_text("fixed branch tooling")
            git("commit", "-qam", "fix tooling")
            destination = root / "trusted-tools"
            subprocess.run([os.sys.executable, str(script), base, str(destination)],
                           cwd=repo, check=True, capture_output=True)
            self.assertEqual(git("rev-parse", "HEAD"), base)
            self.assertEqual((destination / "version").read_text(), "fixed branch tooling")
            self.assertEqual((scripts / "version").read_text(), "main tooling")
            (repo / "doc.md").write_text("updated docs")
            git("add", "doc.md")
            git("commit", "-qm", "docs")
            self.assertEqual(git("rev-parse", "HEAD^"), base)
            self.assertEqual(git("diff", "--name-only", base), "doc.md")

    def test_rejects_unpinned_source(self):
        with self.assertRaisesRegex(ValueError, "full commit SHA"):
            prepare_source.prepare("main", "unused")


if __name__ == "__main__":
    unittest.main()
