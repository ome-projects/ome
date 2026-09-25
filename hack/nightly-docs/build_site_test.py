from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import build_site


class BuildSiteTests(unittest.TestCase):
    def test_dependency_and_generated_changes_cannot_enter_source_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "site", root / "build"
            source.mkdir()
            (source / "go.mod").write_text("module example.invalid/site\n")
            (source / "page.md").write_text("documentation change\n")

            def run(command, *, cwd, check):
                self.assertEqual(Path(cwd), output)
                self.assertTrue(check)
                if command[0] == "go":
                    self.assertEqual(command[-1], "-require=github.com/google/docsy@v0.14.3")
                    (output / "go.mod").write_text("resolved theme dependencies")
                    (output / "go.sum").write_text("checksums")
                elif command[0] == "npm":
                    (output / "node_modules").mkdir()
                else:
                    self.assertTrue((output / "node_modules").is_dir())
                    self.assertEqual((output / "page.md").read_text(), "documentation change\n")
                    (output / "public").mkdir()

            with patch.object(build_site.subprocess, "run", side_effect=run):
                build_site.build(source, output, root / "hugo")
            self.assertEqual((source / "go.mod").read_text(), "module example.invalid/site\n")
            self.assertEqual(sorted(p.name for p in source.iterdir()), ["go.mod", "page.md"])
            self.assertTrue((output / "public").is_dir())


if __name__ == "__main__":
    unittest.main()
