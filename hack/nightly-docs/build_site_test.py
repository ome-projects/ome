from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import build_site


class BuildSiteTests(unittest.TestCase):
    def test_isolated_website_checks_run_before_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'website', root / 'build'
            source.mkdir()
            (source / 'package.json').write_text('{}')
            (source / 'page.md').write_text('documentation change\n')
            (source / 'node_modules').mkdir()
            (source / 'node_modules' / 'untrusted').write_text('not copied')
            commands = []

            def run(command, *, cwd, check):
                self.assertEqual(Path(cwd), output)
                self.assertTrue(check)
                self.assertFalse((output / 'node_modules' / 'untrusted').exists())
                commands.append(command)
                (output / 'package.json').write_text('isolated mutation')

            with patch.object(build_site.subprocess, 'run', side_effect=run):
                build_site.build(source, output)
            self.assertEqual(commands, [['pnpm', 'install', '--frozen-lockfile'],
                ['pnpm', 'lint'], ['pnpm', 'test'], ['pnpm', 'check'], ['pnpm', 'build']])
            self.assertEqual((source / 'package.json').read_text(), '{}')

    def test_failed_content_validation_blocks_build(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory, 'website')
            source.mkdir()
            def run(command, **kwargs):
                if command == ['pnpm', 'test']:
                    raise subprocess.CalledProcessError(1, command)
            with patch.object(build_site.subprocess, 'run', side_effect=run) as mocked:
                with self.assertRaises(subprocess.CalledProcessError):
                    build_site.build(source, Path(directory, 'build'))
                self.assertNotIn(['pnpm', 'build'], [call.args[0] for call in mocked.call_args_list])
