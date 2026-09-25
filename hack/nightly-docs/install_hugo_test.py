import hashlib
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import install_hugo


class InstallHugoTests(unittest.TestCase):
    def archive(self):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz") as bundle:
            binary = b"fixture executable"
            member = tarfile.TarInfo("hugo")
            member.size = len(binary)
            bundle.addfile(member, io.BytesIO(binary))
        return output.getvalue()

    def test_installs_verified_binary_without_compiler_or_external_commands(self):
        archive = self.archive()
        for machine, arch in [("x86_64", "amd64"), ("aarch64", "arm64"), ("arm64", "arm64")]:
            with self.subTest(machine=machine), tempfile.TemporaryDirectory() as directory:
                with patch.dict(os.environ, {"PATH": ""}), \
                        patch.object(install_hugo.platform, "system", return_value="Linux"), \
                        patch.object(install_hugo.platform, "machine", return_value=machine), \
                        patch.dict(install_hugo.CHECKSUMS, {arch: hashlib.sha256(archive).hexdigest()}), \
                        patch.object(install_hugo, "urlopen", return_value=io.BytesIO(archive)) as download:
                    install_hugo.install(directory)
                self.assertIn(f"linux-{arch}.tar.gz", download.call_args.args[0])
                binary = Path(directory) / "hugo"
                self.assertEqual(binary.read_bytes(), b"fixture executable")
                self.assertEqual(binary.stat().st_mode & 0o777, 0o755)

    def test_bad_checksum_does_not_replace_existing_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "hugo"
            binary.write_bytes(b"existing binary")
            with patch.object(install_hugo.platform, "system", return_value="Linux"), \
                    patch.object(install_hugo.platform, "machine", return_value="x86_64"), \
                    patch.object(install_hugo, "urlopen", return_value=io.BytesIO(b"corrupt archive")):
                with self.assertRaisesRegex(ValueError, "checksum"):
                    install_hugo.install(directory)
            self.assertEqual(binary.read_bytes(), b"existing binary")

    def test_unsupported_runner_fails_before_download(self):
        for system, machine in [("Darwin", "arm64"), ("Linux", "s390x")]:
            with self.subTest(system=system, machine=machine), \
                    patch.object(install_hugo.platform, "system", return_value=system), \
                    patch.object(install_hugo.platform, "machine", return_value=machine), \
                    patch.object(install_hugo, "urlopen") as download:
                with self.assertRaisesRegex(ValueError, "requires a Linux"):
                    install_hugo.install("unused")
                download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
