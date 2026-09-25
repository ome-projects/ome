"""Keep workflow-revision tooling while documenting a pinned main revision."""

from pathlib import Path
import re
import shutil
import subprocess
import sys


def prepare(source_sha, destination):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Documentation source must be a full commit SHA")
    subprocess.run(["git", "cat-file", "-e", f"{source_sha}^{{commit}}"], check=True)
    shutil.copytree(Path(__file__).resolve().parent, destination)
    subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "checkout", "--detach", source_sha], check=True)


if __name__ == "__main__":
    prepare(sys.argv[1], sys.argv[2])
