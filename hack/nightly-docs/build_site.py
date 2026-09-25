"""Build an isolated documentation copy with a Hugo-compatible theme."""

from pathlib import Path
import shutil
import subprocess
import sys


def build(source, destination, hugo):
    shutil.copytree(source, destination)
    # site/go.mod has no theme requirement. A floating latest Docsy can change
    # module layout or require a newer Hugo/Dart Sass without any OME change.
    subprocess.run(["go", "mod", "edit", "-require=github.com/google/docsy@v0.14.3"],
                   cwd=destination, check=True)
    subprocess.run(["npm", "ci"], cwd=destination, check=True)
    subprocess.run([str(Path(hugo).resolve()), "--gc", "--minify", "--enableGitInfo=false"],
                   cwd=destination, check=True)


if __name__ == "__main__":
    build(*sys.argv[1:])
