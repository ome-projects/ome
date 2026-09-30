"""Validate and build an isolated SvelteKit website copy."""

from pathlib import Path
import shutil
import subprocess
import sys


def build(source, destination):
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns(
        'node_modules', '.svelte-kit', '.wrangler', 'dist', 'build'))
    # Only dependencies/scripts from the trusted base run here. Imported nav.ts
    # is restricted to literals before any website tooling can load it.
    for arguments in [('install', '--frozen-lockfile'), ('lint',), ('test',), ('check',), ('build',)]:
        subprocess.run(['pnpm', *arguments], cwd=Path(destination), check=True)


if __name__ == "__main__":
    build(*sys.argv[1:])
