"""Install a verified Hugo Extended binary without a runner C toolchain."""

import hashlib
import io
from pathlib import Path
import platform
import shutil
import sys
import tarfile
from urllib.request import urlopen


VERSION = "0.157.0"
# SHA-256 digests of the official gohugoio/hugo release assets.
CHECKSUMS = {
    "amd64": "5b2fdfe4646a48ee98107035024fd6640299bff31c4486c259eeeb9f4c822492",
    "arm64": "398efd55428589b928afc65df2410f97efc83608f7a3324d1435196f17adaa0a",
}


def install(destination):
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    if platform.system() != "Linux" or arch is None:
        raise ValueError("Hugo installer requires a Linux amd64 or arm64 runner")
    asset = f"hugo_extended_{VERSION}_linux-{arch}.tar.gz"
    url = f"https://github.com/gohugoio/hugo/releases/download/v{VERSION}/{asset}"
    with urlopen(url, timeout=120) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != CHECKSUMS[arch]:
        raise ValueError("Hugo archive checksum mismatch")
    destination = Path(destination)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        member = bundle.getmember("hugo")
        if not member.isfile():
            raise ValueError("Hugo archive member is not a regular file")
        destination.mkdir(parents=True, exist_ok=True)
        binary = destination / "hugo"
        with bundle.extractfile(member) as source, binary.open("wb") as target:
            shutil.copyfileobj(source, target)
        binary.chmod(0o755)


if __name__ == "__main__":
    install(sys.argv[1])
