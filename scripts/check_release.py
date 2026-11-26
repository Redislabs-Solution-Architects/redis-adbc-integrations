"""Check a driver-matched release: one wheel and sdist at the driver's version,
requiring exactly that driver release (any .postN revision of it)."""
import email
from pathlib import Path
import re
import sys
import zipfile


def check(dist, version):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise SystemExit(f"invalid driver-matched version: {version}")
    dist = Path(dist)
    stem = f"redis_adbc_integrations-{version}"
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if [w.name for w in wheels] != [f"{stem}-py3-none-any.whl"]:
        raise SystemExit(f"expected only {stem}-py3-none-any.whl, found {[w.name for w in wheels]}")
    if [s.name for s in sdists] != [f"{stem}.tar.gz"]:
        raise SystemExit(f"expected only {stem}.tar.gz, found {[s.name for s in sdists]}")
    with zipfile.ZipFile(wheels[0]) as wheel:
        metadata = email.message_from_bytes(
            wheel.read(next(p for p in wheel.namelist() if p.endswith(".dist-info/METADATA"))))
    if metadata["Version"] != version:
        raise SystemExit(f"wheel version {metadata['Version']}, expected {version}")
    driver = [r for r in metadata.get_all("Requires-Dist", [])
              if re.match(r"adbc-driver-redis\b", r)]
    expected = f"adbc-driver-redis[dbapi]=={version}.*"
    if driver != [expected]:
        raise SystemExit(f"driver requirement {driver}, expected [{expected!r}]")
    print(f"{wheels[0].name}, {sdists[0].name}: require {expected}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: check_release.py DIST_DIR VERSION")
    check(sys.argv[1], sys.argv[2])
