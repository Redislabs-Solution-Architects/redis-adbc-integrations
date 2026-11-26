"""Driver-matched release metadata must name exactly the tested driver release."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

SCRIPT = Path(__file__).resolve().with_name("check_release.py")


def write_dist(root, version="0.0.32", requirement="adbc-driver-redis[dbapi]==0.0.32.*",
               wheel_version=None, extra_wheel=False):
    stem = f"redis_adbc_integrations-{version}"
    metadata = (f"Metadata-Version: 2.4\nName: redis-adbc-integrations\n"
                f"Version: {wheel_version or version}\n"
                f"Requires-Dist: {requirement}\n"
                "Requires-Dist: dagster>=1.13.30; extra == \"dagster\"\n")
    with zipfile.ZipFile(root / f"{stem}-py3-none-any.whl", "w") as wheel:
        wheel.writestr(f"{stem}.dist-info/METADATA", metadata)
    (root / f"{stem}.tar.gz").write_bytes(b"sdist")
    if extra_wheel:
        with zipfile.ZipFile(root / "redis_adbc_integrations-0.0.31-py3-none-any.whl", "w") as wheel:
            wheel.writestr("redis_adbc_integrations-0.0.31.dist-info/METADATA", metadata)


class CheckReleaseTests(unittest.TestCase):
    def run_check(self, version="0.0.32", **dist):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_dist(root, **dist)
            return subprocess.run([sys.executable, str(SCRIPT), str(root), version],
                                  capture_output=True, text=True)

    def test_matching_release_passes(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("require adbc-driver-redis[dbapi]==0.0.32.*", result.stdout)

    def test_floating_driver_requirement_is_rejected(self):
        result = self.run_check(requirement="adbc-driver-redis[dbapi]>=0.0.31")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("driver requirement", result.stderr)

    def test_other_driver_release_is_rejected(self):
        result = self.run_check(requirement="adbc-driver-redis[dbapi]==0.0.31.*")
        self.assertNotEqual(result.returncode, 0)

    def test_unstamped_wheel_version_is_rejected(self):
        result = self.run_check(wheel_version="0.1.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wheel version 0.1.0", result.stderr)

    def test_unexpected_extra_distribution_is_rejected(self):
        result = self.run_check(extra_wheel=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected only", result.stderr)

    def test_post_release_version_is_not_a_matched_version(self):
        result = self.run_check(version="0.0.32.post1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid driver-matched version", result.stderr)


if __name__ == "__main__":
    unittest.main()
