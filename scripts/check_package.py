"""Reject accidental driver vendoring or misplaced framework entry points."""
import email
from pathlib import Path
import sys
import zipfile

for name in sys.argv[1:]:
    with zipfile.ZipFile(name) as wheel:
        files = wheel.namelist()
        assert not any(p.startswith(("adbc_driver_redis/", "adbc_driver_rediss/")) for p in files)
        assert not any(p.endswith((".so", ".dylib", ".dll")) for p in files)
        assert all(f"redis_adbc_integrations/{module}.py" in files
                   for module in ("airflow", "airflow_provider", "dagster", "dask", "ray", "dlt", "superset"))
        metadata = email.message_from_bytes(wheel.read(next(p for p in files if p.endswith("/METADATA"))))
        assert metadata["Name"] == "redis-adbc-integrations"
        assert any(r.startswith("adbc-driver-redis") for r in metadata.get_all("Requires-Dist", []))
        entries = wheel.read(next(p for p in files if p.endswith("/entry_points.txt"))).decode()
        assert "redis_adbc_integrations.airflow_provider" in entries
        assert "redis_adbc_integrations.superset" in entries
    print(Path(name).name + ": independent package verified")
