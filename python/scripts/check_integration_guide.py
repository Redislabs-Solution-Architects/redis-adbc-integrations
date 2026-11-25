"""Execute the extracted framework guides against an owned verified fixture."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

GUIDES = ("airflow", "dagster", "dask", "ray", "prefect", "dlt")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=(*GUIDES, "superset"))
    parser.add_argument("--require-read-write", action="store_true")
    args = parser.parse_args()
    if args.tool == "superset":
        parser.error("Superset 6.1 is unsupported on Python 3.14+; see docs/integrations/superset.md")
    if not os.environ.get("REDIS_URI"):
        parser.error("set REDIS_URI using the verified Software setup runner")
    if args.require_read_write:
        result = subprocess.run([sys.executable, __file__, args.tool], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
        print(result.stdout, end="", flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)
        if "REDIS_ADBC_READ_WRITE_OK" not in result.stdout:
            raise RuntimeError("guide passed without output write/read-back evidence")
        return
    root = Path(__file__).resolve().parents[2]
    env = {**os.environ}
    env["PYTHONPATH"] = os.pathsep.join((str(root / "python/examples"), str(root / "python"), env.get("PYTHONPATH", "")))
    env["REDIS_ADBC_REPO"] = str(root)
    guide = (root / "docs/integrations" / f"{args.tool}.md").read_text()
    with tempfile.TemporaryDirectory(prefix="redis-framework-guide-") as directory:
        work = Path(directory)
        if args.tool in {"dagster", "dask"}:
            filename = "dagster_smoke.py" if args.tool == "dagster" else "dask_read.py"
            command = [sys.executable, str(root / "python/examples" / filename)]
        else:
            code = "\n\n".join(re.findall(r"```python\n(.*?)```", guide, re.DOTALL))
            if not code:
                raise ValueError("guide has no Python examples")
            if args.tool == "airflow":
                env["AIRFLOW_CONN_REDIS_ANALYTICS"] = json.dumps({"conn_type": "redis_adbc", "extra": {"db_kwargs": {"uri": env["REDIS_URI"]}}})
                env["AIRFLOW_HOME"] = str(work / "airflow")
                code += "\nassert query.execute(context={}) == [(42,)]\n"
            example = work / "example.py"
            example.write_text(code)
            command = [sys.executable, str(example)]
        subprocess.run(command, cwd=work, env=env, check=True, timeout=500)
        if args.tool == "airflow":
            env["RUN_AIRFLOW_TESTS"] = "1"
            subprocess.run([sys.executable, "-m", "pytest", "-q", str(root / "python/tests/test_airflow.py")], cwd=work, env=env, check=True, timeout=500)
    print(json.dumps({"guide": args.tool, "status": "passed"}))


if __name__ == "__main__":
    main()
