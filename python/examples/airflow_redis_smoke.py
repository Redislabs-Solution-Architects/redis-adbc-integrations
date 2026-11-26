"""Airflow 3 TaskFlow smoke DAG using Redis ADBC directly.

Set REDIS_URI to a disposable Redis database with Search. Install the driver
and PyArrow in every Airflow worker. Only small summaries go through XCom.
Tasks use Airflow's default live mode: the fixture is a live table in
``redis_live`` whose rows are HASHes under ``airflow:<table>:``.
"""

from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task


def table_name():
    import hashlib

    # A stable name across retries, isolated from other DAG runs.
    run_id = get_current_context()["run_id"]
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:24]
    return f"airflow_adbc_smoke_{suffix}"


def drop_live(cur, table):
    # DROP TABLE only unregisters a live table; delete its HASHes first.
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'redis_live' AND table_name = ?",
        (table,),
    )
    if cur.fetchall():
        cur.execute(f"DELETE FROM redis_live.{table}")
        cur.execute(f"DROP TABLE redis_live.{table}")


@dag(
    dag_id="redis_adbc_smoke",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["redis", "adbc"],
)
def redis_adbc_smoke():
    @task
    def load():
        import json
        import os

        import pyarrow as pa
        from redis_adbc_integrations.modes import connect

        table = table_name()
        with connect(os.environ["REDIS_URI"], "airflow") as conn, conn.cursor() as cur:
            # Autocommit only. Replacing this run's fixture makes retries safe.
            drop_live(cur, table)
            cur.adbc_statement.set_options(**{
                "adbc.redis.ingest.live_source": json.dumps({"prefix": f"airflow:{table}:"}),
            })
            cur.adbc_ingest(
                table,
                pa.table({
                    "id": [1, 2, 3, 4],
                    "region": ["US", "EU", "US", "EU"],
                    "amount": [10, 20, 30, 40],
                }),
                mode="create",
                db_schema_name="redis_live",
            )
        return {"table": table, "rows": 4}

    @task
    def verify(loaded):
        import os

        from redis_adbc_integrations.modes import connect

        table = table_name()
        assert loaded == {"table": table, "rows": 4}, loaded
        # A fresh task-local connection reads the prior task's persisted data.
        with connect(os.environ["REDIS_URI"], "airflow") as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT region, COUNT(*) AS n, SUM(amount) AS total "
                f"FROM redis_live.{table} GROUP BY region ORDER BY region"
            )
            rows = cur.fetch_arrow_table().to_pylist()
            expected = [
                {"region": "EU", "n": 2, "total": 60},
                {"region": "US", "n": 2, "total": 40},
            ]
            assert rows == expected, rows
            cur.execute(f"SELECT id FROM redis_live.{table} WHERE amount > ? ORDER BY id", [20])
            assert cur.fetchall() == [(3,), (4,)]
        print(f"Redis ADBC verified: {loaded['rows']} rows; aggregates={rows}")
        return {"rows": loaded["rows"], "groups": len(rows)}

    @task(trigger_rule="all_done")
    def cleanup():
        import os

        from redis_adbc_integrations.modes import connect

        with connect(os.environ["REDIS_URI"], "airflow") as conn, conn.cursor() as cur:
            drop_live(cur, table_name())

    loaded = load()
    checked = verify(loaded)
    # Cleanup does not depend on a successful XCom result.
    [loaded, checked] >> cleanup()


dag = redis_adbc_smoke()

if __name__ == "__main__":
    run = dag.test()
    states = {ti.task_id: str(ti.state) for ti in run.get_task_instances()}
    # An all_done leaf can hide upstream failure in the overall DagRun state.
    assert states == {"load": "success", "verify": "success", "cleanup": "success"}, states
    print(f"Redis ADBC Airflow smoke passed: {states}")
