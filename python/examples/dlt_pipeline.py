"""Run with REDIS_URI pointing to Redis with Search; appends on each run."""

import os

import dlt
from redis_adbc_integrations.dlt import redis_adbc
from redis_adbc_integrations.modes import connect


def main():
    uri = os.environ["REDIS_URI"]
    pipeline = dlt.pipeline(
        pipeline_name="redis_orders",
        destination=redis_adbc(uri=uri, schema_name="dlt_example"),
    )
    info = pipeline.run(
        [{"id": 1, "amount": 10.5}, {"id": 2, "amount": 20.0}],
        table_name="orders",
        write_disposition="append",
    )
    info.raise_on_failed_jobs()
    # Live mode (the default) keeps the rows as HASHes under dlt:dlt_example:orders:.
    with connect(uri, "dlt") as conn, conn.cursor() as cur:
        cur.execute("SELECT id, amount FROM dlt_example.orders ORDER BY id")
        print(cur.fetch_arrow_table())


if __name__ == "__main__":
    main()
