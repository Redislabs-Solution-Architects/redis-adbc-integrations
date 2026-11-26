"""Small, owned live Redis tables for the integration guide examples.

The guides use the integrations' default live mode, so these helpers create live
HASH tables in ``redis_live``. Each table stores its rows as HASHes under its own
``guide:<table>:`` prefix; cleanup deletes those HASHes before unregistering the
table, because ``DROP TABLE`` on a live table keeps them.
"""
from contextlib import contextmanager
import json
import os
import re
from uuid import uuid4

import pyarrow as pa
from redis_adbc_integrations.modes import LIVE_SCHEMA, connect

SCHEMA = LIVE_SCHEMA


def _connect():
    return connect(os.environ["REDIS_URI"], "prefect", mode="live")


def _create(cur, name, table):
    cur.adbc_statement.set_options(**{
        "adbc.redis.ingest.live_source": json.dumps({"prefix": f"guide:{name}:"}),
    })
    return cur.adbc_ingest(name, table, mode="create", db_schema_name=SCHEMA)


def _remove(cur, name):
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        (SCHEMA, name),
    )
    if cur.fetchall():
        cur.execute(f'DELETE FROM {SCHEMA}."{name}"')
        cur.execute(f'DROP TABLE {SCHEMA}."{name}"')


@contextmanager
def example_table():
    """Yield the name of a three-row live table in ``redis_live``."""
    name = "guide_" + uuid4().hex
    rows = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "label": pa.array(["alpha", None, "雪"], type=pa.string()),
        "amount": pa.array([2.5, None, 7.5], type=pa.float64()),
        "active": pa.array([True, None, False], type=pa.bool_()),
    })
    with _connect() as conn, conn.cursor() as cur:
        try:
            _create(cur, name, rows)
            yield name
        finally:
            _remove(cur, name)


def read_rows(name, minimum=1):
    if not re.fullmatch(r"guide_[0-9a-f]{32}", name):
        raise ValueError("expected an example-owned table name")
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(f'SELECT * FROM {SCHEMA}."{name}" WHERE id >= ? ORDER BY id', (minimum,))
        return cur.fetch_arrow_table()


def write_result(table):
    """Write a tool's Arrow output and verify it through a fresh connection.

    This local example sink fully materializes the output into a live table.
    Each invocation owns its table and cleans it up, including after a failed
    ingest or read. It is not a transactional, distributed, or retry-safe
    production sink.
    """
    if not isinstance(table, pa.Table) or not table.num_columns:
        raise ValueError("expected an Arrow table with at least one column")
    if "guide_row_order" in table.column_names:
        raise ValueError("guide_row_order is reserved for example row ordering")
    name = "guide_output_" + uuid4().hex
    columns = ", ".join('"' + column.replace('"', '""') + '"' for column in table.column_names)
    ordered = table.append_column("guide_row_order", pa.array(range(table.num_rows), type=pa.int64()))
    with _connect() as conn, conn.cursor() as cur:
        try:
            count = _create(cur, name, ordered)
            assert count == table.num_rows, (count, table.num_rows)
            with _connect() as reader, reader.cursor() as read:
                read.execute(f'SELECT {columns} FROM {SCHEMA}."{name}" ORDER BY guide_row_order')
                result = read.fetch_arrow_table()
                assert result.equals(table, check_metadata=False), (result, table)
            print(f"REDIS_ADBC_READ_WRITE_OK rows={table.num_rows}", flush=True)
            return result
        finally:
            _remove(cur, name)
