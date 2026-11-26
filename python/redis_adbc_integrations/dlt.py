"""Append-only dlt destination using Arrow ingestion through Redis ADBC."""

import json
from typing import Dict, Optional

import dlt
import pyarrow as pa
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema.typing import TTableSchema

from adbc_driver_redis import dbapi

from .modes import LIVE, LIVE_SCHEMA, driver_options, resolve_mode


def _quote(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise DestinationTerminalException(
            "SQL names must be nonempty strings without NUL"
        )
    return '"' + name.replace('"', '""') + '"'


@dlt.destination(
    name="redis_adbc",
    batch_size=1000,
    loader_file_format="parquet",
    naming_convention="snake_case",
    skip_dlt_columns_and_tables=True,
    loader_parallelism_strategy="sequential",
)
def redis_adbc(
    items: pa.RecordBatch,
    table: TTableSchema,
    uri: str = dlt.secrets.value,
    schema_name: Optional[str] = None,
    db_kwargs: Optional[Dict[str, str]] = None,
    index_columns: str = "*",
    mode: Optional[str] = None,
    key_prefix: str = "dlt:",
) -> None:
    """Load dlt resources into ``schema_name`` with ADBC create/append ingestion.

    Install ``redis-adbc-integrations[dlt]``. Pass ``uri`` explicitly or configure
    ``destination.redis_adbc.uri`` as a dlt secret. ``schema_name`` is the Redis
    SQL schema, independent of dlt's dataset_name. Existing tables must accept
    the incoming columns and types; migrations are managed by the caller.

    ``mode`` defaults to ``"live"``: ``schema_name`` (default ``redis_live``) is a
    live schema, created if needed, and each new table stores its rows as
    application-owned HASHes under ``<key_prefix><schema>:<table>:``. A row whose
    values are all NULL is refused. ``index_columns`` applies when a table is
    created. A ``schema_name`` that names a managed schema fails in live mode
    rather than loading managed tables. ``mode="managed"`` loads managed tables
    (``schema_name`` defaults to ``public``).

    Only append is supported. No primary-key upsert, rollback or exactly-once
    delivery is provided. Keep dlt's local pipeline state for checkpoints and
    incremental sources. A failed batch may partially apply; retrying it can
    duplicate rows. Serialize independent pipelines writing to the same table.
    """
    if table.get("write_disposition", "append") != "append":
        raise DestinationTerminalException(
            "Redis ADBC dlt destination supports write_disposition='append' only"
        )
    mode = resolve_mode("dlt", mode, db_kwargs)
    if schema_name is None:
        schema_name = LIVE_SCHEMA if mode == LIVE else "public"
    schema = _quote(schema_name)
    _quote(table["name"])
    if not isinstance(items, pa.RecordBatch):
        raise DestinationTerminalException(
            "Redis ADBC dlt destination requires Parquet Arrow batches; "
            "keep loader_file_format='parquet' and batch_size > 0"
        )
    if items.num_rows == 0:
        return
    data = pa.Table.from_batches([items])
    options = driver_options(mode, db_kwargs)
    with dbapi.connect(uri, db_kwargs=options) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        if mode != LIVE:
            cur.adbc_statement.set_options(
                **{"adbc.redis.ingest.index_columns": index_columns}
            )
            cur.adbc_ingest(
                table["name"], data, mode="create_append", db_schema_name=schema_name
            )
            return
        cur.execute(
            "SELECT key_prefix FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            (schema_name, table["name"]),
        )
        rows = cur.fetchall()
        try:
            if rows:
                # Naming the registered prefix makes the driver refuse a managed
                # table; the registration keeps its own index policy.
                ingest_mode, prefix = "append", rows[0][0]
            else:
                ingest_mode = "create"
                prefix = f"{key_prefix}{schema_name}:{table['name']}:"
                cur.adbc_statement.set_options(
                    **{"adbc.redis.ingest.index_columns": index_columns}
                )
            cur.adbc_statement.set_options(
                **{"adbc.redis.ingest.live_source": json.dumps({"prefix": prefix})}
            )
            cur.adbc_ingest(
                table["name"], data, mode=ingest_mode, db_schema_name=schema_name
            )
        except (dbapi.ProgrammingError, dbapi.IntegrityError) as exc:
            # A refused live write (wrong target, all-NULL row) cannot succeed on
            # retry, and a retry after a partial write could duplicate rows.
            raise DestinationTerminalException(
                f"Redis refused the live load into {schema_name}.{table['name']}: "
                f"{exc}. Live mode needs a live schema and table; pass "
                "mode='managed' to load managed tables."
            ) from exc
