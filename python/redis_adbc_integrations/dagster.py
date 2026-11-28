"""Dagster asset storage over Arrow; writes are not transactional."""

import json
import re
import uuid

import pyarrow as pa
from dagster import ConfigurableIOManager, TableColumn, TableSchema
from pydantic import Field

from adbc_driver_redis import dbapi

from .modes import LIVE, LIVE_SCHEMA, driver_options, resolve_mode


def _quote(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("SQL names must be nonempty strings without NUL")
    return '"' + name.replace('"', '""') + '"'


def _asset_name(parts, *, reserved=()):
    """Encode paths injectively, keeping ordinary single identifiers readable."""
    if not parts:
        raise ValueError("Asset paths must not be empty")
    for part in parts:
        _quote(part)  # Validate before encoding invalid SQL names into valid ones.
    if (
        len(parts) == 1
        and re.fullmatch(r"[a-z_][a-z0-9_]*", parts[0])
        and not parts[0].startswith("dagster__")
        and parts[0] not in reserved
    ):
        return parts[0]
    # Hex cannot contain the separator. Reserve the marker even for literal
    # components, so an encoded path cannot alias an unencoded name.
    return "dagster__" + "_".join(part.encode("utf-8").hex() for part in parts)


def _arrow(obj):
    if isinstance(obj, (pa.Table, pa.RecordBatchReader)):
        return obj
    if type(obj).__module__.split(".")[0] == "pandas":
        return pa.Table.from_pandas(obj, preserve_index=False)
    if type(obj).__module__.split(".")[0] == "polars":
        return obj.to_arrow()
    raise TypeError(
        "Redis IO manager accepts Arrow tables/readers, pandas or polars DataFrames"
    )


class RedisADBCIOManager(ConfigurableIOManager):
    """Persist asset outputs as tables; serialize writes to the same asset.

    ``mode`` defaults to ``"live"``: assets are live HASH tables in live schemas
    (``redis_live`` for unprefixed assets), stored as application-owned HASHes
    under ``key_prefix``. Pass ``mode="managed"`` for managed tables (``public``
    for unprefixed assets).

    Unpartitioned writes stage a new table before replacing the old one (a
    brief missing-table window). A failed stage write preserves the old
    version. In live mode the old rows are deleted before the swap, so a
    failure after that point can leave the asset empty or partial; the error
    then names the staging table that holds the new rows. Each live
    materialization gets a fresh key prefix. Partition writes DELETE then
    append; retries replace the partition. A failed partition append may leave
    partial rows. No rollback or concurrent-writer isolation is promised.
    """

    uri: str
    db_kwargs: dict[str, str] = Field(default_factory=dict)
    schema_name: str | None = None
    index_columns: str = "*"
    mode: str | None = None
    key_prefix: str = "dagster:"

    def _mode(self):
        return resolve_mode("dagster", self.mode, self.db_kwargs)

    def _connect(self):
        return dbapi.connect(
            self.uri, db_kwargs=driver_options(self._mode(), self.db_kwargs)
        )

    def _table(self, context):
        parts = context.asset_key.path
        if self.schema_name:
            return self.schema_name.lower(), _asset_name(parts)
        if self._mode() == LIVE:
            default, reserved = LIVE_SCHEMA, ("public", LIVE_SCHEMA)
        else:
            default, reserved = "public", ("public",)
        schema = (
            _asset_name(parts[:-1], reserved=reserved) if len(parts) > 1 else default
        )
        return schema, _asset_name(parts[-1:])

    def _partition(self, context):
        if not context.has_asset_partitions:
            return "", ()
        column = context.definition_metadata.get("partition_expr")
        if not isinstance(column, str):
            raise TypeError(
                "Partitioned assets require metadata partition_expr naming one column"
            )
        col = _quote(column)
        from dagster import MultiPartitionsDefinition, TimeWindowPartitionsDefinition

        if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
            raise TypeError("Multi-dimensional partitions are unsupported")
        if isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
            window = context.asset_partitions_time_window
            return f"{col} >= ? AND {col} < ?", (window.start, window.end)
        keys = list(context.asset_partition_keys)
        if not all(isinstance(key, str) for key in keys):
            raise ValueError(
                "Only static/dynamic string or time-window partitions are supported"
            )
        if not keys:
            return "FALSE", ()
        return f"{col} IN ({','.join('?' for _ in keys)})", tuple(keys)

    def handle_output(self, context, obj):
        data = _arrow(obj)
        schema, table = self._table(context)
        predicate, params = self._partition(context)
        mode = self._mode()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(schema)}")
            if mode == LIVE:
                count, prefix = self._write_live(cur, schema, table, data, predicate, params)
            else:
                count = self._write_managed(cur, schema, table, data, predicate, params)
        metadata = {
            "row_count": count,
            "table": f"{schema}.{table}",
            "mode": mode,
            "dagster/column_schema": TableSchema(
                columns=[
                    TableColumn(name=f.name, type=str(f.type)) for f in data.schema
                ]
            ),
        }
        if mode == LIVE:
            metadata["key_prefix"] = prefix
        context.add_output_metadata(metadata)

    def _write_managed(self, cur, schema, table, data, predicate, params):
        target = f"{_quote(schema)}.{_quote(table)}"
        cur.adbc_statement.set_options(
            **{"adbc.redis.ingest.index_columns": self.index_columns}
        )
        if predicate:
            # Missing table is expected on first materialization only.
            if _key_prefix(cur, schema, table) is not None:
                cur.execute(f"DELETE FROM {target} WHERE {predicate}", params or None)
            return cur.adbc_ingest(table, data, mode="create_append", db_schema_name=schema)
        stage = f"{table}__dagster_{uuid.uuid4().hex}"
        staged = f"{_quote(schema)}.{_quote(stage)}"
        try:
            count = cur.adbc_ingest(stage, data, mode="create", db_schema_name=schema)
            cur.execute(f"DROP TABLE IF EXISTS {target}")
            cur.execute(f"ALTER TABLE {staged} RENAME TO {_quote(table)}")
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {staged}")
        return count

    def _write_live(self, cur, schema, table, data, predicate, params):
        target = f"{_quote(schema)}.{_quote(table)}"
        existing = _key_prefix(cur, schema, table)
        if predicate:
            if existing is not None:
                # Naming the registered prefix makes the driver refuse a managed
                # table; the zero-row probe also checks the columns before DELETE.
                try:
                    _use_live_source(cur, existing)
                    cur.adbc_ingest(
                        table, data.schema.empty_table(), mode="append",
                        db_schema_name=schema,
                    )
                except dbapi.ProgrammingError as exc:
                    exc.add_note(
                        f"{schema}.{table} was not changed. It is not a compatible "
                        "live table; set mode='managed' to write managed tables."
                    )
                    raise
                cur.execute(f"DELETE FROM {target} WHERE {predicate}", params or None)
                count = cur.adbc_ingest(table, data, mode="append", db_schema_name=schema)
                return count, existing
            prefix = self._create_live(cur, schema, table)
            return cur.adbc_ingest(table, data, mode="create", db_schema_name=schema), prefix
        stage = f"{table}__dagster_{uuid.uuid4().hex}"
        staged = f"{_quote(schema)}.{_quote(stage)}"
        prefix = self._create_live(cur, schema, table)
        try:
            count = cur.adbc_ingest(stage, data, mode="create", db_schema_name=schema)
        except BaseException as exc:
            _discard_live(cur, staged, exc)
            raise
        try:
            if existing is not None:
                # DROP TABLE only unregisters a live table; delete its HASHes first.
                cur.execute(f"DELETE FROM {target}")
                cur.execute(f"DROP TABLE {target}")
            cur.execute(f"ALTER TABLE {staged} RENAME TO {_quote(table)}")
        except BaseException as exc:
            exc.add_note(
                f"The new rows remain in live table {schema}.{stage} under key "
                f"prefix {prefix!r}; {schema}.{table} may be missing or partial."
            )
            raise
        return count, prefix

    def _create_live(self, cur, schema, table):
        """Configure the cursor to create a live table under a fresh prefix."""
        prefix = f"{self.key_prefix}{schema}:{table}:{uuid.uuid4().hex}:"
        cur.adbc_statement.set_options(
            **{"adbc.redis.ingest.index_columns": self.index_columns}
        )
        _use_live_source(cur, prefix)
        return prefix

    def load_input(self, context):
        schema, table = self._table(context)
        columns = context.definition_metadata.get("columns")
        if columns is not None and (
            not isinstance(columns, (list, tuple)) or not columns
        ):
            raise ValueError("columns metadata must be a nonempty list of column names")
        projection = ", ".join(_quote(c) for c in columns) if columns else "*"
        # Partition expression belongs to the producing asset's metadata.
        partition_context = context
        if (
            context.has_asset_partitions
            and "partition_expr" not in context.definition_metadata
        ):
            from types import SimpleNamespace

            partition_context = SimpleNamespace(
                has_asset_partitions=True,
                definition_metadata=context.upstream_output.definition_metadata,
                asset_partitions_def=context.asset_partitions_def,
                asset_partition_keys=context.asset_partition_keys,
            )
            from dagster import TimeWindowPartitionsDefinition

            if isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
                partition_context.asset_partitions_time_window = (
                    context.asset_partitions_time_window
                )
        predicate, params = self._partition(partition_context)
        sql = f"SELECT {projection} FROM {_quote(schema)}.{_quote(table)}"
        if predicate:
            sql += f" WHERE {predicate}"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params or None)
            data = cur.fetch_arrow_table()
        annotation = context.dagster_type.typing_type
        module = getattr(annotation, "__module__", "").split(".")[0]
        if module == "pandas":
            return data.to_pandas()
        if module == "polars":
            import polars as pl

            return pl.from_arrow(data)
        if annotation is pa.RecordBatchReader:
            return data.to_reader()
        return data


def _use_live_source(cur, prefix):
    """Require a live target with this prefix for the cursor's next ingestion."""
    cur.adbc_statement.set_options(
        **{"adbc.redis.ingest.live_source": json.dumps({"prefix": prefix})}
    )


def _key_prefix(cur, schema, table):
    """Return the table's key prefix, or None when it does not exist."""
    cur.execute(
        "SELECT key_prefix FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        (schema, table),
    )
    rows = cur.fetchall()
    return rows[0][0] if rows else None


def _discard_live(cur, staged, error):
    """Remove a failed live stage's HASHes and registration, best effort."""
    try:
        cur.execute(f"DELETE FROM {staged}")
    except dbapi.Error:
        pass  # The stage may never have been registered.
    try:
        cur.execute(f"DROP TABLE IF EXISTS {staged}")
    except dbapi.Error as cleanup:
        error.add_note(
            f"Cleanup of live staging table {staged} failed ({cleanup}); "
            "delete its rows and DROP TABLE it."
        )
