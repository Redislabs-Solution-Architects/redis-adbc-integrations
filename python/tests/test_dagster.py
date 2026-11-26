from uuid import uuid4

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from adbc_driver_redis import dbapi
from redis_adbc_integrations.dagster import RedisADBCIOManager, _quote
from redis_adbc_integrations.modes import connect as mode_connect
from dagster import (
    AssetIn,
    AssetKey,
    DailyPartitionsDefinition,
    Definitions,
    StaticPartitionsDefinition,
    asset,
    build_input_context,
    build_output_context,
    materialize,
)
from dagster._core.types.dagster_type import resolve_dagster_type


@pytest.fixture(params=["live", "managed"])
def io(request, uri, schema_cleanup):
    schema_cleanup("it_python_dagster")
    return RedisADBCIOManager(
        uri=uri, schema_name="it_python_dagster", mode=request.param
    )


def connect(manager):
    return mode_connect(manager.uri, "dagster", mode=manager.mode)


def output(name, **kwargs):
    return build_output_context(asset_key=AssetKey([name]), **kwargs)


@pytest.mark.parametrize("mode", ["live", "managed"])
@pytest.mark.parametrize("schema_name", [None, "fixed"])
@pytest.mark.parametrize(
    "left,right",
    [
        (["orders"], ["redis_live", "orders"]),
        (["a_b", "c", "orders"], ["a", "b_c", "orders"]),
        (["a_b", "orders"], ["a", "b", "orders"]),
        (["a", "orders"], ["b", "orders"]),
        (["orders"], ["public", "orders"]),
        (["Orders"], ["orders"]),
        (["Sales", "orders"], ["sales", "orders"]),
        (["雪", "orders"], ["雨", "orders"]),
        (["a", "orders"], ["dagster__61", "orders"]),
        (["Orders"], ["dagster__4f7264657273"]),
    ],
)
def test_distinct_asset_names(mode, schema_name, left, right):
    manager = RedisADBCIOManager(uri="redis://unused", schema_name=schema_name, mode=mode)
    with (
        build_output_context(asset_key=AssetKey(left)) as first,
        build_output_context(asset_key=AssetKey(right)) as second,
    ):
        assert manager._table(first) != manager._table(second)


@pytest.mark.parametrize(
    "mode,path,schema_name,expected",
    [
        ("managed", ["orders"], None, ("public", "orders")),
        ("managed", ["sales", "orders"], None, ("sales", "orders")),
        ("managed", ["orders"], "fixed", ("fixed", "orders")),
        ("managed", ["a_b", "c", "orders"], None, ("dagster__615f62_63", "orders")),
        ("managed", ["a", "b_c", "orders"], None, ("dagster__61_625f63", "orders")),
        ("managed", ["public", "orders"], None, ("dagster__7075626c6963", "orders")),
        ("managed", ["sales", "orders"], "FIXED", ("fixed", "dagster__73616c6573_6f7264657273")),
        # Live mode is the default; unprefixed assets go to redis_live.
        (None, ["orders"], None, ("redis_live", "orders")),
        ("live", ["sales", "orders"], None, ("sales", "orders")),
        ("live", ["public", "orders"], None, ("dagster__7075626c6963", "orders")),
        ("live", ["redis_live", "orders"], None, ("dagster__72656469735f6c697665", "orders")),
        ("live", ["sales", "orders"], "FIXED", ("fixed", "dagster__73616c6573_6f7264657273")),
    ],
)
def test_asset_name_mapping(mode, path, schema_name, expected):
    manager = RedisADBCIOManager(uri="redis://unused", schema_name=schema_name, mode=mode)
    with build_output_context(asset_key=AssetKey(path)) as context:
        assert manager._table(context) == expected


@pytest.mark.parametrize("mode", ["live", "managed"])
@pytest.mark.parametrize("fixed_schema", [False, True])
def test_colliding_asset_roundtrips(uri, schema_cleanup, mode, fixed_schema):
    prefix = "it_dagster_" + uuid4().hex
    manager = RedisADBCIOManager(
        uri=uri, schema_name=prefix if fixed_schema else None, mode=mode
    )
    paths = [[prefix + "_a", "b", "orders"], [prefix, "a_b", "orders"]]
    for path, value in zip(paths, [1, 2]):
        with build_output_context(asset_key=AssetKey(path)) as context:
            schema = manager._table(context)[0]
            schema_cleanup(schema)
            manager.handle_output(context, pa.table({"id": [value]}))
    for path, value in zip(paths, [1, 2]):
        with build_input_context(
            asset_key=AssetKey(path), dagster_type=resolve_dagster_type(pa.Table)
        ) as context:
            assert manager.load_input(context).to_pylist() == [{"id": value}]


@pytest.mark.parametrize("kind", ["arrow", "reader", "pandas", "polars"])
def test_roundtrip(io, kind):
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    obj = {
        "arrow": table,
        "reader": table.to_reader(),
        "pandas": table.to_pandas(),
        "polars": pl.from_arrow(table),
    }[kind]
    expected_type = {
        "arrow": pa.Table,
        "reader": pa.RecordBatchReader,
        "pandas": pd.DataFrame,
        "polars": pl.DataFrame,
    }[kind]
    with output("roundtrip") as context:
        io.handle_output(context, obj)
        with build_input_context(
            asset_key=context.asset_key,
            upstream_output=context,
            dagster_type=resolve_dagster_type(expected_type),
        ) as read:
            result = io.load_input(read)
        assert isinstance(result, expected_type)
        if kind == "reader":
            result = result.read_all()
        elif kind == "pandas":
            result = pa.Table.from_pandas(result, preserve_index=False)
        elif kind == "polars":
            result = result.to_arrow()
        assert (
            sorted(result.to_pylist(), key=lambda row: row["id"]) == table.to_pylist()
        )
        metadata = context.get_logged_metadata()
        assert metadata["row_count"].value == 2
        assert metadata["mode"].value == io.mode
        assert ("key_prefix" in metadata) == (io.mode == "live")


def test_materialize_dependencies_and_projection(io):
    @asset(io_manager_key="redis")
    def source() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2], "unused": [3, 4]})

    @asset(
        io_manager_key="redis", ins={"source": AssetIn(metadata={"columns": ["id"]})}
    )
    def downstream(source: pl.DataFrame) -> pa.Table:
        assert source.columns == ["id"]
        return source.to_arrow()

    definitions = Definitions(assets=[source, downstream], resources={"redis": io})
    assert definitions.get_repository_def()
    assert materialize([source, downstream], resources={"redis": io}).success


def test_static_partition_retries(io):
    partitions = StaticPartitionsDefinition(["a", "b"])

    @asset(
        io_manager_key="redis",
        partitions_def=partitions,
        metadata={"partition_expr": "part"},
    )
    def static_asset(context) -> pa.Table:
        return pa.table({"part": [context.partition_key], "n": [1]})

    for key in ["b", "a", "a"]:
        assert materialize(
            [static_asset], resources={"redis": io}, partition_key=key
        ).success
    with connect(io) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT part, count(*) FROM it_python_dagster.static_asset GROUP BY part ORDER BY part"
        )
        assert cur.fetchall() == [("a", 1), ("b", 1)]


def test_daily_partition_retries(io):
    partitions = DailyPartitionsDefinition(start_date="2026-01-01")

    @asset(
        io_manager_key="redis",
        partitions_def=partitions,
        metadata={"partition_expr": "day"},
    )
    def daily_asset(context) -> pa.Table:
        return pa.table({"day": [context.partition_time_window.start], "n": [1]})

    for key in ["2026-01-02", "2026-01-01", "2026-01-01"]:
        assert materialize(
            [daily_asset], resources={"redis": io}, partition_key=key
        ).success
    with connect(io) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM it_python_dagster.daily_asset")
        assert cur.fetchone() == (2,)


def test_failed_stream_preserves_previous(io):
    with output("safe") as context:
        io.handle_output(context, pa.table({"id": [1]}))
    schema = pa.schema([("id", pa.int64())])

    def broken():
        yield pa.record_batch([[2]], schema=schema)
        raise RuntimeError("injected stream failure")

    with (
        output("safe") as context,
        pytest.raises(Exception, match="injected stream failure"),
    ):
        io.handle_output(context, pa.RecordBatchReader.from_batches(schema, broken()))
    with build_input_context(
        asset_key=AssetKey(["safe"]), dagster_type=resolve_dagster_type(pa.Table)
    ) as context:
        result = io.load_input(context)
    assert result.to_pylist() == [{"id": 1}]
    with connect(io) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='it_python_dagster'"
        )
        assert cur.fetchall() == [("safe",)]


def test_live_rematerialization_replaces_hashes(uri, schema_cleanup, redis_client):
    schema = "it_dagster_live_" + uuid4().hex
    schema_cleanup(schema)
    manager = RedisADBCIOManager(uri=uri, schema_name=schema)
    assert manager._mode() == "live"
    prefixes = []
    for values in ([1, 2], [3]):
        with output("events") as context:
            manager.handle_output(context, pa.table({"id": values}))
            metadata = context.get_logged_metadata()
        assert metadata["mode"].value == "live"
        prefixes.append(metadata["key_prefix"].value)
        assert prefixes[-1].startswith(f"dagster:{schema}:events:")

    def keys(prefix):
        return sorted(redis_client.scan_iter(match=prefix.encode() + b"*", count=1000))

    first, second = prefixes
    assert first != second
    # The previous version's HASHes are deleted, not merely unregistered.
    assert keys(first) == []
    new_keys = keys(second)
    assert len(new_keys) == 1
    assert redis_client.type(new_keys[0]) == b"hash"
    assert b"id" in redis_client.hgetall(new_keys[0])
    with connect(manager) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT __key, id FROM "{schema}".events')
        assert cur.fetchall() == [(new_keys[0].decode(), 3)]
        cur.execute(
            "SELECT table_name, key_prefix FROM information_schema.tables "
            "WHERE table_schema = ?", (schema,),
        )
        assert cur.fetchall() == [("events", second)]


def test_live_mode_refuses_managed_schema(uri, schema_cleanup):
    schema = "it_dagster_managed_" + uuid4().hex
    schema_cleanup(schema)
    with dbapi.connect(uri) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
    manager = RedisADBCIOManager(uri=uri, schema_name=schema)
    with output("orders") as context, pytest.raises(dbapi.Error, match="live schema"):
        manager.handle_output(context, pa.table({"id": [1]}))
    with connect(manager) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            (schema,),
        )
        assert cur.fetchall() == []


def test_live_partition_refuses_managed_table_before_delete(uri, schema_cleanup):
    schema = "it_dagster_mixed_" + uuid4().hex
    schema_cleanup(schema)
    partitions = StaticPartitionsDefinition(["a", "b"])

    @asset(io_manager_key="redis", partitions_def=partitions,
           metadata={"partition_expr": "part"})
    def mixed(context) -> pa.Table:
        return pa.table({"part": [context.partition_key], "n": [1]})

    managed = RedisADBCIOManager(uri=uri, schema_name=schema, mode="managed")
    assert materialize([mixed], resources={"redis": managed}, partition_key="a").success
    live = RedisADBCIOManager(uri=uri, schema_name=schema)
    result = materialize([mixed], resources={"redis": live}, partition_key="a",
                         raise_on_error=False)
    assert not result.success
    # The refusal precedes DELETE, so the managed partition is intact.
    with connect(managed) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT part, n FROM "{schema}".mixed')
        assert cur.fetchall() == [("a", 1)]
