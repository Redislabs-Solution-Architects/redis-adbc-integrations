"""Dask graph, lifecycle, and distributed Redis read contracts."""

import uuid
from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pytest
from adbc_driver_redis import dbapi
from redis_adbc_integrations.dask import read_query
from redis_adbc_integrations.modes import connect as mode_connect
from distributed import Client, LocalCluster

SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string())])


LIVE_OPTIONS = {"adbc.redis.live_tables": "true", "adbc.redis.schema_ownership": "live"}
MANAGED_OPTIONS = {"adbc.redis.live_tables": "false"}


@pytest.mark.parametrize("mode,options", [
    (None, LIVE_OPTIONS), ("live", LIVE_OPTIONS), ("managed", MANAGED_OPTIONS),
])
@pytest.mark.parametrize("failure", [None, "execute", "fetch", "schema"])
def test_lazy_graph_and_task_cleanup(monkeypatch, failure, mode, options):
    events = []

    class Cursor:
        def execute(self, query, parameters):
            assert query == "SELECT id, name FROM example WHERE id >= ? AND id < ?"
            assert parameters == (0, 2)
            if failure == "execute":
                raise RuntimeError("execute failed")

        def fetch_arrow_table(self):
            if failure == "fetch":
                raise RuntimeError("fetch failed")
            if failure == "schema":
                return pa.table({"id": [1], "name": [2]})
            return pa.table({"id": [1], "name": [None]}, schema=SCHEMA)

    class Connection:
        @contextmanager
        def cursor(self):
            try:
                yield Cursor()
            finally:
                events.append("cursor closed")

    @contextmanager
    def connect(uri, *, db_kwargs, conn_kwargs):
        assert uri == "redis://unused"
        assert db_kwargs == {"adbc.redis.query_workers": "1", **options}
        assert conn_kwargs == {}
        events.append("connected")
        try:
            yield Connection()
        finally:
            events.append("connection closed")

    monkeypatch.setattr(dbapi, "connect", connect)
    frame = read_query(
        "redis://unused",
        "SELECT id, name FROM example WHERE id >= ? AND id < ?",
        [(0, 2)],
        schema=SCHEMA,
        db_kwargs={"adbc.redis.query_workers": "1"},
        mode=mode,
    )
    assert events == []
    assert not frame.known_divisions
    assert frame.npartitions == 1
    pd.testing.assert_frame_equal(
        frame._meta, SCHEMA.empty_table().to_pandas(types_mapper=pd.ArrowDtype)
    )
    if failure:
        error = ValueError if failure == "schema" else RuntimeError
        message = (
            "partition schema differs" if failure == "schema" else f"{failure} failed"
        )
        with pytest.raises(error, match=message):
            frame.compute(scheduler="synchronous")
    else:
        result = frame.compute(scheduler="synchronous")
        pd.testing.assert_frame_equal(
            result,
            pa.table({"id": [1], "name": [None]}, schema=SCHEMA).to_pandas(
                types_mapper=pd.ArrowDtype
            ),
        )
    assert events == ["connected", "cursor closed", "connection closed"]


@pytest.mark.parametrize(
    "partitions, schema, error",
    [
        ([], SCHEMA, ValueError),
        (["bad"], SCHEMA, TypeError),
        ([(0, 1)], None, TypeError),
    ],
)
def test_invalid_inputs(partitions, schema, error):
    with pytest.raises(error):
        read_query("redis://unused", "SELECT 1", partitions, schema=schema)


@pytest.mark.parametrize("kwargs", [
    {"mode": "snapshot"},
    {"mode": "live", "db_kwargs": {"adbc.redis.live_tables": "false"}},
    {"mode": "managed", "db_kwargs": {"adbc.redis.live_tables": "true"}},
    {"db_kwargs": {"adbc.redis.live_tables": "yes"}},
    {"mode": "managed", "db_kwargs": {"adbc.redis.schema_ownership": "live"}},
])
def test_invalid_mode_fails_before_connect(monkeypatch, kwargs):
    monkeypatch.setattr(dbapi, "connect", lambda *a, **k: pytest.fail("must not connect"))
    with pytest.raises(ValueError, match="live_tables|mode"):
        read_query("redis://unused", "SELECT 1", [()], schema=SCHEMA, **kwargs)


@pytest.mark.parametrize("db_kwargs,expected", [
    ({"adbc.redis.live_tables": "false"}, MANAGED_OPTIONS),
    ({"adbc.redis.flex": "true"}, MANAGED_OPTIONS),
    ({"adbc.redis.live_tables": "true"}, LIVE_OPTIONS),
    ({"adbc.redis.schema_ownership": "managed"},
     {"adbc.redis.live_tables": "true", "adbc.redis.schema_ownership": "managed"}),
])
def test_explicit_driver_options_choose_mode(monkeypatch, db_kwargs, expected):
    seen = []

    class Cursor:
        def execute(self, query, parameters):
            pass

        def fetch_arrow_table(self):
            return SCHEMA.empty_table()

    class Connection:
        @contextmanager
        def cursor(self):
            yield Cursor()

    @contextmanager
    def connect(uri, *, db_kwargs, conn_kwargs):
        seen.append(db_kwargs)
        yield Connection()

    monkeypatch.setattr(dbapi, "connect", connect)
    read_query("redis://unused", "SELECT 1", [()], schema=SCHEMA,
               db_kwargs=db_kwargs).compute(scheduler="synchronous")
    assert seen == [{**db_kwargs, **expected}]


def test_distributed_partition_coverage(uri):
    table = f"it_python_dask_{uuid.uuid4().hex}"
    source = pa.table(
        {"id": [-1, 0, 1, 2, 3, None], "name": ["a", "b", None, "d", "e", "f"]},
        schema=SCHEMA,
    )
    with dbapi.connect(uri) as conn, conn.cursor() as cur:
        cur.adbc_ingest(table, source, mode="create")
        try:
            query = (
                f'SELECT id, name FROM "{table}" '
                "WHERE (id >= ? AND id < ?) OR (id IS NULL AND ?)"
            )
            frame = read_query(
                uri,
                query,
                [
                    (-2, 0, False),
                    (0, 2, False),
                    (2, 4, False),
                    (4, 6, False),
                    (0, 0, True),
                ],
                schema=SCHEMA,
                mode="managed",
            )
            with (
                LocalCluster(
                    n_workers=2,
                    threads_per_worker=1,
                    processes=True,
                    dashboard_address=None,
                ) as cluster,
                Client(cluster) as client,
            ):
                # Distinct processes import the adapter and native library themselves.
                assert len(client.scheduler_info()["workers"]) == 2
                parts = client.gather(client.compute(frame.to_delayed()))
                assert [len(part) for part in parts] == [1, 2, 2, 0, 1]
                for part in parts:
                    assert part.dtypes.equals(frame._meta.dtypes)
                result = (
                    pd.concat(parts)
                    .sort_values("id", na_position="last")
                    .reset_index(drop=True)
                )
                expected = source.to_pandas(types_mapper=pd.ArrowDtype)
                pd.testing.assert_frame_equal(result, expected)
                # A projection must preserve nullable Arrow types, too.
                projected = (
                    frame[["id"]]
                    .compute()
                    .sort_values("id", na_position="last")
                    .reset_index(drop=True)
                )
                pd.testing.assert_frame_equal(projected, expected[["id"]])
                single = read_query(
                    uri, f'SELECT id, name FROM "{table}"', [()], schema=SCHEMA,
                    mode="managed",
                )
                pd.testing.assert_frame_equal(
                    single.compute()
                    .sort_values("id", na_position="last")
                    .reset_index(drop=True),
                    expected,
                )
        finally:
            cur.execute(f'DROP TABLE "{table}"')


def test_live_partitioned_read(uri, live_table):
    source = pa.table({"id": [-1, 0, 1, 2, 3], "name": ["a", "b", None, "d", "e"]},
                      schema=SCHEMA)
    name = live_table(source)
    # The default live mode reads application-owned HASHes in redis_live.
    frame = read_query(
        uri, f'SELECT id, name FROM redis_live."{name}" WHERE id >= ? AND id < ?',
        [(-2, 0), (0, 2), (2, 4)], schema=SCHEMA,
    )
    result = (frame.compute(scheduler="synchronous")
              .sort_values("id").reset_index(drop=True))
    pd.testing.assert_frame_equal(result, source.to_pandas(types_mapper=pd.ArrowDtype))
    with mode_connect(uri, "dask") as conn, conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM redis_live."{name}" WHERE __key LIKE ?',
                    (f"it:{name}:%",))
        assert cur.fetchone() == (5,)
