"""Actual dlt extract/normalize/load pipelines through the native ADBC driver."""

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from uuid import uuid4

import dlt
import pyarrow as pa
import pytest
from adbc_driver_redis import dbapi
from redis_adbc_integrations.dlt import _quote, redis_adbc
from redis_adbc_integrations.modes import connect as mode_connect
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed


@pytest.fixture
def pipeline_factory(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")

    def make(**kwargs):
        return dlt.pipeline(
            pipeline_name="redis_test_" + uuid4().hex,
            pipelines_dir=str(tmp_path),
            **kwargs,
        )

    return make


@pytest.fixture(params=["live", "managed"])
def target(request, uri, schema_cleanup):
    # Exercise quoted schema names, with isolated storage for every test.
    schema = 'it_dlt_"' + uuid4().hex
    schema_cleanup(schema)
    return uri, schema, request.param


def read(target, table="events"):
    uri, schema, mode = target
    with mode_connect(uri, "dlt", mode=mode) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {_quote(schema)}.{_quote(table)} ORDER BY id")
        return cur.fetch_arrow_table()


def test_pipeline_batches_and_append(target, pipeline_factory, monkeypatch):
    uri, schema, mode = target
    pipeline = pipeline_factory(
        destination=redis_adbc(uri=uri, schema_name=schema, mode=mode, batch_size=2),
        dataset_name="does_not_select_the_redis_schema",
    )
    # Observe the actual ADBC calls while still executing the native ingestion.
    from adbc_driver_manager.dbapi import Cursor

    counts = []
    ingest = Cursor.adbc_ingest

    def record_ingest(self, table_name, data, **kwargs):
        counts.append(data.num_rows)
        return ingest(self, table_name, data, **kwargs)

    monkeypatch.setattr(Cursor, "adbc_ingest", record_ingest)
    rows = [{"id": n, "label": f"row {n}"} for n in range(5)]
    info = pipeline.run(rows, table_name="events")
    assert not info.has_failed_jobs
    assert counts == [2, 2, 1]
    assert read(target).to_pylist() == rows
    assert not any(c.startswith("_dlt") for c in read(target).column_names)
    assert not pipeline.run(
        [{"id": 5, "label": "later"}], table_name="events"
    ).has_failed_jobs
    assert read(target).to_pylist() == rows + [{"id": 5, "label": "later"}]
    with mode_connect(uri, "dlt", mode=mode) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name, key_prefix FROM information_schema.tables "
            "WHERE table_schema = ?",
            (schema,),
        )
        rows = cur.fetchall()
    assert [name for name, _ in rows] == ["events"]
    if mode == "live":
        assert rows[0][1] == f"dlt:{schema}:events:"


def test_pipeline_types_and_nulls(target, pipeline_factory):
    uri, schema, mode = target
    pipeline = pipeline_factory(
        destination=redis_adbc(
            uri=uri,
            schema_name=schema,
            mode=mode,
            db_kwargs={"adbc.redis.time_zone": "UTC"},
            index_columns="id",
        )
    )
    rows = [
        {
            "id": 1,
            "label": "café 'quoted'",
            "active": True,
            "amount": Decimal("1234.5678"),
            "day": date(2026, 10, 29),
            "at": datetime(2026, 10, 29, 12, 34, 56, 123456, timezone.utc),
            "payload": b"\x00\x01\xff",
            "ratio": 1.25,
        },
        {
            "id": 2,
            **{
                c: None
                for c in ("label", "active", "amount", "day", "at", "payload", "ratio")
            },
        },
    ]
    assert not pipeline.run(rows, table_name="events").has_failed_jobs
    assert read(target).to_pylist() == rows


def test_arrow_input_and_multiple_tables(target, pipeline_factory):
    uri, schema, mode = target
    pipeline = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema, mode=mode))
    events = dlt.resource(pa.table({"id": [1, 2]}), name="events")
    other = dlt.resource([{"id": 3, "description": "other table"}], name="other")
    assert not pipeline.run([events, other]).has_failed_jobs
    assert read(target).to_pylist() == [{"id": 1}, {"id": 2}]
    assert read(target, "other").to_pylist() == [
        {"id": 3, "description": "other table"}
    ]


def test_incremental_source_keeps_local_checkpoint(target, pipeline_factory):
    uri, schema, mode = target
    pipeline = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema, mode=mode))
    rows = [{"id": 1}, {"id": 2}]

    @dlt.resource(name="events", primary_key="id")
    def events(cursor=dlt.sources.incremental("id")):
        yield rows

    assert not pipeline.run(events()).has_failed_jobs
    rows.append({"id": 3})
    # Recreate the pipeline from its local directory, as a subsequent process would.
    pipeline = dlt.pipeline(
        pipeline_name=pipeline.pipeline_name,
        pipelines_dir=pipeline.pipelines_dir,
        destination=redis_adbc(uri=uri, schema_name=schema, mode=mode),
    )
    assert not pipeline.run(events()).has_failed_jobs
    assert read(target).to_pylist() == rows


@pytest.mark.parametrize("disposition", ["replace", "merge"])
def test_unsupported_disposition_fails_before_connect(
    disposition, pipeline_factory, monkeypatch
):
    def forbidden(*args, **kwargs):
        pytest.fail("unsupported dispositions must not connect or modify Redis")

    monkeypatch.setattr(dbapi, "connect", forbidden)
    pipeline = pipeline_factory(destination=redis_adbc(uri="redis://unused:6379/0"))
    with pytest.raises(PipelineStepFailed, match="append.*only"):
        pipeline.run(
            [{"id": 1}],
            table_name="events",
            primary_key="id",
            write_disposition=disposition,
        )


@pytest.mark.parametrize("name", ["", "bad\x00name", None])
def test_invalid_identifiers(name):
    with pytest.raises(DestinationTerminalException, match="SQL names"):
        _quote(name)


def test_secret_configuration(target, pipeline_factory, monkeypatch):
    uri, schema, mode = target
    monkeypatch.setenv("DESTINATION__REDIS_ADBC__URI", uri)
    pipeline = pipeline_factory(destination=redis_adbc(schema_name=schema, mode=mode))
    assert not pipeline.run([{"id": 42}], table_name="events").has_failed_jobs
    assert read(target).to_pylist() == [{"id": 42}]


def test_retry_resumes_after_checkpointed_batch(target, pipeline_factory, monkeypatch):
    from adbc_driver_manager.dbapi import Cursor
    from dlt.common.destination.exceptions import DestinationTransientException

    uri, schema, mode = target
    monkeypatch.setenv("LOAD__RAISE_ON_MAX_RETRIES", "1")
    pipeline = pipeline_factory(
        destination=redis_adbc(uri=uri, schema_name=schema, mode=mode, batch_size=2)
    )
    ingest = Cursor.adbc_ingest

    def fail_second_batch(self, table_name, data, **kwargs):
        if data.column("id")[0].as_py() == 2:
            raise DestinationTransientException("injected failure before writing batch")
        return ingest(self, table_name, data, **kwargs)

    monkeypatch.setattr(Cursor, "adbc_ingest", fail_second_batch)
    rows = [{"id": n} for n in range(5)]
    with pytest.raises(PipelineStepFailed):
        pipeline.run(rows, table_name="events")
    assert read(target).to_pylist() == rows[:2]
    monkeypatch.setattr(Cursor, "adbc_ingest", ingest)
    assert not pipeline.run().has_failed_jobs
    assert read(target).to_pylist() == rows


def test_nested_values_as_json(target, pipeline_factory):
    uri, schema, mode = target
    pipeline = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema, mode=mode))
    payload = {"name": "nested", "tags": ["a", "b"]}
    assert not pipeline.run(
        [{"id": 1, "payload": payload}], table_name="events"
    ).has_failed_jobs
    result = read(target).to_pylist()
    assert result[0]["id"] == 1
    assert json.loads(result[0]["payload"]) == payload


def test_default_live_schema(uri, pipeline_factory, redis_client):
    table = "it_dlt_default_" + uuid4().hex
    pipeline = pipeline_factory(destination=redis_adbc(uri=uri))
    try:
        assert not pipeline.run([{"id": 1, "label": "live"}], table_name=table).has_failed_jobs
        with mode_connect(uri, "dlt") as conn, conn.cursor() as cur:
            cur.execute(f"SELECT __key, id, label FROM redis_live.{_quote(table)}")
            (key, *values), = cur.fetchall()
        assert values == [1, "live"]
        assert key.startswith(f"dlt:redis_live:{table}:")
        assert redis_client.hgetall(key)[b"label"] == b"live"
    finally:
        with mode_connect(uri, "dlt") as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM redis_live.{_quote(table)}")
            cur.execute(f"DROP TABLE IF EXISTS redis_live.{_quote(table)}")


def test_live_refuses_all_null_rows(uri, schema_cleanup, pipeline_factory):
    schema = "it_dlt_nulls_" + uuid4().hex
    schema_cleanup(schema)
    pipeline = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema))
    rows = [{"id": 1, "label": "kept"}, {"id": None, "label": None}]
    with pytest.raises(PipelineStepFailed, match="refused the live load"):
        pipeline.run(rows, table_name="events")


def test_live_refuses_existing_managed_table(uri, schema_cleanup, pipeline_factory):
    schema = "it_dlt_managed_" + uuid4().hex
    schema_cleanup(schema)
    managed = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema, mode="managed"))
    assert not managed.run([{"id": 1}], table_name="events").has_failed_jobs
    live = pipeline_factory(destination=redis_adbc(uri=uri, schema_name=schema))
    # A terminal refusal: dlt does not retry it.
    with pytest.raises(PipelineStepFailed, match="mode='managed'"):
        live.run([{"id": 2}], table_name="events")
    # The managed table was not appended to.
    assert read((uri, schema, "managed")).to_pylist() == [{"id": 1}]
