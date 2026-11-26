import json
import os
from uuid import uuid4

import pytest
from adbc_driver_redis import dbapi
from redis_adbc_integrations.modes import LIVE_SCHEMA, connect as mode_connect


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def live_connect(uri):
    """A connection that can see and modify live and managed tables."""
    # The Ray TLS candidate run trusts its private CA only through this option.
    ca = os.environ.get("REDIS_TLS_CA_FILE")
    options = {"adbc.redis.tls_ca_file": ca} if ca else None
    return mode_connect(uri, "dagster", mode="live", db_kwargs=options)


def delete_and_drop(cur, schema, table):
    """Remove a live table's HASHes, then unregister it (DROP TABLE keeps HASHes)."""
    target = f"{quote(schema)}.{quote(table)}"
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        (schema, table),
    )
    if cur.fetchall():
        cur.execute(f"DELETE FROM {target}")
        cur.execute(f"DROP TABLE {target}")


def drop_schema(uri, schema):
    """Drop a live or managed schema without leaving live HASHes behind."""
    with live_connect(uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            (schema,),
        )
        for (table,) in cur.fetchall():
            delete_and_drop(cur, schema, table)
        if schema != LIVE_SCHEMA:
            cur.execute(f"DROP SCHEMA IF EXISTS {quote(schema)} CASCADE")


@pytest.fixture
def uri():
    value = os.environ.get("REDIS_URI")
    if not value:
        pytest.skip("REDIS_URI must name a disposable Redis instance with Search")
    return value


@pytest.fixture
def connection(uri):
    with dbapi.connect(uri) as conn:
        yield conn


@pytest.fixture
def workflow_rows(connection):
    from uuid import uuid4

    import pyarrow as pa

    name = "it_workflow_" + uuid4().hex
    expected = pa.table(
        {
            "id": [1, 2, 3],
            "label": ["alpha", None, "雪"],
            "amount": [2.5, None, 7.5],
            "active": [True, None, False],
        }
    )
    try:
        with connection.cursor() as cur:
            cur.adbc_ingest(name, expected, mode="create")
        yield name, expected
    finally:
        with connection.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')


@pytest.fixture
def schema_cleanup(uri):
    """Register schemas to drop (rows first) when the test finishes."""
    schemas = []
    yield schemas.append
    for schema in reversed(schemas):
        drop_schema(uri, schema)


@pytest.fixture
def live_table(uri):
    """Create live tables with Arrow; delete their rows and unregister them afterwards."""
    created = []

    def create(data, *, schema=LIVE_SCHEMA, name=None, index_columns="*"):
        name = name or "it_live_" + uuid4().hex
        created.append((schema, name))
        with live_connect(uri) as conn, conn.cursor() as cur:
            cur.adbc_statement.set_options(**{
                "adbc.redis.ingest.index_columns": index_columns,
                "adbc.redis.ingest.live_source": json.dumps({"prefix": f"it:{name}:"}),
            })
            cur.adbc_ingest(name, data, mode="create", db_schema_name=schema)
        return name

    yield create
    with live_connect(uri) as conn, conn.cursor() as cur:
        for schema, name in reversed(created):
            delete_and_drop(cur, schema, name)


@pytest.fixture
def redis_client(uri):
    """A raw Redis client for checking application-owned HASHes directly."""
    redis = pytest.importorskip("redis")
    options = {"decode_responses": False}
    ca = os.environ.get("REDIS_TLS_CA_FILE") or os.environ.get("SSL_CERT_FILE")
    if uri.startswith("rediss://") and ca:
        options["ssl_ca_certs"] = ca
    client = redis.Redis.from_url(uri, **options)
    try:
        yield client
    finally:
        client.close()


def pytest_sessionstart(session):
    from urllib.parse import parse_qs, urlsplit

    for name in ("REDIS_URI", "REDIS_ADMIN_URI", "REDIS_FLEX_URI"):
        value = os.environ.get(name)
        if not value:
            continue
        try:
            uri = urlsplit(value)
            valid = (uri.scheme == "rediss" and uri.hostname and uri.username and uri.password
                     and not {"skip_verify", "ssl_cert_reqs", "ssl_check_hostname"}.intersection(parse_qs(uri.query)))
        except ValueError:
            valid = False
        if not valid:
            raise pytest.UsageError(
                f"{name} must use rediss:// with a username and password on Redis Enterprise; trust its TLS CA"
            )
