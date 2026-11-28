> This guide is maintained in redis-adbc-integrations. See [the repository README](../../README.md) for installation, migration and test commands.

# Redis integration guide for Dagster

Redis must run separately with the Query Engine (Search). See
[installation and driver discovery](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver)
for Python/native setup, supported wheel platforms, TLS and source builds.
Tables must be created, ingested or adopted through the driver; arbitrary Redis
keys do not automatically become SQL tables. Writes autocommit without rollback.

**Default mode: live.** Dagster is in the orchestration group, so
`RedisADBCIOManager` defaults to the driver's live mode; only read-heavy BI and
visualization integrations default to managed tables. Live mode sets
`adbc.redis.live_tables=true` and `adbc.redis.schema_ownership=live`: each asset
is a live table in a live schema, and each row is an application-owned HASH that
Redis clients can read and write directly. Live reads have no statement
snapshot, writes apply per row, and a row whose values are all NULL is refused.
Set `mode="managed"` on the IO manager for managed tables with statement
snapshots, and on Redis Flex, which does not support live tables. The community
`ADBCResource` below is unaffected: it uses whatever `db_kwargs` you give it. See
[driver modes](../../README.md#driver-modes).

Install the integration from PyPI; its version matches the driver release it
requires, so pip also installs that `adbc-driver-redis` wheel:

```sh
python -m pip install --only-binary adbc-driver-redis 'redis-adbc-integrations[dagster]'
```

This guide describes the current repository. A helper added after the latest
release is available only from a source install of this checkout
(`./python`); do not assume an older release contains every helper documented
here.

## Dagster

The community [`dagster-adbc`](https://pypi.org/project/dagster-adbc/) package
provides `ADBCResource` for SQL and Arrow access inside assets. Install
`dagster-adbc` alongside the Redis driver wheel. With the Redis driver manifest
installed as described above:

```python
from dagster import Definitions, asset
from dagster_adbc import ADBCResource

@asset
def order_count(redis: ADBCResource) -> int:
    with redis.get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM analytics.orders")
        return cur.fetchone()[0]

defs = Definitions(
    assets=[order_count],
    resources={"redis": ADBCResource(
        driver="redis",
        uri="redis://localhost:6379/0",
        autocommit=True,
    )},
)
```

Alternatively, set `driver` to the absolute native library path. Use
`autocommit=True`: Redis does not implement transactions, and the community
resource's default (`False`) produces a driver-manager warning. Native database
options can be passed through `db_kwargs`, for example
`{"adbc.redis.default_schema": "analytics"}`. The resource exposes a connection;
assets explicitly execute queries or call `cursor.adbc_ingest`.

The `redis-adbc-integrations[dagster]` extra covers this; add pandas/polars when used. This repository
ships `RedisADBCIOManager`; it is not part of the community `dagster-adbc` package.

```python
import pyarrow as pa
from dagster import Definitions, asset
from redis_adbc_integrations.dagster import RedisADBCIOManager

@asset(io_manager_key="redis", key_prefix=["analytics"])
def orders() -> pa.Table:
    return pa.table({"id": [1, 2], "amount": [10, 20]})

defs = Definitions(
    assets=[orders],
    resources={"redis": RedisADBCIOManager(uri="redis://localhost:6379/0")},
)
```

For a runnable smoke test from a source checkout, use a dedicated Redis database
with Search and follow the development installation steps above. Run:

```sh
REDIS_URI=redis://localhost:6379/0 python python/examples/dagster_smoke.py
REDIS_URI=redis://localhost:6379/0 python -m pytest -q python/tests/test_dagster.py
```

The [smoke example](https://github.com/Redislabs-Solution-Architects/redis-adbc-integrations/blob/main/python/examples/dagster_smoke.py) materializes `orders`,
loads it from Redis into the dependent `revenue` asset, and checks the persisted
result with SQL: three orders totaling 42. Each invocation uses a unique schema
and drops it on exit. The integration suite also checks Arrow/pandas/polars
round trips, column projection, partition retries, and failed-stream preservation
in both modes, plus live HASH replacement on rematerialization; it creates and
removes the fixed `it_python_dagster` schema, so run it serially.

Outputs may be `pyarrow.Table`, `RecordBatchReader`, pandas or polars DataFrames.
Inputs default to Arrow tables; annotate them with pandas/polars DataFrame or
RecordBatchReader to request that type. Input `columns` metadata selects columns.
Reader outputs ingest in batches; reader inputs currently materialize the result
before returning an in-memory reader.

Asset names preserve component boundaries and case. With no `schema_name`, the
prefix path determines the schema (without a prefix, `redis_live` in live mode
and `public` in managed mode), and the final component determines the table. With `schema_name`, the entire asset path
determines the table inside that schema (the schema setting is lowercased).
A single lowercase ASCII identifier matching `[a-z_][a-z0-9_]*` stays readable,
unless it begins with the reserved `dagster__` marker. Other paths are encoded
as `dagster__` followed by each component's UTF-8 hex, separated by underscores.
An explicit `public` prefix is also encoded to distinguish it from no prefix,
as is an explicit `redis_live` prefix in live mode.
For example, `["a_b", "c", "orders"]` maps to
`dagster__615f62_63.orders`, while `["a", "b_c", "orders"]` maps to
`dagster__61_625f63.orders`. Simple `orders` and `sales.orders` stay unchanged.

**Migration:** this replaces the earlier underscore join and lowercasing of
asset keys. Multi-component prefixes, case-sensitive or encoded names, explicit
`public` prefixes, and prefixed assets using `schema_name` may move to new
targets. Inspect the manager's output table metadata, then deliberately migrate
or rematerialize affected assets before downstream reads. The manager does not
read, drop, or migrate ambiguous legacy tables automatically. Previously
overwritten asset data must be recovered from its source or a backup.

Configure `mode` (`live` by default, or `managed`), `db_kwargs` for native
options and `index_columns` for ingestion indexing (`*` by default; applied when
a live table is created). Live tables store rows under
`<key_prefix><schema>:<table>:<id>:`, with `key_prefix` defaulting to `dagster:`.
Output metadata includes row count, table name, mode, Arrow column schema and,
in live mode, the table's `key_prefix`. In live mode a prefix that names an
existing managed schema fails instead of writing a managed table.

Unpartitioned writes load a unique staging table before dropping the old table
and renaming the stage. A failed ingest preserves the old version; a failure
between DROP and RENAME can leave it absent. Readers can also observe this brief
window. In live mode, each materialization stages under a fresh key prefix; the
manager then deletes the old table's HASHes, drops (unregisters) it and renames
the stage, so the asset's key prefix changes on every materialization. A failure
after the old rows are deleted can leave the asset empty or partial; the error
names the staging table that still holds the new rows. A failed stage write
deletes the stage's HASHes and unregisters it. Static/dynamic string and daily/time-window partitions require asset
metadata `{"partition_expr": "column_name"}`. The manager uses bound partition
values or a half-open time range, deletes those rows, then appends. A retry
replaces that partition. A failed append may leave it partially written; values
returned by the asset must belong to the selected partition. Serialize writes to
the same asset/partition. Multi-dimensional partitions are unsupported.

## Rerun this guide

From the repository root, with the guide's dependencies and native driver
installed, set `REDIS_URI` to the dedicated local Software fixture and run:

```sh
python python/scripts/check_integration_guide.py dagster
```

This executes the guide's code blocks or its linked runnable example. For
Superset it exercises the documented backend workflow in an isolated application;
it does not automate the browser UI. See the
[per-guide results](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/integration-guides-262/validation.md).

[All integration guides](../../README.md)
