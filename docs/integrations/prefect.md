> This guide is maintained in redis-adbc-integrations. See [the repository README](../../README.md) for installation, migration and test commands.

# Redis integration guide for Prefect

Redis must run separately with the Query Engine (Search). See
[installation and driver discovery](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver)
for Python/native setup, supported wheel platforms, TLS and source builds.
Tables must be created, ingested or adopted through the driver; arbitrary Redis
keys do not automatically become SQL tables. Writes autocommit without rollback.

Install the integration from PyPI; its version matches the driver release it
requires, so pip also installs that `adbc-driver-redis` wheel:

```sh
python -m pip install --only-binary adbc-driver-redis 'redis-adbc-integrations[prefect]'
```

This guide describes the current repository. A helper added after the latest
release is available only from a source install of this checkout
(`./python`); do not assume an older release contains every helper documented
here.

## Prefect custom Python tasks

Install `pip install 'redis-adbc-integrations[prefect]'`. Create connections inside each
task so concurrent tasks and retries each own their connection and cursor:

```python
import os
from adbc_driver_redis import dbapi
from integration_guide_data import write_result
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE, persist_result=False)
def query(uri):
    with dbapi.connect(uri) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 AS one")
        output = cur.fetch_arrow_table()
    return write_result(output)

@flow(persist_result=False)
def pipeline(uri):
    return query.submit(uri).result()

print(pipeline(os.environ["REDIS_URI"]).to_pylist())
```

The [Prefect tests](https://github.com/Redislabs-Solution-Architects/redis-adbc-integrations/blob/main/python/tests/test_prefect.py) run real local flows with
Arrow ingestion/fetch, task dependencies, concurrent queries, a retry after a
committed write, and SQL error propagation. Writes autocommit even if a task
fails. Make retried writes idempotent (the test replaces its output table);
blindly retrying append/INSERT can duplicate rows. Caching and result persistence
are disabled here so each invocation actually accesses Redis. Remote workers
need the driver wheel and Redis connectivity installed separately.

Run against a disposable Redis with Search:

```sh
python -m pip install --only-binary adbc-driver-redis adbc-driver-redis -e './python[prefect]' pytest
REDIS_URI=redis://localhost:6379/0 \
PREFECT_SERVER_ANALYTICS_ENABLED=false \
python -m pytest -q python/tests/test_prefect.py
```

Build the library first as above; use `.so` on Linux. This verifies custom Python
tasks, not a packaged Prefect ADBC connector, `prefect-redis`, Flight SQL, or a
remote Prefect deployment.

## Rerun this guide

From the repository root, with the guide's dependencies and native driver
installed, set `REDIS_URI` to the dedicated local Software fixture and run:

```sh
python python/scripts/check_integration_guide.py prefect
```

This executes the guide's code blocks or its linked runnable example. For
Superset it exercises the documented backend workflow in an isolated application;
it does not automate the browser UI. See the
[per-guide results](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/integration-guides-262/validation.md).

[All integration guides](../../README.md)
