# Redis ADBC framework integrations

Airflow, Dagster, Dask, Ray, Prefect, dlt and Superset integrations for
[the Redis ADBC driver](https://github.com/redis/redis-adbc-driver).
The driver, DBAPI, Arrow, native reads and SQLAlchemy dialect remain upstream.

## Install from source

No release of this extracted distribution has been published yet. Use an
isolated Python 3.14 environment. Each worker needs the driver and its framework
integration installed. The pinned driver submodule provides a reproducible source
build until a matching core wheel is released:

```sh
git clone https://github.com/Redislabs-Solution-Architects/redis-adbc-integrations.git
cd redis-adbc-integrations
git submodule update --init --depth 1 driver
make -C driver/go build
python -m pip install ./driver/python './python[dask]'
export REDIS_ADBC_LIBRARY="$PWD/driver/go/build/libadbc_driver_redis.so" # .dylib on macOS
```

Choose extras `airflow`, `dagster`, `dask`, `ray`, `prefect` or `dlt`. Install
Airflow in a separate environment from Prefect because of conflicting framework
dependencies. `superset` retains the previous 6.1 dependency target for future
requalification: **Superset 6.1 is unsupported on Python 3.14+**, blocked by
upstream dependencies. Driver [issue #471](https://github.com/redis/redis-adbc-driver/issues/471)
is closed with no Superset 7.x run recorded; see
[the Superset guide](docs/integrations/superset.md) for the requalification checks.
It is excluded from passing CI coverage; moving it does not restore support.

## Migration

Install this distribution separately from `adbc-driver-redis`. Replace adapter
imports such as `adbc_driver_redis.dask` with `redis_adbc_integrations.dask`:

```python
from adbc_driver_redis import dbapi
from redis_adbc_integrations.dask import read_query, write_dataframe
from redis_adbc_integrations.dagster import RedisADBCIOManager
from redis_adbc_integrations.dlt import redis_adbc
from redis_adbc_integrations.ray import RedisDatasource
from redis_adbc_integrations.airflow import RedisAdbcHook
```

Each example import requires its corresponding optional dependency. DBAPI
imports stay unchanged. Airflow provider and Superset engine entry points now
belong to this distribution. Prefect uses task-local DBAPI connections and has
examples/tests rather than a custom connector module. Superset still uses the
core driver's `redis+adbc` / `rediss+adbc` SQLAlchemy dialect.

## Guides and tests

- [Airflow](docs/integrations/airflow.md)
- [Dagster](docs/integrations/dagster.md)
- [Dask](docs/integrations/dask.md)
- [Ray](docs/integrations/ray.md)
- [Prefect](docs/integrations/prefect.md)
- [Dlt](docs/integrations/dlt.md)
- [Superset](docs/integrations/superset.md)

Run tests only against a disposable Redis Software fixture with active, verified
4x QPF. From the repository root, after installing the desired test extra:

```sh
python -m pip install ./driver/python './python[analytics-tests]'
make -C driver/go redis-up build
export REDIS_ADBC_LIBRARY="$PWD/driver/go/build/libadbc_driver_redis.so"
bash driver/go/software-setup.sh --exec python -m pytest -q "$PWD/python/tests/test_dask.py" "$PWD/python/tests/test_dask_write.py"
```

CI owns isolated Software fixtures for analytics on Linux amd64/arm64 and for
Airflow and Ray on Linux amd64, preserving the extracted architecture coverage. Ray also
runs an independent private-CA worker trust fixture. Test reports include skips;
an unavailable Superset dependency set is a blocker, not a passing check.

## Provenance and releases

Extracted from `redis/redis-adbc-driver` at
`cacae16c637bd5459a43f9e794e8c80f7fafa08f`, retaining Apache-2.0 notices. The
`driver` gitlink pins the source used by CI and local fixtures. Update it
explicitly to test a newer driver. No native driver source is vendored into
this Python distribution, and its namespace does not overlap the core package.

A published GitHub release runs all supported integration jobs before attaching
the pure Python wheel, source distribution and SHA-256 files to that release.
PyPI publication is not configured; install from source until an artifact is
available. Framework releases are independent of the driver's release workflow.

Upstream extraction and migration review: [redis-adbc-driver #489](https://github.com/redis/redis-adbc-driver/pull/489).
