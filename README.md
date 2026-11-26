# Redis ADBC framework integrations

Airflow, Dagster, Dask, Ray, Prefect, dlt and Superset integrations for
[the Redis ADBC driver](https://github.com/redis/redis-adbc-driver).
The driver, DBAPI, Arrow, native reads and SQLAlchemy dialect remain upstream.

## Install

Use an isolated Python 3.14 environment. Each worker needs the driver and its
framework integration installed. Releases share the driver's version:
`redis-adbc-integrations` X.Y.Z was tested with, and requires, driver release
X.Y.Z (`adbc-driver-redis[dbapi]==X.Y.Z.*`). The driver wheel bundles its native
library (Linux amd64/arm64 with glibc 2.28+, macOS arm64):

```sh
python -m pip install --only-binary adbc-driver-redis 'redis-adbc-integrations[dask]'
```

To use unreleased integration code, install the driver wheel with an extra from
this checkout. CI tests this combination against the latest driver release:

```sh
git clone https://github.com/Redislabs-Solution-Architects/redis-adbc-integrations.git
cd redis-adbc-integrations
python -m pip install --only-binary adbc-driver-redis adbc-driver-redis './python[dask]'
```

To use unreleased driver code instead, build it from the `driver` submodule
(fetch and check out another driver branch or tag first if needed):

```sh
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
4x QPF. From the repository root, install the desired test extra and start the
fixture from the `driver` submodule:

```sh
git submodule update --init --depth 1 driver
python -m pip install --only-binary adbc-driver-redis adbc-driver-redis './python[analytics-tests]'
make -C driver/go redis-up
bash driver/go/software-setup.sh --exec python -m pytest -q "$PWD/python/tests/test_dask.py" "$PWD/python/tests/test_dask_write.py"
```

For a source-built driver, install `./driver/python` instead of the wheel, run
`make -C driver/go build` and set `REDIS_ADBC_LIBRARY` as shown above.

CI owns isolated Software fixtures for analytics on Linux amd64/arm64 and for
Airflow and Ray on Linux amd64, preserving the extracted architecture coverage. Ray also
runs an independent private-CA worker trust fixture. Test reports include skips;
an unavailable Superset dependency set is a blocker, not a passing check.

## Provenance and releases

Extracted from `redis/redis-adbc-driver` at
`cacae16c637bd5459a43f9e794e8c80f7fafa08f`, retaining Apache-2.0 notices. The
`driver` submodule supplies the Redis Software fixtures and optional source
builds; it is not what CI tests by default. Push, pull request and release runs
install the latest `adbc-driver-redis` release from PyPI, as users do, and check
out that release's tag for fixtures. The nightly scheduled run instead builds
and tests the latest upstream driver `main`, so a driver change that breaks an
integration shows up before it is released; a manual run can build another
driver branch or tag through the `driver_ref` input. A new driver release can
therefore fail CI on an unchanged branch: that is the breakage users would see.
Once newer releases exist, CI no longer tests the `adbc-driver-redis>=0.0.31`
floor itself; raise it when an integration needs newer driver behavior. No
native driver source is vendored into this Python distribution, and its
namespace does not overlap the core package.

Each driver release gets a matching release here. The daily `Driver release`
workflow takes the latest `adbc-driver-redis` version on PyPI and runs every
integration suite against that wheel. Only if all of them pass does it build
`redis-adbc-integrations` with the same version, requiring
`adbc-driver-redis[dbapi]==X.Y.Z.*`. It then publishes the wheel, source
distribution and `SHA256SUMS` as GitHub release `vX.Y.Z`, and finally uploads
those same files to [PyPI](https://pypi.org/project/redis-adbc-integrations/)
through Trusted Publishing (the `pypi` environment). A rerun finishes a release
whose PyPI upload is missing and skips versions already on PyPI. Integration-only
fixes ship with the next driver release. A GitHub release published by hand
runs the integration jobs and attaches the same kinds of files, but is not
uploaded to PyPI.

Upstream extraction and migration review: [redis-adbc-driver #489](https://github.com/redis/redis-adbc-driver/pull/489).
