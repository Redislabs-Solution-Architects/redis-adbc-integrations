# Project instructions

Read README.md. Python code uses `redis_adbc_integrations`; the native driver
and DBAPI belong upstream (`adbc-driver-redis`). CI tests its latest PyPI
release, and nightly its `main` through the `driver/` submodule, which also
supplies the Redis Software fixtures. Preserve task-local resource
ownership and acknowledged write/retry semantics. Test on disposable Redis
Software with active, verified 4x QPF via `make -C driver/go redis-up` and the
verified setup runner. Never test Valkey, ElastiCache or Memorystore. Keep TLS
private keys, credentials, operational artifacts and license files private.
Superset 6.1 remains unsupported on Python 3.14+; do not hide its blocker.
