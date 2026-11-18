"""Airflow provider metadata, loadable without the optional Airflow dependencies."""

from importlib.metadata import version


def get_provider_info():
    """Expose the Redis ADBC connection without replacing Redis or generic ADBC."""
    return {
        "package-name": "redis-adbc-integrations",
        "name": "Redis ADBC",
        "description": "SQL and Arrow connectivity to Redis through the Redis ADBC driver.",
        "versions": [version("redis-adbc-integrations")],
        "connection-types": [
            {
                "connection-type": "redis_adbc",
                "hook-class-name": "redis_adbc_integrations.airflow.RedisAdbcHook",
            }
        ],
    }
