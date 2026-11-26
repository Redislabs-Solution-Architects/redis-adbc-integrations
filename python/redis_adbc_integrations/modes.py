"""Default driver ownership mode for each framework integration.

Integrations in read-heavy groups (BI and visualization) default to the driver's
**managed** tables: the driver owns a private copy and every statement reads a
consistent snapshot. Every other integration defaults to **live** HASH tables:
application-owned HASHes registered in ``redis_live`` or another live schema,
read and written in place, with per-row (not statement-level) consistency.

Each integration accepts ``mode="live"`` or ``mode="managed"`` to override its
default. Live mode sets the ``adbc.redis.live_tables=true`` and
``adbc.redis.schema_ownership=live`` database options, so ``CREATE SCHEMA``
creates live schemas; managed mode sets ``adbc.redis.live_tables=false``.
Without an explicit mode, an explicit ``adbc.redis.live_tables`` database option
selects the mode, and ``adbc.redis.flex=true`` selects managed mode because the
driver does not support live tables on Flex.
"""

from adbc_driver_redis import dbapi

LIVE = "live"
MANAGED = "managed"
MODES = (LIVE, MANAGED)

#: The only schema that holds live HASH tables.
LIVE_SCHEMA = "redis_live"
LIVE_TABLES_OPTION = "adbc.redis.live_tables"
SCHEMA_OWNERSHIP_OPTION = "adbc.redis.schema_ownership"
FLEX_OPTION = "adbc.redis.flex"

BI_VISUALIZATION = "bi_visualization"
ORCHESTRATION = "orchestration"
DISTRIBUTED_COMPUTE = "distributed_compute"
DATA_LOADING = "data_loading"

#: Read-heavy groups. Their integrations default to managed mode.
READ_HEAVY_GROUPS = frozenset({BI_VISUALIZATION})

#: The group of every integration in this distribution.
GROUPS = {
    "airflow": ORCHESTRATION,
    "dagster": ORCHESTRATION,
    "prefect": ORCHESTRATION,
    "dask": DISTRIBUTED_COMPUTE,
    "ray": DISTRIBUTED_COMPUTE,
    "dlt": DATA_LOADING,
    "superset": BI_VISUALIZATION,
}


def default_mode(integration):
    """Return ``"managed"`` for read-heavy groups and ``"live"`` otherwise."""
    try:
        group = GROUPS[integration]
    except KeyError:
        raise ValueError(f"unknown integration: {integration!r}") from None
    return MANAGED if group in READ_HEAVY_GROUPS else LIVE


def resolve_mode(integration, mode=None, db_kwargs=None):
    """Validate an explicit mode, or choose one for ``integration``.

    Without ``mode``, an explicit ``adbc.redis.live_tables`` in ``db_kwargs``
    decides; ``adbc.redis.flex=true`` means managed; otherwise the integration's
    group default applies.
    """
    default = default_mode(integration)
    if mode is not None:
        if mode not in MODES:
            raise ValueError(f"mode must be 'live' or 'managed', not {mode!r}")
        return mode
    options = db_kwargs or {}
    explicit = options.get(LIVE_TABLES_OPTION)
    if explicit == "true":
        return LIVE
    if explicit == "false":
        return MANAGED
    if explicit is not None:
        raise ValueError(f"{LIVE_TABLES_OPTION} must be 'true' or 'false'")
    if options.get(FLEX_OPTION) == "true":
        return MANAGED
    return default


def driver_options(mode, db_kwargs=None):
    """Return a copy of ``db_kwargs`` with the driver options for ``mode``.

    An explicit ``adbc.redis.live_tables`` value that disagrees with the mode is
    refused rather than silently overridden, as is
    ``adbc.redis.schema_ownership=live`` in managed mode. In live mode an
    explicit ``adbc.redis.schema_ownership=managed`` is kept, so ``CREATE
    SCHEMA`` still creates managed schemas.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be 'live' or 'managed', not {mode!r}")
    options = dict(db_kwargs or {})
    wanted = "true" if mode == LIVE else "false"
    current = options.get(LIVE_TABLES_OPTION)
    if current is not None and current != wanted:
        raise ValueError(
            f"db_kwargs sets {LIVE_TABLES_OPTION}={current!r}, which conflicts "
            f"with mode={mode!r}; choose the mode instead"
        )
    options[LIVE_TABLES_OPTION] = wanted
    if mode == LIVE:
        options.setdefault(SCHEMA_OWNERSHIP_OPTION, LIVE)
    elif options.get(SCHEMA_OWNERSHIP_OPTION) == LIVE:
        raise ValueError(
            f"db_kwargs sets {SCHEMA_OWNERSHIP_OPTION}='live', which requires "
            f"mode='live'"
        )
    return options


def connect(uri, integration, *, mode=None, db_kwargs=None, conn_kwargs=None, **kwargs):
    """Open a DBAPI connection using ``integration``'s default mode.

    For frameworks without their own adapter module, such as Prefect, where tasks
    open their own connections: ``connect(uri, "prefect")``.
    """
    resolved = resolve_mode(integration, mode, db_kwargs)
    return dbapi.connect(
        uri,
        db_kwargs=driver_options(resolved, db_kwargs),
        conn_kwargs=conn_kwargs,
        **kwargs,
    )
