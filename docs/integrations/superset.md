> This guide is maintained in redis-adbc-integrations. See [the repository README](../../README.md) for installation, migration and test commands.

# Apache Superset integration status

Superset 6.1 is unsupported under this project's Python 3.14-or-newer policy.
It requires PyArrow below 19, which has no usable CPython 3.14 wheels on the
checked Linux platform. The older Python environment, integration extra and
supported CI/profile entry have been removed.

Driver [issue #471](https://github.com/redis/redis-adbc-driver/issues/471),
which tracked upstream dependency support and restoration, was closed on
2026-11-17. No Superset 7.x run has been recorded or validated, so its closure
does not establish support for any Superset release.
Upstream discussion: [Superset SIP-234](https://github.com/apache/superset/issues/45019).
The proposal's closed status does not prove a compatible released dependency set.

A future Superset release counts as supported only after it resolves/installs on
Python 3.14+ without dependency overrides and passes the real application,
dataset/chart and SQL Lab checks against Redis Software with verified 4x QPF.
Historical test records retain their actual runtimes; they do not establish
current support.
