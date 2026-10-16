"""Sentry error reporting for openalex-ingest jobs (oxjob #1685): errors only, no PII.

Every Advanced Scheduler job runs a script that imports common.py, award_fetch.py
or datacite.py; each calls init_sentry() first, so an uncaught exception that ends
a job is reported. Log records are breadcrumbs only, never events: the harvesters
log each unreachable OAI endpoint and each retried 5xx at ERROR, which are remote
conditions, not bugs. A no-op when SENTRY_DSN is unset.
"""
import os

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

_initialized = False


def _strip_query(breadcrumb, hint):
    """HTTP breadcrumbs carry the query string (e.g. KAKEN's appid); keep only scheme://host/path."""
    data = breadcrumb.get("data")
    if isinstance(data, dict):
        data.pop("http.query", None)
        data.pop("http.fragment", None)
        if isinstance(data.get("url"), str):
            data["url"] = data["url"].split("?", 1)[0].split("#", 1)[0]
    return breadcrumb


def init_sentry():
    global _initialized
    dsn = os.environ.get("SENTRY_DSN")
    if _initialized or not dsn:
        return
    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
        # HEROKU_* come from Heroku's runtime-dyno-metadata lab feature.
        release=os.environ.get("HEROKU_SLUG_COMMIT") or os.environ.get("HEROKU_RELEASE_VERSION"),
        server_name=os.environ.get("DYNO"),
        send_default_pii=False,
        # Frame locals can hold secrets (e.g. KAKEN appid / credential-bearing URLs); never send them.
        include_local_variables=False,
        before_breadcrumb=_strip_query,
        traces_sample_rate=0,
        integrations=[LoggingIntegration(event_level=None)],
    )
    _initialized = True
