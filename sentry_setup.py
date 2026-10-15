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
        traces_sample_rate=0,
        integrations=[LoggingIntegration(event_level=None)],
    )
    _initialized = True
