# openalex-ingest

Code to pull external sources into S3. For more, see https://openalex.org.

Please send all bug reports and feature requests to support@openalex.org.

## Repository Harvester

The repository harvester (`repositories.py`) pulls metadata from ~6,000 OAI-PMH endpoints daily and saves records to S3 for processing by the OpenAlex pipeline.

### Architecture (Simplified January 2026)

The harvester uses a simple, massively parallel approach:

1. **Daily Job**: One scheduled job harvests ALL endpoints
2. **Parallelization**: 100 concurrent threads with per-host rate limiting (max 3 per host)
3. **Health Tracking**: Each endpoint's status is recorded after every harvest attempt
4. **Runtime**: ~15 minutes for all ~6,000 endpoints

This replaced a complex 4-tier system that scheduled endpoints separately based on "reliability" tiers. The old system was:
- Overly complex with 4 separate scheduled jobs
- Based on stale data (`retry_interval` was never updated)
- Slow due to sequential processing

### Usage

```bash
# Daily job (recommended): harvest all endpoints in parallel
python repositories.py --all-endpoints --n_threads 100

# Harvest a specific endpoint
python repositories.py --endpoint-id abc123

# Custom date range
python repositories.py --all-endpoints --start-date 2026-01-01 --end-date 2026-01-15
```

### Health Tracking

The harvester tracks endpoint health with these database columns:

| Column | Description |
|--------|-------------|
| `last_health_status` | Status from last attempt: `success`, `blocked`, `timeout`, `connection_error`, `malformed`, `oai_error` |
| `last_health_check` | Timestamp of last harvest attempt |
| `last_response_time` | Response time in seconds |
| `last_error_message` | Error details if harvest failed |

### Legacy Columns (DO NOT USE)

The following columns are historical artifacts from the old tiering system. They are **NOT used** by the current harvester and may be removed in a future migration:

| Column | Was Used For | Current Status |
|--------|--------------|----------------|
| `retry_interval` | Exponential backoff scheduling | **IGNORED** - never updated |
| `retry_at` | Scheduling retries | **IGNORED** - never checked |
| `is_core` | Prioritizing "core" endpoints | **IGNORED** - all endpoints treated equally |

**DO NOT** add new logic that depends on these columns. They exist only for historical context and to avoid a breaking migration.

### Database Migration

Before running the new harvester, apply the migration to add health tracking columns:

```bash
psql $DATABASE_URL -f migrations/001_add_endpoint_health_columns.sql
```

### Heroku Scheduler

Replace the old 4 scheduled jobs with a single daily job:

**Old (remove these):**
- `python repositories.py --core-endpoints`
- `python repositories.py --reliable-endpoints`
- `python repositories.py --other-endpoints`
- `python repositories.py --abandoned-endpoints` (monthly)

**New (add this):**
- `python repositories.py --all-endpoints --n_threads 100` (daily)

### Configuration

Settings are defined at the top of `repositories.py`:

```python
MAX_WORKERS = 100       # Total concurrent threads
MAX_PER_HOST = 3        # Max concurrent requests per host
REQUEST_TIMEOUT = 15    # Seconds before giving up
BATCH_SIZE = 5000       # Records per S3 file
```
