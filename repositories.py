"""
Repository OAI-PMH Harvester

This module harvests metadata records from OAI-PMH repository endpoints
and saves them to S3 for processing by the OpenAlex pipeline.

SIMPLIFIED ARCHITECTURE (January 2026):
The harvester runs a single daily job that harvests ALL ~5,000 endpoints
in parallel (~15 minutes total). This replaced a complex 4-tier system
that scheduled endpoints separately based on reliability.

LEGACY COLUMNS (DO NOT USE):
The following endpoint columns are historical artifacts from the old tiering
system and are NOT used by the current harvester. They may be removed in a
future migration:

  - retry_interval: Was used for exponential backoff scheduling. Now ignored.
  - retry_at: Was used for scheduling retries. Now ignored.
  - is_core: Was used to prioritize "core" endpoints. Now ignored (all
    endpoints are treated equally and harvested daily).

These columns remain in the database because:
1. Removing them requires a migration and we haven't prioritized cleanup
2. They provide historical context about how endpoints were previously handled
3. Some downstream queries may still reference them

DO NOT add new logic that depends on these columns.

CURRENT HEALTH TRACKING:
The harvester now uses these columns to track endpoint health:

  - last_health_status: Current status from most recent harvest attempt
    Values: 'success', 'empty', 'first_harvest_timeout', 'blocked', 'timeout', 'connection_error',
    'malformed', 'oai_error' ('empty' = a first harvest, no checkpoint and no 'from', that
    returned 0 records; 'first_harvest_timeout' = a first harvest stopped at
    FIRST_HARVEST_DEADLINE_SECONDS with no checkpoint written)
  - last_health_check: Timestamp of last harvest attempt
  - last_response_time: Response time in seconds
  - last_error_message: Error details if harvest failed

PARALLELIZATION:
- Uses ThreadPoolExecutor with 100 concurrent workers; hosts that answer 403 are paced and retried,
  and still-blocked endpoints get a quiet second pass at the end of the run (oxjob #1425)
- Rate-limited to max 3 concurrent requests per host (prevents overloading)
- 30-second connect / 60-second read timeout per request (oxjob #1425 H2, H2b)
- Total runtime: ~15 minutes for all ~5,000 endpoints
"""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gc
import gzip
import hashlib
import logging
import os
import re
import threading
from time import sleep, time
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import defusedxml.ElementTree as DefusedET
from defusedxml.ElementTree import ParseError
import requests
from requests.adapters import HTTPAdapter
import ssl
import certifi
from cryptography import x509
from cryptography.x509.oid import AuthorityInformationAccessOID, ExtensionOID
import shortuuid
from sickle import Sickle, oaiexceptions
from sickle.iterator import OAIItemIterator
from sickle.models import ResumptionToken
from sickle.oaiexceptions import NoRecordsMatch
from sickle.response import OAIResponse
from sqlalchemy import BigInteger, Column, Text, DateTime, Boolean, Interval, Float, select
import tenacity
import xml.etree.ElementTree as ET

from common import Base, LOGGER, S3_BUCKET, Session, db


# =============================================================================
# CONFIGURATION
# =============================================================================

# Parallelization settings
MAX_WORKERS = 100           # Total concurrent harvesting threads
MAX_PER_HOST = 3            # Max concurrent requests to same host
REQUEST_TIMEOUT = 30        # Connect timeout (seconds); Python counts the TLS handshake here, slow hosts need 20-30 s (oxjob #1425 H2b)
# Read timeout (seconds). Small OJS installs routinely take 20-30 s to render a ListRecords
# page; at 15 s they failed every night while dead hosts still fail fast on connect
# (oxjob #1425 H2: 9 OJS feeds, 13K held records, plus 3 new #1417 feeds).
READ_TIMEOUT = 60
# Hosts that answer 403 under the daily run's load but serve a lone request (632 endpoints on
# 2026-10-03, mostly Cloudflare bot-score/rate actions; oxjob #1425): after a 403 the host is
# paced (one request every SLOW_HOST_INTERVAL s, doubling per further 403) and the request is
# retried after these waits; endpoints still blocked at the end of the run get one more pass
# with RETRY_BLOCKED_WORKERS threads once the load is gone.
BLOCKED_RETRY_WAITS = (30, 60, 120)
SLOW_HOST_INTERVAL = 3.0
SLOW_HOST_MAX_INTERVAL = 15.0
RETRY_BLOCKED_WORKERS = 5
BATCH_SIZE = 5000           # Records per S3 file
EMPTY_FIRST_HARVEST_MSG = "First harvest (no 'from') returned no records"
# Wall-clock cap on one FIRST harvest (no checkpoint, whole feed). The daily --all-endpoints run
# (100 threads) finished all ~4,600 endpoints in 2h09m on 2026-09-28 and its slowest endpoint took
# 90 min; 6h lets a large new feed finish in one run while keeping a runaway walk from holding a
# worker thread and the dyno for most of the day. Checked after each record and by MySickle before
# every HTTP attempt and retry sleep, so the overrun is at most the one HTTP attempt already in
# flight (plus a <=10s tenacity backoff). Explicit --start-date runs checkpoint as they go and have
# no cap.
FIRST_HARVEST_DEADLINE_SECONDS = 6 * 3600
# Upper bound on a server-sent Retry-After (503/429); it was honoured uncapped.
MAX_RETRY_AFTER_SECONDS = 300


class FirstHarvestDeadline(Exception):
    """A first harvest hit FIRST_HARVEST_DEADLINE_SECONDS; recorded as 'first_harvest_timeout'."""


# Shared boto3 S3 client. boto3 clients are thread-safe and expensive to
# construct (event handlers, parsers, credential resolution), so we reuse
# one across all endpoint harvests instead of allocating per-harvest.
_s3_client_lock = threading.Lock()
_s3_client = None


def _get_shared_s3_client():
    global _s3_client
    if _s3_client is None:
        with _s3_client_lock:
            if _s3_client is None:
                _s3_client = boto3.client('s3')
    return _s3_client


# =============================================================================
# DATABASE MODEL
# =============================================================================

class Endpoint(Base):
    """
    Represents an OAI-PMH endpoint that we harvest metadata from.

    LEGACY COLUMNS (not used by current harvester - see module docstring):
      - retry_interval, retry_at, is_core

    CURRENT HEALTH TRACKING:
      - last_health_status, last_health_check, last_response_time, last_error_message
    """
    # oxjob #83.13 (Casey, 2026-09-01): registry table renamed from `endpoint`.
    __tablename__ = "oai_pmh_endpoint"

    id = Column(Text, primary_key=True)
    pmh_url = Column(Text)
    pmh_set = Column(Text)
    last_harvest_started = Column(DateTime)
    last_harvest_finished = Column(DateTime)
    most_recent_date_harvested = Column(DateTime)
    earliest_timestamp = Column(DateTime)
    email = Column(Text)
    error = Column(Text)
    harvest_identify_response = Column(Text)
    harvest_test_recent_dates = Column(Text)
    sample_pmh_record = Column(Text)
    policy_promises_no_submitted = Column(Boolean)
    policy_promises_no_submitted_evidence = Column(Text)
    ready_to_run = Column(Boolean)
    metadata_prefix = Column(Text)
    in_walden = Column(Boolean)

    # LEGACY COLUMNS - DO NOT USE (see module docstring)
    # These are from the old tiering system and are no longer used.
    retry_interval = Column(Interval)  # LEGACY: was for exponential backoff
    retry_at = Column(DateTime)         # LEGACY: was for scheduling retries
    is_core = Column(Boolean)           # LEGACY: was for tiering priority

    # CURRENT HEALTH TRACKING COLUMNS
    last_health_status = Column(Text)     # success, empty, first_harvest_timeout, blocked, timeout, connection_error, malformed, oai_error
    last_health_check = Column(DateTime)  # when we last tested
    last_response_time = Column(Float)    # seconds
    last_error_message = Column(Text)     # details if failed
    last_record_count = Column(BigInteger)  # records retrieved in the attempt stamped by last_health_check (partial on failure); oxjob #804

    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)
        if not self.id:
            self.id = shortuuid.uuid()[0:20].lower()
        if not self.metadata_prefix:
            self.metadata_prefix = 'oai_dc'


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

class StateManager:
    """
    Database operations for endpoints.

    The old tiering methods (get_core_endpoints, get_reliable_non_core_endpoints,
    get_other_endpoints, get_abandoned_endpoints) have been removed. The new
    approach simply harvests all endpoints daily using get_all_harvestable_endpoints().
    """

    @staticmethod
    def get_endpoint(endpoint_id: str, session) -> Optional[Endpoint]:
        """Get a single endpoint by ID."""
        stmt = select(Endpoint).filter_by(id=endpoint_id)
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def update_endpoint_state(state: Endpoint, session):
        """Update an endpoint's state in the database."""
        session.merge(state)
        session.commit()

    @staticmethod
    def get_all_harvestable_endpoints(session, health_statuses=None) -> List[Endpoint]:
        """
        Get all endpoints that should be harvested.

        Criteria:
        - ready_to_run == True
        - in_walden == True (part of OpenAlex pipeline)
        - optionally last_health_status in `health_statuses` (re-run only e.g. the blocked ones)

        Unlike the old tiering system, this returns ALL harvestable endpoints.
        The parallelization handles the load efficiently.

        Uses yield_per() to avoid loading all 6K+ rows into memory at once.
        """
        stmt = select(Endpoint).filter(
            Endpoint.ready_to_run == True,
            Endpoint.in_walden == True
        )
        if health_statuses:
            stmt = stmt.filter(Endpoint.last_health_status.in_(list(health_statuses)))
        stmt = stmt.execution_options(yield_per=500)
        return list(session.execute(stmt).scalars())

    @staticmethod
    def update_health_status(
        endpoint: Endpoint,
        session,
        status: str,
        response_time: float,
        error_message: Optional[str] = None,
        record_count: Optional[int] = None
    ):
        """
        Update endpoint health tracking columns after a harvest attempt.

        Args:
            endpoint: The endpoint to update
            session: Database session
            status: One of 'success', 'empty', 'first_harvest_timeout', 'blocked',
                   'timeout', 'connection_error', 'malformed', 'oai_error'. 'empty' =
                   a first harvest (no checkpoint) that got 0 records;
                   'first_harvest_timeout' = a first harvest that hit its deadline.
            response_time: How long the request took in seconds
            error_message: Error details if status is not 'success'
            record_count: Records retrieved from the feed in this attempt
                   (0 for NoRecordsMatch; partial count on failure; None only
                   when the harvester never got far enough to count). oxjob #804.
        """
        endpoint.last_health_status = status
        endpoint.last_health_check = datetime.now(timezone.utc)
        endpoint.last_response_time = response_time
        endpoint.last_error_message = error_message
        endpoint.last_record_count = record_count
        session.merge(endpoint)
        session.commit()


# =============================================================================
# RATE LIMITING
# =============================================================================

class HostRateLimiter:
    """
    Per-host rate limiting using semaphores.

    Prevents overwhelming any single host with too many concurrent requests.
    Each host gets a semaphore allowing MAX_PER_HOST concurrent connections.
    """

    def __init__(self, max_per_host: int = MAX_PER_HOST):
        self.max_per_host = max_per_host
        self._semaphores = defaultdict(lambda: threading.Semaphore(self.max_per_host))
        self._lock = threading.Lock()
        self._slow = {}        # host -> seconds between requests, once the host has answered 403
        self._next_slot = {}   # host -> epoch seconds when the next paced request may start
        self._pace_locks = defaultdict(threading.Lock)

    @staticmethod
    def _host(url):
        return urlparse(url).hostname

    def mark_slow(self, url: str, interval: float = SLOW_HOST_INTERVAL) -> float:
        """Pace a host after a 403: first call sets `interval`, each later one doubles it."""
        host = self._host(url)
        with self._lock:
            current = self._slow.get(host)
            new = min(max(interval, current * 2), SLOW_HOST_MAX_INTERVAL) if current else interval
            self._slow[host] = new
            return new

    def is_slow(self, url: str) -> bool:
        with self._lock:
            return self._host(url) in self._slow

    def pace(self, url: str):
        """Before a request: if the host is paced, wait for its next slot (serialises its threads)."""
        host = self._host(url)
        with self._lock:
            interval = self._slow.get(host)
        if not interval:
            return
        with self._pace_locks[host]:
            wait = self._next_slot.get(host, 0) - time()
            if wait > 0:
                sleep(wait)
            self._next_slot[host] = time() + interval

    def get_semaphore(self, url: str) -> threading.Semaphore:
        """Get the semaphore for a URL's host."""
        host = urlparse(url).hostname
        with self._lock:
            return self._semaphores[host]

    @contextmanager
    def limit(self, url: str):
        """Context manager that acquires/releases the host's semaphore."""
        semaphore = self.get_semaphore(url)
        semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()


# Global rate limiter instance
host_rate_limiter = HostRateLimiter()


# =============================================================================
# THREAD-LOCAL LOGGING
# =============================================================================

thread_local = threading.local()

def get_thread_logger():
    """Get a logger for the current thread."""
    if not hasattr(thread_local, "logger"):
        thread_local.logger = logging.getLogger(f"harvester.{threading.current_thread().name}")
        if not thread_local.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            thread_local.logger.addHandler(handler)
            thread_local.logger.setLevel(logging.INFO)
            thread_local.logger.propagate = False
    return thread_local.logger


# =============================================================================
# METRICS LOGGING
# =============================================================================

class MetricsLogger:
    """Logs harvesting metrics at regular intervals."""

    def __init__(self, interval=5):
        self.interval = interval
        self.record_count = 0
        self.start_time = time()
        self.last_datestamp = None
        self.last_url = None
        self.running = False
        self.lock = threading.Lock()
        self.thread = None
        self.total_records = None
        self.logger = get_thread_logger()

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._log_metrics)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def increment_count(self):
        with self.lock:
            self.record_count += 1

    def update_datestamp(self, datestamp):
        with self.lock:
            self.last_datestamp = datestamp

    def update_url(self, url):
        with self.lock:
            self.last_url = url

    def _log_metrics(self):
        while self.running:
            with self.lock:
                elapsed_time = time() - self.start_time
                records_per_second = self.record_count / elapsed_time if elapsed_time > 0 else 0
                datestamp = self.last_datestamp or "N/A"
                url = self.last_url or "N/A"
                LOGGER.info(
                    f"Harvested records: {self.record_count} | Total records: {self.total_records} | "
                    f"Speed: {records_per_second:.2f} records/sec | Last Datestamp: {datestamp} | "
                    f"Last URL: {url}")

            sleep(self.interval)


# =============================================================================
# ENDPOINT HARVESTER
# =============================================================================

class EndpointHarvester:
    """Harvests records from a single OAI-PMH endpoint."""

    def __init__(self, endpoint: Endpoint, db_session, batch_size=BATCH_SIZE):
        self.state = endpoint
        self.batch_size = batch_size
        self.db = db_session
        self.error = None
        self.metrics = MetricsLogger()
        self.logger = get_thread_logger()
        self.date_format = self.detect_date_format()

    def harvest(self, s3_bucket, first=None, last=None):
        """
        Harvest records from the endpoint over the given date range.
        Assumes that first and last are already bounded (e.g., 1–5 days),
        and does NOT update any state. first=None means a first harvest:
        no 'from' is sent, so the feed returns everything up to last.
        """
        if isinstance(first, datetime):
            first = first.date()
        if isinstance(last, datetime):
            last = last.date()

        self.logger.info(f"Harvesting from {first} to {last} (UTC range)")

        try:
            self.metrics.start()
            with self._get_s3_client() as s3_client:
                self.call_pmh_endpoint(
                    s3_client=s3_client,
                    s3_bucket=s3_bucket,
                    first=first,
                    last=last
                )
        finally:
            self.metrics.stop()

    def _iter_records_safe(self, records):
        """Iterate over OAI records, skipping any that fail to parse.

        Some OAI endpoints serve records that are not marked as deleted but
        have empty <metadata></metadata> elements (e.g. RWTH Aachen record
        oai:publications.rwth-aachen.de:52515). Sickle's Record.__init__
        calls .getchildren()[0] on the metadata element, which raises
        IndexError and kills the entire harvest. This wrapper catches
        per-record errors so the rest of the batch continues.
        """
        while True:
            try:
                record = next(records)
                yield record
            except StopIteration:
                break
            except (IndexError, AttributeError) as e:
                self.logger.warning(f"Skipping malformed OAI record: {e}")
                continue
            except Exception as e:
                # Re-raise errors that aren't record-parsing issues
                # (e.g. network errors, OAI protocol errors)
                if 'list index out of range' in str(e):
                    self.logger.warning(f"Skipping malformed OAI record: {e}")
                    continue
                raise

    def call_pmh_endpoint(self, s3_client, s3_bucket, first, last):
        until_date = format_oai_datestamp(last, self.date_format)

        args = {
            'metadataPrefix': self.state.metadata_prefix,
            'until': until_date
        }

        # No 'from' on a first harvest. Identify's earliestDatestamp is not a safe
        # lower bound: figshare advertises 1800-01-01 and answers noRecordsMatch for
        # it; DSpace 7 advertises its last reindex time, so the window starts after
        # every record it has. Both left new endpoints at 0 records forever.
        #
        # A first harvest walks the whole feed, and feeds are not always in datestamp
        # order (figshare is newest-first). A mid-walk checkpoint would then land near
        # the newest date and a failed run would skip everything older on the next day.
        # So a first harvest persists no checkpoint until the walk completes, and then
        # writes the max datestamp it saw. A failed first harvest restarts from scratch.
        first_harvest = first is None
        max_date_key = None
        min_date_key = None
        deadline = time() + FIRST_HARVEST_DEADLINE_SECONDS if first_harvest else None
        if not first_harvest:
            args['from'] = format_oai_datestamp(first, self.date_format)

        if self.state.pmh_set:
            args["set"] = self.state.pmh_set

        self.logger.info(f"OAI-PMH request parameters: {args}")

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, metrics_logger=self.metrics)
            my_sickle.deadline = deadline  # MySickle stops retrying/sleeping past it
            records = self._make_oai_request(my_sickle, **args)

            if hasattr(records._items, 'oai_response'):
                resumption_token_element = records._items.oai_response.xml.find(
                    './/' + records._items.sickle.oai_namespace + 'resumptionToken')
                if resumption_token_element is not None:
                    complete_list_size = resumption_token_element.attrib.get('completeListSize')
                    if complete_list_size:
                        self.metrics.total_records = int(complete_list_size)
                        self.logger.info(f"Total records to harvest: {self.metrics.total_records}")

            # Group records by date
            records_by_date = {}
            batch_counters = {}
            current_date_processing = None
            records_saved = 0

            for record in self._iter_records_safe(records):
                self.metrics.increment_count()
                self.metrics.update_datestamp(record.header.datestamp)

                datestamp = record.header.datestamp
                date_key = datestamp.split('T')[0] if 'T' in datestamp else datestamp
                if max_date_key is None or date_key > max_date_key:
                    max_date_key = date_key
                if min_date_key is None or date_key < min_date_key:
                    min_date_key = date_key

                # Checkpoint when date changes.
                #
                # This MUST fire on any change, not just an increase. A feed that returns
                # records out of datestamp order (OpenEdition does: 2026-09-14, then 09-09,
                # then 09-11 ...) leaves the bucket for every date we step *down* from open,
                # and the final cleanup below used to write only the single date we happened
                # to end on -- silently dropping 20% of a 113,872-record walk while still
                # exiting 0 with "0 malformed, 0 OAI errors" (oxjob #1118, 2026-09-16).
                # On an ordered feed dates only ever increase, so `!=` is a no-op there.
                if current_date_processing and date_key != current_date_processing:
                    if current_date_processing in records_by_date and records_by_date[current_date_processing]:
                        records_saved += len(records_by_date[current_date_processing])
                        self.save_batch(s3_client, s3_bucket, batch_counters[current_date_processing],
                                        records_by_date[current_date_processing], current_date_processing)
                    # Drop the finished date's entries entirely so the dicts
                    # don't grow unbounded for endpoints walking through
                    # years of historical dates.
                    records_by_date.pop(current_date_processing, None)
                    batch_counters.pop(current_date_processing, None)

                    checkpoint_dt = parse_datestamp(current_date_processing)
                    if not first_harvest and (not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested):
                        self.state.most_recent_date_harvested = checkpoint_dt
                        self.db.merge(self.state)
                        self.db.commit()
                        self.logger.info(f"Checkpoint: {current_date_processing} complete")

                current_date_processing = date_key

                if date_key not in records_by_date:
                    records_by_date[date_key] = []
                    batch_counters[date_key] = 1

                records_by_date[date_key].append(record)

                if len(records_by_date[date_key]) >= self.batch_size:
                    records_saved += len(records_by_date[date_key])
                    self.save_batch(s3_client, s3_bucket, batch_counters[date_key],
                                    records_by_date[date_key], date_key)
                    records_by_date[date_key] = []
                    batch_counters[date_key] += 1

                # Checked after the record is buffered, so it is counted and saved. Raised inside
                # the try: the except below saves the open bucket to S3, and first_harvest keeps
                # it from writing a checkpoint.
                if deadline is not None and time() > deadline:
                    raise FirstHarvestDeadline("deadline reached between records")

            # Final cleanup -- sweep EVERY bucket still open, not just the current date.
            # The `!=` flush above should leave at most one, but an early break or a future
            # change to the flush rule must not be able to resurrect the silent drop.
            for leftover_date in sorted(records_by_date):
                if records_by_date[leftover_date]:
                    records_saved += len(records_by_date[leftover_date])
                    self.save_batch(s3_client, s3_bucket,
                                    batch_counters.get(leftover_date, 1),
                                    records_by_date[leftover_date], leftover_date)

            # Reconcile: record_count counts records ITERATED, which is what made the drop
            # above invisible for as long as it existed. Compare against what we actually
            # wrote, and against the feed's own completeListSize when it gave us one.
            if records_saved != self.metrics.record_count:
                self.logger.warning(
                    f"Record loss: iterated {self.metrics.record_count}, saved {records_saved} "
                    f"({self.metrics.record_count - records_saved} unsaved) for {self.state.pmh_url}")
            if self.metrics.total_records and records_saved < self.metrics.total_records:
                self.logger.warning(
                    f"Short harvest: feed advertised {self.metrics.total_records}, "
                    f"saved {records_saved} for {self.state.pmh_url}")

            # First harvest: the walk completed, so the max datestamp seen is safe.
            final_date = max_date_key if first_harvest else current_date_processing
            if final_date:
                checkpoint_dt = parse_datestamp(final_date)
                if not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested:
                    self.state.most_recent_date_harvested = checkpoint_dt
                    self.db.merge(self.state)
                    self.db.commit()
                    self.logger.info(f"Checkpoint: {final_date} complete (final)")

        except NoRecordsMatch:
            self.logger.info(f"No records found for {self.state.pmh_url} with args {args}")

        except Exception as e:
            self.state.error = f"Error harvesting records: {str(e)}"

            # Save partial progress before raising
            # This ensures we don't re-harvest dates we've already completed
            try:
                # Save any pending records for the current date
                if 'current_date_processing' in dir() and current_date_processing:
                    if 'records_by_date' in dir() and records_by_date.get(current_date_processing):
                        batch_num = batch_counters.get(current_date_processing, 1)
                        self.save_batch(s3_client, s3_bucket, batch_num,
                                        records_by_date[current_date_processing], current_date_processing)
                        self.logger.info(f"Saved partial batch for {current_date_processing} before error")

                    # Update checkpoint to last completed date (one before current)
                    # We don't checkpoint the current date since it may be incomplete.
                    # Never on a first harvest: its walk order is unknown (see above).
                    checkpoint_dt = parse_datestamp(current_date_processing) - timedelta(days=1)
                    if not first_harvest and checkpoint_dt > datetime(2000, 1, 1):
                        if not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested:
                            self.state.most_recent_date_harvested = checkpoint_dt
                            self.db.merge(self.state)
                            self.db.commit()
                            self.logger.info(f"Saved checkpoint at {checkpoint_dt.date()} before error")
            except Exception as save_error:
                self.logger.warning(f"Failed to save partial progress: {save_error}")

            if isinstance(e, FirstHarvestDeadline):
                raise FirstHarvestDeadline(
                    f"First harvest stopped at the {FIRST_HARVEST_DEADLINE_SECONDS // 3600}h deadline ({e}) after "
                    f"{self.metrics.record_count} records (datestamps {min_date_key} to {max_date_key}); "
                    f"no checkpoint written. Recover by hand with an explicit window, which checkpoints "
                    f"as it goes: python repositories.py --endpoint-id {self.state.id} --start-date YYYY-MM-DD") from e
            raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type((BotoCoreError, ClientError)),
        before=tenacity.before_log(LOGGER, logging.INFO),
        after=tenacity.after_log(LOGGER, logging.INFO),
        reraise=True
    )
    def save_batch(self, s3_client, s3_bucket, batch_number, records, date_key):
        """Save a batch of records to S3."""
        try:
            date_path = self.get_datetime_path(date_key)

            # oxjob #1118: hash the record bodies as well as their identifiers. Keying on
            # identifiers alone made a re-harvest a no-op whenever a provider rewrote a
            # record's content without moving its datestamp -- the key matched, head_object
            # skipped, and the correction never landed. Cairn has been doing that for years
            # (DOIs, live URLs and access rights all added under frozen 2021 datestamps).
            # Identical content still skips; changed content now lands at a new key, which
            # is what Auto Loader needs to see it (overwriting a path does not re-trigger it).
            record_ids = sorted([r.header.identifier for r in records])
            record_bodies = sorted(str(r.raw) for r in records)
            content_hash = hashlib.md5(
                ("".join(record_ids) + "".join(record_bodies)).encode()
            ).hexdigest()[:12]
            if self.state.id == "irdb_nii_ac_jp":
                object_key = f"irdb/{date_path}/{content_hash}.xml.gz"
            else:
                object_key = f"repositories/{self.state.id}/{date_path}/{content_hash}.xml.gz"

            try:
                s3_client.head_object(Bucket=s3_bucket, Key=object_key)
                self.logger.info(f"Skipping existing batch: {object_key}")
                return date_key
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise

            root = ET.Element('oai_records')

            for record in records:
                record_elem = DefusedET.fromstring(record.raw)
                root.append(record_elem)

            xml_content = ET.tostring(root, encoding='unicode', method='xml')
            compressed_content = gzip.compress(xml_content.encode('utf-8'))

            metadata = {
                'record_count': str(len(records)),
                'content_hash': content_hash,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

            s3_client.put_object(
                Bucket=s3_bucket,
                Key=object_key,
                Body=compressed_content,
                ContentType='application/x-gzip',
                Metadata=metadata
            )

            self.logger.info(f"Uploaded batch {batch_number} to {object_key}")
            return date_key

        except Exception as e:
            LOGGER.exception(f"Error saving batch {batch_number}")
            self.state.error = f"Error saving batch: {str(e)}"
            raise

    @contextmanager
    def _get_s3_client(self):
        yield _get_shared_s3_client()

    def detect_date_format(self):
        """Detect if the repository requires a full timestamp format or just 'YYYY-MM-DD'."""
        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=(10, READ_TIMEOUT))
            identify = my_sickle.Identify()
            earliest = identify.earliestDatestamp

            if 'T' in earliest:
                self.logger.info("Repository supports full timestamp format 'YYYY-MM-DDTHH:MM:SSZ'")
                return '%Y-%m-%dT%H:%M:%SZ'
            else:
                self.logger.info("Repository supports date-only format 'YYYY-MM-DD'")
                return '%Y-%m-%d'
        except Exception as e:
            LOGGER.warning(f"Unable to determine date format; defaulting to 'YYYY-MM-DD': {e}")
            return '%Y-%m-%d'

    def _validate_record(self, record: str) -> bool:
        if not record.strip():
            LOGGER.warning("Empty record found")
            return False

        try:
            DefusedET.fromstring(record)
            return True
        except ParseError as e:
            LOGGER.warning(f"Invalid XML record: {str(e)}")
            return False

    def get_earliest_datestamp(self):
        if not self.state.pmh_url:
            LOGGER.warning("No PMH URL provided, returning default date")
            return datetime(2000, 1, 1)

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=(10, READ_TIMEOUT))
            identify = my_sickle.Identify()
            earliest = identify.earliestDatestamp

            if earliest:
                try:
                    if 'T' in earliest:
                        return datetime.strptime(earliest, '%Y-%m-%dT%H:%M:%SZ')
                    else:
                        return datetime.strptime(earliest, '%Y-%m-%d')
                except ValueError:
                    LOGGER.warning(f"Could not parse earliest datestamp: {earliest}")
                    return datetime(2000, 1, 1)
            else:
                LOGGER.warning("No earliest datestamp found in Identify response")
                return datetime(2000, 1, 1)

        except Exception as e:
            LOGGER.error(f"Error getting earliest datestamp: {str(e)}")
            return datetime(2000, 1, 1)

    def get_datetime_path(self, date_key):
        """Get the date path for S3 storage from a date string (YYYY-MM-DD)"""
        dt = parse_datestamp(date_key)
        return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        # Re-raise the last real exception, not tenacity.RetryError: classify_error and
        # last_error_message need the HTTP status (oxjob #1425 H1: 159 OJS rows read
        # "RetryError[... HTTPError]" and every 403 WAF block was filed as connection_error).
        reraise=True,
        before=tenacity.before_log(LOGGER, logging.INFO),
        after=tenacity.after_log(LOGGER, logging.INFO)
    )
    def _make_oai_request(self, sickle, **kwargs):
        """Wrapper for OAI-PMH requests with retry logic"""
        return sickle.ListRecords(**kwargs)


# =============================================================================
# SICKLE OAI-PMH CLIENT CUSTOMIZATIONS
# =============================================================================

class MyOAIItemIterator(OAIItemIterator):
    def _get_resumption_token(self):
        resumption_token_element = self.oai_response.xml.find(
            './/' + self.sickle.oai_namespace + 'resumptionToken')
        if resumption_token_element is None:
            return None

        token = resumption_token_element.text
        cursor = resumption_token_element.attrib.get('cursor', None)
        complete_list_size = resumption_token_element.attrib.get('completeListSize', None)
        expiration_date = resumption_token_element.attrib.get('expirationDate', None)

        return ResumptionToken(
            token=token,
            cursor=cursor,
            complete_list_size=complete_list_size,
            expiration_date=expiration_date
        )


class OSTIItemIterator(MyOAIItemIterator):
    """Special handling for OSTI which needs metadataPrefix included with resumptionToken."""

    def _next_response(self):
        params = self.params
        if self.resumption_token:
            params = {
                'resumptionToken': self.resumption_token.token,
                'verb': self.verb,
                'metadataPrefix': params.get('metadataPrefix')
            }
        self.oai_response = self.sickle.harvest(**params)
        error = self.oai_response.xml.find('.//' + self.sickle.oai_namespace + 'error')
        if error is not None:
            code = error.attrib.get('code', 'UNKNOWN')
            description = error.text or ''
            try:
                raise getattr(oaiexceptions, code[0].upper() + code[1:])(description)
            except AttributeError:
                raise oaiexceptions.OAIError(description)
        self.resumption_token = self._get_resumption_token()
        self._items = self.oai_response.xml.iterfind(
            './/' + self.sickle.oai_namespace + self.element)


class MySickle(Sickle):
    """Custom Sickle client with retry logic and special handling."""

    DEFAULT_RETRY_SECONDS = 5

    def __init__(self, *args, **kwargs):
        self.metrics_logger = None
        self.deadline = None  # epoch seconds; set by call_pmh_endpoint for first harvests
        self.http_method = kwargs.get('http_method', 'GET')
        kwargs['max_retries'] = kwargs.get('max_retries', 3)
        if 'osti.gov/oai' in args[0]:
            kwargs['timeout'] = (30, 300)
        if 'irdb.nii.ac.jp' in args[0]:
            kwargs['timeout'] = (120, 600)
        if 'et.ippt.pan.pl' in args[0]:
            # Engineering Transactions: ~100 s per OAI response from the harvester (oxjob #1407/#1417)
            kwargs['timeout'] = (60, 300)
        self.logger = get_thread_logger()
        super(MySickle, self).__init__(*args, **kwargs)

    def set_metrics_logger(self, metrics_logger):
        self.metrics_logger = metrics_logger

    def _check_deadline(self, wait=0):
        """Stop instead of starting a request or a sleep that would end past the deadline."""
        if self.deadline is not None and time() + wait > self.deadline:
            raise FirstHarvestDeadline("deadline reached during OAI request retries")

    @staticmethod
    def _retry_after(value, fallback):
        try:
            return min(max(int(value), 0), MAX_RETRY_AFTER_SECONDS)
        except (TypeError, ValueError):
            return fallback

    # oxjob #1418: some OJS installs print PHP notices as HTML *before* the XML declaration, e.g. Maestro y
    # Sociedad (OJS 3.3.0.15): "Array to string conversion in PKPString.inc.php" once per array-valued subtitle,
    # 381 records. The document after them is intact. Drop the prefix, but only when it consists entirely of
    # PHP's display_errors blocks and the XML declaration follows; any other junk still fails as before.
    _PHP_NOTICE_BLOCK = re.compile(
        rb'\s*<br\s*/?>\s*<b>(?:Notice|Warning|Deprecated|Strict Standards)</b>:[^<]{0,1000}'
        rb'(?:<b>[^<]{0,500}</b>[^<]{0,100}){0,2}<br\s*/?>',
        re.IGNORECASE)

    def _strip_php_notice_prefix(self, http_response):
        content = http_response.content
        start = content.find(b'<?xml')
        if start <= 0 or not content[:20].lstrip().lower().startswith(b'<br'):
            return
        prefix = content[:start]
        pos, blocks = 0, 0
        while pos < len(prefix):
            m = self._PHP_NOTICE_BLOCK.match(prefix, pos)
            if not m:
                break
            pos, blocks = m.end(), blocks + 1
        if blocks == 0 or prefix[pos:].strip():
            return
        http_response._content = content[start:]
        LOGGER.warning(f"Stripped {blocks} PHP notice(s) before the XML declaration from {self.endpoint}")

    def harvest(self, **kwargs):
        headers = {'User-Agent': 'OpenAlexHarvester/1.0 (+https://help.openalex.org/how-to/repositories/; mailto:support@openalex.org)'}
        retry_wait = self.DEFAULT_RETRY_SECONDS
        attempt = 0
        blocked_attempts = 0

        while attempt < self.max_retries:
            attempt += 1
            self._check_deadline()
            try:
                host_rate_limiter.pace(self.endpoint)
                if self.http_method == 'GET':
                    payload_str = "&".join(f"{k}={v}" for k, v in kwargs.items())
                    url = f"{self.endpoint}?{payload_str}"
                    if "doaj.org" in self.endpoint:
                        doaj_api_key = os.getenv("DOAJ_API_KEY")
                        if doaj_api_key:
                            url += f"&api_key={doaj_api_key}"
                    http_response = _tls_tolerant_request('get', url, headers=headers, **self.request_args)
                else:
                    http_response = _tls_tolerant_request('post', self.endpoint, headers=headers, data=kwargs, **self.request_args)

                if self.metrics_logger:
                    self.metrics_logger.update_url(http_response.url)

                if http_response.status_code == 422 and 'zenodo.org' in self.endpoint:
                    self.logger.info("Zenodo returned 422 - treating as no records available")
                    empty_response = '<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><responseDate>2024-11-11T14:30:00Z</responseDate><request verb="ListRecords">' + self.endpoint + '</request><error code="noRecordsMatch">No matching records found</error></OAI-PMH>'
                    http_response._content = empty_response.encode('utf-8')
                    http_response.status_code = 200
                elif http_response.status_code == 503:
                    retry_after = http_response.headers.get('Retry-After')
                    if retry_after:
                        retry_wait = self._retry_after(retry_after, self.DEFAULT_RETRY_SECONDS)
                    else:
                        retry_wait = min(retry_wait * 2, 60)
                    self.logger.info(f"HTTP 503! Retrying after {retry_wait} seconds...")
                    self._check_deadline(retry_wait)
                    sleep(retry_wait)
                    continue
                elif http_response.status_code == 403 and blocked_attempts < len(BLOCKED_RETRY_WAITS):
                    # Cloudflare-style bot/rate action: slow this host down for the rest of the run
                    # and try again after a real pause (oxjob #1425). A 403 on every retry still
                    # raises below and classifies as 'blocked'.
                    wait = self._retry_after(http_response.headers.get('Retry-After'),
                                             BLOCKED_RETRY_WAITS[blocked_attempts])
                    interval = host_rate_limiter.mark_slow(self.endpoint)
                    blocked_attempts += 1
                    attempt -= 1  # 403 retries do not consume the generic retries
                    self.logger.warning(f"HTTP 403 from {self.endpoint}: pacing host at {interval:.0f} s/request, "
                                        f"retry {blocked_attempts}/{len(BLOCKED_RETRY_WAITS)} after {wait} s")
                    self._check_deadline(wait)
                    sleep(wait)
                    continue
                elif http_response.status_code == 429:
                    retry_after = http_response.headers.get('Retry-After')
                    if retry_after:
                        retry_wait = self._retry_after(retry_after, self.DEFAULT_RETRY_SECONDS)
                    else:
                        retry_wait = min(retry_wait * 2, 60)

                    self.logger.warning(f"HTTP 429 Too Many Requests. Retrying after {retry_wait} seconds...")
                    self._check_deadline(retry_wait)
                    sleep(retry_wait)
                    continue

                http_response.raise_for_status()

                if not http_response.text.strip():
                    raise Exception("Empty response received from server")
                self._strip_php_notice_prefix(http_response)
                response_start = http_response.text.strip()[:100].lower()
                if not (response_start.startswith('<?xml') or response_start.startswith('<oai-pmh')):
                    raise Exception(f"Invalid XML response: {http_response.text[:100]}")

                if self.encoding:
                    http_response.encoding = self.encoding

                return OAIResponse(http_response, params=kwargs)

            except FirstHarvestDeadline:
                raise
            except Exception as e:
                LOGGER.error(f"Error harvesting from {self.endpoint}: {str(e)}")
                still_403 = getattr(getattr(e, 'response', None), 'status_code', None) == 403
                if attempt >= self.max_retries or (still_403 and blocked_attempts >= len(BLOCKED_RETRY_WAITS)):
                    raise
                self.logger.info(f"Retrying after {retry_wait} seconds due to error...")
                self._check_deadline(retry_wait)
                sleep(retry_wait)

        raise Exception(f"Failed to harvest after {self.max_retries} retries")


# =============================================================================
# TLS COMPATIBILITY FALLBACK (oxjob #1425 H3)
# =============================================================================
# Two failures the dyno's OpenSSL 3 rejects while browsers (and macOS) connect fine:
#   1. the server sends only its leaf certificate, without the intermediate
#      ("unable to get local issuer certificate"; e.g. ojs.unesp.br, polilog.pl,
#      www.journals.ufrpe.br): browsers fetch the intermediate from the leaf's AIA
#      caIssuers URL, so we do the same;
#   2. the server only speaks legacy TLS options (SSLV3_ALERT_HANDSHAKE_FAILURE /
#      UNSAFE_LEGACY_RENEGOTIATION; e.g. cendie.abc.gob.ar): allow legacy renegotiation and
#      SECLEVEL=1 ciphers.
# The fallback runs only after a normal verified request raised SSLError, verification stays
# on (only the chain is completed), and one context per host is cached.

_OP_LEGACY_SERVER_CONNECT = getattr(ssl, 'OP_LEGACY_SERVER_CONNECT', 0x4)
_tls_fallback_sessions = {}
_tls_fallback_lock = threading.Lock()


class _ContextAdapter(HTTPAdapter):
    def __init__(self, ssl_context, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs['ssl_context'] = self._ssl_context
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def _aia_intermediates_pem(host, port=443, max_depth=3):
    """Follow AIA caIssuers from the server's leaf certificate; return PEM intermediates."""
    pems = []
    try:
        der = ssl.PEM_cert_to_DER_cert(ssl.get_server_certificate((host, port), timeout=15))
    except Exception as e:
        LOGGER.info(f"TLS fallback: could not read leaf certificate of {host}: {e}")
        return pems
    for _ in range(max_depth):
        cert = x509.load_der_x509_certificate(der)
        if cert.issuer == cert.subject:
            break
        try:
            aia = cert.extensions.get_extension_for_oid(ExtensionOID.AUTHORITY_INFORMATION_ACCESS).value
        except x509.ExtensionNotFound:
            break
        urls = [d.access_location.value for d in aia
                if d.access_method == AuthorityInformationAccessOID.CA_ISSUERS]
        if not urls:
            break
        try:
            der = requests.get(urls[0], timeout=15).content
            issuer = x509.load_der_x509_certificate(der)
        except Exception as e:
            LOGGER.info(f"TLS fallback: could not fetch issuer {urls[0]} for {host}: {e}")
            break
        if issuer.issuer == issuer.subject:
            break  # reached a root; roots come from certifi, never from AIA
        pems.append(ssl.DER_cert_to_PEM_cert(der))
    return pems


def _tls_fallback_session(url):
    host = urlparse(url).hostname
    with _tls_fallback_lock:
        if host in _tls_fallback_sessions:
            return _tls_fallback_sessions[host]
    ctx = ssl.create_default_context(cafile=certifi.where())
    ctx.options |= _OP_LEGACY_SERVER_CONNECT
    ctx.set_ciphers('DEFAULT:@SECLEVEL=1')
    for pem in _aia_intermediates_pem(host):
        ctx.load_verify_locations(cadata=pem)
    session = requests.Session()
    adapter = _ContextAdapter(ctx)
    session.mount('https://', adapter)
    with _tls_fallback_lock:
        _tls_fallback_sessions.setdefault(host, session)
        return _tls_fallback_sessions[host]


def _tls_tolerant_request(method, url, **kwargs):
    """requests.get/post; on SSLError retry once through a per-host compatibility session."""
    try:
        return getattr(requests, method)(url, **kwargs)
    except requests.exceptions.SSLError as e:
        # also covers an http URL redirected to an https host with a broken chain: the
        # fallback session follows the redirect with its https adapter
        LOGGER.info(f"TLS fallback for {urlparse(url).hostname}: {str(e)[:160]}")
        return getattr(_tls_fallback_session(url), method)(url, **kwargs)


def _get_my_sickle(repo_pmh_url, metrics_logger=None, timeout=(REQUEST_TIMEOUT, READ_TIMEOUT)):
    """Create a customized Sickle client for the given URL."""
    if not repo_pmh_url:
        return None

    # Route all harvests through the QuotaGuard static IP so repositories see
    # a consistent source address (previously only citeseerx, pure.coventry,
    # and irdb.nii.ac.jp were proxied).
    proxy_url = os.getenv("QUOTAGUARDSTATIC_URL") or os.getenv("STATIC_IP_PROXY")
    proxies = {"https": proxy_url, "http": proxy_url} if proxy_url else {}
    iterator = OSTIItemIterator if 'osti.gov/oai' in repo_pmh_url else MyOAIItemIterator
    sickle = MySickle(repo_pmh_url, proxies=proxies, timeout=timeout, iterator=iterator)

    if metrics_logger:
        sickle.set_metrics_logger(metrics_logger)

    return sickle


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_date(date_str: str):
    """Parse a date string from command line argument."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def format_oai_datestamp(d, date_format):
    """Format a from/until value. strftime('%Y') is not zero-padded on Linux for
    years < 1000, and DSpace 7 endpoints advertise earliestDatestamp years like
    0002; an unpadded 'from' fails their granularity check (oxjob #953)."""
    ymd = f"{d.year:04d}-{d.month:02d}-{d.day:02d}"
    if 'T' not in date_format:
        return ymd
    hms = d.strftime('%H:%M:%S') if isinstance(d, datetime) else '00:00:00'
    return f"{ymd}T{hms}Z"


def parse_datestamp(datestamp_str):
    """Parse an OAI-PMH datestamp."""
    try:
        if 'T' in datestamp_str:
            return datetime.strptime(datestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        else:
            return datetime.strptime(datestamp_str, '%Y-%m-%d')
    except ValueError:
        LOGGER.warning(f"Could not parse datestamp: {datestamp_str}")
        return datetime(2000, 1, 1)


def classify_error(error: Exception) -> str:
    """
    Classify an exception into a health status category.

    Returns one of: 'first_harvest_timeout', 'timeout', 'connection_error', 'blocked',
    'malformed', 'oai_error'
    """
    if isinstance(error, tenacity.RetryError) and error.last_attempt.failed:
        error = error.last_attempt.exception()
    error_str = str(error).lower()
    error_type = type(error).__name__
    status_code = getattr(getattr(error, 'response', None), 'status_code', None)

    if isinstance(error, FirstHarvestDeadline):
        return 'first_harvest_timeout'
    elif isinstance(error, requests.exceptions.Timeout):
        return 'timeout'
    elif isinstance(error, requests.exceptions.ConnectionError):
        return 'connection_error'
    elif isinstance(error, requests.exceptions.HTTPError):
        if status_code in (401, 403, 429) or '403' in error_str or '401' in error_str:
            return 'blocked'
        return 'connection_error'
    elif 'timeout' in error_str:
        return 'timeout'
    elif 'connection' in error_str or 'refused' in error_str:
        return 'connection_error'
    elif 'blocked' in error_str or 'forbidden' in error_str:
        return 'blocked'
    elif 'xml' in error_str or 'parse' in error_str:
        return 'malformed'
    elif isinstance(error, oaiexceptions.OAIError):
        return 'oai_error'
    else:
        return 'connection_error'  # Default fallback


# =============================================================================
# MAIN HARVESTING LOGIC
# =============================================================================

def harvest_single_endpoint(
    endpoint_id: str,
    pmh_url: str,
    s3_bucket: str,
    start_date,
    end_date
) -> Tuple[str, str, float, Optional[str]]:
    """
    Harvest a single endpoint with rate limiting and health tracking.

    Args:
        endpoint_id: The endpoint ID to harvest
        pmh_url: The PMH URL for rate limiting (avoids loading endpoint before acquiring lock)
        s3_bucket: S3 bucket for storing records
        start_date: Start date for harvesting
        end_date: End date for harvesting

    Returns:
        Tuple of (endpoint_id, status, response_time, error_message)
    """
    logger = get_thread_logger()
    start_time = time()

    try:
        # Apply per-host rate limiting
        with host_rate_limiter.limit(pmh_url):
            logger.info(f"Starting harvest for endpoint: {pmh_url}")

            # Use context manager to ensure session is always closed
            with Session() as session:
                try:
                    # Load endpoint fresh within this thread's session
                    endpoint = StateManager.get_endpoint(endpoint_id, session)
                    if not endpoint:
                        logger.error(f"Endpoint not found: {endpoint_id}")
                        return (endpoint_id, 'connection_error', 0.0, "Endpoint not found")

                    harvester = EndpointHarvester(endpoint, session)
                    harvester.harvest(s3_bucket=s3_bucket, first=start_date, last=end_date)

                    response_time = time() - start_time
                    logger.info(f"Completed {'first ' if start_date is None else ''}harvest for endpoint: {pmh_url}: "
                                f"{harvester.metrics.record_count} records in {response_time:.2f}s")

                    # A first harvest (no checkpoint, no from) that finds nothing is
                    # not a success: the feed is empty or broken for us, and 'success'
                    # hid it. call_pmh_endpoint swallows NoRecordsMatch, so check the count.
                    status, error_message = 'success', None
                    if start_date is None and harvester.metrics.record_count == 0:
                        status = 'empty'
                        error_message = EMPTY_FIRST_HARVEST_MSG

                    # Update health status (using same session)
                    StateManager.update_health_status(
                        endpoint, session,
                        status=status,
                        response_time=response_time,
                        error_message=error_message,
                        record_count=harvester.metrics.record_count
                    )

                    return (endpoint_id, status, response_time, error_message)

                except NoRecordsMatch:
                    # No records is still a successful connection (unless first harvest)
                    response_time = time() - start_time
                    status = 'empty' if start_date is None else 'success'
                    error_message = EMPTY_FIRST_HARVEST_MSG if status == 'empty' else None
                    # Re-fetch endpoint if needed (in case it wasn't loaded due to early exception)
                    if 'endpoint' not in locals():
                        endpoint = StateManager.get_endpoint(endpoint_id, session)
                    if endpoint:
                        StateManager.update_health_status(
                            endpoint, session,
                            status=status,
                            response_time=response_time,
                            error_message=error_message,
                            record_count=0
                        )
                    return (endpoint_id, status, response_time, error_message)

                except Exception as e:
                    response_time = time() - start_time
                    error_message = str(e)
                    status = classify_error(e)

                    logger.error(f"Error harvesting endpoint {pmh_url}: {error_message}")

                    try:
                        # Re-fetch endpoint if needed
                        if 'endpoint' not in locals():
                            endpoint = StateManager.get_endpoint(endpoint_id, session)
                        if endpoint:
                            StateManager.update_health_status(
                                endpoint, session,
                                status=status,
                                response_time=response_time,
                                error_message=error_message[:1000],  # Truncate long errors
                                # Partial count retrieved before the error; None if
                                # we failed before the harvester was constructed.
                                record_count=(harvester.metrics.record_count
                                              if 'harvester' in locals() else None)
                            )
                    except Exception as db_error:
                        logger.error(f"Failed to update health status: {db_error}")

                    return (endpoint_id, status, response_time, error_message)
    finally:
        # Force cycle collection. lxml/ElementTree objects from the OAI parsing
        # path have parent/child cycles that depend on Python's cycle collector;
        # under heavy threading the collector lags and memory accumulates.
        gc.collect()


def harvest_single_endpoint_with_date_detection(
    endpoint_id: str,
    pmh_url: str,
    s3_bucket: str,
    start_date,
    end_date
) -> Tuple[str, str, float, Optional[str]]:
    """
    Harvest a single endpoint. start_date is None when --start-date is not provided
    and the endpoint has never been checkpointed; it is passed through as None so
    the first harvest sends no 'from' (see call_pmh_endpoint).

    Args:
        endpoint_id: The endpoint ID to harvest
        pmh_url: The PMH URL for rate limiting
        s3_bucket: S3 bucket for storing records
        start_date: Start date for harvesting, or None for a first harvest (no 'from')
        end_date: End date for harvesting

    Returns:
        Tuple of (endpoint_id, status, response_time, error_message)
    """
    return harvest_single_endpoint(endpoint_id, pmh_url, s3_bucket, start_date, end_date)


def retry_blocked_endpoints(blocked, s3_bucket, end_date, max_workers=RETRY_BLOCKED_WORKERS) -> dict:
    """
    Second, quiet pass over endpoints that ended the run 'blocked' on a 403: few threads, every
    host paced at the maximum interval. Hosts that only 403 under load pass here (oxjob #1425).
    blocked: list of (endpoint_id, pmh_url, start_date). Returns {status: count}.
    """
    logger = logging.getLogger("harvester.main")
    stats = {}
    if not blocked or max_workers <= 0:
        return stats
    logger.info(f"Retrying {len(blocked)} blocked endpoints with {max_workers} workers, hosts paced")
    for _, pmh_url, _ in blocked:
        host_rate_limiter.mark_slow(pmh_url, SLOW_HOST_MAX_INTERVAL)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(harvest_single_endpoint, eid, url, s3_bucket, start, end_date): url
                   for eid, url, start in blocked}
        for future in as_completed(futures):
            try:
                _, status, _, _ = future.result()
            except Exception as e:
                logger.error(f"Blocked retry failed for {futures[future]}: {e}")
                status = 'connection_error'
            stats[status] = stats.get(status, 0) + 1
    logger.info(f"Blocked retry pass: {stats}")
    return stats


def _is_403(error_msg) -> bool:
    return bool(error_msg) and '403' in str(error_msg)


def harvest_all_endpoints(
    endpoint_data: List[Tuple[str, str]],
    s3_bucket: str,
    start_date,
    end_date,
    max_workers: int = MAX_WORKERS,
    retry_blocked_workers: int = RETRY_BLOCKED_WORKERS
) -> dict:
    """
    Harvest all endpoints in parallel with rate limiting.

    Args:
        endpoint_data: List of (endpoint_id, pmh_url) tuples
        s3_bucket: S3 bucket for storing records
        start_date: Start date for harvesting
        end_date: End date for harvesting
        max_workers: Maximum concurrent threads

    Returns:
        Dict with harvest statistics
    """
    logger = logging.getLogger("harvester.main")
    logger.info(f"Starting parallel harvest of {len(endpoint_data)} endpoints with {max_workers} workers")

    stats = {
        'total': len(endpoint_data),
        'success': 0,
        'blocked': 0,
        'timeout': 0,
        'connection_error': 0,
        'malformed': 0,
        'oai_error': 0,
        'total_time': 0
    }

    start_time = time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                harvest_single_endpoint,
                endpoint_id,
                pmh_url,
                s3_bucket,
                start_date,
                end_date
            ): (endpoint_id, pmh_url)
            for endpoint_id, pmh_url in endpoint_data
        }

        blocked = []
        for future in as_completed(futures):
            endpoint_id, pmh_url = futures[future]
            try:
                result_id, status, response_time, error_msg = future.result()
                stats[status] = stats.get(status, 0) + 1
                if status == 'blocked' and _is_403(error_msg):
                    blocked.append((endpoint_id, pmh_url, start_date))
            except Exception as e:
                logger.error(f"Unexpected error for endpoint {pmh_url}: {e}")
                stats['connection_error'] += 1

    retry_stats = retry_blocked_endpoints(blocked, s3_bucket, end_date, retry_blocked_workers)
    stats['blocked_recovered'] = retry_stats.get('success', 0) + retry_stats.get('empty', 0)
    stats['blocked'] -= stats['blocked_recovered']

    stats['total_time'] = time() - start_time

    logger.info(f"Harvest complete in {stats['total_time']:.2f}s")
    logger.info(f"Results: {stats['success']} success, {stats['blocked']} blocked, "
                f"{stats['timeout']} timeout, {stats['connection_error']} connection errors, "
                f"{stats['malformed']} malformed, {stats['oai_error']} OAI errors")

    return stats


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='OAI-PMH Repository Harvester',
        epilog="""
Examples:
  # Harvest all endpoints (daily job)
  python repositories.py --all-endpoints --n_threads 100

  # Harvest a specific endpoint
  python repositories.py --endpoint-id abc123

  # Harvest with custom date range
  python repositories.py --all-endpoints --start-date 2026-01-01 --end-date 2026-01-15
        """
    )

    parser.add_argument('--endpoint-id', help='Specific endpoint ID to harvest')
    parser.add_argument('--start-date', type=parse_date,
                        help='Start date in YYYY-MM-DD format.')
    parser.add_argument('--end-date', type=parse_date,
                        help='End date in YYYY-MM-DD format.')
    parser.add_argument('--all-endpoints', action='store_true',
                        help='Harvest all harvestable endpoints (recommended for daily job)')
    parser.add_argument('--n_threads', type=int, default=MAX_WORKERS,
                        help=f'Number of concurrent harvesting threads (default: {MAX_WORKERS})')
    parser.add_argument('--health-status',
                        help='With --all-endpoints: only endpoints whose last_health_status is in this '
                             'comma-separated list (e.g. blocked,timeout) for a targeted re-run')
    parser.add_argument('--retry-blocked-threads', type=int, default=RETRY_BLOCKED_WORKERS,
                        help=f'Threads for the end-of-run retry of 403-blocked endpoints, hosts paced '
                             f'(default: {RETRY_BLOCKED_WORKERS}; 0 disables)')

    # Legacy flags (kept for backwards compatibility but deprecated)
    parser.add_argument('--core-endpoints', action='store_true',
                        help='DEPRECATED: Use --all-endpoints instead. All endpoints are now treated equally.')
    parser.add_argument('--reliable-endpoints', action='store_true',
                        help='DEPRECATED: Use --all-endpoints instead. All endpoints are now treated equally.')
    parser.add_argument('--other-endpoints', action='store_true',
                        help='DEPRECATED: Use --all-endpoints instead. All endpoints are now treated equally.')
    parser.add_argument('--abandoned-endpoints', action='store_true',
                        help='DEPRECATED: Use --all-endpoints instead. All endpoints are now treated equally.')

    args = parser.parse_args()

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("harvester.main")

    # Handle deprecated flags
    if any([args.core_endpoints, args.reliable_endpoints, args.other_endpoints, args.abandoned_endpoints]):
        logger.warning("DEPRECATED: Tier-based flags are deprecated. The harvester now treats all endpoints equally.")
        logger.warning("Using --all-endpoints behavior instead.")
        args.all_endpoints = True

    # Load endpoints and extract IDs + URLs (don't pass ORM objects to threads)
    if args.endpoint_id:
        endpoint = StateManager.get_endpoint(args.endpoint_id, db)
        if not endpoint:
            logger.error(f"No endpoint found with ID: {args.endpoint_id}")
            return
        # Extract data we need, then let the ORM object go
        endpoint_data = [(endpoint.id, endpoint.pmh_url, endpoint.most_recent_date_harvested)]
        logger.info(f"Harvesting single endpoint: {endpoint.pmh_url}")
    elif args.all_endpoints:
        statuses = [x.strip() for x in args.health_status.split(',')] if args.health_status else None
        endpoints = StateManager.get_all_harvestable_endpoints(db, statuses)
        endpoint_data = [(e.id, e.pmh_url, e.most_recent_date_harvested) for e in endpoints]
        logger.info(f"Found {len(endpoint_data)} harvestable endpoints" + (f" with status in {statuses}" if statuses else ""))
    else:
        # Default to all endpoints
        endpoints = StateManager.get_all_harvestable_endpoints(db)
        endpoint_data = [(e.id, e.pmh_url, e.most_recent_date_harvested) for e in endpoints]
        logger.info(f"Found {len(endpoint_data)} harvestable endpoints (use --all-endpoints to suppress this message)")

    if not endpoint_data:
        logger.warning("No endpoints to harvest")
        return

    # Determine date range
    end_date = args.end_date or (datetime.now(timezone.utc).date() - timedelta(days=1))

    if args.start_date:
        # Use specified start date for all endpoints
        start_date = args.start_date

        # Extract just (id, url) for harvest_all_endpoints
        harvest_data = [(eid, url) for eid, url, _ in endpoint_data]

        # Harvest all endpoints in parallel
        stats = harvest_all_endpoints(
            endpoint_data=harvest_data,
            s3_bucket=S3_BUCKET,
            start_date=start_date,
            end_date=end_date,
            max_workers=args.n_threads,
            retry_blocked_workers=args.retry_blocked_threads
        )
    else:
        # Compute per-endpoint start dates based on most_recent_date_harvested
        # Start date computation now happens inside each thread for new endpoints
        logger.info("Harvesting with per-endpoint date ranges...")

        with ThreadPoolExecutor(max_workers=args.n_threads) as executor:
            futures = {}
            first_dates = {}

            for endpoint_id, pmh_url, most_recent in endpoint_data:
                if most_recent:
                    first_date = most_recent.date() - timedelta(days=1)
                else:
                    # Never checkpointed: first harvest, no 'from' (see call_pmh_endpoint)
                    first_date = None
                first_dates[endpoint_id] = first_date

                future = executor.submit(
                    harvest_single_endpoint_with_date_detection,
                    endpoint_id,
                    pmh_url,
                    S3_BUCKET,
                    first_date,
                    end_date
                )
                futures[future] = (endpoint_id, pmh_url)

            stats = {
                'total': len(endpoint_data),
                'success': 0,
                'empty': 0,
                'first_harvest_timeout': 0,
                'blocked': 0,
                'timeout': 0,
                'connection_error': 0,
                'malformed': 0,
                'oai_error': 0
            }

            blocked = []
            for future in as_completed(futures):
                endpoint_id, pmh_url = futures[future]
                try:
                    result_id, status, response_time, error_msg = future.result()
                    stats[status] = stats.get(status, 0) + 1
                    if status == 'blocked' and _is_403(error_msg):
                        blocked.append((endpoint_id, pmh_url, first_dates[endpoint_id]))
                except Exception as e:
                    logger.error(f"Harvesting task failed for {pmh_url}: {str(e)}")
                    stats['connection_error'] += 1

        if not args.endpoint_id:
            retry_stats = retry_blocked_endpoints(blocked, S3_BUCKET, end_date, args.retry_blocked_threads)
            stats['blocked_recovered'] = retry_stats.get('success', 0) + retry_stats.get('empty', 0)
            stats['blocked'] -= stats['blocked_recovered']

        logger.info(f"Harvest complete: {stats}")


if __name__ == "__main__":
    main()
