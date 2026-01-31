"""
Repository OAI-PMH Harvester

This module harvests metadata records from OAI-PMH repository endpoints
and saves them to S3 for processing by the OpenAlex pipeline.

SIMPLIFIED ARCHITECTURE (January 2026):
The harvester runs a single daily job that harvests ALL ~6,000 endpoints
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
    Values: 'success', 'blocked', 'timeout', 'connection_error', 'malformed', 'oai_error'
  - last_health_check: Timestamp of last harvest attempt
  - last_response_time: Response time in seconds
  - last_error_message: Error details if harvest failed

PARALLELIZATION:
- Uses ThreadPoolExecutor with 100 concurrent workers
- Rate-limited to max 3 concurrent requests per host (prevents overloading)
- 15-second timeout per request
- Total runtime: ~15 minutes for all ~6,000 endpoints
"""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import logging
import os
import threading
from time import sleep, time
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import defusedxml.ElementTree as DefusedET
from defusedxml.ElementTree import ParseError
import requests
import shortuuid
from sickle import Sickle, oaiexceptions
from sickle.iterator import OAIItemIterator
from sickle.models import ResumptionToken
from sickle.oaiexceptions import NoRecordsMatch
from sickle.response import OAIResponse
from sqlalchemy import Column, Text, DateTime, Boolean, Interval, Float, select
import tenacity
import xml.etree.ElementTree as ET

from common import Base, LOGGER, S3_BUCKET, Session, db


# =============================================================================
# CONFIGURATION
# =============================================================================

# Parallelization settings
MAX_WORKERS = 100           # Total concurrent harvesting threads
MAX_PER_HOST = 3            # Max concurrent requests to same host
REQUEST_TIMEOUT = 15        # Seconds before giving up on a request
BATCH_SIZE = 5000           # Records per S3 file


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
    __tablename__ = "endpoint"

    id = Column(Text, primary_key=True)
    id_old = Column(Text)
    pmh_url = Column(Text)
    pmh_set = Column(Text)
    last_harvest_started = Column(DateTime)
    last_harvest_finished = Column(DateTime)
    most_recent_date_harvested = Column(DateTime)
    earliest_timestamp = Column(DateTime)
    email = Column(Text)
    error = Column(Text)
    repo_request_id = Column(Text)
    harvest_identify_response = Column(Text)
    harvest_test_recent_dates = Column(Text)
    sample_pmh_record = Column(Text)
    contacted = Column(DateTime)
    contacted_text = Column(Text)
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
    last_health_status = Column(Text)     # success, blocked, timeout, connection_error, malformed, oai_error
    last_health_check = Column(DateTime)  # when we last tested
    last_response_time = Column(Float)    # seconds
    last_error_message = Column(Text)     # details if failed

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
    def get_all_harvestable_endpoints(session) -> List[Endpoint]:
        """
        Get all endpoints that should be harvested.

        Criteria:
        - ready_to_run == True
        - in_walden == True (part of OpenAlex pipeline)

        Unlike the old tiering system, this returns ALL harvestable endpoints.
        The parallelization handles the load efficiently.

        Uses yield_per() to avoid loading all 6K+ rows into memory at once.
        """
        stmt = select(Endpoint).filter(
            Endpoint.ready_to_run == True,
            Endpoint.in_walden == True
        ).execution_options(yield_per=500)
        return list(session.execute(stmt).scalars())

    @staticmethod
    def update_health_status(
        endpoint: Endpoint,
        session,
        status: str,
        response_time: float,
        error_message: Optional[str] = None
    ):
        """
        Update endpoint health tracking columns after a harvest attempt.

        Args:
            endpoint: The endpoint to update
            session: Database session
            status: One of 'success', 'blocked', 'timeout', 'connection_error',
                   'malformed', 'oai_error'
            response_time: How long the request took in seconds
            error_message: Error details if status is not 'success'
        """
        endpoint.last_health_status = status
        endpoint.last_health_check = datetime.now(timezone.utc)
        endpoint.last_response_time = response_time
        endpoint.last_error_message = error_message
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
        and does NOT update any state.
        """
        first = first or datetime(2000, 1, 1).date()
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

    def call_pmh_endpoint(self, s3_client, s3_bucket, first, last):
        from_date = first.strftime(self.date_format)
        until_date = last.strftime(self.date_format)

        args = {
            'metadataPrefix': self.state.metadata_prefix,
            'from': from_date,
            'until': until_date
        }

        if self.state.pmh_set:
            args["set"] = self.state.pmh_set

        self.logger.info(f"OAI-PMH request parameters: {args}")

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, metrics_logger=self.metrics)
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

            for record in records:
                self.metrics.increment_count()
                self.metrics.update_datestamp(record.header.datestamp)

                datestamp = record.header.datestamp
                date_key = datestamp.split('T')[0] if 'T' in datestamp else datestamp

                # Checkpoint when date changes
                if current_date_processing and date_key > current_date_processing:
                    if current_date_processing in records_by_date and records_by_date[current_date_processing]:
                        self.save_batch(s3_client, s3_bucket, batch_counters[current_date_processing],
                                        records_by_date[current_date_processing], current_date_processing)
                        records_by_date[current_date_processing] = []

                    checkpoint_dt = parse_datestamp(current_date_processing)
                    if not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested:
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
                    self.save_batch(s3_client, s3_bucket, batch_counters[date_key],
                                    records_by_date[date_key], date_key)
                    records_by_date[date_key] = []
                    batch_counters[date_key] += 1

            # Final cleanup
            if current_date_processing and records_by_date.get(current_date_processing):
                self.save_batch(s3_client, s3_bucket, batch_counters[current_date_processing],
                                records_by_date[current_date_processing], current_date_processing)

            if current_date_processing:
                checkpoint_dt = parse_datestamp(current_date_processing)
                if not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested:
                    self.state.most_recent_date_harvested = checkpoint_dt
                    self.db.merge(self.state)
                    self.db.commit()
                    self.logger.info(f"Checkpoint: {current_date_processing} complete (final)")

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
                    # We don't checkpoint the current date since it may be incomplete
                    checkpoint_dt = parse_datestamp(current_date_processing) - timedelta(days=1)
                    if checkpoint_dt > datetime(2000, 1, 1):
                        if not self.state.most_recent_date_harvested or checkpoint_dt > self.state.most_recent_date_harvested:
                            self.state.most_recent_date_harvested = checkpoint_dt
                            self.db.merge(self.state)
                            self.db.commit()
                            self.logger.info(f"Saved checkpoint at {checkpoint_dt.date()} before error")
            except Exception as save_error:
                self.logger.warning(f"Failed to save partial progress: {save_error}")

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

            record_ids = sorted([r.header.identifier for r in records])
            content_hash = hashlib.md5("".join(record_ids).encode()).hexdigest()[:12]
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
        client = boto3.client('s3')
        try:
            yield client
        finally:
            pass

    def detect_date_format(self):
        """Detect if the repository requires a full timestamp format or just 'YYYY-MM-DD'."""
        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=10)
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
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=10)
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
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
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
        self.http_method = kwargs.get('http_method', 'GET')
        kwargs['max_retries'] = kwargs.get('max_retries', 3)
        if 'osti.gov/oai' in args[0]:
            kwargs['timeout'] = (30, 300)
        self.logger = get_thread_logger()
        super(MySickle, self).__init__(*args, **kwargs)

    def set_metrics_logger(self, metrics_logger):
        self.metrics_logger = metrics_logger

    def harvest(self, **kwargs):
        headers = {'User-Agent': 'OAIHarvester/1.0'}
        retry_wait = self.DEFAULT_RETRY_SECONDS

        for attempt in range(self.max_retries):
            try:
                if self.http_method == 'GET':
                    payload_str = "&".join(f"{k}={v}" for k, v in kwargs.items())
                    url = f"{self.endpoint}?{payload_str}"
                    http_response = requests.get(url, headers=headers, **self.request_args)
                else:
                    http_response = requests.post(self.endpoint, headers=headers, data=kwargs, **self.request_args)

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
                        retry_wait = int(retry_after)
                    else:
                        retry_wait = min(retry_wait * 2, 60)
                    self.logger.info(f"HTTP 503! Retrying after {retry_wait} seconds...")
                    sleep(retry_wait)
                    continue
                elif http_response.status_code == 429:
                    retry_after = http_response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            retry_wait = int(retry_after)
                        except ValueError:
                            retry_wait = self.DEFAULT_RETRY_SECONDS
                    else:
                        retry_wait = min(retry_wait * 2, 60)

                    self.logger.warning(f"HTTP 429 Too Many Requests. Retrying after {retry_wait} seconds...")
                    sleep(retry_wait)
                    continue

                http_response.raise_for_status()

                if not http_response.text.strip():
                    raise Exception("Empty response received from server")
                response_start = http_response.text.strip()[:100].lower()
                if not (response_start.startswith('<?xml') or response_start.startswith('<oai-pmh')):
                    raise Exception(f"Invalid XML response: {http_response.text[:100]}")

                if self.encoding:
                    http_response.encoding = self.encoding

                return OAIResponse(http_response, params=kwargs)

            except Exception as e:
                LOGGER.error(f"Error harvesting from {self.endpoint}: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                self.logger.info(f"Retrying after {retry_wait} seconds due to error...")
                sleep(retry_wait)

        raise Exception(f"Failed to harvest after {self.max_retries} retries")


def _get_my_sickle(repo_pmh_url, metrics_logger=None, timeout=REQUEST_TIMEOUT):
    """Create a customized Sickle client for the given URL."""
    if not repo_pmh_url:
        return None

    proxy_url = None
    if any(fragment in repo_pmh_url for fragment in ["citeseerx", "pure.coventry.ac.uk"]):
        proxy_url = os.getenv("STATIC_IP_PROXY")

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

    Returns one of: 'timeout', 'connection_error', 'blocked', 'malformed', 'oai_error'
    """
    error_str = str(error).lower()
    error_type = type(error).__name__

    if isinstance(error, requests.exceptions.Timeout):
        return 'timeout'
    elif isinstance(error, requests.exceptions.ConnectionError):
        return 'connection_error'
    elif isinstance(error, requests.exceptions.HTTPError):
        if '403' in error_str or '401' in error_str:
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
                logger.info(f"Completed harvest for endpoint: {pmh_url} in {response_time:.2f}s")

                # Update health status (using same session)
                StateManager.update_health_status(
                    endpoint, session,
                    status='success',
                    response_time=response_time
                )

                return (endpoint_id, 'success', response_time, None)

            except NoRecordsMatch:
                # No records is still a successful connection
                response_time = time() - start_time
                # Re-fetch endpoint if needed (in case it wasn't loaded due to early exception)
                if 'endpoint' not in locals():
                    endpoint = StateManager.get_endpoint(endpoint_id, session)
                if endpoint:
                    StateManager.update_health_status(
                        endpoint, session,
                        status='success',
                        response_time=response_time
                    )
                return (endpoint_id, 'success', response_time, None)

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
                            error_message=error_message[:1000]  # Truncate long errors
                        )
                except Exception as db_error:
                    logger.error(f"Failed to update health status: {db_error}")

                return (endpoint_id, status, response_time, error_message)


def harvest_single_endpoint_with_date_detection(
    endpoint_id: str,
    pmh_url: str,
    s3_bucket: str,
    start_date,
    end_date
) -> Tuple[str, str, float, Optional[str]]:
    """
    Harvest a single endpoint, detecting earliest datestamp if start_date is None.

    This is used when --start-date is not provided and the endpoint has never been
    harvested before. The earliest datestamp detection happens inside the thread
    to avoid blocking the main thread.

    Args:
        endpoint_id: The endpoint ID to harvest
        pmh_url: The PMH URL for rate limiting
        s3_bucket: S3 bucket for storing records
        start_date: Start date for harvesting, or None to detect earliest datestamp
        end_date: End date for harvesting

    Returns:
        Tuple of (endpoint_id, status, response_time, error_message)
    """
    logger = get_thread_logger()

    if start_date is None:
        # Detect earliest datestamp for new endpoints
        try:
            with Session() as session:
                endpoint = StateManager.get_endpoint(endpoint_id, session)
                if endpoint:
                    harvester = EndpointHarvester(endpoint, session)
                    start_date = harvester.get_earliest_datestamp().date()
                else:
                    start_date = datetime(2000, 1, 1).date()
        except Exception as e:
            logger.warning(f"Failed to get earliest datestamp for {pmh_url}: {e}")
            start_date = datetime(2000, 1, 1).date()

    return harvest_single_endpoint(endpoint_id, pmh_url, s3_bucket, start_date, end_date)


def harvest_all_endpoints(
    endpoint_data: List[Tuple[str, str]],
    s3_bucket: str,
    start_date,
    end_date,
    max_workers: int = MAX_WORKERS
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

        for future in as_completed(futures):
            endpoint_id, pmh_url = futures[future]
            try:
                result_id, status, response_time, error_msg = future.result()
                stats[status] = stats.get(status, 0) + 1
            except Exception as e:
                logger.error(f"Unexpected error for endpoint {pmh_url}: {e}")
                stats['connection_error'] += 1

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
        endpoints = StateManager.get_all_harvestable_endpoints(db)
        endpoint_data = [(e.id, e.pmh_url, e.most_recent_date_harvested) for e in endpoints]
        logger.info(f"Found {len(endpoint_data)} harvestable endpoints")
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
            max_workers=args.n_threads
        )
    else:
        # Compute per-endpoint start dates based on most_recent_date_harvested
        # Start date computation now happens inside each thread for new endpoints
        logger.info("Harvesting with per-endpoint date ranges...")

        with ThreadPoolExecutor(max_workers=args.n_threads) as executor:
            futures = {}

            for endpoint_id, pmh_url, most_recent in endpoint_data:
                if most_recent:
                    first_date = most_recent.date() - timedelta(days=1)
                else:
                    # For new endpoints, we'll compute earliest datestamp inside the thread
                    # to avoid blocking the main thread. Pass None and handle in worker.
                    first_date = None

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
                'blocked': 0,
                'timeout': 0,
                'connection_error': 0,
                'malformed': 0,
                'oai_error': 0
            }

            for future in as_completed(futures):
                endpoint_id, pmh_url = futures[future]
                try:
                    result_id, status, response_time, error_msg = future.result()
                    stats[status] = stats.get(status, 0) + 1
                except Exception as e:
                    logger.error(f"Harvesting task failed for {pmh_url}: {str(e)}")
                    stats['connection_error'] += 1

        logger.info(f"Harvest complete: {stats}")


if __name__ == "__main__":
    main()
