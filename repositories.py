import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import logging
import os
import threading
from time import sleep, time
from typing import Optional, List


import boto3
from botocore.exceptions import BotoCoreError, ClientError
from defusedxml.ElementTree import ParseError
import requests
import shortuuid
from sickle import Sickle, oaiexceptions
from sickle.iterator import OAIItemIterator
from sickle.models import ResumptionToken
from sickle.oaiexceptions import NoRecordsMatch
from sickle.response import OAIResponse
from sqlalchemy import Column, Text, DateTime, Boolean, Interval, and_, or_, select, func
import tenacity
import xml.etree.ElementTree as ET

from sqlalchemy.orm import selectinload

from common import Base, LOGGER, S3_BUCKET, Session, db


class Endpoint(Base):
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
    retry_interval = Column(Interval)
    retry_at = Column(DateTime)
    is_core = Column(Boolean)
    in_walden = Column(Boolean)

    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)
        if not self.id:
            self.id = shortuuid.uuid()[0:20].lower()
        if not self.metadata_prefix:
            self.metadata_prefix = 'oai_dc'


class StateManager:
    @staticmethod
    def get_endpoint(endpoint_id: str, session) -> Optional[Endpoint]:
        stmt = select(Endpoint).options(selectinload('*')).filter_by(id=endpoint_id)
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def update_endpoint_state(state: Endpoint, session):
        session.merge(state)
        session.commit()

    @staticmethod
    def get_core_endpoints(session) -> List[Endpoint]:
        now = datetime.now(timezone.utc)
        stmt = select(Endpoint).options(selectinload('*')).filter(
            Endpoint.ready_to_run == True,
            or_(Endpoint.retry_at == None, Endpoint.retry_at <= now),
            Endpoint.is_core == True,
            Endpoint.in_walden == True
        )
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def get_reliable_non_core_endpoints(session) -> List[Endpoint]:
        """
        Get non-core endpoints that have been working properly recently.
        Criteria:
        - Not core endpoints but ready to run
        - Have successfully completed a harvest within the last 120 days
        - Have a reasonable retry interval (not too long)
        """
        now = datetime.now(timezone.utc)
        last_harvested_cutoff = now - timedelta(days=180)
        recent_cutoff = now - timedelta(hours=1)  # Consider 1 hour as "recent"

        stmt = select(Endpoint).options(selectinload('*')).filter(
            Endpoint.ready_to_run == True,
            or_(Endpoint.retry_at == None, Endpoint.retry_at <= now),
            or_(Endpoint.is_core == False, Endpoint.is_core == None),
            Endpoint.in_walden == True,
            Endpoint.last_harvest_finished != None,
            Endpoint.last_harvest_finished > last_harvested_cutoff,
            Endpoint.most_recent_date_harvested != None,
            Endpoint.most_recent_date_harvested > last_harvested_cutoff,
            Endpoint.retry_interval < timedelta(days=7),
            or_(
                Endpoint.last_harvest_started == None,
                and_(
                    Endpoint.last_harvest_started < recent_cutoff,
                    or_(
                        Endpoint.last_harvest_finished != None,
                        Endpoint.last_harvest_started < recent_cutoff - timedelta(hours=3)
                        # Assume stalled after 4 hours
                    )
                )
            )
        ).order_by(Endpoint.last_harvest_finished.desc())

        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def get_other_endpoints(session) -> List[Endpoint]:
        """
        Get remaining non-core endpoints that are not in the reliable category.
        These are endpoints that we still want to try but might be problematic.
        """
        now = datetime.now(timezone.utc)
        recent_cutoff = now - timedelta(hours=1)  # Consider 1 hour as "recent"

        reliable_ids = [ep.id for ep in StateManager.get_reliable_non_core_endpoints(session)]

        stmt = select(Endpoint).options(selectinload('*')).filter(
            Endpoint.ready_to_run == True,
            or_(Endpoint.retry_at == None, Endpoint.retry_at <= now),
            Endpoint.retry_interval < timedelta(days=30),
            Endpoint.is_core == False,
            Endpoint.in_walden == True,
            Endpoint.id.notin_(reliable_ids) if reliable_ids else True,
            or_(
                Endpoint.last_harvest_started == None,
                and_(
                    Endpoint.last_harvest_started < recent_cutoff,
                    or_(
                        Endpoint.last_harvest_finished != None,
                        Endpoint.last_harvest_started < recent_cutoff - timedelta(hours=3)
                    )
                )
            )
        ).order_by(func.random())

        return list(session.execute(stmt).scalars().all())


thread_local = threading.local()

def get_thread_logger():
    if not hasattr(thread_local, "logger"):
        thread_local.logger = logging.getLogger(f"harvester.{threading.current_thread().name}")
        # Only add handler if no handlers exist
        if not thread_local.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            thread_local.logger.addHandler(handler)
            thread_local.logger.setLevel(logging.INFO)
            # Prevent propagation to avoid double logging
            thread_local.logger.propagate = False
    return thread_local.logger

class MetricsLogger:
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


class EndpointHarvester:
    def __init__(self, endpoint: Endpoint, db_session, batch_size=1000):
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
                # One call for the entire range
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

            current_batch = []
            batch_number = 1
            first_record_in_batch = None

            for record in records:
                self.metrics.increment_count()
                self.metrics.update_datestamp(record.header.datestamp)

                if not first_record_in_batch:
                    first_record_in_batch = record

                current_batch.append(record)

                if len(current_batch) >= self.batch_size:
                    self.save_batch(s3_client, s3_bucket, batch_number, current_batch, first_record_in_batch)

                    # clear the current batch and reset tracking variables
                    current_batch.clear()
                    first_record_in_batch = None
                    batch_number += 1

            if current_batch and first_record_in_batch:
                self.save_batch(s3_client, s3_bucket, batch_number, current_batch, first_record_in_batch)

        except NoRecordsMatch:
            self.logger.info(f"No records found for {self.state.pmh_url} with args {args}")

        except Exception as e:
            self.state.error = f"Error harvesting records: {str(e)}"
            LOGGER.exception(f"Error harvesting from {self.state.pmh_url}")
            raise

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type((BotoCoreError, ClientError)),
        before=tenacity.before_log(LOGGER, logging.INFO),
        after=tenacity.after_log(LOGGER, logging.INFO),
        reraise=True
    )
    def save_batch(self, s3_client, s3_bucket, batch_number, records, first_record):
        try:
            date_path = self.get_datetime_path(first_record)
            root = ET.Element('oai_records')

            most_recent_date = parse_datestamp(first_record.header.datestamp)

            for record in records:
                record_elem = ET.fromstring(record.raw)
                root.append(record_elem)

                # check if this record's date is more recent
                record_date = parse_datestamp(record.header.datestamp)
                if record_date > most_recent_date:
                    most_recent_date = record_date

            xml_content = ET.tostring(root, encoding='unicode', method='xml')
            compressed_content = gzip.compress(xml_content.encode('utf-8'))

            timestamp = int(parse_datestamp(first_record.header.datestamp).timestamp())
            object_key = f"repositories/{self.state.id}/{date_path}/page_{batch_number}_{timestamp}.xml.gz"

            metadata = {
                'record_count': str(len(records)),
                'batch_number': str(batch_number),
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

            # save checkpoint after every batch, using date - 1 day
            checkpoint_date = most_recent_date - timedelta(days=1)
            if not self.state.most_recent_date_harvested or checkpoint_date > self.state.most_recent_date_harvested:
                self.state.most_recent_date_harvested = checkpoint_date
                self.db.merge(self.state)
                self.db.commit()
                self.logger.info(f"Saved checkpoint at {checkpoint_date} (original date: {most_recent_date})")

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
        """
        Detect if the repository requires a full timestamp format or just 'YYYY-MM-DD'.
        """
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
            ET.fromstring(record)
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
                        return datetime.strptime(earliest,
                                                          '%Y-%m-%dT%H:%M:%SZ')
                    else:
                        return datetime.strptime(earliest, '%Y-%m-%d')
                except ValueError:
                    LOGGER.warning(
                        f"Could not parse earliest datestamp: {earliest}")
                    return datetime(2000, 1, 1)
            else:
                LOGGER.warning(
                    "No earliest datestamp found in Identify response")
                return datetime(2000, 1, 1)

        except Exception as e:
            LOGGER.error(f"Error getting earliest datestamp: {str(e)}")
            return datetime(2000, 1, 1)

    def get_datetime_path(self, record):
        """Get the date path for S3 storage"""
        datestamp = record.header.datestamp
        dt = parse_datestamp(datestamp)
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


class MyOAIItemIterator(OAIItemIterator):
    def _get_resumption_token(self):
        resumption_token_element = self.oai_response.xml.find(
            './/' + self.sickle.oai_namespace + 'resumptionToken')
        if resumption_token_element is None:
            return None

        token = resumption_token_element.text
        cursor = resumption_token_element.attrib.get('cursor', None)
        complete_list_size = resumption_token_element.attrib.get(
            'completeListSize', None)
        expiration_date = resumption_token_element.attrib.get('expirationDate',
                                                              None)

        return ResumptionToken(
            token=token,
            cursor=cursor,
            complete_list_size=complete_list_size,
            expiration_date=expiration_date
        )


class OSTIItemIterator(MyOAIItemIterator):
    def _next_response(self):
        """Get the next response from the OAI server.

        Special handling for OSTI which needs metadataPrefix included with resumptionToken.
        """
        params = self.params
        if self.resumption_token:
            params = {
                'resumptionToken': self.resumption_token.token,
                'verb': self.verb,
                'metadataPrefix': params.get('metadataPrefix')  # Include metadataPrefix for OSTI
            }
        self.oai_response = self.sickle.harvest(**params)
        error = self.oai_response.xml.find(
            './/' + self.sickle.oai_namespace + 'error')
        if error is not None:
            code = error.attrib.get('code', 'UNKNOWN')
            description = error.text or ''
            try:
                raise getattr(
                    oaiexceptions, code[0].upper() + code[1:])(description)
            except AttributeError:
                raise oaiexceptions.OAIError(description)
        self.resumption_token = self._get_resumption_token()
        self._items = self.oai_response.xml.iterfind(
            './/' + self.sickle.oai_namespace + self.element)


class MySickle(Sickle):
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

                # validate response content
                if not http_response.text.strip():
                    raise Exception("Empty response received from server")
                if not http_response.text.strip().startswith('<?xml'):
                    raise Exception(f"Invalid XML response: {http_response.text[:100]}")

                if self.encoding:
                    http_response.encoding = self.encoding

                # successful response
                return OAIResponse(http_response, params=kwargs)

            except Exception as e:
                LOGGER.error(f"Error harvesting from {self.endpoint}: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                self.logger.info(f"Retrying after {retry_wait} seconds due to error...")
                sleep(retry_wait)

        raise Exception(f"Failed to harvest after {self.max_retries} retries")


def _get_my_sickle(repo_pmh_url, metrics_logger=None, timeout=120):
    if not repo_pmh_url:
        return None

    proxy_url = None
    if any(fragment in repo_pmh_url for fragment in
           ["citeseerx", "pure.coventry.ac.uk"]):
        proxy_url = os.getenv("STATIC_IP_PROXY")

    proxies = {"https": proxy_url, "http": proxy_url} if proxy_url else {}
    iterator = OSTIItemIterator if 'osti.gov/oai' in repo_pmh_url else MyOAIItemIterator
    sickle = MySickle(repo_pmh_url, proxies=proxies, timeout=timeout, iterator=iterator)

    if metrics_logger:
        sickle.set_metrics_logger(metrics_logger)

    return sickle


def parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def parse_datestamp(datestamp_str):
    try:
        if 'T' in datestamp_str:
            return datetime.strptime(datestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        else:
            return datetime.strptime(datestamp_str, '%Y-%m-%d')
    except ValueError:
        LOGGER.warning(f"Could not parse datestamp: {datestamp_str}")
        return datetime(2000, 1, 1)


def harvest_endpoint(endpoint, s3_bucket, start_date, end_date, parallel_dates=1):
    logger = get_thread_logger()
    logger.info(f"Starting harvest for endpoint: {endpoint.pmh_url}")
    try:
        s = Session()
        harvester = EndpointHarvester(endpoint, s)

        if parallel_dates == 1:
            harvester.harvest(s3_bucket=s3_bucket, first=start_date, last=end_date)
        else:
            # Create single-day ranges instead of multi-day chunks
            date_ranges = []
            current = start_date
            while current <= end_date:
                # Each range is exactly one day (same start and end date)
                date_ranges.append((current, current))
                current = current + timedelta(days=1)

            # Process up to parallel_dates number of single days concurrently
            with ThreadPoolExecutor(max_workers=parallel_dates) as date_executor:
                futures = []
                for first, last in date_ranges:
                    logger.info(f"Submitting single day: {first}")
                    futures.append(date_executor.submit(harvester.harvest, s3_bucket, first, last))

                for future in as_completed(futures):
                    future.result()

            # After all subtasks finish, update the final state
            endpoint.last_harvest_finished = datetime.now(timezone.utc)
            endpoint.most_recent_date_harvested = end_date
            endpoint.error = None
            endpoint.retry_at = None
            endpoint.retry_interval = timedelta(minutes=5)
            StateManager.update_endpoint_state(endpoint, s)

        logger.info(f"Completed harvest for endpoint: {endpoint.pmh_url}")
    except Exception as e:
        logger.error(f"Error harvesting endpoint {endpoint.pmh_url}: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description='OAI-PMH Repository Harvester')

    parser.add_argument('--endpoint-id', help='Specific endpoint ID to harvest')
    parser.add_argument('--start-date', type=parse_date,
                        help='Start date in YYYY-MM-DD format.')
    parser.add_argument('--end-date', type=parse_date,
                        help='End date in YYYY-MM-DD format.')
    parser.add_argument('--core-endpoints', action='store_true',
                        help='Harvest core endpoints only')
    parser.add_argument('--reliable-endpoints', action='store_true',
                        help='Harvest reliable non-core endpoints only')
    parser.add_argument('--other-endpoints', action='store_true',
                        help='Harvest other less reliable endpoints only')
    parser.add_argument('--n_threads', type=int, default=1,
                        help='Number of concurrent harvesting threads')
    parser.add_argument('--parallel-dates', type=int, default=1,
                        help='Number of days to harvest in parallel for each endpoint')

    args = parser.parse_args()

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("harvester.main")

    if args.endpoint_id:
        endpoint = StateManager.get_endpoint(args.endpoint_id, db)
        if not endpoint:
            logger.error(f"No endpoint found with ID: {args.endpoint_id}")
            return
        endpoints = [endpoint]
    elif args.core_endpoints:
        endpoints = StateManager.get_core_endpoints(db)
        logger.info(f"Found {len(endpoints)} core endpoints ready to harvest")
    elif args.reliable_endpoints:
        endpoints = StateManager.get_reliable_non_core_endpoints(db)
        logger.info(f"Found {len(endpoints)} reliable non-core endpoints ready to harvest")
    elif args.other_endpoints:
        endpoints = StateManager.get_other_endpoints(db)
        logger.info(f"Found {len(endpoints)} other endpoints ready to harvest")
    else:
        # By default, harvest all endpoint types
        core_endpoints = StateManager.get_core_endpoints(db)
        reliable_endpoints = StateManager.get_reliable_non_core_endpoints(db)
        other_endpoints = StateManager.get_other_endpoints(db)

        endpoints = core_endpoints + reliable_endpoints + other_endpoints

        logger.info(f"Found {len(core_endpoints)} core endpoints, " +
                    f"{len(reliable_endpoints)} reliable non-core endpoints, and " +
                    f"{len(other_endpoints)} other endpoints ready to harvest")

    with ThreadPoolExecutor(max_workers=args.n_threads) as executor:
        futures = []

        for endpoint in endpoints:
            if args.start_date:
                first_date = args.start_date
            else:
                harvester = EndpointHarvester(endpoint, db)
                first_date = (
                    endpoint.most_recent_date_harvested.date() - timedelta(
                        days=1)
                    if endpoint.most_recent_date_harvested
                    else harvester.get_earliest_datestamp().date()
                         or datetime(2000, 1, 1).date()
                )

            last_date = args.end_date or (
                        datetime.now(timezone.utc).date() - timedelta(days=1))

            future = executor.submit(
                harvest_endpoint,
                endpoint,
                S3_BUCKET,
                first_date,
                last_date,
                args.parallel_dates
            )
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Harvesting task failed: {str(e)}")


if __name__ == "__main__":
    main()