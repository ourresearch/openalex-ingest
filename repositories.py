import argparse
import datetime
import inspect
import json
import os
import gzip
import threading
from time import sleep, time
from dataclasses import dataclass, asdict
from typing import Optional, List
import logging
import boto3
import requests
import xml.etree.ElementTree as ET
from sickle import Sickle, oaiexceptions
from sickle.iterator import OAIItemIterator
from sickle.models import ResumptionToken
from sickle.oaiexceptions import NoRecordsMatch, BadArgument
from sickle.response import OAIResponse

from common import S3_BUCKET, LOGGER


@dataclass
class EndpointState:
    id: str
    pmh_url: str
    pmh_set: Optional[str]
    metadata_prefix: str = 'oai_dc'
    last_harvest_started: Optional[str] = None
    last_harvest_finished: Optional[str] = None
    most_recent_year_harvested: Optional[str] = None
    error: Optional[str] = None
    ready_to_run: bool = True
    retry_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {k: str(v) if isinstance(v, datetime.datetime) else v
                for k, v in asdict(self).items()}


class StateManager:
    def __init__(self, bucket: str,
                 state_prefix: str = "repositories/endpoints_state"):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.state_prefix = state_prefix

    def _get_state_key(self, endpoint_id: str) -> str:
        return f"{self.state_prefix}/{endpoint_id}.json"

    def get_endpoint(self, endpoint_id: str) -> Optional[EndpointState]:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=self._get_state_key(endpoint_id)
            )
            state_dict = json.loads(response['Body'].read().decode('utf-8'))
            return EndpointState.from_dict(state_dict)
        except self.s3_client.exceptions.NoSuchKey:
            return None

    def update_endpoint_state(self, state: EndpointState):
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self._get_state_key(state.id),
            Body=json.dumps(state.to_dict()),
            ContentType='application/json'
        )

    def get_ready_endpoints(self) -> List[EndpointState]:
        paginator = self.s3_client.get_paginator('list_objects_v2')
        ready_endpoints = []

        for page in paginator.paginate(Bucket=self.bucket,
                                       Prefix=self.state_prefix):
            for obj in page.get('Contents', []):
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.bucket,
                        Key=obj['Key']
                    )
                    state_dict = json.loads(
                        response['Body'].read().decode('utf-8'))
                    state = EndpointState.from_dict(state_dict)

                    if state.ready_to_run:
                        if not state.retry_at or datetime.datetime.fromisoformat(
                                state.retry_at) <= datetime.datetime.utcnow():
                            ready_endpoints.append(state)
                except Exception as e:
                    LOGGER.error(
                        f"Error processing state file {obj['Key']}: {e}")
                    continue

        return ready_endpoints


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
    def __init__(self, endpoint_state: EndpointState, batch_size=1000):
        self.state = endpoint_state
        self.batch_size = batch_size
        self.error = None
        self.harvest_identify_response = None
        self.metrics = MetricsLogger()

    def get_earliest_datestamp(self) -> datetime.datetime:
        if not self.state.pmh_url:
            LOGGER.warning("No PMH URL provided, returning default date")
            return datetime.datetime(2000, 1, 1)

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=10)
            identify = my_sickle.Identify()
            earliest = identify.earliestDatestamp

            if earliest:
                try:
                    if 'T' in earliest:
                        return datetime.datetime.strptime(earliest,
                                                          '%Y-%m-%dT%H:%M:%SZ')
                    else:
                        return datetime.datetime.strptime(earliest, '%Y-%m-%d')
                except ValueError:
                    LOGGER.warning(
                        f"Could not parse earliest datestamp: {earliest}")
                    return datetime.datetime(2000, 1, 1)
            else:
                LOGGER.warning(
                    "No earliest datestamp found in Identify response")
                return datetime.datetime(2000, 1, 1)

        except Exception as e:
            LOGGER.error(f"Error getting earliest datestamp: {str(e)}")
            return datetime.datetime(2000, 1, 1)

    def harvest(self, s3_bucket: str, state_manager: StateManager, first=None,
                last=None):
        if not self.harvest_identify_response:
            self.set_identify_and_initial_query()

        if not first:
            first = datetime.datetime(2000, 1, 1).date()
        if not last:
            last = datetime.datetime.utcnow().date()

        self.state.last_harvest_started = datetime.datetime.utcnow().isoformat()
        self.state.error = None
        state_manager.update_endpoint_state(self.state)

        try:
            self.metrics.start()
            self.call_pmh_endpoint(s3_bucket, first, last)

            self.state.last_harvest_finished = datetime.datetime.utcnow().isoformat()
            self.state.most_recent_year_harvested = last.isoformat()
            self.state.error = None
            self.state.retry_at = None

        except Exception as e:
            self.state.error = str(e)
            retry_interval = datetime.timedelta(minutes=5)
            if self.state.retry_at:
                last_retry = datetime.datetime.fromisoformat(
                    self.state.retry_at)
                now = datetime.datetime.utcnow()
                retry_interval = (last_retry - now) * 2

            self.state.retry_at = (
                        datetime.datetime.utcnow() + retry_interval).isoformat()
            self.state.last_harvest_started = None

        finally:
            self.metrics.stop()
            state_manager.update_endpoint_state(self.state)

    def set_identify_and_initial_query(self):
        if not self.state.pmh_url:
            self.harvest_identify_response = "error, no pmh_url given"
            return

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url, timeout=10)
            my_sickle.Identify()
            self.harvest_identify_response = "SUCCESS!"
        except Exception as e:
            self.error = f"error in calling identify: {e.__class__.__name__} {str(e)}"
            self.harvest_identify_response = self.error

    def get_datetime_path(self, record):
        datestamp = record.header.datestamp
        dt = datetime.datetime.fromisoformat(datestamp.replace('Z', '+00:00'))
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"

    def save_batch(self, s3_client, s3_bucket, batch_number, records,
                   first_record):
        try:
            date_path = self.get_datetime_path(first_record)
            root = ET.Element('oai_records')

            for record in records:
                record_elem = ET.fromstring(record.raw)
                root.append(record_elem)

            xml_content = ET.tostring(root, encoding='unicode', method='xml')
            compressed_content = gzip.compress(xml_content.encode('utf-8'))

            timestamp = int(datetime.datetime.fromisoformat(
                first_record.header.datestamp.replace('Z',
                                                      '+00:00')).timestamp())

            object_key = f"repositories/{self.state.id}/{date_path}/page_{batch_number}_{timestamp}.xml.gz"

            s3_client.put_object(
                Bucket=s3_bucket,
                Key=object_key,
                Body=compressed_content,
                ContentType='application/x-gzip'
            )

            LOGGER.info(f"Uploaded batch {batch_number} to {object_key}")

        except Exception as e:
            LOGGER.exception(f"Error saving batch {batch_number}")
            self.error = f"Error saving batch: {str(e)}"
            raise

    def call_pmh_endpoint(self, s3_bucket, first, last):
        s3_client = boto3.client('s3')

        args = {
            'metadataPrefix': self.state.metadata_prefix,
            'from': first.isoformat()[0:10],
            'until': last.isoformat()[0:10]
        }

        if self.state.pmh_set:
            args["set"] = self.state.pmh_set

        try:
            my_sickle = _get_my_sickle(self.state.pmh_url)
            records = my_sickle.ListRecords(**args)

            # Get total records from first page if available
            if hasattr(records._items, 'oai_response'):
                resumption_token_element = records._items.oai_response.xml.find(
                    './/' + records._items.sickle.oai_namespace + 'resumptionToken')
                if resumption_token_element is not None:
                    complete_list_size = resumption_token_element.attrib.get(
                        'completeListSize')
                    if complete_list_size:
                        self.metrics.total_records = int(complete_list_size)
                        LOGGER.info(
                            f"Total records to harvest: {self.metrics.total_records}")

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
                    self.save_batch(s3_client, s3_bucket, batch_number,
                                    current_batch, first_record_in_batch)
                    current_batch = []
                    first_record_in_batch = None
                    batch_number += 1

            if current_batch:
                self.save_batch(s3_client, s3_bucket, batch_number,
                                current_batch, first_record_in_batch)

        except NoRecordsMatch:
            LOGGER.info(
                f"No records found for {self.state.pmh_url} with args {args}")
        except Exception as e:
            self.error = f"Error harvesting records: {str(e)}"
            LOGGER.exception(f"Error harvesting from {self.state.pmh_url}")


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


class MySickle(Sickle):
    RETRY_SECONDS = 120

    def __init__(self, *args, **kwargs):
        self.metrics_logger = None
        kwargs['max_retries'] = 5
        super(MySickle, self).__init__(*args, **kwargs)

    def set_metrics_logger(self, metrics_logger):
        self.metrics_logger = metrics_logger

    def harvest(self, **kwargs):
        start_time = time()
        headers = {'User-Agent': 'OAIHarvester/1.0'}

        for _ in range(self.max_retries):
            try:
                if self.http_method == 'GET':
                    payload_str = "&".join(
                        f"{k}={v}" for k, v in kwargs.items())
                    url = f"{self.endpoint}?{payload_str}"
                    http_response = requests.get(url, headers=headers,
                                                  data=kwargs,
                                                  **self.request_args)
                    if self.metrics_logger:
                        self.metrics_logger.update_url(http_response.url)

                if http_response.status_code == 503:
                    retry_after = int(http_response.headers.get('Retry-After',
                                                                self.RETRY_SECONDS))
                    LOGGER.info(
                        f"HTTP 503! Retrying after {retry_after} seconds...")
                    sleep(retry_after)
                    continue

                http_response.raise_for_status()

                if not http_response.text.strip():
                    raise Exception("Empty response received from server")

                if not http_response.text.strip().startswith('<?xml'):
                    raise Exception(
                        f"Invalid XML response: {http_response.text[:100]}")

                if self.encoding:
                    http_response.encoding = self.encoding

                return OAIResponse(http_response, params=kwargs)

            except Exception as e:
                LOGGER.error(f"Error harvesting from {self.endpoint}: {str(e)}")
                if _ == self.max_retries - 1:  # Last retry
                    raise
                sleep(self.RETRY_SECONDS)

        raise Exception(f"Failed to harvest after {self.max_retries} retries")


def _get_my_sickle(repo_pmh_url, timeout=120):
    if not repo_pmh_url:
        return None

    proxy_url = None
    if any(fragment in repo_pmh_url for fragment in
           ["citeseerx", "pure.coventry.ac.uk"]):
        proxy_url = os.getenv("STATIC_IP_PROXY")

    proxies = {"https": proxy_url, "http": proxy_url} if proxy_url else {}
    sickle = MySickle(repo_pmh_url, proxies=proxies, timeout=timeout,
                    iterator=MyOAIItemIterator)

    frame = inspect.currentframe()
    try:
        while frame:
            if 'self' in frame.f_locals and isinstance(frame.f_locals['self'], EndpointHarvester):
                harvester = frame.f_locals['self']
                sickle.set_metrics_logger(harvester.metrics)
                break
            frame = frame.f_back
    finally:
        del frame  # Avoid reference cycles

    return sickle


def parse_date(date_str: str) -> datetime.datetime:
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(description='OAI-PMH Repository Harvester')

    parser.add_argument('--endpoint-id',
                        help='Specific endpoint ID to harvest')

    parser.add_argument('--start-date',
                        type=parse_date,
                        help='Start date in YYYY-MM-DD format. If not provided, uses state file date')

    parser.add_argument('--use-earliest',
                        action='store_true',
                        help='Use earliest date available from repository')

    parser.add_argument('--end-date',
                        type=parse_date,
                        help='End date in YYYY-MM-DD format. If not provided, uses current date')

    args = parser.parse_args()

    if args.start_date and args.use_earliest:
        parser.error("Cannot specify both --start-date and --use-earliest")

    state_manager = StateManager(bucket=S3_BUCKET)

    if args.endpoint_id:
        # Run single endpoint
        endpoint_state = state_manager.get_endpoint(args.endpoint_id)
        if not endpoint_state:
            print(f"No endpoint found with ID: {args.endpoint_id}")
            return
        endpoints = [endpoint_state]
    else:
        # Run all ready endpoints
        endpoints = state_manager.get_ready_endpoints()
        print(f"Found {len(endpoints)} endpoints ready to harvest")

    for endpoint_state in endpoints:
        print(f"\nProcessing endpoint: {endpoint_state.pmh_url}")
        harvester = EndpointHarvester(endpoint_state)

        first_date = None
        if args.use_earliest:
            first_date = harvester.get_earliest_datestamp()
            print(f"Using earliest repository date: {first_date}")
        elif args.start_date:
            first_date = args.start_date
            print(f"Using specified start date: {first_date}")

        last_date = args.end_date
        if last_date:
            print(f"Using specified end date: {last_date}")

        harvester.harvest(
            s3_bucket=S3_BUCKET,
            state_manager=state_manager,
            first=first_date,
            last=last_date
        )


if __name__ == "__main__":
    main()