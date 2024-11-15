import gzip
import os
import tempfile
import threading
from queue import Queue, Empty
import argparse
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import boto3
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, \
    wait_exponential
from common import S3_BUCKET, LOGGER


class BatchTracker:
    def __init__(self):
        self.path_counters = {}
        self.lock = threading.Lock()

    def get_next_batch_number(self, date_path):
        with self.lock:
            if date_path is None:
                # For datafile iterator, just use a simple counter
                current = self.path_counters.get('datafile', -1)
                next_number = current + 1
                self.path_counters['datafile'] = next_number
                return next_number

            current = self.path_counters.get(date_path, -1)
            next_number = current + 1
            self.path_counters[date_path] = next_number
            return next_number


class APIWorksIterator:
    BATCH_SIZE = 1000

    def __init__(self, from_date, end_date, fetch_threads=10):
        self.from_date = from_date
        self.end_date = end_date
        self.fetch_threads = fetch_threads
        self.base_url = "https://api.datacite.org/dois"
        self.page_size = 1000

    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type(requests.exceptions.RequestException),
           before_sleep=lambda retry_state: LOGGER.info(
               f"Retrying API request after error: {retry_state.outcome.exception()}. Attempt {retry_state.attempt_number}")
           )
    def _fetch_page(self, args):
        page, total_pages = args
        try:
            params = {
                'page[size]': self.page_size,
                'page[number]': page,
                'query': f'updated:[{self.from_date}T00:00:00Z TO {self.end_date}T23:59:59Z]',
                'sort': 'updated'
            }

            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()

            LOGGER.info(f"Fetched page {page}/{total_pages}")
            return data.get('data', [])
        except Exception as e:
            LOGGER.error(f"Error fetching page {page}: {e}")
            return []

    def _get_total_records(self):
        params = {
            'page[size]': 1,
            'page[number]': 1,
            'query': f'updated:[{self.from_date}T00:00:00Z TO {self.end_date}T23:59:59Z]',
            'sort': 'updated'
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()['meta']['total']
        except Exception as e:
            LOGGER.error(f"Error getting total record count: {e}")
            raise

    def __iter__(self):
        try:
            total_records = self._get_total_records()
            total_pages = (total_records + self.page_size - 1) // self.page_size
            LOGGER.info(
                f"Found {total_records} total records across {total_pages} pages")

            with ThreadPoolExecutor(max_workers=self.fetch_threads) as executor:
                page_args = [(page, total_pages) for page in
                             range(1, total_pages + 1)]

                for works in executor.map(self._fetch_page, page_args):
                    yield from works

        except Exception as e:
            LOGGER.error(f"Error in API iterator: {e}")
            raise

    @staticmethod
    def get_work_details(work):
        updated = work['attributes']['updated']
        dt = datetime.fromisoformat(updated.replace('Z', '+00:00'))
        date_path = f"{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}"
        timestamp = int(dt.timestamp())
        doi = work['attributes']['doi']
        return date_path, timestamp, doi


class DatafileWorksIterator:
    BATCH_SIZE = 5000

    def __init__(self, datafile_path='datacite/datafile_2023'):
        self.datafile_path = datafile_path

    def __iter__(self):
        try:
            s3 = boto3.client('s3')
            LOGGER.info("Starting download of datafile to temp storage")

            temp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                s3.download_file(S3_BUCKET, self.datafile_path, temp_file.name)
                LOGGER.info(
                    "Datafile downloaded successfully, beginning processing")

                with gzip.open(temp_file.name, 'rt') as f:
                    for line in f:
                        if line.strip():
                            try:
                                yield json.loads(line)
                            except json.JSONDecodeError as e:
                                LOGGER.error(f"Error parsing JSON line: {e}")
            finally:
                temp_file.close()
                os.unlink(temp_file.name)
                LOGGER.info("Temporary file cleaned up")

        except Exception as e:
            LOGGER.error(f"Error handling datafile: {e}")
            raise

    @staticmethod
    def get_work_details(work):
        return None, None, work['doi']


def upload_batch(batch_number, works, date_path=None, timestamp=None):
    try:
        s3_client = boto3.client('s3')
        if date_path and timestamp:
            object_key = f"datacite/works/{date_path}/works_page_{batch_number}_{timestamp}.json"
        else:
            object_key = f"datacite/snapshot-2023-12-31/{batch_number}.json"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=object_key,
            Body=json.dumps(works),
            ContentType='application/json'
        )
    except Exception as e:
        LOGGER.error(f"Error uploading batch {batch_number}: {e}")


def upload_worker(q, stop_event):
    while not stop_event.is_set():
        try:
            item = q.get(timeout=1)
            if item is None:
                break

            batch_number, works, date_path, timestamp = item
            upload_batch(batch_number, works, date_path, timestamp)
            q.task_done()
        except Empty:
            continue
        except Exception as e:
            LOGGER.error(f"Error in upload worker: {e}")
            q.task_done()


def harvest_works(works_iterator, upload_threads):
    upload_queue = Queue(maxsize=upload_threads * 2)
    stop_event = threading.Event()
    batch_tracker = BatchTracker()

    upload_workers = []
    for _ in range(upload_threads):
        t = threading.Thread(target=upload_worker,
                             args=(upload_queue, stop_event))
        t.start()
        upload_workers.append(t)

    count = 0
    start_time = time.time()
    current_batch = []
    current_date_path = None

    try:
        for work in works_iterator:
            try:
                date_path, timestamp, doi = works_iterator.get_work_details(
                    work)

                if date_path and date_path != current_date_path:
                    current_date_path = date_path

                current_batch.append(work)
                count += 1

                if len(current_batch) >= works_iterator.BATCH_SIZE:
                    batch_number = batch_tracker.get_next_batch_number(
                        date_path)
                    upload_queue.put(
                        (batch_number, current_batch, date_path, timestamp))
                    current_batch = []

                if count % 1000 == 0:
                    elapsed_hours = (time.time() - start_time) / 3600
                    rate_per_hour = count / elapsed_hours
                    LOGGER.info(
                        f"Processed {count} DataCite works. Rate: {rate_per_hour:.0f}/hour")

            except Exception as e:
                LOGGER.error(f"Error processing work: {e}")

        if current_batch:
            batch_number = batch_tracker.get_next_batch_number(date_path)
            upload_queue.put(
                (batch_number, current_batch, date_path, timestamp))

    except Exception as e:
        LOGGER.error(f"Error processing works: {e}")
    finally:
        stop_event.set()
        for _ in upload_workers:
            upload_queue.put(None)

        for w in upload_workers:
            w.join()

        LOGGER.info(f"Completed. Total works processed: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--fetch-threads', type=int, default=10)
    parser.add_argument('--upload-threads', type=int, default=20)
    parser.add_argument('--from-date', default='2024-01-01')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--source', choices=['api', 'datafile'], default='api')
    parser.add_argument('--backfill-from-api', action='store_true', help="Backfill from January 1, 2024 (snapshot end date)")
    args = parser.parse_args()

    if args.backfill_from_api:
        # temp function to backfill from API
        start_date = datetime.strptime('2024-01-01', '%Y-%m-%d')
        end_date = datetime.strptime('2024-11-12', '%Y-%m-%d')

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            LOGGER.info(f"Processing records for date: {date_str}")

            works_iterator = APIWorksIterator(from_date=date_str, end_date=date_str, fetch_threads=args.fetch_threads)

            harvest_works(works_iterator, args.upload_threads)

            current_date += timedelta(days=1)

        LOGGER.info("Completed backfill for the specified date range.")
        exit()

    if args.source == 'api':
        if args.update:
            from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            LOGGER.info(
                f"Running in update mode. Fetching works updated since {from_date}")
        else:
            from_date = args.from_date
            end_date = datetime.now().strftime('%Y-%m-%d')

        works_iterator = APIWorksIterator(from_date, end_date, args.fetch_threads)
    else:
        works_iterator = DatafileWorksIterator()

    harvest_works(works_iterator, args.upload_threads)