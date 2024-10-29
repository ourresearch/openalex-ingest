import boto3
import requests
from sickle import Sickle
from queue import Queue
import threading
import argparse
import time
import gzip
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

from tenacity import retry, retry_if_exception_type, wait_exponential, \
    stop_after_attempt

from common import S3_BUCKET, LOGGER

BATCH_SIZE = 1000


def get_datetime_path(record):
    datestamp = record.header.datestamp
    dt = datetime.fromisoformat(datestamp.replace('Z', '+00:00'))
    return f"{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}"


def upload_batch(record_type, batch_number, records, first_record, s3_client):
    try:
        date_path = get_datetime_path(first_record)
        root = ET.Element('oai_records')
        for record in records:
            record_elem = ET.fromstring(record)
            root.append(record_elem)

        xml_content = ET.tostring(root, encoding='unicode', method='xml')
        compressed_content = gzip.compress(xml_content.encode('utf-8'))
        timestamp = int(datetime.fromisoformat(first_record.header.datestamp.replace('Z', '+00:00')).timestamp())

        object_key = f"doaj/{record_type}/{date_path}/{record_type}_page_{batch_number}_{timestamp}.xml.gz"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=object_key,
            Body=compressed_content,
            ContentType='application/xml',
            ContentEncoding='gzip'
        )

    except Exception as e:
        LOGGER.error(
            f"Error uploading batch {batch_number} for {record_type}: {e}")


def upload_worker(q):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        try:
            record_type, batch_number, records, first_record = item
            upload_batch(record_type, batch_number, records, first_record, s3)
        except Exception as e:
            LOGGER.error(f"Error in upload worker: {e}")

        q.task_done()


def should_retry_exception(exception):
    retry_exceptions = (
        requests.exceptions.RequestException,
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout
    )
    return isinstance(exception, retry_exceptions)


@retry(
    retry=retry_if_exception_type(should_retry_exception),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(3),
    before_sleep=lambda retry_state: LOGGER.info(
        f"Retrying after error: {retry_state.outcome.exception()}. Attempt {retry_state.attempt_number}")
)
def fetch_and_process_records(sickle, kwargs, record_type, upload_queue):
    records = sickle.ListRecords(**kwargs)
    count = 0
    start_time = time.time()
    current_batch = []
    current_batch_first_record = None
    current_date_path = None
    batch_number = 0

    for record in records:
        new_date_path = get_datetime_path(record)
        if new_date_path != current_date_path:
            current_date_path = new_date_path
            batch_number = 0

        if not current_batch_first_record:
            current_batch_first_record = record
        current_batch.append(record.raw)
        count += 1

        if len(current_batch) >= BATCH_SIZE:
            upload_queue.put((record_type, batch_number, current_batch,
                              current_batch_first_record))
            batch_number += 1
            current_batch = []
            current_batch_first_record = None

        if count % 100 == 0:
            elapsed_hours = (time.time() - start_time) / 3600
            rate_per_hour = count / elapsed_hours
            LOGGER.info(
                f"Fetched {count} DOAJ {record_type}. Rate: {rate_per_hour:.0f}/hour")

    if current_batch:
        upload_queue.put((record_type, batch_number, current_batch,
                          current_batch_first_record))

    return count


def harvest_records(record_type, num_threads, update_mode=False):
    base_url = "https://www.doaj.org/oai.article" if record_type == "articles" else "https://www.doaj.org/oai"
    sickle = Sickle(base_url)

    kwargs = {'metadataPrefix': 'oai_dc'}
    if update_mode:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        kwargs['from'] = yesterday
        LOGGER.info(
            f"Running in update mode. Fetching {record_type} updated since {yesterday}")

    upload_queue = Queue()
    workers = []
    for _ in range(num_threads):
        t = threading.Thread(target=upload_worker, args=(upload_queue,))
        t.start()
        workers.append(t)

    try:
        count = fetch_and_process_records(sickle, kwargs, record_type, upload_queue)
    finally:
        for _ in workers:
            upload_queue.put(None)

        for w in workers:
            w.join()

        LOGGER.info(f"Completed. Total {record_type} processed: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20)
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--type', choices=['articles', 'journals'],
                        required=True)
    args = parser.parse_args()

    harvest_records(args.type, args.threads, args.update)