from urllib.parse import quote
import boto3
from sickle import Sickle
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import argparse
import time
import gzip
from datetime import datetime, timedelta

from common import S3_BUCKET, LOGGER


def upload_worker(q, record_type):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        try:
            identifier, xml_content = item
            object_key = f"doaj/{record_type}/{quote(identifier, safe='')}.xml.gz"

            compressed_content = gzip.compress(xml_content.encode('utf-8'))

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=object_key,
                Body=compressed_content,
                ContentType='application/xml',
                ContentEncoding='gzip'
            )
        except Exception as e:
            LOGGER.error(f"Error uploading {identifier}: {e}")

        q.task_done()


def harvest_records(record_type, num_threads, update_mode=False):
    base_url = "https://www.doaj.org/oai.article" if record_type == "articles" else "https://www.doaj.org/oai"
    sickle = Sickle(base_url)

    kwargs = {'metadataPrefix': 'oai_dc'}
    if update_mode:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        kwargs['from'] = yesterday
        LOGGER.info(
            f"Running in update mode. Fetching {record_type} updated since {yesterday}")

    records = sickle.ListRecords(**kwargs)
    upload_queue = Queue()

    workers = []
    for _ in range(num_threads):
        t = threading.Thread(target=upload_worker,
                             args=(upload_queue, record_type))
        t.start()
        workers.append(t)

    count = 0
    start_time = time.time()

    for record in records:
        upload_queue.put((record.header.identifier, record.raw))
        count += 1
        if count % 100 == 0:
            elapsed_hours = (time.time() - start_time) / 3600
            rate_per_hour = count / elapsed_hours
            LOGGER.info(
                f"Fetched {count} DOAJ {record_type}. Rate: {rate_per_hour:.0f}/hour")

    for _ in workers:
        upload_queue.put(None)

    for w in workers:
        w.join()

    LOGGER.info(f"Completed. Total {record_type} processed: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20,
                        help='Number of upload threads')
    parser.add_argument('--update', action='store_true',
                        help='Only fetch records updated since yesterday')
    parser.add_argument('--type', choices=['articles', 'journals'],
                        required=True, help='Type of records to harvest')
    args = parser.parse_args()

    harvest_records(args.type, args.threads, args.update)