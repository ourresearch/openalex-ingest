from urllib.parse import quote
import boto3
from sickle import Sickle
from queue import Queue
import threading
import argparse
import time
import gzip
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

from common import S3_BUCKET, LOGGER

BATCH_SIZE = 1000


def upload_batch(record_type, batch_number, records, s3_client):
    try:
        start_id = (batch_number * BATCH_SIZE) + 1
        end_id = start_id + len(records) - 1

        root = ET.Element('oai_records')
        for record in records:
            record_elem = ET.fromstring(record)
            root.append(record_elem)

        xml_content = ET.tostring(root, encoding='unicode', method='xml')

        compressed_content = gzip.compress(xml_content.encode('utf-8'))

        object_key = f"doaj/{record_type}/{start_id}-{end_id}.xml.gz"

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
            record_type, batch_number, records = item
            upload_batch(record_type, batch_number, records, s3)
        except Exception as e:
            LOGGER.error(f"Error in upload worker: {e}")

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
        t = threading.Thread(target=upload_worker, args=(upload_queue,))
        t.start()
        workers.append(t)

    count = 0
    start_time = time.time()
    current_batch = []
    batch_number = 0

    try:
        for record in records:
            current_batch.append(record.raw)
            count += 1

            if len(current_batch) >= BATCH_SIZE:
                upload_queue.put((record_type, batch_number, current_batch))
                batch_number += 1
                current_batch = []

            if count % 100 == 0:
                elapsed_hours = (time.time() - start_time) / 3600
                rate_per_hour = count / elapsed_hours
                LOGGER.info(
                    f"Fetched {count} DOAJ {record_type}. Rate: {rate_per_hour:.0f}/hour")

        if current_batch:
            upload_queue.put((record_type, batch_number, current_batch))

    except Exception as e:
        LOGGER.error(f"Error fetching records: {e}")
    finally:
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