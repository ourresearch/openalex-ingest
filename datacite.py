from urllib.parse import quote
import boto3
import requests
from queue import Queue
import threading
import argparse
import time
import json
from datetime import datetime, timedelta

from common import S3_BUCKET, LOGGER


def upload_worker(q):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        try:
            identifier, json_content = item
            object_key = f"datacite/works/{quote(identifier, safe='')}.json"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=object_key,
                Body=json.dumps(json_content),
                ContentType='application/json'
            )
        except Exception as e:
            LOGGER.error(f"Error uploading {identifier}: {e}")

        q.task_done()


def fetch_works(from_date):
    page = 1
    page_size = 1000
    base_url = "https://api.datacite.org/works"

    while True:
        params = {
            'page[size]': page_size,
            'page[number]': page,
            'query': f'updated:[{from_date} TO *]',
            'include': 'data-center,publisher,client,media,references,citations,predecessor-versions,successor-versions,contributors,affiliations'
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        for work in data['data']:
            yield work

        if page * page_size >= data['meta']['total']:
            break

        page += 1


def harvest_works(from_date, num_threads):
    upload_queue = Queue()

    workers = []
    for _ in range(num_threads):
        t = threading.Thread(target=upload_worker, args=(upload_queue,))
        t.start()
        workers.append(t)

    count = 0
    start_time = time.time()

    try:
        for work in fetch_works(from_date):
            doi = work['attributes']['doi']
            upload_queue.put((doi, work))

            count += 1
            if count % 100 == 0:
                elapsed_hours = (time.time() - start_time) / 3600
                rate_per_hour = count / elapsed_hours
                LOGGER.info(
                    f"Fetched {count} DataCite works. Rate: {rate_per_hour:.0f}/hour")

    except Exception as e:
        LOGGER.error(f"Error fetching works: {e}")
    finally:
        for _ in workers:
            upload_queue.put(None)

        for w in workers:
            w.join()

        LOGGER.info(f"Completed. Total works processed: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20,
                        help='Number of upload threads')
    parser.add_argument('--from-date', default='2023-12-01',
                        help='Fetch works updated since this date (YYYY-MM-DD)')
    parser.add_argument('--update', action='store_true',
                        help='Only fetch works updated/created since yesterday')
    args = parser.parse_args()

    if args.update:
        from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        LOGGER.info(
            f"Running in update mode. Fetching works updated since {from_date}")
    else:
        from_date = args.from_date

    harvest_works(from_date, args.threads)