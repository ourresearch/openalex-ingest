import json
import os
import threading
import time
import argparse
from queue import Queue, Empty
import requests
import boto3

from common import S3_BUCKET, LOGGER

BATCH_SIZE = 1000


class ProgressTracker:
    def __init__(self, total_sources):
        self.total_sources = total_sources
        self.completed_sources = 0
        self.successful_sources = 0
        self.failed_sources = 0
        self.lock = threading.Lock()

    def increment(self, success=True):
        with self.lock:
            self.completed_sources += 1
            if success:
                self.successful_sources += 1
            else:
                self.failed_sources += 1

    def get_stats(self):
        with self.lock:
            return (self.completed_sources, self.successful_sources,
                   self.failed_sources, self.total_sources)

def progress_logger(tracker, stop_event):
    start_time = time.time()
    while not stop_event.is_set():
        completed, successes, failures, total = tracker.get_stats()
        elapsed_hours = (time.time() - start_time) / 3600
        rate_per_hour = completed / elapsed_hours if elapsed_hours > 0 else 0
        percent_complete = (completed / total * 100) if total > 0 else 0
        success_rate = (successes / completed * 100) if completed > 0 else 0
        failure_rate = (failures / completed * 100) if completed > 0 else 0

        LOGGER.info(
            f"Progress: {completed}/{total} sources ({percent_complete:.1f}%) | "
            f"Success: {successes} ({success_rate:.1f}%) | "
            f"Failed: {failures} ({failure_rate:.1f}%) | "
            f"Rate: {rate_per_hour:.0f}/hour")
        time.sleep(5)


def get_auth_token():
    username = os.environ.get('ISSN_PORTAL_USERNAME')
    password = os.environ.get('ISSN_PORTAL_PASS')

    if not username or not password:
        raise ValueError(
            "ISSN Portal credentials not found in environment variables")

    auth_url = f"https://api.issn.org/authenticate/{username}/{password}"
    response = requests.get(auth_url, headers={"Accept": "application/json"})
    response.raise_for_status()

    return response.json()['token']


def fetch_issn_record(issn, auth_token):
    url = f"https://api.issn.org/notice/{issn}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"JWT {auth_token}"
    }
    params = {
        "json": "true"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def search_by_display_name(display_name, auth_token):
    search_params = {
        "search": [f"keytitle={display_name}"],
        "page": 0,
        "size": 1
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"JWT {auth_token}"
    }

    response = requests.post("https://api.issn.org/search?json=true",
                             headers=headers, json=search_params)
    response.raise_for_status()

    j = response.json()
    if len(j) > 0:
        return json.loads(j[0])
    return None


def load_sources():
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket='openalex-ingest',
                             Key='issn-portal/sources.json')
    return json.loads(response['Body'].read().decode('utf-8'))


def upload_batch(batch_records, batch_number, s3_client):
    try:
        object_key = f"issn-portal/journals/{batch_number}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=object_key,
            Body=json.dumps(batch_records),
            ContentType='application/json'
        )
    except Exception as e:
        LOGGER.error(f"Error uploading batch {batch_number}: {e}")


def worker(queue, results_queue, auth_token, tracker):
    while True:
        item = queue.get()
        if item is None:
            break

        try:
            source = item
            record = None

            for issn in source['issns']:
                try:
                    record = fetch_issn_record(issn, auth_token)
                    break
                except Exception as e:
                    LOGGER.error(f'Error fetching ISSN {issn}: {e}')
                    continue

            if not record:
                record = search_by_display_name(source['display_name'],
                                                auth_token)

            if record:
                results_queue.put(record)
                tracker.increment(success=True)
            else:
                tracker.increment(success=False)

        except Exception as e:
            LOGGER.error(
                f"Error processing source {source['display_name']}: {e}")
            tracker.increment(success=False)

        queue.task_done()


def upload_worker(results_queue):
    s3 = boto3.client('s3')
    current_batch = []
    batch_number = 0

    while True:
        try:
            record = results_queue.get(timeout=300)  # 5 minute timeout
            current_batch.append(record)

            if len(current_batch) >= BATCH_SIZE:
                upload_batch(current_batch, batch_number, s3)
                batch_number += 1
                current_batch = []

        except Empty:
            if current_batch:
                upload_batch(current_batch, batch_number, s3)
            break

        results_queue.task_done()


def main(num_threads):
    try:
        auth_token = get_auth_token()
        LOGGER.info("Successfully authenticated with ISSN Portal")
    except Exception as e:
        LOGGER.error(f"Authentication failed: {e}")
        return

    sources = load_sources()
    total_sources = len(sources)
    LOGGER.info(f"Loaded {total_sources} sources to process")

    if not sources:
        LOGGER.info("No sources to process")
        return

    tracker = ProgressTracker(total_sources)
    stop_event = threading.Event()

    progress_thread = threading.Thread(
        target=progress_logger,
        args=(tracker, stop_event)
    )
    progress_thread.start()

    source_queue = Queue()
    results_queue = Queue()

    workers = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker,
                             args=(source_queue, results_queue, auth_token, tracker))
        t.start()
        workers.append(t)

    upload_thread = threading.Thread(target=upload_worker,
                                     args=(results_queue, ))
    upload_thread.start()

    for source in sources:
        source_queue.put(source)

    for _ in workers:
        source_queue.put(None)

    for w in workers:
        w.join()

    upload_thread.join()

    stop_event.set()
    progress_thread.join()

    completed, total = tracker.get_stats()
    LOGGER.info(f"Completed processing {completed}/{total} sources")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=10)
    args = parser.parse_args()

    main(args.threads)