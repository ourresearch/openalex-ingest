import argparse
import json
import os
import threading
import time
from queue import Queue, Empty

import boto3
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, \
    retry_if_exception

from common import S3_BUCKET, LOGGER

BATCH_SIZE = 1000

token_lock = threading.Lock()
current_token = None


def get_auth_token():
    username = os.environ.get('ISSN_PORTAL_USERNAME')
    password = os.environ.get('ISSN_PORTAL_PASS')

    if not username or not password:
        raise ValueError("ISSN Portal credentials not found in environment variables")

    auth_url = f"https://api.issn.org/authenticate/{username}/{password}"
    response = requests.get(auth_url, headers={"Accept": "application/json"})
    response.raise_for_status()

    return response.json()['token']


def get_current_token():
    global current_token
    with token_lock:
        if current_token is None:
            current_token = get_auth_token()
        return current_token


def refresh_token():
    global current_token
    with token_lock:
        current_token = get_auth_token()
        LOGGER.info("Successfully refreshed authentication token")
        return current_token


def should_retry_exception(exception):
    if isinstance(exception, requests.exceptions.HTTPError):
        if exception.response.status_code == 403:
            try:
                refresh_token()
                return True
            except Exception as e:
                LOGGER.error(f"Failed to refresh token: {e}")
                return False
    return False


@retry(
    retry=retry_if_exception(should_retry_exception),
    wait=wait_exponential(multiplier=1, min=4, max=300),
    stop=stop_after_attempt(5),
    before_sleep=lambda retry_state: LOGGER.info(
        f"Got 403, retrying with fresh token. Attempt {retry_state.attempt_number}")
)
def fetch_issn_record(issn):
    url = f"https://api.issn.org/notice/{issn}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"JWT {get_current_token()}"
    }
    params = {
        "json": "true"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


@retry(
    retry=retry_if_exception(should_retry_exception),
    wait=wait_exponential(multiplier=1, min=4, max=300),
    stop=stop_after_attempt(5),
    before_sleep=lambda retry_state: LOGGER.info(
        f"Got 403, retrying with fresh token. Attempt {retry_state.attempt_number}")
)
def search_by_display_name(display_name):
    search_params = {
        "search": [f"keytitle={display_name}"],
        "page": 0,
        "size": 1
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"JWT {get_current_token()}"
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


def get_resume_index():
    s3 = boto3.client('s3')
    bucket_name = 'openalex-ingest'
    folder_path = 'issn-portal/journals/'

    try:
        files_count = 0
        paginator = s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_path):
            if 'Contents' in page:
                for file in page['Contents']:
                    if file['Key'] != folder_path:
                        files_count += 1

        resume_index = files_count * BATCH_SIZE
        starting_batch = files_count
        LOGGER.info(
            f"Found {files_count} files in S3, resuming from index {resume_index} with batch {starting_batch}")
        return resume_index, starting_batch

    except Exception as e:
        LOGGER.error(f"Error getting number of files from S3: {e}")
        return 0, 0


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


def worker(queue, results_queue, tracker):
    while True:
        item = queue.get()
        if item is None:
            break

        try:
            source = item
            record = None

            for issn in source['issns']:
                try:
                    record = fetch_issn_record(issn)
                    break
                except Exception as e:
                    LOGGER.error(f'Error fetching ISSN {issn}: {e}')
                    continue

            if not record:
                record = search_by_display_name(source['display_name'])

            if record:
                results_queue.put(record)
                tracker.increment(success=True)
            else:
                tracker.increment(success=False)

        except Exception as e:
            LOGGER.error(f"Error processing source {source['display_name']}: {e}")
            tracker.increment(success=False)

        queue.task_done()


def upload_worker(results_queue, starting_batch=0):
    s3 = boto3.client('s3')
    current_batch = []
    batch_number = starting_batch

    while True:
        try:
            record = results_queue.get(timeout=300)
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
        refresh_token()
        LOGGER.info("Successfully authenticated with ISSN Portal")
    except Exception as e:
        LOGGER.error(f"Authentication failed: {e}")
        return

    sources = load_sources()
    resume_index, starting_batch = get_resume_index()

    if resume_index > 0:
        sources = sources[resume_index:]
        LOGGER.info(
            f"Resuming from index {resume_index}, {len(sources)} sources remaining, starting at batch {starting_batch}")

    total_sources = len(sources)
    LOGGER.info(f"Loaded {total_sources} sources to process")

    if not total_sources:
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
                           args=(source_queue, results_queue, tracker))
        t.start()
        workers.append(t)

    upload_thread = threading.Thread(target=upload_worker,
                                   args=(results_queue, starting_batch))
    upload_thread.start()

    for source in sources:
        source_queue.put(source)

    for _ in workers:
        source_queue.put(None)

    for w in workers:
        w.join()

    results_queue.join()
    upload_thread.join()

    stop_event.set()
    progress_thread.join()

    completed, successes, failures, total = tracker.get_stats()
    LOGGER.info(
        f"Completed processing {completed}/{total} sources. "
        f"Successes: {successes}, Failures: {failures}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=10)
    args = parser.parse_args()

    main(args.threads)