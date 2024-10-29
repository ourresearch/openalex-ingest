import gzip
import tempfile
from ftplib import FTP
import boto3
from queue import Queue
import threading
import argparse
import time
import hashlib
import os
from typing import Dict

from common import S3_BUCKET, LOGGER


class ProgressTracker:
    def __init__(self, total_files):
        self.total_files = total_files
        self.completed_files = 0
        self.failed_files = 0
        self.retried_files = 0
        self.corrupted_files = 0
        self.revalidated_files = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.completed_files += 1

    def increment_failed(self):
        with self.lock:
            self.failed_files += 1

    def increment_retried(self):
        with self.lock:
            self.retried_files += 1

    def increment_corrupted(self):
        with self.lock:
            self.corrupted_files += 1

    def increment_revalidated(self):
        with self.lock:
            self.revalidated_files += 1

    def get_stats(self):
        with self.lock:
            return (self.completed_files, self.failed_files, self.retried_files,
                    self.corrupted_files, self.revalidated_files,
                    self.total_files)


def progress_logger(tracker, stop_event, validation_mode=False):
    start_time = time.time()
    while not stop_event.is_set():
        completed, failed, retried, corrupted, revalidated, total = tracker.get_stats()
        elapsed_hours = (time.time() - start_time) / 3600
        rate_per_hour = completed / elapsed_hours if elapsed_hours > 0 else 0
        percent_complete = (completed / total * 100) if total > 0 else 0

        if validation_mode:
            LOGGER.info(
                f"Validation Progress: {completed}/{total} files ({percent_complete:.1f}%) | "
                f"Corrupted: {corrupted} | Revalidated: {revalidated} | "
                f"Failed: {failed} | Rate: {rate_per_hour:.0f} files/hour"
            )
        else:
            LOGGER.info(
                f"Download Progress: {completed}/{total} files ({percent_complete:.1f}%) | "
                f"Failed: {failed} | Retried: {retried} | "
                f"Rate: {rate_per_hour:.0f} files/hour"
            )
        time.sleep(5)


def pubmed_ftp_client():
    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()
    return ftp


def calculate_md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_expected_md5(ftp_client, filename):
    md5_filename = filename + '.md5'
    temp_md5_file = tempfile.mkstemp()[1]

    try:
        with open(temp_md5_file, 'wb') as f:
            ftp_client.retrbinary(f'RETR {md5_filename}', f.write)

        with open(temp_md5_file, 'r') as f:
            md5_content = f.read().strip()
            return md5_content.split('=')[1].strip()
    except Exception as e:
        LOGGER.error(f"Error retrieving MD5 for {filename}: {e}")
        return None
    finally:
        os.remove(temp_md5_file)


def retrieve_file(ftp_client, filename, max_retries=3):
    expected_md5 = get_expected_md5(ftp_client, filename)
    if not expected_md5:
        LOGGER.error(f"Could not get MD5 hash for {filename}")
        return None, None

    for attempt in range(max_retries):
        local_filename = tempfile.mkstemp()[1]
        try:
            with open(local_filename, 'wb') as f:
                ftp_client.retrbinary(f'RETR {filename}', f.write)

            actual_md5 = calculate_md5(local_filename)

            if actual_md5 == expected_md5:
                LOGGER.info(f"MD5 verification successful for {filename}")
                return local_filename, expected_md5
            else:
                LOGGER.warning(
                    f"MD5 mismatch for {filename} (attempt {attempt + 1}/{max_retries})\n"
                    f"Expected: {expected_md5}\n"
                    f"Got: {actual_md5}"
                )
                os.remove(local_filename)

                if attempt < max_retries - 1:
                    LOGGER.info(f"Retrying download of {filename}")
                    continue

                LOGGER.error(
                    f"Failed to download {filename} after {max_retries} attempts")
                return None, None

        except Exception as e:
            LOGGER.error(
                f"Error downloading {filename} (attempt {attempt + 1}/{max_retries}): {e}")
            if os.path.exists(local_filename):
                os.remove(local_filename)

            if attempt < max_retries - 1:
                continue

            return None, None

    return None, None


def download_s3_file(s3, bucket, key):
    temp_file = tempfile.mkstemp()[1]
    try:
        s3.download_file(bucket, key, temp_file)
        return temp_file
    except Exception as e:
        LOGGER.error(f"Error downloading from S3: {key}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return None


def get_ftp_paths() -> Dict[str, str]:
    ftp_paths = {}
    ftp = pubmed_ftp_client()

    # Get baseline files
    ftp.cwd('/pubmed/baseline')
    baseline_files = ftp.nlst()
    for fname in baseline_files:
        if fname.endswith('.xml.gz'):
            ftp_paths[fname] = f'/pubmed/baseline/{fname}'

    # Get update files
    ftp.cwd('/pubmed/updatefiles')
    update_files = ftp.nlst()
    for fname in update_files:
        if fname.endswith('.xml.gz'):
            ftp_paths[fname] = f'/pubmed/updatefiles/{fname}'

    ftp.quit()
    return ftp_paths


def validate_worker(q, tracker, total_files, ftp_paths):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        i, s3_key = item
        try:
            LOGGER.info(f'Validating {s3_key} ({i + 1}/{total_files})')

            # Extract filename from S3 key and get corresponding FTP path
            filename = s3_key.split('/')[-1]
            if filename not in ftp_paths:
                LOGGER.error(f"Could not find FTP path for file: {filename}")
                tracker.increment_failed()
                q.task_done()
                continue

            ftp_path = ftp_paths[filename]

            # Get file from S3
            temp_s3_file = download_s3_file(s3, S3_BUCKET, s3_key)
            if not temp_s3_file:
                tracker.increment_failed()
                q.task_done()
                continue

            # Calculate actual MD5
            actual_md5 = calculate_md5(temp_s3_file)
            os.remove(temp_s3_file)

            # Get expected MD5
            ftp = pubmed_ftp_client()
            expected_md5 = get_expected_md5(ftp, ftp_path)

            if not expected_md5:
                tracker.increment_failed()
                ftp.quit()
                q.task_done()
                continue

            if actual_md5 != expected_md5:
                LOGGER.warning(f"Corrupted file detected: {s3_key}")
                LOGGER.warning(f"Expected MD5: {expected_md5}")
                LOGGER.warning(f"Actual MD5: {actual_md5}")
                tracker.increment_corrupted()

                # Re-download and upload
                temp_fname, verified_md5 = retrieve_file(ftp, ftp_path)
                if temp_fname and verified_md5 == expected_md5:
                    s3.upload_file(temp_fname, S3_BUCKET, s3_key)
                    os.remove(temp_fname)
                    tracker.increment_revalidated()
                    LOGGER.info(
                        f"Successfully revalidated and uploaded {s3_key}")
                else:
                    tracker.increment_failed()
                    LOGGER.error(f"Failed to revalidate {s3_key}")
            else:
                LOGGER.info(f"Validated {s3_key}")
                tracker.increment()

            ftp.quit()
        except Exception as e:
            tracker.increment_failed()
            LOGGER.error(f"Error validating {s3_key}: {e}")

        q.task_done()


def download_worker(q, tracker, total_files):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        i, filename = item
        try:
            LOGGER.info(f'Starting {filename} ({i + 1}/{total_files})')
            ftp = pubmed_ftp_client()
            temp_fname, _ = retrieve_file(ftp, filename)

            if temp_fname:
                s3.upload_file(temp_fname, S3_BUCKET,
                               'pubmed/' + filename.split('/')[-1])
                os.remove(temp_fname)
                tracker.increment()
                LOGGER.info(f'Finished {filename} ({i + 1}/{total_files})')
            else:
                tracker.increment_failed()
                LOGGER.error(
                    f'Failed to process {filename} ({i + 1}/{total_files})')

            ftp.quit()
        except Exception as e:
            tracker.increment_failed()
            LOGGER.error(f"Error processing {filename}: {e}")

        q.task_done()


def get_s3_files(s3):
    s3_files = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='pubmed/'):
        if 'Contents' in page:
            s3_files.extend([obj['Key'] for obj in page['Contents'] if
                             obj['Key'].endswith('.xml.gz')])

    return s3_files


def validate_files(num_threads):
    # Get FTP paths mapping
    LOGGER.info("Building FTP paths mapping...")
    ftp_paths = get_ftp_paths()

    s3 = boto3.client('s3')
    s3_files = get_s3_files(s3)
    total_files = len(s3_files)

    if not total_files:
        LOGGER.info("No files found in S3 to validate")
        return

    LOGGER.info(f"Starting validation of {total_files} files in S3")
    tracker = ProgressTracker(total_files)
    stop_event = threading.Event()

    progress_thread = threading.Thread(
        target=progress_logger,
        args=(tracker, stop_event, True)
    )
    progress_thread.start()

    validation_queue = Queue()
    workers = []

    for _ in range(num_threads):
        t = threading.Thread(target=validate_worker,
                             args=(
                             validation_queue, tracker, total_files, ftp_paths))
        t.start()
        workers.append(t)

    for i, s3_key in enumerate(s3_files):
        validation_queue.put((i, s3_key))

    for _ in workers:
        validation_queue.put(None)

    for w in workers:
        w.join()

    stop_event.set()
    progress_thread.join()

    completed, failed, _, corrupted, revalidated, total = tracker.get_stats()
    LOGGER.info(
        f"Validation complete: {completed}/{total} files | "
        f"Corrupted: {corrupted} | Revalidated: {revalidated} | "
        f"Failed: {failed}"
    )


def main(num_threads, validate_mode):
    if validate_mode:
        validate_files(num_threads)
        return

    LOGGER.info("Building FTP paths mapping...")
    ftp_paths = get_ftp_paths()
    remote_filenames = sorted(ftp_paths.values())

    s3 = boto3.client('s3')
    existing_fnames = set(fname.split('/')[-1] for fname in get_s3_files(s3))

    files_to_process = [f for f in remote_filenames if
                        f.split('/')[-1] not in existing_fnames]
    total_files = len(files_to_process)

    if not files_to_process:
        LOGGER.info("No new files to process")
        return

    tracker = ProgressTracker(total_files)
    stop_event = threading.Event()

    progress_thread = threading.Thread(
        target=progress_logger,
        args=(tracker, stop_event)
    )
    progress_thread.start()

    download_queue = Queue()
    workers = []

    for _ in range(num_threads):
        t = threading.Thread(target=download_worker,
                             args=(download_queue, tracker, total_files))
        t.start()
        workers.append(t)

    for i, filename in enumerate(files_to_process):
        download_queue.put((i, filename))

    for _ in workers:
        download_queue.put(None)

    for w in workers:
        w.join()

    stop_event.set()
    progress_thread.join()

    completed, failed, retried, _, _, total = tracker.get_stats()
    LOGGER.info(
        f"Download complete: {completed}/{total} files | "
        f"Failed: {failed} | Retried: {retried}"
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20,
                        help='Number of worker threads')
    parser.add_argument('--validate', action='store_true',
                        help='Validate existing S3 files against MD5 hashes')
    args = parser.parse_args()

    main(args.threads, args.validate)