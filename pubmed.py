import gzip
import tempfile
from ftplib import FTP
import boto3
from queue import Queue
import threading
import argparse
import time

from common import S3_BUCKET, LOGGER


class ProgressTracker:
    def __init__(self, total_files):
        self.total_files = total_files
        self.completed_files = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.completed_files += 1

    def get_stats(self):
        with self.lock:
            return self.completed_files, self.total_files


def progress_logger(tracker, stop_event):
    start_time = time.time()
    while not stop_event.is_set():
        completed, total = tracker.get_stats()
        elapsed_hours = (time.time() - start_time) / 3600
        rate_per_hour = completed / elapsed_hours if elapsed_hours > 0 else 0
        percent_complete = (completed / total * 100) if total > 0 else 0

        LOGGER.info(
            f"Progress: {completed}/{total} files ({percent_complete:.1f}%) | Rate: {rate_per_hour:.0f} files/hour")
        time.sleep(5)


def pubmed_ftp_client():
    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()
    return ftp


def retrieve_file(ftp_client, filename):
    local_filename = tempfile.mkstemp()[1]
    with open(local_filename, 'wb') as f:
        ftp_client.retrbinary(f'RETR {filename}', f.write)
    return local_filename


def get_xml_content(xml_gz_filename):
    with gzip.open(xml_gz_filename, "rb") as xml_file:
        return xml_file.read()


def get_existing_fnames(s3):
    existing_fnames = set()
    obj_list = s3.list_objects_v2(Bucket=S3_BUCKET,
                                  Prefix='pubmed',
                                  MaxKeys=1000)
    for o in obj_list["Contents"]:
        existing_fnames.add(o['Key'])
    if 'NextToken' not in obj_list or not obj_list['NextToken']:
        return [fname.split('/')[-1] for fname in existing_fnames]
    while True:
        obj_list = s3.list_objects_v2(Bucket=S3_BUCKET,
                                      Prefix='pubmed',
                                      MaxKeys=1000,
                                      ContinuationToken=obj_list['NextToken'])
        for o in obj_list["Contents"]:
            existing_fnames.add(o['Key'])
        if 'NextToken' not in obj_list or not obj_list['NextToken']:
            break
    return [fname.split('/')[-1] for fname in existing_fnames]


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
            temp_fname = retrieve_file(ftp, filename)
            s3.upload_file(temp_fname, S3_BUCKET, 'pubmed/' + filename.split('/')[-1])
            ftp.quit()
            tracker.increment()
            LOGGER.info(f'Finished {filename} ({i + 1}/{total_files})')
        except Exception as e:
            LOGGER.error(f"Error processing {filename}: {e}")

        q.task_done()


def main(num_threads):
    s3 = boto3.client('s3')
    existing_fnames = get_existing_fnames(s3)

    ftp = pubmed_ftp_client()
    ftp.cwd('/pubmed/baseline')
    baseline_fnames = [f'/pubmed/baseline/{f}' for f in ftp.nlst() if
                       f.endswith('.xml.gz')]
    ftp.cwd('/pubmed/updatefiles')
    update_fnames = [f'/pubmed/updatefiles/{f}' for f in ftp.nlst() if
                     f.endswith('.xml.gz')]
    remote_filenames = sorted(baseline_fnames + update_fnames)
    ftp.quit()

    files_to_process = [f for f in remote_filenames if f.split('/')[-1] not in existing_fnames]
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

    completed, total = tracker.get_stats()
    LOGGER.info(f"Completed processing {completed}/{total} files")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20,
                        help='Number of download threads')
    args = parser.parse_args()

    main(args.threads)