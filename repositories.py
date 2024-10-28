import gzip
from urllib.parse import quote
import boto3
from sickle import Sickle
from queue import Queue
import threading
import argparse
import time

from common import S3_BUCKET, LOGGER


def upload_worker(q):
    s3 = boto3.client('s3')
    while True:
        item = q.get()
        if item is None:
            break

        try:
            repo_id, identifier, xml_content = item
            object_key = f"repositories/{repo_id}/{quote(identifier, safe='')}.xml.gz"

            compressed_content = gzip.compress(xml_content.encode('utf-8'))

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=object_key,
                Body=compressed_content,
                ContentType='application/xml',
                ContentEncoding='gzip'
            )
        except Exception as e:
            LOGGER.error(
                f"Error uploading {identifier} from repository {repo_id}: {e}")

        q.task_done()


def harvest_repository(repo, upload_queue):
    try:
        sickle = Sickle(repo['url'])

        kwargs = {
            'metadataPrefix': repo['metadata_format']
        }
        if repo.get('set'):
            kwargs['set'] = repo['set']

        if repo.get('from_date'):
            kwargs['from'] = repo['from_date']
            LOGGER.info(
                f"Repository {repo['id']}: harvesting records from {repo['from_date']}")

        records = sickle.ListRecords(**kwargs)

        count = 0
        for record in records:
            try:
                identifier = record.header.identifier
                upload_queue.put((repo['id'], identifier, record.raw))
                count += 1
            except Exception as e:
                LOGGER.error(
                    f"Error processing record from repository {repo['id']}: {e}")

        return count
    except Exception as e:
        LOGGER.error(f"Error harvesting repository {repo['id']}: {e}")
        return 0


def harvest_repositories(repositories, num_threads):
    upload_queue = Queue()

    workers = []
    for _ in range(num_threads):
        t = threading.Thread(target=upload_worker, args=(upload_queue,))
        t.start()
        workers.append(t)

    total_count = 0
    start_time = time.time()

    try:
        for repo in repositories:
            LOGGER.info(f"Starting harvest of repository: {repo['id']}")
            count = harvest_repository(repo, upload_queue)
            total_count += count

            elapsed_hours = (time.time() - start_time) / 3600
            rate_per_hour = total_count / elapsed_hours if elapsed_hours > 0 else 0
            LOGGER.info(
                f"Completed repository {repo['id']}. Total records: {total_count}. Rate: {rate_per_hour:.0f}/hour")

    except Exception as e:
        LOGGER.error(f"Error in main harvest loop: {e}")
    finally:
        for _ in workers:
            upload_queue.put(None)

        for w in workers:
            w.join()

        LOGGER.info(f"Completed. Total records processed: {total_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', type=int, default=20,
                        help='Number of upload threads')
    parser.add_argument('--repositories', required=True,
                        help='Path to JSON file containing repository configurations')
    args = parser.parse_args()

    import json

    with open(args.repositories) as f:
        repositories = json.load(f)

    harvest_repositories(repositories, args.threads)