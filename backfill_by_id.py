"""
Backfill one OAI-PMH endpoint record by record (GetRecord), for records a date walk can't reach cheaply
(oxjob #2111: PMC articles stamped on scattered days the harvester skipped).

The identifier list is a text file in S3, one OAI identifier (or a bare local id with --id-prefix) per line. Records
land exactly as a harvest writes them: same S3 folder, one file per datestamp per chunk, content-hashed keys, so the
Repo pipeline picks them up as usual and a re-run of a chunk is a no-op. Each finished chunk leaves a marker object
next to the list; a restart skips marked chunks. Ids the feed says do not exist, and ids that failed after retries,
are written next to the list at the end.

    python backfill_by_id.py --endpoint-id ac9de7698155b820de7 \
        --ids s3://openalex-ingest/backfill-ids/oxjob2111/ids.txt --id-prefix oai:pubmedcentral.nih.gov:

Heroku one-off dynos stop at 24 h: size the list to finish inside that, or restart (marked chunks are skipped).
"""
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone
from time import sleep, time
from urllib.parse import urlparse

from sickle.oaiexceptions import IdDoesNotExist

from common import S3_BUCKET, Session
from repositories import EndpointHarvester, StateManager, _get_my_sickle, _get_shared_s3_client

TRIES_PER_ID = 3


def s3_parts(url):
    p = urlparse(url)
    return p.netloc, p.path.lstrip('/')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint-id', required=True)
    parser.add_argument('--ids', required=True, help='s3://bucket/key of the identifier list')
    parser.add_argument('--id-prefix', default='', help='prepended to each line (e.g. oai:pubmedcentral.nih.gov:)')
    parser.add_argument('--chunk', type=int, default=1000, help='ids per chunk (one S3 file per datestamp per chunk)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("backfill_by_id")
    s3 = _get_shared_s3_client()
    bucket, key = s3_parts(args.ids)
    ids = sorted({line.strip() for line in s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode().splitlines()
                  if line.strip()})
    chunks = [ids[i:i + args.chunk] for i in range(0, len(ids), args.chunk)]
    marker = f"{key}.done/chunk_{{:05d}}"
    done = {o['Key'] for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=f"{key}.done/")
            for o in page.get('Contents', [])}
    logger.info(f"BACKFILL_BY_ID {args.endpoint_id}: {len(ids)} ids in {len(chunks)} chunks, "
                f"{sum(marker.format(i) in done for i in range(len(chunks)))} already done")

    with Session() as session:
        endpoint = StateManager.get_endpoint(args.endpoint_id, session)
        if not endpoint:
            raise SystemExit(f"No endpoint with id {args.endpoint_id}")
        harvester = EndpointHarvester(endpoint, session)   # only its save_batch is used: no checkpoint, no health
        prefix = endpoint.metadata_prefix
        sickle = _get_my_sickle(endpoint.pmh_url)

        started, n_ok, n_gone, failed = time(), 0, [], []
        for i, chunk in enumerate(chunks):
            if marker.format(i) in done:
                continue
            by_date = defaultdict(list)
            for local_id in chunk:
                identifier = args.id_prefix + local_id
                for attempt in range(1, TRIES_PER_ID + 1):
                    try:
                        record = sickle.GetRecord(identifier=identifier, metadataPrefix=prefix)
                        ds = record.header.datestamp
                        by_date[(ds.split('T')[0] if ds else datetime.now(timezone.utc).date().isoformat())].append(record)
                        n_ok += 1
                        break
                    except IdDoesNotExist:
                        n_gone.append(local_id)
                        break
                    except Exception as e:  # noqa: BLE001 - logged, retried, then listed
                        if attempt == TRIES_PER_ID:
                            failed.append(local_id)
                            logger.warning(f"{identifier} failed after {attempt} tries: {str(e)[:200]}")
                        else:
                            sleep(5 * attempt)
            for date_key, records in sorted(by_date.items()):
                harvester.save_batch(s3, S3_BUCKET, i, records, date_key)
            s3.put_object(Bucket=bucket, Key=marker.format(i), Body=b'')
            elapsed = time() - started
            seen = n_ok + len(n_gone) + len(failed)
            rate = seen / elapsed if elapsed else 0
            todo = sum(len(c) for j, c in enumerate(chunks) if j > i and marker.format(j) not in done)
            logger.info(f"BACKFILL_BY_ID chunk {i + 1}/{len(chunks)}: saved {n_ok}, not in feed {len(n_gone)}, "
                        f"failed {len(failed)} | {rate:.1f} ids/s, ETA {todo / rate / 3600 if rate else 0:.1f} h")

    for name, items in (('not_in_feed', n_gone), ('failed', failed)):
        s3.put_object(Bucket=bucket, Key=f"{key}.{name}.txt", Body='\n'.join(items).encode())
    logger.info(f"BACKFILL_BY_ID DONE: saved {n_ok}, not in feed {len(n_gone)}, failed {len(failed)} "
                f"(lists at s3://{bucket}/{key}.not_in_feed.txt / .failed.txt) in {(time() - started) / 3600:.1f} h")


if __name__ == "__main__":
    main()
