"""
Backfill one OAI-PMH endpoint one day at a time (oxjob #1588: all of HAL in xml-tei).

Why by day: HAL lists a date window in identifier order, not datestamp order, and the harvester
starts a new S3 file at every datestamp change, so a multi-day window writes a file per record or
two (HAL's oai_dc endpoint already writes ~35K files a day). A window of one day fills whole batches.

Each day is a normal single-endpoint harvest (from=until=day): same S3 layout, same health update.
The checkpoint only ever moves forward, so walking old days leaves the nightly's checkpoint alone.
Re-running a day is a no-op (content-hashed S3 keys). A day that fails is retried, then listed at
the end; restart from any day with --from.

    python backfill_by_day.py --endpoint-id hal_tei --from 2002-09-23 --to 2025-06-30

Heroku one-off dynos stop at 24 h: give each run a range that finishes inside that.
"""
import argparse
import logging
import re
from datetime import timedelta
from time import time

import requests

from common import S3_BUCKET, Session
from repositories import StateManager, harvest_single_endpoint, parse_date

RETRIES_PER_DAY = 3


def expected_records(pmh_url, metadata_prefix, start, end):
    """The feed's own count for the whole range (completeListSize), for a records-based ETA; None if not given."""
    try:
        r = requests.get(pmh_url, timeout=600, params={'verb': 'ListRecords', 'metadataPrefix': metadata_prefix,
                                                       'from': start.isoformat(), 'until': end.isoformat()})
        m = re.search(r'completeListSize="(\d+)"', r.text)
        return int(m.group(1)) if m else None
    except requests.RequestException:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint-id', required=True)
    parser.add_argument('--from', dest='start', type=parse_date, required=True, help='First day, YYYY-MM-DD')
    parser.add_argument('--to', dest='end', type=parse_date, required=True, help='Last day, YYYY-MM-DD (inclusive)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("backfill")

    with Session() as session:
        endpoint = StateManager.get_endpoint(args.endpoint_id, session)
        if not endpoint:
            raise SystemExit(f"No endpoint with id {args.endpoint_id}")
        pmh_url, metadata_prefix = endpoint.pmh_url, endpoint.metadata_prefix
    expected = expected_records(pmh_url, metadata_prefix, args.start, args.end)
    logger.info(f"BACKFILL {args.endpoint_id} {args.start}..{args.end}: feed says {expected} records")

    n_days = (args.end - args.start).days + 1
    started, total_records, failed = time(), 0, []
    day = args.start
    for i in range(n_days):
        for attempt in range(1, RETRIES_PER_DAY + 1):
            _, status, seconds, error = harvest_single_endpoint(args.endpoint_id, pmh_url, S3_BUCKET, day, day)
            if status in ('success', 'empty'):
                break
            logger.warning(f"{day} attempt {attempt} {status}: {(error or '')[:300]}")
        else:
            failed.append(day.isoformat())
        with Session() as session:
            records = StateManager.get_endpoint(args.endpoint_id, session).last_record_count or 0
        total_records += records
        elapsed = time() - started
        rate = total_records / elapsed if elapsed else 0
        eta = f"ETA {(expected - total_records) / rate / 3600:.1f} h" if expected and rate else "ETA ?"
        logger.info(f"BACKFILL {day} {status} records={records} in {seconds:.0f}s | day {i + 1}/{n_days}, "
                    f"{total_records}/{expected} records, {rate:.1f}/s, {eta} | failed days: {len(failed)}")
        day += timedelta(days=1)

    logger.info(f"BACKFILL DONE {args.start}..{args.end}: {total_records} records in {(time() - started) / 3600:.1f} h; "
                f"failed days ({len(failed)}): {' '.join(failed) or 'none'}")


if __name__ == "__main__":
    main()
