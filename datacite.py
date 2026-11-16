"""
DataCite works harvest. Scheduled 01:00 UTC as `python datacite.py --yesterday` (Heroku advanced scheduler).

--yesterday pulls every hour of DataCite `updated` time from the hour after the last one it finished
(state/datacite_updates.checkpoint.json) through the last full hour, one cursor per hour, and checks each hour against
DataCite's count for it, so a run that dies or comes back short is made up by the next one. Then it re-checks the days
1 week, 1 month and 3 months back (RECHECK_DAYS): DataCite's search can show a record weeks after its `updated` time,
so the first pull of that hour never saw it. A re-check lists the day's DOIs (ids only), compares them with the ids
already in that day's S3 folder and fetches only the missing ones (oxjob #2284).
--from-date D --to-date D pulls those days in full, hour by hour, and leaves the checkpoint alone (backfill).
--recheck-from D --recheck-to D re-checks those days (backfill of records the nightly never saw).

Files land in datacite/works/<YYYY/MM/DD of updated>/, which the walden DataCite pipeline reads.
"""
import argparse
import datetime
import gzip
import json
import logging
import os
import time
from io import BytesIO

import boto3
import requests
import sentry_sdk
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_exponential

from sentry_setup import init_sentry

init_sentry()   # oxjob #1685

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("datacite_harvester")

BASE_URL = "https://api.datacite.org/dois"
PARAMS = "affiliation=true&publisher=true"
BATCH_SIZE = 1000
FETCH_SIZE = 100
S3_BUCKET = "openalex-ingest"
CHECKPOINT_KEY = "state/datacite_updates.checkpoint.json"
LOCK_KEY = "state/datacite_updates.lock.json"
HOUR = datetime.timedelta(hours=1)
LOCK_STALE = datetime.timedelta(minutes=20)
ALERT_BEHIND = datetime.timedelta(hours=36)
HOUR_ATTEMPTS = 3
RECHECK_DAYS = (7, 30, 90)   # 16 Nov 2026: none missing 1-7 days back, 3 at 14 days, 7 at 28, 332 at 42

s3 = boto3.client("s3")
http = requests.Session()
http.headers.update({"User-Agent": "OpenAlex harvester (mailto:dev@ourresearch.org)"})


@retry(stop=stop_after_delay(240), wait=wait_exponential(multiplier=2, min=4, max=60),
       retry=retry_if_exception_type(requests.exceptions.RequestException), reraise=True)
def get_json(url, params=None):
    response = http.get(url, params=params, timeout=120)
    if response.status_code == 429:
        time.sleep(int(response.headers.get("Retry-After", 60)))
    response.raise_for_status()
    return response.json()


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def parse_hour(text):
    return datetime.datetime.strptime(text[:13], "%Y-%m-%dT%H").replace(tzinfo=datetime.timezone.utc)


def fmt_hour(hour):
    return hour.strftime("%Y-%m-%dT%H")


def window(start, end):
    return f"updated:[{start:%Y-%m-%dT%H:%M:%S}Z TO {end:%Y-%m-%dT%H:%M:%S}Z]"


def count(query):
    return get_json(BASE_URL, {"query": query, "page[size]": 1, "fields[dois]": "updated"})["meta"]["total"]


def write_batch(day, name, works):
    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="w") as gz:
        gz.write(json.dumps(works).encode("utf-8"))
    key = f"datacite/works/{day:%Y/%m/%d}/{name}_{int(time.time())}.json.gz"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue(), ContentType="application/gzip")
    LOGGER.info(f"Uploaded {len(works)} records to {key}")


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def write_json(key, value):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(value), ContentType="application/json")


def pull_hour_once(hour, on_page):
    query = window(hour, hour + HOUR - datetime.timedelta(seconds=1))
    url = f"{BASE_URL}?page[cursor]=1&page[size]={BATCH_SIZE}&query={query}&{PARAMS}"
    fetched, batch_number = 0, 0
    while url:
        data = get_json(url)
        works = data.get("data", [])
        if works:
            batch_number += 1
            write_batch(hour, f"batch_{hour:%H}_{batch_number}", works)
            fetched += len(works)
        on_page()
        url = data.get("links", {}).get("next")
    return fetched, count(query)


def pull_hour(hour, on_page):
    """Pull every record whose `updated` falls in this hour; pull it again if we got fewer than DataCite counts now."""
    for attempt in range(1, HOUR_ATTEMPTS + 1):
        try:
            fetched, total = pull_hour_once(hour, on_page)
            if fetched >= total:
                return fetched
            message = f"got {fetched} of {total}"
        except requests.exceptions.RequestException as e:
            message = str(e)
        if attempt == HOUR_ATTEMPTS:
            raise RuntimeError(f"DataCite hour {fmt_hour(hour)} failed {HOUR_ATTEMPTS} times: {message}")
        LOGGER.warning(f"Hour {fmt_hour(hour)}: {message}; pulling it again (attempt {attempt + 1}).")
        time.sleep(60)


def pull_hours(start, end, checkpoint):
    """Pull hours [start, end). With checkpoint=True, record each finished hour and hold a lock against a second run."""
    run_id = f"{os.getenv('DYNO', 'local')}-{utcnow().isoformat()}"
    last_beat = [utcnow()]

    def heartbeat():
        if checkpoint and utcnow() - last_beat[0] > datetime.timedelta(minutes=2):
            write_json(LOCK_KEY, {"run": run_id, "at": utcnow().isoformat()})
            last_beat[0] = utcnow()

    if checkpoint:
        lock = read_json(LOCK_KEY)
        if lock and utcnow() - datetime.datetime.fromisoformat(lock["at"]) < LOCK_STALE:
            LOGGER.info(f"Another DataCite run ({lock['run']}) is live; exiting.")
            return False
        write_json(LOCK_KEY, {"run": run_id, "at": utcnow().isoformat()})

    hour = start
    try:
        while hour < end:
            fetched = pull_hour(hour, heartbeat)
            LOGGER.info(f"Hour {fmt_hour(hour)} done: {fetched} records.")
            if checkpoint:
                write_json(CHECKPOINT_KEY, {"next_hour": fmt_hour(hour + HOUR), "at": utcnow().isoformat()})
            hour += HOUR
    finally:
        if checkpoint:
            s3.delete_object(Bucket=S3_BUCKET, Key=LOCK_KEY)
            behind = utcnow() - hour
            if behind > ALERT_BEHIND:
                sentry_sdk.capture_message(
                    f"DataCite harvest is {behind.total_seconds() / 3600:.0f} h behind (next hour {fmt_hour(hour)})",
                    level="error")
    return True


def held_ids(day):
    """Lower-cased DOIs in every file of the day's S3 folder."""
    ids = set()
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=f"datacite/works/{day:%Y/%m/%d}/")
    for page in pages:
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            ids.update(work["id"].lower() for work in json.loads(gzip.decompress(body)))
    return ids


def listed_ids(day):
    """Every DOI whose `updated` falls on the day, ids only; fails if the listing comes back short."""
    query = window(day, day + datetime.timedelta(days=1, seconds=-1))
    url = f"{BASE_URL}?page[cursor]=1&page[size]={BATCH_SIZE}&query={query}&fields[dois]=updated"
    ids = []
    while url:
        data = get_json(url)
        ids.extend(d["id"].lower() for d in data.get("data", []))
        url = data.get("links", {}).get("next")
    expected = count(query)
    if len(ids) < expected:
        raise RuntimeError(f"DataCite listing of {day:%Y-%m-%d} came back short: {len(ids)} of {expected}")
    return ids


def recheck_day(day):
    """Fetch the DOIs DataCite lists for the day that aren't in the day's S3 folder (100 per request)."""
    missing = sorted(set(listed_ids(day)) - held_ids(day))
    works, fetched, batch_number = [], 0, 0
    for i in range(0, len(missing), FETCH_SIZE):
        query = "doi:(" + " OR ".join(f'"{doi}"' for doi in missing[i:i + FETCH_SIZE]) + ")"
        works.extend(get_json(f"{BASE_URL}?{PARAMS}", {"query": query, "page[size]": FETCH_SIZE})["data"])
        if len(works) >= BATCH_SIZE or i + FETCH_SIZE >= len(missing):
            batch_number += 1
            write_batch(day, f"recheck_{batch_number}", works)
            fetched += len(works)
            works = []
    LOGGER.info(f"Re-check {day:%Y-%m-%d}: {len(missing)} DOIs not held, {fetched} fetched.")
    return fetched


def recheck_days(day_list):
    """Re-check each day; a day that fails goes to Sentry and the rest still run."""
    failed = []
    for day in day_list:
        try:
            recheck_day(day)
        except Exception as e:
            failed.append(f"{day:%Y-%m-%d}")
            LOGGER.error(f"Re-check {day:%Y-%m-%d} failed: {e}")
            sentry_sdk.capture_exception(e)
    if failed:
        raise RuntimeError(f"DataCite re-check failed for {', '.join(failed)}")


def parse_day(text):
    return datetime.datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)


def days(first, last):
    day = parse_day(first)
    while day <= parse_day(last):
        yield day
        day += datetime.timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Harvest DataCite works to S3.")
    parser.add_argument("--yesterday", action="store_true", help="Scheduled run: catch up from the checkpoint, then re-check.")
    parser.add_argument("--from-date", help="Backfill: first day to pull in full (YYYY-MM-DD).")
    parser.add_argument("--to-date", help="Backfill: last day to pull in full (YYYY-MM-DD).")
    parser.add_argument("--recheck-from", help="Re-check: first day (YYYY-MM-DD).")
    parser.add_argument("--recheck-to", help="Re-check: last day (YYYY-MM-DD).")
    args = parser.parse_args()

    if args.yesterday:
        end = utcnow().replace(minute=0, second=0, microsecond=0)
        state = read_json(CHECKPOINT_KEY)
        # No checkpoint yet: start at yesterday 00:00, where the old --yesterday run did.
        start = parse_hour(state["next_hour"]) if state else (end - datetime.timedelta(days=1)).replace(hour=0)
        if pull_hours(start, end, True):
            today = end.replace(hour=0)
            recheck_days(today - datetime.timedelta(days=back) for back in RECHECK_DAYS)
    elif args.from_date and args.to_date:
        pull_hours(parse_day(args.from_date), parse_day(args.to_date) + datetime.timedelta(days=1), False)
    elif args.recheck_from and args.recheck_to:
        recheck_days(days(args.recheck_from, args.recheck_to))
    else:
        parser.error("Use --yesterday, --from-date with --to-date, or --recheck-from with --recheck-to.")


if __name__ == "__main__":
    main()
