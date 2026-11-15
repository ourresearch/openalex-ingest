import argparse
import datetime
import json
import os
import time

import boto3
import requests
import sentry_sdk
from tenacity import retry, stop_after_delay, wait_exponential, retry_if_exception_type

from common import S3_BUCKET, LOGGER

CROSSREF_API_KEY = os.getenv('CROSSREF_API_KEY')

"""
Run with: heroku local:run python crossref.py new

`updates` pulls every hour Crossref re-indexed, from the hour after the last one it finished (state/crossref_updates.checkpoint.json)
through the hour that ended 2 hours ago, one cursor per hour, so a run that dies is made up by the next one (oxjob #2253).
`updates --from 2026-11-11T00 --until 2026-11-12T00` pulls a fixed range and leaves the checkpoint alone.

Download monthly snapshot to ec2: curl -H 'crossref-api-key: mykey' -H 'User-Agent: Downloader/1.1 (mailto:dev@ourresearch.org)' -v -L -o all.json.tar.gz -X GET https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz
"""

CHECKPOINT_KEY = 'state/crossref_updates.checkpoint.json'
LOCK_KEY = 'state/crossref_updates.lock.json'
HOUR = datetime.timedelta(hours=1)
SETTLE = datetime.timedelta(hours=2)
LOCK_STALE = datetime.timedelta(minutes=20)
ALERT_BEHIND = datetime.timedelta(hours=36)
HOUR_ATTEMPTS = 3

s3 = boto3.client('s3')
http = requests.Session()
http.headers.update({
    "Accept": "application/json",
    "User-Agent": "mailto:dev@ourresearch.org",
    "crossref-api-key": CROSSREF_API_KEY or '',
})


# Keeps retrying one page for up to 4 minutes: a Crossref cursor expires after 5 idle minutes.
@retry(stop=stop_after_delay(240), wait=wait_exponential(multiplier=2, min=4, max=60),
       retry=retry_if_exception_type(requests.exceptions.RequestException), reraise=True)
def make_request_with_retry(url):
    response = http.get(url, timeout=120)
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        LOGGER.warning(f"Rate limit exceeded (429). Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
    elif response.status_code >= 500:
        LOGGER.warning(f"Server error {response.status_code} for URL {url}. Retrying...")
    response.raise_for_status()
    return response


def get_crossref_data(filter_params, s3_prefix, file_prefix='works', on_page=None):
    cursor = '*'
    page_number = 1
    fetched = 0
    while True:
        url = f'https://api.crossref.org/works?filter={filter_params}&rows=500&cursor={cursor}'
        LOGGER.info(f"Requesting page {page_number} from URL {url}.")
        message = make_request_with_retry(url).json()['message']
        items = message['items']
        if not items:
            break
        s3_key = f'{s3_prefix}/{file_prefix}_page_{page_number}_{datetime.datetime.now().isoformat()}.json'
        save_to_s3(items, s3_key)
        fetched += len(items)
        if on_page:
            on_page()
        # Crossref omits next-cursor on the final (empty) page.
        cursor = message.get('next-cursor')
        if cursor is None:
            break
        page_number += 1
        time.sleep(.5)
    return fetched


def save_to_s3(items, s3_key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps({'items': items}),  # same format as crossref snapshot
        ContentType='application/json; charset=utf-8'
    )


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None


def write_json(key, value):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(value), ContentType='application/json')


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def parse_hour(text):
    return datetime.datetime.strptime(text[:13], '%Y-%m-%dT%H').replace(tzinfo=datetime.timezone.utc)


def fmt_hour(hour):
    return hour.strftime('%Y-%m-%dT%H')


def pull_hour(hour, on_page):
    """Pull every record Crossref indexed in this hour; restart the hour from its first page if a page keeps failing."""
    filter_params = (f'from-index-date:{hour:%Y-%m-%dT%H}:00:00,'
                     f'until-index-date:{hour:%Y-%m-%dT%H}:59:59')
    for attempt in range(1, HOUR_ATTEMPTS + 1):
        try:
            return get_crossref_data(filter_params, f'crossref/updates/{hour:%Y/%m/%d}', f'works_{hour:%H}', on_page)
        except requests.exceptions.RequestException as e:
            if attempt == HOUR_ATTEMPTS:
                raise
            LOGGER.warning(f"Hour {fmt_hour(hour)} failed ({e}); restarting it (attempt {attempt + 1}).")
            time.sleep(60)


def pull_updates(start, end, checkpoint):
    """Pull hours [start, end). With checkpoint=True, record each finished hour and hold a lock against a second run."""
    run_id = f"{os.getenv('DYNO', 'local')}-{utcnow().isoformat()}"
    last_beat = [utcnow()]

    def heartbeat():
        if checkpoint and utcnow() - last_beat[0] > datetime.timedelta(minutes=2):
            write_json(LOCK_KEY, {'run': run_id, 'at': utcnow().isoformat()})
            last_beat[0] = utcnow()

    if checkpoint:
        lock = read_json(LOCK_KEY)
        if lock and utcnow() - datetime.datetime.fromisoformat(lock['at']) < LOCK_STALE:
            LOGGER.info(f"Another updates run ({lock['run']}) is live; exiting.")
            return
        write_json(LOCK_KEY, {'run': run_id, 'at': utcnow().isoformat()})

    hour = start
    try:
        while hour < end:
            fetched = pull_hour(hour, heartbeat)
            LOGGER.info(f"Hour {fmt_hour(hour)} done: {fetched} records.")
            if checkpoint:
                write_json(CHECKPOINT_KEY, {'next_hour': fmt_hour(hour + HOUR), 'at': utcnow().isoformat()})
            hour += HOUR
    finally:
        if checkpoint:
            s3.delete_object(Bucket=S3_BUCKET, Key=LOCK_KEY)
            behind = utcnow() - hour
            if behind > ALERT_BEHIND:
                sentry_sdk.capture_message(
                    f"Crossref updates harvest is {behind.total_seconds() / 3600:.0f} h behind (next hour {fmt_hour(hour)})",
                    level='error')


def main():
    parser = argparse.ArgumentParser(description='Pull Crossref data (new works or updates).')
    parser.add_argument('mode', choices=['new', 'updates', 'october_2024'],
                        help='Specify whether to pull new works, updates, or October 2024 data.')
    parser.add_argument('--from', dest='from_hour', help='updates only: first hour to pull, YYYY-MM-DDTHH (UTC)')
    parser.add_argument('--until', dest='until_hour', help='updates only: hour to stop before, YYYY-MM-DDTHH (UTC)')
    args = parser.parse_args()

    now = utcnow()

    if args.mode == 'new':
        yesterday_str = (now - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        filter_params = f'from-created-date:{yesterday_str},until-created-date:{now:%Y-%m-%d}'
        get_crossref_data(filter_params, f'crossref/new-works/{now:%Y/%m/%d/%H}')

    elif args.mode == 'updates':
        end = now.replace(minute=0, second=0, microsecond=0) - SETTLE
        if args.from_hour:
            pull_updates(parse_hour(args.from_hour), parse_hour(args.until_hour) if args.until_hour else end, False)
        else:
            state = read_json(CHECKPOINT_KEY)
            # No checkpoint yet: start where the old two-day window did.
            start = parse_hour(state['next_hour']) if state else (end - datetime.timedelta(days=2)).replace(hour=0)
            pull_updates(start, end, True)

    elif args.mode == 'october_2024':
        filter_params = 'from-created-date:2024-10-23,until-created-date:2024-10-30'
        LOGGER.info(f"Running October 2024 batch job with filter: {filter_params}")
        get_crossref_data(filter_params, 'crossref/new-works/2024/10')


if __name__ == '__main__':
    main()
