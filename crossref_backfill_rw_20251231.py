"""One-off backfill of the missed 2025-12-31 Crossref index wave (RW back-propagation).

oxjob #737. Same fetch/land pattern as crossref.py `updates` mode, plus:
- cursor checkpoint in S3 so a mid-run failure (or dyno death) resumes instead of restarting
- progress logging against total-results

Run:  heroku run:detached -a openalex-ingest -- python crossref_backfill_rw_20251231.py
"""
import datetime
import json
import os
import sys
import time

import boto3
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

S3_BUCKET = 'openalex-ingest'
S3_PREFIX = 'crossref/updates/2025/12/31-rw-backfill'
FILTER = 'from-index-date:2025-12-31,until-index-date:2025-12-31'
CHECKPOINT_KEY = 'state/crossref_backfill_rw_20251231.checkpoint.json'
CROSSREF_API_KEY = os.getenv('CROSSREF_API_KEY')

s3 = boto3.client('s3')


@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=2, min=4, max=120),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request_with_retry(url, headers):
    response = requests.get(url, headers=headers, timeout=120)
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        print(f"429 rate limited, sleeping {retry_after}s", flush=True)
        time.sleep(retry_after)
        response.raise_for_status()
    elif response.status_code >= 500:
        print(f"server error {response.status_code}, retrying", flush=True)
        response.raise_for_status()
    response.raise_for_status()
    return response


def load_checkpoint():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=CHECKPOINT_KEY)
        return json.loads(obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        return {'cursor': '*', 'page_number': 1, 'fetched': 0}


def save_checkpoint(cursor, page_number, fetched):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps({'cursor': cursor, 'page_number': page_number, 'fetched': fetched}),
        ContentType='application/json',
    )


def main():
    if not CROSSREF_API_KEY:
        sys.exit("CROSSREF_API_KEY not set")
    headers = {
        "Accept": "application/json",
        "User-Agent": "mailto:dev@ourresearch.org",
        "crossref-api-key": CROSSREF_API_KEY,
    }
    state = load_checkpoint()
    cursor, page_number, fetched = state['cursor'], state['page_number'], state['fetched']
    per_page = 500
    print(f"starting at page {page_number}, fetched so far {fetched}", flush=True)

    while True:
        url = f"https://api.crossref.org/works?filter={FILTER}&rows={per_page}&cursor={cursor}"
        response = make_request_with_retry(url, headers)
        data = response.json()
        message = data['message']
        items = message['items']
        total = message.get('total-results')

        if not items:
            print(f"DONE: no more items at page {page_number}; fetched {fetched}/{total}", flush=True)
            break

        current_timestamp = datetime.datetime.now().isoformat()
        s3_key = f'{S3_PREFIX}/works_page_{page_number}_{current_timestamp}.json'
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps({'items': items}, indent=2),
            ContentType='application/json; charset=utf-8',
        )
        fetched += len(items)
        print(f"page {page_number}: landed {len(items)} items ({fetched}/{total}) -> s3://{S3_BUCKET}/{s3_key}", flush=True)

        next_cursor = message.get('next-cursor')
        if not next_cursor:
            print(f"DONE: no next cursor at page {page_number}; fetched {fetched}/{total}", flush=True)
            break
        cursor = next_cursor
        page_number += 1
        save_checkpoint(cursor, page_number, fetched)
        time.sleep(0.5)

    print("backfill complete", flush=True)


if __name__ == '__main__':
    main()
