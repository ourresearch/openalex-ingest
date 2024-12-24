import datetime
import json
import os
import time
import urllib.parse

import boto3
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from common import S3_BUCKET, LOGGER

CROSSREF_API_KEY = os.getenv('CROSSREF_API_KEY')


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request_with_retry(url, headers):
    response = requests.get(url, headers=headers)

    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        LOGGER.warning(f"Rate limit exceeded (429). Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        response.raise_for_status()

    elif response.status_code >= 500:
        LOGGER.error(f"Server error {response.status_code} for URL {url}. Retrying...")
        response.raise_for_status()

    return response


def get_journals_data(s3_bucket, s3_prefix):
    base_url = 'https://api.crossref.org/journals'
    headers = {
        "Accept": "application/json",
        "User-Agent": "mailto:dev@ourresearch.org",
        "crossref-api-key": CROSSREF_API_KEY
    }
    rows = 1000
    cursor = '*'
    page_number = 1
    has_more_pages = True

    while has_more_pages:
        encoded_cursor = urllib.parse.quote(cursor)
        url = f"{base_url}?rows={rows}&cursor={encoded_cursor}"

        response = make_request_with_retry(url, headers)
        LOGGER.info(f"Requesting page {page_number} using cursor: {cursor}")

        data = response.json()
        items = data['message']['items']
        total_results = data['message']['total-results']

        if items:
            current_timestamp = datetime.datetime.now().isoformat()
            s3_key = f'{s3_prefix}/journals_page_{page_number}_{current_timestamp}.json'
            save_to_s3(items, s3_bucket, s3_key)
            LOGGER.info(f"Saved page {page_number} with {len(items)} journals")
        else:
            LOGGER.info(f"No more items to fetch on page {page_number}. Ending pagination.")
            has_more_pages = False

        if 'next-cursor' not in data['message']:
            LOGGER.info("No next cursor found. Pagination complete.")
            has_more_pages = False
        else:
            cursor = data['message']['next-cursor']
            page_number += 1
            time.sleep(.5)  # Basic rate limiting


def save_to_s3(json_data, s3_bucket, s3_key):
    LOGGER.info(f"Saving {len(json_data)} journals to S3 bucket {s3_bucket} with key {s3_key}.")
    s3 = boto3.client('s3')
    data_to_save = {'items': json_data}
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(data_to_save, indent=2),
        ContentType='application/json; charset=utf-8'
    )


def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    s3_prefix = f'crossref-journals/{now.strftime("%Y/%m/%d/%H")}'
    get_journals_data(S3_BUCKET, s3_prefix)


if __name__ == '__main__':
    main()