import requests
import boto3
import json
import logging
import argparse
from datetime import datetime, timedelta
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import gzip
from io import BytesIO

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("datacite_harvester")

BASE_URL = "https://api.datacite.org/dois"
BATCH_SIZE = 1000
S3_BUCKET = "openalex-ingest"

s3_client = boto3.client("s3")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def fetch_page(cursor_url):
    """
    Fetch a single page of results using the given cursor URL.
    Retries on network errors.
    """
    LOGGER.info(f"Fetching page: {cursor_url}")
    response = requests.get(cursor_url)
    response.raise_for_status()
    return response.json()


def compress_json(data):
    """
    Compress JSON data using Gzip.
    Returns a BytesIO object containing the compressed data.
    """
    compressed_buffer = BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode="w") as gz_file:
        gz_file.write(json.dumps(data).encode("utf-8"))
    compressed_buffer.seek(0)
    return compressed_buffer


def upload_to_s3(date_path, batch_number, works):
    """
    Upload a batch of works to S3 in a date-based directory.
    """
    timestamp = int(datetime.now().timestamp())
    object_key = f"datacite/works-new/{date_path}/batch_{batch_number}_{timestamp}.json.gz"
    compressed_data = compress_json(works)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=object_key,
        Body=compressed_data,
        ContentType="application/gzip",
    )
    LOGGER.info(f"Uploaded batch {batch_number} to S3: {object_key}")


def get_date_path_from_batch(works):
    """
    Get the date-based directory path from the first work in the batch.
    """
    if not works:
        return "unknown_date"
    first_work = works[0]
    updated_str = first_work["attributes"].get("updated")
    if updated_str:
        updated_dt = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
        return updated_dt.strftime("%Y/%m/%d")  # Format: YYYY/MM/DD
    return "unknown_date"


def harvest_datacite_works(from_date, to_date):
    """
    Harvest works from the DataCite API using cursor pagination and upload to S3 in date-based directories.
    """
    LOGGER.info(f"Starting harvest from {from_date} to {to_date}")

    # Start with initial cursor
    cursor_url = (
        f"{BASE_URL}?page[cursor]=1&page[size]={BATCH_SIZE}&query=updated:[{from_date}T00:00:00Z TO {to_date}T23:59:59Z]&sort=updated"
    )

    total_records = 0
    batch_number = 0
    works_batch = []

    while cursor_url:
        data = fetch_page(cursor_url)
        works = data.get("data", [])
        total_records += len(works)
        works_batch.extend(works)

        if len(works_batch) >= BATCH_SIZE:
            batch_number += 1
            date_path = get_date_path_from_batch(works_batch)  # Use the first record's date
            upload_to_s3(date_path, batch_number, works_batch)
            works_batch = []

        # update cursor URL to the next page
        links = data.get("links", {})
        cursor_url = links.get("next")

        if not cursor_url:
            LOGGER.info("No more pages to fetch.")

    # upload any remaining records
    if works_batch:
        batch_number += 1
        date_path = get_date_path_from_batch(works_batch)  # Use the first record's date
        upload_to_s3(date_path, batch_number, works_batch)

    LOGGER.info(f"Harvest complete. Total records fetched: {total_records}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Harvest DataCite works and upload to S3.")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD).")
    parser.add_argument("--yesterday", action="store_true", help="Fetch records from the last 24 hours.")
    args = parser.parse_args()

    if args.yesterday:
        now = datetime.utcnow()
        previous_day = now - timedelta(days=1)
        from_date = previous_day.strftime("%Y-%m-%d")
        to_date = previous_day.strftime("%Y-%m-%d")
        LOGGER.info(f"--today argument provided. Fetching updates for {from_date} (full day).")
    elif args.from_date and args.to_date:
        from_date = args.from_date
        to_date = args.to_date
    else:
        parser.error("You must specify either --yesterday or both --from-date and --to-date.")

    harvest_datacite_works(from_date, to_date)
