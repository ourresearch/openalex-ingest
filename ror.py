import io
import json
import requests
import logging
import boto3
import pandas as pd
from zipfile import ZipFile

logging = logging.getLogger(__name__)

def get_most_recent_ror_dump_metadata():
    # https://ror.readme.io/docs/data-dump#download-ror-data-dumps-programmatically-with-the-zenodo-api
    url = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    r = requests.get(url)
    if r.status_code >= 400:
        return None
    most_recent_hit = r.json()["hits"]["hits"][0]
    files = most_recent_hit["files"]
    most_recent_file_obj = files[-1]
    return most_recent_file_obj


def download_and_unzip_ror_data(url):
    r_zipfile = requests.get(url)
    r_zipfile.raise_for_status()
    with ZipFile(io.BytesIO(r_zipfile.content)) as myzip:
        for fname in myzip.namelist():
            if "ror-data" in fname and fname.endswith(".json") and "schema_v2" in fname:
                with myzip.open(fname) as myfile:
                    ror_data = json.loads(myfile.read())
                    return ror_data, fname.split(".json")[0]
    return None, None

def get_file_list_s3_bucket(bucket_name, prefix):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    return [obj.key for obj in bucket.objects.filter(Prefix=prefix) if obj.key.endswith(".parquet")]

def main():
    ror_bucket = "openalex-ingest"

    most_recent_file_obj = get_most_recent_ror_dump_metadata()
    if most_recent_file_obj is None:
        logging.info("Failed to get ROR data. Exiting without doing any updates...")
        return
    
    try:
        file_url = most_recent_file_obj["links"]["self"]
    except KeyError:
        logging.error("Failed to get URL out of the most recent file! Exiting without doing any updates...")
        raise

    logging.info(f"downloading and unzipping ROR data from {file_url}")
    ror_data, fname = download_and_unzip_ror_data(file_url)
    if not ror_data:
        raise RuntimeError(
            "Failed to download and unzip ROR data! Exiting without doing any updates..."
        )

    files_in_s3 = get_file_list_s3_bucket(ror_bucket, "ror_snapshots")

    if f"{fname}.parquet" in files_in_s3:
        logging.info(f"Most recent ROR snapshot already saved. Exiting without saving snapshot...")
        return
    
    logging.info(f"Saving snapshot for {len(ror_data)} ROR records")
    pd.DataFrame(ror_data).to_parquet(f"s3://{ror_bucket}/ror_snapshots/{fname}.parquet")
    pd.DataFrame(ror_data).to_parquet(f"s3://{ror_bucket}/ror_current/ror_snapshot.parquet")
    logging.info(f"Saved snapshots!")

if __name__ == '__main__':
    main()