"""Shared bounded downloads and dev-only publication for award fetchers.

Deliberately independent of common.py, which opens the registry database on import.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import io
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import struct
import tempfile
import time
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def http_session():
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def download(session, url, path, expected_size=None):
    # Retry interrupted bodies from byte zero; never reuse a partial download.
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=(15, 120)) as response:
                response.raise_for_status()
                size = expected_size or int(response.headers.get("Content-Length", 0))
                if shutil.disk_usage(path.parent).free < size + 1024 ** 3:
                    raise OSError("Less than 1 GiB reserve after this download")
                with path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        handle.write(chunk)
                        if shutil.disk_usage(path.parent).free < 512 * 1024 ** 2:
                            raise OSError("Disk reserve below 512 MiB")
                if size and path.stat().st_size != size:
                    raise ValueError("Incomplete download (size mismatch)")
            return
        except (requests.RequestException, ValueError):
            path.unlink(missing_ok=True)
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def destination(source, date):
    allowed = f"s3://openalex-ingest/awards/dev-gr1009/{source}/"
    prefix = os.environ.get(f"{source.upper()}_AWARDS_S3_PREFIX", allowed)
    if prefix != allowed:
        raise ValueError(f"This dev build only permits {allowed}")
    parsed = urlparse(prefix)
    return parsed.netloc, parsed.path.lstrip("/") + f"{date:%Y-%m-%d}.parquet"


def is_due(source, now):
    return now.weekday() == 0 if source == "nsf" else now.day == 1


def previous_rows(s3, bucket, prefix):
    """Compare with the newest complete dev snapshot; S3 errors must not bypass the guard."""
    objects = (obj for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix) for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet"))
    latest = max(objects, key=lambda obj: (obj["LastModified"], obj["Key"]), default=None)
    if latest is None:
        return 0
    kwargs = {"Bucket": bucket, "Key": latest["Key"]}
    with closing(s3.get_object(**kwargs, Range="bytes=-8")["Body"]) as body:
        tail = body.read()
    length = struct.unpack("<I", tail[:4])[0]
    with closing(s3.get_object(**kwargs, Range=f"bytes=-{length + 8}")["Body"]) as body:
        footer = body.read()
    return pq.ParquetFile(io.BytesIO(b"PAR1" + footer)).metadata.num_rows


def run(source, build, schema):
    parser = argparse.ArgumentParser(description=f"Fetch {source} awards into the dev S3 prefix")
    parser.add_argument("--scheduled", action="store_true",
                        help="Daily Scheduler entry: run Monday (NSF) or day 1 (Wellcome), UTC")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    bucket, key = destination(source, now)  # Reject unsafe config before any I/O.
    if args.scheduled and not is_due(source, now):
        print(f"{source}: not due on {now:%Y-%m-%d} UTC", flush=True)
        return
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"awards-{source}-") as workdir:
        path = build(Path(workdir))
        parquet = pq.ParquetFile(path)
        rows = parquet.metadata.num_rows
        if not rows or not parquet.schema_arrow.equals(schema, check_metadata=False):
            raise ValueError("Empty output or changed parquet schema; refusing upload")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        size = path.stat().st_size
        s3 = boto3.client("s3", config=Config(retries={"mode": "standard", "max_attempts": 5}))
        baseline_rows = previous_rows(s3, bucket, key.rsplit("/", 1)[0] + "/")
        if rows < baseline_rows:
            raise ValueError(f"Output shrank from {baseline_rows} to {rows}; refusing upload")
        # Single PUT avoids multipart cleanup and exposes only a complete object.
        if size >= 5 * 1024 ** 3:
            raise ValueError("Output exceeds the single-PUT limit")
        s3.upload_file(str(path), bucket, key,
                       ExtraArgs={"Metadata": {"sha256": digest.hexdigest(), "rows": str(rows)}},
                       Config=TransferConfig(multipart_threshold=5 * 1024 ** 3, use_threads=False))
        head = s3.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or head["Metadata"].get("sha256") != digest.hexdigest():
            raise ValueError("Uploaded object verification failed")
        print(json.dumps({"source": source, "uri": f"s3://{bucket}/{key}", "rows": rows,
                          "bytes": size, "runtime_seconds": round(time.monotonic() - started, 3),
                          "sha256": digest.hexdigest()}), flush=True)
