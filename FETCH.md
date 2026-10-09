Harvesters are top-level Python modules with main() entry points and S3 output.
Procfile contains only dummy_process: python -m ror.
README configures daily repository harvesting through Heroku Scheduler.
There is no clock process, cron file, or in-repo scheduler implementation.
common.py opens the registry database on import; award fetchers avoid that import.

Design: DESIGN v1.2; vendor HTTP -> local fetcher -> S3 -> Databricks.
Fresh ingest base: 79f863e4901e206fdac51bfe3a3667c51f873505.
Walden origin checked via gh api: c3db0a327d5c2c12f2aee6e65591b472507c9252.
Both supplied origin/main laptop scripts are byte-identical to current GitHub origin.

Run NSF: python -m nsf_awards
Run Wellcome: python -m wellcome_awards
NSF_AWARDS_S3_PREFIX defaults to s3://openalex-ingest/awards/dev-gr1009/nsf/.
WELLCOME_AWARDS_S3_PREFIX defaults to s3://openalex-ingest/awards/dev-gr1009/wellcome/.
This build rejects any other prefix before network or S3 access.
Object names use the run-start UTC date: YYYY-MM-DD.parquet.
No registry database access and no Databricks API calls.

NSF streams one ZIP at a time and writes batches of 1,000 records to one parquet.
Each ZIP is deleted after its records are written; no JSON extraction or raw cache.
Wellcome discovers one grants XLSX link from https://wellcome.org/research-funding/funding-portfolio.
Missing, ambiguous, or off-host workbook links fail; there is no old-URL fallback.
Wellcome retains Internal ID grant_ref, fallback, date/amount conversion and deduplication.
Production column order and Arrow types are frozen in award_schemas.py.
New populated NSF null-only fields or changed Wellcome columns fail for review.
HTTP has timeouts/retries; partial inputs and empty outputs cannot upload.
Repeat runs reject shrink versus the newest dev parquet; failed baseline reads also stop.
Uploads use boto3 single PUT; HEAD verifies bytes and SHA256 metadata.
Temporary raw and parquet files are removed on completion or failure.
Download guards require 1 GiB reserve initially and stop below 512 MiB.

Schedule NSF: daily Heroku Scheduler entry python -m nsf_awards --scheduled at 20:00 UTC.
That entry performs the full fetch on Mondays only; other days exit before I/O.
Schedule Wellcome: daily entry python -m wellcome_awards --scheduled at 20:00 UTC.
That entry performs the full fetch on UTC day 1 only; other days exit before I/O.
Manual commands without --scheduled run immediately for a missed run or retry.
Casey: review and take this local commit into his deployment checkout.
Casey: deploy it through his existing openalex-ingest Heroku release flow.
Dependencies add openpyxl==3.1.5; existing boto3, pandas, pyarrow, requests and lxml are reused.
Casey: configure AWS credentials limited to the two dev prefixes and set the env vars above.
Casey: add the two Scheduler entries and monitor nonzero exits and JSON completion logs.
No Procfile change or database migration is needed.
Production promotion requires a separately approved prefix-guard change and DESIGN rollout gates.
No pushes, Heroku commands, production writes, or messages were performed here.

Wellcome run: 26,288 rows; 19,818,446 bytes; 16.434 seconds.
Wellcome prod: 19,618 rows; 15,277,282 bytes; modified 2026-07-31T13:57:33Z.
Wellcome delta: +6,670 rows; exact 29-column order/type match by remote footer.
Wellcome source now covers October 2000 to September 2026, per the discovered filename.
Wellcome dev: s3://openalex-ingest/awards/dev-gr1009/wellcome/2026-10-09.parquet

Validation: 7 offline tests passed with both local and repo-pinned boto3/PyArrow.
Pinned check: boto3 1.35.47, PyArrow 17.0.0, pandas 2.2.3, openpyxl 3.1.5.
Live downloads ran on Python 3.9.6, boto3 1.20.33, PyArrow 14.0.2.
Heroku runtime.txt specifies Python 3.10.8; no Heroku execution was attempted.
Both --scheduled commands correctly skipped 2026-10-09 without I/O.
The repeat-run shrink guard was added after the first live runs started and tested offline plus read-only against both new dev objects.
Evidence lives beside this clone in ../fetch: run logs, prod metadata, dev verification, query log and SHA256SUMS.

NSF run: 651,053 rows; 589,627,277 bytes; 558.434 seconds.
NSF prod: 644,453 rows; 571,953,499 bytes; modified 2026-01-08T20:38:54+00:00.
NSF delta: +6,600 rows; exact 49-column order/type match by remote footer.
NSF processed all 70 ZIP archives in the discovery manifest.
NSF dev: s3://openalex-ingest/awards/dev-gr1009/nsf/2026-10-09.parquet
Both production objects retain their measured size and LastModified.
No award temp directories, raw downloads or local parquet outputs remain.
Initial df -h showed 19 GiB free; concurrent machine use varied during the run.
