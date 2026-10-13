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

Owner: Rohan (writes, deploys, watches failures). Casey and Jason get an FYI.
Deploy: push to openalex-ingest main; Heroku auto-deploys. Confirm `heroku releases -a openalex-ingest` shows the new commit before any run.
Dependencies add openpyxl==3.1.5; existing boto3, pandas, pyarrow, requests and lxml are reused.
No Procfile change, release phase or database migration.
Schedule (Advanced Scheduler add-on, cron, UTC):
  NSF:      `0 20 * * 1`  python -m nsf_awards --scheduled   (full fetch Mondays)
  Wellcome: `0 20 1 * *`  python -m wellcome_awards --scheduled   (full fetch on day 1)
Manual commands without --scheduled run immediately for a missed run or retry.
Failures exit nonzero with a JSON log line; Rohan watches them.
Uploads are refused unless the prefix is exactly s3://openalex-ingest/awards/dev-gr1009/{nsf,wellcome}/ (checked in code before any I/O).
Each Awards run reads the newest snapshot only; prune old dev snapshots before the Awards job is unpaused.
Production promotion requires a separately approved prefix-guard change and DESIGN rollout gates.

Rollback:
  1. Delete the two Advanced Scheduler entries.
  2. `heroku rollback <previous release> -a openalex-ingest` (the release before this push).
  3. `git revert` the award-fetcher commits on main (no force-push).
  4. If needed, delete objects under s3://openalex-ingest/awards/dev-gr1009/{nsf,wellcome}/.

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
Evidence (run logs, prod metadata, dev verification, query log, checksums) is kept outside the repo.

NSF run: 651,053 rows; 589,627,277 bytes; 558.434 seconds.
NSF prod: 644,453 rows; 571,953,499 bytes; modified 2026-01-08T20:38:54+00:00.
NSF delta: +6,600 rows; exact 49-column order/type match by remote footer.
NSF processed all 70 ZIP archives in the discovery manifest.
NSF dev: s3://openalex-ingest/awards/dev-gr1009/nsf/2026-10-09.parquet
Both production objects retain their measured size and LastModified.
No award temp directories, raw downloads or local parquet outputs remain.
Initial df -h showed 19 GiB free; concurrent machine use varied during the run.

Batch 1 completion: CORDIS, Gates and KAKEN.
Run: python -m cordis_awards; python -m gates_awards; python -m kaken_awards.
Each accepts --scheduled and runs only on UTC day 1; manual runs bypass the calendar.
Origin scripts specify no other recurring cadence. No Scheduler entry was created.
EUROPEAN_COMMISSION_AWARDS_S3_PREFIX, GATES_AWARDS_S3_PREFIX and KAKEN_AWARDS_S3_PREFIX
accept only s3://openalex-ingest/awards/dev-gr1009/{european_commission,gates,kaken}/ respectively.
The shared harness dates object names YYYY-MM-DD.parquet and verifies the upload.
KAKEN requires KAKEN_APPID in the process environment before any HTTP request.
No local .env auto-loading, credential logging, HTML scraping, registry or Databricks access.
Register an application ID at https://support.nii.ac.jp/en/cinii/api/developer.

CORDIS downloads all three programmes, one ZIP at a time; any failed programme aborts.
The current JSON ZIP has per-project documents rather than the origin's flat arrays.
A JSON adapter restores the flat inputs, including nested grant DOI, scheme and coordinator.
JSON preserves objective line breaks; the CSV alternative failed a fresh-file content check.
Origin programme order, first-ID deduplication, date conversions and output mapping are retained.
Each raw ZIP is removed after its programme has been written to parquet.
Gates retains the origin date/duration/amount conversion and first-ID deduplication.
All-empty CSV records are omitted; populated rows without grant IDs fail.
KAKEN retains the origin EN/JA overlay, fiscal-year paging and first-ID deduplication.
Each XML page is streamed, checked against totalResults, written to parquet, then removed.
Missing totals, malformed records, short/repeated pages or changing totals abort.
KAKEN output is the measured production 17-column schema.
Six API-only fields are deliberately omitted: amount_direct, amount_indirect, project_type,
project_status, institution_nii_code, members_json. No SQL migration is needed.
These three production schemas are frozen in award_schemas.py.
