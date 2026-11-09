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
NSF_AWARDS_S3_PREFIX defaults to s3://openalex-ingest/awards/nsf/ (the prefix walden's Awards job reads).
WELLCOME_AWARDS_S3_PREFIX defaults to s3://openalex-ingest/awards/wellcome/.
Any other prefix (including the old awards/dev-gr1009/) is rejected before network or S3 access.
Object names use the run-start UTC date: YYYY-MM-DD.parquet.
No registry database access and no Databricks API calls.

NSF streams one ZIP at a time and writes batches of 1,000 records to one parquet.
Each ZIP is deleted after its records are written; no JSON extraction or raw cache.
Wellcome discovers one grants XLSX link from https://wellcome.org/research-funding/funding-portfolio.
Missing, ambiguous, or off-host workbook links fail; there is no old-URL fallback.
Wellcome retains Internal ID grant_ref, fallback, date/amount conversion and deduplication.
Production column order and Arrow types are frozen in award_schemas.py.
New populated NSF null-only fields or changed Wellcome columns fail for review.
NSF keeps the awardee (inst block) as awardee_name / awardee_country_name next to perf_inst's place of
performance (inst_*), since 2026-11-05 (oxjob #2032): NSF leaves perf_inst 'DATA NOT AVAILABLE' on ~145k pre-FY1995 awards.
HTTP has timeouts/retries; partial inputs and empty outputs cannot upload.
Repeat runs reject shrink versus the newest parquet under the prefix; failed baseline reads also stop.
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
Uploads are refused unless the prefix is exactly s3://openalex-ingest/awards/<source>/ (checked in code before any I/O).
Each Awards run reads the newest object per prefix only (max LastModified, then path); older objects are ignored.
Prod promotion (ce1015-awards-prod-prefix): the next 00:30 UTC Awards run after a fetch MERGEs the fresh object.

Rollback:
  1. Delete the two Advanced Scheduler entries.
  2. `heroku rollback <previous release> -a openalex-ingest` (the release before this push).
  3. `git revert` the award-fetcher commits on main (no force-push).
  4. Delete every dated object (YYYY-MM-DD.parquet) the fetchers wrote under s3://openalex-ingest/awards/<source>/;
     otherwise the next 00:30 UTC Awards run still reads it as the newest object.

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
EUROPEAN_COMMISSION_AWARDS_S3_PREFIX, GATES_AWARDS_S3_PREFIX and KAKEN_API_AWARDS_S3_PREFIX
accept only s3://openalex-ingest/awards/{european_commission,gates,kaken_api}/ respectively.
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
Five API-only fields are deliberately omitted: amount_direct, amount_indirect, project_type,
project_status, institution_nii_code. members_json (every member: role, names, researcher number, affiliation)
is the 18th column; walden maps its co-investigators. No SQL migration is needed.
These three production schemas are frozen in award_schemas.py.

Gateway to Research (UKRI: AHRC, BBSRC, EPSRC, ESRC, MRC, NERC, STFC, Innovate UK).
Run: python -m gtr_awards. --scheduled runs on UTC day 1 only. No key needed.
GATEWAY_TO_RESEARCH_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/gateway_to_research/.
The old laptop file (s3://openalex-ingest/awards/gtr/gtr_projects.parquet, 2026-05-15) stays where it is; nothing reads it after the walden move.
Three bulk passes at 100 per page, one page at a time, 0.5 s apart: /persons (JSON), /funds (XML), /projects (XML).
Persons are read as JSON because the XML form leaves out orcidId; the other two keep walden's laptop parser (gtr_to_s3.py, origin d3fd37ba).
Every page is checked against totalSize/totalPages; a moved total, a short or repeated page, or a final count short of totalSize aborts.
Projects are deduplicated on lower(grant reference), first page wins; projects without an RCUK reference are dropped (walden drops them too).
persons: one entry per person linked to the project (every *_PER link), best role plus all roles, sorted by role rank, family name, given name, person id.
More than 0.5% of person links naming no listed person refuses the upload.
Dates keep the laptop conversion (UTC date of GtR's instant), so UK summer dates stay one day early, as served today.
Measured 2026-11-02: 158,710 projects, 95,176 persons, 158,710 funds: 4,128 pages, about 2 hours, about 1.6 GB downloaded one page at a time.
GtR re-keyed its records in 2026-09: ids in the 2026-05 file do not carry over (19 of 200 sampled old project ids now open another grant).
Schedule (after the first manual run is checked and walden reads the prefix):
  GtR:      `0 20 1 * *`  python -m gtr_awards --scheduled   (full fetch on day 1)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/gateway_to_research/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip GtR and fail).

FCT SciPROJ (Fundação para a Ciência e a Tecnologia, Portugal; FCT/FCCN's national project registry, CC BY 4.0).
Run: python -m fct_awards. Weekly; SciPROJ compiles its dump weekly. No --scheduled: the Scheduler cron is Monday-only.
Needs SCIPROJ_API_USERNAME and SCIPROJ_API_PASSWORD (HTTP Basic; the production login) in Heroku config, never in code,
logs or URLs; a missing value fails the run before any HTTP request.
FCT_SCIPROJ_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/fct_sciproj/.
Lists /services/searches/projects/dump?year=YYYY (last year too in early January), takes the newest getDumpXMLyyyymmdd_PUB|PRV.zip on
api.ptcris.pt with the agent `Mozilla/5.0 (compatible; OpenAlex-ingest/1.0; +https://openalex.org)` (SciPROJ's WAF
refuses python-requests' default) (a public file wins a same-day tie; an off-host link is refused), downloads it once, and parses each CERIF
record with walden's laptop parser (scripts/local/fct_to_s3.py) ported, with two changes (Rohan, 11-05: "as published, no fill-ins"):
amount is the project total, the sum of the record's <Funded> streams (FCT's national share + FEDER co-funding; 6,618 records have
2-5 streams), where the laptop took the first stream; a stream in any currency but EUR fails the run. And the frozen 2026-06 fct.pt
XLSX list (its URL 404s) is no longer merged in.
Added: pi_orcid, pi_ciencia_id, team_json (members in SciPROJ's order, PI and repeats left out), sciproj_id, source_dump.
E-mail addresses, birth dates, gender and people's country codes in the private dump are never read into the output.
Refuses upload below 100,000 rows, or when under 85% of FCT rows have a PI (a dump without Person, or truncated).
Download links carry a token in their path; urllib3's retry log lines are redacted (getFileDump/***).
Measured 2026-11-05 on the newest public file (getDumpXML20261104_PUB.zip, 215,849,280 bytes, 106 s to download from a laptop):
106,785 records -> 106,785 rows, 25 s to parse; 49,563 with an amount; 533,404 team members (126k on projects), 42,487 PI ORCIDs.
With this login SciPROJ lists public-profile files only (45 in 2026). Fellowship amounts and programmes are blank because FCT
keeps them private; the public file has no e-mail, birth date or gender. The listing answers XML whatever Accept says.
The old object (s3://openalex-ingest/awards/fct/fct_grants.parquet, 2026-08-20) and its zip stay where they are; nothing reads them
after the walden move.
Schedule (after the first manual run is gated and walden reads the prefix):
  FCT:      `0 20 * * 1`  python -m fct_awards   (Mondays 20:00 UTC)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/fct_sciproj/ only after walden's
  Awards job no longer reads that prefix.

KAKEN moves to its own prefix (feeds-d, 2026-11): python -m kaken_awards publishes to s3://openalex-ingest/awards/kaken_api/.
awards/kaken/ keeps the 2026-07-08 page scrape (kaken_projects.parquet) that walden's kaken row reads until its walden push points
the row at awards/kaken_api/; a manual fetch therefore changes nothing served until that push, and the gate runs in between.
members_json carries every project member in KAKEN's order (role code, family/given name, researcher number, e-Rad code,
affiliation, institution NII code); walden maps co_investigator_buntan / co_investigator_renkei to investigators.
Paging: one OpenSearch query per start fiscal year (o1=1, s1=s2=FY, 1964..this year), 500 per page; KAKEN caps a query at
200,000 results and the largest start year holds well under that. ~1,120,000 projects is ~2,300 pages at >= 1 s each.
KAKEN_APPID (the CiNii Web API application id) lives in Heroku config only; the URL that carries it is never logged.
Schedule (after the first manual run is gated and walden reads the prefix):
  KAKEN:    `0 20 1 * *`  python -m kaken_awards --scheduled   (full fetch on day 1)
Rollback: delete the Scheduler entry, revert the commit; delete the dated objects under awards/kaken_api/ only after walden's
  kaken row points back at awards/kaken/ (an empty prefix makes the Awards job skip KAKEN and fail).

TÜBİTAK (ARDEB) projects from TR Dizin (ULAKBİM). Run: python -m tubitak_awards. --scheduled runs on UTC day 1 only. No key needed.
TUBITAK_TRDIZIN_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/tubitak_trdizin/.
The old laptop file (s3://openalex-ingest/awards/tubitak/tubitak_trdizin_projects.parquet, 2026-06-03, 27,345 rows) stays where it is;
nothing reads it after the walden move.
Reads TR Dizin's search API (https://search.trdizin.gov.tr/api/defaultSearch/publication/, facet-documentType=PROJECT), 100 per page,
0.5 s apart; every page is checked against hits.total, and the distinct hit ids must equal it. walden's laptop parser (tubitak_to_s3.py,
origin e22fdf7f) is ported unchanged: 44 string columns, project-number dedupe, lead = first MANAGER by order.
Measured 2026-11-05 (laptop harness): 29,236 hits on 293 pages -> 28,687 projects, about 9 minutes, about 1,000 MB downloaded.
Schedule (after the first manual run is checked and walden reads the prefix):
  TÜBİTAK:  `0 20 1 * *`  python -m tubitak_awards --scheduled   (full fetch on day 1)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/tubitak_trdizin/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip TÜBİTAK and fail).
Memory: the laptop run peaked at ~1.0 GB RSS before rows were written in chunks of 2,000; run the one-off and the Scheduler
entry on a standard-2x dyno (`heroku run --size=standard-2x python -m tubitak_awards -a openalex-ingest`).

NHMRC (Australia) outcomes: a manual source, one object. Run: python -m nhmrc_awards (no key; no Scheduler entry).
NHMRC_OUTCOMES_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/nhmrc_outcomes/.
NHMRC publishes per-round "Summary of results" XLSX only on www.nhmrc.gov.au, whose Akamai front refuses
non-browser clients (2026-11-05: HTTP/2 INTERNAL_ERROR, HTTP/1.1 read timeout, even robots.txt), so the module downloads nothing.
It reads, read-only: s3://openalex-ingest/awards/nhmrc/nhmrc_projects.parquet (2013-2025 rounds, frozen) and
nhmrc_historical_1993_2016.parquet (frozen), plus every .xlsx under s3://openalex-ingest/awards/manual/nhmrc/ (parsed as
walden's scripts/local/nhmrc_to_s3.py did; round year from NHMRC's file name; a drop's rows replace primary rows with the
same grant id; later round, then later upload, wins). Output: part ('primary'|'historical') + the primary file's 64 columns
+ the 6 historical-only columns, all strings. Refuses: short frozen inputs (< 11,902 / 21,000 rows), changed columns,
a drop with no round year in its name or no grant id / title / CIA column.
Measured 2026-11-05 on the frozen inputs, no drops: 32,914 rows (11,902 primary + 21,012 historical), 5.7 MB, 6 s.
Hand step per NHMRC release (2-4 a year; NHMRC releases in stages, re-issuing a round's file):
  1. In a browser, download the round's "Summary of results" XLSX from
     https://www.nhmrc.gov.au/funding/data-research/outcomes and keep NHMRC's file name (it carries the round year).
  2. aws s3 cp <that file> s3://openalex-ingest/awards/manual/nhmrc/
  3. heroku run -a openalex-ingest python -m nhmrc_awards   (prints one JSON line per drop and the totals)
  The next 00:30 UTC Awards run reads the new object (walden sources.csv row nhmrc, cadence manual).
Rollback: delete the dated objects under awards/nhmrc_outcomes/ only after walden's Awards job no longer reads that prefix
  (an empty prefix makes the job skip nhmrc and fail); revert the commit. The frozen inputs are never written.

Research Council of Norway (Forskningsrådet), Prosjektbanken, Kilde=FORISS (RCN's own grants). Run: python -m rcn_awards.
--scheduled runs on UTC day 1 only. No key needed.
RCN_PROJECT_BANK_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/rcn_project_bank/.
The old laptop file (s3://openalex-ingest/awards/research_council_norway/research_council_norway_projects.parquet, 2026-06-04,
44,316 rows) stays where it is; nothing reads it after the walden move.
Reads the explore page's __NEXT_DATA__ for the current Next.js buildId, then /_next/data/<buildId>/en/explore/projects.json,
500 per page, 0.5 s apart, checked against pagination.totalHits; walden's laptop parser (research_council_norway_to_s3.py,
origin e22fdf7f) ported unchanged: the same 28 string columns, alphabetical, sorted by source_record_id.
Refuses the upload on: no buildId, an empty page before totalHits, totalHits moving mid-pull, a project id repeated while
paging, distinct projects != totalHits, or a duplicate award id. totalHits goes in the object's footer (openalex.funder_total). Prosjektbanken's robots.txt disallows named AI crawlers only.
Not yet run from Heroku (2026-11-05): the first manual run is its live test (expect ~44.5k rows, ~90 pages, a few minutes).
Schedule (after the first manual run is checked and walden reads the prefix):
  RCN:      `0 20 1 * *`  python -m rcn_awards --scheduled   (full fetch on day 1)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/rcn_project_bank/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip RCN and fail).

USAspending prime awards (grants and cooperative agreements) for 11 walden rows: DOD, DOE, NASA, USDA, EPA, USAID,
CDC, AHRQ, NOAA, NIST and DOE Office of Science. Replaces the 11 laptop scripts (walden scripts/local/<agency>_to_s3.py).
Run: python -m usaspending_awards (all 11; a failed row does not stop the others; exit nonzero names the failed rows).
One or more rows: USASPENDING_SOURCES=usaspending_cdc,usaspending_ahrq python -m usaspending_awards
No key needed. User-Agent names OpenAlex. No registry or Databricks access.
Prefixes (one per row; each USASPENDING_<AGENCY>_AWARDS_S3_PREFIX accepts only its own):
  s3://openalex-ingest/awards/usaspending_{dod,doe,nasa,usda,epa,usaid,cdc,ahrq,noaa,nist,doe_sc}/
The old laptop objects (awards/dod/, doe/, nasa/, usda/, epa/, cdc/, usaid/, noaa/, nist/, ahrq/, doe_sc/) stay where they are.
Ten rows read the Award Data Archive (files.usaspending.gov/award_data_archive/), discovered from the public bucket listing:
  the newest set of FY<year>_<toptier>_Assistance_Full_<date>.zip per agency, FY2007 through the fiscal year of its date
  (and every year an older set lists), all one date, else stop.
  Toptier files 097, 089, 080, 012, 068, 072, 075 (CDC and AHRQ), 013 (NOAA and NIST): each downloaded once per run.
  Prime award types 02-05; agency filter as the laptop scripts (awarding toptier or subtier name); FAIN required.
  One row per FAIN: the latest transaction (action_date, then modification_number as a number, then the transaction key).
  Each ZIP's size is checked against the listing; reading each CSV member to its end checks its CRC; the ZIP is deleted.
  usaspending.gov publishes no rate limit, but its CDN refused a laptop for over an hour after ~15 files 2 s apart (2026-11-05):
  30 s before each archive file, one API or listing page per 2 s, a refused file retried twice 10 min apart (~85 min of waits a run);
  each try is one request (no adapter retries for archive files).
  Volume per run (set 20261006): 168 files, about 6.5 GB (USDA 4.1 GB, HHS 2.1 GB, the other six 0.3 GB).
  The archive starts at FY2007: awards whose every transaction is older stay in raw as retained rows (no DELETE).
DOE Office of Science reads POST /api/v2/search/spending_by_award/ (CFDA 81.049, types 02-05), one calendar year at a
  time from 2008 (months when a year reaches the API's 10,000-row cap); a failed page or capped month stops the row.
Columns are frozen in award_schemas.py: USASPENDING_SCHEMA (47 strings: what walden reads plus recipient UEI) and
  USASPENDING_DOE_SC_SCHEMA (the laptop file's 13 strings).
USAspending names no investigator: every row has 0 people, as today.
Schedule (after the first manual run is checked and walden reads the prefixes; the archive set lands mid-month):
  USAspending: `0 21 20 * *`  python -m usaspending_awards   (monthly cron, no --scheduled)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under the 11 prefixes only after
  walden's Awards job no longer reads them (an empty prefix makes the job skip that row and fail).

NIH RePORTER (National Institutes of Health and the HHS agencies it reports for; walden row nih_exporter, priority 3).
Run: python -m nih_awards. No key needed. Weekly: the Scheduler entry is a cron with no --scheduled flag.
NIH_REPORTER_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/nih_reporter/.
The old laptop file (s3://openalex-ingest/awards/nih/nih_projects_combined.parquet, 2026-10-06) stays where it is; nothing reads it after the walden move.
One object, the laptop file's 48 columns (NIH_SCHEMA), so walden's mapping reads both alike and the NIH fold's keys carry over:
  FY1985-2025 from ExPORTER's annual project files, one ZIP at a time, CRC-checked, read with pandas dtype=str as the laptop script read them;
  FY2026 to the current fiscal year from the RePORTER API v2 (projects/search), every project re-read in full each week.
Rows are deduplicated on (full_project_num, subproject_id), newest fiscal year first (API years before ExPORTER years).
API paging: whole-day windows of date_added (00:00:00-23:59:59), halved until each holds at most 15,000 projects (offset cap 14,999),
sorted by appl_id; a moved total, a short or repeated page, or windows that do not add up to the year's total abort.
One request a second (NIH's published limit); User-Agent OpenAlex-NIH-Ingest/1.0. NIH asks big jobs to run at night or weekends ET.
Measured 2026-11-05: FY2026 69,881 projects in 9 windows, 319 s (~160 requests); FY2027 109 so far.
ExPORTER: 41 files, 62 MB for FY2025 (195 MB CSV); every file last changed on or before 2026-07-09 (so the 10-06 laptop pull read the same files).
One FY2025 download ended early with HTTP 200 and no Content-Length (19.5 of 62 MB): the CRC check (zipfile.testzip) rejects such a file.
API records are written in ExPORTER's formats (to_exporter_row): checked against FY2025, where both exist, every column walden reads
matches except where NIH edited the record after the July file. project_terms, ed_inst_type and the two renamed columns (cfda_code,
foa_number) are left empty for API years: walden does not read them.
Schedule (after the first manual run is gated and walden reads the prefix):
  NIH:      `0 6 * * 1`  python -m nih_awards   (Monday 06:00 UTC = 01:00 ET, after RePORTER's Sunday refresh)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/nih_reporter/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip NIH and fail).

FWF (Austrian Science Fund) from the FWF Open API (CC0).
Run: python -m fwf_awards. FWF_OPEN_API_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/fwf_open_api/.
The old OpenAIRE-derived file (s3://openalex-ingest/awards/fwf/fwf_projects.parquet, 2026-01-25) stays where it is; nothing reads it after the walden move.
Bearer key: the one FWF publishes at https://openapi.fwf.ac.at/fwfkey/, read at run time and never logged (no registration, no Heroku config).
One pass over /indexes/projects/documents, 1,000 per page, 1 s apart (21 pages); every page is checked against the listing's total.
The projects index's updatedAt is read before and after: FWF rebuilds the index daily (~03:15 UTC) and its API ids change, so a moved stamp aborts (rerun).
Rows are keyed on the project code (the grant DOI's suffix, P22135), first page wins, written sorted by code.
Measured 2026-11-05: 20,600 projects, 13,834 with a PI ORCID; 22 GETs, 104 s, 51.8 MB parquet (summaries included).
Schedule (after the first manual run is checked and walden reads the prefix):
  FWF:      `0 20 * * 1`  python -m fwf_awards   (weekly, Mondays; no --scheduled: is_due is day-1 for every source but NSF)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/fwf_open_api/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip FWF and fail).

NWO (Dutch Research Council) from NWOpen (https://nwopen-api.nwo.nl/NWOpen-API/api/Projects; CC0 1.0; no key).
Run: python -m nwo_awards. Weekly. No --scheduled: award_fetch.is_due keeps day 1 for every source but NSF, so the
weekly cadence lives in the Scheduler's cron (below), not in the shared core.
NWOPEN_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/nwopen/.
The old laptop file (s3://openalex-ingest/awards/nwo/nwo_projects.parquet, 2026-01-08, 14,599 rows) stays where it is;
nothing reads it after the walden move (walden's scripts/local/nwo_to_s3.py is deleted in the same push).
One pass, 100 projects per page (the API maximum), one page at a time, 0.5 s apart (each page takes ~6 s to serve).
Every page is checked against the listing's meta: count, pages and load date; a moved count or date (NWOpen reloads
daily), a short page, a page of already-seen ids, or a final count short of meta.count aborts.
Projects are deduplicated on project_id, first page wins; more than 0.5% duplicate ids refuses the upload.
The first 16 columns are the laptop file's, parsed the same way (lead = first Main Applicant, else Project leader, else
the first member; ORCID placeholder 'https://orcid.org/-' dropped); added: department, sub_department,
parent_project_id, grant_doi (10.61686/...), award_amount (EUR), members (one per member_id, best role + all roles,
sorted by role rank, family name, given name, member id), pi_member_id and pi_ror.
Measured 2026-11-05 (local harness, no upload): 34,306 projects (34,311 listed, 5 ids twice), 344 pages, 574 s, 65.9 MB parquet, ~270 MB downloaded one page at a time.
Schedule (after the first manual run is checked and walden reads the prefix):
  NWO:      `0 20 * * 1`  python -m nwo_awards   (full fetch Mondays; no --scheduled, see above)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/nwopen/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip nwopen and fail).

Taiwan NSTC (GRB, the Government Research Bulletin). Run: python -m grb_awards. --scheduled runs on UTC day 1 only. No key.
GRB_NSTC_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/grb_nstc/.
The old laptop export (s3://openalex-ingest/awards/nstc_grb/nstc_grb_projects.parquet, 2026-06-03) stays where it is;
walden's grb_nstc_projects mapping still reads its abstracts (GRB's open files have none). MOST stays frozen in walden.
Source: GRB's open-data catalogue (data.gov.tw dataset 18707, Government Open Data License v1):
  https://mas.nstc.gov.tw/OPENDATA/GetFile?format=csv&serialno=527&fileodr=1 lists one file per ROC year, linked through
  tinyurl; the fetcher reads the year list there and downloads https://manager.grb.gov.tw/GRB_{year}_xml.zip directly.
Every run reads ROC 111 (2022, NSTC's first year) to the newest catalogue year, one zip at a time, and keeps the plan organ
  國家科學及技術委員會(本會). Annual rows aggregate to one row per plan number (spaces removed, upper case), as the laptop
  script did: amounts summed (thousand TWD x 1000), first period start / last period end, first non-empty value otherwise.
People: the PI and the researchers (研究人員) as printed, in GRB's order; Chinese names stay whole (walden splits them).
Checks: the catalogue must list every year from 111 without a gap and be at most a year old; each zip must hold exactly
  GRB_<year>.xml (under 512 MiB); every row needs a numeric ID; a past year under 5,000 NSTC rows (ROC 111: 1,000) refuses.
XML is parsed with lxml iterparse, entities off, no network.
Measured 2026-11-05 (local harness, build() only): 5 zips, 34 MB downloaded, 73,454 NSTC annual rows (111: 1,445;
  112: 15,371; 113: 18,280; 114: 19,316; 115: 19,042) -> 56,917 awards, 23.5 MB parquet, 8 s (after download, ~12 s total).
Schedule (after the first manual run is checked and walden reads the prefix):
  GRB NSTC: `0 20 1 * *`  python -m grb_awards --scheduled   (full fetch on day 1)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/grb_nstc/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip the source and fail).

Australian Research Council (ARC): Research Grants Search (dataportal.arc.gov.au/RGS/API), JSON:API. No key needed.
Run: python -m arc_awards. --scheduled runs on UTC day 1 only.
ARC_RGS_AWARDS_S3_PREFIX accepts only s3://openalex-ingest/awards/arc_rgs/.
The old laptop file (s3://openalex-ingest/awards/arc/arc_projects.parquet, 2026-01-19) stays where it is; nothing reads it after the walden move.
Two passes: the list (/grants, 1,000 per page, every page checked against meta total-size/total-pages; a moved total, a short or
repeated page, or a final count short of total-size aborts), then one detail call per grant (/grants/{code}); the API refuses 'Include'.
The first 20 columns are the laptop file's (scripts/local/arc_to_s3.py, retired in the same walden push) in its order and types.
Detail adds project_start_date, people (title, given and family name, role, fellowship flag, ORCID; the lead first) and organisations.
Detail calls start at most 1 per second over 2 connections. A detail call that fails after its retries leaves the grant with
detail_status 'missing' (list columns only); more than 0.5% missing refuses the upload.
Grants are deduplicated on lower(code), first page wins. RGS also lists grants other agencies fund and ARC administers
(grant_funder: Office of National Intelligence, Department of Defence); they are kept, and walden's mapping decides.
Measured 2026-11-05 (local harness, no upload): 34,963 grants listed (35 pages, 314 s); a seeded random 1,500 detail records: 0 failed, 3 retried, ~2.6 KB each, ~1.9 calls/s at the 2/s cap; at 1 call/s a full run is ~35k s (~9.7 h).
Schedule (after the first manual run is checked and walden reads the prefix):
  ARC:      `0 20 1 * *`  python -m arc_awards --scheduled   (full fetch on day 1; ~10 h at 1 detail call per second)
Rollback: delete the Scheduler entry, revert the commit, and delete the dated objects under awards/arc_rgs/
  only after walden's Awards job no longer reads that prefix (an empty prefix makes the job skip ARC and fail).
