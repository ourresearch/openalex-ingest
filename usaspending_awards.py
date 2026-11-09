"""USAspending prime awards (grants and cooperative agreements), monthly, for 11 walden rows. No key needed.

One module, one object per walden row, each under its own prefix s3://openalex-ingest/awards/<name>/ (award_fetch.run
once per row). The old laptop objects (awards/dod/, doe/, nasa/, usda/, epa/, cdc/, usaid/, noaa/, nist/, ahrq/,
doe_sc/) stay where they are; nothing reads them after the walden move.

Ten rows come from the Award Data Archive (files.usaspending.gov/award_data_archive/), the monthly full files per
awarding toptier agency and fiscal year (FY2007 on; the set dated 20261006 was posted 2026-10-14). They port the ten
laptop scripts walden scripts/local/<agency>_to_s3.py (bulk download by fiscal year, prime award types 02-05, one
awarding toptier or subtier agency, deduplicated on award_id_fain keeping the latest action_date):
    run name            walden row          archive  agency filter
    usaspending_dod     usaspending_p26     097      awarding_agency_name     Department of Defense
    usaspending_doe     usaspending_p25     089      awarding_agency_name     Department of Energy
    usaspending_nasa    usaspending_p23     080      awarding_agency_name     National Aeronautics and Space Administration
    usaspending_usda    usaspending_usda    012      awarding_agency_name     Department of Agriculture
    usaspending_epa     usaspending_epa     068      awarding_agency_name     Environmental Protection Agency
    usaspending_usaid   usaspending_usaid   072      awarding_agency_name     Agency for International Development
    usaspending_cdc     usaspending_cdc     075      awarding_sub_agency_name Centers for Disease Control and Prevention
    usaspending_ahrq    usaspending_ahrq    075      awarding_sub_agency_name Agency for Healthcare Research and Quality
    usaspending_noaa    usaspending_noaa    013      awarding_sub_agency_name National Oceanic and Atmospheric Administration
    usaspending_nist    usaspending_nist    013      awarding_sub_agency_name National Institute of Standards and Technology
  Each archive file is downloaded once (its size checked against the bucket listing), read one CSV chunk at a time
  (reading a member to its end checks its CRC), filtered for every row it serves, then deleted. Rows keep the columns
  walden reads plus the recipient's UEI; all strings, read as the laptop scripts read them (pandas dtype=str).
  Latest transaction per FAIN: action_date, then modification_number compared as a number ("10" after "9"; alphanumeric
  values in natural order, after the numeric ones), then the transaction key (the laptops sorted on action_date alone, so
  their same-day ties were arbitrary).
  The newest dated set must cover every fiscal year from FY2007 through the federal fiscal year of its date (the set dated
  20261006 holds FY2007-FY2027) and every year an older set lists; anything less is refused, first run included.
  Archive files are fetched with one attempt per try and no adapter retries: fetch_file() owns every retry and its wait.
  Each object's parquet footer (openalex.funder_total) records the archive files read, their row counts, and how the
  agency's transactions became its awards, for the funder-total gate.
One row comes from the search API, as its laptop script did (scripts/local/doe_sc_to_s3.py):
    usaspending_doe_sc  doe_sc              POST /api/v2/search/spending_by_award/, CFDA 81.049, award types 02-05,
                                            one calendar year at a time (months when a year reaches the 10,000 cap)
  Any failed page, skipped year or capped month stops that row; the laptop skipped years and checked a row floor.
USAspending names no investigator: every row has 0 people, as today.
Run all rows: python -m usaspending_awards. One or more rows: USASPENDING_SOURCES=usaspending_cdc,usaspending_ahrq.
A failed row does not stop the others; the process exits nonzero naming every failed row.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter

from award_fetch import check_disk, http_session, run
from award_schemas import USASPENDING_DOE_SC_SCHEMA, USASPENDING_SCHEMA

ARCHIVE = "https://files.usaspending.gov/award_data_archive/"
SEARCH = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
USER_AGENT = "OpenAlex-USAspending-Ingest/1.0 (contact@openalex.org)"
FIRST_ARCHIVE_FY = 2007          # the archive's first fiscal year
GRANT_TYPES = ("02", "03", "04", "05")   # block, formula, project grants; cooperative agreements
CSV_CHUNK = 50000
REDUCE_AT = 250000                # rows held per row before keeping one per FAIN (USDA's yearly files are 0.4-0.5 GB)
# usaspending.gov publishes no rate limit (usaspending-api repo searched 2026-11-05), but its CDN refused a laptop for over an
# hour after ~15 archive files 2 s apart. Stay well inside that: one API or listing page per 2 s, 30 s before each archive
# file (a full run is ~168 files: ~85 min of waits), and a refused file waits 10 minutes before each of 2 more tries.
REQUEST_DELAY = 2.0              # seconds between API pages and listing pages
FILE_DELAY = 30.0                # seconds before each archive file
FILE_ATTEMPTS = 3
FILE_RETRY_WAIT = 600


@dataclass(frozen=True)
class Agency:
    name: str          # award_fetch.run name: the prefix awards/<name>/
    code: str          # archive toptier code
    column: str        # awarding_agency_name (toptier) or awarding_sub_agency_name (subtier)
    agency: str        # the value the laptop script filtered on


AGENCIES = (
    Agency("usaspending_dod", "097", "awarding_agency_name", "Department of Defense"),
    Agency("usaspending_doe", "089", "awarding_agency_name", "Department of Energy"),
    Agency("usaspending_nasa", "080", "awarding_agency_name", "National Aeronautics and Space Administration"),
    Agency("usaspending_usda", "012", "awarding_agency_name", "Department of Agriculture"),
    Agency("usaspending_epa", "068", "awarding_agency_name", "Environmental Protection Agency"),
    Agency("usaspending_usaid", "072", "awarding_agency_name", "Agency for International Development"),
    Agency("usaspending_cdc", "075", "awarding_sub_agency_name", "Centers for Disease Control and Prevention"),
    Agency("usaspending_ahrq", "075", "awarding_sub_agency_name", "Agency for Healthcare Research and Quality"),
    Agency("usaspending_noaa", "013", "awarding_sub_agency_name", "National Oceanic and Atmospheric Administration"),
    Agency("usaspending_nist", "013", "awarding_sub_agency_name", "National Institute of Standards and Technology"),
)
DOE_SC = "usaspending_doe_sc"
SOURCES = tuple(a.name for a in AGENCIES) + (DOE_SC,)
ORDER = ["action_date", "modification_number", "assistance_transaction_unique_key"]   # latest transaction wins


# ---------- Award Data Archive ----------
def archive_listing(session):
    """(key, size) of every object in the public archive bucket (S3 ListBucket XML, 1,000 keys a page)."""
    keys, marker = [], None
    while True:
        response = session.get(ARCHIVE, params={"marker": marker} if marker else None, timeout=(15, 120))
        response.raise_for_status()
        text = response.text
        page = re.findall(r"<Key>([^<]+)</Key>.*?<Size>(\d+)</Size>", text, re.S)
        if not page and "<ListBucketResult" not in text:
            raise ValueError("USAspending archive: the listing is not an S3 bucket listing")
        keys += [(k, int(s)) for k, s in page]
        if "<IsTruncated>true</IsTruncated>" not in text:
            return keys
        marker = keys[-1][0]
        time.sleep(REQUEST_DELAY)


def fiscal_year(yyyymmdd):
    """The federal fiscal year a date falls in (FY2027 runs 2026-10-01 to 2027-09-30)."""
    return int(yyyymmdd[:4]) + (int(yyyymmdd[4:6]) >= 10)


def archive_files(keys, code):
    """The newest complete set of one agency's full files: [(fiscal year, key, size)], FY2007 through the fiscal year of
    the set's date, one date, and no year an older set lists missing."""
    pattern = re.compile(rf"FY(\d{{4}})_{code}_Assistance_Full_(\d{{8}})(?:_\d+)?\.zip$")
    by_date = {}
    for key, size in keys:
        m = pattern.fullmatch(key)
        if m:
            by_date.setdefault(m.group(2), []).append((int(m.group(1)), key, size))
    if not by_date:
        raise ValueError(f"USAspending archive: no assistance full files for agency {code}")
    newest = max(by_date)
    files = sorted(by_date[newest])
    years = sorted({fy for fy, _, _ in files})
    expected = list(range(FIRST_ARCHIVE_FY, fiscal_year(newest) + 1))
    older = {fy for date, found in by_date.items() if date != newest for fy, _, _ in found}
    if years != expected or older - set(years):
        raise ValueError(f"USAspending archive {code} {newest}: fiscal years {years[0]}-{years[-1]} ({len(years)} files); "
                         f"expected FY{expected[0]}-FY{expected[-1]}" + (f", and an older set lists {sorted(older - set(years))}"
                                                                     if older - set(years) else "") + "; refusing a partial set")
    return newest, files


def archive_session():
    """A session for archive files with no adapter retries: fetch_file() alone decides when to try again."""
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=0))
    session.headers["User-Agent"] = USER_AGENT
    return session


def download_once(session, url, path, expected_size):
    """One GET of one archive file (award_fetch.download without its own 3 tries), its size checked; a partial file is
    removed before the error goes up."""
    try:
        with session.get(url, stream=True, timeout=(15, 120)) as response:
            response.raise_for_status()
            if shutil.disk_usage(path.parent).free < expected_size + 1024 ** 3:
                raise OSError("Less than 1 GiB reserve after this download")
            with path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    handle.write(chunk)
                    if shutil.disk_usage(path.parent).free < 512 * 1024 ** 2:
                        raise OSError("Disk reserve below 512 MiB")
        if path.stat().st_size != expected_size:
            raise ValueError("Incomplete download (size mismatch)")
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def fetch_file(session, url, path, size):
    """One archive file, its size checked; retried after the archive refuses or drops the connection (seen 2026-11-05
    after a burst of quick downloads), FILE_ATTEMPTS times, FILE_RETRY_WAIT seconds apart, FILE_DELAY seconds between files.
    Every try is one request (download_once, archive_session): no nested retry can fire inside the wait."""
    time.sleep(FILE_DELAY)
    for attempt in range(FILE_ATTEMPTS):
        try:
            return download_once(session, url, path, size)
        except (requests.RequestException, ValueError):
            if attempt == FILE_ATTEMPTS - 1:
                raise RuntimeError(f"USAspending archive: {url} failed after {FILE_ATTEMPTS} attempts") from None
            time.sleep(FILE_RETRY_WAIT)


def read_archive(path, wanted):
    """Chunks of one archive ZIP (every CSV member, to its end, so each CRC is checked), only WANTED columns."""
    import zipfile
    with zipfile.ZipFile(path) as archive:
        members = sorted(m for m in archive.namelist() if m.lower().endswith(".csv"))
        if not members:
            raise ValueError(f"USAspending archive {path.name}: no CSV inside")
        for member in members:
            with archive.open(member) as handle:
                header = handle.readline().decode("utf-8-sig").rstrip("\r\n").split(",")
                missing = set(wanted) - set(header)
                if missing:
                    raise ValueError(f"USAspending archive {member}: columns missing {sorted(missing)}")
            with archive.open(member) as handle:
                yield from pd.read_csv(handle, dtype=str, usecols=list(wanted), chunksize=CSV_CHUNK)


def modification_key(value):
    """A sortable form of modification_number: digit runs compare as numbers ("10" after "9", "A10" after "A9"), text runs
    by lower-case text, a purely numeric value before any value that starts with text; missing -> None (sorts oldest)."""
    if not isinstance(value, str) or not value.strip():
        return None
    parts = re.findall(r"\d+|\D+", value.strip().lower())
    return "\x01".join(f"0{int(p):030d}" if p.isdecimal() else f"1{p}" for p in parts)   # isdecimal is what \d matched ("²" stays text)


def latest_per_fain(frame):
    """One row per award_id_fain: the latest transaction (ORDER, descending; nulls last), modification_number by
    modification_key."""
    frame = frame.assign(_modification=frame["modification_number"].map(modification_key))
    frame = frame.sort_values(["action_date", "_modification", "assistance_transaction_unique_key"], ascending=False,
                              na_position="last", kind="mergesort")
    return frame.drop_duplicates(subset=["award_id_fain"], keep="first").drop(columns="_modification")


def agency_rows(chunk, agency):
    """(the agency's grant transactions with a FAIN, how many it has without one)."""
    keep = chunk["assistance_type_code"].isin(GRANT_TYPES) & (chunk[agency.column] == agency.agency)
    has_fain = chunk["award_id_fain"].fillna("").str.strip() != ""
    return chunk[keep & has_fain], int((keep & ~has_fain).sum())


def prepare_archive(session, keys, agencies, directory):
    """Download each fiscal year of one toptier agency once and build every row it serves: {name: parquet path}."""
    code = agencies[0].code
    newest, files = archive_files(keys, code)
    wanted = [c for c in USASPENDING_SCHEMA.names if c not in ("ingested_at", "archive_file")]
    found = {a.name: [] for a in agencies}
    # the funder-total tally, kept apart from the dedupe: every matching transaction, and the FAINs seen
    tally = {a.name: {"with_fain": 0, "no_fain": 0, "fains": set()} for a in agencies}
    file_rows = []
    for fy, key, size in reversed(files):
        check_disk(directory)
        path = directory / f"archive_{code}_{fy}.zip"
        fetch_file(session, ARCHIVE + key, path, size)
        n = 0
        for chunk in read_archive(path, wanted):
            n += len(chunk)
            chunk["archive_file"] = key
            for agency in agencies:
                rows, no_fain = agency_rows(chunk, agency)
                tally[agency.name]["no_fain"] += no_fain
                tally[agency.name]["with_fain"] += len(rows)
                tally[agency.name]["fains"].update(rows["award_id_fain"])
                if len(rows):
                    found[agency.name].append(rows)
                    if sum(len(f) for f in found[agency.name]) > REDUCE_AT:   # memory: one row per FAIN so far
                        found[agency.name] = [latest_per_fain(pd.concat(found[agency.name], ignore_index=True))]
        path.unlink()
        file_rows.append([key, n])
        for agency in agencies:
            if found[agency.name]:
                found[agency.name] = [latest_per_fain(pd.concat(found[agency.name], ignore_index=True))]
        print(f"USAspending {code} FY{fy}: {key} read", flush=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out = {}
    for agency in agencies:
        if not found[agency.name]:
            raise ValueError(f"{agency.name}: no awards in archive {code} {newest}; refusing an empty object")
        frame = found[agency.name][0].sort_values("award_id_fain", kind="mergesort").reset_index(drop=True)
        frame["ingested_at"] = stamp
        path = directory / f"{agency.name}.parquet"
        t = tally[agency.name]
        total = {"measure": f"grant transactions (types 02-05) of {agency.agency} in the Award Data Archive's {code} files "
                            f"dated {newest}, FY{files[0][0]}-FY{files[-1][0]}",
                 "value": t["with_fain"] + t["no_fain"],
                 "folded": {"earlier transactions of an award (one row per FAIN, the latest kept)": t["with_fain"] - len(t["fains"])},
                 "excluded": {"transactions without a FAIN": t["no_fain"]},
                 "archive_files": len(file_rows), "archive_rows": sum(r for _, r in file_rows), "files": sorted(file_rows)}
        schema = USASPENDING_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(total)})   # the gate reads it
        pq.write_table(pa.Table.from_pandas(frame[USASPENDING_SCHEMA.names], schema=schema, preserve_index=False), path)
        out[agency.name] = path
        print(f"{agency.name}: {len(frame)} awards from archive {code} {newest}", flush=True)
    return out


# ---------- DOE Office of Science (search API, as doe_sc_to_s3.py) ----------
DOE_SC_FIELDS = ["Award ID", "Recipient Name", "Award Amount", "Start Date", "End Date", "Awarding Agency",
                 "Awarding Sub Agency", "Description", "Award Type", "prime_award_recipient_id"]
DOE_SC_PAGE = 100
DOE_SC_MAX_PAGES = 100           # 100 x 100 = the API's 10,000 deep-paging cap
DOE_SC_FIRST_YEAR = 2008


def _s(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text.lower() != "none" else None


def _whole_dollars(value):
    """Award Amount -> signed whole-dollar string, cents truncated (doe_sc_to_s3._amount)."""
    if value is None:
        return None
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return None


def doe_sc_row(record):
    return {"funder_award_id": _s(record.get("Award ID")), "title": _s(record.get("Description")), "pi_full": None,
            "pi_given": None, "pi_family": None, "institution": _s(record.get("Recipient Name")),
            "amount": _whole_dollars(record.get("Award Amount")), "currency": "USD", "scheme": _s(record.get("Award Type")),
            "start_date_raw": _s(record.get("Start Date")), "end_date_raw": _s(record.get("End Date")),
            "description": _s(record.get("Description")), "landing_page_url": None}


def doe_sc_slice(session, start, end):
    """Every award whose period overlaps [start, end] (no date_type: the laptop's semantics). None when capped."""
    rows, page = [], 1
    while True:
        body = {"filters": {"award_type_codes": list(GRANT_TYPES), "time_period": [{"start_date": start, "end_date": end}],
                            "program_numbers": ["81.049"]},
                "fields": DOE_SC_FIELDS, "limit": DOE_SC_PAGE, "page": page, "sort": "Award Amount", "order": "desc"}
        for attempt in range(5):
            try:
                response = session.post(SEARCH, json=body, timeout=(15, 120))
                response.raise_for_status()
                data = response.json()
                break
            except (requests.RequestException, ValueError):
                if attempt == 4:
                    raise RuntimeError(f"{DOE_SC}: page {page} of {start}..{end} failed after 5 attempts") from None
                time.sleep(min(60, 2 ** (attempt + 1)))
        results = data.get("results") or []
        rows += results
        more = bool((data.get("page_metadata") or {}).get("hasNext"))
        if not more or not results:
            return rows
        if page >= DOE_SC_MAX_PAGES:
            return None
        page += 1
        time.sleep(REQUEST_DELAY)


def build_doe_sc(directory, today=None):
    today = today or datetime.now(timezone.utc).date()
    output = directory / f"{DOE_SC}.parquet"
    rows = []
    with http_session() as session:
        session.headers["User-Agent"] = USER_AGENT
        for year in range(DOE_SC_FIRST_YEAR, today.year + 1):
            got = doe_sc_slice(session, f"{year}-01-01", f"{year}-12-31")
            if got is None:                            # the year reached the cap: one month at a time
                got = []
                for month in range(1, 13):
                    end = (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(1)).strftime("%Y-%m-%d")
                    part = doe_sc_slice(session, f"{year}-{month:02d}-01", end)
                    if part is None:
                        raise ValueError(f"{DOE_SC}: {year}-{month:02d} alone reaches the 10,000 cap")
                    got += part
            rows += got
            print(f"{DOE_SC} {year}: {len(got)} rows", flush=True)
    frame = pd.DataFrame([doe_sc_row(r) for r in rows], columns=USASPENDING_DOE_SC_SCHEMA.names)
    seen = {r["funder_award_id"] for r in frame.to_dict("records")}   # the tally, apart from the dedupe
    # Dedup by FAIN, first occurrence (amount-descending within each year, years in order), as the laptop script did.
    frame = frame.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    total = {"measure": f"results of the search API (CFDA 81.049, types 02-05, {DOE_SC_FIRST_YEAR}-{today.year} slices)",
             "value": len(rows), "folded": {"an award listed again in a later slice (the first kept)": len(rows) - len(seen)}}
    schema = USASPENDING_DOE_SC_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(total)})   # the gate reads it
    pq.write_table(pa.Table.from_pandas(frame.astype("string"), schema=schema, preserve_index=False), output)
    print(f"{DOE_SC}: {len(frame)} awards", flush=True)
    return output


# ---------- entry point ----------
def selected(environ=os.environ):
    names = [n.strip() for n in environ.get("USASPENDING_SOURCES", "").split(",") if n.strip()] or list(SOURCES)
    unknown = sorted(set(names) - set(SOURCES))
    if unknown:
        raise SystemExit(f"USASPENDING_SOURCES: unknown {unknown}; known {list(SOURCES)}")
    return names


def main():
    names = selected()
    failed = []
    with tempfile.TemporaryDirectory(prefix="awards-usaspending-") as shared:
        shared = Path(shared)
        groups = {}
        for agency in AGENCIES:
            if agency.name in names:
                groups.setdefault(agency.code, []).append(agency)
        keys = None
        for code, agencies in groups.items():
            try:
                if keys is None:
                    with http_session() as session:
                        session.headers["User-Agent"] = USER_AGENT
                        keys = archive_listing(session)
                with archive_session() as session:
                    built = prepare_archive(session, keys, agencies, shared)
            except Exception as error:                 # one agency's archive failing does not stop the others
                print(f"USAspending archive {code}: {type(error).__name__}: {error}", flush=True)
                failed += [a.name for a in agencies]
                continue
            for agency in agencies:
                try:
                    run(agency.name, lambda _workdir, path=built[agency.name]: path, USASPENDING_SCHEMA)
                except Exception as error:
                    print(f"{agency.name}: {type(error).__name__}: {error}", flush=True)
                    failed.append(agency.name)
                built[agency.name].unlink(missing_ok=True)
        if DOE_SC in names:
            try:
                run(DOE_SC, build_doe_sc, USASPENDING_DOE_SC_SCHEMA)
            except Exception as error:
                print(f"{DOE_SC}: {type(error).__name__}: {error}", flush=True)
                failed.append(DOE_SC)
    if failed:
        raise SystemExit(f"USAspending: failed rows {failed}; the others were uploaded")


if __name__ == "__main__":
    sys.exit(main())
