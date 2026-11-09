"""NIH RePORTER projects, weekly (Monday, after RePORTER's Sunday-night refresh). No key needed.

One object with the 48 columns of the laptop file it replaces (walden scripts/local/nih_exporter_to_s3.py; the saved
object s3://openalex-ingest/awards/nih/nih_projects_combined.parquet, 2026-10-06), so walden's NIH mapping reads both
the same way and today's award keys carry over:
  FY1985-2025  ExPORTER annual project files (reporter.nih.gov/exporter/projects/download/<FY>), one ZIP at a time,
               read as the laptop script read them: every column a string (pandas dtype=str, default NA values),
               names lower-cased with spaces and hyphens as underscores, sub-project ids stripped.
  FY2026 on    the RePORTER API v2 (api.reporter.nih.gov/v2/projects/search): every project of each fiscal year from
               API_FIRST_FY to the current one, re-read in full every week, so nothing depends on an earlier run.
               ExPORTER publishes no file for these years (FY2026: HTTP 404 on 2026-11-05). Each API record is written
               in ExPORTER's column formats (to_exporter_row; checked against ExPORTER FY2025, where both exist).
Rows are deduplicated on (full_project_num, subproject_id), newest fiscal year first, as the laptop file was: an API
year wins over an ExPORTER year, and within one file the first row wins.
API paging: windows of whole days of date_added (00:00:00-23:59:59: the API reads a bare to_date as its midnight),
halved until each holds at most 15,000 projects (the API's offset cap is 14,999, 500 a page), sorted by appl_id; every page must repeat its window's total, pages may not be short or repeat an
appl_id, and the windows must add up to the fiscal year's total. One request a second (NIH's limit); NIH asks big
jobs to run at night or weekends ET, so the Scheduler entry runs Monday 06:00 UTC.
A ZIP must pass its CRC check before any row is used: one 2026-11-05 download ended early with HTTP 200 and no
Content-Length (19.5 of 62 MB).
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
import time
import zipfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from award_fetch import check_disk, download, http_session, run
from award_schemas import NIH_SCHEMA

EXPORTER_URL = "https://reporter.nih.gov/exporter/projects/download/{fy}"
FIRST_FY = 1985
LAST_EXPORTER_FY = 2025          # the last year read from ExPORTER; later years always come from the API
API_FIRST_FY = LAST_EXPORTER_FY + 1
API = "https://api.reporter.nih.gov/v2/projects/search"
PAGE_SIZE = 500                  # the API maximum
WINDOW_MAX = 15000               # offset cap 14,999 + one page of 500 = 15,000 rows reachable per query
REQUEST_DELAY = 1.0              # seconds between requests (NIH: no more than one a second)
CSV_CHUNK = 20000                # ExPORTER rows per pandas chunk: memory stays flat on a small dyno
USER_AGENT = "OpenAlex-NIH-Ingest/1.0 (contact@openalex.org)"
INCLUDE_FIELDS = [
    "ApplId", "SubprojectId", "FiscalYear", "ProjectNum", "CoreProjectNum", "ProjectNumSplit", "Organization",
    "OrganizationType", "AwardType", "ActivityCode", "AwardAmount", "PrincipalInvestigators", "ProgramOfficers",
    "AgencyIcAdmin", "AgencyIcFundings", "CongDist", "SpendingCategoriesDesc", "ProjectStartDate", "ProjectEndDate",
    "OpportunityNumber", "FullStudySection", "AwardNoticeDate", "CfdaCode", "DirectCostAmt", "IndirectCostAmt",
    "BudgetStart", "BudgetEnd", "ArraFunded", "FundingMechanism", "PhrText", "ProjectTitle", "DateAdded",
]
EARLIEST_ADDED = date(2000, 1, 1)
GRANT_NUMBER = re.compile(r"^[1-9][A-Z][A-Z0-9]{2}[A-Z]{2}[0-9]{6}")   # type + activity + institute + serial


def current_fy(today):
    """NIH's fiscal year: FY2026 runs 2025-10-01 to 2026-09-30."""
    return today.year + 1 if today.month >= 10 else today.year


def _blank(value):
    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


def _upper(value):
    text = _blank(value)
    return text.upper() if text else None


def _lines(value):
    """ExPORTER writes each line break as one space (FY2025: 2,049 of 2,083 PHRs then equal; the rest were edited)."""
    text = _blank(value)
    return re.sub(r"\r\n|\n|\r", " ", text) if text else None


def _day(value):
    return value[:10] if isinstance(value, str) and value.strip() else None


def _number(value):
    """API amounts as ExPORTER writes them: whole dollars without a decimal point."""
    if value is None or value == "":
        return None
    number = float(value)
    return str(int(number)) if number == int(number) else repr(number)


def _plain_int(value):
    """'02' -> '2'; a serial such as 'DK141969' -> '141969' (ExPORTER drops leading zeros and the institute)."""
    text = _blank(value)
    if text is None:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return str(int(digits)) if digits else text


def pi_fields(principal_investigators):
    """ExPORTER's PI_NAMEs and PI_IDS: 'LAST, FIRST MIDDLE' upper-case, ' (contact)' after the contact PI, joined with
    ';' in ExPORTER's order (by name), ids in the same order. None when the API lists nobody."""
    people = []
    for p in principal_investigators or []:
        last = (p.get("last_name") or "").upper()
        first = (p.get("first_name") or "").upper()
        middle = (p.get("middle_name") or "").upper()
        name = f"{last}, {first} {middle}"
        contact = " (contact)" if p.get("is_contact_pi") else ""
        pid = p.get("profile_id")
        people.append((name, f"{name}{contact}", f"{pid}{contact}" if pid is not None else None))
    if not people:
        return None, None
    people.sort(key=lambda x: x[0])
    ids = [x[2] for x in people]
    return ";".join(x[1] for x in people), (";".join(ids) if all(i is not None for i in ids) else None)


def to_exporter_row(record):
    """One RePORTER API project -> the 48 ExPORTER columns (strings, None for empty), as the laptop file holds them."""
    org = record.get("organization") or {}
    split = record.get("project_num_split") or {}
    admin = record.get("agency_ic_admin") or {}
    section = record.get("full_study_section") or {}
    pi_names, pi_ids = pi_fields(record.get("principal_investigators"))
    officers = [f"{(o.get('last_name') or '').upper()}, {(o.get('first_name') or '').upper()} {(o.get('middle_name') or '').upper()}"
                for o in record.get("program_officers") or []]
    subproject = _blank(record.get("subproject_id"))
    subproject = subproject.strip() if subproject else None
    amount = _number(record.get("award_amount"))
    district = record.get("cong_dist") or ""
    duns = org.get("org_duns") or []
    number = _blank(record.get("project_num"))
    # Contracts (75N..., 272..., AAI...): ExPORTER's core is the number before the first '-'; the API's is cut short.
    core = _blank(record.get("core_project_num")) if number and GRANT_NUMBER.match(number) else (number and number.split("-")[0])
    fundings = sorted((f.get("abbreviation"), _number(f.get("total_cost"))) for f in record.get("agency_ic_fundings") or []
                      if f.get("abbreviation") and f.get("total_cost") is not None and f.get("fy") in (None, record.get("fiscal_year")))
    fips = _blank(org.get("org_fips"))
    row = {
        "application_id": _blank(record.get("appl_id")),
        "activity": _blank(record.get("activity_code")),
        "administering_ic": _blank(admin.get("code")),
        "application_type": _blank(split.get("appl_type_code")),
        "arra_funded": _blank(record.get("arra_funded")),
        "award_notice_date": _day(record.get("award_notice_date")),
        "budget_start": _day(record.get("budget_start")),
        "budget_end": _day(record.get("budget_end")),
        "cfda_code": None,                       # ExPORTER renamed it ASSISTANCE_LISTING_NUMBER (FY2025 file)
        "core_project_num": core,
        "ed_inst_type": None,                    # not in the API's fields; walden does not read it
        "foa_number": None,                      # renamed OPPORTUNITY NUMBER
        "full_project_num": number,
        "subproject_id": subproject,
        "funding_ics": "".join(f"{abbr}:{cost}\\" for abbr, cost in fundings) or None,   # 'NEI:450000\\NINDS:23582\\'
        "fy": int(record["fiscal_year"]),
        "ic_name": _upper(admin.get("name")),
        "nih_spending_cats": _blank(record.get("spending_categories_desc")) and record["spending_categories_desc"].replace("; ", ";"),
        "org_city": _blank(org.get("org_city")),
        "org_country": _blank(org.get("org_country")) if fips else None,   # ExPORTER: none for NIH's own (intramural) orgs
        "org_dept": _blank(org.get("dept_type")),
        "org_district": district.rsplit("-", 1)[-1].zfill(2) if district else None,
        "org_duns": ";".join(duns) if duns else None,
        "org_fips": fips,
        "org_name": _blank(org.get("org_name")) and org["org_name"].strip(),
        "org_state": _blank(org.get("org_state")),
        "org_zipcode": _blank(org.get("org_zipcode")),
        "phr": _lines(record.get("phr_text")),
        "pi_ids": pi_ids,
        "pi_names": pi_names,
        "program_officer_name": ";".join(officers) if officers else None,
        "project_start": _day(record.get("project_start_date")),
        "project_end": _day(record.get("project_end_date")),
        "project_terms": None,                   # not fetched: walden does not read it, and it is most of each record
        "project_title": _blank(record.get("project_title")),
        "serial_number": _plain_int(split.get("serial_num")),
        "study_section": _blank(section.get("srg_code")),
        "study_section_name": _blank(section.get("name")),
        "suffix": _blank(split.get("suffix_code")),
        "support_year": _plain_int(split.get("support_year")),
        "total_cost": None if subproject else amount,
        "total_cost_sub_project": amount if subproject else None,
        "opportunity_number": _blank(record.get("opportunity_number")),
        "funding_mechanism": _upper(record.get("funding_mechanism")),
        "org_ipf_code": _blank(org.get("org_ipf_code")),
        "direct_cost_amt": _number(record.get("direct_cost_amt")),
        "indirect_cost_amt": _number(record.get("indirect_cost_amt")),
        "assistance_listing_number": _blank(record.get("cfda_code")),
    }
    return {name: row[name] for name in NIH_SCHEMA.names}


class Api:
    """RePORTER API v2 projects/search, one request a second, every page checked against its window's total."""

    def __init__(self, session, today):
        self.session, self.today, self.last = session, today, 0.0

    def post(self, body):
        for attempt in range(4):
            wait = self.last + REQUEST_DELAY - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()
            try:
                response = self.session.post(API, json=body, timeout=(15, 120))
                if response.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {response.status_code}")
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError):
                if attempt == 3:
                    raise RuntimeError(f"RePORTER API failed after 4 attempts: {body['criteria']}") from None
                time.sleep(30 * (attempt + 1))

    def query(self, fy, start, end, offset, limit):
        # The API compares date_added as a timestamp: a bare to_date means its midnight, so a day is 00:00:00-23:59:59.
        criteria = {"fiscal_years": [fy], "date_added": {"from_date": f"{start.isoformat()}T00:00:00",
                                                          "to_date": f"{end.isoformat()}T23:59:59"}}
        return self.post({"criteria": criteria, "offset": offset, "limit": limit, "sort_field": "appl_id",
                          "sort_order": "asc", "include_fields": INCLUDE_FIELDS})

    def total(self, fy, start, end):
        meta = self.query(fy, start, end, 0, 1).get("meta") or {}
        if "total" not in meta:
            raise ValueError(f"RePORTER FY{fy} {start}..{end}: no total in the response")
        return int(meta["total"])

    def windows(self, fy):
        """(start, end, total) windows of date_added that cover the fiscal year, each at most WINDOW_MAX projects."""
        whole = (EARLIEST_ADDED, self.today + timedelta(days=1))
        expected = self.total(fy, *whole)
        todo, leaves = [(*whole, expected)], []
        while todo:
            start, end, n = todo.pop()
            if n <= WINDOW_MAX:
                if n:
                    leaves.append((start, end, n))
                continue
            if start == end:
                raise ValueError(f"RePORTER FY{fy}: {n} projects added on {start} alone; more than one query can page")
            middle = start + (end - start) // 2
            todo += [(middle + timedelta(days=1), end, self.total(fy, middle + timedelta(days=1), end)),
                     (start, middle, self.total(fy, start, middle))]
        if sum(n for *_, n in leaves) != expected:
            raise ValueError(f"RePORTER FY{fy}: windows hold {sum(n for *_, n in leaves)} projects, the year {expected}")
        return sorted(leaves), expected

    def fiscal_year(self, fy):
        """Every project of one fiscal year, as API records."""
        leaves, expected = self.windows(fy)
        records, seen = [], set()
        for start, end, n in leaves:
            for offset in range(0, n, PAGE_SIZE):
                body = self.query(fy, start, end, offset, PAGE_SIZE)
                total = (body.get("meta") or {}).get("total")
                page = body.get("results") or []
                if total != n:
                    raise ValueError(f"RePORTER FY{fy} {start}..{end}: total moved from {n} to {total}; rerun")
                ids = [r.get("appl_id") for r in page]
                if len(page) != min(PAGE_SIZE, n - offset) or None in ids or len(set(ids)) != len(ids) or seen & set(ids):
                    raise ValueError(f"RePORTER FY{fy} {start}..{end}: page at {offset} short or repeated; rerun")
                if any(r.get("fiscal_year") != fy for r in page):
                    raise ValueError(f"RePORTER FY{fy}: a record of another fiscal year in the results")
                seen.update(ids)
                records += page
        if len(records) != expected:
            raise ValueError(f"RePORTER FY{fy}: {len(records)} records for a total of {expected}")
        print(f"NIH API FY{fy}: {expected} projects in {len(leaves)} windows", flush=True)
        return records


def key_hashes(frame):
    """64-bit hashes of (full_project_num, stripped subproject_id): the laptop file's dedupe key."""
    values = (frame["full_project_num"].fillna("\x00") + "\x1f" + frame["subproject_id"].fillna("")).tolist()
    return np.fromiter((int.from_bytes(hashlib.blake2b(v.encode(), digest_size=8).digest(), "little", signed=True)
                        for v in values), dtype=np.int64, count=len(values))


class Writer:
    """Writes rows newest year first, skipping any (full_project_num, subproject_id) already written."""

    def __init__(self, path):
        self.writer = pq.ParquetWriter(path, NIH_SCHEMA)
        self.seen = np.empty(0, dtype=np.int64)
        self.rows = self.dropped = 0

    def write(self, frame):
        hashes = key_hashes(frame)
        _, first = np.unique(hashes, return_index=True)
        keep = np.zeros(len(frame), dtype=bool)
        keep[first] = True                                   # first row of each key within this chunk
        keep &= ~np.isin(hashes, self.seen)                  # and not written by a newer year or an earlier chunk
        self.dropped += int(len(frame) - keep.sum())
        frame = frame[keep]
        self.seen = np.union1d(self.seen, hashes[keep])
        if len(frame):
            self.writer.write_table(pa.Table.from_pandas(frame, schema=NIH_SCHEMA, preserve_index=False))
            self.rows += len(frame)

    def close(self):
        self.writer.close()


def exporter_frames(path, fy):
    """One ExPORTER year file -> chunks with the laptop file's columns and parsing. The CRC is checked first."""
    with zipfile.ZipFile(path) as archive:
        if archive.testzip() is not None:
            raise ValueError(f"ExPORTER FY{fy}: corrupt ZIP member; rerun")
        members = [m for m in archive.namelist() if m.upper().endswith(".CSV")]
        if len(members) != 1:
            raise ValueError(f"ExPORTER FY{fy}: expected one CSV in the ZIP, found {members}")
        with archive.open(members[0]) as handle:
            for chunk in pd.read_csv(handle, dtype=str, chunksize=CSV_CHUNK):
                chunk.columns = chunk.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")
                unknown = set(chunk.columns) - set(NIH_SCHEMA.names)
                if unknown:
                    raise ValueError(f"ExPORTER FY{fy}: new columns {sorted(unknown)}; add them to NIH_SCHEMA for review")
                fys = pd.to_numeric(chunk["fy"], errors="coerce")
                if fys.isna().any():
                    raise ValueError(f"ExPORTER FY{fy}: rows without a fiscal year")
                chunk["fy"] = fys.astype("int64")
                stripped = chunk["subproject_id"].fillna("").str.strip()
                chunk["subproject_id"] = stripped.mask(stripped == "")
                for name in NIH_SCHEMA.names:
                    if name not in chunk:
                        chunk[name] = None
                yield chunk[NIH_SCHEMA.names]


def build(directory, today=None, first_fy=FIRST_FY):
    today = today or datetime.now(timezone.utc).date()
    output = directory / "nih_reporter.parquet"
    writer = Writer(output)
    listed = {}   # NIH's own count per fiscal year: RePORTER's meta.total, or the ExPORTER file's rows (funder-total gate)
    try:
        with http_session() as session:
            session.headers["User-Agent"] = USER_AGENT
            api = Api(session, today)
            for fy in range(current_fy(today), API_FIRST_FY - 1, -1):
                rows = [to_exporter_row(r) for r in api.fiscal_year(fy)]
                listed[f"FY{fy} RePORTER API"] = len(rows)   # fiscal_year() refuses unless it equals meta.total
                writer.write(pd.DataFrame(rows, columns=NIH_SCHEMA.names).astype({"fy": "int64"}))
            for fy in range(LAST_EXPORTER_FY, first_fy - 1, -1):
                check_disk(directory)
                archive = directory / f"exporter_{fy}.zip"
                download(session, EXPORTER_URL.format(fy=fy), archive)
                before, read = writer.rows, 0
                for frame in exporter_frames(archive, fy):
                    read += len(frame)
                    writer.write(frame)
                listed[f"FY{fy} ExPORTER file"] = read
                archive.unlink()
                if writer.rows == before:
                    raise ValueError(f"ExPORTER FY{fy}: no rows; refusing an incomplete history")
                print(f"NIH ExPORTER FY{fy}: {writer.rows - before} rows", flush=True)
        writer.writer.add_key_value_metadata({"openalex.funder_total": json.dumps({
            "measure": "NIH's records per fiscal year (RePORTER API meta.total; ExPORTER file rows)", "value": sum(listed.values()),
            "folded": {"an application (full_project_num, subproject_id) already written from a newer year": writer.dropped},
            "by_year": listed})})
    finally:
        writer.close()
    print(f"NIH: {writer.rows} rows written, {writer.dropped} older duplicates dropped", flush=True)
    return output


def main():
    run("nih_reporter", build, NIH_SCHEMA)


if __name__ == "__main__":
    main()
