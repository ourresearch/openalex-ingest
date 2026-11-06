"""Research Council of Norway (Forskningsrådet) projects from Prosjektbanken, monthly. No key needed.

Prosjektbanken (prosjektbanken.forskningsradet.no) is RCN's own public project database. Its explore page is a Next.js
app whose data route returns the project list as JSON:
  1. GET /en/explore/projects            -> the page's __NEXT_DATA__ names the current buildId
  2. GET /_next/data/<buildId>/en/explore/projects.json?Kilde=FORISS&...&resultCount=500&offset=N, until totalHits
Only Kilde=FORISS, RCN's own grants (not the EU or SkatteFUNN views). walden's laptop loader
(scripts/local/research_council_norway_to_s3.py, origin e22fdf7f) is ported unchanged: the same 28 string columns
(alphabetical, as the saved file), the same funder_award_id (rcn-<source>-<id>), the same name split and year bounds,
so walden's sources/research_council_norway_project_bank.sql reads it as it read the saved file.
Fails closed on: no buildId, an empty page before totalHits, totalHits moving during the pull, a project id repeated
while paging (a repeated or shifted page), distinct projects != totalHits, or a duplicate award id.
The funder's own total (totalHits) is recorded in the parquet footer (key openalex.funder_total) for the gate.
"""
import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import RCN_SCHEMA

BASE_URL = "https://prosjektbanken.forskningsradet.no"
EXPLORE_URL = f"{BASE_URL}/en/explore/projects"
PAGE_SIZE = 500
PAGE_DELAY = 0.5           # seconds between pages (≤ 2 requests a second)
FILTER_PARAMS = {
    "view": "projects",
    "Kilde": "FORISS",
    "distribution": "Ar",
    "chart": "bar",
    "calcType": "funding",
    "Sprak": "en",
    "sortBy": "date",
    "sortOrder": "desc",
}
NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', flags=re.DOTALL)
SUFFIX_TOKENS = {
    "phd", "ph.d.", "md", "m.d.", "dphil", "dr.", "dr", "prof.", "prof",
    "jr.", "sr.", "jr", "sr", "ii", "iii", "iv",
}


# --- the laptop loader's normalisers, unchanged ---------------------------------------------------------------------
def clean_text(value):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def json_string(value):
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def split_name(name):
    if not name:
        return None, None
    tokens = re.split(r"\s+", name.strip())
    while tokens and tokens[0].lower().strip(",") in SUFFIX_TOKENS:
        tokens.pop(0)
    while tokens and tokens[-1].lower().strip(",") in SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def year_bounds(years):
    if not isinstance(years, list):
        return None, None, None, None
    parsed = []
    for value in years:
        try:
            year = int(value)
        except (TypeError, ValueError):
            continue
        if 1800 <= year <= 2100:
            parsed.append(year)
    if not parsed:
        return None, None, None, None
    start_year, end_year = min(parsed), max(parsed)
    return str(start_year), str(end_year), f"{start_year:04d}-01-01", f"{end_year:04d}-12-31"


def primary_organization(organizations):
    if not isinstance(organizations, list) or not organizations:
        return None
    hierarchy = organizations[0]
    if not isinstance(hierarchy, list) or not hierarchy:
        return None
    if len(hierarchy) >= 3 and clean_text(hierarchy[2]):
        return clean_text(hierarchy[2])
    return clean_text(hierarchy[-1])


def project_url(source, project_id):
    return f"{BASE_URL}/en/project/{clean_text(source) or 'FORISS'}/{project_id}"


def flatten_project(project, downloaded_at):
    """One Prosjektbanken project -> the saved file's 28 columns (research_council_norway_to_s3.flatten_project)."""
    project_id = str(project.get("id") or "").strip()
    if not project_id:
        raise ValueError("Prosjektbanken project without an id")
    source = clean_text(project.get("source")) or "FORISS"
    title = clean_text(project.get("title"))
    if not title:
        raise ValueError(f"Prosjektbanken project {source}:{project_id} has no title")
    years = project.get("yearsActive")
    start_year, end_year, start_date, end_date = year_bounds(years)
    duration = project.get("duration") if isinstance(project.get("duration"), dict) else {}
    if duration:
        start_year = start_year or clean_text(duration.get("startYear"))
        end_year = end_year or clean_text(duration.get("endYear"))
        if start_year and not start_date:
            start_date = f"{int(start_year):04d}-01-01"
        if end_year and not end_date:
            end_date = f"{int(end_year):04d}-12-31"
    amount = project.get("totalFunding")
    amount_text = clean_text(amount) if amount not in (None, "") else None
    lead_name = clean_text(project.get("leadName"))
    given_name, family_name = split_name(lead_name)
    current = project.get("currentActivity") if isinstance(project.get("currentActivity"), dict) else {}
    return {
        "source_record_id": f"{source}:{project_id}",
        "project_id": project_id,
        "source": source,
        "funder_award_id": f"rcn-{source.lower()}-{project_id.lower()}",
        "display_name": title,
        "teaser": clean_text(project.get("teaser")),
        "pop_sci_description": clean_text(project.get("popSciDescription")),
        "project_summary": clean_text(project.get("projectSummary")),
        "lead_name": lead_name,
        "lead_given_name": given_name,
        "lead_family_name": family_name,
        "primary_organization": primary_organization(project.get("organisations")),
        "organizations_json": json_string(project.get("organisations")),
        "geographies_json": json_string(project.get("geographies")),
        "disciplines_json": json_string(project.get("disciplines")),
        "current_activity_code": clean_text(current.get("activity")),
        "current_activity_year": clean_text(current.get("year")),
        "current_activity_json": json_string(current),
        "years_active_json": json_string(years),
        "start_year": start_year,
        "end_year": end_year,
        "start_date": start_date,
        "end_date": end_date,
        "total_funding": amount_text,
        "currency": "NOK" if amount_text else None,
        "landing_page_url": project_url(source, project_id),
        "source_list_url": EXPLORE_URL,
        "downloaded_at": downloaded_at,
    }


# --- fetch ------------------------------------------------------------------------------------------------------------
def discover_build_id(session, directory):
    raw = directory / "explore.html"
    download(session, EXPLORE_URL, raw)
    match = NEXT_DATA_RE.search(raw.read_text(encoding="utf-8", errors="replace"))
    raw.unlink()
    if not match:
        raise ValueError("Prosjektbanken explore page has no __NEXT_DATA__")
    build_id = json.loads(match.group(1)).get("buildId")
    if not build_id or not re.fullmatch(r"[A-Za-z0-9_-]+", build_id):
        raise ValueError("Prosjektbanken __NEXT_DATA__ has no usable buildId")
    return build_id


def list_url(build_id, offset):
    return (f"{BASE_URL}/_next/data/{build_id}/en/explore/projects.json?"
            + urlencode({**FILTER_PARAMS, "resultCount": PAGE_SIZE, "offset": offset}))


def fetch_rows(session, directory):
    build_id = discover_build_id(session, directory)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    rows, expected, offset, pages = {}, None, 0, 0
    while expected is None or offset < expected:
        check_disk(directory)
        raw = directory / "page.json"
        download(session, list_url(build_id, offset), raw)
        payload = json.loads(raw.read_bytes())
        raw.unlink()
        listing = ((payload.get("pageProps") or {}).get("initialState") or {}).get("PROJECTLIST") or {}
        projects = listing.get("projects") or []
        total = int((listing.get("pagination") or {}).get("totalHits") or 0)
        if total <= 0:
            raise ValueError(f"Prosjektbanken offset {offset}: no pagination.totalHits")
        if expected is None:
            expected = total
        elif total != expected:
            raise ValueError(f"Prosjektbanken totalHits moved during the pull ({expected} -> {total}); refusing a mixed snapshot")
        if not projects:
            raise ValueError(f"Prosjektbanken returned an empty page at offset {offset} of {expected}")
        for project in projects:
            row = flatten_project(project, downloaded_at)
            if row["source_record_id"] in rows:   # offset paging repeated (so also skipped) a page: never a silent drop
                raise ValueError(f"Prosjektbanken offset {offset}: project {row['source_record_id']} repeated while paging; "
                                 "refusing a partial snapshot")
            rows[row["source_record_id"]] = row
        offset += len(projects)
        pages += 1
        time.sleep(PAGE_DELAY)
    if len(rows) != expected:
        raise ValueError(f"Prosjektbanken: {len(rows)} distinct projects for totalHits {expected}; refusing a partial snapshot")
    print(f"Prosjektbanken: buildId {build_id}, totalHits {expected}, {pages} pages, {len(rows)} distinct projects", flush=True)
    return rows, expected


def build(directory):
    output = directory / "rcn_project_bank.parquet"
    with http_session() as session:
        session.headers.update({"User-Agent": "OpenAlex-RCN-Ingest/1.0 (+https://openalex.org)",
                                "Accept": "application/json, text/html;q=0.8"})
        rows, total = fetch_rows(session, directory)
    values = [rows[k] for k in sorted(rows)]   # the saved file is sorted by source_record_id
    award_ids = [r["funder_award_id"] for r in values]
    if len(set(award_ids)) != len(award_ids):
        raise ValueError("RCN: duplicate funder_award_id values; refusing upload")
    schema = RCN_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(
        {"measure": "Prosjektbanken totalHits (Kilde=FORISS)", "value": total})})   # the gate compares rows with it
    with pq.ParquetWriter(output, schema) as writer:
        for i in range(0, len(values), 5000):
            writer.write_table(pa.Table.from_pylist(values[i:i + 5000], schema=schema))
    print(f"RCN: {len(values)} projects written ({sum(1 for r in values if r['total_funding'])} with an amount, "
          f"{sum(1 for r in values if r['project_summary'] or r['pop_sci_description'])} with a summary)", flush=True)
    return output


def main():
    run("rcn_project_bank", build, RCN_SCHEMA)


if __name__ == "__main__":
    main()
