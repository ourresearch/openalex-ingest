"""NWO (Dutch Research Council) projects from the NWOpen API, with every project member. Weekly. No key needed. CC0 1.0.

One pass over https://nwopen-api.nwo.nl/NWOpen-API/api/Projects, 100 per page (the API maximum), one page at a time,
each page checked against the listing's meta (count, pages, date): a moved count or load date, a short page or a page
whose ids were all seen before aborts the harvest. Projects are deduplicated on project_id, first page wins.
The first 16 columns are walden's saved laptop file (s3://openalex-ingest/awards/nwo/nwo_projects.parquet, 2026-01-08,
scripts/local/nwo_to_s3.py) in its order and types, parsed the same way (lead = first Main Applicant, else first
Project leader, else first member; ORCID placeholder 'https://orcid.org/-' dropped; top organisation before '||').
Added: department, sub_department, parent_project_id, grant_doi (NWO's grant DOI, 10.61686/...), award_amount (EUR),
pi_member_id and pi_ror (the lead's member id and ROR), and members: one entry per person (member_id) with their best role and all roles, sorted by role rank, then family
name, given name and member id, so reruns write the same arrays.
The listing's meta.count and the ids it lists twice go in the parquet footer (key openalex.funder_total) for the gate.
"""
import json
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from award_fetch import check_disk, download, http_session, run
from award_schemas import NWO_SCHEMA

API = "https://nwopen-api.nwo.nl/NWOpen-API/api/Projects"
PAGE_SIZE = 100            # the API maximum
PAGE_DELAY = 0.5           # seconds between pages (each page takes ~6 s to serve)
PAGE_ATTEMPTS = 4          # whole-page retries on top of the session's own, 30 s apart
ROLE_RANK = {"Main Applicant": 0, "Project leader": 1, "Co-applicant": 2, "Researcher": 3}
MAX_DUPLICATE_IDS = 0.005  # project_ids listed twice; more than this looks like a moving listing, not a data quirk
ORCID_PLACEHOLDER = "https://orcid.org/-"


def _clean(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def _date(value):
    # The laptop file's conversion: NWOpen gives local midnights as 'YYYY-MM-DDT00:00:00'; keep the date part.
    value = _clean(value)
    return value[:10] if value and len(value) >= 10 and value[4] == "-" and value[7] == "-" else None


def _int(value):
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _float(value):
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _orcid(value):
    value = _clean(value)
    return None if value is None or value == ORCID_PLACEHOLDER else value


def _doi(value):
    value = _clean(value)
    if not value:
        return None
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "doi:"):
        if value.lower().startswith(prefix):
            return value[len(prefix):]
    return value


def role_rank(role):
    return (ROLE_RANK.get(role, len(ROLE_RANK)), role or "")


def laptop_lead(members):
    """The laptop parser's lead: first Main Applicant, else first Project leader, else the first member."""
    for role in ("Main Applicant", "Project leader"):
        for member in members:
            if member.get("role", "") == role:
                return member
    return members[0] if members else None


def project_members(members):
    """API member rows (one per person and role) -> one entry per member_id, best role first, sorted deterministically."""
    people = {}
    for m in members:
        key = str(m.get("member_id")) if m.get("member_id") is not None else \
            "|".join(str(m.get(k) or "") for k in ("last_name", "initials", "first_name", "organisation_id"))
        people.setdefault(key, []).append(m)
    out = []
    for key, rows in people.items():
        rows = sorted(rows, key=lambda r: role_rank(r.get("role")))
        best = rows[0]
        roles = sorted({_clean(r.get("role")) for r in rows if _clean(r.get("role"))}, key=role_rank)
        out.append({
            "member_id": key,
            "role": _clean(best.get("role")),
            "roles": roles,
            "first_name": _clean(best.get("first_name")),
            "initials": _clean(best.get("initials")),
            "prefix": _clean(best.get("prefix")),
            "last_name": _clean(best.get("last_name")),
            "degree_pre_nominal": _clean(best.get("degree_pre_nominal")),
            "degree_post_nominal": _clean(best.get("degree_post_nominal")),
            "orcid": next((o for o in (_orcid(r.get("orcid")) for r in rows) if o), None),
            "organisation": _clean(best.get("organisation")),
            "organisation_id": None if best.get("organisation_id") is None else str(best.get("organisation_id")),
            "ror": _clean(best.get("ror")),
            "active": best.get("active") if isinstance(best.get("active"), bool) else None,
        })
    out.sort(key=lambda p: (role_rank(p["role"]), p["last_name"] or "", p["first_name"] or "", p["member_id"]))
    return out


def project_row(proj, stamp):
    """One NWOpen project -> NWO_SCHEMA row (the laptop file's 16 columns, then the added ones)."""
    members = [m for m in (proj.get("project_members") or []) if isinstance(m, dict)]
    lead = laptop_lead(members)
    org = _clean(lead.get("organisation")) if lead else None
    products = proj.get("products") or []
    updates = proj.get("summary_updates") or []
    row = {
        "project_id": _clean(proj.get("project_id")),
        "title": proj.get("title"),
        "abstract": proj.get("summary_en") or proj.get("summary_nl"),
        "grant_category": proj.get("funding_scheme"),
        "funding_scheme_id": _int(proj.get("funding_scheme_id")),
        "lead_funder": proj.get("department") or proj.get("sub_department"),
        "start_date": _date(proj.get("start_date")),
        "end_date": _date(proj.get("end_date")),
        "reporting_year": _int(proj.get("reporting_year")),
        "lead_org_name": org.split("||")[0].strip() if org else None,
        "pi_id": _orcid(lead.get("orcid")) if lead else None,
        "pi_given_name": lead.get("first_name") if lead else None,
        "pi_family_name": lead.get("last_name") if lead else None,
        "products_json": json.dumps(products) if products else None,
        "summary_updates_json": json.dumps(updates) if updates else None,
        "ingested_at": stamp,
        "department": _clean(proj.get("department")),
        "sub_department": _clean(proj.get("sub_department")),
        "parent_project_id": _clean(proj.get("parent_project_id")),
        "grant_doi": _doi(proj.get("grant_id")),
        "award_amount": _float(proj.get("award_amount")),
        "members": project_members(members),
        "pi_member_id": None if not lead or lead.get("member_id") is None else str(lead.get("member_id")),
        "pi_ror": _clean(lead.get("ror")) if lead else None,
    }
    return {name: row[name] for name in NWO_SCHEMA.names}


def _fetch(session, directory, page):
    """Download one page to a temp file (disk-guarded); retry the whole page a few times."""
    url = requests.Request("GET", API, params={"page": page, "per_page": PAGE_SIZE}).prepare().url
    raw = directory / "page.json"
    for attempt in range(PAGE_ATTEMPTS):
        try:
            download(session, url, raw)
            return json.loads(raw.read_bytes())
        except (requests.RequestException, ValueError):
            raw.unlink(missing_ok=True)
            if attempt == PAGE_ATTEMPTS - 1:
                raise RuntimeError(f"NWOpen page {page} failed after {PAGE_ATTEMPTS} attempts") from None
            time.sleep(30)
        finally:
            raw.unlink(missing_ok=True)


def pages(session, directory):
    """Yield (meta.count, each page's projects); stop if the listing's count, page count or load date moves, or a page is
    short or repeated."""
    expected = n_pages = load_date = None
    seen, listed, page = set(), 0, 1
    while n_pages is None or page <= n_pages:
        check_disk(directory)
        body = _fetch(session, directory, page)
        meta = body.get("meta") or {}
        projects = body.get("projects")
        if not isinstance(projects, list):
            raise ValueError(f"NWOpen page {page}: no projects list; refusing an incomplete harvest")
        if expected is None:
            expected, n_pages, load_date = _int(meta.get("count")), _int(meta.get("pages")), meta.get("date")
            if not expected or not n_pages or n_pages != -(-expected // PAGE_SIZE):
                raise ValueError(f"NWOpen: listing meta {meta!r} is empty or inconsistent; refusing an incomplete harvest")
        if (_int(meta.get("count")), _int(meta.get("pages")), meta.get("date")) != (expected, n_pages, load_date):
            raise ValueError(f"NWOpen: listing changed during the harvest ({expected}/{load_date} -> "
                             f"{meta.get('count')}/{meta.get('date')}); rerun")
        full = PAGE_SIZE if page < n_pages else expected - PAGE_SIZE * (n_pages - 1)
        ids = [p.get("project_id") for p in projects]
        if len(ids) != full or any(not _clean(i) for i in ids) or (ids and seen.issuperset(ids)):
            raise ValueError(f"NWOpen: page {page} short, repeated or missing project ids; rerun")
        seen.update(ids)
        listed += len(ids)
        yield expected, projects
        page += 1
        time.sleep(PAGE_DELAY)
    if listed != expected:
        raise ValueError(f"NWOpen: {listed} records for count {expected}")
    if expected - len(seen) > MAX_DUPLICATE_IDS * expected:
        raise ValueError(f"NWOpen: {expected - len(seen)} duplicate project ids in {expected} records; refusing upload")


def build(directory):
    output = directory / "nwopen.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    seen, rows_out, people, orcids, amounts, duplicates, total = set(), 0, 0, 0, 0, 0, None
    with http_session() as session:
        session.headers["User-Agent"] = "OpenAlex-NWO-Ingest/1.0 (+https://openalex.org)"
        session.headers["Accept"] = "application/json"
        with pq.ParquetWriter(output, NWO_SCHEMA) as writer:
            for total, projects in pages(session, directory):
                rows = []
                for proj in projects:
                    key = _clean(proj.get("project_id"))
                    if key in seen:
                        duplicates += 1
                        continue        # first page wins, as the laptop file's keep="first"
                    seen.add(key)
                    row = project_row(proj, stamp)
                    people += len(row["members"])
                    orcids += sum(1 for m in row["members"] if m["orcid"])
                    amounts += row["award_amount"] is not None
                    rows.append(row)
                if rows:
                    writer.write_table(pa.Table.from_pylist(rows, schema=NWO_SCHEMA))
                    rows_out += len(rows)
            writer.add_key_value_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
                {"measure": "NWOpen Projects listing meta.count", "value": total,
                 "excluded": {"project_id listed twice (first page wins)": duplicates}})})
    print(f"NWOpen projects: {rows_out} rows, {people} members ({orcids} with an ORCID), {amounts} with an amount",
          flush=True)
    return output


def main():
    run("nwopen", build, NWO_SCHEMA)


if __name__ == "__main__":
    main()
