"""FWF (Austrian Science Fund) projects from the FWF Open API, weekly. CC0. No registration.

The API is a Meilisearch instance: GET https://openapi.fwf.ac.at/indexes/projects/documents?limit=1000&offset=N, with a
bearer key FWF publishes at https://openapi.fwf.ac.at/fwfkey/ (read at run time, never logged; FWF can rotate it).
FWF rebuilds its indexes once a day (~03:15 UTC), and the API's own `id` changes with every rebuild, so:
  - the harvest records the projects index's updatedAt before and after, and refuses the upload if it moved (rerun);
  - every page is checked against the listing's total: a moved total, a short or repeated page, or a final count
    short of the total aborts;
  - rows are keyed on the project code (the grant DOI's suffix, e.g. P 22135 -> P22135), never on the API id, and
    written sorted by code, so reruns of the same load write the same rows.
The listing's total and every record not written, by reason, go in the parquet footer (key openalex.funder_total) for
the gate.
One row per project: titles, programme, call, status, dates, approved amount (EUR), the principal investigator (names,
role, ORCID, institution, ROR), the public summary, and the team lists as FWF writes them (strings), which walden can use
later. Outputs and further funding are separate indexes and are not read here.
"""
import json
import re
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import FWF_SCHEMA

API = "https://openapi.fwf.ac.at"
KEY_URL = f"{API}/fwfkey/"
PAGE_SIZE = 1000           # the documented page size
PAGE_DELAY = 1.0           # seconds between requests; FWF states no limit and asks for no "mass access"
CODE = re.compile(r"^[A-Z]{1,5}[0-9]{1,8}$")
ORCID = re.compile(r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$")
MAX_BAD_CODES = 0.005      # records whose code can't be read; more than this refuses the upload


def _clean(value):
    if isinstance(value, str):
        value = " ".join(value.split())
        return value or None
    return None


def _list(value):
    if not isinstance(value, list):
        return None
    out = [v for v in (_clean(x) for x in value) if v]
    return out or None


def project_code(record):
    """The project code from the grant DOI (10.55776/P22135 -> P22135), else from the API id (projects-P22135)."""
    doi = _clean(record.get("_str.grantdoi")) or ""
    if doi.lower().startswith("10.55776/"):
        code = doi.split("/", 1)[1].upper()
    else:
        code = (_clean(record.get("id")) or "").removeprefix("projects-").upper()
    return code if CODE.match(code) else None


def _orcid(value):
    value = (_clean(value) or "").upper().removeprefix("HTTPS://ORCID.ORG/")
    return value if ORCID.match(value) else None


def _amount(value):
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def parse_project(record, stamp, index_updated_at):
    code = project_code(record)
    if code is None:
        return None
    doi = _clean(record.get("_str.grantdoi"))
    return {
        "code": code,
        "grant_doi": doi.lower() if doi else f"10.55776/{code.lower()}",
        "program_en": _clean(record.get("_str.program.en")),
        "program_de": _clean(record.get("_str.program.de")),
        "call_en": _clean(record.get("_str.call.en")),
        "status_en": _clean(record.get("_str.status.en")),
        "title_en": _clean(record.get("_str.projecttitle.en")),
        "title_de": _clean(record.get("_str.projecttitle.de")),
        "summary_en": _clean(record.get("_str.prproposalsummary.en")),
        "summary_de": _clean(record.get("_str.prproposalsummary.de")),
        "approval_date": _clean(record.get("_date.approvaldate")),
        "start_date": _clean(record.get("_date.startdate")),
        "end_date": _clean(record.get("_date.enddate")),
        "amount": _amount(record.get("_long.approvedamount")),
        "pi_role": _clean(record.get("_str.principalinvestigator.role")),
        "pi_first": _clean(record.get("_str.principalinvestigator.firstname")),
        "pi_last": _clean(record.get("_str.principalinvestigator.lastname")),
        "pi_orcid": _orcid(record.get("_str.principalinvestigator.orcid")),
        "pi_inst": _clean(record.get("_str.principalinvestigator.researchinstitute.name")),
        "pi_ror": _clean(record.get("_str.principalinvestigator.researchinstitute.ror")),
        "research_radar_url": _clean(record.get("_str.url")),
        "keywords": _list(record.get("_list.keywords.split")),
        "research_institutes": _list(record.get("_list.researchinstitutes")),
        "key_researchers": _list(record.get("_list.keyresearchers")),
        "associate_researchers": _list(record.get("_list.associateresearchers")),
        "national_participants": _list(record.get("_list.nationalprojectparticipants.en")),
        "international_participants": _list(record.get("_list.internationalprojectparticipants.en")),
        "consortium": _list(record.get("_list.consortium")),
        "n_outputs_listed": len(record.get("_list.connected.output") or []),
        "index_updated_at": index_updated_at,
        "ingested_at": stamp,
    }


def _get_json(session, directory, url):
    raw = directory / "page.json"
    download(session, url, raw)
    try:
        return json.loads(raw.read_bytes())
    finally:
        raw.unlink(missing_ok=True)


def _key(session, directory):
    raw = directory / "key.txt"
    download(session, KEY_URL, raw)
    key = raw.read_text().strip()
    raw.unlink()
    if not re.fullmatch(r"[0-9a-f]{64}", key):
        raise ValueError("FWF: the published API key page did not hold a key")   # never print the page
    return key


def _updated_at(session, directory):
    body = _get_json(session, directory, f"{API}/indexes")
    stamps = {i.get("uid"): i.get("updatedAt") for i in body.get("results") or []}
    if not stamps.get("projects"):
        raise ValueError("FWF: /indexes lists no projects index")
    return stamps["projects"]


def harvest(session, directory):
    """Yield (updatedAt, the listing's total, one page of records), checked against the total and the index's updatedAt."""
    before = _updated_at(session, directory)
    expected, offset, seen = None, 0, set()
    while expected is None or offset < expected:
        check_disk(directory)
        time.sleep(PAGE_DELAY)
        body = _get_json(session, directory, f"{API}/indexes/projects/documents?limit={PAGE_SIZE}&offset={offset}")
        total, records = body.get("total"), body.get("results") or []
        if expected is None:
            if not total:
                raise ValueError("FWF projects: empty listing; refusing an incomplete harvest")
            expected = total
        if total != expected:
            raise ValueError(f"FWF projects: total moved during the harvest ({expected} -> {total}); rerun")
        ids = [r.get("id") for r in records]
        if len(records) != min(PAGE_SIZE, expected - offset) or None in ids or len(set(ids)) != len(ids) or seen & set(ids):
            raise ValueError(f"FWF projects: page at offset {offset} short or repeated; rerun")
        seen.update(ids)
        yield before, expected, records
        offset += PAGE_SIZE
    time.sleep(PAGE_DELAY)
    if _updated_at(session, directory) != before:
        raise ValueError("FWF projects: the index was rebuilt during the harvest; rerun")
    if len(seen) != expected:
        raise ValueError(f"FWF projects: {len(seen)} records for total {expected}")


def build(directory):
    output = directory / "fwf_open_api.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows, bad, duplicates, records, total = {}, 0, 0, 0, None
    with http_session() as session:
        session.headers["User-Agent"] = "OpenAlex-FWF-Ingest/1.0 (+https://openalex.org)"
        session.headers["Authorization"] = f"Bearer {_key(session, directory)}"
        for updated_at, total, page in harvest(session, directory):
            for record in page:
                records += 1
                row = parse_project(record, stamp, updated_at)
                if row is None:
                    bad += 1
                elif row["code"] in rows:
                    duplicates += 1          # first page wins
                else:
                    rows[row["code"]] = row
    if bad > MAX_BAD_CODES * records:
        raise ValueError(f"FWF: {bad} of {records} records have no readable project code; refusing upload")
    schema = FWF_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
        {"measure": "FWF Open API projects index total (/indexes/projects/documents)", "value": total,
         "excluded": {"no readable project code": bad, "project code repeated (first page wins)": duplicates}})})
    table = pa.Table.from_pylist([rows[c] for c in sorted(rows)], schema=schema)
    pq.write_table(table, output)
    with_orcid = sum(1 for r in rows.values() if r["pi_orcid"])
    print(f"FWF projects: {len(rows)} rows from {records} records ({bad} without a code, {duplicates} repeated codes); "
          f"{with_orcid} with a PI ORCID", flush=True)
    return output


def main():
    run("fwf_open_api", build, FWF_SCHEMA)


if __name__ == "__main__":
    main()
