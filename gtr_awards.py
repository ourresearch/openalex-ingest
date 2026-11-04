"""UKRI Gateway to Research (GtR) projects with their people, monthly. No key needed.

Three bulk passes, one page at a time, each checked against the API's totalSize:
  /persons  JSON: names and orcidId (the XML form of a person omits orcidId and the ORCID_ID link)
  /funds    XML: INCOME_ACTUAL amounts, summed per project
  /projects XML: walden's laptop parser (scripts/local/gtr_to_s3.py, origin d3fd37ba) ported unchanged, so the 16 old
            columns parse exactly as the saved file did; the names are now strings and filled.
Added: pi_orcid, and persons, one entry per person linked to the project (every *_PER link), with their best role and
all roles, sorted by role rank, then family name, given name and person id, so reruns write the same arrays.
GtR re-keyed its records in 2026-09: project and person ids from older files do not carry over; the grant reference does.
"""
import json
import time
from datetime import datetime, timezone

from lxml import etree
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from award_fetch import check_disk, download, http_session, run
from award_schemas import GTR_SCHEMA

API = "https://gtr.ukri.org/gtr/api"
PAGE_SIZE = 100            # the API maximum
PAGE_DELAY = 0.5           # seconds between pages; GtR rate-limits in long windows (walden oxjob #195)
PAGE_ATTEMPTS = 4          # whole-page retries on top of the session's own, 60 s apart
JSON_ACCEPT = "application/vnd.rcuk.gtr.json-v7"
NS1 = "{http://gtr.rcuk.ac.uk/gtr/api}"
PROJECT_NS = "{http://gtr.rcuk.ac.uk/gtr/api/project}"
FUND_NS = "{http://gtr.rcuk.ac.uk/gtr/api/fund}"
# Investigator roles first. Every *_PER role is kept; unknown ones sort last, by name.
ROLE_RANK = {"PI_PER": 0, "FELLOW_PER": 1, "COI_PER": 2, "RESEARCH_COI_PER": 3, "PM_PER": 4, "RESEARCH_PER": 5}
MAX_UNRESOLVED_LINKS = 0.005   # person links whose person is missing from /persons


def _text(element):
    return element.text.strip() if element is not None and element.text and element.text.strip() else None


def _clean(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def _date(value):
    # The laptop file's conversion (pd.to_datetime, utc=True): GtR gives UK midnight as a UTC instant, so summer dates
    # land one day early. Kept on purpose: changing it would re-date half the served awards; fix it as its own change.
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    return None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def role_rank(role):
    return (ROLE_RANK.get(role, len(ROLE_RANK)), role)


def parse_project(proj):
    """One ns2:project element -> the laptop parser's fields plus every person link. Same lookups as gtr_to_s3.py."""
    grant_reference = None
    identifiers = proj.find(f"{PROJECT_NS}identifiers")
    if identifiers is not None:
        for ident in identifiers.findall(f"{PROJECT_NS}identifier"):
            if ident.get(f"{PROJECT_NS}type") == "RCUK":
                grant_reference = _text(ident)
                break
    start_date = end_date = lead_org_id = None
    person_links = []
    links = proj.find(f"{NS1}links")
    if links is not None:
        for link in links.findall(f"{NS1}link"):
            rel = link.get(f"{NS1}rel") or ""
            href = link.get(f"{NS1}href", "")
            if rel == "FUND":
                start_date, end_date = link.get(f"{NS1}start"), link.get(f"{NS1}end")
            elif rel == "LEAD_ORG":
                lead_org_id = href.split("/")[-1] if href else None
            elif rel.endswith("_PER") and href:
                person_links.append((href.split("/")[-1], rel))
    lead_org_name = None
    participants = proj.find(f"{PROJECT_NS}participantValues")
    if participants is not None:
        for participant in participants.findall(f"{PROJECT_NS}participant"):
            if _text(participant.find(f"{PROJECT_NS}role")) == "LEAD_PARTICIPANT":
                lead_org_name = _text(participant.find(f"{PROJECT_NS}organisationName"))
                break
    return {
        "project_id": proj.get(f"{NS1}id"),
        "grant_reference": grant_reference,
        "title": _text(proj.find(f"{PROJECT_NS}title")),
        "abstract": _text(proj.find(f"{PROJECT_NS}abstractText")) or _text(proj.find(f"{PROJECT_NS}techAbstractText")),
        "status": _text(proj.find(f"{PROJECT_NS}status")),
        "grant_category": _text(proj.find(f"{PROJECT_NS}grantCategory")),
        "lead_funder": _text(proj.find(f"{PROJECT_NS}leadFunder")),
        "start_date": _date(start_date),
        "end_date": _date(end_date),
        "lead_org_id": lead_org_id,
        "lead_org_name": lead_org_name,
        "person_links": person_links,
    }


def parse_person(record):
    """One JSON person -> (id, names, ORCID as GtR gives it). walden normalizes and validates the ORCID."""
    return record["id"], {"given_name": _clean(record.get("firstName")), "other_names": _clean(record.get("otherNames")),
                          "family_name": _clean(record.get("surname")), "orcid": _clean(record.get("orcidId"))}


def parse_fund(fund):
    """One ns2:fund element -> (project id, GBP amount, category); as gtr_to_s3.parse_funds_xml."""
    project_id = None
    links = fund.find(f"{NS1}links")
    if links is not None:
        for link in links.findall(f"{NS1}link"):
            if link.get(f"{NS1}rel") == "FUNDED":
                href = link.get(f"{NS1}href", "")
                project_id = href.rsplit("/", 1)[-1] if href else None
                break
    amount = None
    value = fund.find(f"{FUND_NS}valuePounds")
    if value is not None and value.get(f"{NS1}amount"):
        try:
            amount = float(value.get(f"{NS1}amount"))
        except ValueError:
            pass
    return project_id, amount, _text(fund.find(f"{FUND_NS}category"))


def project_people(person_links, persons):
    """Person links of one project -> one sorted entry per person, and the number of links whose person is unknown."""
    roles, unresolved = {}, 0
    for person_id, rel in person_links:
        roles.setdefault(person_id, set()).add(rel)
    people = []
    for person_id, rels in roles.items():
        names = persons.get(person_id)
        if names is None:
            unresolved += len(rels)
            names = {"given_name": None, "other_names": None, "family_name": None, "orcid": None}
        ordered = sorted(rels, key=role_rank)
        people.append({"person_id": person_id, "role": ordered[0], "roles": ordered, **names})
    people.sort(key=lambda p: (role_rank(p["role"]), p["family_name"] or "", p["given_name"] or "", p["person_id"]))
    return people, unresolved


def project_row(project, persons, amounts, stamp):
    people, unresolved = project_people(project["person_links"], persons)
    pi = next((p for p in people if "PI_PER" in p["roles"]), None)
    row = {k: v for k, v in project.items() if k != "person_links"}
    row.update(amount=amounts.get(project["project_id"]), pi_id=pi and pi["person_id"],
               pi_given_name=pi and pi["given_name"], pi_family_name=pi and pi["family_name"],
               ingested_at=stamp, pi_orcid=pi and pi["orcid"], persons=people)
    return {name: row[name] for name in GTR_SCHEMA.names}, unresolved


def _xml_root(path):
    return etree.parse(str(path), etree.XMLParser(resolve_entities=False, no_network=True)).getroot()


def _fetch(session, directory, endpoint, page, accept):
    """Download one page to a temp file (disk-guarded); retry the page after GtR's long 429 windows."""
    session.headers["Accept"] = accept
    url = requests.Request("GET", f"{API}/{endpoint}", params={"p": page, "s": PAGE_SIZE}).prepare().url
    raw = directory / "page.raw"
    for attempt in range(PAGE_ATTEMPTS):
        try:
            download(session, url, raw)
            return raw
        except requests.RequestException:
            if attempt == PAGE_ATTEMPTS - 1:
                raise RuntimeError(f"GtR {endpoint} page {page} failed after {PAGE_ATTEMPTS} attempts") from None
            time.sleep(60)


def _pages(session, directory, endpoint, accept, read):
    """Yield (page number, items) for every page; stop if the API's totals move or a page is short or repeated."""
    expected = pages = None
    seen = set()
    page = 1
    while pages is None or page <= pages:
        check_disk(directory)
        raw = _fetch(session, directory, endpoint, page, accept)
        total, n_pages, items, ids = read(raw)
        raw.unlink()
        if expected is None:
            expected, pages = total, n_pages
            if not total or not n_pages:
                raise ValueError(f"GtR {endpoint}: empty listing; refusing an incomplete harvest")
        if (total, n_pages) != (expected, pages):
            raise ValueError(f"GtR {endpoint}: totals changed during the harvest ({expected} -> {total}); rerun")
        full = PAGE_SIZE if page < pages else expected - PAGE_SIZE * (pages - 1)
        if len(ids) != full or len(set(ids)) != len(ids) or seen & set(ids):
            raise ValueError(f"GtR {endpoint}: page {page} short or repeated; rerun")
        seen.update(ids)
        yield page, items
        page += 1
        time.sleep(PAGE_DELAY)
    if len(seen) != expected:
        raise ValueError(f"GtR {endpoint}: {len(seen)} records for totalSize {expected}")


def _read_persons(raw):
    body = json.loads(raw.read_bytes())
    records = body.get("person") or []
    return body.get("totalSize"), body.get("totalPages"), [parse_person(r) for r in records], [r["id"] for r in records]


def _read_xml(element):
    def read(raw):
        root = _xml_root(raw)
        nodes = root.findall(element)
        items = [parse_project(n) for n in nodes] if element.endswith("project") else [parse_fund(n) for n in nodes]
        return (int(root.get(f"{NS1}totalSize", 0)), int(root.get(f"{NS1}totalPages", 0)), items,
                [n.get(f"{NS1}id") for n in nodes])
    return read


def build(directory):
    output = directory / "gateway_to_research.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with http_session() as session:
        session.headers["User-Agent"] = "OpenAlex-GtR-Ingest/2.0 (contact@openalex.org)"
        persons = {}
        for _, items in _pages(session, directory, "persons", JSON_ACCEPT, _read_persons):
            persons.update(items)
        print(f"GtR persons: {len(persons)} ({sum(1 for p in persons.values() if p['orcid'])} with an ORCID)", flush=True)
        amounts = {}
        for _, items in _pages(session, directory, "funds", "application/xml", _read_xml(f"{FUND_NS}fund")):
            for project_id, amount, category in items:
                # Only INCOME_ACTUAL is the awarded amount; extensions and supplements add up (gtr_to_s3.py).
                if project_id and amount is not None and category == "INCOME_ACTUAL":
                    amounts[project_id] = amounts.get(project_id, 0.0) + amount
        print(f"GtR funds: {len(amounts)} projects with an amount", flush=True)
        seen, links, unresolved, rows_out, no_reference = set(), 0, 0, 0, 0
        with pq.ParquetWriter(output, GTR_SCHEMA) as writer:
            for _, items in _pages(session, directory, "projects", "application/xml", _read_xml(f"{PROJECT_NS}project")):
                rows = []
                for project in items:
                    key = project["grant_reference"] and project["grant_reference"].lower()
                    if not key:
                        no_reference += 1   # walden's mapping drops these too
                        continue
                    if key in seen:
                        continue            # first page wins, as the laptop file's keep="first"
                    seen.add(key)
                    row, missing = project_row(project, persons, amounts, stamp)
                    links += len(project["person_links"])
                    unresolved += missing
                    rows.append(row)
                if rows:
                    writer.write_table(pa.Table.from_pylist(rows, schema=GTR_SCHEMA))
                    rows_out += len(rows)
    if links and unresolved > MAX_UNRESOLVED_LINKS * links:
        raise ValueError(f"GtR: {unresolved} of {links} person links name no listed person; refusing upload")
    print(f"GtR projects: {rows_out} rows, {no_reference} without a reference, {links} person links, "
          f"{unresolved} unresolved", flush=True)
    return output


def main():
    run("gateway_to_research", build, GTR_SCHEMA)


if __name__ == "__main__":
    main()
