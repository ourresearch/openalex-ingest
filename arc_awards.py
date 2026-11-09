"""Australian Research Council (ARC) grants with every named investigator, their role and ORCID, monthly. No key needed.

Two passes over ARC's Research Grants Search API (dataportal.arc.gov.au/RGS/API, JSON:API):
  list    /grants?page[number]=N&page[size]=1000, every page checked against meta total-size / total-pages: the 20 columns of
          walden's laptop file (scripts/local/arc_to_s3.py, s3://openalex-ingest/awards/arc/arc_projects.parquet) in its order,
          so the mapping's existing columns read the same values. The list flattens investigators into one string.
  detail  /grants/{code}, one call per listed grant: investigators[] (title, first and family name, role, fellowship flag,
          orcid-identifier), organisations[] (name, role, state) and project-start-date. The API has no bulk form ('Include'
          is refused), so this pass is ~35k calls: DETAIL_RATE per second over DETAIL_WORKERS connections, about 10 h.
Grants are deduplicated on lower(code), first page wins. people keeps ARC's order with the lead moved first; the lead is the
person whose 'title first family' is the list's lead-investigator string (else the first person). A grant whose detail call
fails after its retries keeps its list columns and detail_status 'missing'; more than MAX_MISSING_DETAIL of them refuses the
upload. RGS also lists grants another agency funds and ARC administers (grant_funder); they are kept here, and walden's mapping
decides what it serves. The list's total-size and every listed grant not written, by reason, go in the parquet footer (key
openalex.funder_total) for the gate.
"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from award_fetch import check_disk, http_session, run
from award_schemas import ARC_SCHEMA

API = "https://dataportal.arc.gov.au/RGS/API/grants"
USER_AGENT = "OpenAlex-ARC-Ingest/1.0 (+https://openalex.org)"
PAGE_SIZE = 1000            # the API maximum
PAGE_DELAY = 0.5            # seconds between list pages
DETAIL_RATE = 1.0           # detail requests started per second, overall
DETAIL_WORKERS = 2          # connections in flight: ARC answers in ~1-1.5 s, so 2 keep the rate near DETAIL_RATE
CALL_ATTEMPTS = 4           # whole-call retries on top of the session's own (429/5xx), 30 s apart
MAX_MISSING_DETAIL = 0.005  # grants listed whose detail record could not be read
TIMEOUT = (15, 120)


def _clean(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def _int(value):
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def _float(value):
    try:
        return float(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def list_row(item, stamp):
    """One list item -> the laptop file's columns (arc_to_s3.parse_single_grant), same names and order."""
    attrs = item.get("attributes") or {}
    scheme = attrs.get("scheme-information") or {}
    priorities = attrs.get("grant-priorities") or []
    end = attrs.get("anticipated-end-date")
    return {
        "grant_id": item.get("id"),
        "code": attrs.get("code"),
        "title": attrs.get("grant-title"),
        "summary": attrs.get("grant-summary"),
        "start_year": _int(attrs.get("funding-commencement-year")),
        "end_date": str(end) if end is not None else None,
        "grant_value": _float(attrs.get("grant-value")),
        "grant_status": attrs.get("grant-status"),
        "grant_funder": attrs.get("grant-funder"),
        "program_name": attrs.get("program-name"),
        "scheme_name": attrs.get("scheme-name"),
        "scheme_code": (scheme.get("scheme-code") or "").strip() or None,
        "scheme_round": scheme.get("scheme-round"),
        "submission_year": _int(scheme.get("submission-year")),
        "primary_field_of_research": attrs.get("primary-field-of-research"),
        "lead_investigator": attrs.get("lead-investigator"),
        "investigators": attrs.get("investigators"),
        "grantee": attrs.get("grantee"),
        "grant_priorities_json": json.dumps(priorities) if priorities else None,
        "ingested_at": stamp,
    }


def _full_name(person):
    return " ".join(p for p in (person.get("title"), person.get("given_name"), person.get("family_name")) if p)


def _norm(text):
    return " ".join((text or "").lower().split())


def detail_fields(record, lead_string):
    """One detail record (data.attributes) -> project_start_date, people (lead first, then ARC's order), organisations."""
    attrs = record or {}
    people = []
    for inv in attrs.get("investigators") or []:
        people.append({
            "title": _clean(inv.get("title")),
            "given_name": _clean(inv.get("first-name")),
            "family_name": _clean(inv.get("family-name")),
            "role_name": _clean(inv.get("role-name")),
            "role_code": _clean(inv.get("role-code")),
            "is_fellowship": inv.get("is-fellowship") if isinstance(inv.get("is-fellowship"), bool) else None,
            "orcid": _clean(inv.get("orcid-identifier")),
            "is_lead": False,
        })
    lead = next((i for i, p in enumerate(people) if _norm(_full_name(p)) == _norm(lead_string)), None)
    if lead is None and lead_string:
        # the list string sometimes carries a title the record spells differently: match on first + family name
        lead = next((i for i, p in enumerate(people)
                     if _norm(" ".join(x for x in (p["given_name"], p["family_name"]) if x))
                     and _norm(lead_string).endswith(_norm(" ".join(x for x in (p["given_name"], p["family_name"]) if x)))), None)
    if people:
        lead = 0 if lead is None else lead
        people[lead]["is_lead"] = True
        people.insert(0, people.pop(lead))
    organisations = [{"name": _clean(o.get("organisation-name")), "role_name": _clean(o.get("role-name")),
                      "state": _clean(o.get("state"))} for o in attrs.get("organisations") or []]
    start = attrs.get("project-start-date")
    return {"project_start_date": str(start) if start else None, "people": people, "organisations": organisations}


def _get_json(session, url):
    """GET one URL as JSON. Returns (status, body); retries whole calls after the session's own retries give up."""
    for attempt in range(CALL_ATTEMPTS):
        try:
            response = session.get(url, timeout=TIMEOUT)
            if response.status_code == 400:
                return 400, None          # 'A Grant does not exist for id': listed but no detail record
            response.raise_for_status()
            return response.status_code, response.json()
        except (requests.RequestException, ValueError):
            if attempt == CALL_ATTEMPTS - 1:
                return None, None
            time.sleep(30)


class _Pacer:
    """Starts at most `rate` requests per second across threads."""

    def __init__(self, rate):
        self.interval, self.lock, self.last = 1.0 / rate, threading.Lock(), 0.0

    def wait(self):
        with self.lock:
            delay = self.last + self.interval - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self.last = time.monotonic()


def list_pages(session, directory):
    """Yield (total-size, the items of every list page); stop if the totals move or a page is short or repeated."""
    expected = pages = None
    seen = set()
    page = 1
    while pages is None or page <= pages:
        check_disk(directory)
        url = requests.Request("GET", API, params={"page[number]": page, "page[size]": PAGE_SIZE}).prepare().url
        status, body = _get_json(session, url)
        if status != 200 or body is None:
            raise RuntimeError(f"ARC list page {page} failed after {CALL_ATTEMPTS} attempts")
        meta, items = body.get("meta") or {}, body.get("data") or []
        total, n_pages = _int(meta.get("total-size")), _int(meta.get("total-pages"))
        if expected is None:
            expected, pages = total, n_pages
            if not total or not n_pages:
                raise ValueError("ARC list: empty listing; refusing an incomplete harvest")
        if (total, n_pages) != (expected, pages):
            raise ValueError(f"ARC list: totals changed during the harvest ({expected} -> {total}); rerun")
        ids = [item.get("id") for item in items]
        full = PAGE_SIZE if page < pages else expected - PAGE_SIZE * (pages - 1)
        if len(ids) != full or len(set(ids)) != len(ids) or seen & set(ids) or None in ids:
            raise ValueError(f"ARC list: page {page} short or repeated; rerun")
        seen.update(ids)
        yield expected, items
        page += 1
        time.sleep(PAGE_DELAY)
    if len(seen) != expected:
        raise ValueError(f"ARC list: {len(seen)} grants for total-size {expected}")


_local = threading.local()


def _session():
    """One session per thread (requests sessions are not shared across threads)."""
    if not hasattr(_local, "session"):
        _local.session = http_session()
        _local.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return _local.session


def build(directory):
    output = directory / "arc_rgs.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    started = time.monotonic()
    rows, keys, total, no_code, repeated = [], set(), None, 0, 0
    for total, items in list_pages(_session(), directory):
        for item in items:
            row = list_row(item, stamp)
            key = row["code"] and row["code"].strip().lower()
            if not key:
                no_code += 1
                continue
            if key in keys:
                repeated += 1
                continue                    # first page wins, as the laptop file's keep="first"
            keys.add(key)
            rows.append(row)
    print(f"ARC list: {len(rows)} grants in {time.monotonic() - started:.0f} s", flush=True)
    pacer, counts = _Pacer(DETAIL_RATE), {"ok": 0, "missing": 0}

    def detail(row):
        pacer.wait()
        status, body = _get_json(_session(), f"{API}/{requests.utils.quote(row['code'], safe='')}")
        return row, ((body or {}).get("data") or {}).get("attributes") if status == 200 else None

    with ThreadPoolExecutor(DETAIL_WORKERS) as pool:
        for n, (row, record) in enumerate(pool.map(detail, rows), start=1):
            if record is None or (record.get("code") or "").strip().lower() != row["code"].strip().lower():
                row.update(project_start_date=None, people=None, organisations=None, detail_status="missing")
                counts["missing"] += 1
            else:
                row.update(detail_fields(record, row["lead_investigator"]), detail_status="ok")
                counts["ok"] += 1
            if n % 2000 == 0:
                check_disk(directory)
                print(f"ARC detail: {n}/{len(rows)} ({counts['missing']} missing), {time.monotonic() - started:.0f} s",
                      flush=True)
    if counts["missing"] > MAX_MISSING_DETAIL * len(rows):
        raise ValueError(f"ARC: {counts['missing']} of {len(rows)} grants have no readable detail record; refusing upload")
    schema = ARC_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
        {"measure": "ARC RGS /grants list meta total-size", "value": total,
         "excluded": {"no grant code": no_code, "grant code repeated, any case (first page wins)": repeated},
         "written without a detail record": counts["missing"]})})
    table = pa.Table.from_pylist([{name: row.get(name) for name in ARC_SCHEMA.names} for row in rows], schema=schema)
    pq.write_table(table, output)
    people = sum(len(r["people"] or []) for r in rows)
    orcids = sum(1 for r in rows for p in (r["people"] or []) if p["orcid"])
    print(f"ARC: {len(rows)} grants, {counts['missing']} without detail, {people} people, {orcids} with an ORCID, "
          f"{time.monotonic() - started:.0f} s", flush=True)
    return output


def main():
    run("arc_rgs", build, ARC_SCHEMA)


if __name__ == "__main__":
    main()
