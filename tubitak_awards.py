"""TÜBİTAK (ARDEB) projects from TR Dizin's public search API, monthly. No key needed.

TÜBİTAK publishes its completed and ongoing ARDEB projects through ULAKBİM's TR Dizin
(https://tubitak.gov.tr/tr/duyuru/tubitak-destekli-projeler-veri-tabani). The search UI's own JSON endpoint is read
with facet-documentType=PROJECT, 100 records a page, one page at a time, each page checked against hits.total.
walden's laptop loader (scripts/local/tubitak_to_s3.py, origin e22fdf7f) is ported unchanged: the same 44 string columns
in the same order, the same project-number dedupe (best record by MANAGER present, title, abstract, author count,
firstIndexDate, id) and the same lead rule (first MANAGER by order, else the first contributor), so walden's
sources/trdizin_tubitak_projects.sql reads it as it read the saved file. source_raw_path is now the API URL.
hits.total bounds the paging only when it is exact (relation "eq"): Elasticsearch reports {"value": 10000, "relation": "gte"}
when it stops counting, and paging to that would silently stop at 10,000. Every page must also be complete: not timed out,
every shard answering (an Elasticsearch partial result drops a failed shard's hits, total included). The exact total is
recorded in the parquet footer
(key openalex.funder_total) and in every row (reported_total) for the gate.
"""
import json
import math
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import TUBITAK_SCHEMA

API_URL = "https://search.trdizin.gov.tr/api/defaultSearch/publication/"
SOURCE_NOTICE_URL = "https://tubitak.gov.tr/tr/duyuru/tubitak-destekli-projeler-veri-tabani"
SEARCH_PAGE_URL = "https://search.trdizin.gov.tr/tr/proje/ara"
PAGE_SIZE = 100
PAGE_DELAY = 0.5           # seconds between pages (≤ 2 requests a second)
MIN_ROWS_SHARE = 0.98      # unique projects written vs the API's reported total
# The only record keys the saved file's columns read; the rest (references, citedReferences, attachments …) is dropped
# per page, so ~29k records fit a small dyno (a page of 100 is ~3.4 MB of JSON).
KEEP = ("id", "projectnumber", "abstracts", "authors", "subjects", "startedDate", "endDate", "orderTitle", "projectGroup",
        "publicationYear", "accessType", "language", "endPage", "pdf", "viewCount", "downloadCount", "firstIndexDate",
        "indexDate", "docType")

SPACE_RE = re.compile(r"\s+")
SUFFIX_RE = re.compile(
    r"\b(?:Ph\.?D\.?|MD|M\.?D\.?|Dr\.?|Prof\.?|Assoc\.?|Doç\.?|Jr\.?|Sr\.?|II|III|IV)\b\.?",
    re.IGNORECASE,
)


# --- the laptop loader's normalisers, unchanged ---------------------------------------------------------------------
def clean_text(value):
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = SPACE_RE.sub(" ", text).strip()
    if not text or text.lower() in {"none", "null", "nan", "-"}:
        return None
    return text


def date_part(value):
    text = clean_text(value)
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        if text.startswith("0001-") or text == "1900-01-01":
            return None
        return text
    return None


def int_string(value):
    if value is None:
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return clean_text(value)


def json_string(value):
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def pick_localized(items, key, language):
    language = language.upper()
    for item in items:
        if clean_text(item.get("language")) == language:
            return clean_text(item.get(key))
    return None


def split_name(name):
    text = clean_text(name)
    if not text:
        return None, None
    text = SUFFIX_RE.sub("", text)
    text = SPACE_RE.sub(" ", text).strip(" ,")
    if not text:
        return None, None
    parts = text.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def normalize_project_number(value):
    text = clean_text(value)
    if not text:
        return None
    return SPACE_RE.sub("", text).upper()


def contributor_from_author(author, start_date):
    name = clean_text(author.get("name")) or clean_text(author.get("inPublicationName"))
    given, family = split_name(name)
    institution = author.get("institution") if isinstance(author.get("institution"), dict) else {}
    return {
        "name": name,
        "given_name": given,
        "family_name": family,
        "orcid": clean_text(author.get("orcid")),
        "duty": clean_text(author.get("duty")),
        "order": int_string(author.get("order")),
        "role_start": start_date,
        "institution_name": clean_text(author.get("institutionName")),
        "institution_title": clean_text((institution or {}).get("title")),
        "institution_root_title": clean_text((institution or {}).get("rootTitle")),
        "institution_country": clean_text((institution or {}).get("country")),
    }


def sorted_contributors(authors, start_date):
    contributors = [contributor_from_author(author, start_date) for author in authors or []]
    return sorted(contributors, key=lambda c: int(c.get("order") or "999999"))


def select_lead(contributors):
    managers = [c for c in contributors if c.get("duty") == "MANAGER"]
    lead = managers[0] if managers else (contributors[0] if contributors else None)
    if lead is None:
        return None, []
    return lead, [c for c in contributors if c is not lead]


def project_quality(row):
    authors = row.get("authors") or []
    abstracts = row.get("abstracts") or []
    return (
        1 if any(a.get("duty") == "MANAGER" for a in authors) else 0,
        1 if any(clean_text(a.get("title")) for a in abstracts) else 0,
        1 if any(clean_text(a.get("abstract")) for a in abstracts) else 0,
        len(authors),
        clean_text(row.get("firstIndexDate")) or "",
        str(row.get("id") or ""),
    )


def dedupe_by_project_number(records):
    groups = defaultdict(list)
    missing = 0
    for record in records:
        number = normalize_project_number(record.get("projectnumber"))
        if not number:
            missing += 1
            continue
        groups[number].append(record)
    deduped = []
    for candidates in groups.values():
        best = sorted(candidates, key=project_quality, reverse=True)[0]
        best["_duplicate_trdizin_ids"] = [clean_text(c.get("_trdizin_hit_id")) or clean_text(c.get("id"))
                                          for c in candidates if c is not best]
        best["_duplicate_source_count"] = len(candidates)
        deduped.append(best)
    deduped.sort(key=lambda r: (int(r.get("publicationYear") or 0), normalize_project_number(r.get("projectnumber")) or ""))
    return deduped, missing


def project_row(record, reported_total, retrieved_at):
    """One deduped TR Dizin record -> the saved file's 44 columns (tubitak_to_s3.normalize_records)."""
    number = normalize_project_number(record.get("projectnumber"))
    abstracts = record.get("abstracts") or []
    subjects = record.get("subjects") or []
    start_date = date_part(record.get("startedDate"))
    contributors = sorted_contributors(record.get("authors") or [], start_date)
    lead, others = select_lead(contributors)
    title_tr = pick_localized(abstracts, "title", "TUR")
    title_en = pick_localized(abstracts, "title", "ENG")
    abstract_tr = pick_localized(abstracts, "abstract", "TUR")
    abstract_en = pick_localized(abstracts, "abstract", "ENG")
    trdizin_id = clean_text(record.get("_trdizin_hit_id")) or int_string(record.get("id"))
    names = [clean_text(s.get("name")) for s in subjects if clean_text(s.get("name"))]
    roots = [clean_text(s.get("rootName")) for s in subjects if clean_text(s.get("rootName"))]
    return {
        "funder_award_id": number,
        "trdizin_id": trdizin_id,
        "display_name": title_tr or title_en or clean_text(record.get("orderTitle")) or f"TUBITAK project {number}",
        "description": abstract_tr or abstract_en,
        "title_tr": title_tr,
        "title_en": title_en,
        "abstract_tr": abstract_tr,
        "abstract_en": abstract_en,
        "keywords_tr": pick_localized(abstracts, "keywords", "TUR"),
        "keywords_en": pick_localized(abstracts, "keywords", "ENG"),
        "project_number": number,
        "project_group": clean_text(record.get("projectGroup")),
        "publication_year": int_string(record.get("publicationYear")),
        "start_date": start_date,
        "end_date": date_part(record.get("endDate")),
        "lead_name": lead.get("name") if lead else None,
        "lead_given_name": lead.get("given_name") if lead else None,
        "lead_family_name": lead.get("family_name") if lead else None,
        "lead_orcid": lead.get("orcid") if lead else None,
        "lead_role": lead.get("duty") if lead else None,
        "contributors_json": json_string(contributors),
        "other_investigators_json": json_string(others),
        "contributor_count": int_string(len(contributors)),
        "subject_names": "; ".join(names) if names else None,
        "subject_roots": "; ".join(sorted(set(roots))) if roots else None,
        "subjects_json": json_string(subjects),
        "access_type": clean_text(record.get("accessType")),
        "language": clean_text(record.get("language")),
        "report_pages": int_string(record.get("endPage")),
        "pdf_id": clean_text(record.get("pdf")),
        "view_count": int_string(record.get("viewCount")),
        "download_count": int_string(record.get("downloadCount")),
        "first_index_date": date_part(record.get("firstIndexDate")),
        "index_date": date_part(record.get("indexDate")),
        "source_doc_type": clean_text(record.get("docType")),
        "source_raw_path": API_URL,   # was the laptop's local JSONL path
        "source_notice_url": SOURCE_NOTICE_URL,
        "search_page_url": SEARCH_PAGE_URL,
        "api_url": API_URL,
        "landing_page_url": f"https://search.trdizin.gov.tr/tr/yayin/detay/{trdizin_id}" if trdizin_id else SEARCH_PAGE_URL,
        "duplicate_trdizin_ids_json": json_string(record.get("_duplicate_trdizin_ids")),
        "duplicate_source_count": int_string(record.get("_duplicate_source_count")),
        "reported_total": int_string(reported_total),
        "retrieved_at": retrieved_at,
    }


# --- fetch ------------------------------------------------------------------------------------------------------------
def page_url(page):
    return API_URL + "?" + urlencode({"q": "", "order": "publicationYear-DESC", "page": page, "limit": PAGE_SIZE,
                                      "facet-documentType": "PROJECT"})


def fetch_records(session, directory):
    """Every PROJECT hit, one page at a time; each page's total must equal the first page's, and no page may be empty."""
    records, hit_ids, expected, pages = [], set(), None, None
    page = 1
    while pages is None or page <= pages:
        check_disk(directory)
        raw = directory / "page.json"
        download(session, page_url(page), raw)
        body = json.loads(raw.read_bytes())
        raw.unlink()
        shards = body.get("_shards") or {}
        if body.get("timed_out") is not False or not shards.get("total") or shards.get("failed") != 0 \
                or shards.get("successful") != shards.get("total"):   # a partial result: its hits and total miss a shard
            raise ValueError(f"TR Dizin page {page}: partial result (timed_out {body.get('timed_out')!r}, shards {shards}); "
                             "refusing to trust its total")
        hits = body.get("hits") or {}
        total = (hits.get("total") or {}).get("value")
        if not isinstance(total, int) or total <= 0:
            raise ValueError(f"TR Dizin page {page}: no hits.total")
        relation = (hits.get("total") or {}).get("relation")
        if relation != "eq":   # "gte" is a lower bound (ES stops counting at 10,000): paging to it would drop the rest
            raise ValueError(f"TR Dizin page {page}: hits.total is {relation!r} {total}, not an exact count; refusing to page by it")
        if expected is None:
            expected, pages = total, math.ceil(total / PAGE_SIZE)
        elif total != expected:
            raise ValueError(f"TR Dizin total moved during the pull ({expected} -> {total}); refusing a mixed snapshot")
        items = hits.get("hits") or []
        if not items:
            raise ValueError(f"TR Dizin page {page} of {pages} is empty")
        for hit in items:
            source = {k: v for k, v in (hit.get("_source") or {}).items() if k in KEEP}
            source["_trdizin_hit_id"] = hit.get("_id")
            hit_ids.add(hit.get("_id"))
            records.append(source)
        page += 1
        time.sleep(PAGE_DELAY)
    if len(hit_ids) != expected:
        raise ValueError(f"TR Dizin: {len(hit_ids)} distinct hits for total {expected} (paging skipped or repeated records)")
    print(f"TR Dizin: {expected} PROJECT hits on {pages} pages", flush=True)
    return records, expected


def build(directory):
    output = directory / "tubitak_trdizin.parquet"
    retrieved_at = datetime.now(timezone.utc).isoformat()
    with http_session() as session:
        session.headers.update({"User-Agent": "OpenAlex-TUBITAK-Ingest/1.0 (+https://openalex.org)",
                                "Accept": "application/json"})
        records, total = fetch_records(session, directory)
    hits = len(records)
    deduped, missing = dedupe_by_project_number(records)
    del records   # the folded duplicates go; rows are built and written 2,000 at a time to keep the dyno's memory low
    if len(deduped) < MIN_ROWS_SHARE * total:
        raise ValueError(f"TÜBİTAK: {len(deduped)} projects for {total} hits (below {MIN_ROWS_SHARE:.0%}); refusing upload")
    schema = TUBITAK_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
        {"measure": "TR Dizin hits.total (documentType=PROJECT, exact)", "value": total,
         "excluded": {"hits without a project number": missing,
                      "duplicate hits folded by project number": hits - missing - len(deduped)}})})
    with pq.ParquetWriter(output, schema) as writer:
        for i in range(0, len(deduped), 2000):
            rows = [project_row(r, total, retrieved_at) for r in deduped[i:i + 2000]]
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    print(f"TÜBİTAK: {len(deduped)} projects from {hits} hits; {missing} without a project number, "
          f"{hits - missing - len(deduped)} duplicate hits folded", flush=True)
    return output


def main():
    run("tubitak_trdizin", build, TUBITAK_SCHEMA)


if __name__ == "__main__":
    main()
