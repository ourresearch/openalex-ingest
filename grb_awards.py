"""Taiwan NSTC grants from the Government Research Bulletin's open per-year XML files (GRB), monthly. No key needed.

GRB (政府研究資訊系統, run by NIAR/STPI for the Executive Yuan) publishes one XML file per ROC year, listed in its open-data
catalogue (data.gov.tw dataset 18707, Government Open Data License v1) through tinyurl links to
https://manager.grb.gov.tw/GRB_{ROC year}_xml.zip. The fetcher downloads those files directly: the catalogue host
(mas.nstc.gov.tw) refuses OpenSSL 3 clients (handshake failure, 2026-11-05), and tinyurl adds nothing but a hop.
Every file holds every agency's projects for that year; this fetcher keeps the National Science and Technology Council's
own plans (plan organ 國家科學及技術委員會(本會), walden's grb_nstc_projects), which exist from ROC 111 (2022) on.

Each run reads every NSTC year (ROC 111 to the current ROC year), one zip at a time, and aggregates the annual
rows into one row per cited plan number, as walden's laptop script did (scripts/local/nstc_grb_to_s3.py, the GRB export):
  funder_award_id  the plan number with spaces removed, upper case (e.g. NSTC115-2320-B303-001); else the GRB system number
                   (PROJKEY), else GRB-<id>
  amounts          本期經費(千元) x 1000, summed over the plan's annual rows (TWD)
  years            first ROC period start / last period end, as Gregorian years
  people           the PI, and the researchers (研究人員) as printed, in order; a Latin name with a space is split into
                   given + family as the laptop script did; any other name (Chinese) stays whole in family_name
                   (walden's mapping splits Chinese names)
  first non-empty value per column, in (start year, period start, system number) order, for titles and the rest
The open files carry no abstract text (only a link to the detail page), so there is no description column.
Checks: only the current year may be unpublished (GRB answers an unpublished year with an empty 200); each zip must hold
exactly GRB_<year>.xml; a past year with fewer NSTC rows than its floor, an unparsable row or a row without an id refuses
the upload. A current-year file that disappears shrinks the output, which award_fetch's shrink guard refuses.
The parquet footer (key openalex.funder_total) records GRB's own count, the NSTC annual rows read, and as folded the annual
rows merged into their plan's row (annual rows minus the distinct plan numbers seen while reading, so a plan lost before
writing shows as unexplained), with the rows per year (NSTC and all agencies), for the gate. No annual row goes unwritten:
one without a plan number is keyed on its system number or GRB id, and one without an ID refuses the upload.
"""
import json
import re
import zipfile
from datetime import datetime, timezone

from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import GRB_SCHEMA

FILE_URL = "https://manager.grb.gov.tw/GRB_{year}_xml.zip"
FIRST_YEAR = 111                       # ROC 111 = 2022: NSTC replaced MOST on 2022-07-27
NSTC_ORGAN = "國家科學及技術委員會(本會)"
MIN_PAST_YEAR_ROWS = 5000              # NSTC rows in a full past year (measured 2026-11-05: 112-114 have 13-15k)
MIN_FIRST_YEAR_ROWS = 1000             # ROC 111 holds only NSTC's first five months (1,445 rows)
MAX_XML_BYTES = 512 * 1024 ** 2        # one year's XML is ~35 MB
FIELDS = ["ID", "PROJKEY", "PLAN_NO", "PNCH_DESC", "PENG_DESC", "EXCU_ORGAN_NAME", "PLAN_YEAR", "PLAN_ORGAN_CODE",
          "RESEARCH_TYPE", "RESEARCH_ATTRIBUTE", "RESEARCH_FIELD", "PERIOD_STYM", "PERIOD_ENYM", "PLAN_AMT", "PI",
          "RESEARCHER", "KEYWORD_C", "KEYWORD_E"]

SPACE_RE = re.compile(r"\s+")
DELIMITER_RE = re.compile(r"\s*[;；、,，/]\s*")
LATIN_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z .,'-]+$")
SUFFIX_RE = re.compile(r"\b(?:Ph\.?D\.?|MD|M\.?D\.?|Dr\.?|Prof\.?|Jr\.?|Sr\.?|II|III|IV)\b\.?", re.IGNORECASE)


# --- the laptop script's normalisation, unchanged (scripts/local/nstc_grb_to_s3.py) ------------------------------------
def clean_text(value):
    if value is None:
        return None
    text = SPACE_RE.sub(" ", str(value).replace("\r", " ").replace("\n", " ")).strip()
    if not text or text.lower() in {"none", "null", "nan", "-"}:
        return None
    return text


def normalize_award_id(value):
    text = clean_text(value)
    return SPACE_RE.sub("", text).upper() if text else None


def roc_year(value, now_year):
    text = clean_text(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 3:
        return None
    year = int(digits[:3]) + 1911
    return None if year < 1900 or year > now_year + 1 else str(year)


def amount_twd(value):
    text = clean_text(value)
    number = re.sub(r"[^0-9.\-]", "", text) if text else ""
    if not number:
        return None
    try:
        twd = float(number) * 1000.0
    except ValueError:
        return None
    return twd if twd > 0 else None


def split_latin_name(name):
    text = SPACE_RE.sub(" ", SUFFIX_RE.sub("", name)).strip(" ,")
    if not text:
        return None, None
    parts = text.split()
    return (None, parts[0]) if len(parts) == 1 else (" ".join(parts[:-1]), parts[-1])


def person(name):
    """One printed name -> {name, given_name, family_name}; Chinese names stay whole in family_name, as in the laptop file."""
    text = clean_text(name)
    if not text:
        return None
    given, family = split_latin_name(text) if LATIN_NAME_RE.match(text) and " " in text else (None, text)
    return {"name": text, "given_name": given, "family_name": family}


def split_people(value):
    text = clean_text(value)
    if not text:
        return []
    return [p for p in (person(part) for part in DELIMITER_RE.split(text)) if p]


# --- the open files --------------------------------------------------------------------------------------------------
def published(path):
    """GRB answers a year it has not published with HTTP 200 and an empty body."""
    with path.open("rb") as handle:
        head = handle.read(4)
    if not head:
        return False
    if head != b"PK\x03\x04":
        raise ValueError(f"{path.name}: not a zip ({head!r}); refusing a partial harvest")
    return True


def read_year(path, year):
    """One GRB_<year>_xml.zip -> (rows in the file, NSTC rows as dicts of the FIELDS)."""
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if names != [f"GRB_{year}.xml"]:
            raise ValueError(f"GRB {year}: zip holds {names}, expected GRB_{year}.xml")
        if archive.getinfo(names[0]).file_size > MAX_XML_BYTES:
            raise ValueError(f"GRB {year}: XML larger than {MAX_XML_BYTES} bytes")
        total, nstc = 0, []
        with archive.open(names[0]) as handle:
            parser = etree.iterparse(handle, events=("end",), tag="row", resolve_entities=False, no_network=True,
                                     huge_tree=True)
            for _, element in parser:
                total += 1
                record = {name: element.findtext(name) for name in FIELDS}
                element.clear()
                if not (record["ID"] or "").strip().isdigit():
                    raise ValueError(f"GRB {year}: row {total} has no numeric ID")
                if clean_text(record["PLAN_ORGAN_CODE"]) == NSTC_ORGAN:
                    nstc.append(record)
    if not total:
        raise ValueError(f"GRB {year}: no rows")
    return total, nstc


def annual_row(record, now_year):
    grb_id = normalize_award_id(record["ID"])
    plan_no = normalize_award_id(record["PLAN_NO"])
    projkey = normalize_award_id(record["PROJKEY"])
    title_zh, title_en = clean_text(record["PNCH_DESC"]), clean_text(record["PENG_DESC"])
    key = plan_no or projkey or f"GRB-{grb_id}"
    return {
        "funder_award_id": key, "plan_no": plan_no, "projkey": projkey, "grb_id": grb_id,
        "display_name": title_en or title_zh or f"NSTC project {key}", "title_zh": title_zh, "title_en": title_en,
        "executing_institution": clean_text(record["EXCU_ORGAN_NAME"]), "plan_year_roc": clean_text(record["PLAN_YEAR"]),
        "research_type": clean_text(record["RESEARCH_TYPE"]), "research_attribute": clean_text(record["RESEARCH_ATTRIBUTE"]),
        "research_field": clean_text(record["RESEARCH_FIELD"]),
        "period_start_roc_ym": clean_text(record["PERIOD_STYM"]), "period_end_roc_ym": clean_text(record["PERIOD_ENYM"]),
        "start_year": roc_year(record["PERIOD_STYM"], now_year) or roc_year(record["PLAN_YEAR"], now_year),
        "end_year": roc_year(record["PERIOD_ENYM"], now_year),
        "amount": amount_twd(record["PLAN_AMT"]), "pi": person(record["PI"]), "researchers": split_people(record["RESEARCHER"]),
        "keywords_zh": clean_text(record["KEYWORD_C"]), "keywords_en": clean_text(record["KEYWORD_E"]),
        "landing_page_url": f"https://www.grb.gov.tw/search/planDetail?id={grb_id}",
    }


def _order(row):
    # pandas sort_values(["funder_award_id", "start_year", "period_start_roc_ym", "system_number"], na_position="last")
    return tuple(x for v in (row["start_year"], row["period_start_roc_ym"], row["projkey"]) for x in (v is None, v or ""))


def aggregate(rows, stamp):
    """Annual rows -> one row per funder_award_id, sorted by it (the laptop file's aggregate_awards)."""
    groups = {}
    for row in rows:
        groups.setdefault(row["funder_award_id"], []).append(row)
    out = []
    for key in sorted(groups):
        group = sorted(groups[key], key=_order)
        first = lambda col: next((r[col] for r in group if r[col] is not None), None)
        values = lambda col: [r[col] for r in group if r[col] is not None]
        amounts = values("amount")
        total = sum(amounts) if amounts else None
        with_people = next((r for r in group if r["researchers"]), None)
        out.append({
            "funder_award_id": key, "plan_no": first("plan_no"), "projkey": first("projkey"), "grb_id": first("grb_id"),
            "grb_ids": list(dict.fromkeys(values("grb_id"))), "display_name": first("display_name"),
            "title_zh": first("title_zh"), "title_en": first("title_en"),
            "executing_institution": first("executing_institution"),
            "plan_year_roc": min(values("plan_year_roc"), default=None),
            "research_type": first("research_type"), "research_attribute": first("research_attribute"),
            "research_field": first("research_field"),
            "period_start_roc_ym": min(values("period_start_roc_ym"), default=None),
            "period_end_roc_ym": max(values("period_end_roc_ym"), default=None),
            "start_year": min(values("start_year"), default=None), "end_year": max(values("end_year"), default=None),
            "amount": float(int(total)) if total and total > 0 else None, "currency": "TWD" if total and total > 0 else None,
            "pi": first("pi"), "researchers": with_people["researchers"] if with_people else [],
            "researchers_institution": with_people["executing_institution"] if with_people else None,
            "keywords_zh": first("keywords_zh"), "keywords_en": first("keywords_en"),
            "landing_page_url": first("landing_page_url"), "source_row_count": len(group), "ingested_at": stamp,
        })
    return out


def build(directory):
    output = directory / "grb_nstc.parquet"
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    rows, plans = [], set()            # plans: the plan numbers seen while reading, tallied apart from aggregate()
    current = now.year - 1911
    years, file_rows, nstc_rows = [], {}, {}
    with http_session() as session:
        session.headers["User-Agent"] = "OpenAlex-GRB-Ingest/1.0 (+https://openalex.org)"
        for year in range(FIRST_YEAR, current + 1):
            check_disk(directory)
            path = directory / f"GRB_{year}_xml.zip"
            download(session, FILE_URL.format(year=year), path)
            if not published(path):
                path.unlink()
                if year == current:
                    print(f"GRB {year}: not published yet", flush=True)
                    continue
                raise ValueError(f"GRB {year}: past year not published (empty file); refusing a partial harvest")
            total, nstc = read_year(path, year)
            path.unlink()
            years.append(year)
            file_rows[str(year)], nstc_rows[str(year)] = total, len(nstc)
            print(f"GRB {year}: {total} rows, {len(nstc)} NSTC", flush=True)
            floor = MIN_FIRST_YEAR_ROWS if year == FIRST_YEAR else MIN_PAST_YEAR_ROWS
            if year < current and len(nstc) < floor:
                raise ValueError(f"GRB {year}: {len(nstc)} NSTC rows in a past year (< {floor}); refusing upload")
            annual = [annual_row(r, now.year) for r in nstc]
            plans.update(r["funder_award_id"] for r in annual)
            rows.extend(annual)
    awards = aggregate(rows, stamp)
    annual_rows = sum(nstc_rows.values())
    schema = GRB_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
        {"measure": f"NSTC annual rows (plan organ {NSTC_ORGAN}) in GRB's open per-year XML files, before folding into plans",
         "value": annual_rows,
         "folded": {"a plan's other annual rows, merged into the plan's row": annual_rows - len(plans)},
         "nstc_annual_rows_by_roc_year": nstc_rows, "all_agencies_rows_by_roc_year": file_rows})})
    pq.write_table(pa.Table.from_pylist(awards, schema=schema), output)
    print(f"GRB NSTC: {len(rows)} annual rows from ROC {years[0]}-{years[-1]} -> {len(awards)} awards", flush=True)
    return output


def main():
    run("grb_nstc", build, GRB_SCHEMA)


if __name__ == "__main__":
    main()
