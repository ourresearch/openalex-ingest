"""KAKEN official API, monthly. Requires KAKEN_APPID; no scraper fallback."""
from datetime import datetime
import json
import os
import re
import time
from typing import Optional
from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq
from award_fetch import download, http_session, run, check_disk
from award_schemas import KAKEN_SCHEMA

API_URL = "https://kaken.nii.ac.jp/opensearch/"
ROWS_PER_REQUEST = 500
FIRST_FISCAL_YEAR = 1964


def get_appid():
    appid = os.environ.get("KAKEN_APPID", "").strip()
    if not appid:
        raise RuntimeError("KAKEN_APPID is required; register a CiNii Web API application ID")
    return appid


def _lang_pick(elements, lang: str = "en") -> Optional[str]:
    """Pick text of the element in the requested language, else the first."""
    if not elements:
        return None
    for el in elements:
        if el.get("{http://www.w3.org/XML/1998/namespace}lang") == lang:
            text = (el.text or "").strip()
            if text:
                return text
    text = (elements[0].text or "").strip()
    return text or None

def _txt(parent, path: str) -> Optional[str]:
    el = parent.find(path)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()
    return None

def _member_names(m) -> tuple:
    """(family, given) from a <member>'s personalName."""
    name_el = m.find("personalName")
    if name_el is None:
        return None, None
    family = _txt(name_el, "familyName")
    given = _txt(name_el, "givenName")
    full = _txt(name_el, "fullName")
    if full and not (family or given):
        parts = full.split()
        if len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        return full, None
    return family, given

def parse_grant_award(ga) -> Optional[dict]:
    """Map one <grantAward> element to a parquet row (scraper-compatible).

    Live-response shape (validated against 20H00001, 2026-07-07): the
    Japanese <summary> is the master record (all members, periodOfAward,
    keywordList, overallAwardAmount); the English <summary> is a sparse
    overlay carrying romanized names and translated titles/categories.
    Merge accordingly: display strings prefer EN, structure comes from JA.
    """
    award_number = ga.get("awardNumber")
    if not award_number:
        return None

    XMLLANG = "{http://www.w3.org/XML/1998/namespace}lang"
    summaries = ga.findall("summary")
    if not summaries:
        return None
    en = next((s for s in summaries if s.get(XMLLANG) == "en"), None)
    ja = next((s for s in summaries if s.get(XMLLANG) == "ja"), None)
    master = ja if ja is not None else (en if en is not None else summaries[0])
    display = en if en is not None else master

    def _from_both(path):
        """Element from the display (EN) summary, falling back to master."""
        el = display.find(path)
        if el is None or (el.text is not None and not el.text.strip() and len(el) == 0):
            fb = master.find(path)
            if fb is not None:
                return fb
        return el if el is not None else master.find(path)

    title = _txt(display, "title") or _txt(master, "title")
    # EN category text is English ("Grant-in-Aid for ...") — required by the
    # downstream funding_type CASE mapping in CreateKAKENAwards.
    category = _txt(display, "category") or _txt(master, "category")

    # Period (JA summary only in live responses)
    period = master.find("periodOfAward")
    if period is None and en is not None:
        period = en.find("periodOfAward")
    start_date = end_date = None
    if period is not None:
        start_date = _txt(period, "startDate")
        end_date = _txt(period, "endDate")
        if not start_date:
            fy = _txt(period, "startFiscalYear") or period.get("searchStartFiscalYear")
            if fy:
                start_date = f"{fy}-04-01"
        if not end_date:
            fy = _txt(period, "endFiscalYear") or period.get("searchEndFiscalYear")
            if fy:
                end_date = f"{fy}-03-31"

    # Budget: overall totals; keep direct/indirect as additive columns
    amount = amount_direct = amount_indirect = None
    overall = master.find("overallAwardAmount")
    if overall is None and en is not None:
        overall = en.find("overallAwardAmount")
    if overall is not None:
        def _num(tag):
            v = _txt(overall, tag)
            if v:
                v = re.sub(r"[^\d.]", "", v)
                return float(v) if v else None
            return None
        amount = _num("totalCost") or _num("convertedJpyTotalCost")
        amount_direct = _num("directCost")
        amount_indirect = _num("indirectCost")

    # Members: roster from the master (JA) summary; romanized names overlaid
    # from the EN summary by eradCode/researcherNumber (fallback: sequence).
    def _member_key(m):
        return m.get("researcherNumber") or m.get("eradCode") or f"seq{m.get('sequence')}"

    en_names = {}
    if en is not None:
        for m in en.findall("member"):
            en_names[_member_key(m)] = _member_names(m)

    pi_given = pi_family = pi_affil = pi_nrid = None
    members = []
    for m in master.findall("member"):
        role = m.get("role")
        # researcher number: @researcherNumber in the schema, but live records
        # often carry it only as @eradCode (same 8-digit KAKEN number).
        rn = m.get("researcherNumber") or m.get("eradCode")
        family, given = _member_names(m)
        en_fam, en_giv = en_names.get(_member_key(m), (None, None))
        # Prefer romanized names (matches the scraper's /en/ page output).
        family = en_fam or family
        given = en_giv or given
        affil_bits = []
        aff = m.find("affiliation")
        for tag in ("institution", "department", "jobTitle"):
            # affiliation/* is current; direct member/* children are the
            # deprecated-but-still-populated fallback.
            t = (_txt(aff, tag) if aff is not None else None) or _txt(m, tag)
            if t:
                affil_bits.append(t)
        affiliation = ", ".join(affil_bits) or None
        inst_code = None
        for el in ((aff.find("institution") if aff is not None else None), m.find("institution")):
            if el is not None and el.get("niiCode"):
                inst_code = el.get("niiCode")
                break
        members.append({
            "role": role,
            "family_name": family,
            "given_name": given,
            "researcher_number": rn,
            "erad_code": m.get("eradCode"),
            "affiliation": affiliation,
            "institution_nii_code": inst_code,
        })
        if role == "principal_investigator" and pi_family is None:
            pi_family, pi_given, pi_affil = family, given, affiliation
            # NRID = '1000' + researcher number zero-padded to 9 digits
            # (matches the nrid.nii.ac.jp URLs the scraper harvested, e.g.
            # researcher 50287950 -> NRID 1000050287950).
            pi_nrid = f"1000{rn.zfill(9)}" if rn else None

    # Institution (project host): EN display name, niiCode from either
    institution = None
    institution_nii_code = None
    for s in (display, master):
        el = s.find("institution")
        if el is not None:
            if institution is None and el.text and el.text.strip():
                institution = el.text.strip()
            if institution_nii_code is None:
                institution_nii_code = el.get("niiCode")

    # Keywords (JA summary only in live responses)
    keywords = None
    for s in (display, master):
        kws = [
            (k.text or "").strip()
            for k in s.findall("keywordList/keyword")
            if k.text and k.text.strip()
        ]
        if kws:
            keywords = ", ".join(kws)
            break

    # Abstract: paragraphList typed purpose/abstract/achievement, EN preferred
    abstract = None
    for s in (display, master):
        for want in ("abstract", "purpose", "outline", "achievement"):
            for pl in s.findall("paragraphList"):
                if (pl.get("type") or "").lower().startswith(want):
                    texts = [
                        (p.text or "").strip()
                        for p in pl.findall("paragraph")
                        if p.text and p.text.strip()
                    ]
                    if texts:
                        abstract = "\n".join(texts)
                        break
            if abstract:
                break
        if abstract:
            break

    # productListEnriched = KAKEN's own (funder-side) identifier resolution.
    # Index enriched DOIs by <ref id> so DOI-less self-reports can be
    # backfilled, explicitly labeled enriched_doi (provenance stays visible).
    enriched_doi = {}
    ple = ga.find("productListEnriched")
    if ple is not None:
        for p in ple.findall("product"):
            ref = p.find("ref")
            doi = _txt(p, "doi")
            if ref is not None and ref.get("id") and doi:
                enriched_doi[ref.get("id")] = doi

    # Research products (self-reported grant -> output links). Superset of the
    # scraper's {type,title,year,doi,naid}; all values strings so the junction
    # notebook's array<map<string,string>> parse keeps working.
    products = []
    pl_el = ga.find("productList")
    for p in (pl_el.findall("product") if pl_el is not None else []):
        def _ident(tag):
            el = p.find(tag)
            if el is None or not (el.text or "").strip():
                return None, None
            return el.text.strip(), el.get("authenticated")
        doi, doi_auth = _ident("doi")
        naid, _ = _ident("naid")
        pmid, _ = _ident("pmid")
        issn, _ = _ident("issn")
        prod = {
            "type": p.get("type"),
            "title": _lang_pick(p.findall("title")),
            "year": _txt(p, "year"),
            "doi": doi,
            "naid": naid,
            "pmid": pmid,
            "issn": issn,
            "doi_authenticated": doi_auth,
            "journal_title": _lang_pick(p.findall("journalTitle")),
            "reviewed": p.get("reviewed"),
            "open_access": p.get("openAccess"),
            "acknowledgement": p.get("acknowledgement"),
        }
        if not doi and p.get("id") in enriched_doi:
            prod["enriched_doi"] = enriched_doi[p.get("id")]
        if any(prod.get(k) for k in ("title", "doi", "naid", "pmid")):
            products.append({k: v for k, v in prod.items() if v is not None})

    return {
        "project_id": award_number,
        "title": title,
        "abstract": abstract,
        "category": category,
        "start_date": start_date,
        "end_date": end_date,
        "amount": amount,
        "currency": "JPY",
        "pi_given_name": pi_given,
        "pi_family_name": pi_family,
        "pi_affiliation": pi_affil,
        "pi_nrid": pi_nrid,
        "institution": institution,
        "keywords": keywords,
        "products_json": json.dumps(products, ensure_ascii=False) if products else None,
        "landing_page_url": f"https://kaken.nii.ac.jp/grant/KAKENHI-PROJECT-{award_number}/",
        # -- additive columns (not consumed by current notebooks) --
        "amount_direct": amount_direct,
        "amount_indirect": amount_indirect,
        "project_type": ga.get("projectType"),
        "project_status": (
            master.find("projectStatus").get("statusCode")
            if master.find("projectStatus") is not None else None
        ),
        "institution_nii_code": institution_nii_code,
        "members_json": json.dumps(members, ensure_ascii=False) if members else None,
    }

def strip_namespaces(root) -> None:
    """Drop any default XML namespace in place so element paths stay simple.

    The response envelope is not publicly documented (format=xml is
    appid-gated), so tolerate both namespaced and plain markup. xml:lang
    attributes are unaffected.
    """
    for el in root.iter():
        if isinstance(el.tag, str) and el.tag.startswith("{"):
            el.tag = etree.QName(el).localname
    etree.cleanup_namespaces(root)

def parse_page(path):
    root = etree.parse(str(path), etree.XMLParser(resolve_entities=False, no_network=True)).getroot()
    strip_namespaces(root)
    total = root.get("totalResults")
    if total is None:
        total = next((el.text for el in root.iter("totalResults")), None)
    if total is None or not total.strip().isdigit():
        raise ValueError("KAKEN response lacks totalResults; refusing incomplete harvest")
    awards = list(root.iter("grantAward"))
    rows = [parse_grant_award(ga) for ga in awards]
    if any(row is None or not row["project_id"].strip() for row in rows):
        raise ValueError("Invalid KAKEN award record")
    return int(total), rows


def build(directory):
    appid = get_appid()  # Before HTTP, S3, or raw-file creation.
    output = directory / "kaken.parquet"
    seen = set()
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with http_session() as session, pq.ParquetWriter(output, KAKEN_SCHEMA) as writer:
        session.headers["User-Agent"] = "OpenAlex-KAKEN-Ingest/2.0 (contact@openalex.org)"
        for year in range(FIRST_FISCAL_YEAR, datetime.utcnow().year + 1):
            start, expected, count = 1, None, 0
            while True:
                check_disk(directory)
                time.sleep(1)
                raw = directory / "page.xml"
                # Never log this URL or HTTP exceptions, which may contain appid.
                import requests
                url = requests.Request("GET", API_URL, params={"appid": appid, "format": "xml",
                    "lang": "en", "s1": year, "s2": year, "o1": 1,
                    "rw": ROWS_PER_REQUEST, "st": start}).prepare().url
                try:
                    download(session, url, raw)
                except Exception:
                    raise RuntimeError(f"KAKEN download failed for year {year}, start {start}") from None
                total, rows = parse_page(raw)
                if expected is None:
                    expected = total
                if total != expected or len(rows) > ROWS_PER_REQUEST or count + len(rows) > expected:
                    raise ValueError("KAKEN pagination total changed or overflowed")
                count += len(rows)
                unique = []
                for row in rows:
                    if row["project_id"] not in seen:
                        seen.add(row["project_id"])
                        row["ingested_at"] = stamp
                        # API enrichments are outside the measured 17-column prod contract.
                        extras = {"amount_direct", "amount_indirect", "project_type",
                                  "project_status", "institution_nii_code", "members_json"}
                        if set(row) != set(KAKEN_SCHEMA.names) | extras:
                            raise ValueError("KAKEN output columns changed")
                        unique.append({name: row[name] for name in KAKEN_SCHEMA.names})
                if unique:
                    writer.write_table(pa.Table.from_pylist(unique, schema=KAKEN_SCHEMA))
                check_disk(directory)
                raw.unlink()
                if count == expected:
                    break
                if len(rows) != ROWS_PER_REQUEST or not unique:
                    raise ValueError("KAKEN truncated or repeated page")
                start += ROWS_PER_REQUEST
            print(f"KAKEN {year}: {count} records", flush=True)
    return output


def main():
    run("kaken", build, KAKEN_SCHEMA)


if __name__ == "__main__":
    main()
