"""FCT SciPROJ (api.ptcris.pt), weekly. Needs SCIPROJ_API_USERNAME and SCIPROJ_API_PASSWORD (HTTP Basic, Heroku config only).

SciPROJ is Portugal's national project registry (FCT/FCCN, CC BY 4.0). It compiles a dump every week:
  GET /services/searches/projects/dump?year=YYYY  lists that year's dump files (getDumpXMLyyyymmdd_PRV|PUB.zip)
  each file is a ZIP of per-project OpenAIRE CERIF 1.1 XML records.
We take the newest file. With OpenAlex's login SciPROJ lists public-profile files only (_PUB; 45 weekly files in 2026 on 11-05);
they carry Person names, ORCIDs and CIENCIA IDs but no e-mail, birth date or gender. A _PRV file (FCCN's 2026-07 e-mail) also holds
those private fields; the parser never reads them, and on the same date a _PUB file is preferred.
The listing answers XML whatever the Accept header says (its dataset: prefix is unbound, so it is read with a pattern), or JSON.
The record parser is walden's laptop loader (scripts/local/fct_to_s3.py, the 2026-08 load) ported, with two changes, both
decided 11-05 (Rohan, via grants-hub: "as published, no fill-ins"):
  - amount is the project total: the sum of the record's <Funded> streams (e.g. FCT's national share + FEDER co-funding),
    where the laptop took the first stream only; a stream in any currency but EUR fails the run;
  - the frozen 2026-06 fct.pt XLSX list (its URL 404s) is no longer merged in.
Added: pi_orcid, pi_ciencia_id, team_json (the project's team members, in SciPROJ's order, without the PI), sciproj_id,
source_dump, ingested_at.
Never read or written: e-mail addresses, birth dates, gender, country codes of people (the private dump holds them).
"""
import json
import logging
import math
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import pyarrow as pa
import sentry_sdk
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import FCT_SCHEMA

API = "https://api.ptcris.pt/services/searches/projects/dump"
HOST = "api.ptcris.pt"
# SciPROJ's WAF refuses python-requests' default agent (Kyle, 11-05); a browser-compatible agent that still names us passes.
USER_AGENT = "Mozilla/5.0 (compatible; OpenAlex-ingest/1.0; +https://openalex.org)"
DUMP_NAME = re.compile(r"^getDumpXML(\d{8})_(PRV|PUB)\.zip$")
MIN_ROWS = 100_000          # 2026-07 dump: 106,773 rows (FCT 98,827, ANI 3,713, EC 3,491, ERDF 742)
MIN_FCT_PI_SHARE = 0.85     # 2026-07 private dump and 2026-11-04 public dump: 92%; a file without Person falls far below
FCT_ROR = "https://ror.org/00snfqn58"
SCIPROJ_LANDING = "https://sciproj.ptcris.pt/"
CIENCIA_ID = "Q122584897"   # Identifier type (wikidata item) of a CIENCIA ID, e.g. B41B-3322-A1EB
_NULLISH = {"", "n/a", "na", "-", "—", "/", "nan", "none"}
NS = {"oai": "http://www.openarchives.org/OAI/2.0/", "c": "https://www.openaire.eu/cerif-profile/1.1/"}
_COUNTRY = {
    "pt": "Portugal", "portugal": "Portugal",
    "gb": "United Kingdom", "reino unido": "United Kingdom",
    "es": "Spain", "espanha": "Spain",
    "us": "United States", "estados unidos": "United States",
    "fr": "France", "frança": "France",
    "it": "Italy", "itália": "Italy",
    "de": "Germany", "alemanha": "Germany",
    "be": "Belgium", "bélgica": "Belgium",
    "nl": "Netherlands", "holanda": "Netherlands", "países baixos": "Netherlands",
    "br": "Brazil", "brasil": "Brazil",
    "ch": "Switzerland", "suíça": "Switzerland",
    "at": "Austria", "áustria": "Austria",
    "se": "Sweden", "suécia": "Sweden",
    "dk": "Denmark", "dinamarca": "Denmark",
    "no": "Norway", "noruega": "Norway",
    "fi": "Finland", "finlândia": "Finland",
    "ie": "Ireland", "irlanda": "Ireland",
    "pl": "Poland", "polónia": "Poland",
    "gr": "Greece", "grécia": "Greece",
}
_SUBJECT_COMMENT = re.compile(r"<!--<Subject[^>]*>([^<]+)</Subject>-->")
_ORCID = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])$", re.I)
_DUMP_ENTRY = re.compile(r'<dump-file\s+name="([^"]+)"\s*>\s*([^<\s]+)\s*</dump-file>')
_TOKEN = re.compile(r"(getFileDump/)[^\s'\"?&]+")


class _RedactToken(logging.Filter):
    """A dump link carries an opaque token in its path; urllib3 logs the URL when it retries. Redact it first."""

    def filter(self, record):
        record.msg = _TOKEN.sub(r"\1***", record.getMessage())   # format first: %d args stay numbers
        record.args = ()
        return True


for _logger in ("urllib3.connectionpool", "urllib3.util.retry", "urllib3", "sentry_sdk.errors"):   # sentry logs URLs in debug mode
    logging.getLogger(_logger).addFilter(_RedactToken())


def _redact_dump_links(event, hint):
    """Sentry keeps each HTTP request as a breadcrumb; sentry_setup strips query strings, but the dump link's token is in
    the path. Any event this process sends gets the token replaced first."""
    for crumb in (event.get("breadcrumbs") or {}).get("values") or []:
        if isinstance(crumb.get("message"), str):
            crumb["message"] = _TOKEN.sub(r"\1***", crumb["message"])
        data = crumb.get("data")
        if isinstance(data, dict):
            for k, v in list(data.items()):
                if isinstance(v, str):
                    data[k] = _TOKEN.sub(r"\1***", v)
    return event


sentry_sdk.get_global_scope().add_event_processor(_redact_dump_links)
OLD_COLUMNS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family", "institution", "amount", "currency",
               "scheme", "start_date_raw", "end_date_raw", "description", "landing_page_url", "funder_key",
               "institution_ror", "institution_country", "funding_type"]


def credentials():
    user, password = os.environ.get("SCIPROJ_API_USERNAME", "").strip(), os.environ.get("SCIPROJ_API_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError("SCIPROJ_API_USERNAME and SCIPROJ_API_PASSWORD are required (SciPROJ production login)")
    return user, password


# --- the laptop parser (fct_to_s3.py), unchanged except where marked ------------------------------------------------
def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def split_pi(name):
    name = clean(name)
    if not name or not re.search(r"[A-Za-zÀ-ÿ]{2}", name):
        return None, None, None
    parts = name.split()
    if len(parts) < 2:
        return name, None, name
    return name, " ".join(parts[:-1]), parts[-1]   # family = last token (PT order)


def norm_country(v):
    v = clean(v)
    if not v:
        return None
    return _COUNTRY.get(v.lower(), v.title())


def funding_type(ref, program):
    r = (ref or "").lower()
    if (r.startswith("sfrh/") or re.search(r"\.(bd|bpd|bl|bi)$", r)
            or (program or "").upper() == "FARH"):
        return "fellowship"
    return "grant"


def classify_funder(proj):
    funder = proj.find(".//c:Funded/c:By/c:OrgUnit", NS)
    if funder is None:
        return "fct"
    ror = funder.findtext("c:RORID", default=None, namespaces=NS)
    name = (funder.findtext("c:Name", default=None, namespaces=NS) or "")
    acr = (funder.findtext("c:Acronym", default=None, namespaces=NS) or "")
    if ror == FCT_ROR or (not ror and not name):
        return "fct"
    if ror == "https://ror.org/01mvsby80" or acr == "ANI":
        return "ani"
    if acr == "EC" or name == "European Commission":
        return "ec"
    if acr.startswith("UE -") or name.startswith("União Europeia"):
        return "erdf"
    return "fct"


def orcid(value):
    """Bare ORCID (0000-0002-1825-0097) from a URL or bare form; None when it is not one."""
    m = _ORCID.search(clean(value) or "")
    return m.group(1).upper() if m else None


def person(el):
    """Name and public ids of one CERIF Person. Never reads ElectronicAddress, Birthdate, Gender or CountryCode."""
    ciencia = None
    for ident in el.findall("c:Identifier", NS):
        if (ident.get("type") or "").rstrip("/").endswith(CIENCIA_ID) and ciencia is None:
            ciencia = clean(ident.text)
    full, given, family = split_pi(el.findtext("c:PersonName", default=None, namespaces=NS))
    return {"person_id": el.get("id"), "name": full, "given": given, "family": family,
            "orcid": orcid(el.findtext("c:ORCID", default=None, namespaces=NS)), "ciencia_id": ciencia}


def parse_cerif(xml_bytes):
    """One SciPROJ CERIF record -> dict, or None if no citable reference (fct_to_s3.py, plus the people fields)."""
    root = ET.fromstring(xml_bytes)
    proj = root.find(".//c:Project", NS)
    if proj is None:
        return None
    funder_key = classify_funder(proj)
    ref = program = None
    for ident in proj.findall("c:Identifier", NS):
        t = (ident.get("type") or "").rsplit("#", 1)[-1]
        if t == "ProjectReference" and ref is None:
            ref = clean(ident.text)
        elif t == "FundingProgram" and program is None:
            program = clean(ident.text)
    if not ref:
        return None
    titles = {}
    for t in proj.findall("c:Title", NS):
        lang = t.get("{http://www.w3.org/XML/1998/namespace}lang") or "und"
        titles[lang] = clean(t.text)
    title = titles.get("pt") or titles.get("en") or next(iter(titles.values()), None)
    # The project total: one <Funded> block per funding stream (2023.16009.ICDT: FCT 37,499.76 + FEDER 212,498.64 =
    # 249,998.40, fct.pt's published total). A stream that is not EUR fails the run; a stream with no readable amount
    # drops the total, so a partial sum is never published as the project's amount.
    amount, total, readable = None, 0.0, True
    for funded in proj.findall("./c:Funded", NS):
        el = funded.find("./c:As/c:Funding/c:Amount", NS)
        if el is None:
            readable = False
            continue
        if (el.get("currency") or "") != "EUR":
            raise ValueError(f"SciPROJ {ref}: a funding stream in {el.get('currency')!r}, not EUR; refusing to sum")
        try:
            value = float(clean(el.text) or "x")
        except ValueError:
            readable = False
            continue
        if not math.isfinite(value):
            readable = False
            continue
        total += value
    if readable and total > 0 and math.isfinite(total):
        amount = f"{total:.2f}"
    # the sum the pre-11-06 parser published when a stream had no <Amount> (counted for the gate; not a column)
    partial = f"{total:.2f}" if not readable and total > 0 and math.isfinite(total) else None
    coord = proj.find(".//c:Consortium/c:Coordinator/c:OrgUnit", NS)
    inst = inst_ror = inst_country = None
    if coord is not None:
        inst = clean(coord.findtext("c:Name", default=None, namespaces=NS))
        inst_ror = clean(coord.findtext("c:RORID", default=None, namespaces=NS))
        inst_country = norm_country(coord.findtext("c:Country", default=None, namespaces=NS))
    pi = proj.find(".//c:Team/c:PrincipalInvestigator/c:Person/c:PersonName", NS)
    pf, pg, pfam = split_pi(pi.text if pi is not None else None)
    subjects = [clean(s) for s in _SUBJECT_COMMENT.findall(xml_bytes.decode("utf-8", "replace"))]
    description = "; ".join(dict.fromkeys(s for s in subjects if s)) or None
    start = clean(proj.findtext("c:StartDate", default=None, namespaces=NS))
    end = clean(proj.findtext("c:EndDate", default=None, namespaces=NS))
    start = start if start and re.match(r"^(19|20)\d{2}-", start) else None
    end = end if end and re.match(r"^(19|20)\d{2}-", end) else None
    # Added: the PI's public ids and the team (members in SciPROJ's order, the PI and repeats left out).
    pi_el = proj.find(".//c:Team/c:PrincipalInvestigator/c:Person", NS)
    lead = person(pi_el) if pi_el is not None else {}
    lead_id, lead_name = lead.get("person_id") or None, (lead.get("name") or "").lower()
    team, seen = [], set()
    for el in proj.findall("./c:Team/c:Member/c:Person", NS):
        p = person(el)
        pid, name = p["person_id"] or None, (p["name"] or "").lower()
        if not name:
            continue
        # the PI again: the same id, or the same name where either side has no id (two ids that differ are two people)
        if (pid and lead_id and pid == lead_id) or (name == lead_name and not (pid and lead_id)):
            continue
        key = pid or name
        if key in seen:
            continue
        seen.add(key)
        team.append(p)
    return {
        "funder_key": funder_key,
        "funder_award_id": ref,
        "title": title,
        "pi_full": pf, "pi_given": pg, "pi_family": pfam,
        "institution": inst,
        "institution_ror": inst_ror,
        "institution_country": inst_country,
        "amount": amount,
        "currency": "EUR" if amount else None,
        "scheme": program,
        "start_date_raw": start,
        "end_date_raw": end,
        "description": description,
        "landing_page_url": SCIPROJ_LANDING,
        "pi_orcid": lead.get("orcid") if pfam else None,
        "pi_ciencia_id": lead.get("ciencia_id") if pfam else None,
        "team_json": json.dumps(team, ensure_ascii=False) if team else None,
        "sciproj_id": proj.get("id"),
        "_partial_sum": partial,
    }


def load_sciproj(zip_path):
    """Every record with a reference, keyed (funder_key, lower(ref)). A repeated key keeps the last record in the
    zip's order (the laptop rule); the count is reported so a change in SciPROJ's packing is visible."""
    recs, skipped, errors, repeats = {}, 0, 0, 0
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.endswith(".xml")]
        for n in names:
            try:
                r = parse_cerif(z.read(n))
            except ET.ParseError:
                errors += 1
                continue
            if r is None:
                skipped += 1
                continue
            key = (r["funder_key"], r["funder_award_id"].lower())
            repeats += key in recs
            recs[key] = r
    print(f"SciPROJ: {len(names)} XML records, kept {len(recs)}, no reference {skipped}, unparseable {errors}, "
          f"repeated keys {repeats}", flush=True)
    if errors > 0.001 * max(len(names), 1):
        raise ValueError(f"SciPROJ: {errors} of {len(names)} records do not parse; refusing upload")
    return recs, {"value": len(names), "excluded": {"no project reference": skipped, "unparseable": errors,
                                                    "repeated key (the last record kept)": repeats}}


def rows_from(sciproj):
    """Every SciPROJ record, with the laptop's funding_type rule."""
    out = []
    for sp in sciproj.values():
        sp["funding_type"] = (funding_type(sp["funder_award_id"], sp.get("scheme"))
                              if sp["funder_key"] == "fct" else "grant")
        out.append(sp)
    return out


# --- the download ----------------------------------------------------------------------------------------------------
def parse_listing(body):
    """(name, url) for every dump file in a /dump?year= answer: XML (what SciPROJ sends) or JSON (what its Swagger shows)."""
    text = body.decode("utf-8-sig") if isinstance(body, bytes) else body
    if text.lstrip().startswith("{"):
        files = ((json.loads(text) or {}).get("publicDataset") or {}).get("dumpFiles") or []
        return [(str(f.get("name") or ""), str(f.get("url") or "")) for f in files]
    if "dump-file" not in text and "public-dataset" not in text:
        raise ValueError("SciPROJ dump listing is neither the XML nor the JSON form; refusing")
    return [(name, url) for name, url in _DUMP_ENTRY.findall(text)]


def pick_dump(files):
    """The newest weekly dump on api.ptcris.pt; on the same date a public-profile (_PUB) file wins over _PRV."""
    best = None
    for name, url in files:
        m = DUMP_NAME.match(name)
        if not m:
            continue
        u = urlparse(url)
        if u.scheme != "https" or u.hostname != HOST:
            raise ValueError("SciPROJ dump link is off-host; refusing")
        rank = (m.group(1), m.group(2) == "PUB")
        if best is None or rank > best[0]:
            best = (rank, name, url)
    return (best[1], best[2]) if best else None


def latest_dump(session, now):
    for year in (now.year, now.year - 1):   # early January the new year may have no file yet
        try:
            response = session.get(API, params={"year": str(year)}, headers={"Accept": "application/json"},
                                   timeout=(15, 120))
        except Exception:
            raise RuntimeError(f"SciPROJ dump listing failed for {year}") from None
        if response.status_code == 404:
            continue
        if response.status_code == 401:
            raise RuntimeError("SciPROJ refused the login (401)")
        response.raise_for_status()
        found = pick_dump(parse_listing(response.content))
        if found:
            return found
    raise RuntimeError("SciPROJ lists no weekly dump this year or last")


def build(directory):
    user, password = credentials()   # before any HTTP
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    raw = directory / "sciproj.zip"
    with http_session() as session:
        session.auth = (user, password)
        session.headers["User-Agent"] = USER_AGENT
        name, url = latest_dump(session, now)
        check_disk(directory)
        try:
            download(session, url, raw)
        except Exception:
            raise RuntimeError(f"SciPROJ download failed for {name}") from None
    print(f"SciPROJ dump {name}: {raw.stat().st_size} bytes", flush=True)
    sciproj, counts = load_sciproj(raw)
    raw.unlink()
    rows = rows_from(sciproj)
    for r in rows:
        r["source_dump"], r["ingested_at"] = name, stamp
    rows.sort(key=lambda r: (r["funder_key"], r["funder_award_id"].lower(), r["funder_award_id"]))
    fct_rows = [r for r in rows if r["funder_key"] == "fct"]
    pi_share = sum(1 for r in fct_rows if r.get("pi_family")) / max(len(fct_rows), 1)
    if len(rows) < MIN_ROWS or pi_share < MIN_FCT_PI_SHARE:
        raise ValueError(f"SciPROJ: {len(rows)} rows, FCT PI share {pi_share:.1%}; expected >= {MIN_ROWS} and "
                         f">= {MIN_FCT_PI_SHARE:.0%} (a dump without Person, or truncated?); refusing upload")
    output = directory / "fct_sciproj.parquet"
    partial = [r for r in rows if r.get("_partial_sum")]
    schema = FCT_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(dict(   # the gate compares rows with it
        measure=f"XML records in the SciPROJ dump {name}", **counts, amount_dropped_partial=len(partial),
        amount_dropped_partial_examples=[[r["funder_award_id"], r["_partial_sum"]] for r in partial[:10]]))})
    table = pa.Table.from_pylist([{c: r.get(c) for c in FCT_SCHEMA.names} for r in rows], schema=schema)
    pq.write_table(table, output)
    people = sum(len(json.loads(r["team_json"])) for r in rows if r.get("team_json"))
    print(f"FCT: {len(rows)} rows ({len(fct_rows)} FCT), PI on {pi_share:.1%} of FCT rows, "
          f"{sum(1 for r in rows if r.get('pi_orcid'))} PI ORCIDs, {people} team members", flush=True)
    return output


def main():
    run("fct_sciproj", build, FCT_SCHEMA)


if __name__ == "__main__":
    main()
