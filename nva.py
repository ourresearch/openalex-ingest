"""NVA (Sikt's national research archive) read from its search API and served as oai_dc records (oxjob #2380).

Norway's university repositories (Brage) and the NORA aggregator closed when their records moved to NVA in 2025.
NVA has an OAI-PMH feed (https://api.nva.unit.no/publication-oai-pmh), but its oai_dc names every person on a record,
author and supervisors alike, as dc:contributor with no dc:creator, and carries no rights statement: a thesis read from
it would have no author, or its supervisors as authors. The search API (https://api.nva.unit.no/search/resources) has
each contributor's role, the files and their licences, the Brage handle and the DOI. So the harvester reads that API for
the endpoints in repositories.NVA_SEARCH_ENDPOINTS and writes each record as the oai_dc record NVA's own feed should serve,
under the same OAI identifier (https://api.nva.unit.no/publication/<id>), so Repo.py parses it with its usual rules.

The endpoint row's pmh_set holds the NVA instance types to read, comma-separated (DegreePhd,DegreeMaster,...).
A window is read by modification date; NVA's search index holds published records only, so a record NVA unpublishes is
never seen again (its OAI feed reports no deletions either).
"""
from datetime import date, timedelta
from time import sleep, time
from types import SimpleNamespace
import logging
import re
import xml.etree.ElementTree as ET

import requests
from sickle.oaiexceptions import NoRecordsMatch

LOGGER = logging.getLogger('openalex-ingest')  # common.py's logger, without importing common (it needs the database)

SEARCH_URL = "https://api.nva.unit.no/search/resources"
USER_AGENT = "OpenAlexHarvester/1.0 (+https://help.openalex.org/how-to/repositories/; mailto:support@openalex.org)"
PAGE_SIZE = 100  # the API's maximum
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"

# NVA instance type -> the info:eu-repo type Repo.py maps (repo_types.py: doctoralthesis, masterthesis, ... ->
# dissertation). The NVA type itself is kept as a second dc:type.
EU_REPO_TYPE = {
    "DegreePhd": "info:eu-repo/semantics/doctoralThesis",
    "ArtisticDegreePhd": "info:eu-repo/semantics/doctoralThesis",
    "DegreeLicentiate": "info:eu-repo/semantics/doctoralThesis",
    "DegreeMaster": "info:eu-repo/semantics/masterThesis",
    "DegreeBachelor": "info:eu-repo/semantics/bachelorThesis",
    "AcademicArticle": "info:eu-repo/semantics/article",
    "AcademicLiteratureReview": "info:eu-repo/semantics/review",
    "AcademicChapter": "info:eu-repo/semantics/bookPart",
    "AcademicMonograph": "info:eu-repo/semantics/book",
    "ReportResearch": "info:eu-repo/semantics/report",
    "ReportWorkingPaper": "info:eu-repo/semantics/workingPaper",
}
# Who wrote it: the Creator role; an artistic doctorate may name its candidate as Artist. Supervisors never.
AUTHOR_ROLES = ("Creator", "Artist")
# lexvo gives ISO 639-3; Repo.py's normalize_language_code maps the three-letter codes (nob, nno, sme ...) and 'und'.
# The one it spells differently: Dutch, which it knows as the bibliographic 'dut'.
LANGUAGE = {"nld": "nl"}
HANDLE = re.compile(r"hdl\.handle\.net/(\d[\d.]*/\d+)")
# Characters XML 1.0 forbids (C0 controls but tab, newline and CR; U+FFFE/U+FFFF; lone surrogates). ElementTree writes
# them without complaint, and the harvester's parser then rejects the record and stops the run: 55 of the 319,472 NVA
# theses carry one, mostly U+0002 pasted into an abstract, and the first harvest died on the first of them (19 Nov).
# The OAI path strips the C0 ones from each response (repositories._XML_BAD_CTRL); here each value is cleaned.
XML_INVALID = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff￾￿]")


def nva_record(hit):
    """What the harvester reads from a sickle Record: header.identifier, header.datestamp, raw."""
    stamp = datestamp(hit)
    return SimpleNamespace(header=SimpleNamespace(identifier=hit["id"], datestamp=stamp), raw=to_oai_record(hit, stamp))


def datestamp(hit):
    """modifiedDate (nanoseconds) at second granularity, as NVA's OAI feed stamps it: 2025-08-26T09:05:00Z."""
    return re.sub(r"\.\d+Z$", "Z", hit.get("modifiedDate") or hit.get("createdDate"))


def _sequence(contributor):
    """Byline position; the API sends it as a string ("10" sorts before "2")."""
    try:
        return int(contributor.get("sequence") or 0)
    except (TypeError, ValueError):
        return 0


def _size(f):
    try:
        return int(f.get("size") or 0)
    except (TypeError, ValueError):
        return 0


def _file_link(hit):
    """The file link of the record's main public PDF, which Taxicab follows to the file (NVA serves files only through
    short-lived presigned URLs). Among the open, unembargoed PDFs: one with a stated version, else the largest (artistic
    PhDs list songbooks and catalogues beside the thesis)."""
    today = date.today().isoformat()
    pdfs = [f for f in hit.get("associatedArtifacts") or []
            if f.get("type") == "OpenFile" and f.get("identifier") and "pdf" in (f.get("mimeType") or "").lower()
            and not (f.get("embargoDate") or "") > today]
    if not pdfs:
        return None
    main = max(pdfs, key=lambda f: (f.get("publisherVersion") is not None, _size(f)))
    return f"{hit['id']}/filelink/{main['identifier']}"


def _handle(hit):
    """The record's landing handle: Brage's own (the one NORA and Brage copies carry), else the one NVA minted."""
    found = [(a.get("sourceName") or "", m.group(1)) for a in hit.get("additionalIdentifiers") or []
             if a.get("type") == "HandleIdentifier" and (m := HANDLE.search(a.get("value") or ""))]
    return next((h for src, h in found if src.startswith("brage@")), found[0][1] if found else None)


def to_oai_record(hit, stamp=None):
    """One NVA search hit -> an OAI-PMH <record> with oai_dc metadata, as an XML string."""
    ed = hit.get("entityDescription") or {}
    ref = ed.get("reference") or {}
    itype = (ref.get("publicationInstance") or {}).get("type")

    rec = ET.Element(f"{{{OAI_NS}}}record")
    header = ET.SubElement(rec, f"{{{OAI_NS}}}header")
    ET.SubElement(header, f"{{{OAI_NS}}}identifier").text = hit["id"]
    ET.SubElement(header, f"{{{OAI_NS}}}datestamp").text = stamp or datestamp(hit)
    if itype:
        ET.SubElement(header, f"{{{OAI_NS}}}setSpec").text = f"resourceTypeGeneral:{itype}"
    dc = ET.SubElement(ET.SubElement(rec, f"{{{OAI_NS}}}metadata"), f"{{{OAI_DC_NS}}}dc")

    def add(tag, value):
        value = XML_INVALID.sub("", str(value)).strip() if value is not None else ""
        if value:
            ET.SubElement(dc, f"{{{DC_NS}}}{tag}").text = value

    add("title", ed.get("mainTitle"))
    people = sorted(ed.get("contributors") or [], key=_sequence)
    for role in AUTHOR_ROLES:
        names = [(c.get("identity") or {}).get("name") for c in people if (c.get("role") or {}).get("type") == role]
        if any(names):
            for name in names:
                add("creator", name)
            break
    for tag in ed.get("tags") or []:
        add("subject", tag)
    add("description", ed.get("abstract"))
    orgs = hit.get("topLevelOrganizations") or []
    if orgs:
        labels = orgs[0].get("labels") or {}
        add("publisher", labels.get("en") or labels.get("nb") or labels.get("nn"))
    pd = ed.get("publicationDate") or {}
    if pd.get("year"):
        parts = [str(pd["year"])]
        if pd.get("month"):
            parts.append(str(pd["month"]).zfill(2))
            if pd.get("day"):
                parts.append(str(pd["day"]).zfill(2))
        add("date", "-".join(parts))
    add("type", EU_REPO_TYPE.get(itype))
    add("type", itype)
    # The main PDF's file link comes first: TaxiCab fetches a record's first 3 URLs (a DOI and the two pages below
    # already fill them). Repo.py types it pdf from the "(application/pdf)" note, so the landing page stays the first
    # html URL, the handle; CreateSuperLocations keeps the link itself out of pdf_url (it answers JSON).
    file_link = _file_link(hit)
    if file_link:
        add("identifier", f"{file_link} (application/pdf)")
    handle = _handle(hit)
    if handle:
        add("identifier", f"https://hdl.handle.net/{handle}")
    add("identifier", f"https://nva.sikt.no/registration/{hit.get('identifier')}")
    if ref.get("doi"):
        add("identifier", ref["doi"] if ref["doi"].startswith("http") else f"https://doi.org/{ref['doi']}")
    for a in hit.get("additionalIdentifiers") or []:
        if a.get("type") == "CristinIdentifier" and a.get("value"):
            add("identifier", f"CRISTIN:{a['value']}")
    lang = (ed.get("language") or "").rsplit("/", 1)[-1]
    if lang:
        add("language", LANGUAGE.get(lang, lang))
    # Rights: Repo.py takes the first creativecommons.org value, else the first value. A record with a public file is
    # open (a Creative Commons licence, else info:eu-repo openAccess, which normalises to other-oa); one without says nothing.
    open_files = [f for f in hit.get("associatedArtifacts") or [] if f.get("type") == "OpenFile"]
    if open_files:
        licences = (f.get("license") for f in open_files)
        licences = (lic.get("value") if isinstance(lic, dict) else lic for lic in licences)
        for lic in dict.fromkeys(lic for lic in licences if lic and "creativecommons.org" in lic):
            add("rights", lic)
        add("rights", "info:eu-repo/semantics/openAccess")
    return ET.tostring(rec, encoding="unicode")


class NvaRecords:
    """Iterator over one modification-date window, the stand-in for sickle's ListRecords result in
    EndpointHarvester.call_pmh_endpoint. The first page is read here, so an empty window raises NoRecordsMatch and
    `total` is known before the walk, as with an OAI feed's completeListSize."""

    ATTEMPTS = 6

    def __init__(self, types, first=None, last=None, deadline=None, deadline_error=TimeoutError, session=None,
                 page_size=PAGE_SIZE):
        params = {"instanceType": types, "aggregation": "none", "size": page_size, "sort": "modified_date:asc"}
        if first is not None:  # None = first harvest: the whole feed
            params["modified_since"] = first.isoformat()
        if last is not None:
            params["modified_before"] = (last + timedelta(days=1)).isoformat()  # exclusive bound; 'last' is inclusive
        self.deadline, self.deadline_error = deadline, deadline_error
        self.http = session or requests.Session()
        self.http.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        self._first_page = self._get(SEARCH_URL, params)
        self.total = self._first_page.get("totalHits")
        if not self._first_page.get("hits"):
            raise NoRecordsMatch(f"NVA search: no {types} records modified in [{first}, {last}]")
        self._gen = self._walk()

    def _left(self):
        return None if self.deadline is None else self.deadline - time()

    def _get(self, url, params):
        for attempt in range(self.ATTEMPTS):
            if (left := self._left()) is not None and left <= 0:
                raise self.deadline_error("NVA search: deadline reached")
            try:
                r = self.http.get(url, params=params, timeout=(30, 120))
                if r.status_code == 200:
                    return r.json()
                err = f"HTTP {r.status_code}: {r.text[:200]}"
                if r.status_code < 500 and r.status_code != 429:
                    raise requests.exceptions.HTTPError(err, response=r)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                err = repr(e)
            if attempt + 1 < self.ATTEMPTS:
                LOGGER.warning(f"NVA search retry {attempt + 1}: {err}")
                wait = 5 * 2 ** attempt
                sleep(wait if (left := self._left()) is None else max(0, min(wait, left)))
        raise requests.exceptions.ConnectionError(f"NVA search: gave up after {self.ATTEMPTS} attempts: {err}")

    def _walk(self):
        page, self._first_page = self._first_page, None
        while True:
            for hit in page.get("hits") or []:
                # A generator that raises is finished, and the harvester's _iter_records_safe would read that as the
                # end of the feed: skip a record that won't convert instead.
                try:
                    record = nva_record(hit)
                except (KeyError, TypeError, AttributeError, ValueError) as e:
                    LOGGER.warning(f"NVA search: skipped {hit.get('id')}: {e!r}")
                    continue
                yield record
            nxt = page.get("nextSearchAfterResults")
            if not page.get("hits") or not nxt:
                return
            page = self._get(nxt, None)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._gen)
