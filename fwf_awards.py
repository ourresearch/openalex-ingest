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
later.
Outputs (oxjob #1765): the `output` index is harvested in the same load: both indexes' updatedAt are read at the start
and the end of the crawl and must match. A project's outputs are the union of what either side lists (a project's
_list.connected.output, an output's _list.connected.projects), each with what walden's CreateFWFWorkAwards matches on:
DOI, PMID, title and the authors' surnames (at most MAX_SURNAMES). Output ids change with every rebuild,
so links are joined inside the load and never kept across loads. A link to a project or an output the load doesn't hold
is counted in the footer (FWF sub-project ids like projects-F81 land there). Trouble on the output side alone (the index
missing, empty or rebuilt, or more than MAX_UNKNOWN_LINKS) withholds the outputs: the column is NULL and the footer says
why, so the weekly projects refresh still lands and walden keeps last week's links. A rebuilt projects index refuses.
Further funding is a separate index and is not read here.
"""
import json
import re
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, download, http_session, run
from award_schemas import FWF_OUTPUT, FWF_SCHEMA

API = "https://openapi.fwf.ac.at"
KEY_URL = f"{API}/fwfkey/"
PAGE_SIZE = 1000           # the documented page size
PAGE_DELAY = 1.0           # seconds between requests; FWF states no limit and asks for no "mass access"
CODE = re.compile(r"^[A-Z]{1,5}[0-9]{1,8}$")
ORCID = re.compile(r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$")
MAX_BAD_CODES = 0.005      # records whose code can't be read; more than this refuses the upload
MAX_UNKNOWN_LINKS = 0.01   # links naming a project or an output this load doesn't hold; more than this withholds the outputs
MAX_SURNAMES = 50          # per output (consortium papers list ~3K authors; walden skips the author check on big teams)
INITIALS = re.compile(r"^[A-Z][A-Z.-]{0,4}$")   # the initials after a surname: "Heid E", "Xu N-L"
DOI = re.compile(r"^10\.[0-9]{4,9}/\S+$")
PMID = re.compile(r"^[0-9]{1,9}$")


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


def _doi(value):
    value = (_clean(value) or "").lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "doi:"):
        value = value.removeprefix(prefix)
    return value if DOI.match(value) else None


def _pmid(value):
    value = _clean(value if not isinstance(value, int) else str(value)) or ""
    return value if PMID.match(value) else None


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


def surnames(authors):
    """FWF author strings -> the surname part of each, deduplicated, at most MAX_SURNAMES (walden folds and compares them).
    "Bergmeister, Stefan" -> Bergmeister; "Heid E" -> Heid; "Von Haeseler A" -> Von Haeseler. Without a comma or initials
    the order is unknown ("Thomas Müller", "Müller Thomas"), so the first and the last word both count."""
    out = []
    for name in authors or []:
        words = name.split()
        if not words:
            continue
        if "," in name:
            parts = [name.split(",", 1)[0].strip()]
        elif len(words) > 1 and INITIALS.match(words[-1]):
            parts = [" ".join(words[:-1])]
        else:
            parts = [words[0], words[-1]]
        for part in parts:
            if part and part not in out:
                out.append(part)
        if len(out) >= MAX_SURNAMES:
            return out[:MAX_SURNAMES]
    return out or None


def parse_output(record):
    """One output record (its id is checked by harvest) -> (the fields walden matches on, the project API ids it lists)."""
    return {
        "output_id": record["id"],
        "category": _clean(record.get("_str.category")),
        "type": _clean(record.get("_str.type")),
        "doi": _doi(record.get("_str.doi")),
        "pmid": _pmid(record.get("_str.pmid")),
        "title": _clean(record.get("_str.title")),
        "surnames": surnames(_list(record.get("_list.author"))),
    }, _list(record.get("_list.connected.projects")) or []


def _updated_at(session, directory):
    """{"projects": stamp, "output": stamp or None}: the projects index must be listed; a missing output index only withholds."""
    body = _get_json(session, directory, f"{API}/indexes")
    stamps = {i.get("uid"): i.get("updatedAt") for i in body.get("results") or []}
    if not stamps.get("projects"):
        raise ValueError("FWF: /indexes lists no projects index")
    return {"projects": stamps["projects"], "output": stamps.get("output") or None}


def harvest(session, directory, index):
    """Yield (the listing's total, one page of records), each page checked against the total; build() checks the stamps."""
    expected, offset, seen = None, 0, set()
    while expected is None or offset < expected:
        check_disk(directory)
        time.sleep(PAGE_DELAY)
        body = _get_json(session, directory, f"{API}/indexes/{index}/documents?limit={PAGE_SIZE}&offset={offset}")
        total, records = body.get("total"), body.get("results") or []
        if expected is None:
            if not total:
                raise ValueError(f"FWF {index}: empty listing; refusing an incomplete harvest")
            expected = total
        if total != expected:
            raise ValueError(f"FWF {index}: total moved during the harvest ({expected} -> {total}); rerun")
        ids = [r.get("id") for r in records]
        if len(records) != min(PAGE_SIZE, expected - offset) or None in ids or len(set(ids)) != len(ids) or seen & set(ids):
            raise ValueError(f"FWF {index}: page at offset {offset} short or repeated; rerun")
        seen.update(ids)
        yield expected, records
        offset += PAGE_SIZE
    if len(seen) != expected:
        raise ValueError(f"FWF {index}: {len(seen)} records for total {expected}")


OUTPUT_ORDER = [(f, "ascending") for f in ("doi", "pmid", "title", "output_id")]   # content first: reruns of one load agree
PROJECT_SCHEMA = pa.schema([f for f in FWF_SCHEMA if f.name not in ("outputs", "output_index_updated_at")])


def outputs_column(codes, links, outputs):
    """The outputs list column for projects in `codes` order. links {code: [output id]}; outputs: a table of FWF_OUTPUT
    fields, one row per output. Each page was turned into Arrow as it arrived, so no Python object per output is kept."""
    outputs = outputs.sort_by(OUTPUT_ORDER, null_placement="at_start")
    row_of = {oid: i for i, oid in enumerate(outputs.column("output_id").to_pylist())}
    take, offsets, valid = [], [0], []
    for code in codes:
        rows = sorted(row_of[o] for o in links.get(code, ()))
        take += rows
        offsets.append(len(take))
        valid.append(bool(rows))
    listed = outputs.take(pa.array(take, type=pa.int64()))
    struct = pa.StructArray.from_arrays([listed.column(f.name).combine_chunks() for f in FWF_OUTPUT], fields=list(FWF_OUTPUT))
    return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), struct, mask=pa.array([not v for v in valid]))


def build(directory):
    output = directory / "fwf_open_api.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    bad, duplicates, records, total, output_total, unknown_project = 0, 0, 0, None, None, 0
    project_pages, output_pages, codes, code_of = [], [], set(), {}   # code_of: project API id -> code
    pairs = set()                  # (code, output id), listed by either side
    with http_session() as session:
        session.headers["User-Agent"] = "OpenAlex-FWF-Ingest/1.0 (+https://openalex.org)"
        session.headers["Authorization"] = f"Bearer {_key(session, directory)}"
        load = _updated_at(session, directory)  # both stamps must be the same when the crawl ends
        for total, page in harvest(session, directory, "projects"):
            rows = []
            for record in page:
                records += 1
                row = parse_project(record, stamp, load["projects"])
                if row is None:
                    bad += 1
                    continue
                code_of[record.get("id")] = row["code"]
                pairs.update((row["code"], oid) for oid in _list(record.get("_list.connected.output")) or [])
                if row["code"] in codes:
                    duplicates += 1          # first page wins
                else:
                    codes.add(row["code"])
                    rows.append(row)
            project_pages.append(pa.RecordBatch.from_pylist(rows, schema=PROJECT_SCHEMA))
        withheld = None if load["output"] else "/indexes lists no output index"
        if withheld is None:
            try:
                for output_total, page in harvest(session, directory, "output"):
                    rows = []
                    for record in page:
                        fields, projects = parse_output(record)
                        rows.append(fields)
                        for p in projects:
                            if p in code_of:
                                pairs.add((code_of[p], fields["output_id"]))
                            else:
                                unknown_project += 1   # e.g. a sub-project id like projects-F81
                    output_pages.append(pa.RecordBatch.from_pylist(rows, schema=pa.schema(list(FWF_OUTPUT))))
            except ValueError as error:  # output-side trouble withholds the outputs, never the projects
                withheld = f"output harvest: {error}"
        end = _updated_at(session, directory)
        if end["projects"] != load["projects"]:
            raise ValueError("FWF: the projects index was rebuilt during the crawl; rerun")
        if withheld is None and end["output"] != load["output"]:
            withheld = "the output index was rebuilt during the crawl"
    if bad > MAX_BAD_CODES * records:
        raise ValueError(f"FWF: {bad} of {records} records have no readable project code; refusing upload")
    outputs = pa.Table.from_batches(output_pages, schema=pa.schema(list(FWF_OUTPUT)))
    known = set(outputs.column("output_id").to_pylist())
    unknown_output = sum(1 for _, oid in pairs if oid not in known)
    if withheld is None and unknown_project + unknown_output > MAX_UNKNOWN_LINKS * max(len(pairs) + unknown_project, 1):
        withheld = (f"{unknown_project} links to unknown projects and {unknown_output} to unknown outputs in "
                    f"{len(pairs) + unknown_project} (over {MAX_UNKNOWN_LINKS:.0%})")
    links = {}
    if withheld is None:
        for code, oid in pairs:
            if oid in known:
                links.setdefault(code, []).append(oid)
    del pairs, known
    table = pa.Table.from_batches(project_pages, schema=PROJECT_SCHEMA).sort_by("code")
    del project_pages
    linked = sum(len(v) for v in links.values())
    schema = FWF_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(   # the gate compares rows with it
        {"measure": "FWF Open API projects index total (/indexes/projects/documents)", "value": total,
         "excluded": {"no readable project code": bad, "project code repeated (first page wins)": duplicates},
         "outputs": {"output index total": output_total, "project-output links written": linked,
                     "links to a project id the projects index doesn't hold": unknown_project,
                     "links to an output id the output index doesn't hold": unknown_output,
                     **({"withheld": withheld} if withheld else {})}})})
    table = table.append_column(FWF_SCHEMA.field("outputs"), outputs_column(table.column("code").to_pylist(), links, outputs))
    table = table.append_column(FWF_SCHEMA.field("output_index_updated_at"),
                                pa.array([None if withheld else load["output"]] * table.num_rows, type=pa.string()))
    table = table.replace_schema_metadata(schema.metadata)
    pq.write_table(table, output)
    no_orcid = table.column("pi_orcid").null_count
    print(f"FWF projects: {table.num_rows} rows from {records} records ({bad} without a code, {duplicates} repeated codes); "
          f"{table.num_rows - no_orcid} with a PI ORCID; {linked} project-output links from {output_total} outputs "
          f"({unknown_project} to unknown projects, {unknown_output} to unknown outputs)"
          + (f"; OUTPUTS WITHHELD: {withheld}" if withheld else ""), flush=True)
    return output


def main():
    run("fwf_open_api", build, FWF_SCHEMA)


if __name__ == "__main__":
    main()
