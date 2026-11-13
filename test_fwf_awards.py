"""Offline checks for the FWF fetcher: python -m unittest -v test_fwf_awards."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq

import award_fetch
from award_schemas import FWF_OUTPUT, FWF_SCHEMA
import fwf_awards as fwf

KEY = "ab" * 32


def project(code, last="Muster", orcid="0000-0002-7747-4015", doi=True, **extra):
    record = {"id": f"projects-{code}", "_str.projecttitle.en": f" Title  {code} ", "_str.projecttitle.de": f"Titel {code}",
              "_str.program.en": "Principal Investigator Projects", "_str.status.en": "Ended",
              "_date.approvaldate": "2019-01-28", "_date.startdate": "2019-03-01", "_date.enddate": "2023-02-28",
              "_long.approvedamount": 398000, "_str.principalinvestigator.firstname": "Eva",
              "_str.principalinvestigator.lastname": last, "_str.principalinvestigator.orcid": orcid,
              "_str.principalinvestigator.researchinstitute.name": "Universität Wien",
              "_str.principalinvestigator.researchinstitute.ror": "https://ror.org/03prydq77",
              "_list.keywords.split": ["Archaea", " "], "_list.connected.output": ["output-1", "output-2"],
              "_list.nationalprojectparticipants.en": ["Ada Zed, Universität Graz (https://ror.org/01faaaf77), national collaboration partner"]}
    if doi:
        record["_str.grantdoi"] = f"10.55776/{code}"
    record.update(extra)
    return record


def output(n, projects, doi=None, pmid=None, title=None, **extra):
    record = {"id": f"output-{n}", "_str.category": "publications", "_str.type": "Journal Article",
              "_list.connected.projects": [f"projects-{p}" for p in projects], "_list.year": ["2020"],
              "_str.title": title or f"Paper {n}", "_list.author": ["Heid E", "Boresch S"]}
    if doi:
        record["_str.doi"] = doi
    if pmid:
        record["_str.pmid"] = pmid
    record.update(extra)
    return record


class Api:
    """Serves the key page, /indexes and paged documents of both indexes through the download() seam (page size 2)."""

    def __init__(self, projects, updated=("2026-11-05T03:15:32Z",), outputs=None, output_updated=("2026-11-05T03:16:40Z",)):
        self.projects, self.updated, self.calls, self.total = projects, list(updated), [], None
        self.outputs = outputs if outputs is not None else [output(1, []), output(2, [])]   # what project() lists
        self.output_updated = list(output_updated)

    def download(self, session, url, path, **kwargs):
        parsed = urlparse(url)
        self.calls.append(parsed.path)
        if url == fwf.KEY_URL:
            path.write_text(KEY + "\n")
        elif parsed.path == "/indexes":
            assert session.headers["Authorization"] == f"Bearer {KEY}"
            stamp = self.updated.pop(0) if len(self.updated) > 1 else self.updated[0]
            out = self.output_updated.pop(0) if len(self.output_updated) > 1 else self.output_updated[0]
            path.write_text(json.dumps({"results": [{"uid": "output", "updatedAt": out}, {"uid": "projects", "updatedAt": stamp}]}))
        else:
            q = parse_qs(parsed.query)
            offset, limit = int(q["offset"][0]), int(q["limit"][0])
            docs = self.outputs if parsed.path == "/indexes/output/documents" else self.projects
            assert parsed.path in ("/indexes/output/documents", "/indexes/projects/documents"), parsed.path
            total = self.total if self.total is not None and docs is self.projects else len(docs)
            path.write_text(json.dumps({"offset": offset, "limit": limit, "total": total,
                                        "results": docs[offset:offset + limit]}))


def build_with(api):
    with tempfile.TemporaryDirectory() as tmp, patch.object(fwf, "download", api.download), \
            patch.object(fwf, "PAGE_SIZE", 2), patch.object(fwf, "PAGE_DELAY", 0):
        path = fwf.build(Path(tmp))
        return pq.read_table(path)


class FwfTests(unittest.TestCase):
    def test_destination_is_its_own_prefix(self):
        now = datetime(2026, 11, 9, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination("fwf_open_api", now),
                             ("openalex-ingest", "awards/fwf_open_api/2026-11-09.parquet"))
        with patch.dict(os.environ, {"FWF_OPEN_API_AWARDS_S3_PREFIX": "s3://openalex-ingest/awards/fwf/"}):
            with self.assertRaises(ValueError):
                award_fetch.destination("fwf_open_api", now)

    def test_rows_keyed_on_code_sorted_and_frozen(self):
        api = Api([project("P35"), project("AR590", orcid="https://orcid.org/0000-0003-0951-336x"),
                   project("P35", last="Later", id="projects-P35-copy"), project("PIN1503126", doi=False, orcid="not-an-orcid"),
                   project("I2353", orcid=None)])
        table = build_with(api)
        self.assertTrue(table.schema.equals(FWF_SCHEMA, check_metadata=False))
        rows = table.to_pylist()
        self.assertEqual([r["code"] for r in rows], ["AR590", "I2353", "P35", "PIN1503126"])   # sorted; P35 first page wins
        p35 = next(r for r in rows if r["code"] == "P35")
        self.assertEqual((p35["pi_last"], p35["title_en"], p35["grant_doi"], p35["amount"]), ("Muster", "Title P35", "10.55776/p35", 398000.0))
        self.assertEqual((p35["keywords"], p35["n_outputs_listed"], p35["index_updated_at"]), (["Archaea"], 2, "2026-11-05T03:15:32Z"))
        self.assertEqual(rows[0]["pi_orcid"], "0000-0003-0951-336X")
        self.assertIsNone(rows[3]["pi_orcid"])
        self.assertEqual(rows[3]["grant_doi"], "10.55776/pin1503126")          # from the API id when FWF gives no DOI
        self.assertEqual(api.calls[:2], ["/fwfkey/", "/indexes"])
        self.assertEqual(api.calls.count("/indexes"), 2)                       # the stamps at the start and the end
        total = json.loads(table.schema.metadata[b"openalex.funder_total"])   # what the gate compares rows with
        self.assertEqual(total, {"measure": "FWF Open API projects index total (/indexes/projects/documents)", "value": 5,
                                 "excluded": {"no readable project code": 0, "project code repeated (first page wins)": 1},
                                 "outputs": {"output index total": 2, "project-output links written": 8,
                                             "links to a project id the projects index doesn't hold": 0,
                                             "links to an output id the output index doesn't hold": 0}})

    def test_moved_total_short_or_repeated_page_aborts(self):
        api = Api([project("P1"), project("P2"), project("P3")])
        api.total = 3
        projects = api.projects
        api.projects = projects[:2] + projects[:1]               # page 2 repeats an id
        with self.assertRaisesRegex(ValueError, "short or repeated"):
            build_with(api)
        api = Api([project("P1"), project("P2"), project("P3")])
        api.total = 4                                             # the listing says 4, only 3 exist: the last page is short
        with self.assertRaisesRegex(ValueError, "short or repeated"):
            build_with(api)

    def test_index_rebuilt_during_harvest_aborts(self):
        api = Api([project("P1"), project("P2"), project("P3")], updated=("2026-11-05T03:15:32Z", "2026-11-06T03:15:32Z"))
        with self.assertRaisesRegex(ValueError, "rebuilt"):
            build_with(api)

    def test_bad_key_page_and_unreadable_codes_refuse(self):
        api = Api([project("P1")])
        original = api.download
        def bad_key(session, url, path, **kwargs):
            if url == fwf.KEY_URL:
                path.write_text("<html>moved</html>")
            else:
                original(session, url, path, **kwargs)
        with tempfile.TemporaryDirectory() as tmp, patch.object(fwf, "download", bad_key), patch.object(fwf, "PAGE_DELAY", 0):
            with self.assertRaisesRegex(ValueError, "did not hold a key"):
                fwf.build(Path(tmp))
        api = Api([project("P1", doi=False, id="projects-p 1?"), project("P2")])
        with self.assertRaisesRegex(ValueError, "no readable project code"):
            build_with(api)
        with patch.object(fwf, "MAX_BAD_CODES", 1.0):   # under the refusal level: written, and counted in the footer
            table = build_with(Api([project("P1", doi=False, id="projects-p 1?"), project("P2")]))
        total = json.loads(table.schema.metadata[b"openalex.funder_total"])
        self.assertEqual((table.num_rows, total["value"], total["excluded"]["no readable project code"]), (1, 2, 1))

    def test_outputs_linked_from_either_side_cleaned_sorted_and_counted(self):
        projects = [project("P1", **{"_list.connected.output": ["output-1", "output-2", "output-9"]}),
                    project("P2", **{"_list.connected.output": []})]
        outputs = [output(1, ["P1"], doi="https://doi.org/10.1093/GBE/evaa046", pmid="32145015", title="Zeta"),
                   output(2, ["P1", "P2"], doi="doi:10.1063/1.5143746", pmid="PMC123"),
                   output(3, ["F81"], doi="10.1000/sub.project"),          # a sub-project id the projects index lacks
                   output(4, ["P2"], title="Alpha", **{"_list.author": ["Bergmeister, Stefan", "Von Haeseler A"]})]
        with patch.object(fwf, "MAX_UNKNOWN_LINKS", 1.0):
            table = build_with(Api(projects, outputs=outputs))
        self.assertTrue(table.schema.equals(FWF_SCHEMA, check_metadata=False))
        rows = {r["code"]: r for r in table.to_pylist()}
        got = {c: [(o["output_id"], o["doi"], o["pmid"], o["surnames"]) for o in r["outputs"]] for c, r in rows.items()}
        self.assertEqual(got, {   # sorted: DOI, PMID, title
            "P1": [("output-2", "10.1063/1.5143746", None, ["Heid", "Boresch"]),
                   ("output-1", "10.1093/gbe/evaa046", "32145015", ["Heid", "Boresch"])],
            "P2": [("output-4", None, None, ["Bergmeister", "Von Haeseler"]), ("output-2", "10.1063/1.5143746", None, ["Heid", "Boresch"])]})
        self.assertEqual(rows["P2"]["outputs"][0]["title"], "Alpha")
        self.assertEqual(rows["P1"]["output_index_updated_at"], "2026-11-05T03:16:40Z")
        total = json.loads(table.schema.metadata[b"openalex.funder_total"])["outputs"]
        self.assertEqual(total, {"output index total": 4, "project-output links written": 4,
                                 "links to a project id the projects index doesn't hold": 1,
                                 "links to an output id the output index doesn't hold": 1})
        table = build_with(Api(projects, outputs=outputs))               # 2 unknown of 5 links is over the 1% line:
        self.assertEqual({r["code"]: r["outputs"] for r in table.to_pylist()}, {"P1": None, "P2": None})   # withheld,
        self.assertIn("unknown projects", json.loads(table.schema.metadata[b"openalex.funder_total"])["outputs"]["withheld"])

    def test_surnames_are_the_surname_parts_deduplicated_and_capped(self):
        self.assertEqual(fwf.surnames(["Bergmeister, Stefan", "Heid E", "Xu N-L", "Von Haeseler A", "Thomas Müller",
                                       "O'Brien J", "Heid, E.", " ", "Q"]),
                         ["Bergmeister", "Heid", "Xu", "Von Haeseler", "Thomas", "Müller", "O'Brien", "Q"])
        self.assertEqual(len(fwf.surnames([f"Author{i:03d} A" for i in range(80)])), fwf.MAX_SURNAMES)
        self.assertIsNone(fwf.surnames(["", " "]))
        self.assertIsNone(fwf.surnames(None))

    def test_output_trouble_withholds_the_outputs_not_the_projects(self):
        class NoOutputIndex(Api):   # /indexes lists only projects
            def download(self, session, url, path, **kwargs):
                super().download(session, url, path, **kwargs)
                if urlparse(url).path == "/indexes":
                    body = json.loads(path.read_text())
                    path.write_text(json.dumps({"results": [i for i in body["results"] if i["uid"] == "projects"]}))
        for api, why in ((Api([project("P1")], output_updated=("2026-11-05T03:16:40Z", "2026-11-06T03:16:40Z")), "rebuilt"),
                         (Api([project("P1")], outputs=[]), "FWF output: empty listing"),   # an empty output index
                         (NoOutputIndex([project("P1")]), "no output index")):
            table = build_with(api)
            self.assertTrue(table.schema.equals(FWF_SCHEMA, check_metadata=False))
            rows = table.to_pylist()
            self.assertEqual([(r["code"], r["outputs"], r["output_index_updated_at"]) for r in rows], [("P1", None, None)])
            self.assertIn(why, json.loads(table.schema.metadata[b"openalex.funder_total"])["outputs"]["withheld"])

    def test_outputs_column_matches_the_schema_and_keeps_nulls(self):
        outputs = pa.Table.from_pylist([{"output_id": "output-1", "title": "B", "surnames": ["Heid"]},
                                        {"output_id": "output-2", "title": "A"}], schema=pa.schema(list(FWF_OUTPUT)))
        column = fwf.outputs_column(["P1", "P2"], {"P2": ["output-1", "output-2"]}, outputs)
        self.assertEqual(column.type, FWF_SCHEMA.field("outputs").type)
        self.assertEqual(column.to_pylist()[0], None)
        self.assertEqual([(o["output_id"], o["surnames"]) for o in column.to_pylist()[1]], [("output-2", None), ("output-1", ["Heid"])])


if __name__ == "__main__":
    unittest.main()
