"""Offline checks for the FWF fetcher: python -m unittest -v test_fwf_awards."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq

import award_fetch
from award_schemas import FWF_SCHEMA
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


class Api:
    """Serves the key page, /indexes and paged documents through the download() seam (page size 2)."""

    def __init__(self, projects, updated=("2026-11-05T03:15:32Z",)):
        self.projects, self.updated, self.calls, self.total = projects, list(updated), [], None

    def download(self, session, url, path, **kwargs):
        parsed = urlparse(url)
        self.calls.append(parsed.path)
        if url == fwf.KEY_URL:
            path.write_text(KEY + "\n")
        elif parsed.path == "/indexes":
            assert session.headers["Authorization"] == f"Bearer {KEY}"
            stamp = self.updated.pop(0) if len(self.updated) > 1 else self.updated[0]
            path.write_text(json.dumps({"results": [{"uid": "output", "updatedAt": "x"}, {"uid": "projects", "updatedAt": stamp}]}))
        else:
            q = parse_qs(parsed.query)
            offset, limit = int(q["offset"][0]), int(q["limit"][0])
            total = self.total if self.total is not None else len(self.projects)
            path.write_text(json.dumps({"offset": offset, "limit": limit, "total": total,
                                        "results": self.projects[offset:offset + limit]}))


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
        total = json.loads(table.schema.metadata[b"openalex.funder_total"])   # what the gate compares rows with
        self.assertEqual(total, {"measure": "FWF Open API projects index total (/indexes/projects/documents)", "value": 5,
                                 "excluded": {"no readable project code": 0, "project code repeated (first page wins)": 1}})

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


if __name__ == "__main__":
    unittest.main()
