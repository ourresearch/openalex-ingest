"""Offline checks for the RCN (Prosjektbanken) fetcher: python -m unittest -v test_rcn_awards."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq

from award_schemas import RCN_SCHEMA
import rcn_awards as rcn

EXPLORE = ('<html><head></head><body><script id="__NEXT_DATA__" type="application/json">'
           '{"buildId":"AbC123_x-9","page":"/[lang]/explore/[view]"}</script></body></html>')


def project(pid, lead="Prof. Kari Nordmann", years=(2019, 2020, 2021), funding=1500000, **extra):
    p = {"id": pid, "source": "FORISS", "title": f"  Project   {pid} ", "leadName": lead, "yearsActive": list(years),
         "totalFunding": funding, "teaser": "Teaser", "popSciDescription": None, "projectSummary": "Summary",
         "organisations": [["UH-sektor", "Universiteter", "UNIVERSITETET I OSLO", "Det matematisk-naturvitenskapelige fakultet"]],
         "geographies": [["Oslo", "Oslo"]], "disciplines": [["Medisin og helsefag"]],
         "currentActivity": {"activity": "FRIPRO-Fri prosjektstøtte", "year": 2021}}
    p.update(extra)
    return p


class Api:
    """Serves the explore page and the Next.js data route through the download() seam; PAGE_SIZE is 2 in tests."""

    def __init__(self, projects, total=None):
        self.projects, self.total, self.calls, self.override, self.explore = projects, total, [], {}, EXPLORE

    def download(self, session, url, path, **kwargs):
        u = urlparse(url)
        if u.path == "/en/explore/projects":
            self.calls.append("explore")
            path.write_text(self.explore)
            return
        assert u.path == "/_next/data/AbC123_x-9/en/explore/projects.json", u.path
        q = parse_qs(u.query)
        assert q["Kilde"] == ["FORISS"] and int(q["resultCount"][0]) == rcn.PAGE_SIZE
        offset = int(q["offset"][0])
        self.calls.append(offset)
        body = self.override.get(offset) or {"pageProps": {"initialState": {"PROJECTLIST": {
            "projects": self.projects[offset:offset + rcn.PAGE_SIZE],
            "pagination": {"totalHits": self.total if self.total is not None else len(self.projects)}}}}}
        path.write_text(json.dumps(body))


PROJECTS = [project("300100"), project("101233", lead="Asbjørn Mo", years=(1993, 2015), funding="2169161"),
            project("255000", lead=None, years=[], funding=None, duration={"startYear": 2016, "endYear": 2018})]


class RcnFetcherTest(unittest.TestCase):
    def setUp(self):
        self.p = [patch.object(rcn, "PAGE_SIZE", 2), patch.object(rcn, "PAGE_DELAY", 0)]
        for p in self.p:
            p.start()

    def tearDown(self):
        for p in self.p:
            p.stop()

    def build(self, api):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with patch.object(rcn, "download", api.download):
            return rcn.build(Path(tmp.name)), Path(tmp.name)

    def test_rows_match_the_saved_file_shape(self):
        api = Api(PROJECTS)
        path, tmp = self.build(api)
        table = pq.read_table(path)
        self.assertTrue(table.schema.equals(RCN_SCHEMA, check_metadata=False))
        self.assertEqual(api.calls, ["explore", 0, 2])
        total = json.loads(pq.read_metadata(path).metadata[b"openalex.funder_total"])   # what the gate compares rows with
        self.assertEqual(total, {"measure": "Prosjektbanken totalHits (Kilde=FORISS)", "value": 3})
        rows = table.to_pylist()
        self.assertEqual([r["source_record_id"] for r in rows], ["FORISS:101233", "FORISS:255000", "FORISS:300100"])
        a = {r["project_id"]: r for r in rows}
        self.assertEqual(a["300100"]["funder_award_id"], "rcn-foriss-300100")
        self.assertEqual((a["300100"]["display_name"], a["300100"]["lead_given_name"], a["300100"]["lead_family_name"]),
                         ("Project 300100", "Kari", "Nordmann"))   # title spaces collapsed; "Prof." dropped
        self.assertEqual((a["300100"]["start_year"], a["300100"]["end_year"], a["300100"]["start_date"], a["300100"]["end_date"]),
                         ("2019", "2021", "2019-01-01", "2021-12-31"))
        self.assertEqual((a["300100"]["total_funding"], a["300100"]["currency"]), ("1500000", "NOK"))
        self.assertEqual(a["300100"]["primary_organization"], "UNIVERSITETET I OSLO")
        self.assertEqual(json.loads(a["300100"]["current_activity_json"]), {"activity": "FRIPRO-Fri prosjektstøtte", "year": 2021})
        self.assertEqual(a["300100"]["current_activity_code"], "FRIPRO-Fri prosjektstøtte")
        self.assertEqual(a["300100"]["landing_page_url"], "https://prosjektbanken.forskningsradet.no/en/project/FORISS/300100")
        self.assertEqual(a["101233"]["years_active_json"], "[1993, 2015]")
        b = a["255000"]   # no years: the duration fills them; no lead, no amount
        self.assertEqual((b["start_year"], b["end_year"], b["start_date"], b["end_date"]), ("2016", "2018", "2016-01-01", "2018-12-31"))
        self.assertEqual((b["lead_name"], b["total_funding"], b["currency"]), (None, None, None))
        self.assertFalse(list(tmp.iterdir())[1:])   # only the parquet is left

    def test_refusals(self):
        api = Api(PROJECTS)
        api.explore = "<html>no data</html>"
        with self.assertRaisesRegex(ValueError, "__NEXT_DATA__"):
            self.build(api)
        api = Api(PROJECTS)
        api.override[2] = {"pageProps": {"initialState": {"PROJECTLIST": {"projects": [], "pagination": {"totalHits": 3}}}}}
        with self.assertRaisesRegex(ValueError, "empty page"):
            self.build(api)
        api = Api(PROJECTS)
        api.override[2] = {"pageProps": {"initialState": {"PROJECTLIST": {"projects": PROJECTS[2:], "pagination": {"totalHits": 4}}}}}
        with self.assertRaisesRegex(ValueError, "totalHits moved"):
            self.build(api)
        api = Api(PROJECTS)
        api.override[2] = {"pageProps": {"initialState": {"PROJECTLIST": {"projects": PROJECTS[:1], "pagination": {"totalHits": 3}}}}}
        with self.assertRaisesRegex(ValueError, "repeated while paging"):   # a repeated project: 2 distinct for 3 hits
            self.build(api)
        api = Api(PROJECTS)   # the last page carries one more project than totalHits allows: 4 distinct for 3
        api.override[2] = {"pageProps": {"initialState": {"PROJECTLIST": {"projects": PROJECTS[2:] + [project("777")], "pagination": {"totalHits": 3}}}}}
        with self.assertRaisesRegex(ValueError, "4 distinct projects for totalHits 3"):
            self.build(api)
        api = Api(PROJECTS + [project("101233", source="foriss")])   # same award id under another case
        with self.assertRaisesRegex(ValueError, "duplicate funder_award_id"):
            self.build(api)
        api = Api([project("1", title=" ")])
        with self.assertRaisesRegex(ValueError, "no title"):
            self.build(api)

    def test_a_repeated_page_under_the_old_98_percent_floor_is_refused(self):
        # Codex 11-06: totalHits 44,500, one 500-project page served twice -> 44,000 distinct, which passed the 98% floor and
        # published without 500 projects. Same shape at test scale: 150 hits, pages of 2, the page at offset 100 repeats
        # offset 98's (148 distinct = 98.7%).
        many = [project(str(400000 + i)) for i in range(150)]
        api = Api(many)
        api.override[100] = {"pageProps": {"initialState": {"PROJECTLIST": {"projects": many[98:100], "pagination": {"totalHits": 150}}}}}
        with self.assertRaisesRegex(ValueError, "FORISS:400098 repeated while paging"):
            self.build(api)
        self.assertEqual(api.calls[-1], 100)   # refused on the repeated page itself, not at the end
        api = Api(many)   # the same pages without the repeat pass
        path, _ = self.build(api)
        self.assertEqual(pq.read_metadata(path).num_rows, 150)

    def test_prefix(self):
        import award_fetch
        self.assertEqual(award_fetch.destination("rcn_project_bank", __import__("datetime").date(2026, 12, 1)),
                         ("openalex-ingest", "awards/rcn_project_bank/2026-12-01.parquet"))


if __name__ == "__main__":
    unittest.main()
