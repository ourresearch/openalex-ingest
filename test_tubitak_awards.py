"""Offline checks for the TÜBİTAK (TR Dizin) fetcher: python -m unittest -v test_tubitak_awards."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq

from award_schemas import TUBITAK_SCHEMA
import tubitak_awards as tb


def author(name, duty, order, orcid=None):
    return {"name": name, "duty": duty, "order": order, "orcid": orcid, "institution": {"title": None}}


def record(rid, number, authors, year=2024, title="Başlık", extra=None):
    src = {"id": rid, "projectnumber": number, "projectGroup": "EEEAG", "publicationYear": year,
           "startedDate": f"{year - 2}-04-01T00:00:00Z", "endDate": f"{year}-04-01T00:00:00Z",
           "firstIndexDate": "2026-11-02T13:12:02Z", "indexDate": "2026-11-05T17:32:17Z", "docType": "PROJECT",
           "abstracts": [{"language": "TUR", "title": title, "abstract": "Özet", "keywords": "a; b"},
                         {"language": "ENG", "title": "Title", "abstract": "Abstract"}],
           "authors": authors, "subjects": [{"name": "Fizik", "rootName": "Fen"}],
           "references": ["big"] * 3, "citedReferences": ["big"]}
    src.update(extra or {})
    return {"_id": str(rid), "_source": src}


OK = {"timed_out": False, "_shards": {"total": 3, "successful": 3, "skipped": 0, "failed": 0}}   # TR Dizin's envelope, 11-06


class Api:
    """Serves hit pages through the download() seam; PAGE_SIZE is 2 in the tests, so paging is exercised."""

    def __init__(self, hits, total=None):
        self.hits, self.total, self.calls, self.override = hits, total if total is not None else len(hits), [], {}
        self.relation = "eq"

    def download(self, session, url, path, **kwargs):
        q = parse_qs(urlparse(url).query)
        page, size = int(q["page"][0]), int(q["limit"][0])
        assert q["facet-documentType"] == ["PROJECT"] and size == tb.PAGE_SIZE
        self.calls.append(page)
        body = self.override.get(page) or {**OK, "hits": {"total": {"value": self.total, "relation": self.relation},
                                                     "hits": self.hits[(page - 1) * size:page * size]}}
        path.write_text(json.dumps(body))


HITS = [
    record(1, "124E058", [author("Filiz Sari", "RESEARCHER", 2, "0000-0001-8462-175X"),
                          author("Prof. Dr. Ali Veli Yılmaz", "MANAGER", 1, "0000-0002-0000-0001")]),
    record(2, "124e 058", [author("Kazım Gemici", "RESEARCHER", 1)], title=None),   # same project, worse record
    record(3, "119K001", [author("Ayşe Kaya", "RESEARCHER", 3), author("Mehmet Demir", "ADVISOR", 2)], year=2021),
    record(4, None, [author("No Number", "MANAGER", 1)]),
]


class TubitakFetcherTest(unittest.TestCase):
    def setUp(self):
        self.p = [patch.object(tb, "PAGE_SIZE", 2), patch.object(tb, "PAGE_DELAY", 0), patch.object(tb, "MIN_ROWS_SHARE", 0.5)]
        for p in self.p:
            p.start()

    def tearDown(self):
        for p in self.p:
            p.stop()

    def build(self, api):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with patch.object(tb, "download", api.download):
            return tb.build(Path(tmp.name)), Path(tmp.name)

    def test_rows_dedupe_and_lead(self):
        api = Api(HITS)
        path, tmp = self.build(api)
        table = pq.read_table(path)
        self.assertTrue(table.schema.equals(TUBITAK_SCHEMA, check_metadata=False))
        rows = {r["funder_award_id"]: r for r in table.to_pylist()}
        self.assertEqual(sorted(rows), ["119K001", "124E058"])          # numbers normalised, one row each, no-number dropped
        self.assertEqual(api.calls, [1, 2])
        a = rows["124E058"]
        self.assertEqual((a["trdizin_id"], a["display_name"], a["description"]), ("1", "Başlık", "Özet"))   # best record wins
        self.assertEqual((a["lead_name"], a["lead_given_name"], a["lead_family_name"], a["lead_orcid"], a["lead_role"]),
                         ("Prof. Dr. Ali Veli Yılmaz", "Ali Veli", "Yılmaz", "0000-0002-0000-0001", "MANAGER"))
        self.assertEqual([o["name"] for o in json.loads(a["other_investigators_json"])], ["Filiz Sari"])
        self.assertEqual(json.loads(a["duplicate_trdizin_ids_json"]), ["2"])
        self.assertEqual((a["start_date"], a["end_date"], a["publication_year"]), ("2022-04-01", "2024-04-01", "2024"))
        self.assertEqual(a["landing_page_url"], "https://search.trdizin.gov.tr/tr/yayin/detay/1")
        self.assertEqual(a["source_raw_path"], tb.API_URL)
        self.assertEqual(a["reported_total"], "4")
        self.assertEqual(json.loads(pq.read_metadata(path).metadata[b"openalex.funder_total"]),   # what the gate reads
                         {"measure": "TR Dizin hits.total (documentType=PROJECT, exact)", "value": 4,
                          "excluded": {"hits without a project number": 1, "duplicate hits folded by project number": 1}})
        b = rows["119K001"]   # no MANAGER: the first contributor by order leads
        self.assertEqual((b["lead_name"], b["lead_role"]), ("Mehmet Demir", "ADVISOR"))
        self.assertEqual([o["name"] for o in json.loads(b["other_investigators_json"])], ["Ayşe Kaya"])
        self.assertFalse((tmp / "page.json").exists())

    def test_total_moving_or_short_pages_refuse(self):
        api = Api(HITS)
        api.override[2] = {**OK, "hits": {"total": {"value": 5, "relation": "eq"}, "hits": HITS[2:4]}}
        with self.assertRaisesRegex(ValueError, "total moved"):
            self.build(api)
        api = Api(HITS)
        api.override[2] = {**OK, "hits": {"total": {"value": 4, "relation": "eq"}, "hits": []}}
        with self.assertRaisesRegex(ValueError, "empty"):
            self.build(api)
        api = Api(HITS)
        api.override[2] = {**OK, "hits": {"total": {"value": 4, "relation": "eq"}, "hits": HITS[:2]}}   # a repeated page
        with self.assertRaisesRegex(ValueError, "distinct hits"):
            self.build(api)
        with self.assertRaisesRegex(ValueError, "no hits.total"):
            api = Api(HITS)
            api.override[1] = {**OK, "hits": {"hits": HITS[:2]}}
            self.build(api)

    def test_a_lower_bound_total_is_refused(self):
        # Codex 11-06: {"value": 10000, "relation": "gte"} was taken as exactly 10,000 hits; paging stopped there and the
        # distinct-hits check passed. Test scale: the API reports "gte" 4 while it holds 6 hits.
        api = Api(HITS + [record(i, f"{i}E{i}", [author("X Y", "MANAGER", 1)]) for i in (31, 32)], total=4)
        api.relation = "gte"
        with self.assertRaisesRegex(ValueError, "'gte' 4, not an exact count"):
            self.build(api)
        self.assertEqual(api.calls, [1])   # refused on the first page, before any paging
        api = Api(HITS)
        api.override[2] = {**OK, "hits": {"total": {"value": 4}, "hits": HITS[2:4]}}   # no relation on a later page: not exact either
        with self.assertRaisesRegex(ValueError, "page 2: hits.total is None 4"):
            self.build(api)

    def test_a_partial_shard_result_is_refused(self):
        # Codex 11-06 (re-check): {_shards: {total 2, successful 1, failed 1}} with hits.total {1, eq} published a one-row
        # snapshot whose footer total (1) also passed the funder gate. Every page must be complete.
        ok = {"total": 3, "successful": 3, "skipped": 0, "failed": 0}
        for envelope, why in (({"timed_out": False, "_shards": {"total": 2, "successful": 1, "skipped": 0, "failed": 1}}, "failed"),
                              ({"timed_out": True, "_shards": ok}, "timed out"),
                              ({"_shards": ok}, "no timed_out"), ({"timed_out": False}, "no _shards")):
            api = Api(HITS[:1])
            api.override[1] = dict(envelope, hits={"total": {"value": 1, "relation": "eq"}, "hits": HITS[:1]})
            with self.assertRaisesRegex(ValueError, "page 1: partial result", msg=why):
                self.build(api)

    def test_too_few_projects_refuse(self):
        hits = [record(i, None, [author("X Y", "MANAGER", 1)]) for i in range(11, 14)] + [HITS[0]]
        with self.assertRaisesRegex(ValueError, "refusing upload"):
            self.build(Api(hits))

    def test_scheduled_prefix(self):
        import award_fetch
        self.assertEqual(award_fetch.destination("tubitak_trdizin", __import__("datetime").date(2026, 12, 1)),
                         ("openalex-ingest", "awards/tubitak_trdizin/2026-12-01.parquet"))


if __name__ == "__main__":
    unittest.main()
