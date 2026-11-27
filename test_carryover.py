"""Offline checks for the carry-over re-fetch on feeds that repeat the previous record's fields (oxjob #2662):
python -m unittest -v test_carryover. No network, S3 or database."""
from datetime import date, datetime
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

os.environ.setdefault("SOURCES_DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import repositories as R
from test_harvest_checkpoint import FakeDb, FakeFeed

CAIRN = "asswjsx35xuxkrfsyfwn"


def rec(identifier, creator=(), description=(), deleted=False, source="list"):
    md = {}
    if creator:
        md["creator"] = list(creator)
    if description:
        md["description"] = list(description)
    r = SimpleNamespace(header=SimpleNamespace(identifier=identifier, datestamp="2016-10-01", deleted=deleted),
                        raw=f"<record><header><identifier>{identifier}</identifier></header></record>",
                        source=source)
    if not deleted:
        r.metadata = md
    return r


class FakeSickle:
    """GetRecord stand-in: the record's own fields from `truth`; ids in `fail` raise."""

    def __init__(self, truth, fail=(), flaky=None):
        self.truth, self.fail, self.deadline, self.calls = truth, set(fail), None, []
        self.flaky = dict(flaky or {})  # identifier -> failures before it answers

    def GetRecord(self, identifier, metadataPrefix):
        self.calls.append(identifier)
        if identifier in self.fail:
            raise RuntimeError("timeout")
        if self.flaky.get(identifier, 0) > 0:
            self.flaky[identifier] -= 1
            raise RuntimeError("timeout")
        cr, de = self.truth[identifier]
        return rec(identifier, cr, de, source="get")


def run(endpoint_id, records, sickle):
    state = SimpleNamespace(id=endpoint_id, pmh_url="https://oai.example.org/oai.php", pmh_set=None,
                            metadata_prefix="oai_dc", most_recent_date_harvested=datetime(2016, 9, 30),
                            fetch_via_zyte=False, error=None)
    saved = []

    def save_batch(self, s3_client, s3_bucket, batch_number, recs, date_key, folder=None):
        saved.extend(recs)
        return date_key

    with patch.object(R.EndpointHarvester, "detect_date_format", lambda self: "%Y-%m-%d"), \
            patch.object(R, "_get_my_sickle", lambda *a, **k: sickle), \
            patch.object(R.EndpointHarvester, "_make_oai_request", lambda self, s, **kw: FakeFeed(records)), \
            patch.object(R.EndpointHarvester, "save_batch", save_batch):
        h = R.EndpointHarvester(state, FakeDb())
        h.harvest("bucket", first=date(2016, 10, 1), last=date(2016, 10, 1))
    return saved, h


# Formation emploi 135 as Cairn lists it: 0155 has its own abstract; 0179 has its own author and no abstract; 0187 and
# 0189a have neither. 0007 shares its author with 0001 for real (same person wrote both).
LISTED = [
    rec("0001", ["Vergnies"], []),
    rec("0007", ["Vergnies"], []),
    rec("0155", ["Garrouste"], ["CIPEGE abstract"]),
    rec("0179", ["Bazillier"], ["CIPEGE abstract"]),
    rec("0187", ["Bazillier"], ["CIPEGE abstract"]),
    rec("0189a", ["Bazillier"], ["CIPEGE abstract"]),
    rec("0190", [], [], deleted=True),
    rec("0191", ["Other"], ["Own abstract"]),
]
TRUTH = {"0007": (["Vergnies"], []), "0179": (["Bazillier"], []), "0187": ([], []), "0189a": ([], [])}


class CarryOver(unittest.TestCase):
    def test_suspects_fetched_again_and_replaced(self):
        sickle = FakeSickle(TRUTH)
        saved, h = run(CAIRN, LISTED, sickle)
        self.assertEqual(sickle.calls, ["0007", "0179", "0187", "0189a"])
        by_id = {r.header.identifier: r for r in saved}
        self.assertEqual(len(saved), len(LISTED))
        self.assertEqual(by_id["0187"].source, "get")
        self.assertEqual(by_id["0187"].metadata, {})
        self.assertEqual(by_id["0179"].metadata, {"creator": ["Bazillier"]})
        self.assertEqual(by_id["0155"].source, "list")
        self.assertEqual(by_id["0191"].source, "list")
        self.assertEqual((h.carryover_refetched, h.carryover_failed), (4, 0))

    def test_failed_getrecord_keeps_listed_copy(self):
        sickle = FakeSickle(TRUTH, fail={"0187"})
        with patch.object(R.EndpointHarvester, "CARRYOVER_BACKOFF_S", 0):
            saved, h = run(CAIRN, LISTED, sickle)
        by_id = {r.header.identifier: r for r in saved}
        self.assertEqual(by_id["0187"].source, "list")
        self.assertEqual((h.carryover_refetched, h.carryover_failed), (3, 1))

    def test_deleted_record_keeps_the_comparison(self):
        # the server may keep its fields across a deleted record: 0192 repeats 0191's abstract after it
        listed = [rec("0191", ["Other"], ["Own abstract"]), rec("0190", [], [], deleted=True),
                  rec("0192", [], ["Own abstract"])]
        sickle = FakeSickle({"0192": ([], [])})
        saved, h = run(CAIRN, listed, sickle)
        self.assertEqual(sickle.calls, ["0192"])
        self.assertEqual({r.header.identifier: r.source for r in saved}, {"0191": "list", "0190": "list", "0192": "get"})

    def test_empty_elements_are_no_value(self):
        listed = [rec("0001", ["A"], [None]), rec("0002", ["B"], [None]), rec("0003", ["C"], ["  "])]
        sickle = FakeSickle({})
        run(CAIRN, listed, sickle)
        self.assertEqual(sickle.calls, [])

    def test_getrecord_retried_before_keeping_listed_copy(self):
        sickle = FakeSickle(TRUTH, flaky={"0187": 2})
        with patch.object(R.EndpointHarvester, "CARRYOVER_BACKOFF_S", 0):
            saved, h = run(CAIRN, LISTED, sickle)
        by_id = {r.header.identifier: r for r in saved}
        self.assertEqual(sickle.calls.count("0187"), 3)
        self.assertEqual(by_id["0187"].source, "get")
        self.assertEqual((h.carryover_refetched, h.carryover_failed), (4, 0))

    def test_other_endpoints_untouched(self):
        sickle = FakeSickle(TRUTH)
        saved, _ = run("some-other-endpoint", LISTED, sickle)
        self.assertEqual(sickle.calls, [])
        self.assertTrue(all(r.source == "list" for r in saved))


if __name__ == "__main__":
    unittest.main()
