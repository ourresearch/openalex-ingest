"""Offline checks for the harvester's checkpoint on out-of-order feeds and undated records (oxjob #2111):
python -m unittest -v test_harvest_checkpoint. No network, S3 or database."""
from datetime import date, datetime
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

os.environ.setdefault("SOURCES_DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import repositories as R


def rec(identifier, datestamp):
    return SimpleNamespace(header=SimpleNamespace(identifier=identifier, datestamp=datestamp),
                           raw=f"<record><header><identifier>{identifier}</identifier></header></record>")


class FakeFeed:
    """ListRecords stand-in: yields records, then raises `fail` after `fail_after` records if given."""

    def __init__(self, records, fail_after=None, fail=None):
        self.records, self.fail_after, self.fail = list(records), fail_after, fail
        self.i = 0
        self._items = object()   # no oai_response: skips the completeListSize read

    def __iter__(self):
        return self

    def __next__(self):
        if self.fail_after is not None and self.i == self.fail_after:
            raise self.fail
        if self.i >= len(self.records):
            raise StopIteration
        self.i += 1
        return self.records[self.i - 1]


class FakeDb:
    def merge(self, _):
        pass

    def commit(self):
        pass


def run(feed, checkpoint, first=date(2026, 11, 5), last=date(2026, 11, 8)):
    """Harvest `feed` from a state checkpointed at `checkpoint`; return (final checkpoint, saved ids, error)."""
    state = SimpleNamespace(id="test-endpoint", pmh_url="https://example.org/oai", pmh_set=None,
                            metadata_prefix="oai_dc", most_recent_date_harvested=checkpoint,
                            fetch_via_zyte=False, error=None)
    saved = []

    def save_batch(self, s3_client, s3_bucket, batch_number, records, date_key, folder=None):
        saved.extend((date_key, r.header.identifier) for r in records)
        return date_key

    error = None
    with patch.object(R.EndpointHarvester, "detect_date_format", lambda self: "%Y-%m-%d"), \
            patch.object(R, "_get_my_sickle", lambda *a, **k: SimpleNamespace(deadline=None)), \
            patch.object(R.EndpointHarvester, "_make_oai_request", lambda self, sickle, **kw: feed), \
            patch.object(R.EndpointHarvester, "save_batch", save_batch):
        h = R.EndpointHarvester(state, FakeDb())
        try:
            h.harvest("bucket", first=first, last=last)
        except Exception as e:  # noqa: BLE001 - the test inspects it
            error = e
    return state.most_recent_date_harvested, saved, error


START = datetime(2026, 11, 6)
# PMC's order for from=2026-11-05 until=2026-11-08 (9 Nov 2026): interleaved, plus a record stamped months earlier.
PMC_LIKE = [rec("a", "2026-11-06"), rec("b", "2026-11-08"), rec("c", "2026-11-07"), rec("d", "2025-03-03"),
            rec("e", "2026-11-05"), rec("f", "2026-11-08"), rec("g", "2026-11-05")]


class OutOfOrderFeed(unittest.TestCase):
    def test_complete_walk_checkpoints_the_window_end(self):
        cp, saved, err = run(FakeFeed(PMC_LIKE), START)
        self.assertIsNone(err)
        self.assertEqual(cp, datetime(2026, 11, 8))
        self.assertEqual(sorted(i for _, i in saved), list("abcdefg"))

    def test_failed_walk_keeps_the_starting_checkpoint(self):
        # Before the fix the mid-walk checkpoint reached 2026-11-08 at record 'c', so a failure at 'e'
        # left the next run starting on 11-07 and 'e' and 'g' (2026-11-05) were never read.
        cp, saved, err = run(FakeFeed(PMC_LIKE, fail_after=4, fail=ConnectionError("dropped")), START)
        self.assertIsInstance(err, ConnectionError)
        self.assertEqual(cp, START)

    def test_step_back_after_checkpoints_rolls_back(self):
        feed = [rec("a", "2026-11-05"), rec("b", "2026-11-06"), rec("c", "2026-11-07"), rec("d", "2026-11-05")]
        cp, _, err = run(FakeFeed(feed, fail_after=4, fail=ConnectionError("dropped")), datetime(2026, 11, 4))
        self.assertIsNotNone(err)
        self.assertEqual(cp, datetime(2026, 11, 4))


class OrderedFeed(unittest.TestCase):
    """Behaviour kept for the ~5,000 ordered endpoints."""

    def test_failure_checkpoints_the_day_before_the_open_one(self):
        feed = [rec("a", "2026-11-05"), rec("b", "2026-11-06"), rec("c", "2026-11-07"), rec("d", "2026-11-07")]
        cp, saved, err = run(FakeFeed(feed, fail_after=3, fail=ConnectionError("dropped")), datetime(2026, 11, 4))
        self.assertIsNotNone(err)
        self.assertEqual(cp, datetime(2026, 11, 6))
        self.assertEqual([i for _, i in saved], ["a", "b", "c"])

    def test_complete_walk_checkpoints_the_last_date_walked(self):
        feed = [rec("a", "2026-11-05"), rec("b", "2026-11-06")]
        cp, _, err = run(FakeFeed(feed), datetime(2026, 11, 4))
        self.assertIsNone(err)
        self.assertEqual(cp, datetime(2026, 11, 6))


class UndatedRecord(unittest.TestCase):
    def test_no_datestamp_is_kept_and_does_not_crash(self):
        # oai:pubmedcentral.nih.gov:13700003 came with <datestamp></datestamp> and killed every run that reached it.
        feed = [rec("a", "2026-11-05"), rec("pmc13700003", None), rec("b", "2026-11-06"), rec("c", "")]
        cp, saved, err = run(FakeFeed(feed), datetime(2026, 11, 4))
        self.assertIsNone(err)
        self.assertIn(("2026-11-08", "pmc13700003"), saved)
        self.assertIn(("2026-11-08", "c"), saved)
        self.assertEqual(cp, datetime(2026, 11, 6))


class EmptyWindowAs404(unittest.TestCase):
    def test_pmc_404_with_no_records_body_is_no_records_match(self):
        # PMC, 9 Nov 2026: from=until=2006-07-02 answers HTTP 404 with <error code="noRecordsMatch">.
        import requests
        from sickle.oaiexceptions import NoRecordsMatch
        body = (b'<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
                b'<responseDate>2026-11-09T15:15:28Z</responseDate><request>https://pmc.example/oai</request>'
                b'<error code="noRecordsMatch">No records match the query.</error></OAI-PMH>')
        resp = requests.Response()
        resp.status_code, resp._content, resp.url = 404, body, "https://pmc.example/oai"
        resp.encoding = "utf-8"
        with patch.object(R, "_tls_tolerant_request", lambda *a, **k: resp):
            sickle = R._get_my_sickle("https://pmc.example/oai")
            with self.assertRaises(NoRecordsMatch):
                sickle.ListRecords(metadataPrefix="oai_dc", **{"from": "2006-07-02", "until": "2006-07-02"})

    def test_pmc_404_unknown_identifier_is_id_does_not_exist(self):
        import requests
        from sickle.oaiexceptions import IdDoesNotExist
        body = (b'<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
                b'<responseDate>2026-11-09T15:40:00Z</responseDate><request>https://pmc.example/oai</request>'
                b'<error code="idDoesNotExist">Identifier does not exist.</error></OAI-PMH>')
        resp = requests.Response()
        resp.status_code, resp._content, resp.url, resp.encoding = 404, body, "https://pmc.example/oai", "utf-8"
        with patch.object(R, "_tls_tolerant_request", lambda *a, **k: resp):
            sickle = R._get_my_sickle("https://pmc.example/oai")
            with self.assertRaises(IdDoesNotExist):
                sickle.GetRecord(identifier="oai:pubmedcentral.nih.gov:99999999999", metadataPrefix="oai_dc")


class SlowHostTimeouts(unittest.TestCase):
    """The (connect, read) timeout each request goes out with (oxjob #227: IRUA needs ~90 s for an old window)."""

    def sent_timeout(self, url):
        import requests
        from sickle.oaiexceptions import NoRecordsMatch
        seen = {}
        resp = requests.Response()
        resp.status_code, resp.url, resp.encoding = 200, url, "utf-8"
        resp._content = (b'<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
                         b'<responseDate>2026-11-17T14:00:00Z</responseDate><request>x</request>'
                         b'<error code="noRecordsMatch">none</error></OAI-PMH>')

        def fake_request(method, u, **kwargs):
            seen.update(kwargs)
            return resp

        with patch.object(R, "_tls_tolerant_request", fake_request):
            with self.assertRaises(NoRecordsMatch):
                R._get_my_sickle(url).ListRecords(metadataPrefix="oai_dc", **{"from": "2002-12-31"})
        return seen["timeout"]

    def test_irua_gets_a_300_s_read_timeout(self):
        self.assertEqual(self.sent_timeout("https://repository.uantwerpen.be/oai/abua/server.phtml"),
                         (R.REQUEST_TIMEOUT, 300))

    def test_other_slow_hosts_keep_theirs(self):
        self.assertEqual(self.sent_timeout("https://et.ippt.pan.pl/index.php/et/oai"), (60, 300))
        self.assertEqual(self.sent_timeout("https://www.osti.gov/oai"), (30, 300))

    def test_any_other_host_gets_the_default(self):
        self.assertEqual(self.sent_timeout("https://repo.example.org/oai"), (R.REQUEST_TIMEOUT, R.READ_TIMEOUT))


def run_refusing_dates(feed, checkpoint, answers_undated=True, first=date(2026, 11, 5), last=date(2026, 11, 8)):
    """Like run(), on a server that answers 500 to any request carrying 'from' (oxjob #2626)."""
    state = SimpleNamespace(id="test-endpoint", pmh_url="https://example.org/oai", pmh_set=None,
                            metadata_prefix="oai_dc", most_recent_date_harvested=checkpoint,
                            fetch_via_zyte=False, error=None)
    saved, calls = [], []

    def request(self, sickle, **kw):
        calls.append(kw)
        if 'from' in kw:
            raise R.requests.exceptions.HTTPError("500 Server Error", response=SimpleNamespace(status_code=500))
        return feed

    def save_batch(self, s3_client, s3_bucket, batch_number, records, date_key, folder=None):
        saved.extend((date_key, r.header.identifier) for r in records)

    error = None
    with patch.object(R.EndpointHarvester, "detect_date_format", lambda self: "%Y-%m-%d"), \
            patch.object(R, "_get_my_sickle", lambda *a, **k: SimpleNamespace(deadline=None)), \
            patch.object(R.EndpointHarvester, "_make_oai_request", request), \
            patch.object(R.EndpointHarvester, "_answers_without_dates", lambda self, sickle, params: answers_undated), \
            patch.object(R.EndpointHarvester, "save_batch", save_batch):
        h = R.EndpointHarvester(state, FakeDb())
        try:
            h.harvest("bucket", first=first, last=last)
        except Exception as e:  # noqa: BLE001 - the test inspects it
            error = e
    return state.most_recent_date_harvested, saved, error, calls


# Mersin's feed undated, in identifier order: records from every year, two in the window
WHOLE_FEED = [rec("old1", "2019-04-02"), rec("new1", "2026-11-07"), rec("old2", "2024-01-15"), rec("new2", "2026-11-05"),
              rec("old3", "2026-11-04")]


class RefusesDateFilters(unittest.TestCase):
    def test_walks_undated_and_keeps_only_the_window(self):
        cp, saved, err, calls = run_refusing_dates(FakeFeed(WHOLE_FEED), START)
        self.assertIsNone(err)
        self.assertEqual(sorted(i for _, i in saved), ["new1", "new2"])
        self.assertNotIn('from', calls[-1])
        self.assertEqual(cp, datetime(2026, 11, 8))  # completed walk: the window's end

    def test_a_server_that_refuses_both_keeps_its_error_and_checkpoint(self):
        cp, saved, err, calls = run_refusing_dates(FakeFeed(WHOLE_FEED), START, answers_undated=False)
        self.assertIsInstance(err, R.requests.exceptions.HTTPError)
        self.assertEqual((cp, saved, len(calls)), (START, [], 1))

    def test_an_undated_walk_that_dies_keeps_the_starting_checkpoint(self):
        cp, saved, err, _ = run_refusing_dates(FakeFeed(WHOLE_FEED, fail_after=3, fail=R.requests.exceptions.ReadTimeout("x")), START)
        self.assertIsNotNone(err)
        self.assertEqual(cp, START)  # in no date order, so no mid-walk checkpoint: the next run re-walks

    def test_a_timeout_is_a_slow_server_not_a_refusal(self):
        feed = FakeFeed(WHOLE_FEED)
        with patch.object(R.EndpointHarvester, "_answers_without_dates", lambda self, sickle, params: True):
            state = SimpleNamespace(id="t", pmh_url="https://example.org/oai", pmh_set=None, metadata_prefix="oai_dc",
                                    most_recent_date_harvested=START, fetch_via_zyte=False, error=None)

            def request(self, sickle, **kw):
                if 'from' in kw:
                    raise R.requests.exceptions.ReadTimeout("read timed out")
                return feed
            with patch.object(R.EndpointHarvester, "detect_date_format", lambda self: "%Y-%m-%d"), \
                    patch.object(R, "_get_my_sickle", lambda *a, **k: SimpleNamespace(deadline=None)), \
                    patch.object(R.EndpointHarvester, "_make_oai_request", request):
                with self.assertRaises(R.requests.exceptions.ReadTimeout):
                    R.EndpointHarvester(state, FakeDb()).harvest("bucket", first=date(2026, 11, 5), last=date(2026, 11, 8))
        self.assertEqual(state.most_recent_date_harvested, START)

    def test_a_first_harvest_never_needs_it(self):
        cp, saved, err, calls = run_refusing_dates(FakeFeed(WHOLE_FEED), None, first=None)
        self.assertIsNone(err)
        self.assertEqual(len(saved), 5)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
