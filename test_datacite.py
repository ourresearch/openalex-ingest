"""Offline checks for datacite.py's hourly checkpoint and re-check (oxjob #2284): python -m unittest -v test_datacite.
No network or S3."""
import datetime
import gzip
import json
import unittest
from unittest.mock import patch

import requests

import datacite as D

UTC = datetime.timezone.utc


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey()
        body = self.objects[Key]

        class Body:
            def read(self_inner):
                return body
        return {"Body": Body()}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, _):
        objects = self.objects

        class Pages:
            def paginate(self_inner, Bucket, Prefix):
                return [{"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}]
        return Pages()


def work(doi, updated):
    return {"id": doi, "attributes": {"updated": updated}}


class FakeDataCite:
    """Serves hour pulls (one page per hour), counts, day listings and 100-DOI fetches from a dict of records."""

    def __init__(self, records, short_hours=(), fail_hours=(), extra_count=None):
        self.records = records               # doi -> updated ISO string
        self.short_hours = set(short_hours)  # hours whose first pull drops one record
        self.fail_hours = set(fail_hours)    # hours whose pull always raises
        self.extra_count = extra_count or {}
        self.pulls = {}

    def in_window(self, query):
        start, end = query[len("updated:["):-1].split(" TO ")
        return sorted(d for d, u in self.records.items() if start <= u <= end)

    def __call__(self, url, params=None):
        params = params or {}
        if "query" in params and params["query"].startswith("doi:("):
            dois = [d.strip('"') for d in params["query"][5:-1].split(" OR ")]
            return {"data": [work(d, self.records[d]) for d in dois if d in self.records]}
        if "query" in params:   # count
            q = params["query"]
            return {"meta": {"total": len(self.in_window(q)) + self.extra_count.get(q, 0)}}
        query = url.split("query=")[1].split("&")[0]
        hour = query[len("updated:["):len("updated:[") + 13]
        if hour in self.fail_hours:
            raise requests.exceptions.ConnectionError("boom")
        dois = self.in_window(query)
        n = self.pulls[hour] = self.pulls.get(hour, 0) + 1
        if hour in self.short_hours and n == 1:
            dois = dois[1:]
        return {"meta": {"total": len(dois)}, "data": [work(d, self.records[d]) for d in dois], "links": {}}


def s3_ids(fake_s3, prefix="datacite/works/"):
    ids = []
    for key, body in fake_s3.objects.items():
        if key.startswith(prefix):
            ids.extend(w["id"] for w in json.loads(gzip.decompress(body)))
    return ids


class DataCiteHarvestTest(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        patches = [patch.object(D, "s3", self.s3), patch.object(D.time, "sleep", lambda s: None)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def run_hours(self, api, start, end, checkpoint=True):
        with patch.object(D, "get_json", api):
            return D.pull_hours(start, end, checkpoint)

    def test_short_hour_is_pulled_again(self):
        api = FakeDataCite({"10.1/a": "2026-11-15T10:05:00Z", "10.1/b": "2026-11-15T10:50:00Z"},
                           short_hours={"2026-11-15T10"})
        self.run_hours(api, datetime.datetime(2026, 11, 15, 10, tzinfo=UTC), datetime.datetime(2026, 11, 15, 11, tzinfo=UTC))
        self.assertEqual(api.pulls["2026-11-15T10"], 2)
        self.assertEqual(set(s3_ids(self.s3)), {"10.1/a", "10.1/b"})

    def test_checkpoint_stops_at_the_hour_that_failed(self):
        api = FakeDataCite({"10.1/a": "2026-11-15T10:05:00Z", "10.1/b": "2026-11-15T11:05:00Z"},
                           fail_hours={"2026-11-15T11"})
        with self.assertRaises(RuntimeError):
            self.run_hours(api, datetime.datetime(2026, 11, 15, 10, tzinfo=UTC), datetime.datetime(2026, 11, 15, 13, tzinfo=UTC))
        self.assertEqual(json.loads(self.s3.objects[D.CHECKPOINT_KEY])["next_hour"], "2026-11-15T11")
        self.assertNotIn(D.LOCK_KEY, self.s3.objects)

    def test_live_lock_stops_a_second_run(self):
        self.s3.put_object(None, D.LOCK_KEY, json.dumps({"run": "other", "at": D.utcnow().isoformat()}), None)
        api = FakeDataCite({"10.1/a": "2026-11-15T10:05:00Z"})
        ran = self.run_hours(api, datetime.datetime(2026, 11, 15, 10, tzinfo=UTC), datetime.datetime(2026, 11, 15, 11, tzinfo=UTC))
        self.assertFalse(ran)
        self.assertEqual(s3_ids(self.s3), [])

    def test_backfill_leaves_the_checkpoint_alone(self):
        api = FakeDataCite({"10.1/a": "2025-08-20T10:05:00Z"})
        self.run_hours(api, datetime.datetime(2025, 8, 20, tzinfo=UTC), datetime.datetime(2025, 8, 21, tzinfo=UTC), False)
        self.assertEqual(s3_ids(self.s3), ["10.1/a"])
        self.assertNotIn(D.CHECKPOINT_KEY, self.s3.objects)

    def test_recheck_fetches_only_what_the_day_folder_lacks(self):
        records = {f"10.1/{i}": "2026-10-05T02:31:00Z" for i in range(250)}
        with patch.object(D, "get_json", FakeDataCite(records)):
            D.write_batch(datetime.datetime(2026, 10, 5, tzinfo=UTC), "batch_02_1",
                          [work(f"10.1/{i}", records[f"10.1/{i}"]) for i in range(100)])
            with patch.object(D, "listed_ids", lambda day: sorted(records)):
                fetched = D.recheck_day(datetime.datetime(2026, 10, 5, tzinfo=UTC))
                again = D.recheck_day(datetime.datetime(2026, 10, 5, tzinfo=UTC))
        self.assertEqual((fetched, again), (150, 0))
        self.assertEqual(sorted(s3_ids(self.s3)), sorted(records))

    def test_listing_that_stops_short_resumes_from_its_last_page(self):
        records = [f"10.1/{i:04d}" for i in range(2500)]
        cut = {"done": False}

        def api(url, params=None):
            if params:   # count
                return {"meta": {"total": len(records)}}
            offset = int(url.split("page[cursor]=")[1].split("&")[0]) - 1
            page = records[offset:offset + 1000]
            if offset == 1000 and not cut["done"]:   # DataCite ends the listing mid-way once
                cut["done"] = True
                return {"data": [{"id": d} for d in page[:400]], "links": {}}
            links = {"next": f"x?page[cursor]={offset + 1001}&q"} if offset + 1000 < len(records) else {}
            return {"data": [{"id": d} for d in page], "links": links}

        with patch.object(D, "get_json", api):
            ids = D.listed_ids(datetime.datetime(2025, 12, 17, tzinfo=UTC))
        self.assertTrue(cut["done"])
        self.assertEqual(ids, {d.lower() for d in records})

    def test_one_failed_recheck_day_does_not_stop_the_others(self):
        done = []

        def recheck(day):
            if day.day == 2:
                raise RuntimeError("short listing")
            done.append(day.day)

        with patch.object(D, "recheck_day", recheck), patch.object(D.sentry_sdk, "capture_exception", lambda e: None):
            with self.assertRaisesRegex(RuntimeError, "2026-11-02"):
                D.recheck_days(D.days("2026-11-01", "2026-11-03"))
        self.assertEqual(done, [1, 3])


if __name__ == "__main__":
    unittest.main()
