"""Offline tests for usaspending_awards: python -m unittest -v test_usaspending_awards."""
import csv
import http.client
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pyarrow.parquet as pq
import urllib3

from award_schemas import USASPENDING_DOE_SC_SCHEMA, USASPENDING_SCHEMA
import usaspending_awards as u

COLUMNS = [c for c in USASPENDING_SCHEMA.names if c not in ("archive_file", "ingested_at")] + ["sai_number"]


def tx(fain, action, mod="0", kind="04", agency="Department of Health and Human Services",
       sub="Centers for Disease Control and Prevention", title=None, amount="100.00"):
    row = {c: "" for c in COLUMNS}
    row.update(award_id_fain=fain, action_date=action, modification_number=mod, assistance_type_code=kind,
               awarding_agency_name=agency, awarding_sub_agency_name=sub, total_obligated_amount=amount,
               prime_award_base_transaction_description=title or f"{fain} {action} {mod}",
               assistance_transaction_unique_key=f"{fain}_{mod}_{action}", cfda_title="X", usaspending_permalink=f"https://x/{fain}")
    return row


def archive_zip(path, rows, members=1):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for m in range(members):
            out = io.StringIO()
            w = csv.DictWriter(out, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(rows[m::members])
            z.writestr(f"part_{m + 1}.csv", out.getvalue())


def listing(code, years, date="20261006"):
    return [(f"FY{y}_{code}_Assistance_Full_{date}.zip", 10) for y in years]


class ArchiveTests(unittest.TestCase):
    def test_newest_complete_set(self):
        keys = listing("075", range(2007, 2028)) + listing("075", range(2007, 2027), "20260906") + [("FY(All)_075_Assistance_Delta_20261006.zip", 5)]
        date, files = u.archive_files(keys, "075")
        self.assertEqual(date, "20261006")
        self.assertEqual([f[0] for f in files], list(range(2007, 2028)))
        with self.assertRaises(ValueError):
            u.archive_files(listing("075", [2007, 2008, 2010]), "075")    # a missing year
        with self.assertRaises(ValueError):
            u.archive_files(listing("075", range(2008, 2028)), "075")     # no FY2007
        with self.assertRaises(ValueError):
            u.archive_files(listing("089", range(2007, 2028)), "075")     # no files for the agency

    def test_an_incomplete_newest_set_is_refused(self):
        # Codex 11-06: a newly publishing set holding only FY2007-2009 passed while the previous set reached FY2026, and the
        # first run (no baseline to shrink against) would have published it. The set must reach its own date's fiscal year.
        with self.assertRaisesRegex(ValueError, "fiscal years 2007-2009 .*expected FY2007-FY2027"):
            u.archive_files(listing("075", range(2007, 2010)) + listing("075", range(2007, 2027), "20260906"), "075")
        with self.assertRaisesRegex(ValueError, "expected FY2007-FY2027"):
            u.archive_files(listing("075", range(2007, 2027)), "075")    # 20261006 is in FY2027: its file is missing
        with self.assertRaisesRegex(ValueError, r"an older set lists \[2028\]"):
            u.archive_files(listing("075", range(2007, 2028)) + listing("075", range(2007, 2029), "20260906"), "075")
        self.assertEqual(u.archive_files(listing("075", range(2007, 2027), "20260906"), "075")[0], "20260906")   # FY2026
        self.assertEqual((u.fiscal_year("20260930"), u.fiscal_year("20261001")), (2026, 2027))

    def build(self, years_rows):
        cdc = next(a for a in u.AGENCIES if a.name == "usaspending_cdc")
        ahrq = next(a for a in u.AGENCIES if a.name == "usaspending_ahrq")
        keys = listing("075", sorted(years_rows))
        downloaded = []

        def fake_download(session, url, path, expected_size=None):
            fy = int(url.rsplit("/", 1)[1][2:6])
            archive_zip(path, years_rows[fy], members=2)
            downloaded.append((fy, expected_size))

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with patch.object(u, "download_once", fake_download), patch.object(u.time, "sleep"):
            out = u.prepare_archive(None, keys, [cdc, ahrq], Path(tmp.name))
        self.assertEqual(sorted(downloaded), [(fy, 10) for fy in sorted(years_rows)])   # once per year, size checked
        self.assertEqual(sorted(p.name for p in Path(tmp.name).iterdir()), ["usaspending_ahrq.parquet", "usaspending_cdc.parquet"])
        return {k: pq.read_table(v) for k, v in out.items()}

    def test_filters_latest_transaction_and_one_download_for_two_rows(self):
        years = {y: [] for y in range(2007, 2028)}
        years[2020] = [tx("A1", "2019-11-01"), tx("A1", "2020-03-01", mod="1"), tx("B2", "2020-01-01", kind="10"),
                       tx("C3", "2020-02-01", sub="Agency for Healthcare Research and Quality"), tx("", "2020-01-05"),
                       tx("D4", "2020-05-05", agency="Department of Energy", sub="Office of Science")]
        years[2024] = [tx("A1", "2024-01-01", mod="2"), tx("A1", "2024-01-01", mod="3"), tx("E5", "2024-02-02", kind="05")]
        tables = self.build(years)
        cdc = tables["usaspending_cdc"]
        self.assertEqual(cdc.schema, USASPENDING_SCHEMA)
        self.assertEqual(cdc["award_id_fain"].to_pylist(), ["A1", "E5"])              # types 02-05, CDC only, FAIN set
        self.assertEqual(cdc["modification_number"].to_pylist()[0], "3")              # latest date, then latest mod
        self.assertTrue(cdc["archive_file"].to_pylist()[0].startswith("FY2024_075_"))
        self.assertEqual(tables["usaspending_ahrq"]["award_id_fain"].to_pylist(), ["C3"])
        total = json.loads(cdc.schema.metadata[b"openalex.funder_total"])   # what the funder-total gate reads
        self.assertEqual((total["value"], list(total["folded"].values()), list(total["excluded"].values())), (6, [3], [1]))
        self.assertEqual((total["archive_files"], total["archive_rows"]), (21, 9))
        self.assertEqual(json.loads(tables["usaspending_ahrq"].schema.metadata[b"openalex.funder_total"])["value"], 1)

    def test_modification_numbers_compare_as_numbers(self):
        # Codex 11-06: as strings, a same-day modification "9" sorted after "10", so the older transaction supplied the
        # award's description and dates.
        years = {y: [] for y in range(2007, 2028)}
        years[2025] = [tx("A1", "2025-03-01", mod="9", title="older"), tx("A1", "2025-03-01", mod="10", title="newer"),
                       tx("C3", "2025-03-01", mod="A9", sub="Agency for Healthcare Research and Quality", title="a9"),
                       tx("C3", "2025-03-01", mod="A10", sub="Agency for Healthcare Research and Quality", title="a10"),
                       tx("C3", "2025-03-01", mod="", sub="Agency for Healthcare Research and Quality", title="none")]
        tables = self.build(years)
        self.assertEqual(tables["usaspending_cdc"]["prime_award_base_transaction_description"].to_pylist(), ["newer"])
        self.assertEqual(tables["usaspending_ahrq"]["prime_award_base_transaction_description"].to_pylist(), ["a10"])
        self.assertEqual(sorted(["10", "9", "0010", "A10", "A9", "M002", None], key=lambda v: (u.modification_key(v) is not None, u.modification_key(v) or "")),
                         [None, "9", "10", "0010", "A9", "A10", "M002"])
        self.assertEqual(u.modification_key("2²"), u.modification_key("2") + "\x011²")   # Codex 11-06: "²".isdigit(), int() refuses it

    def test_empty_row_refuses(self):
        years = {y: [] for y in range(2007, 2028)}
        years[2020] = [tx("A1", "2020-01-01")]
        with self.assertRaises(ValueError):
            self.build(years)                                                           # AHRQ has no awards

    def test_missing_column_or_bad_crc_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.zip"
            with zipfile.ZipFile(path, "w") as z:
                z.writestr("a.csv", "award_id_fain,action_date\nA,2020-01-01\n")
            with self.assertRaises(ValueError):
                list(u.read_archive(path, ["award_id_fain", "cfda_title"]))
            archive_zip(path, [tx("A1", "2020-01-01")] * 200)
            data = bytearray(path.read_bytes())
            data[len(data) // 3] ^= 0xFF                                                # damage the compressed body
            path.write_bytes(bytes(data))
            with self.assertRaises((zipfile.BadZipFile, Exception)):
                list(u.read_archive(path, ["award_id_fain"]))

    def test_dropped_connection_is_retried_then_fails(self):
        calls = []

        def flaky(session, url, path, expected_size=None):
            calls.append(url)
            raise u.requests.ConnectionError("Remote end closed connection without response")
        with tempfile.TemporaryDirectory() as tmp, patch.object(u, "download_once", flaky), patch.object(u.time, "sleep"):
            with self.assertRaises(RuntimeError):
                u.fetch_file(None, "https://files.usaspending.gov/award_data_archive/x.zip", Path(tmp) / "x.zip", 10)
        self.assertEqual(len(calls), u.FILE_ATTEMPTS)

    def test_one_request_per_try_and_fetch_file_owns_every_wait(self):
        # Codex 11-06: download()'s own tries (1 s, 2 s apart) and the session adapter's retries ran inside each of
        # fetch_file's tries: 9+ requests in bursts while the CDN was refusing. Now: one request per try, and the only waits
        # are FILE_DELAY once and FILE_RETRY_WAIT between tries (urllib3's backoff would call the same time.sleep).
        made, waits = [], []

        def refuse(pool, conn, method, url, *a, **k):
            made.append(url)
            raise urllib3.exceptions.ProtocolError("Connection aborted.", http.client.RemoteDisconnected("closed"))
        with tempfile.TemporaryDirectory() as tmp, patch.object(urllib3.connectionpool.HTTPConnectionPool, "_make_request", refuse), \
                patch.object(u.time, "sleep", waits.append):
            with u.archive_session() as session, self.assertRaises(RuntimeError):
                u.fetch_file(session, "https://files.usaspending.gov/award_data_archive/x.zip", Path(tmp) / "x.zip", 10)
            self.assertFalse((Path(tmp) / "x.zip").exists())
        self.assertEqual(len(made), u.FILE_ATTEMPTS)
        self.assertEqual(waits, [u.FILE_DELAY] + [u.FILE_RETRY_WAIT] * (u.FILE_ATTEMPTS - 1))


class Response:
    def __init__(self, body, status=200):
        self.body, self.status_code = body, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise u.requests.HTTPError(str(self.status_code))

    def json(self):
        return self.body


class DoeScTests(unittest.TestCase):
    def test_year_slices_cap_to_months_and_dedup(self):
        posts = []

        class Session:
            headers = {}

            def __enter__(self): return self

            def __exit__(self, *a): pass

            def post(self, url, json=None, timeout=None):
                period = json["filters"]["time_period"][0]
                posts.append((period["start_date"], json["page"]))
                if period["start_date"] == "2009-01-01" and period["end_date"] == "2009-12-31":
                    return Response({"results": [{"Award ID": "X"}], "page_metadata": {"hasNext": True}})   # capped
                rows = [{"Award ID": f"DE-SC{period['start_date'][:4]}", "Award Amount": 12.9, "Description": "T",
                         "Recipient Name": "Lab", "Award Type": "Grant", "Start Date": "2008-01-01", "End Date": None}]
                return Response({"results": rows, "page_metadata": {"hasNext": False}})
        with tempfile.TemporaryDirectory() as tmp, patch.object(u, "http_session", Session), \
                patch.object(u, "DOE_SC_MAX_PAGES", 1), patch.object(u.time, "sleep"):
            out = u.build_doe_sc(Path(tmp), today=u.datetime(2010, 6, 1).date())
            table = pq.read_table(out)
        self.assertEqual(table.schema, USASPENDING_DOE_SC_SCHEMA)
        self.assertEqual(table["funder_award_id"].to_pylist(), ["DE-SC2008", "DE-SC2009", "DE-SC2010"])
        self.assertEqual(table["amount"].to_pylist(), ["12", "12", "12"])                 # whole dollars, as the laptop
        self.assertEqual(sum(1 for s, p in posts if s.startswith("2009-") and s != "2009-01-01"), 11)   # months 2-12

    def test_failed_page_stops_the_row(self):
        class Session:
            headers = {}

            def __enter__(self): return self

            def __exit__(self, *a): pass

            def post(self, *a, **k): return Response({}, status=503)
        with tempfile.TemporaryDirectory() as tmp, patch.object(u, "http_session", Session), patch.object(u.time, "sleep"):
            with self.assertRaises(RuntimeError):
                u.build_doe_sc(Path(tmp), today=u.datetime(2008, 6, 1).date())


class EntryTests(unittest.TestCase):
    def test_selection(self):
        self.assertEqual(u.selected({}), list(u.SOURCES))
        self.assertEqual(u.selected({"USASPENDING_SOURCES": "usaspending_cdc, usaspending_doe_sc"}),
                         ["usaspending_cdc", "usaspending_doe_sc"])
        with self.assertRaises(SystemExit):
            u.selected({"USASPENDING_SOURCES": "usaspending_nih"})

    def test_one_failed_agency_does_not_stop_the_others(self):
        uploaded = []

        def prepare(session, keys, agencies, directory):
            if agencies[0].code == "075":
                raise ValueError("archive moved")
            out = {}
            for a in agencies:
                out[a.name] = directory / f"{a.name}.parquet"
                out[a.name].write_bytes(b"x")
            return out
        with patch.dict(os.environ, {"USASPENDING_SOURCES": "usaspending_cdc,usaspending_epa,usaspending_doe_sc"}), \
                patch.object(u, "archive_listing", lambda s: []), patch.object(u, "prepare_archive", prepare), \
                patch.object(u, "run", lambda name, build, schema: uploaded.append(name)):
            with self.assertRaises(SystemExit) as stop:
                u.main()
        self.assertEqual(uploaded, ["usaspending_epa", "usaspending_doe_sc"])
        self.assertIn("usaspending_cdc", str(stop.exception))


if __name__ == "__main__":
    unittest.main()
