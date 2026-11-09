"""Offline checks for the NIH fetcher: python -m unittest -v test_nih_awards."""
from datetime import date, datetime, timezone
import json
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd
import pyarrow.parquet as pq

import award_fetch
from award_schemas import NIH_SCHEMA
import nih_awards as nih

HEADER = ('APPLICATION_ID,ACTIVITY,ADMINISTERING_IC,CORE_PROJECT_NUM,OPPORTUNITY NUMBER,FULL_PROJECT_NUM,FY,ORG_COUNTRY,'
          'ORG_NAME,PHR,PI_IDS,PI_NAMEs,PROJECT_START,PROJECT_END,PROJECT_TITLE,SUBPROJECT_ID,TOTAL_COST,TOTAL_COST_SUB_PROJECT')


def exporter_zip(path, rows, header=HEADER):
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('RePORTER_PRJ_C_FY2025.csv', header + '\n' + '\n'.join(rows) + '\n')


def record(appl_id, number, fy=2026, sub=None, pis=(), added='2026-05-02T17:04:38', **extra):
    rec = {'appl_id': appl_id, 'project_num': number, 'fiscal_year': fy, 'subproject_id': sub,
           'core_project_num': number[1:12], 'activity_code': number[1:4], 'award_amount': 1000,
           'agency_ic_admin': {'code': number[4:6], 'name': 'National Cancer Institute', 'abbreviation': 'NCI'},
           'organization': {'org_name': 'UNIV ', 'org_country': 'UNITED STATES', 'org_fips': 'US'},
           'project_num_split': {'appl_type_code': number[0], 'serial_num': number[6:12], 'support_year': '02'},
           'principal_investigators': list(pis), 'project_start_date': '2025-01-01T00:00:00',
           'project_end_date': '2027-11-30T00:00:00', 'project_title': 'T', 'phr_text': 'line one\nline two',
           'date_added': added}
    rec.update(extra)
    return rec


class FakeApi:
    """Serves projects/search: totals per date_added window and appl_id-sorted pages, as the RePORTER API does."""

    def __init__(self, records, page_hook=None):
        self.records, self.page_hook, self.calls = records, page_hook, 0

    def post(self, body):
        self.calls += 1
        c = body['criteria']
        lo, hi = c['date_added']['from_date'], c['date_added']['to_date']
        hits = sorted((r for r in self.records if r['fiscal_year'] in c['fiscal_years'] and lo <= r['date_added'] <= hi),
                      key=lambda r: r['appl_id'])
        page = hits[body['offset']:body['offset'] + body['limit']]
        if self.page_hook:
            page = self.page_hook(body, page)
        return {'meta': {'total': len(hits)}, 'results': page}


class NihTests(unittest.TestCase):
    def test_destination_and_fiscal_year(self):
        now = datetime(2026, 11, 9, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination('nih_reporter', now),
                             ('openalex-ingest', 'awards/nih_reporter/2026-11-09.parquet'))
        with patch.dict(os.environ, {'NIH_REPORTER_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/nih/'}):
            with self.assertRaises(ValueError):   # the laptop object's prefix stays read-only
                award_fetch.destination('nih_reporter', now)
        self.assertEqual(nih.current_fy(date(2026, 9, 30)), 2026)
        self.assertEqual(nih.current_fy(date(2026, 10, 1)), 2027)

    def test_api_record_in_exporter_formats(self):
        pis = [{'profile_id': 3, 'first_name': 'Luci', 'middle_name': '', 'last_name': 'Leykum', 'is_contact_pi': True},
               {'profile_id': 9, 'first_name': 'Stuti', 'middle_name': '', 'last_name': 'Dang', 'is_contact_pi': False},
               {'profile_id': 8, 'first_name': 'Erin', 'middle_name': 'P', 'last_name': 'Finley', 'is_contact_pi': False}]
        row = nih.to_exporter_row(record(11, '5R01CA123456-02', pis=pis, cong_dist='MA-7'))
        self.assertEqual(list(row), NIH_SCHEMA.names)
        # ExPORTER lists PIs by name, ids in the same order, ' (contact)' after the contact PI (FY2025 file)
        self.assertEqual(row['pi_names'], 'DANG, STUTI ;FINLEY, ERIN P;LEYKUM, LUCI  (contact)')
        self.assertEqual(row['pi_ids'], '9;8;3 (contact)')
        self.assertEqual(row['phr'], 'line one line two')
        self.assertEqual((row['org_name'], row['org_country'], row['org_district']), ('UNIV', 'UNITED STATES', '07'))
        self.assertEqual((row['project_start'], row['total_cost'], row['total_cost_sub_project']), ('2025-01-01', '1000', None))
        self.assertEqual((row['application_id'], row['fy'], row['support_year'], row['application_type']), ('11', 2026, '2', '5'))
        sub = nih.to_exporter_row(record(12, '5P30CA014195-53', sub=' 7632 '))
        self.assertEqual((sub['subproject_id'], sub['total_cost'], sub['total_cost_sub_project']), ('7632', None, '1000'))
        # contracts: ExPORTER's core is the number before the first '-'; intramural orgs have no country
        contract = nih.to_exporter_row(record(13, '75N97023F00079-P00004-0-1', core_project_num='75N97023F00',
                                              organization={'org_name': 'NINR', 'org_country': 'UNITED STATES'}))
        self.assertEqual((contract['core_project_num'], contract['org_country']), ('75N97023F00079', None))
        self.assertEqual(nih.to_exporter_row(record(14, '1R01CA000001-01'))['pi_names'], None)

    def test_windows_split_until_pageable_and_add_up(self):
        records = [record(i, f'5R01CA{i:06d}-02', added=f'2026-0{1 + i % 9}-1{i % 9}T10:00:00') for i in range(1, 40)]
        api = nih.Api(None, date(2026, 11, 5))
        api.post = FakeApi(records).post
        with patch.object(nih, 'WINDOW_MAX', 6), patch.object(nih, 'PAGE_SIZE', 4), patch.object(nih.time, 'sleep'):
            leaves, total = api.windows(2026)
            self.assertEqual(total, 39)
            self.assertTrue(all(n <= 6 for *_, n in leaves) and sum(n for *_, n in leaves) == 39)
            got = api.fiscal_year(2026)
        self.assertEqual(sorted(r['appl_id'] for r in got), list(range(1, 40)))

    def test_one_day_over_the_cap_stops(self):
        records = [record(i, f'5R01CA{i:06d}-02', added='2026-05-02T17:04:38') for i in range(1, 9)]
        api = nih.Api(None, date(2026, 11, 5))
        api.post = FakeApi(records).post
        with patch.object(nih, 'WINDOW_MAX', 5), self.assertRaisesRegex(ValueError, 'added on 2026-05-02 alone'):
            api.windows(2026)

    def test_short_repeated_or_moving_pages_stop(self):
        records = [record(i, f'5R01CA{i:06d}-02') for i in range(1, 11)]
        hooks = {'short': lambda body, page: page[:-1] if body['offset'] == 4 else page,
                 'repeated': lambda body, page: page[:1] * len(page) if body['offset'] == 4 else page}
        for name, hook in hooks.items():
            api = nih.Api(None, date(2026, 11, 5))
            api.post = FakeApi(records, hook).post
            with patch.object(nih, 'PAGE_SIZE', 4), self.assertRaisesRegex(ValueError, 'short or repeated'):
                api.fiscal_year(2026)
        fake = FakeApi(records)
        api = nih.Api(None, date(2026, 11, 5))
        def moving(body):
            out = fake.post(body)
            if body['offset'] == 4:
                out['meta']['total'] += 1
            return out
        api.post = moving
        with patch.object(nih, 'PAGE_SIZE', 4), self.assertRaisesRegex(ValueError, 'total moved'):
            api.fiscal_year(2026)

    def test_exporter_file_parsed_as_the_laptop_script(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'fy.zip'
            exporter_zip(path, ['1,R01,CA,R01CA1,PA-1,5R01CA000001-02,2025,UNITED STATES,U,p,9 (contact),"A, B (contact)",'
                                '2025-01-01,2026-01-01,T, 0038 ,,5', '2,R01,CA,R01CA2,,5R01CA000002-02,2025,,NA,,,,,,N/A,,,'])
            frame = pd.concat(list(nih.exporter_frames(path, 2025)))
        self.assertEqual(list(frame.columns), NIH_SCHEMA.names)
        self.assertEqual(frame['opportunity_number'].tolist()[0], 'PA-1')          # 'OPPORTUNITY NUMBER' -> opportunity_number
        self.assertEqual(frame['subproject_id'].tolist()[0], '0038')                # stripped, as the laptop script did
        self.assertTrue(pd.isna(frame['subproject_id'].tolist()[1]))
        self.assertTrue(pd.isna(frame['org_name'].tolist()[1]))                     # pandas' default NA values ('NA'), as before
        self.assertEqual(frame['fy'].dtype, 'int64')

    def test_truncated_zip_or_new_column_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'fy.zip'
            exporter_zip(path, ['1,R01,CA,R01CA1,,5R01CA000001-02,2025,,,,,,,,,,,'] * 2000)
            data = path.read_bytes()
            path.write_bytes(data[:len(data) // 2])                                  # ended early, as one 11-05 download did
            with self.assertRaises(zipfile.BadZipFile):
                list(nih.exporter_frames(path, 2025))
            exporter_zip(path, ['1,R01,CA,R01CA1,,5R01CA000001-02,2025,,,,,,,,,,,,x'], header=HEADER + ',NEW_COLUMN')
            with self.assertRaisesRegex(ValueError, 'new columns'):
                list(nih.exporter_frames(path, 2025))

    def test_build_newest_year_wins_and_writes_the_frozen_schema(self):
        api_records = [record(1, '5R01CA000001-02', pis=[{'profile_id': 7, 'first_name': 'A', 'middle_name': '',
                                                          'last_name': 'Z', 'is_contact_pi': True}]),
                       record(2, '5P30CA000009-05', sub='0001')]
        def download(session, url, path, **kwargs):
            fy = int(url.rsplit('/', 1)[1])
            exporter_zip(path, [f'{fy}1,R01,CA,R01CA000001,,5R01CA000001-02,{fy},,OLD,,,,,,old,,9,',
                                f'{fy}2,R01,CA,R01CA00000{fy % 10},,5R01CA00000{fy % 10}-01,{fy},,,,,,,,t,,9,'])
        class Session:
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): pass
        fake = FakeApi(api_records)
        with tempfile.TemporaryDirectory() as tmp, patch.object(nih, 'download', download), \
                patch.object(nih, 'http_session', Session), patch.object(nih.Api, 'post', lambda self, body: fake.post(body)):
            out = nih.build(Path(tmp), today=date(2026, 11, 5), first_fy=2023)
            table = pq.read_table(out)
            total = json.loads(pq.read_metadata(out).metadata[b'openalex.funder_total'])   # what the funder-total gate reads
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), [out.name])   # each ZIP is removed
        self.assertEqual(list(total['folded'].values()), [3])        # 5R01CA000001-02 again in FY2023-2025's files
        self.assertEqual(total['value'], table.num_rows + 3)
        self.assertEqual({k: v for k, v in total['by_year'].items() if 'ExPORTER' in k},
                         {'FY2025 ExPORTER file': 2, 'FY2024 ExPORTER file': 2, 'FY2023 ExPORTER file': 2})
        self.assertEqual(table.schema, NIH_SCHEMA)
        rows = table.to_pylist()
        first = [r for r in rows if r['full_project_num'] == '5R01CA000001-02']
        self.assertEqual(len(first), 1)                              # the API's FY2026 row beat three ExPORTER years
        self.assertEqual((first[0]['fy'], first[0]['project_title'], first[0]['pi_ids']), (2026, 'T', '7 (contact)'))
        self.assertEqual(sorted(r['fy'] for r in rows), [2023, 2024, 2025, 2026, 2026])


if __name__ == '__main__':
    unittest.main()
