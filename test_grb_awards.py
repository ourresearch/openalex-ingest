"""Offline checks for the Taiwan GRB (NSTC) fetcher: python -m unittest -v test_grb_awards."""
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pyarrow.parquet as pq

import award_fetch
from award_schemas import GRB_SCHEMA
import grb_awards as grb

NSTC = grb.NSTC_ORGAN
NOW_ROC = datetime.now(timezone.utc).year - 1911
YEARS = list(range(grb.FIRST_YEAR, NOW_ROC + 1))


def row(id_, organ=NSTC, plan_no='', projkey='PB0000-0001', year=112, st='11201', en='11212', amt='100', pi='王小明',
        researchers='', title_zh='計畫', title_en='', inst='國立臺灣大學物理學系'):
    def tag(name, value):
        return f'<{name}/>' if value == '' else f'<{name}><![CDATA[{value}]]></{name}>'
    return ('<row>' + tag('ID', id_) + tag('PROJKEY', projkey) + tag('PLAN_NO', plan_no) + tag('PNCH_DESC', title_zh)
            + tag('PENG_DESC', title_en) + tag('EXCU_ORGAN_NAME', inst) + tag('PLAN_YEAR', year) + tag('PLAN_ORGAN_CODE', organ)
            + tag('RESEARCH_TYPE', '學術補助') + tag('RESEARCH_ATTRIBUTE', '基礎研究') + tag('RESEARCH_FIELD', '物理')
            + tag('PERIOD_STYM', st) + tag('PERIOD_ENYM', en) + tag('PLAN_AMT', amt) + tag('PI', pi)
            + tag('RESEARCHER', researchers) + tag('KEYWORD_C', '') + tag('KEYWORD_E', '')
            + tag('ABSTRACT_C', f'https://www.grb.gov.tw/search/planDetail2?id={id_}') + '</row>')


def year_zip(year, rows, name=None, head=''):
    body = f'<?xml version="1.0" encoding="UTF-8" standalone="no"?>{head}<GRB_DATA>{"".join(rows)}</GRB_DATA>'.encode()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr(name or f'GRB_{year}.xml', body)
    return buf.getvalue()


class Site:
    """Serves the year zips through the download() seam; a year missing from `files` answers as GRB does for an
    unpublished year (HTTP 200, empty body), unless `filler` gives it one NSTC row."""

    def __init__(self, files, filler=True):
        self.files, self.calls, self.filler = files, [], filler

    def download(self, session, url, path, expected_size=None):
        self.calls.append(url)
        year = int(url.rsplit('_', 2)[-2])
        if url != grb.FILE_URL.format(year=year):
            raise AssertionError(url)
        if year in self.files:
            path.write_bytes(self.files[year])
        elif self.filler:
            path.write_bytes(year_zip(year, [row('9', organ='農業部', year=year), row(f'{year}9', plan_no=f'FILL{year}', year=year)]))
        else:
            path.write_bytes(b'')


def build(site):
    with tempfile.TemporaryDirectory() as d, patch.object(grb, 'download', site.download), \
            patch.object(grb, 'MIN_PAST_YEAR_ROWS', 1), patch.object(grb, 'MIN_FIRST_YEAR_ROWS', 0):
        out = grb.build(Path(d))
        table = pq.read_table(out)
        left = sorted(p.name for p in Path(d).iterdir())
    return table, left


def full_site():
    return Site({y: year_zip(y, [row(f'{y}01', plan_no=f'NSTC{y}-0001', projkey=f'PB{y}-1', year=y, st=f'{y}01', en=f'{y}12')])
                 for y in YEARS})


class GrbTests(unittest.TestCase):
    def test_destination_and_monthly_schedule(self):
        bucket, key = award_fetch.destination('grb_nstc', datetime(2026, 12, 1, tzinfo=timezone.utc))
        self.assertEqual((bucket, key), ('openalex-ingest', 'awards/grb_nstc/2026-12-01.parquet'))
        with patch.dict(os.environ, {'GRB_NSTC_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/nstc_grb/'}):
            with self.assertRaises(ValueError):
                award_fetch.destination('grb_nstc', datetime(2026, 12, 1, tzinfo=timezone.utc))
        self.assertTrue(award_fetch.is_due('grb_nstc', datetime(2026, 12, 1, tzinfo=timezone.utc)))
        self.assertFalse(award_fetch.is_due('grb_nstc', datetime(2026, 12, 2, tzinfo=timezone.utc)))

    def test_aggregates_annual_rows_like_the_laptop_file(self):
        y0 = YEARS[1]
        files = {
            y0: year_zip(y0, [
                row('101', plan_no='NSTC 112-2112-M-002-001-MY2', year=y0, st=f'{y0}08', en=f'{y0 + 1}07', amt='1,000',
                    pi='Chen Wei-Ting', researchers='李柏青,吳文超', title_en='Planets'),
                row('102', plan_no='', projkey='PG11201-0001', year=y0, amt='0', pi='林陳美玲'),
                row('103', organ='衛生福利部', plan_no='MOHW112-1', year=y0),
            ]),
            y0 + 1: year_zip(y0 + 1, [
                row('201', plan_no='nstc112-2112-m-002-001-my2', year=y0 + 1, st=f'{y0 + 1}08', en=f'{y0 + 2}07', amt='500',
                    pi='Chen Wei-Ting', researchers='吳文超；蔡欣熹', title_en=''),
            ]),
        }
        site = Site(files)
        table, left = build(site)
        self.assertTrue(table.schema.equals(GRB_SCHEMA, check_metadata=False))
        self.assertEqual(left, ['grb_nstc.parquet'], 'the zips are removed')
        self.assertEqual(site.calls, [grb.FILE_URL.format(year=y) for y in YEARS])
        rows = {r['funder_award_id']: r for r in table.to_pylist() if not r['funder_award_id'].startswith('FILL')}
        self.assertEqual(sorted(rows), ['NSTC112-2112-M-002-001-MY2', 'PG11201-0001'])
        a = rows['NSTC112-2112-M-002-001-MY2']
        self.assertEqual(a['amount'], 1500000.0)                      # thousand TWD x 1000, summed over both years
        self.assertEqual((a['start_year'], a['end_year']), (str(y0 + 1911), str(y0 + 2 + 1911)))
        self.assertEqual(a['grb_ids'], ['101', '201'])
        self.assertEqual(a['source_row_count'], 2)
        self.assertEqual(a['display_name'], 'Planets')
        self.assertEqual(a['pi'], {'name': 'Chen Wei-Ting', 'given_name': 'Chen', 'family_name': 'Wei-Ting'})
        self.assertEqual([p['name'] for p in a['researchers']], ['李柏青', '吳文超'])   # first year that lists any, in GRB's order
        self.assertEqual(a['researchers'][0], {'name': '李柏青', 'given_name': None, 'family_name': '李柏青'})
        self.assertEqual(a['landing_page_url'], 'https://www.grb.gov.tw/search/planDetail?id=101')
        b = rows['PG11201-0001']
        self.assertIsNone(b['amount']); self.assertIsNone(b['currency'])
        self.assertEqual(b['pi']['family_name'], '林陳美玲')               # Chinese names stay whole; walden splits them
        self.assertEqual(b['display_name'], '計畫')
        total = json.loads(table.schema.metadata[b'openalex.funder_total'])   # what the gate compares rows with
        self.assertEqual(total['value'], len(YEARS) + 1)                   # GRB's NSTC annual rows: 101 and 201 are one plan
        self.assertEqual(total['folded'], {"a plan's other annual rows, merged into the plan's row": 1})
        self.assertEqual(total['value'] - sum(total['folded'].values()), table.num_rows)   # nothing unexplained
        self.assertEqual((total['nstc_annual_rows_by_roc_year'][str(y0)], total['all_agencies_rows_by_roc_year'][str(y0)]), (2, 3))
        self.assertNotIn('excluded', total)

    def test_a_plan_lost_between_reading_and_writing_is_left_unexplained(self):
        # folded comes from the plan numbers seen while reading, not from the written rows, so a plan dropped on the way to
        # the file is a row no counted reason explains: lost = value - rows - folded = 1 != sum(excluded) = 0, which is
        # funder_gate.py's FAIL ('rows missing that no counted reason explains')
        aggregate = grb.aggregate
        with patch.object(grb, 'aggregate', lambda rows, stamp: aggregate(rows, stamp)[1:]):
            table, _ = build(full_site())
        total = json.loads(table.schema.metadata[b'openalex.funder_total'])
        lost = total['value'] - table.num_rows - sum(total['folded'].values())
        self.assertEqual((total['value'], table.num_rows, lost, sum((total.get('excluded') or {}).values())),
                         (len(YEARS), len(YEARS) - 1, 1, 0))

    def test_reruns_write_the_same_rows(self):
        first, _ = build(full_site())
        second, _ = build(full_site())
        drop = lambda t: t.drop(['ingested_at']).to_pylist()
        self.assertEqual(drop(first), drop(second))

    def test_unpublished_current_year_is_skipped_but_a_past_year_refuses(self):
        site = full_site()
        del site.files[YEARS[-1]]
        site.filler = False
        table, _ = build(site)
        self.assertNotIn(f'NSTC{YEARS[-1]}-0001', table.column('funder_award_id').to_pylist())
        self.assertEqual(table.num_rows, len(YEARS) - 1)
        site = full_site()
        del site.files[YEARS[1]]
        site.filler = False
        with self.assertRaisesRegex(ValueError, 'past year not published'):
            build(site)
        site = full_site()
        site.files[YEARS[0]] = b'<html>busy</html>'
        with self.assertRaisesRegex(ValueError, 'not a zip'):
            build(site)

    def test_unexpected_zip_member_refuses(self):
        y = YEARS[0]
        site = full_site()
        site.files[y] = year_zip(y, [row('1', year=y)], name='other.xml')
        with self.assertRaisesRegex(ValueError, 'expected GRB_'):
            build(site)

    def test_short_past_year_refuses(self):
        site = full_site()
        site.files[YEARS[1]] = year_zip(YEARS[1], [row('5', organ='農業部', year=YEARS[1])])
        with patch.object(grb, 'download', site.download), tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(ValueError, 'NSTC rows in a past year'):
                grb.build(Path(d))

    def test_row_without_id_refuses(self):
        site = full_site()
        site.files[YEARS[0]] = year_zip(YEARS[0], [row('', year=YEARS[0])])
        with self.assertRaisesRegex(ValueError, 'no numeric ID'):
            build(site)

    def test_external_entities_are_not_resolved(self):
        y = YEARS[0]
        secret = Path(tempfile.mkdtemp()) / 'secret.txt'
        secret.write_text('TOP-SECRET')
        head = f'<!DOCTYPE GRB_DATA [<!ENTITY x SYSTEM "file://{secret}">]>'
        evil = row('7', plan_no='NSTC111-0007', year=y).replace('<PI><![CDATA[王小明]]></PI>', '<PI>&x;</PI>')
        site = full_site()
        site.files[y] = year_zip(y, [evil], head=head)
        table, _ = build(site)
        pis = [r['pi'] for r in table.to_pylist() if r['funder_award_id'] == 'NSTC111-0007']
        self.assertTrue(all(p is None or 'TOP-SECRET' not in (p['name'] or '') for p in pis))


if __name__ == '__main__':
    unittest.main()
