"""Offline checks for the NHMRC outcomes converter: python -m unittest -v test_nhmrc_awards. No S3, no network."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from openpyxl import Workbook
import pyarrow as pa
import pyarrow.parquet as pq

from award_schemas import NHMRC_SCHEMA
import nhmrc_awards as nh

PRIMARY_COLS = nh.PRIMARY_COLUMNS


def primary_table(rows):
    """Frozen primary file shape: 64 string columns except application_round_year (int64), as the real object."""
    data = {c: [r.get(c) for r in rows] for c in PRIMARY_COLS}
    arrays = [pa.array([int(v) for v in data[c]], pa.int64()) if c == 'application_round_year'
              else pa.array(data[c], pa.string()) for c in PRIMARY_COLS]
    return pa.Table.from_arrays(arrays, names=PRIMARY_COLS)


def historical_table(rows):
    return pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in nh.HISTORICAL_COLUMNS]))


BASE = [
    {'grant_id': '2043672', 'app_id': '2043672', 'grant_title': 'Old title', 'cia_name': 'Prof Jane Roe', 'total': '100',
     'application_round_year': 2025, 'chief_investigator_team': 'Prof Jane Roe | Dr Sam Poe'},
    {'grant_id': '1100001', 'app_id': '1100001', 'grant_title': 'Kept', 'cia_name': 'Dr A/Pr Kim Lee', 'total': '5',
     'application_round_year': 2016, 'start_year': '2017'},
]
HIST = [{'award_id': '943301', 'application_year': '1993', 'title': 'Road Accident Research Unit', 'pi_name': 'Prof Allan McLean',
         'grant_type': 'Research Unit Grant', 'institution': 'University of Adelaide', 'start_year': '1994', 'end_year': '2000',
         'budget_total': '44441.19'}]


def workbook(path, header, rows, sheet='GRANTS DATA', preamble=2):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    for i in range(preamble):
        ws.append([f'NHMRC summary of results, note {i}'])
    ws.append(header)
    for r in rows:
        ws.append(r)
    wb.save(path)


class Inputs:
    def __init__(self, primary=BASE, historical=HIST, drops=()):
        self.p, self.h, self.d = primary, historical, list(drops)

    def primary(self):
        return primary_table(self.p)

    def historical(self):
        return historical_table(self.h)

    def drops(self):
        return self.d


class NhmrcTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        for name, value in (('PRIMARY_MIN_ROWS', 1), ('HISTORICAL_MIN_ROWS', 1)):
            p = patch.object(nh, name, value)
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        self.tmp.cleanup()

    def read(self, path):
        t = pq.read_table(path)
        self.assertTrue(t.schema.equals(NHMRC_SCHEMA, check_metadata=False))
        return t.to_pylist()

    def test_frozen_inputs_only(self):
        rows = self.read(nh.build(self.dir, Inputs()))
        self.assertEqual([r['part'] for r in rows], ['primary', 'primary', 'historical'])
        self.assertEqual(rows[0]['application_round_year'], '2025')           # int64 in the frozen file, a string here
        self.assertEqual(rows[0]['chief_investigator_team'], 'Prof Jane Roe | Dr Sam Poe')
        self.assertEqual(rows[2]['award_id'], '943301')
        self.assertEqual(rows[2]['grant_type'], 'Research Unit Grant')       # shared column, historical value
        self.assertIsNone(rows[2]['grant_id'])
        self.assertIsNone(rows[0]['award_id'])
        self.assertTrue(all(isinstance(v, (str, type(None))) for r in rows for v in r.values()))

    def test_drop_replaces_and_adds(self):
        xl = self.dir / 'in.xlsx'
        workbook(xl, ['Application ID', 'Application Title', 'Chief Investigator A (Project Lead)', 'CIA ORCID ID',
                      'Total Amount Awarded', 'Funding Scheme', 'Brand New Column'],
                 [['2043672', 'New title', 'Prof Jane Roe', '0000-0002-1825-0097', '200', 'Ideas Grants', 'x'],
                  ['2100001', 'A 2026 grant', 'Dr Ann Bo', None, '300', 'Investigator Grants', 'y']])
        rows = self.read(nh.build(self.dir, Inputs(drops=[('Summary-of-result-2026-app-round-15122026.xlsx',
                                                          '2026-12-16T00:00:00+00:00', xl)])))
        prim = {r['grant_id']: r for r in rows if r['part'] == 'primary'}
        self.assertEqual(sorted(prim), ['1100001', '2043672', '2100001'])
        self.assertEqual(prim['2043672']['grant_title'], 'New title')        # the drop replaced the baseline row
        self.assertEqual(prim['2043672']['cia_orcid'], '0000-0002-1825-0097')
        self.assertEqual(prim['2043672']['total'], '200')
        self.assertEqual(prim['2043672']['application_round_year'], '2026')
        self.assertEqual(prim['2100001']['app_id'], '2100001')               # grant_id falls back to the application id
        self.assertEqual(prim['1100001']['grant_title'], 'Kept')
        self.assertNotIn('brand_new_column', NHMRC_SCHEMA.names)              # unknown headers are reported and dropped

    def test_later_round_then_later_upload_wins(self):
        a, b = self.dir / 'a.xlsx', self.dir / 'b.xlsx'
        head = ['Grant ID', 'Grant Title', 'CIA Name', 'Grant Value']
        workbook(a, head, [['2043672', 'First issue', 'Prof Jane Roe', '1']])
        workbook(b, head, [['2043672', 'Re-issue', 'Prof Jane Roe', '2']])
        drops = [('Summary-of-result-2025-app-round-01102025.xlsx', '2025-10-02T00:00:00+00:00', a),
                 ('Summary-of-result-2025-app-round-22122025.xlsx', '2025-12-23T00:00:00+00:00', b)]
        prim = {r['grant_id']: r for r in self.read(nh.build(self.dir, Inputs(drops=drops))) if r['part'] == 'primary'}
        self.assertEqual(prim['2043672']['grant_title'], 'Re-issue')

    def test_a_reissue_with_padded_ids_is_one_grant(self):
        # Codex 11-06: re-issued files with "2043672" and " 2043672 " kept both drop rows (dedupe on the raw id) and still
        # removed the baseline row (replacement on the trimmed id). One canonical id for both: one row, the later upload's.
        a, b = self.dir / 'a.xlsx', self.dir / 'b.xlsx'
        head = ['Grant ID', 'Grant Title', 'CIA Name', 'Grant Value']
        workbook(a, head, [['2043672', 'First issue', 'Prof Jane Roe', '1'], ['2100002', 'Other', 'Dr B', '3']])
        workbook(b, head, [[' 2043672 ', 'Re-issue', 'Prof Jane Roe', '2']])
        drops = [('Summary-of-result-2025-app-round-01102025.xlsx', '2025-10-02T00:00:00+00:00', a),
                 ('Summary-of-result-2025-app-round-22122025.xlsx', '2025-12-23T00:00:00+00:00', b)]
        path = nh.build(self.dir, Inputs(drops=drops))
        prim = [r for r in self.read(path) if r['part'] == 'primary']
        ours = [r for r in prim if r['grant_id'].strip() == '2043672']
        self.assertEqual([r['grant_title'] for r in ours], ['Re-issue'])      # once, the later upload, no baseline copy
        self.assertEqual(sorted(r['grant_id'].strip() for r in prim), ['1100001', '2043672', '2100002'])
        total = json.loads(pq.read_metadata(path).metadata[b'openalex.funder_total'])   # what the gate reads
        self.assertEqual((total['value'], list(total['excluded'].values()), list(total['included'].values())),
                         (3, [1], [1, 1]))
        self.assertEqual([(f['round'], f['list_rows']) for f in total['files']], [(2025, 1), (2025, 2)])

    def test_refusals(self):
        xl = self.dir / 'x.xlsx'
        workbook(xl, ['Grant ID', 'Grant Title', 'CIA Name'], [['1', 'T', 'Dr X']])
        with self.assertRaisesRegex(ValueError, 'no round year'):
            nh.build(self.dir, Inputs(drops=[('outcomes.xlsx', '2026-01-01T00:00:00+00:00', xl)]))
        workbook(xl, ['Grant ID', 'Something', 'Else'], [['1', 'T', 'Dr X']])
        with self.assertRaisesRegex(ValueError, 'no .* column'):
            nh.build(self.dir, Inputs(drops=[('nhmrc_2026.xlsx', '2026-01-01T00:00:00+00:00', xl)]))
        with patch.object(nh, 'PRIMARY_MIN_ROWS', 3):
            with self.assertRaisesRegex(ValueError, 'frozen inputs short'):
                nh.build(self.dir, Inputs())
        with self.assertRaisesRegex(ValueError, 'columns changed'):
            nh.build(self.dir, BadHistorical())

    def test_round_year_patterns(self):
        for name, year in (('nhmrc_2025.xlsx', 2025), ('2017-application-round.xlsx', 2017),
                           ('1-summary_of_results_2022_app_round_24022023 (1).xlsx', 2022),
                           ('summary-of-results-2019-appround-07122019.xlsx', 2019)):
            self.assertEqual(nh.round_year(name), year)


class BadHistorical(Inputs):
    def historical(self):
        return pa.Table.from_pylist([dict(HIST[0], extra='1')])


if __name__ == '__main__':
    unittest.main()
