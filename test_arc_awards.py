"""Offline checks for the ARC fetcher: python -m unittest -v test_arc_awards."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

import pyarrow.parquet as pq

import award_fetch
from award_schemas import ARC_SCHEMA
import arc_awards as arc


def grant(code, lead='Prof Ada Zed', investigators='Prof Ada Zed', funder='Australian Research Council'):
    return {'type': 'grants', 'id': code, 'attributes': {
        'code': code, 'program-name': 'Discovery', 'scheme-name': 'Discovery Projects', 'funding-commencement-year': 2022,
        'scheme-information': {'scheme-code': 'DP  ', 'program': 'Discovery', 'submission-year': 2021, 'round-number': 1,
                               'scheme-round': 'DP22 Round 1'},
        'grantee': 'The University of Sydney', 'grant-funder': funder, 'grant-title': f'Title {code}',
        'grant-summary': 'Summary', 'lead-investigator': lead, 'grant-value': 400000.0, 'grant-status': 'Active',
        'primary-field-of-research': '4410 - Sociology', 'anticipated-end-date': '2025-06-30',
        'investigators': investigators, 'grant-priorities': []}}


def person(title, first, family, role='Chief Investigator', code='CI', orcid=None):
    return {'title': title, 'first-name': first, 'family-name': family, 'role-name': role, 'role-code': code,
            'is-fellowship': False, 'orcid-identifier': orcid}


def detail(code, people, start='2022-02-01'):
    return {'data': {'type': 'grant-details', 'id': code, 'attributes': {
        'code': code, 'project-start-date': start, 'investigators': people,
        'organisations': [{'organisation-name': 'The University of Sydney', 'role-name': 'Administering Organisation',
                           'state': 'NSW'}]}}}


class Api:
    """Serves list pages and detail records through the _get_json seam; page size 2, so paging is exercised."""

    def __init__(self, grants, details, size=2):
        self.grants, self.details, self.size, self.calls = grants, details, size, []
        self.override = {}

    def get_json(self, session, url):
        parsed = urlparse(url)
        self.calls.append(url)
        if parsed.path.endswith('/grants'):
            q = parse_qs(parsed.query)
            page = int(q['page[number]'][0])
            if page in self.override:
                return 200, self.override[page]
            pages = -(-len(self.grants) // self.size)
            chunk = self.grants[(page - 1) * self.size: page * self.size]
            return 200, {'meta': {'total-size': len(self.grants), 'total-pages': pages, 'actual-page-size': len(chunk)},
                         'data': chunk}
        code = unquote(parsed.path.rsplit('/', 1)[-1])
        if code not in self.details:
            return 400, None
        return 200, self.details[code]


GRANTS = [grant('DP220100001', lead='Em/Prof Ada Zed', investigators='Dr Bo Abel; Em/Prof Ada Zed'),
          grant('dp220100001'),                                           # same code, other case: first page wins
          grant('LP220100002', lead='Dr Cy Moe', investigators='Dr Cy Moe'),
          grant('NI220100003', lead='Prof Di Lu', investigators='Prof Di Lu', funder='Office of National Intelligence')]
DETAILS = {'DP220100001': detail('DP220100001', [person('Dr', 'Bo', 'Abel', orcid='0000-0002-7747-4015'),
                                                 person('Em/Prof', 'Ada', 'Zed'),
                                                 person('Mr', 'Ed', 'Kay', 'Partner Investigator', 'PI')]),
           'LP220100002': detail('LP220100002', [person('Dr', 'Cy', 'Moe', 'Discovery Early Career Award', 'DE')], None),
           'NI220100003': detail('NI220100003', [person('Prof', 'Di', 'Lu')])}


class ArcTests(unittest.TestCase):
    def build(self, api, missing_ok=True):
        with tempfile.TemporaryDirectory() as tmp, patch.object(arc, '_get_json', api.get_json), \
                patch.object(arc.time, 'sleep'), patch.object(arc, 'PAGE_SIZE', api.size), \
                patch.object(arc, 'MAX_MISSING_DETAIL', 1.0 if missing_ok else 0.0):
            out = arc.build(Path(tmp))
            table = pq.read_table(out)
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), [out.name])
            return table

    def test_destination_and_monthly_schedule(self):
        now = datetime(2026, 12, 1, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination('arc_rgs', now), ('openalex-ingest', 'awards/arc_rgs/2026-12-01.parquet'))
        with patch.dict(os.environ, {'ARC_RGS_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/arc/'}):
            with self.assertRaises(ValueError):
                award_fetch.destination('arc_rgs', now)
        self.assertTrue(award_fetch.is_due('arc_rgs', now))
        self.assertFalse(award_fetch.is_due('arc_rgs', datetime(2026, 12, 2)))

    def test_rows_people_lead_first_and_contract(self):
        table = self.build(Api(GRANTS, DETAILS))
        self.assertTrue(table.schema.equals(ARC_SCHEMA, check_metadata=False))
        rows = {r['code']: r for r in table.to_pylist()}
        self.assertEqual(sorted(rows), ['DP220100001', 'LP220100002', 'NI220100003'])
        a = rows['DP220100001']
        self.assertEqual((a['start_year'], a['submission_year'], a['scheme_code'], a['end_date'], a['grant_value']),
                         (2022, 2021, 'DP', '2025-06-30', 400000.0))
        self.assertEqual((a['lead_investigator'], a['investigators']), ('Em/Prof Ada Zed', 'Dr Bo Abel; Em/Prof Ada Zed'))
        self.assertEqual((a['project_start_date'], a['detail_status']), ('2022-02-01', 'ok'))
        # the lead (matched on title + names) moves first; the rest keep ARC's order
        self.assertEqual([(p['given_name'], p['family_name'], p['title'], p['role_code'], p['is_lead'], p['orcid'])
                          for p in a['people']],
                         [('Ada', 'Zed', 'Em/Prof', 'CI', True, None), ('Bo', 'Abel', 'Dr', 'CI', False, '0000-0002-7747-4015'),
                          ('Ed', 'Kay', 'Mr', 'PI', False, None)])
        self.assertEqual(a['organisations'], [{'name': 'The University of Sydney', 'role_name': 'Administering Organisation',
                                               'state': 'NSW'}])
        self.assertIsNone(rows['LP220100002']['project_start_date'])
        self.assertEqual(rows['NI220100003']['grant_funder'], 'Office of National Intelligence')   # kept; walden decides
        total = json.loads(table.schema.metadata[b'openalex.funder_total'])   # what the gate compares rows with
        self.assertEqual(total, {'measure': 'ARC RGS /grants list meta total-size', 'value': 4,
                                 'excluded': {'no grant code': 0, 'grant code repeated, any case (first page wins)': 1},
                                 'written without a detail record': 0})

    def test_lead_matched_without_title_and_unmatched_lead_is_first_person(self):
        out = arc.detail_fields(detail('X', [person('Dr', 'Bo', 'Abel'), person('Professor', 'Ada', 'Zed')])['data']['attributes'],
                                'Prof Ada Zed')
        self.assertEqual([(p['family_name'], p['is_lead']) for p in out['people']], [('Zed', True), ('Abel', False)])
        out = arc.detail_fields(detail('X', [person('Dr', 'Bo', 'Abel'), person('Dr', 'Cy', 'Moe')])['data']['attributes'],
                                'Prof Nobody Here')
        self.assertEqual([(p['family_name'], p['is_lead']) for p in out['people']], [('Abel', True), ('Moe', False)])
        self.assertEqual(arc.detail_fields({}, 'Prof A B')['people'], [])

    def test_missing_detail_kept_or_refused(self):
        details = dict(DETAILS); del details['LP220100002']
        table = self.build(Api(GRANTS, details))
        rows = {r['code']: r for r in table.to_pylist()}
        self.assertEqual((rows['LP220100002']['detail_status'], rows['LP220100002']['people']), ('missing', None))
        total = json.loads(table.schema.metadata[b'openalex.funder_total'])   # written, so not excluded; noted beside
        self.assertEqual((total['value'], total['written without a detail record']), (4, 1))
        with self.assertRaisesRegex(ValueError, 'no readable detail record'):
            self.build(Api(GRANTS, details), missing_ok=False)

    def test_totals_moving_or_repeated_pages_stop_the_harvest(self):
        api = Api(GRANTS, DETAILS)
        api.override[2] = {'meta': {'total-size': 5, 'total-pages': 3}, 'data': GRANTS[2:4]}
        with self.assertRaisesRegex(ValueError, 'totals changed'):
            self.build(api)
        api = Api(GRANTS, DETAILS)
        api.override[2] = {'meta': {'total-size': 4, 'total-pages': 2}, 'data': GRANTS[:2]}
        with self.assertRaisesRegex(ValueError, 'short or repeated'):
            self.build(api)
        api = Api(GRANTS, DETAILS)
        api.override[2] = {'meta': {'total-size': 4, 'total-pages': 2}, 'data': GRANTS[2:3]}
        with self.assertRaisesRegex(ValueError, 'short or repeated'):
            self.build(api)

    def test_failed_list_page_never_uploads(self):
        with patch('sys.argv', ['arc_awards']), patch.object(arc, '_get_json', lambda s, u: (None, None)), \
                patch.object(arc.time, 'sleep'), patch.object(award_fetch.boto3, 'client') as client:
            with self.assertRaisesRegex(RuntimeError, 'list page 1 failed'):
                arc.main()
            client.assert_not_called()

    def test_pacer_spaces_requests(self):
        pacer, slept = arc._Pacer(2.0), []
        with patch.object(arc.time, 'sleep', slept.append), patch.object(arc.time, 'monotonic', side_effect=[10.0, 10.0, 10.1, 10.5]):
            pacer.wait(); pacer.wait()
        self.assertEqual(len(slept), 1)
        self.assertAlmostEqual(slept[0], 0.4)   # 0.1 s had already passed


if __name__ == '__main__':
    unittest.main()
