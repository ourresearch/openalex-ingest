"""Offline checks for the NWO fetcher: python -m unittest -v test_nwo_awards."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq

import award_fetch
from award_schemas import NWO_SCHEMA
import nwo_awards as nwo


def member(mid, role, last, first=None, orcid=None, org='Universiteit Leiden||Faculteit', ror='https://ror.org/027bh9e22',
           active=True):
    m = {'role': role, 'member_id': mid, 'last_name': last, 'initials': (first or 'X')[0] + '.', 'organisation': org,
         'organisation_id': 80319, 'ror': ror, 'active': active, 'degree_pre_nominal': 'dr.'}
    if first:
        m['first_name'] = first
    if orcid is not None:
        m['orcid'] = orcid
    return m


def project(pid, members=(), **extra):
    p = {'project_id': pid, 'title': f'Title {pid}', 'funding_scheme_id': 2327, 'funding_scheme': 'Veni', 'department': '',
         'sub_department': 'NWO-breed', 'start_date': '2019-07-01T00:00:00', 'end_date': '2023-07-07T00:00:00',
         'reporting_year': 2018, 'summary_nl': 'NL', 'summary_en': '', 'project_members': list(members)}
    p.update(extra)
    return p


PROJECTS = [
    project('013.18.001', [member(1, 'Researcher', 'Linda', 'Katrin'),
                           member(2, 'Project leader', 'Kasri', 'Nael', active=False),
                           member(2, 'Main Applicant', 'Kasri', 'Nael', orcid='https://orcid.org/0000-0002-7747-4015'),
                           member(3, 'Co-applicant', 'Abel', 'Bo', orcid='https://orcid.org/-')],
            award_amount=250000, grant_id='https://doi.org/10.61686/ENPNZ86422', products=[{'title': 'Paper'}]),
    project('014-24-809', [member(4, 'Project leader', 'Moe', 'Di')], parent_project_id='014.24.800',
            department='ENW', start_date=None),
    project('013.18.001', [member(9, 'Main Applicant', 'Late', 'Copy')]),        # listed twice: first page wins
    project('016.Veni.173.076', []),
    project('024.003.012', [member(5, 'Researcher', 'Zed', 'Ada'), member(6, 'Researcher', 'Abel', 'Cy')]),
]


class Api:
    """Serves NWOpen-shaped pages through the download() seam; page size 2, so paging is exercised."""

    def __init__(self, projects, size=2, date='2026-11-05T00:00:00'):
        self.projects, self.size, self.date, self.calls, self.override = projects, size, date, [], {}

    def body(self, page, projects=None, count=None, date=None):
        items = self.projects if projects is None else projects
        count = len(self.projects) if count is None else count
        pages = -(-count // self.size)
        chunk = items[(page - 1) * self.size: page * self.size] if projects is None else items
        return {'meta': {'api_type': 'NWO Projects API', 'count': count, 'per_page': self.size, 'pages': pages,
                         'page': page, 'date': date or self.date}, 'projects': chunk}

    def download(self, session, url, path, **kwargs):
        query = parse_qs(urlparse(url).query)
        page = int(query['page'][0])
        self.calls.append(page)
        path.write_bytes(json.dumps(self.override.get(page) or self.body(page)).encode())


class NwoTests(unittest.TestCase):
    def build(self, api, duplicates_ok=True):
        with tempfile.TemporaryDirectory() as tmp, patch.object(nwo, 'download', api.download), \
                patch.object(nwo.time, 'sleep'), patch.object(nwo, 'PAGE_SIZE', api.size), \
                patch.object(nwo, 'MAX_DUPLICATE_IDS', 1.0 if duplicates_ok else 0.0):
            out = nwo.build(Path(tmp))
            table = pq.read_table(out)
            self.footer = pq.read_metadata(out).metadata   # the file's key-value metadata, as the gate reads it
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), [out.name])   # page files are removed
            return table

    def test_destination_and_weekly_cron_runs_without_scheduled(self):
        now = datetime(2026, 11, 16, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination('nwopen', now), ('openalex-ingest', 'awards/nwopen/2026-11-16.parquet'))
        with patch.dict(os.environ, {'NWOPEN_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/nwo/'}):
            with self.assertRaises(ValueError):                  # the old laptop prefix is refused
                award_fetch.destination('nwopen', now)

    def test_rows_laptop_columns_and_members(self):
        api = Api(PROJECTS)
        table = self.build(api)
        self.assertTrue(table.schema.equals(NWO_SCHEMA, check_metadata=False))
        self.assertEqual(api.calls, [1, 2, 3])
        rows = {r['project_id']: r for r in table.to_pylist()}
        self.assertEqual(sorted(rows), ['013.18.001', '014-24-809', '016.Veni.173.076', '024.003.012'])
        a = rows['013.18.001']
        # the laptop file's lead: the Main Applicant, even when listed after a Project leader row
        self.assertEqual((a['pi_given_name'], a['pi_family_name'], a['pi_id']),
                         ('Nael', 'Kasri', 'https://orcid.org/0000-0002-7747-4015'))
        self.assertEqual((a['abstract'], a['grant_category'], a['lead_funder'], a['lead_org_name']),
                         ('NL', 'Veni', 'NWO-breed', 'Universiteit Leiden'))
        self.assertEqual((a['start_date'], a['end_date'], a['reporting_year'], a['funding_scheme_id']),
                         ('2019-07-01', '2023-07-07', 2018, 2327))
        self.assertEqual((a['award_amount'], a['grant_doi'], a['products_json']),
                         (250000.0, '10.61686/ENPNZ86422', '[{"title": "Paper"}]'))
        self.assertEqual([(m['member_id'], m['role'], m['roles'], m['orcid']) for m in a['members']], [
            ('2', 'Main Applicant', ['Main Applicant', 'Project leader'], 'https://orcid.org/0000-0002-7747-4015'),
            ('3', 'Co-applicant', ['Co-applicant'], None),          # the 'https://orcid.org/-' placeholder is dropped
            ('1', 'Researcher', ['Researcher'], None)])
        self.assertTrue(a['members'][0]['active'])                 # the best role's row
        self.assertEqual((a['pi_member_id'], a['pi_ror']), ('2', 'https://ror.org/027bh9e22'))
        b = rows['014-24-809']
        self.assertEqual((b['pi_family_name'], b['parent_project_id'], b['lead_funder'], b['start_date'], b['award_amount']),
                         ('Moe', '014.24.800', 'ENW', None, None))
        v = rows['016.Veni.173.076']
        self.assertEqual((v['members'], v['pi_family_name'], v['lead_org_name'], v['products_json'], v['pi_member_id']),
                         ([], None, None, None, None))
        g = rows['024.003.012']                                    # no applicant or leader: the first member, as the laptop did
        self.assertEqual(g['pi_family_name'], 'Zed')
        self.assertEqual([m['last_name'] for m in g['members']], ['Abel', 'Zed'])
        total = json.loads(self.footer[b'openalex.funder_total'])   # what the gate compares rows with
        self.assertEqual(total, {'measure': 'NWOpen Projects listing meta.count', 'value': 5,
                                 'excluded': {'project_id listed twice (first page wins)': 1}})

    def test_member_order_does_not_follow_api_order(self):
        members = PROJECTS[0]['project_members'] + [member(7, 'Co-applicant', 'Abel', 'Ann'), member(8, 'Other', 'Ox')]
        first = nwo.project_members(members)
        for seed in range(5):
            shuffled = members[:]
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(nwo.project_members(shuffled), first)
        self.assertEqual([m['role'] for m in first][-1], 'Other')   # unknown roles sort last

    def test_moving_listing_or_short_or_repeated_pages_stop_the_harvest(self):
        api = Api(PROJECTS)
        api.override[2] = api.body(2, count=6)
        with self.assertRaisesRegex(ValueError, 'listing changed'):
            self.build(api)
        api = Api(PROJECTS)
        api.override[3] = api.body(3, date='2026-11-06T00:00:00')     # NWOpen reloaded mid-harvest
        with self.assertRaisesRegex(ValueError, 'listing changed'):
            self.build(api)
        api = Api(PROJECTS)
        api.override[2] = api.body(2, projects=PROJECTS[:1])
        with self.assertRaisesRegex(ValueError, 'short, repeated'):
            self.build(api)
        api = Api(PROJECTS)
        api.override[2] = api.body(2, projects=PROJECTS[:2])
        with self.assertRaisesRegex(ValueError, 'short, repeated'):
            self.build(api)

    def test_too_many_duplicate_ids_refuse_upload(self):
        with self.assertRaisesRegex(ValueError, 'duplicate project ids'):
            self.build(Api(PROJECTS), duplicates_ok=False)

    def test_failed_page_never_uploads(self):
        def fail(session, url, path, **kwargs):
            raise nwo.requests.ConnectionError('down')
        with patch('sys.argv', ['nwo_awards']), patch.object(nwo, 'download', fail), patch.object(nwo.time, 'sleep'), \
                patch.object(award_fetch.boto3, 'client') as client:
            with self.assertRaisesRegex(RuntimeError, 'NWOpen page 1 failed'):
                nwo.main()
            client.assert_not_called()


if __name__ == '__main__':
    unittest.main()
