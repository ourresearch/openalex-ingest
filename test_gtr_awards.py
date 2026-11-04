"""Offline checks for the GtR fetcher: python -m unittest -v test_gtr_awards."""
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
from award_schemas import GTR_SCHEMA
import gtr_awards as gtr

ROOT = ('xmlns:ns1="http://gtr.rcuk.ac.uk/gtr/api" xmlns:ns2="http://gtr.rcuk.ac.uk/gtr/api/{kind}" '
        'ns1:page="{page}" ns1:size="{size}" ns1:totalPages="{pages}" ns1:totalSize="{total}"')


def project_xml(pid, ref, people=(), funder='EPSRC'):
    links = ''.join(f'<ns1:link ns1:href="http://gtr.ukri.org/gtr/api/persons/{p}" ns1:rel="{rel}"/>' for p, rel in people)
    return (f'<ns2:project ns1:id="{pid}"><ns1:links>'
            f'<ns1:link ns1:href="http://gtr.ukri.org/gtr/api/organisations/ORG-{pid}" ns1:rel="LEAD_ORG"/>'
            f'<ns1:link ns1:href="http://gtr.ukri.org/gtr/api/funds/F-{pid}" ns1:rel="FUND" '
            f'ns1:start="2019-09-30T23:00:00Z" ns1:end="2024-03-30T00:00:00Z"/>{links}</ns1:links>'
            f'<ns2:title> Title {pid} </ns2:title><ns2:status>Closed</ns2:status><ns2:grantCategory>Research Grant'
            f'</ns2:grantCategory><ns2:leadFunder>{funder}</ns2:leadFunder><ns2:abstractText/>'
            f'<ns2:techAbstractText>Tech {pid}</ns2:techAbstractText>'
            + (f'<ns2:identifiers><ns2:identifier ns2:type="RCUK">{ref}</ns2:identifier></ns2:identifiers>' if ref else '')
            + '</ns2:project>')


def fund_xml(fid, pid, amount, category='INCOME_ACTUAL'):
    return (f'<ns2:fund ns1:id="{fid}"><ns1:links><ns1:link ns1:href="http://gtr.ukri.org/gtr/api/projects/{pid}" '
            f'ns1:rel="FUNDED"/></ns1:links><ns2:valuePounds ns1:amount="{amount}" ns1:currencyCode="GBP"/>'
            f'<ns2:category>{category}</ns2:category></ns2:fund>')


def listing(kind, items, page, total, pages):
    tag = {'project': 'projects', 'fund': 'funds'}[kind]
    head = ROOT.format(kind=kind, page=page, size=len(items), pages=pages, total=total)
    return f'<?xml version="1.0" encoding="UTF-8"?><ns2:{tag} {head}>{"".join(items)}</ns2:{tag}>'.encode()


class Api:
    """Serves paged fixtures through the download() seam; size 2 pages, so paging is exercised."""

    def __init__(self, persons, funds, projects, size=2):
        self.size, self.calls = size, []
        self.data = {'persons': persons, 'funds': funds, 'projects': projects}
        self.override = {}

    def download(self, session, url, path, **kwargs):
        parsed = urlparse(url)
        endpoint, page = parsed.path.rsplit('/', 1)[-1], int(parse_qs(parsed.query)['p'][0])
        self.calls.append((endpoint, page))
        if (endpoint, page) in self.override:
            path.write_bytes(self.override[(endpoint, page)])
            return
        items = self.data[endpoint]
        pages = -(-len(items) // self.size)
        chunk = items[(page - 1) * self.size: page * self.size]
        if endpoint == 'persons':
            path.write_bytes(json.dumps({'page': page, 'size': len(chunk), 'totalPages': pages,
                                         'totalSize': len(items), 'person': chunk}).encode())
        else:
            path.write_bytes(listing(endpoint[:-1], chunk, page, len(items), pages))


def person(pid, first, last, orcid=None):
    return {'id': pid, 'firstName': first, 'otherNames': '', 'surname': last, 'email': None, 'orcidId': orcid}


PERSONS = [person('P1', 'Ada', 'Zed', '0000-0002-7747-4015'), person('P2', 'Bo', 'Abel'), person('P3', 'Cy', 'Abel'),
           person('P4', 'Di', 'Moe')]
FUNDS = [fund_xml('F1', 'A', 100), fund_xml('F2', 'A', 50.5), fund_xml('F3', 'A', 999, 'EXPENDITURE_ACTUAL'),
         fund_xml('F4', 'B', 7)]
PROJECTS = [project_xml('A', 'EP/X000001/1', [('P1', 'PI_PER'), ('P1', 'FELLOW_PER'), ('P3', 'COI_PER'), ('P2', 'COI_PER'),
                                              ('P4', 'PM_PER'), ('P9', 'RESEARCH_PER')]),
            project_xml('B', 'ep/x000001/1', [('P2', 'PI_PER')]),     # same grant, other case: first page wins
            project_xml('C', None),                                      # no reference: dropped, as walden's mapping does
            project_xml('D', '2440634', funder='EPSRC')]


class GtrTests(unittest.TestCase):
    def build(self, api, unresolved_ok=True):
        with tempfile.TemporaryDirectory() as tmp, patch.object(gtr, 'download', api.download), \
                patch.object(gtr.time, 'sleep'), \
                patch.object(gtr, 'MAX_UNRESOLVED_LINKS', 1.0 if unresolved_ok else 0.0):
            out = gtr.build(Path(tmp))
            table = pq.read_table(out)
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), [out.name])   # page files are removed
            return table

    def test_destination_and_monthly_schedule(self):
        now = datetime(2026, 12, 1, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination('gateway_to_research', now),
                             ('openalex-ingest', 'awards/gateway_to_research/2026-12-01.parquet'))
        with patch.dict(os.environ, {'GATEWAY_TO_RESEARCH_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/gtr/'}):
            with self.assertRaises(ValueError):
                award_fetch.destination('gateway_to_research', now)
        self.assertTrue(award_fetch.is_due('gateway_to_research', now))
        self.assertFalse(award_fetch.is_due('gateway_to_research', datetime(2026, 12, 2)))

    def test_rows_people_and_amounts(self):
        api = Api(PERSONS, FUNDS, PROJECTS)
        with patch.object(gtr, 'PAGE_SIZE', 2):
            table = self.build(api)
        self.assertTrue(table.schema.equals(GTR_SCHEMA, check_metadata=False))
        rows = {r['project_id']: r for r in table.to_pylist()}
        self.assertEqual(sorted(rows), ['A', 'D'])
        a = rows['A']
        self.assertEqual((a['grant_reference'], a['title'], a['abstract']), ('EP/X000001/1', 'Title A', 'Tech A'))
        self.assertEqual((a['start_date'], a['end_date'], a['lead_org_id']), ('2019-09-30', '2024-03-30', 'ORG-A'))
        self.assertEqual(a['amount'], 150.5)                       # INCOME_ACTUAL only, summed
        self.assertEqual((a['pi_id'], a['pi_given_name'], a['pi_family_name'], a['pi_orcid']),
                         ('P1', 'Ada', 'Zed', '0000-0002-7747-4015'))
        self.assertEqual([(p['person_id'], p['role'], p['roles']) for p in a['persons']], [
            ('P1', 'PI_PER', ['PI_PER', 'FELLOW_PER']), ('P2', 'COI_PER', ['COI_PER']), ('P3', 'COI_PER', ['COI_PER']),
            ('P4', 'PM_PER', ['PM_PER']), ('P9', 'RESEARCH_PER', ['RESEARCH_PER'])])
        self.assertIsNone(a['persons'][-1]['family_name'])           # a link to a person /persons does not list
        d = rows['D']
        self.assertEqual((d['persons'], d['pi_id'], d['pi_given_name'], d['amount']), ([], None, None, None))

    def test_people_order_does_not_follow_link_order(self):
        names = {p['id']: gtr.parse_person(p)[1] for p in PERSONS}
        links = [('P1', 'PI_PER'), ('P1', 'FELLOW_PER'), ('P3', 'COI_PER'), ('P2', 'COI_PER'), ('P4', 'PM_PER')]
        first = gtr.project_people(links, names)
        for seed in range(5):
            shuffled = links[:]
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(gtr.project_people(shuffled, names), first)

    def test_unresolved_people_refuse_upload(self):
        with patch.object(gtr, 'PAGE_SIZE', 2), self.assertRaisesRegex(ValueError, 'name no listed person'):
            self.build(Api(PERSONS, FUNDS, PROJECTS), unresolved_ok=False)

    def test_totals_moving_or_repeated_pages_stop_the_harvest(self):
        api = Api(PERSONS, FUNDS, PROJECTS)
        api.override[('projects', 2)] = listing('project', [project_xml('X', 'EP/Y/1')], 2, 5, 3)
        with patch.object(gtr, 'PAGE_SIZE', 2), self.assertRaisesRegex(ValueError, 'totals changed'):
            self.build(api)
        api = Api(PERSONS, FUNDS, PROJECTS)
        api.override[('projects', 2)] = listing('project', PROJECTS[:2], 2, 4, 2)
        with patch.object(gtr, 'PAGE_SIZE', 2), self.assertRaisesRegex(ValueError, 'short or repeated'):
            self.build(api)

    def test_failed_page_never_uploads(self):
        def fail(session, url, path, **kwargs):
            raise gtr.requests.ConnectionError('down')
        with patch('sys.argv', ['gtr_awards']), patch.object(gtr, 'download', fail), patch.object(gtr.time, 'sleep'), \
                patch.object(award_fetch.boto3, 'client') as client:
            with self.assertRaisesRegex(RuntimeError, 'persons page 1 failed'):
                gtr.main()
            client.assert_not_called()


if __name__ == '__main__':
    unittest.main()
