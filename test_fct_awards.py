"""Offline checks for the FCT SciPROJ fetcher: python -m unittest -v test_fct_awards."""
import io
import json
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pyarrow.parquet as pq

from award_schemas import FCT_SCHEMA
import fct_awards as fct

C = 'https://www.openaire.eu/cerif-profile/1.1/'
CIENCIA = 'https://www.wikidata.org/wiki/Q122584897'
KEY = 'https://www.wikidata.org/wiki/Q122538528'


def person(pid, name, orcid=None, ciencia=None):
    # The private dump's shape: e-mail, birth date and gender sit next to the name; the fetcher must never copy them.
    return (f'<Person id="{pid}"><CountryCode>Portugal</CountryCode>'
            + (f'<Identifier type="{CIENCIA}">{ciencia}</Identifier>' if ciencia else '')
            + (f'<ORCID>https://orcid.org/{orcid}</ORCID>' if orcid else '')
            + f'<Identifier type="{KEY}">J{pid}</Identifier><PersonName>{name}</PersonName><Birthdate>1980-01-02</Birthdate>'
            f'<Gender>F</Gender><ElectronicAddress>secret{pid}@example.pt</ElectronicAddress></Person>')


def record(pid, ref, title='Título', funder='<Acronym>FCT</Acronym><Name>Fundação para a Ciência e a Tecnologia</Name>'
           '<RORID>https://ror.org/00snfqn58</RORID>', program='PTDC', pi=None, members=(), amount='1000', streams=()):
    team = ''
    if pi:
        team += f'<PrincipalInvestigator>{person(*pi)}</PrincipalInvestigator><Contact>{person(*pi)}</Contact>'
    team += ''.join(f'<Member>{person(*m)}</Member>' for m in members)
    ident = f'<Identifier type="https://w3id.org/cerif/vocab/IdentifierTypes#ProjectReference">{ref}</Identifier>' if ref else ''
    return (f'<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords><record>'
            f'<metadata><Project xmlns="{C}" id="{pid}"><Title xml:lang="pt">{title}</Title>'
            f'<Identifier type="https://w3id.org/cerif/vocab/IdentifierTypes#FundingProgram">{program}</Identifier>{ident}'
            f'<StartDate>2021-01-01</StartDate><EndDate>2023-12-31</EndDate>'
            f'<Consortium><Coordinator><OrgUnit id="1"><Name xml:lang="pt">Universidade de Lisboa</Name>'
            f'<RORID>https://ror.org/01c27hj86</RORID><Country>PT</Country></OrgUnit></Coordinator></Consortium>'
            f'<Team>{team}</Team><Funded><By><OrgUnit id="2">{funder}</OrgUnit></By><As><Funding id="{pid}">'
            f'<Amount currency="EUR">{amount}</Amount></Funding></As></Funded>'
            + ''.join(f'<Funded><By><OrgUnit id="9"><Name>UE</Name></OrgUnit></By><As><Funding id="{pid}B">'
                      f'<Amount currency="{cur}">{v}</Amount></Funding></As></Funded>' for cur, v in streams)
            + f'<!--<Subject scheme="x" xml:lang="pt">Matemática</Subject>--></Project></metadata></record></ListRecords></OAI-PMH>').encode()


RECORDS = {
    'p1.xml': record('P1', 'PTDC/MAT/1/2020', pi=('10', 'Doutora Ana Maria Silva', '0000-0002-1825-0097', 'AAAA-BBBB-CCCC'),
                     members=[('11', 'Rui Costa', '0000-0001-5109-3700', 'DDDD-EEEE-FFFF'), ('10', 'Doutora Ana Maria Silva'),
                              ('12', 'Joana Pires')]),
    'p2.xml': record('P2', 'UIDB/00001/2020', pi=('20', 'Carlos Melo'), members=[('21', 'Luis Dias'), ('22', 'Ines Rocha')]),
    'p3.xml': record('P3', '123456', funder='<Acronym>EC</Acronym><Name>European Commission</Name>', program='FP7',
                     pi=('30', 'Pedro Gomes', '0000-0003-1415-9269')),
    'p4.xml': record('P4', None, pi=('40', 'Sem Referencia')),
    'p5.xml': record('P5', '2023.16009.ICDT', pi=('50', 'Marta Pinto', '0000-0002-9079-593X'), amount='37499.76',
                     streams=[('EUR', '212498.64')]),   # FCT's 15% + FEDER's 85% = the project total
}
# SciPROJ's real answer (11-05, tokens replaced): XML with an unbound dataset: prefix, whatever the Accept header asks for.
LISTING = (b'<?xml version="1.0" encoding="UTF-8"?>\n<dataset:public-dataset datestamp="2026-11-09T22:38:38Z" total ="3">\n'
           b'<dump-file name="getDumpXML20261109_PRV.zip">https://api.ptcris.pt/services/getFileDump/c+Tok1</dump-file>\n'
           b'<dump-file name="getDumpXML20261109_PUB.zip">https://api.ptcris.pt/services/getFileDump/b_Tok2</dump-file>\n'
           b'<dump-file name="getDumpXML20261102_PUB.zip">https://api.ptcris.pt/services/getFileDump/a-Tok3</dump-file>\n'
           b'</dataset:public-dataset>')
# The form SciPROJ's Swagger documents, with a byte-order mark.
LISTING_JSON = '\ufeff' + json.dumps({'publicDataset': {'total': 1, 'dumpFiles': [
    {'name': 'getDumpXML20261109_PUB.zip', 'url': 'https://api.ptcris.pt/services/getFileDump/b_Tok2'}]}})


class Response:
    def __init__(self, status, body):
        self.status_code = status
        self.content = body.encode('utf-8') if isinstance(body, str) else body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class Session:
    def __init__(self, listings):
        self.listings, self.headers, self.auth, self.calls = listings, {}, None, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        return self.listings.get(params['year'], Response(404, None))


def zip_bytes(records):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        for name, data in records.items():
            z.writestr(name, data)
    return buf.getvalue()


class FctTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'SCIPROJ_API_USERNAME': 'u', 'SCIPROJ_API_PASSWORD': 'p'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def run_build(self, listings, records=RECORDS, min_rows=1):
        session = Session(listings)
        downloads = []

        def download(s, url, path, **kw):
            downloads.append(url)
            path.write_bytes(zip_bytes(records))
        with tempfile.TemporaryDirectory() as tmp, patch.object(fct, 'http_session', lambda: session), \
                patch.object(fct, 'download', download), patch.object(fct, 'MIN_ROWS', min_rows):
            out = fct.build(Path(tmp))
            table = pq.read_table(out)
            self.assertFalse((Path(tmp) / 'sciproj.zip').exists())
            return table, session, downloads

    def test_newest_private_dump_and_rows(self):
        year = str(fct.datetime.now(fct.timezone.utc).year)
        table, session, downloads = self.run_build({year: Response(200, LISTING)})
        self.assertEqual(downloads, ['https://api.ptcris.pt/services/getFileDump/b_Tok2'])   # newest; _PUB wins a same-day tie
        self.assertEqual(session.auth, ('u', 'p'))
        self.assertEqual(session.headers['User-Agent'], 'Mozilla/5.0 (compatible; OpenAlex-ingest/1.0; +https://openalex.org)')
        self.assertTrue(table.schema.equals(FCT_SCHEMA))
        rows = {r['funder_award_id']: r for r in table.to_pylist()}
        self.assertEqual(sorted(rows), ['123456', '2023.16009.ICDT', 'PTDC/MAT/1/2020', 'UIDB/00001/2020'])   # no-reference record dropped
        p1 = rows['PTDC/MAT/1/2020']
        self.assertEqual((p1['pi_given'], p1['pi_family'], p1['pi_orcid'], p1['pi_ciencia_id']),
                         ('Doutora Ana Maria', 'Silva', '0000-0002-1825-0097', 'AAAA-BBBB-CCCC'))   # titles leave in walden
        team = json.loads(p1['team_json'])
        self.assertEqual([(m['name'], m['family'], m['orcid'], m['ciencia_id']) for m in team],
                         [('Rui Costa', 'Costa', '0000-0001-5109-3700', 'DDDD-EEEE-FFFF'), ('Joana Pires', 'Pires', None, None)])
        self.assertEqual(p1['source_dump'], 'getDumpXML20261109_PUB.zip')
        self.assertEqual(rows['123456']['funder_key'], 'ec')
        # the amount is the project total across funding streams (FCT + FEDER), as fct.pt published it
        self.assertEqual((rows['2023.16009.ICDT']['amount'], rows['2023.16009.ICDT']['currency']), ('249998.40', 'EUR'))
        self.assertEqual(rows['PTDC/MAT/1/2020']['amount'], '1000.00')
        text = json.dumps(table.to_pylist(), ensure_ascii=False)
        for private in ('@example.pt', '1980-01-02', 'Birthdate', 'Gender', 'J10'):
            self.assertNotIn(private, text)

    def test_previous_year_in_january_and_refusals(self):
        now = fct.datetime.now(fct.timezone.utc)
        table, session, _ = self.run_build({str(now.year - 1): Response(200, LISTING)})
        self.assertEqual([p['year'] for _, p in session.calls], [str(now.year), str(now.year - 1)])
        _, _, downloads = self.run_build({str(now.year): Response(200, LISTING_JSON)})
        self.assertEqual(downloads, ['https://api.ptcris.pt/services/getFileDump/b_Tok2'])
        with self.assertRaisesRegex(RuntimeError, 'no weekly dump'):
            self.run_build({str(now.year): Response(200, b'<dataset:public-dataset total ="0"></dataset:public-dataset>')})
        with self.assertRaisesRegex(ValueError, 'neither'):
            self.run_build({str(now.year): Response(200, b'<html>blocked</html>')})
        off_host = LISTING.replace(b'https://api.ptcris.pt', b'https://evil.example')
        with self.assertRaisesRegex(ValueError, 'off-host'):
            self.run_build({str(now.year): Response(200, off_host)})
        with self.assertRaisesRegex(RuntimeError, '401'):
            self.run_build({str(now.year): Response(401, None)})

    def test_guards(self):
        year = str(fct.datetime.now(fct.timezone.utc).year)
        usd = dict(RECORDS, **{'p6.xml': record('P6', 'PTDC/X/6/2024', pi=('60', 'Rui Dias'), streams=[('USD', '10')])})
        with self.assertRaisesRegex(ValueError, 'not EUR'):
            self.run_build({year: Response(200, LISTING)}, records=usd)
        with self.assertRaisesRegex(ValueError, 'refusing upload'):
            self.run_build({year: Response(200, LISTING)}, min_rows=10)
        no_people = {k: v.replace(b'<PrincipalInvestigator>', b'<X>').replace(b'</PrincipalInvestigator>', b'</X>') for k, v in RECORDS.items()}
        with self.assertRaisesRegex(ValueError, 'PI share'):
            self.run_build({year: Response(200, LISTING)}, records=no_people)
        with patch.dict(os.environ, {'SCIPROJ_API_PASSWORD': ''}), self.assertRaisesRegex(RuntimeError, 'required'):
            self.run_build({})   # before any HTTP

    def test_a_stream_without_an_amount_drops_the_total(self):
        year = str(fct.datetime.now(fct.timezone.utc).year)
        partial = record('P7', 'PTDC/X/7/2024', pi=('70', 'Rita Sousa'), amount='100').replace(
            b'<!--', b'<Funded><By><OrgUnit id="9"><Name>UE</Name></OrgUnit></By></Funded><!--')   # a second stream, no amount
        table, _, _ = self.run_build({year: Response(200, LISTING)}, records=dict(RECORDS, **{'p7.xml': partial}))
        rows = {r['funder_award_id']: r for r in table.to_pylist()}
        self.assertIsNone(rows['PTDC/X/7/2024']['amount'])   # never the 100.00 partial sum
        self.assertEqual(rows['2023.16009.ICDT']['amount'], '249998.40')   # complete streams still sum
        self.assertNotIn('_partial_sum', table.schema.names)
        total = json.loads(table.schema.metadata[b'openalex.funder_total'])   # what the gate reads
        self.assertEqual(total['value'] - sum(total['excluded'].values()), table.num_rows)
        self.assertEqual((total['amount_dropped_partial'], total['amount_dropped_partial_examples']),
                         (1, [['PTDC/X/7/2024', '100.00']]))

    def test_non_finite_amounts_and_an_id_less_pi_stay_out(self):
        year = str(fct.datetime.now(fct.timezone.utc).year)
        recs = dict(RECORDS, **{
            'p8.xml': record('P8', 'PTDC/X/8/2024', pi=('80', 'Ana Lima'), amount='Infinity'),
            'p9.xml': record('P9', 'PTDC/X/9/2024', pi=('', 'Rui Lopes'), members=[('', 'Rui Lopes'), ('', 'Eva Nunes')]),
        })
        table, _, _ = self.run_build({year: Response(200, LISTING)}, records=recs)
        rows = {r['funder_award_id']: r for r in table.to_pylist()}
        self.assertIsNone(rows['PTDC/X/8/2024']['amount'])   # never "inf"
        self.assertEqual([m['name'] for m in json.loads(rows['PTDC/X/9/2024']['team_json'])], ['Eva Nunes'])   # PI left out by name
        mixed = dict(RECORDS, **{'p10.xml': record('P10', 'PTDC/X/10/2024', pi=('100', 'Ana Silva'),
                                                   members=[('', 'Ana Silva'), ('101', 'Ana Silva'), ('102', 'Rui Melo')])})
        table, _, _ = self.run_build({year: Response(200, LISTING)}, records=mixed)
        team = json.loads({r['funder_award_id']: r for r in table.to_pylist()}['PTDC/X/10/2024']['team_json'])
        self.assertEqual([(m['name'], m['person_id']) for m in team], [('Ana Silva', '101'), ('Rui Melo', '102')])   # same-name colleague kept

    def test_sentry_debug_log_never_carries_the_dump_token(self):
        logger = logging.getLogger('sentry_sdk.errors')
        record = logging.LogRecord(logger.name, logging.DEBUG, __file__, 1, '[Tracing] GET %s',
                                   ('https://api.ptcris.pt/services/getFileDump/secretTok',), None)
        ours = [f for f in logger.filters if isinstance(f, fct._RedactToken)]   # sentry adds its own debug-only filter
        self.assertTrue(ours)
        for f in ours:
            f.filter(record)
        self.assertNotIn('secretTok', record.getMessage())
        self.assertIn('getFileDump/***', record.getMessage())

    def test_sentry_events_never_carry_the_dump_token(self):
        event = {'breadcrumbs': {'values': [
            {'category': 'httplib', 'data': {'url': 'https://api.ptcris.pt/services/getFileDump/secretTok', 'method': 'GET'}},
            {'message': 'GET https://api.ptcris.pt/services/getFileDump/secretTok 200'}]}}
        out = fct._redact_dump_links(event, {})
        self.assertNotIn('secretTok', json.dumps(out))
        self.assertIn('getFileDump/***', out['breadcrumbs']['values'][0]['data']['url'])

    def test_download_token_never_reaches_a_log_record(self):
        import logging
        with self.assertLogs('urllib3.connectionpool', 'WARNING') as logs:
            logging.getLogger('urllib3.connectionpool').warning("Retrying (%r) after connection broken by '%r': %s",
                                                                 'Retry(total=3)', 'err', '/services/getFileDump/wYfZsecretTok+1')
        self.assertNotIn('secretTok', ''.join(logs.output))
        self.assertIn('getFileDump/***', ''.join(logs.output))

    def test_prefix(self):
        self.assertEqual(fct.API, 'https://api.ptcris.pt/services/searches/projects/dump')
        import award_fetch
        self.assertEqual(award_fetch.destination('fct_sciproj', fct.datetime(2026, 11, 9))[0], 'openalex-ingest')
        self.assertTrue(award_fetch.destination('fct_sciproj', fct.datetime(2026, 11, 9))[1].startswith('awards/fct_sciproj/'))


if __name__ == '__main__':
    unittest.main()
