"""Offline boundary and conversion checks for batch 1 completion."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import zipfile

import pandas as pd
import pyarrow.parquet as pq

import award_fetch
import cordis_awards as cordis
import gates_awards as gates
import kaken_awards as kaken
from award_schemas import CORDIS_SCHEMA, GATES_SCHEMA, KAKEN_SCHEMA


class Fetch2Tests(unittest.TestCase):
    def test_destinations_and_monthly_no_io(self):
        for source, module, schema in [('european_commission', cordis, CORDIS_SCHEMA),
                                       ('gates', gates, GATES_SCHEMA), ('kaken', kaken, KAKEN_SCHEMA)]:
            with patch.dict(os.environ, {source.upper()+'_AWARDS_S3_PREFIX': f's3://openalex-ingest/awards/dev-gr1009/{source}/'}):
                with self.assertRaises(ValueError):
                    award_fetch.destination(source, datetime.now(timezone.utc))
            self.assertTrue(award_fetch.is_due(source, datetime(2026, 11, 1)))
            self.assertFalse(award_fetch.is_due(source, datetime(2026, 11, 2)))
            with patch('sys.argv', [source, '--scheduled']), patch.object(award_fetch, 'datetime') as dt, \
                    patch.object(module, 'build') as build, patch.object(award_fetch.boto3, 'client') as s3:
                dt.now.return_value = datetime(2026, 11, 2, tzinfo=timezone.utc)
                module.main()
                build.assert_not_called()
                s3.assert_not_called()

    def test_cordis_all_programmes_nested_members_and_cleanup(self):
        def download(session, url, path):
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr('data/Project.json', json.dumps([{'id': '123', 'title': 'Research',
                    'totalCost': '10.5', 'startDate': '2025-01-01'}]))
                z.writestr('data/Organization.json', json.dumps([{'projectID': '123',
                    'role': 'coordinator', 'name': 'University'}]))
        with tempfile.TemporaryDirectory() as tmp, patch.object(cordis, 'download', download):
            out = cordis.build(Path(tmp))
            t = pq.read_table(out)
            self.assertEqual(t.num_rows, 1)
            self.assertTrue(t.schema.equals(CORDIS_SCHEMA, check_metadata=False))
            self.assertEqual(t['framework_programme'].to_pylist(), ['HORIZON'])
            self.assertEqual(t['coordinator_name'].to_pylist(), ['University'])
            self.assertEqual(t['total_cost'].to_pylist(), [10.5])
            self.assertEqual(list(Path(tmp).iterdir()), [out])
        with tempfile.TemporaryDirectory() as tmp, patch.object(cordis, 'download', side_effect=ValueError('missing programme')):
            with self.assertRaises(ValueError):
                cordis.build(Path(tmp))

    def test_cordis_structured_json_preserves_text_and_identifiers(self):
        doc = {"contenttype": "project", "id": "123", "objective": 'First; "quoted"\nSecond',
               "identifiers": {"grantDoi": "10.3030/123"}, "relations": {
                   "categories": [{"code": "/HORIZON-RIA", "attributes": {"classification": "projectFundingSchemeCategory"}}],
                   "associations": [{"contenttype": "organization", "id": "999", "legalName": "University",
                     "attributes": {"type": "coordinator", "ecContribution": "10.5"},
                     "address": {"country": "DE", "city": "Berlin"}}]}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"data.zip"
            with zipfile.ZipFile(path, "w") as z:
                z.writestr("project-rcn-1_en.json", json.dumps(doc))
            with zipfile.ZipFile(path) as z:
                row = cordis.read_programme(z, "HORIZON")[0]
            self.assertEqual(row["objective"], doc["objective"])
            self.assertEqual(row["grant_doi"], "10.3030/123")
            self.assertEqual(row["funding_scheme"], "HORIZON-RIA")
            self.assertEqual(row["coordinator_name"], "University")
            self.assertEqual(row["coordinator_country"], "DE")
            self.assertEqual(row["keywords"], "")

    def test_gates_blank_footer_and_contract(self):
        cols=['GRANT ID','GRANTEE','PURPOSE','DIVISION','DATE COMMITTED','DURATION (MONTHS)',
              'AMOUNT COMMITTED','GRANTEE WEBSITE','GRANTEE CITY','GRANTEE STATE','GRANTEE COUNTRY','REGION SERVED','TOPIC']
        row=['INV-123','University','Research','Health','2026-01','12','1,234.50',None,None,None,'US',None,None]
        frame=pd.DataFrame([row,[None]*len(cols)],columns=cols)
        with tempfile.TemporaryDirectory() as tmp, patch.object(pd, 'read_csv', return_value=frame):
            t=pq.read_table(gates.process_csv(Path(tmp)/'x.csv',Path(tmp)))
            self.assertEqual(t.num_rows,1)
            self.assertTrue(t.schema.equals(GATES_SCHEMA,check_metadata=False))
            self.assertEqual(t['amount'].to_pylist(),[1234.5])
            self.assertEqual(t['start_date'].to_pylist(),['2026-01-01'])
        frame['NEW FIELD']='unexpected'
        with tempfile.TemporaryDirectory() as tmp, patch.object(pd,'read_csv',return_value=frame):
            with self.assertRaisesRegex(ValueError,'columns changed'):
                gates.process_csv(Path(tmp)/'x.csv',Path(tmp))

    def test_missing_kaken_key_precedes_network(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(kaken,'http_session') as http, \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'KAKEN_APPID'):
                kaken.build(Path(tmp))
            http.assert_not_called()
            self.assertEqual(list(Path(tmp).iterdir()),[])

    def test_kaken_page_and_frozen_contract(self):
        xml='''<?xml version="1.0"?><results totalResults="1"><grantAward awardNumber="26H00001">
<summary xml:lang="ja"><title>日本語</title><periodOfAward><startFiscalYear>2026</startFiscalYear></periodOfAward>
<overallAwardAmount><totalCost>100000</totalCost><directCost>90000</directCost></overallAwardAmount>
<member role="principal_investigator" researcherNumber="50287950"><personalName><familyName>Test</familyName><givenName>A</givenName></personalName></member>
<member role="co_investigator_buntan" researcherNumber="80304020"><personalName><familyName>Second</familyName><givenName>B</givenName></personalName></member></summary>
<summary xml:lang="en"><title>Research title</title><category>Grant-in-Aid</category></summary>
</grantAward></results>'''
        def download(session,url,path):path.write_text(xml)
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ,{'KAKEN_APPID':'test'}), \
                patch.object(kaken,'download',download),patch.object(kaken.time,'sleep'), \
                patch.object(kaken,'FIRST_FISCAL_YEAR',datetime.utcnow().year):
            out=kaken.build(Path(tmp));t=pq.read_table(out)
            self.assertEqual(t.schema,KAKEN_SCHEMA)
            self.assertEqual(t['title'].to_pylist(),['Research title'])
            self.assertEqual(t['pi_nrid'].to_pylist(),['1000050287950'])
            self.assertEqual(t['start_date'].to_pylist(),['2026-04-01'])
            self.assertEqual(t['amount'].to_pylist(),[100000.0])
            self.assertEqual(t['landing_page_url'].to_pylist(),['https://kaken.nii.ac.jp/en/grant/KAKENHI-PROJECT-26H00001/'])
            self.assertNotIn('amount_direct',t.column_names)
            total=json.loads(pq.read_metadata(out).metadata[b'openalex.funder_total'])   # what the gate reads
            self.assertEqual((total['value'],total['by_year'],total['excluded']),(1,{str(datetime.utcnow().year):1},{'projects repeated across start fiscal years':0}))
            # every member, in KAKEN's order, for walden's kaken.sql (co-investigators -> investigators)
            members=json.loads(t['members_json'][0].as_py())
            self.assertEqual([(m['role'],m['family_name'],m['researcher_number']) for m in members],
                             [('principal_investigator','Test','50287950'),('co_investigator_buntan','Second','80304020')])
            self.assertFalse((Path(tmp)/'page.xml').exists())
            p=Path(tmp)/'invalid.xml';p.write_text(xml.replace('totalResults="1"','totalResults="2"'))
            # A short page cannot silently become a partial full harvest.
            def short(session,url,path):path.write_bytes(p.read_bytes())
            with patch.object(kaken,'download',short),self.assertRaisesRegex(ValueError,'truncated'):
                kaken.build(Path(tmp))

    def test_kaken_products_keep_authors_volume_pages_and_typed_doi(self):
        # oxjob #2632: the fields a work record or a volume+page match needs, all strings (walden parses
        # products_json as array<map<string,string>>); existing keys unchanged
        xml='''<?xml version="1.0"?><results totalResults="1"><grantAward awardNumber="22590893">
<summary xml:lang="en"><title>Research title</title></summary>
<productList>
<product id="PRODUCT-1" type="journal_article" sequence="1" reviewed="true"><parent id="22590893seika"/>
<doi originalValue="DOI:10.1002/art.30183">10.1002/art.30183</doi><author>Tada Y, Kondo S, 他5名</author>
<title>Interferon regulatory factor 5</title><journalTitle>Arthritis Rheum</journalTitle><volume>63</volume><issue>3</issue>
<pages originalValue="738-748">738-748</pages><year originalValue="2011">2011</year></product>
<product id="PRODUCT-2" type="journal_article" sequence="2"><parent id="225908932010jisseki"/><parent id="225908932011jisseki"/>
<doi originalValue="doi: 10.1016/j.example.2011.01.001" authenticated="false"/><author>Tada Y</author>
<title>A second paper</title><year>2011</year><description>「研究成果報告書概要(欧文)」より</description></product>
<product id="PRODUCT-3" type="book" sequence="3"><doi originalValue="なし" authenticated="false"/><author>Editor A</author>
<title>A book</title><publisher>Publisher P</publisher><isbn>9784000000000</isbn><totalPages originalValue="105">105</totalPages><year>2012</year></product>
</productList></grantAward></results>'''
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'page.xml';p.write_text(xml)
            total,rows=kaken.parse_page(p)
        prods=json.loads(rows[0]['products_json'])
        self.assertTrue(all(isinstance(v,str) for x in prods for v in x.values()))
        a,b,c=prods
        self.assertEqual((a['doi'],a['authors'],a['volume'],a['issue'],a['pages'],a['report'],a['product_id']),
                         ('10.1002/art.30183','Tada Y, Kondo S, 他5名','63','3','738-748','22590893seika','PRODUCT-1'))
        self.assertNotIn('doi_original',a)                       # only when KAKEN left <doi> empty
        self.assertNotIn('doi',b)                                # doi keeps its meaning: KAKEN's normalised value
        self.assertEqual(b['doi_original'],'doi: 10.1016/j.example.2011.01.001')
        self.assertEqual(b['report'],'225908932010jisseki,225908932011jisseki')
        self.assertEqual(b['description'],'「研究成果報告書概要(欧文)」より')
        self.assertNotIn('doi_original',c)                       # なし is not a DOI
        self.assertEqual((c['publisher'],c['isbn'],c['total_pages']),('Publisher P','9784000000000','105'))
        self.assertEqual(set(a)-{'product_id','report','authors','volume','issue','pages','total_pages','publisher','isbn',
                                 'language','date','description','doi_original'},
                         {'type','title','year','doi','journal_title','reviewed'})

    def test_kaken_appid_never_reaches_a_log_record(self):
        secret='appid-'+os.urandom(8).hex()
        import logging
        with self.assertLogs('urllib3.connectionpool','WARNING') as logs:
            logging.getLogger('urllib3.connectionpool').warning("Retrying (%r) after connection broken by '%r': %s",
                'Retry(total=3)','err',f'/opensearch/?appid={secret}&format=xml&s1=2026')
        self.assertNotIn(secret,''.join(logs.output))
        self.assertIn('appid=***',''.join(logs.output))

    def test_kaken_publishes_to_its_own_prefix(self):
        # awards/kaken/ holds the page scrape walden reads; the API fetch lands beside it until walden's row moves
        with patch.object(kaken,'run') as run:
            kaken.main()
        self.assertEqual(run.call_args[0][0],'kaken_api')
        self.assertEqual(award_fetch.destination('kaken_api',datetime(2026,11,16))[1],'awards/kaken_api/2026-11-16.parquet')

    def test_disk_floor(self):
        with patch.object(award_fetch.shutil,'disk_usage',return_value=type('Disk',(),{'free':511*1024**2})()):
            with self.assertRaises(OSError):award_fetch.check_disk('.')


if __name__ == '__main__':
    unittest.main()
