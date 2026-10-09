"""Offline regression tests: python -m unittest -v test_award_fetchers."""
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd
import pyarrow.parquet as pq

import award_fetch
from award_schemas import NSF_SCHEMA, WELLCOME_SCHEMA
import nsf_awards
import wellcome_awards


class FetchTests(unittest.TestCase):
    def test_destination_and_schedule(self):
        now = datetime(2026, 10, 5, tzinfo=timezone.utc)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(award_fetch.destination('nsf', now),
                             ('openalex-ingest', 'awards/dev-gr1009/nsf/2026-10-05.parquet'))
        with patch.dict(os.environ, {'NSF_AWARDS_S3_PREFIX': 's3://openalex-ingest/awards/nsf/'}):
            with self.assertRaises(ValueError):
                award_fetch.destination('nsf', now)
        self.assertTrue(award_fetch.is_due('nsf', now))
        self.assertFalse(award_fetch.is_due('wellcome', now))
        first = datetime(2026, 11, 1, tzinfo=timezone.utc)
        self.assertTrue(award_fetch.is_due('wellcome', first))
        self.assertFalse(award_fetch.is_due('nsf', first))

    def test_discovery_fails_without_unique_current_link(self):
        link = 'https://cms.wellcome.org/files/Wellcome-grants-awarded-current.xlsx'
        self.assertEqual(wellcome_awards.discover_xlsx(f'<a href="{link}">grants</a>',
                                                     wellcome_awards.WELLCOME_PAGE), link)
        for page in ['<a href="old.xlsx">old</a>',
                     f'<a href="{link}"></a><a href="{link.replace("current", "other")}"></a>',
                     '<a href="https://example.org/Wellcome-grants-awarded-current.xlsx"></a>']:
            with self.assertRaises(ValueError):
                wellcome_awards.discover_xlsx(page, wellcome_awards.WELLCOME_PAGE)

    def test_nsf_archive_stream_and_failed_parse(self):
        class Response:
            def raise_for_status(self): pass
            def json(self):
                return {'files': [{'fileName': '2026.zip', 'size': 10, 'downloadUrl': 'https://example.org'}]}
        class Session:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def get(self, *args, **kwargs): return Response()
        award = {'awd_id': '1234567', 'awd_amount': 42.0,
                 'perf_inst': {'perf_inst_name': 'University'},
                 'pi': [{'pi_full_name': 'A'}, {'pi_full_name': 'B'}]}
        def download(session, url, path, **kwargs):
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr('award.json', json.dumps(award))
        with tempfile.TemporaryDirectory() as tmp, patch.object(nsf_awards, 'http_session', Session), \
                patch.object(nsf_awards, 'download', download):
            out = nsf_awards.build(Path(tmp))
            table = pq.read_table(out)
            self.assertEqual(table.schema, NSF_SCHEMA)
            self.assertEqual(table['pi_count'].to_pylist(), [2])
            self.assertEqual(table['inst_name'].to_pylist(), ['University'])
            self.assertFalse((Path(tmp) / 'download.zip').exists())
            award['awd_id'] = None
            with self.assertRaises(ValueError):
                nsf_awards.build(Path(tmp))

    def test_wellcome_citable_ids_and_deduplication(self):
        # Use the complete legacy source contract plus the columns transformed by the port.
        columns = [n for n in WELLCOME_SCHEMA.names if n not in {'grant_ref', 'ingested_at'}]
        frame = pd.DataFrame([{c: None for c in columns} for _ in range(21)])
        frame['internal_id'] = [f'{100000+i}/Z/24/Z' for i in range(20)] + ['100000/Z/24/Z']
        frame['identifier_360g'] = frame['internal_id'].str.replace('/', '_', regex=False).radd('360G-Wellcome-')
        frame.loc[1, 'internal_id'] = None
        frame['amount'] = '1,234.50'
        frame['start_date'] = '2024-01-02'
        with tempfile.TemporaryDirectory() as tmp, patch.object(pd, 'read_excel', return_value=frame):
            out = wellcome_awards.process_xlsx(Path(tmp) / 'source.xlsx', Path(tmp))
            table = pq.read_table(out)
            self.assertTrue(table.schema.equals(WELLCOME_SCHEMA, check_metadata=False))
            self.assertEqual(table.num_rows, 20)
            self.assertEqual(table['grant_ref'][1].as_py(), '100001/Z/24/Z')
            self.assertEqual(table['amount'][0].as_py(), 1234.5)

    def test_shrink_guard_and_failed_baseline_reads(self):
        import pyarrow as pa
        from unittest.mock import MagicMock
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [{"Contents": [
            {"Key": "awards/dev-gr1009/nsf/2026-10-08.parquet", "LastModified": 1}]}]
        buf = io.BytesIO()
        pq.write_table(pa.table({"award": ["a", "b"]}), buf)
        data = buf.getvalue()
        length = int.from_bytes(data[-8:-4], "little")
        s3.get_object.side_effect = [{"Body": io.BytesIO(data[-8:])},
                                    {"Body": io.BytesIO(data[-length-8:])}]
        self.assertEqual(award_fetch.previous_rows(s3, 'openalex-ingest', 'awards/dev-gr1009/nsf/'), 2)
        s3.get_object.side_effect = OSError('S3 unavailable')
        with self.assertRaises(OSError):
            award_fetch.previous_rows(s3, 'openalex-ingest', 'awards/dev-gr1009/nsf/')

    def test_shrinking_output_cannot_upload(self):
        import pyarrow as pa
        def build(path):
            out = path / "out.parquet"
            pq.write_table(pa.Table.from_pylist([{"awd_id": "1"}], schema=NSF_SCHEMA), out)
            return out
        with patch('sys.argv', ['nsf_awards']), patch.object(award_fetch.boto3, 'client') as client, \
                patch.object(award_fetch, 'previous_rows', return_value=2):
            with self.assertRaisesRegex(ValueError, 'shrank'):
                award_fetch.run('nsf', build, NSF_SCHEMA)
            client.return_value.upload_file.assert_not_called()

    def test_builder_failure_never_uploads_and_cleans_temp(self):
        directory = []
        def fail(path):
            directory.append(path)
            (path / 'partial.zip').write_bytes(b'partial')
            raise ValueError('failed vendor input')
        with patch('sys.argv', ['nsf_awards']), patch.object(award_fetch.boto3, 'client') as client:
            with self.assertRaises(ValueError):
                award_fetch.run('nsf', fail, NSF_SCHEMA)
            client.assert_not_called()
        self.assertFalse(directory[0].exists())


if __name__ == '__main__':
    unittest.main()
