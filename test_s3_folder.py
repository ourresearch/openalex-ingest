"""Offline check of where each endpoint's harvested files land (oxjob #2429): python -m unittest -v test_s3_folder."""
import os
from types import SimpleNamespace
import unittest

os.environ.setdefault("SOURCES_DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import repositories as R


def ep(id, prefix):
    return SimpleNamespace(id=id, metadata_prefix=prefix)


class S3Folder(unittest.TestCase):
    def test_oai_dc_goes_under_repositories(self):
        self.assertEqual(R.s3_folder(ep("0a26303df1172c2974a", "oai_dc")), "repositories/0a26303df1172c2974a")

    def test_unlisted_prefixes_stay_under_repositories(self):
        # unpaywall_dc (ORA) and qdc are dc-shaped and read by repo_items as before
        self.assertEqual(R.s3_folder(ep("ora", "unpaywall_dc")), "repositories/ora")

    def test_oai_openaire_gets_its_own_folder(self):
        self.assertEqual(R.s3_folder(ep("0a26303df1172c2974a", "oai_openaire")), "openaire/0a26303df1172c2974a")

    def test_unreviewed_oai_openaire_feed_lands_in_trial(self):
        # switched but not yet through its dry run: nothing in production reads openaire-trial/
        self.assertEqual(R.s3_folder(ep("83acdeb0a2b75a2e162", "oai_openaire")), "openaire-trial/83acdeb0a2b75a2e162")

    def test_live_list_only_matters_for_oai_openaire(self):
        # a live endpoint switched back to oai_dc goes under repositories/ again
        self.assertEqual(R.s3_folder(ep("0a26303df1172c2974a", "oai_dc")), "repositories/0a26303df1172c2974a")

    def test_own_folder_wins(self):
        self.assertEqual(R.s3_folder(ep("hal_tei", "xml-tei")), "hal-tei")
        self.assertEqual(R.s3_folder(ep("irdb_nii_ac_jp", "jpcoar_2.0")), "irdb")


class BatchBytes(unittest.TestCase):
    def harvester(self, id, prefix):
        return R.EndpointHarvester(SimpleNamespace(id=id, metadata_prefix=prefix, fetch_via_zyte=False), db_session=None)

    def test_oai_openaire_files_are_capped(self):
        h = self.harvester("0a26303df1172c2974a", "oai_openaire")
        self.assertEqual(h.batch_bytes, 4_000_000)
        self.assertTrue(h.measure_bytes)

    def test_oai_dc_is_not_capped_or_measured(self):
        h = self.harvester("0a26303df1172c2974a", "oai_dc")
        self.assertEqual(h.batch_bytes, float("inf"))
        self.assertFalse(h.measure_bytes)

    def test_hal_tei_keeps_its_own_cap(self):
        self.assertEqual(self.harvester("hal_tei", "xml-tei").batch_bytes, 32_000_000)


if __name__ == "__main__":
    unittest.main()
