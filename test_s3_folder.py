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

    def test_own_folder_wins(self):
        self.assertEqual(R.s3_folder(ep("hal_tei", "xml-tei")), "hal-tei")
        self.assertEqual(R.s3_folder(ep("irdb_nii_ac_jp", "jpcoar_2.0")), "irdb")


if __name__ == "__main__":
    unittest.main()
