"""Offline checks for the bot-wall detector that sends an endpoint to a Zyte browser session (oxjobs #1588, #883):
python -m unittest -v test_bot_challenge. No network, S3 or database."""
import os
import unittest

import requests

os.environ.setdefault("SOURCES_DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import repositories as R


def resp(body, status=200):
    r = requests.Response()
    r.status_code = status
    r._content = body.encode()
    return r


# Bodies as KAUST's Imperva edge served them on 2026-11-16 (direct, then through Zyte's plain mode).
INCAPSULA_DIRECT = ('<html style="height:100%"><head><META NAME="ROBOTS" CONTENT="NOINDEX, NOFOLLOW">'
                    '<meta name="format-detection" content="telephone=no"></head><body><iframe '
                    'src="/_Incapsula_Resource?CWUDNSAI=24&xinfo=1" frameborder=0></iframe></body></html>')
INCAPSULA_ZYTE = ('<html>\n<head>\n<META NAME="robots" CONTENT="noindex,nofollow">\n'
                  '<script src="/_Incapsula_Resource?SWJIYLWA=5074a744e2e3d891814e9a2dace20bd4,719d34d31c8e3a6e6fffd425f7e032f3">'
                  '</script>\n<body>\n</body></html>')


class BotChallenge(unittest.TestCase):
    def test_incapsula_pages_are_walls(self):
        self.assertTrue(R.is_bot_challenge(resp(INCAPSULA_DIRECT)))
        self.assertTrue(R.is_bot_challenge(resp(INCAPSULA_ZYTE)))

    def test_anubis_still_a_wall(self):
        self.assertTrue(R.is_bot_challenge(resp("<html><title>Making sure you're not a bot!</title></html>")))

    def test_oai_xml_is_never_a_wall(self):
        xml = ('<?xml version="1.0" encoding="UTF-8"?><OAI-PMH><ListRecords><record><metadata>'
               'see /_Incapsula_Resource in a description</metadata></record></ListRecords></OAI-PMH>')
        self.assertFalse(R.is_bot_challenge(resp(xml)))

    def test_ordinary_html_is_not_a_wall(self):
        self.assertFalse(R.is_bot_challenge(resp("<html><body>DSpace 7 home page</body></html>")))


if __name__ == "__main__":
    unittest.main()
