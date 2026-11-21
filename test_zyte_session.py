"""Offline check: an expired Zyte session is reopened and the same URL fetched again (oxjob #883).
python -m unittest -v test_zyte_session. No network, S3 or database."""
import base64
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SOURCES_DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")
os.environ.setdefault("ZYTE_API_KEY", "test")

import repositories as R

XML = b'<?xml version="1.0"?><OAI-PMH><ListRecords/></OAI-PMH>'
EXPIRED = '{"detail": "Session has expired. Try creating a new session.", "status": 422, "title": "Session has expired"}'


class Resp:
    def __init__(self, status, body=None, text=""):
        self.status_code, self._body, self.text = status, body, text

    def json(self):
        return self._body


OPENED = Resp(200, {"browserHtml": "<html></html>"})
PLAIN = Resp(200, {"statusCode": 200, "url": "u", "httpResponseHeaders": [],
                   "httpResponseBody": base64.b64encode(XML).decode()})


class ZyteSessionExpiry(unittest.TestCase):
    def setUp(self):
        R._zyte_sessions.clear()

    def test_expired_session_is_reopened_and_url_retried(self):
        calls = [OPENED, Resp(422, text=EXPIRED), OPENED, PLAIN]
        with patch.object(R.requests, "post", side_effect=calls) as post:
            r = R.zyte_get("https://h.example/oai?verb=ListRecords&resumptionToken=t1", {},
                           session_init_url="https://h.example/oai?verb=ListRecords")
        self.assertEqual(r.content, XML)
        sessions = [c.kwargs["json"]["session"]["id"] for c in post.call_args_list]
        self.assertEqual(sessions[0], sessions[1])           # first fetch used the first session
        self.assertNotEqual(sessions[1], sessions[3])        # retry ran in a new one
        self.assertEqual(post.call_args_list[1].kwargs["json"]["url"], post.call_args_list[3].kwargs["json"]["url"])
        self.assertEqual(R._zyte_sessions["h.example"], sessions[3])

    def test_expired_twice_raises_connection_error(self):
        calls = [OPENED, Resp(422, text=EXPIRED), OPENED, Resp(422, text=EXPIRED)]
        with patch.object(R.requests, "post", side_effect=calls):
            with self.assertRaises(R.requests.exceptions.ConnectionError):
                R.zyte_get("https://h.example/oai?x=1", {}, session_init_url="https://h.example/oai")

    def test_other_422_is_not_retried(self):
        calls = [OPENED, Resp(422, text='{"detail": "bad header"}')]
        with patch.object(R.requests, "post", side_effect=calls) as post:
            with self.assertRaises(R.requests.exceptions.ConnectionError):
                R.zyte_get("https://h.example/oai?x=1", {}, session_init_url="https://h.example/oai")
        self.assertEqual(post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
