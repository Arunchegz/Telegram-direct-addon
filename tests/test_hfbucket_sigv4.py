"""
Regression tests for the hand-rolled SigV4 presigner in hfbucket.py.

The expected signature below is a fixed known-answer vector: if canonical
request, query-string or signing key logic ever changes, the baked constant
stops matching and the test fails loudly.
"""

import os
import unittest
from datetime import datetime, timezone

os.environ["HF_S3_ACCESS_KEY"] = "AKIDEXAMPLE"
os.environ["HF_S3_SECRET_KEY"] = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
os.environ["HF_S3_ENDPOINT"] = "https://s3.hf.co"
os.environ["HF_BUCKET_ID"] = "arunchegz1/Telegram_stremio-storage"
os.environ["HF_S3_REGION"] = "us-east-1"
os.environ["HF_S3_EXPIRES"] = "3600"

from hfbucket import (  # noqa: E402  (env must be set before import)
    presigned_uri,
    _sigv4_canonical_request,
    _sigv4_query_string,
    _sigv4_signing_key,
)

EXPECTED_SIGNATURE = "02b463348fba4bd8a360c3b7e4df2a378c53a18bc543a871adb79bc483777bd5"


class TestSigV4(unittest.TestCase):
    def test_presigned_uri_known_answer(self):
        fixed = datetime(2026, 8, 10, 4, 0, 0, tzinfo=timezone.utc)
        url = presigned_uri("GET", "tgstream/foo.mkv", now=fixed)
        self.assertTrue(
            url.startswith("https://s3.hf.co/arunchegz1/tgstream/foo.mkv?"),
            url,
        )
        self.assertIn("X-Amz-Algorithm=AWS4-HMAC-SHA256", url)
        self.assertIn("X-Amz-Credential=AKIDEXAMPLE%2F20260810%2Fus-east-1%2Fs3%2Faws4_request", url)
        self.assertIn("X-Amz-Date=20260810T040000Z", url)
        self.assertIn("X-Amz-Expires=3600", url)
        self.assertIn("X-Amz-SignedHeaders=host", url)
        self.assertEqual(url.split("X-Amz-Signature=")[1], EXPECTED_SIGNATURE)

    def test_query_string_sorted_and_escaped(self):
        qs = _sigv4_query_string("AKIDEXAMPLE", "20260810T040000Z", "20260810", "us-east-1")
        parts = qs.split("&")
        # X-Amz-Algorithm must sort first; keys are percent-escaped
        self.assertTrue(parts[0].startswith("X-Amz-Algorithm="))
        self.assertIn("X-Amz-Credential=AKIDEXAMPLE%2F20260810%2Fus-east-1%2Fs3%2Faws4_request", parts)

    def test_canonical_request_shape(self):
        qs = _sigv4_query_string("AKIDEXAMPLE", "20260810T040000Z", "20260810", "us-east-1")
        cr = _sigv4_canonical_request("GET", "/arunchegz1/tgstream/foo.mkv", qs, "s3.hf.co")
        lines = cr.split("\n")
        self.assertEqual(lines[0], "GET")
        self.assertEqual(lines[1], "/arunchegz1/tgstream/foo.mkv")
        # canonical_headers ends with a required trailing newline, so the
        # split produces an empty line before the signed-headers entry
        self.assertEqual(lines[3], "host:s3.hf.co")
        self.assertEqual(lines[4], "")
        self.assertEqual(lines[5], "host")
        self.assertEqual(lines[6], "UNSIGNED-PAYLOAD")

    def test_signing_key_length(self):
        key = _sigv4_signing_key("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20260810", "us-east-1")
        self.assertEqual(len(key), 32)


if __name__ == "__main__":
    unittest.main()