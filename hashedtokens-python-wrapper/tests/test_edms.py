import unittest

from hashedtokens.edms import EDMS_STATUS_CODES, build_collection_payload, sanitize_headers


class TestEDMS(unittest.TestCase):
    def test_payload_matches_documented_insert_fields(self):
        payload = build_collection_payload(
            url="https://example.com/api/posts?page=1",
            method="GET",
            collection="demo",
            request_payload={},
            response_payload={"posts": []},
            request_headers={"Accept": "application/json"},
            response_headers={"Content-Type": "application/json"},
            tags=["blog", "read"],
            collection_tags=["v1"],
            annotation="list posts",
            collection_annotation="blog api",
            notes="bookmark note",
            timeout_ms=1000,
        )
        self.assertEqual(payload["ops"], "insert")
        self.assertEqual(payload["endpoint_str"], "https://example.com/api/posts?page=1")
        self.assertEqual(payload["method"], "GET")
        self.assertEqual(payload["collection"], "demo")
        self.assertEqual(payload["response_body"], {"posts": []})
        self.assertEqual(payload["tags"], ["blog", "read"])
        self.assertEqual(payload["timeout_ms"], 1000)

    def test_status_codes_are_documented(self):
        self.assertEqual(EDMS_STATUS_CODES[0], "OK")
        self.assertEqual(EDMS_STATUS_CODES[101], "MISSING_PARAM")
        self.assertEqual(EDMS_STATUS_CODES[104], "DUPLICATE_QP")
        self.assertEqual(EDMS_STATUS_CODES[108], "COMPUTE_STALLED")

    def test_sensitive_headers_are_redacted_before_storage(self):
        cleaned = sanitize_headers({
            "Authorization": "Bearer secret",
            "Cookie": "session=secret",
            "Accept": "application/json",
        })
        self.assertEqual(cleaned["Authorization"], "[REDACTED]")
        self.assertEqual(cleaned["Cookie"], "[REDACTED]")
        self.assertEqual(cleaned["Accept"], "application/json")

    def test_wrapper_rejects_replay_operation(self):
        with self.assertRaisesRegex(ValueError, "ops='insert'"):
            build_collection_payload(
                url="https://example.com",
                method="GET",
                collection="demo",
                request_payload={},
                response_payload={},
                ops="test_and_insert",
            )
