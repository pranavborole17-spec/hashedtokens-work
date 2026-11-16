import json
import unittest

from hashedtokens.client import HashedTokensClient
from hashedtokens.models import ExternalResponse


class FakeHTTPClient:
    def __init__(self):
        self.calls = []

    async def request(self, url, method="GET", payload=None, headers=None, timeout=15.0, **kwargs):
        self.calls.append({"url": url, "method": method, "payload": payload, "headers": headers})
        if url == "https://example.com/api/posts?page=1":
            return ExternalResponse(
                url=url,
                method=method,
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"posts": []}).encode("utf-8"),
            )
        if url == "http://localhost:3000/hashedtokens/edms/api/collections/":
            return ExternalResponse(
                url=url,
                method=method,
                status_code=202,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"ok": True, "transaction_id": "tx-42", "ops": "insert"}).encode("utf-8"),
            )
        if url == "http://localhost:3000/jobs/tx-42":
            return ExternalResponse(
                url=url,
                method=method,
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"status": "done", "result": {"status": {"code": 0, "name": "OK"}, "endpoint_id": "E0001-AAA", "request_number": 1}}).encode("utf-8"),
            )
        raise AssertionError(f"Unexpected URL: {url}")


class FakeWebSocketManager:
    async def connect(self, timeout=None):
        return None

    async def wait_for_transaction(self, transaction_id, timeout=None):
        return {
            "type": "event",
            "event": {
                "type": "DevApiResult",
                "payload": {
                    "transaction_id": transaction_id,
                    "op": "insert",
                    "result": {"status": {"code": 0, "name": "OK"}, "endpoint_id": "E0001-AAA", "request_number": 1},
                },
            },
        }


class TestClient(unittest.IsolatedAsyncioTestCase):
    async def test_client_returns_original_response_and_edms_result(self):
        client = HashedTokensClient(
            edms_url="http://localhost:3000/hashedtokens/edms/api/collections/",
            ws_url="ws://localhost:3000/hashedtokens/edms/api/ws",
            timeout=5.0,
            http_client=FakeHTTPClient(),
            websocket_manager=FakeWebSocketManager(),
        )

        result = await client.request("https://example.com/api/posts?page=1", {"page": 1}, "GET", timeout=5)
        self.assertEqual(result.response_obj.status_code, 200)
        self.assertEqual(result.response_obj.json, {"posts": []})
        self.assertTrue(result.ht_response["ok"])
        self.assertEqual(result.ht_response["result"]["status"]["code"], 0)
