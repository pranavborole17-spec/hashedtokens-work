import asyncio
import json
import unittest

from hashedtokens.client import HashedTokensClient
from hashedtokens.models import ExternalResponse


class FakeHTTPClient:
    def __init__(self):
        self.calls = []

    async def request(self, url, method="GET", payload=None, headers=None, timeout=15.0, **kwargs):
        self.calls.append((url, method, payload))
        if url.startswith("http://localhost:3000/hashedtokens/edms/api/collections"):
            tx_id = f"tx-{len(self.calls)}"
            return ExternalResponse(
                url=url,
                method=method,
                status_code=202,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"ok": True, "transaction_id": tx_id, "ops": "insert"}).encode("utf-8"),
            )
        if url.startswith("https://example.com/data"):
            return ExternalResponse(
                url=url,
                method=method,
                status_code=200,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"value": url}).encode("utf-8"),
            )
        return ExternalResponse(url=url, method=method, status_code=200, headers={}, body=b"{}")


class FakeWebSocketManager:
    async def connect(self, timeout=None):
        return None

    async def wait_for_transaction(self, transaction_id, timeout=None):
        await asyncio.sleep(0.01)
        return {
            "type": "event",
            "event": {"payload": {"transaction_id": transaction_id, "op": "insert", "result": {"status": {"code": 0, "name": "OK"}}}},
        }


class TestConcurrency(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_requests_keep_transaction_results_separate(self):
        client = HashedTokensClient(
            edms_url="http://localhost:3000/hashedtokens/edms/api/collections/",
            ws_url="ws://localhost:3000/hashedtokens/edms/api/ws",
            timeout=5,
            http_client=FakeHTTPClient(),
            websocket_manager=FakeWebSocketManager(),
        )

        async def run_request(value):
            return await client.request(value, None, "GET", timeout=5)

        results = await asyncio.gather(run_request("https://example.com/data/1"), run_request("https://example.com/data/2"))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].response_obj.json["value"], "https://example.com/data/1")
        self.assertEqual(results[1].response_obj.json["value"], "https://example.com/data/2")
        self.assertEqual(results[0].ht_response["result"]["status"]["code"], 0)
        self.assertEqual(results[1].ht_response["result"]["status"]["code"], 0)
