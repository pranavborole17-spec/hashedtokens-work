import asyncio
import json
import unittest

from hashedtokens.websocket import WebSocketClient, WebSocketManager


def event(transaction_id):
    return {
        "type": "event",
        "event": {
            "type": "DevApiResult",
            "payload": {
                "transaction_id": transaction_id,
                "op": "insert",
                "result": {"status": {"code": 0, "name": "OK"}},
            },
        },
    }


class QueueWebSocket:
    def __init__(self, messages):
        self.messages = asyncio.Queue()
        self.closed = False
        for message in messages:
            self.messages.put_nowait(message)

    async def receive(self):
        return await self.messages.get()


class TestWebSocket(unittest.IsolatedAsyncioTestCase):
    def test_extract_transaction_id_from_final_event(self):
        self.assertEqual(WebSocketManager._extract_transaction_id(event("tx-123")), "tx-123")

    def test_transactions_snapshot_is_not_a_final_result(self):
        snapshot = {"type": "transactions", "transactions": [{"transaction_id": "tx-123"}]}
        self.assertIsNone(WebSocketManager._extract_transaction_id(snapshot))

    async def test_unrelated_events_are_ignored_and_target_result_is_correlated(self):
        manager = WebSocketManager("ws://example.test", timeout=1)
        manager.client = QueueWebSocket([
            {"type": "transactions", "transactions": []},
            event("other-transaction"),
            event("target-transaction"),
        ])
        manager._reader_task = asyncio.create_task(manager._read_loop())
        try:
            result = await manager.wait_for_transaction("target-transaction", timeout=1)
            self.assertEqual(result["event"]["payload"]["transaction_id"], "target-transaction")
        finally:
            manager._reader_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await manager._reader_task

    async def test_fragmented_text_message_is_reassembled(self):
        message = json.dumps({"type": "transactions"}).encode("utf-8")
        split = len(message) // 2
        frames = (
            bytes([0x01, split]) + message[:split]
            + bytes([0x80, len(message) - split]) + message[split:]
        )
        reader = asyncio.StreamReader()
        reader.feed_data(frames)
        reader.feed_eof()
        client = WebSocketClient("ws://example.test")
        client.reader = reader
        opcode, payload = await client._read_message()
        self.assertEqual(opcode, 0x1)
        self.assertEqual(json.loads(payload), {"type": "transactions"})
