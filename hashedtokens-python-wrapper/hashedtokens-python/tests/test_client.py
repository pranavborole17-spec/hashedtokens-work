import asyncio
import base64
import hashlib
import json
import unittest
from hashedtokens import (
    EDMSProtocolError,
    TransactionTimeout,
    request,
)


def _http_response(status, body, content_type="application/json"):
    encoded = json.dumps(body).encode("utf-8") if not isinstance(body, bytes) else body
    reason = {200: "OK", 202: "Accepted", 400: "Bad Request", 404: "Not Found"}[status]
    headers = (
        "HTTP/1.1 {} {}\r\n".format(status, reason)
        + "Content-Type: {}\r\n".format(content_type)
        + "Content-Length: {}\r\n".format(len(encoded))
        + "Connection: close\r\n\r\n"
    ).encode("ascii")
    return headers + encoded


def _server_frame(value):
    payload = json.dumps(value).encode("utf-8")
    size = len(payload)
    if size < 126:
        return bytes((0x81, size)) + payload
    if size <= 0xFFFF:
        return bytes((0x81, 126)) + size.to_bytes(2, "big") + payload
    return bytes((0x81, 127)) + size.to_bytes(8, "big") + payload


async def _read_client_frame(reader):
    first, second = await reader.readexactly(2)
    length = second & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    mask = await reader.readexactly(4)
    payload = await reader.readexactly(length)
    decoded = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return first & 0x0F, decoded


class TargetServer:
    def __init__(self):
        self.requests = []
        self.status = 200
        self.body = {"posts": []}

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    @property
    def url(self):
        return "http://127.0.0.1:{}/api/posts".format(self.port)

    async def _handle(self, reader, writer):
        try:
            line = await reader.readline()
            method, target, _ = line.decode("ascii").strip().split(" ", 2)
            headers = {}
            while True:
                header = await reader.readline()
                if header == b"\r\n":
                    break
                name, value = header.decode("latin-1").split(":", 1)
                headers[name.lower()] = value.strip()
            body = await reader.readexactly(int(headers.get("content-length", "0")))
            self.requests.append((method, target, headers, body))
            writer.write(_http_response(self.status, self.body))
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def close(self):
        self.server.close()
        await self.server.wait_closed()


class EDMSMock:
    def __init__(
        self,
        *,
        status_code=0,
        immediate_error=False,
        disconnect_ws=False,
        send_result=True,
        omit_transaction_id=False,
    ):
        self.status_code = status_code
        self.immediate_error = immediate_error
        self.disconnect_ws = disconnect_ws
        self.send_result = send_result
        self.omit_transaction_id = omit_transaction_id
        self.submissions = []
        self.order = []
        self.ws_writers = []
        self.poll_count = 0
        self.existing_endpoint_id = None
        self._counter = 0
        self._send_lock = asyncio.Lock()

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    @property
    def base_url(self):
        return "http://127.0.0.1:{}".format(self.port)

    @property
    def ws_connections(self):
        return len(self.ws_writers)

    async def _handle(self, reader, writer):
        try:
            line = await reader.readline()
            if not line:
                return
            method, target, _ = line.decode("ascii").strip().split(" ", 2)
            headers = {}
            while True:
                header = await reader.readline()
                if header == b"\r\n":
                    break
                name, value = header.decode("latin-1").split(":", 1)
                headers[name.lower()] = value.strip()

            if method == "GET" and target.endswith("/hashedtokens/edms/api/ws"):
                await self._handle_websocket(headers, reader, writer)
                return

            body = await reader.readexactly(int(headers.get("content-length", "0")))
            if method == "GET" and target.split("?", 1)[0].endswith("/endpoints/lookup"):
                self.order.append("lookup")
                if self.existing_endpoint_id is None:
                    writer.write(_http_response(404, {"ok": False}))
                else:
                    writer.write(_http_response(200, {
                        "ok": True,
                        "endpoint": {"endpoint_id": self.existing_endpoint_id},
                    }))
                await writer.drain()
                return

            if method == "POST" and target.endswith("/hashedtokens/edms/api/collections/"):
                self.order.append("post")
                self.submissions.append(json.loads(body))
                if self.immediate_error:
                    writer.write(_http_response(400, {
                        "ok": False,
                        "status": {"code": 103, "name": "INVALID_PARAM", "message": "invalid"},
                        "detail": "invalid test value",
                    }))
                    await writer.drain()
                    return

                self._counter += 1
                transaction_id = "tx-{}".format(self._counter)
                accepted = {"ok": True, "ops": "insert"}
                if not self.omit_transaction_id:
                    accepted["transaction_id"] = transaction_id
                writer.write(_http_response(202, accepted))
                await writer.drain()
                if self.send_result and not self.disconnect_ws:
                    asyncio.create_task(self._send_results(transaction_id, self.submissions[-1]))
                return

            if method == "GET" and "/jobs/" in target:
                self.poll_count += 1
                transaction_id = target.rsplit("/", 1)[-1]
                result = self._result(transaction_id, {})
                writer.write(_http_response(200, {"status": "done", "result": result}))
                await writer.drain()
                return

            writer.write(_http_response(404, {"ok": False}))
            await writer.drain()
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def _handle_websocket(self, headers, reader, writer):
        self.order.append("websocket")
        key = headers["sec-websocket-key"]
        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
            ).digest()
        ).decode("ascii")
        writer.write(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "Sec-WebSocket-Accept: {}\r\n\r\n".format(accept)
            ).encode("ascii")
        )
        await writer.drain()
        self.ws_writers.append(writer)
        writer.write(_server_frame({"type": "transactions", "transactions": []}))
        await writer.drain()
        if self.disconnect_ws:
            writer.close()
            await writer.wait_closed()
            return
        try:
            while True:
                opcode, payload = await _read_client_frame(reader)
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    writer.write(bytes((0x8A, len(payload))) + payload)
                    await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass

    async def _send_results(self, transaction_id, submission):
        await asyncio.sleep(0.01)
        async with self._send_lock:
            for writer in list(self.ws_writers):
                if writer.is_closing():
                    continue
                writer.write(_server_frame({
                    "type": "event",
                    "event": {
                        "type": "DevApiResult",
                        "payload": {
                            "transaction_id": "unrelated",
                            "op": "insert",
                            "result": self._result("unrelated", submission),
                        },
                    },
                }))
                writer.write(_server_frame({
                    "type": "event",
                    "event": {
                        "type": "DevApiResult",
                        "payload": {
                            "transaction_id": transaction_id,
                            "op": "insert",
                            "result": self._result(transaction_id, submission),
                        },
                    },
                }))
                await writer.drain()

    def _result(self, transaction_id, submission):
        endpoint = submission.get("endpoint_str", "")
        endpoint_name = endpoint.rsplit("/", 1)[-1] or "root"
        return {
            "status": {
                "code": self.status_code,
                "name": {
                    0: "OK",
                    -1: "UNKNOWN",
                    100: "INVALID_REQUEST",
                    101: "MISSING_PARAM",
                    102: "UNKNOWN_OP",
                    103: "INVALID_PARAM",
                    104: "DUPLICATE_QP",
                    105: "TEST_FAILED",
                    106: "TEST_TIMED_OUT",
                    107: "COMPUTE_UNAVAILABLE",
                    108: "COMPUTE_STALLED",
                    109: "INTERNAL_ERROR",
                }.get(self.status_code, "UNKNOWN"),
                "message": "test result",
            },
            "endpoint_id": "eid-" + endpoint_name,
            "request_number": 1,
        }

    async def close(self):
        self.server.close()
        await self.server.wait_closed()
        for writer in self.ws_writers:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()


class WrapperTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.target = await TargetServer().start()
        self.edms = await EDMSMock().start()

    async def asyncTearDown(self):
        await self.target.close()
        await self.edms.close()

    async def test_complete_insert_transaction_and_header_redaction(self):
        result = await request(
            self.target.url + "?page=1",
            payload={},
            method="GET",
            headers={
                "Authorization": "Bearer secret",
                "Accept": "application/json",
            },
            edms_url=self.edms.base_url,
            collection="demo",
            tags=["blog"],
        )

        self.assertEqual(len(self.target.requests), 1)
        self.assertEqual(len(self.edms.submissions), 1)
        self.assertEqual(self.edms.order, ["lookup", "websocket", "post"])
        self.assertEqual(result.response_obj.status_code, 200)
        self.assertEqual(result.response_obj.json(), {"posts": []})
        self.assertEqual(result.ht_response["status"]["code"], 0)
        submitted = self.edms.submissions[0]
        self.assertEqual(submitted["ops"], "insert")
        self.assertEqual(submitted["endpoint_str"], self.target.url + "?page=1")
        self.assertEqual(submitted["request_body"], {})
        self.assertEqual(submitted["response_body"], {"posts": []})
        self.assertEqual(submitted["tags"], ["blog"])
        self.assertEqual(submitted["request_headers"]["Authorization"], "[REDACTED]")
        self.assertNotIn("status_code", submitted)
        self.assertNotIn("time_ms", submitted)
        self.assertGreaterEqual(result.response_obj.elapsed_ms, 0)

    async def test_non_success_target_status_is_still_saved(self):
        self.target.status = 404
        self.target.body = {"error": "missing"}

        result = await request(
            self.target.url,
            method="GET",
            edms_url=self.edms.base_url,
        )

        self.assertEqual(result.response_obj.status_code, 404)
        self.assertEqual(result.ht_response["status"]["code"], 0)
        self.assertEqual(self.edms.submissions[0]["response_body"], {"error": "missing"})

    async def test_post_captures_request_body_and_additional_redaction(self):
        body = {"title": "hello"}
        result = await request(
            self.target.url,
            body,
            "POST",
            headers={"X-Private": "secret"},
            redact_headers=["X-Private"],
            edms_url=self.edms.base_url,
        )

        self.assertEqual(json.loads(self.target.requests[0][3]), body)
        self.assertEqual(self.edms.submissions[0]["request_body"], body)
        self.assertEqual(
            self.edms.submissions[0]["request_headers"]["X-Private"], "[REDACTED]"
        )
        self.assertEqual(result.response_obj.request_method, "POST")

    async def test_duplicate_status_is_returned(self):
        self.edms.status_code = 104

        result = await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(result.ht_response["status"]["code"], 104)

    async def test_edms_test_timeout_status_is_not_client_timeout(self):
        self.edms.status_code = 106

        result = await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(result.ht_response["status"]["code"], 106)

    async def test_existing_endpoint_id_is_reused(self):
        self.edms.existing_endpoint_id = "E0001-AAA"

        await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(self.edms.submissions[0]["endpoint_id"], "E0001-AAA")

    async def test_immediate_edms_error_is_returned(self):
        self.edms.immediate_error = True

        result = await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(result.response_obj.status_code, 200)
        self.assertEqual(result.ht_response["status"]["code"], 103)

    async def test_missing_transaction_id_is_a_protocol_error(self):
        self.edms.omit_transaction_id = True

        with self.assertRaises(EDMSProtocolError) as raised:
            await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(raised.exception.response_obj.status_code, 200)

    async def test_websocket_disconnect_uses_documented_job_polling(self):
        self.edms.disconnect_ws = True

        result = await request(self.target.url, method="GET", edms_url=self.edms.base_url)

        self.assertEqual(result.ht_response["status"]["code"], 0)
        self.assertEqual(self.edms.poll_count, 1)

    async def test_concurrent_requests_are_correlated_by_transaction_id(self):
        first, second = await asyncio.gather(
            request(self.target.url + "/one", method="GET", edms_url=self.edms.base_url),
            request(self.target.url + "/two", method="GET", edms_url=self.edms.base_url),
        )

        self.assertEqual(len(self.edms.submissions), 2)
        self.assertEqual(first.ht_response["endpoint_id"], "eid-one")
        self.assertEqual(second.ht_response["endpoint_id"], "eid-two")
        self.assertEqual(self.edms.order.count("websocket"), 1)

    async def test_total_deadline_preserves_original_response(self):
        self.edms.send_result = False

        with self.assertRaises(TransactionTimeout) as raised:
            await request(
                self.target.url,
                method="GET",
                edms_url=self.edms.base_url,
                timeout=0.5,
            )

        self.assertEqual(raised.exception.response_obj.status_code, 200)

    async def test_malformed_final_result_raises_with_original_response(self):
        class BadResultEDMS(EDMSMock):
            async def _send_results(self, transaction_id, submission):
                await asyncio.sleep(0.01)
                async with self._send_lock:
                    for writer in list(self.ws_writers):
                        writer.write(_server_frame({
                            "type": "event",
                            "event": {
                                "type": "DevApiResult",
                                "payload": {
                                    "transaction_id": transaction_id,
                                    "op": "insert",
                                    "result": {"status": {"code": 999}},
                                },
                            },
                        }))
                        await writer.drain()

        await self.edms.close()
        self.edms = await BadResultEDMS().start()
        with self.assertRaises(EDMSProtocolError) as raised:
            await request(self.target.url, method="GET", edms_url=self.edms.base_url)
        self.assertEqual(raised.exception.response_obj.status_code, 200)

    async def test_invalid_method_fails_before_network(self):
        with self.assertRaises(ValueError):
            await request(self.target.url, method="TRACE", edms_url=self.edms.base_url)
        self.assertEqual(self.target.requests, [])
        self.assertEqual(self.edms.submissions, [])


if __name__ == "__main__":
    unittest.main()
