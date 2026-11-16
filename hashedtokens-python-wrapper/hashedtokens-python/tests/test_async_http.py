import asyncio
import json
import unittest

from hashedtokens import (
    HTTPConnectionError,
    HTTPProtocolError,
    HTTPRequestTimeout,
    request_external,
)


class AsyncHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.responses = []
        self.server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def _handle_client(self, reader, writer):
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            method, target, _ = request_line.decode("ascii").rstrip("\r\n").split(" ", 2)
            headers = {}
            while True:
                line = await reader.readline()
                if line == b"\r\n":
                    break
                name, value = line.decode("latin-1").split(":", 1)
                headers[name.lower()] = value.strip()
            body = await reader.readexactly(int(headers.get("content-length", "0")))
            self.requests.append((method, target, headers, body))

            response = self.responses.pop(0)
            if isinstance(response, asyncio.Event):
                await response.wait()
                response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
            writer.write(response)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    def url(self, path="/"):
        return "http://127.0.0.1:{0}{1}".format(self.port, path)

    async def test_get_encodes_query_and_parses_json_response(self):
        self.responses.append(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}"
        )

        response = await request_external(self.url("/items?existing=1"), {"tag": ["a", "b"]}, "GET")

        self.assertEqual(self.requests[0][0:2], ("GET", "/items?existing=1&tag=a&tag=b"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})

    async def test_post_sends_json_and_custom_headers(self):
        self.responses.append(
            b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n"
            b"Content-Length: 2\r\nConnection: close\r\n\r\n{}"
        )

        response = await request_external(
            self.url("/items"),
            {"name": "item"},
            "POST",
            headers={"X-Test": "yes"},
        )

        method, target, headers, body = self.requests[0]
        self.assertEqual((method, target), ("POST", "/items"))
        self.assertEqual(headers["x-test"], "yes")
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(int(headers["content-length"]), len(body))
        self.assertEqual(json.loads(body), {"name": "item"})
        self.assertEqual(response.status_code, 201)

    async def test_put_patch_and_delete_are_sent(self):
        for method in ("PUT", "PATCH", "DELETE"):
            self.responses.append(
                b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
            )
            response = await request_external(self.url("/resource"), {"v": 1}, method)
            self.assertEqual(response.status_code, 204)
        self.assertEqual([item[0] for item in self.requests], ["PUT", "PATCH", "DELETE"])

    async def test_chunked_and_non_json_responses_are_preserved(self):
        self.responses.append(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n"
            b"5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trace: yes\r\n\r\n"
        )

        response = await request_external(self.url(), method="GET")

        self.assertEqual(response.body, b"hello world")
        self.assertEqual(response.text, "hello world")
        self.assertEqual(response.get_header("x-trace"), None)

    async def test_http_error_status_is_returned_not_raised(self):
        self.responses.append(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n"
            b"Connection: close\r\n\r\nbusy"
        )

        response = await request_external(self.url())

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.text, "busy")

    async def test_response_without_content_length_is_read_until_close(self):
        self.responses.append(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nplain text")

        response = await request_external(self.url())

        self.assertEqual(response.body, b"plain text")

    async def test_invalid_input_is_rejected(self):
        with self.assertRaises(ValueError):
            await request_external("ftp://example.com/")
        with self.assertRaises(ValueError):
            await request_external(self.url(), method="TRACE")
        with self.assertRaises(ValueError):
            await request_external(self.url(), timeout=0)
        with self.assertRaises(ValueError):
            await request_external(self.url(), headers={"X-Test": "bad\r\nInjected: yes"})
        with self.assertRaises(ValueError):
            await request_external(self.url(), {"not": object()}, "POST")

    async def test_malformed_response_is_reported(self):
        self.responses.append(b"not an HTTP response\r\n")

        with self.assertRaises(HTTPProtocolError):
            await request_external(self.url())

    async def test_total_timeout_is_reported(self):
        self.responses.append(asyncio.Event())

        with self.assertRaises(HTTPRequestTimeout):
            await request_external(self.url(), timeout=0.02)

    async def test_connection_failure_is_reported(self):
        self.server.close()
        await self.server.wait_closed()

        with self.assertRaises(HTTPConnectionError):
            await request_external(self.url(), timeout=1)


if __name__ == "__main__":
    unittest.main()
