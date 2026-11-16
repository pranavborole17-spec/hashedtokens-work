import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from hashedtokens.async_http import AsyncHTTPClient


class FakeWriter:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, data):
        self.data.extend(data)

    async def drain(self):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class TestAsyncHTTP(unittest.IsolatedAsyncioTestCase):
    async def _request(self, response: bytes, url: str, **kwargs):
        reader = make_reader(response)
        writer = FakeWriter()
        with patch("asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))) as open_connection:
            result = await AsyncHTTPClient().request(url, **kwargs)
        return result, bytes(writer.data), open_connection

    async def test_get_query_params_are_encoded_and_existing_params_preserved(self):
        response, sent, _ = await self._request(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
            "http://example.test/?existing=1",
            method="GET",
            payload={"page": 1, "q": "hello world"},
        )
        self.assertIn(b"GET /?existing=1&page=1&q=hello+world HTTP/1.1", sent)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"ok": True})

    async def test_post_json_request_body_is_sent(self):
        response, sent, _ = await self._request(
            b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
            "http://example.test/",
            method="POST",
            payload={"page": 1, "ok": True},
            headers={"Authorization": "Bearer secret"},
        )
        self.assertIn(b"Content-Type: application/json\r\n", sent)
        self.assertIn(b"Authorization: Bearer secret\r\n", sent)
        self.assertTrue(sent.endswith(b'{"page":1,"ok":true}'))
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json, {"ok": True})
        self.assertEqual(response.request_headers["Authorization"], "[REDACTED]")

    async def test_put_patch_and_delete_are_supported(self):
        for method in ("PUT", "PATCH", "DELETE"):
            with self.subTest(method=method):
                response, sent, _ = await self._request(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
                    "http://example.test/resource",
                    method=method,
                    payload={"value": 1},
                )
                self.assertIn(f"{method} /resource HTTP/1.1".encode(), sent)
                self.assertEqual(response.status_code, 204)

    async def test_chunked_response_is_decoded(self):
        response, _, _ = await self._request(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: done\r\n\r\n",
            "http://example.test/",
            method="GET",
        )
        self.assertEqual(response.body, b"hello world")
        self.assertEqual(response.text, "hello world")

    async def test_external_http_404_is_preserved(self):
        response, _, _ = await self._request(
            b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{\"error\":\"nope\"}",
            "http://example.test/missing",
            method="GET",
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json, {"error": "nope"})

    async def test_https_uses_default_certificate_validation(self):
        reader = make_reader(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        writer = FakeWriter()
        ssl_context = object()
        with patch("ssl.create_default_context", return_value=ssl_context), patch(
            "asyncio.open_connection", new=AsyncMock(return_value=(reader, writer))
        ) as open_connection:
            await AsyncHTTPClient().request("https://example.test/", method="GET")
        self.assertIs(open_connection.await_args.kwargs["ssl"], ssl_context)
