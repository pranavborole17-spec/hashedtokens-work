import asyncio
import base64
import hashlib
import unittest

from hashedtokens.websocket_client import (
    AsyncWebSocket,
    WebSocketProtocolError,
)


async def _read_masked_client_frame(reader):
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


class WebSocketClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.mode = "normal"
        self.client_control_frame = None

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    @property
    def url(self):
        return "ws://127.0.0.1:{}/hashedtokens/edms/api/ws".format(self.port)

    async def _handle(self, reader, writer):
        try:
            request_line = await reader.readline()
            headers = {}
            while True:
                line = await reader.readline()
                if line == b"\r\n":
                    break
                name, value = line.decode("latin-1").split(":", 1)
                headers[name.lower()] = value.strip()
            if self.mode == "bad-handshake":
                writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return

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

            if self.mode == "bad-frame":
                writer.write(b"\xC1\x00")
                await writer.drain()
                return
            if self.mode == "ping":
                writer.write(b"\x89\x04ping")
                await writer.drain()
                self.client_control_frame = await _read_masked_client_frame(reader)
                writer.write(b"\x81\x02{}")
                await writer.drain()
                return

            await reader.read()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def test_valid_handshake_and_text_frame(self):
        self.mode = "ping"
        websocket = await AsyncWebSocket.connect(self.url)
        try:
            message = await websocket.receive_text()
        finally:
            await websocket.close()

        self.assertEqual(message, "{}")
        self.assertEqual(self.client_control_frame, (0xA, b"ping"))

    async def test_handshake_failure_is_reported(self):
        self.mode = "bad-handshake"

        with self.assertRaises(WebSocketProtocolError):
            await AsyncWebSocket.connect(self.url)

    async def test_malformed_frame_is_reported(self):
        self.mode = "bad-frame"
        websocket = await AsyncWebSocket.connect(self.url)
        try:
            with self.assertRaises(WebSocketProtocolError):
                await websocket.receive_text()
        finally:
            await websocket.close()


if __name__ == "__main__":
    unittest.main()
