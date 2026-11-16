from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import struct
import weakref
from collections import OrderedDict
from typing import Any
from urllib.parse import urlsplit


_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_MAX_HEADER_BYTES = 64 * 1024
_MAX_FRAME_BYTES = 4 * 1024 * 1024


class WebSocketError(Exception):
    """Base error for the standard-library WebSocket client."""


class WebSocketProtocolError(WebSocketError):
    """The peer returned an invalid WebSocket handshake or frame."""


class WebSocketClosed(WebSocketError):
    """The peer closed the WebSocket before the expected result arrived."""


class AsyncWebSocket:
    """A small RFC 6455 client for an unsecured ws:// connection."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._closed = False

    @classmethod
    async def connect(cls, url: str) -> "AsyncWebSocket":
        try:
            parsed = urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ValueError("ws_url has an invalid host or port") from exc

        if parsed.scheme != "ws" or not parsed.hostname:
            raise ValueError("WebSocket URL must use ws:// and include a host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("credentials in ws_url are not supported")
        if port == 0:
            raise ValueError("ws_url port must be between 1 and 65535")

        host = parsed.hostname
        if ":" not in host:
            try:
                host = host.encode("idna").decode("ascii")
            except UnicodeError as exc:
                raise ValueError("ws_url contains an invalid host name") from exc
        port = port or 80
        display_host = "[{}]".format(host) if ":" in host else host
        host_header = display_host if port == 80 else "{}:{}".format(display_host, port)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        reader, writer = await asyncio.open_connection(host, port)
        socket = cls(reader, writer)
        key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        request = (
            "GET {} HTTP/1.1\r\n"
            "Host: {}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: {}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).format(path, host_header, key)
        try:
            writer.write(request.encode("ascii"))
            await writer.drain()
            status_line = await reader.readline()
            if not status_line.endswith(b"\r\n"):
                raise WebSocketProtocolError("Incomplete WebSocket handshake status")
            try:
                status = status_line.decode("latin-1").rstrip("\r\n").split(" ", 2)
            except UnicodeDecodeError as exc:
                raise WebSocketProtocolError("Invalid WebSocket handshake status") from exc
            if len(status) < 2 or status[0] != "HTTP/1.1" or status[1] != "101":
                raise WebSocketProtocolError("EDMS did not accept the WebSocket upgrade")

            response_headers = await _read_handshake_headers(reader)
            if response_headers.get("upgrade", "").lower() != "websocket":
                raise WebSocketProtocolError("WebSocket handshake has no Upgrade: websocket")
            if "upgrade" not in {
                token.strip().lower()
                for token in response_headers.get("connection", "").split(",")
            }:
                raise WebSocketProtocolError("WebSocket handshake Connection header is invalid")
            expected_accept = base64.b64encode(
                hashlib.sha1((key + _GUID).encode("ascii")).digest()
            ).decode("ascii")
            if response_headers.get("sec-websocket-accept") != expected_accept:
                raise WebSocketProtocolError("WebSocket handshake accept key did not match")
            return socket
        except BaseException:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            raise

    async def receive_text(self) -> str:
        """Receive a complete text message, handling control and continuation frames."""
        message = bytearray()
        fragmented = False
        while True:
            fin, opcode, payload = await self._read_frame()

            if opcode == 0x8:
                await self._send_frame(0x8, payload[:125])
                self._closed = True
                raise WebSocketClosed("EDMS closed the WebSocket")
            if opcode == 0x9:
                await self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue

            if opcode == 0x1:
                if fragmented:
                    raise WebSocketProtocolError("New data frame during fragmented message")
                message = bytearray(payload)
                fragmented = not fin
            elif opcode == 0x0:
                if not fragmented:
                    raise WebSocketProtocolError("Unexpected continuation frame")
                message.extend(payload)
                fragmented = not fin
            else:
                raise WebSocketProtocolError("Only JSON text WebSocket messages are supported")

            if len(message) > _MAX_FRAME_BYTES:
                raise WebSocketProtocolError("WebSocket message exceeds the configured limit")
            if fin:
                try:
                    return message.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise WebSocketProtocolError("WebSocket text message is not valid UTF-8") from exc

    async def _read_frame(self) -> tuple[bool, int, bytes]:
        try:
            header = await self._reader.readexactly(2)
        except asyncio.IncompleteReadError as exc:
            raise WebSocketClosed("EDMS disconnected from the WebSocket") from exc

        first, second = header
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        if first & 0x70:
            raise WebSocketProtocolError("WebSocket extensions are not negotiated")
        if second & 0x80:
            raise WebSocketProtocolError("Server WebSocket frames must not be masked")

        length = second & 0x7F
        try:
            if length == 126:
                length = struct.unpack("!H", await self._reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", await self._reader.readexactly(8))[0]
                if length & (1 << 63):
                    raise WebSocketProtocolError("Invalid WebSocket payload length")
            if length > _MAX_FRAME_BYTES:
                raise WebSocketProtocolError("WebSocket frame exceeds the configured limit")
            if opcode >= 0x8 and (not fin or length > 125):
                raise WebSocketProtocolError("Invalid WebSocket control frame")
            payload = await self._reader.readexactly(length)
        except asyncio.IncompleteReadError as exc:
            raise WebSocketProtocolError("Incomplete WebSocket frame") from exc
        return fin, opcode, payload

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self._closed:
            return
        if len(payload) > 125 and opcode >= 0x8:
            raise WebSocketProtocolError("WebSocket control payload is too large")
        mask = secrets.token_bytes(4)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        length = len(payload)
        if length < 126:
            header = bytes((0x80 | opcode, 0x80 | length))
        elif length <= 0xFFFF:
            header = bytes((0x80 | opcode, 0x80 | 126)) + struct.pack("!H", length)
        else:
            header = bytes((0x80 | opcode, 0x80 | 127)) + struct.pack("!Q", length)
        self._writer.write(header + mask + masked)
        await self._writer.drain()

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                mask = secrets.token_bytes(4)
                payload = struct.pack("!H", 1000)
                masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
                self._writer.write(bytes((0x88, 0x80 | len(payload))) + mask + masked)
                await self._writer.drain()
            except (ConnectionError, OSError):
                pass
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class WebSocketHub:
    """Share one status stream and route DevApiResult events by transaction ID."""

    def __init__(self, url: str, cache_limit: int = 512):
        self._url = url
        self._cache_limit = cache_limit
        self._lock = asyncio.Lock()
        self._socket: AsyncWebSocket | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._leases = 0
        self._waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._early_results: OrderedDict[str, dict[str, Any]] = OrderedDict()

    async def acquire(self) -> None:
        """Ensure the shared socket is open before a caller submits a POST."""
        async with self._lock:
            if self._socket is not None and self._reader_task is not None:
                if not self._reader_task.done():
                    self._leases += 1
                    return

            socket = await AsyncWebSocket.connect(self._url)
            self._socket = socket
            self._reader_task = asyncio.create_task(self._read_events(socket))
            self._leases += 1

    async def wait_for_result(self, transaction_id: str) -> dict[str, Any]:
        """Wait for exactly the event matching this transaction ID."""
        async with self._lock:
            cached = self._early_results.pop(transaction_id, None)
            if cached is not None:
                return cached
            if self._socket is None or self._reader_task is None or self._reader_task.done():
                raise WebSocketClosed("EDMS WebSocket is not connected")
            future = asyncio.get_running_loop().create_future()
            self._waiters[transaction_id] = future

        try:
            return await future
        finally:
            async with self._lock:
                if self._waiters.get(transaction_id) is future:
                    self._waiters.pop(transaction_id, None)

    async def release(self) -> None:
        """Release one transaction's use of the shared connection."""
        async with self._lock:
            if self._leases:
                self._leases -= 1
            if self._leases:
                return
            socket = self._socket
            reader_task = self._reader_task
            self._socket = None
            self._reader_task = None
            self._early_results.clear()

        if reader_task is not None and not reader_task.done():
            reader_task.cancel()
            await asyncio.gather(reader_task, return_exceptions=True)
        if socket is not None:
            await socket.close()

    async def _read_events(self, socket: AsyncWebSocket) -> None:
        failure: BaseException | None = None
        try:
            while True:
                raw_message = await socket.receive_text()
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError as exc:
                    raise WebSocketProtocolError("WebSocket message was not valid JSON") from exc
                if not isinstance(message, dict):
                    raise WebSocketProtocolError("WebSocket message was not a JSON object")
                if message.get("type") == "transactions":
                    continue
                if message.get("type") != "event":
                    continue
                event = message.get("event")
                if not isinstance(event, dict) or event.get("type") != "DevApiResult":
                    continue
                payload = event.get("payload")
                if not isinstance(payload, dict):
                    raise WebSocketProtocolError("DevApiResult payload was not an object")
                transaction_id = payload.get("transaction_id")
                if not isinstance(transaction_id, str) or not transaction_id:
                    raise WebSocketProtocolError("DevApiResult had no transaction_id")
                await self._dispatch(transaction_id, payload)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            failure = exc
        finally:
            if failure is not None:
                await self._fail_waiters(failure)
                if socket is self._socket:
                    self._socket = None
                    self._reader_task = None
                await socket.close()

    async def _dispatch(self, transaction_id: str, payload: dict[str, Any]) -> None:
        async with self._lock:
            future = self._waiters.get(transaction_id)
            if future is not None and not future.done():
                future.set_result(payload)
                return
            self._early_results[transaction_id] = payload
            self._early_results.move_to_end(transaction_id)
            while len(self._early_results) > self._cache_limit:
                self._early_results.popitem(last=False)

    async def _fail_waiters(self, error: BaseException) -> None:
        async with self._lock:
            for future in self._waiters.values():
                if not future.done():
                    future.set_exception(error)


_hubs: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[str, WebSocketHub]
] = weakref.WeakKeyDictionary()


def websocket_hub(url: str) -> WebSocketHub:
    """Return one WebSocket dispatcher per running event loop and URL."""
    loop = asyncio.get_running_loop()
    loop_hubs = _hubs.setdefault(loop, {})
    hub = loop_hubs.get(url)
    if hub is None:
        hub = WebSocketHub(url)
        loop_hubs[url] = hub
    return hub


async def _read_handshake_headers(reader: asyncio.StreamReader) -> dict[str, str]:
    headers: dict[str, str] = {}
    total = 0
    while True:
        line = await reader.readline()
        total += len(line)
        if total > _MAX_HEADER_BYTES or not line.endswith(b"\r\n"):
            raise WebSocketProtocolError("Malformed or oversized WebSocket handshake")
        if line == b"\r\n":
            return headers
        name, separator, value = line[:-2].partition(b":")
        if not separator:
            raise WebSocketProtocolError("Malformed WebSocket handshake header")
        try:
            key = name.decode("ascii").strip().lower()
        except UnicodeDecodeError as exc:
            raise WebSocketProtocolError("Invalid WebSocket handshake header name") from exc
        headers[key] = value.decode("latin-1").strip()
