from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import ssl
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from .exceptions import TimeoutExceeded, WebSocketError

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_MAX_MESSAGE_SIZE = 16 * 1024 * 1024


class WebSocketClient:
    def __init__(self, url: str, timeout: float = 15.0):
        self.url = url
        self.timeout = timeout
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.closed = True

    async def connect(self) -> None:
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"ws", "wss"} or parsed.hostname is None:
            raise WebSocketError("Invalid WebSocket URL")
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        ssl_context = ssl.create_default_context() if parsed.scheme == "wss" else None
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_context),
                timeout=self.timeout,
            )
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n\r\n"
            )
            self.writer.write(request.encode("latin-1"))
            await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)
            status_line = await self._read_line()
            status_parts = status_line.split(" ", 2)
            if len(status_parts) < 2 or status_parts[1] != "101":
                raise WebSocketError("WebSocket upgrade was rejected")
            response_headers: Dict[str, str] = {}
            while True:
                line = await self._read_line()
                if not line:
                    break
                name, separator, value = line.partition(":")
                if separator:
                    response_headers[name.strip().lower()] = value.strip()
            expected_accept = base64.b64encode(
                hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()
            ).decode("ascii")
            if response_headers.get("sec-websocket-accept") != expected_accept:
                raise WebSocketError("WebSocket upgrade returned an invalid accept key")
            if response_headers.get("upgrade", "").lower() != "websocket":
                raise WebSocketError("WebSocket upgrade response is missing Upgrade: websocket")
            self.closed = False
        except asyncio.TimeoutError as exc:
            await self._close_transport()
            raise TimeoutExceeded("Timed out connecting to EDMS WebSocket") from exc
        except (OSError, UnicodeError) as exc:
            await self._close_transport()
            raise WebSocketError("Could not establish EDMS WebSocket connection") from exc
        except WebSocketError:
            await self._close_transport()
            raise

    async def close(self) -> None:
        if not self.closed:
            try:
                await self._send_frame(0x8, (1000).to_bytes(2, "big"))
            except (OSError, asyncio.TimeoutError, WebSocketError):
                pass
        await self._close_transport()

    async def _close_transport(self) -> None:
        self.closed = True
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except OSError:
                pass

    async def send_json(self, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await self._send_frame(0x1, data)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.writer is None or self.closed:
            raise WebSocketError("WebSocket is not connected")
        mask_key = os.urandom(4)
        masked = bytes(byte ^ mask_key[i % 4] for i, byte in enumerate(payload))
        frame = bytearray([0x80 | opcode])
        length = len(payload)
        if length < 126:
            frame.append(0x80 | length)
        elif length < 65536:
            frame.append(0x80 | 126)
            frame.extend(length.to_bytes(2, "big"))
        else:
            frame.append(0x80 | 127)
            frame.extend(length.to_bytes(8, "big"))
        frame.extend(mask_key)
        frame.extend(masked)
        self.writer.write(frame)
        await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)

    async def receive(self) -> Optional[Dict[str, Any]]:
        while True:
            opcode, payload = await self._read_message()
            if opcode == 0x9:
                await self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x8:
                self.closed = True
                return None
            if opcode == 0x1:
                try:
                    decoded = json.loads(payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise WebSocketError("Received invalid JSON over EDMS WebSocket") from exc
                if isinstance(decoded, dict):
                    return decoded
                raise WebSocketError("EDMS WebSocket message must be a JSON object")
            raise WebSocketError("Received an unsupported EDMS WebSocket message")

    async def _read_message(self) -> tuple[int, bytes]:
        fragments = bytearray()
        message_opcode: Optional[int] = None
        while True:
            first, second = await self._read_exact(2)
            final = bool(first & 0x80)
            opcode = first & 0x0F
            masked = bool(second & 0x80)
            length = second & 0x7F
            if length == 126:
                length = int.from_bytes(await self._read_exact(2), "big")
            elif length == 127:
                length = int.from_bytes(await self._read_exact(8), "big")
            if length > _MAX_MESSAGE_SIZE or len(fragments) + length > _MAX_MESSAGE_SIZE:
                raise WebSocketError("EDMS WebSocket message exceeds the size limit")
            mask = await self._read_exact(4) if masked else b""
            payload = await self._read_exact(length)
            if masked:
                payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))

            if opcode >= 0x8:
                if not final or length > 125:
                    raise WebSocketError("Malformed WebSocket control frame")
                if opcode in {0x8, 0x9, 0xA}:
                    if opcode == 0x9:
                        await self._send_frame(0xA, payload)
                    elif opcode == 0x8:
                        return opcode, payload
                    continue
                raise WebSocketError("Unsupported WebSocket control frame")

            if opcode in {0x1, 0x2}:
                if message_opcode is not None:
                    raise WebSocketError("Unexpected new WebSocket message during fragmentation")
                message_opcode = opcode
            elif opcode != 0x0 or message_opcode is None:
                raise WebSocketError("Malformed fragmented WebSocket message")

            fragments.extend(payload)
            if final:
                return message_opcode or opcode, bytes(fragments)

    async def _read_exact(self, length: int) -> bytes:
        if self.reader is None:
            raise WebSocketError("WebSocket is not connected")
        try:
            return await asyncio.wait_for(self.reader.readexactly(length), timeout=self.timeout)
        except asyncio.TimeoutError:
            raise
        except (asyncio.IncompleteReadError, OSError) as exc:
            self.closed = True
            raise WebSocketError("EDMS WebSocket connection closed unexpectedly") from exc

    async def _read_line(self) -> str:
        if self.reader is None:
            raise WebSocketError("WebSocket is not connected")
        return (await asyncio.wait_for(self.reader.readuntil(b"\r\n"), timeout=self.timeout))[:-2].decode("latin-1")


class WebSocketManager:
    def __init__(self, url: str, timeout: float = 15.0):
        self.url = url
        self.timeout = timeout
        self.client: Optional[WebSocketClient] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._unmatched: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._reader_task: Optional[asyncio.Task] = None
        self._failure: Optional[WebSocketError] = None

    async def connect(self, timeout: Optional[float] = None) -> WebSocketClient:
        async with self._connect_lock:
            if self.client is not None and not self.client.closed and self._reader_task is not None and not self._reader_task.done():
                return self.client
            self._failure = None
            self.client = WebSocketClient(self.url, timeout=timeout if timeout is not None else self.timeout)
            await self.client.connect()
            self._reader_task = asyncio.create_task(self._read_loop())
            return self.client

    async def wait_for_transaction(
        self,
        transaction_id: str,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            buffered = self._unmatched.pop(transaction_id, None)
            if buffered is not None:
                return buffered
        if self.client is None or self.client.closed or self._reader_task is None or self._reader_task.done():
            await self.connect()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async with self._lock:
            buffered = self._unmatched.pop(transaction_id, None)
            if buffered is not None:
                return buffered
            if self._failure is not None:
                raise self._failure
            self._pending[transaction_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout if timeout is not None else self.timeout)
        except asyncio.TimeoutError as exc:
            async with self._lock:
                self._pending.pop(transaction_id, None)
            raise TimeoutExceeded(f"Timed out waiting for EDMS transaction {transaction_id}") from exc
        finally:
            async with self._lock:
                self._pending.pop(transaction_id, None)

    async def _read_loop(self) -> None:
        if self.client is None:
            return
        failure: Optional[WebSocketError] = None
        try:
            while not self.client.closed:
                message = await self.client.receive()
                if message is None:
                    failure = WebSocketError("EDMS WebSocket closed before transaction results arrived")
                    break
                transaction_id = self._extract_transaction_id(message)
                if transaction_id is None:
                    continue
                async with self._lock:
                    future = self._pending.pop(transaction_id, None)
                    if future is None:
                        self._unmatched[transaction_id] = message
                        while len(self._unmatched) > 128:
                            self._unmatched.pop(next(iter(self._unmatched)))
                if future is not None and not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, OSError, WebSocketError):
            failure = WebSocketError("EDMS WebSocket result reader failed")
        finally:
            self._failure = failure or self._failure
            if failure is not None:
                async with self._lock:
                    pending = tuple(self._pending.values())
                    self._pending.clear()
                for future in pending:
                    if not future.done():
                        future.set_exception(failure)

    @staticmethod
    def _extract_transaction_id(message: Dict[str, Any]) -> Optional[str]:
        event = message.get("event")
        if not isinstance(event, dict) or event.get("type") != "DevApiResult":
            return None
        payload = event.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("result"), dict):
            return None
        transaction_id = payload.get("transaction_id")
        return str(transaction_id) if transaction_id else None
