from __future__ import annotations

import asyncio
import json
import ssl
import time
from typing import Any, Dict, Mapping, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .edms import sanitize_headers
from .exceptions import HTTPClientError, TimeoutExceeded
from .models import ExternalResponse


class AsyncHTTPClient:
    """Minimal async HTTP client implemented with the Python standard library."""

    async def request(
        self,
        url: str,
        method: str = "GET",
        payload: Any = None,
        headers: Optional[Mapping[str, Any]] = None,
        timeout: float = 15.0,
        validate_cert: bool = True,
        params: Optional[Mapping[str, Any]] = None,
    ) -> ExternalResponse:
        method = (method or "GET").upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError(f"Unsupported HTTP method: {method}")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        deadline = time.monotonic() + timeout
        url = self._apply_query_params(url, payload if method == "GET" else None, params)
        request_headers = dict(headers or {})
        request_body, outgoing_headers = self._prepare_request_body(method, payload, request_headers)
        final_headers = dict(outgoing_headers)

        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")
        if parsed.hostname is None:
            raise ValueError(f"Invalid URL: {url!r}")

        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        ssl_context = ssl.create_default_context() if validate_cert and parsed.scheme == "https" else None
        writer: Optional[asyncio.StreamWriter] = None
        try:
            remaining = self._remaining(deadline)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(parsed.hostname, port, ssl=ssl_context),
                timeout=remaining,
            )
            request_line = self._build_request_line(parsed, method)
            writer.write(self._encode_request(method, parsed, request_line, final_headers, request_body))
            await asyncio.wait_for(writer.drain(), timeout=self._remaining(deadline))

            status_code, reason, response_headers, response_body = await self._read_response(reader, deadline)
            elapsed_ms = int((time.monotonic() - (deadline - timeout)) * 1000)
            response = ExternalResponse(
                url=url,
                method=method,
                status_code=status_code,
                headers=response_headers,
                body=response_body,
                reason=reason,
                elapsed_ms=elapsed_ms,
                request_headers=sanitize_headers(final_headers),
                request_body=payload,
            )
            return response
        except asyncio.TimeoutError as exc:
            raise TimeoutExceeded("Request timed out") from exc
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
            raise HTTPClientError("Malformed or incomplete HTTP response") from exc
        except OSError as exc:
            raise HTTPClientError(f"HTTP request failed: {exc}") from exc
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - time.monotonic()
        return max(0.0, remaining)

    def _apply_query_params(
        self,
        url: str,
        payload: Any,
        params: Optional[Mapping[str, Any]] = None,
    ) -> str:
        parsed = urlsplit(url)
        items = parse_qsl(parsed.query, keep_blank_values=True)
        if params is not None:
            if isinstance(params, Mapping):
                items.extend((str(k), str(v)) for k, v in params.items() if v is not None)
            else:
                items.extend(params)
        if payload is not None and isinstance(payload, Mapping):
            for key, value in payload.items():
                if value is None:
                    continue
                items.append((str(key), str(value)))
        if not items:
            return url
        encoded = urlencode(items, doseq=True)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, encoded, parsed.fragment))

    def _prepare_request_body(
        self,
        method: str,
        payload: Any,
        headers: Dict[str, Any],
    ) -> tuple[bytes, Dict[str, Any]]:
        if method == "GET":
            return b"", headers
        if payload is None:
            return b"", headers
        if isinstance(payload, (dict, list, tuple, int, float, bool)):
            headers.setdefault("Content-Type", "application/json")
            encoded = json.dumps(payload).encode("utf-8")
            return encoded, headers
        if isinstance(payload, bytes):
            return payload, headers
        if isinstance(payload, str):
            headers.setdefault("Content-Type", "text/plain; charset=utf-8")
            return payload.encode("utf-8"), headers
        headers.setdefault("Content-Type", "application/json")
        return json.dumps(payload).encode("utf-8"), headers

    def _build_request_line(self, parsed, method: str) -> str:
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return f"{method} {path} HTTP/1.1\r\n"

    def _encode_request(
        self,
        method: str,
        parsed,
        request_line: str,
        headers: Dict[str, Any],
        body: bytes,
    ) -> bytes:
        header_lines = [request_line]
        default_port = 443 if parsed.scheme == "https" else 80
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        if parsed.port and parsed.port != default_port:
            host = f"{host}:{parsed.port}"
        header_lines.append(f"Host: {host}\r\n")
        header_lines.append("Connection: close\r\n")
        header_lines.append("User-Agent: hashedtokens-python-wrapper/0.1\r\n")
        has_content_length = any(str(key).lower() == "content-length" for key in headers)
        for key, value in headers.items():
            if "\r" in str(key) or "\n" in str(key) or "\r" in str(value) or "\n" in str(value):
                raise ValueError("HTTP header names and values cannot contain newlines")
            header_lines.append(f"{key}: {value}\r\n")
        if body and not has_content_length:
            header_lines.append(f"Content-Length: {len(body)}\r\n")
        elif not has_content_length and method in {"POST", "PUT", "PATCH"}:
            header_lines.append("Content-Length: 0\r\n")
        header_lines.append("\r\n")
        return ("".join(header_lines)).encode("latin-1") + body

    async def _read_response(self, reader: asyncio.StreamReader, deadline: float) -> tuple[int, str, Dict[str, str], bytes]:
        status_line = await self._read_line(reader, deadline)
        if not status_line:
            raise HTTPClientError("Server closed the connection before sending a response")
        parts = status_line.split(" ", 2)
        if len(parts) < 3:
            raise HTTPClientError(f"Malformed HTTP status line: {status_line!r}")
        try:
            status_code = int(parts[1])
        except ValueError as exc:
            raise HTTPClientError(f"Malformed HTTP status line: {status_line!r}") from exc
        reason = parts[2] if len(parts) > 2 else ""

        headers: Dict[str, str] = {}
        while True:
            line = await self._read_line(reader, deadline)
            if line == "":
                break
            name, sep, value = line.partition(":")
            if not sep:
                continue
            headers[name.strip()] = value.strip()

        lowered_headers = {key.lower(): value for key, value in headers.items()}
        if status_code in {204, 304}:
            body = b""
        elif "chunked" in lowered_headers.get("transfer-encoding", "").lower():
            body = await self._read_chunked_body(reader, deadline)
        elif "content-length" in lowered_headers:
            try:
                size = int(lowered_headers["content-length"])
            except ValueError as exc:
                raise HTTPClientError("Malformed Content-Length response header") from exc
            if size < 0:
                raise HTTPClientError("Negative Content-Length response header")
            if size > 0:
                body = await asyncio.wait_for(reader.readexactly(size), timeout=self._remaining(deadline))
            else:
                body = b""
        else:
            body = await self._read_until_close(reader, deadline)

        return status_code, reason, headers, body

    async def _read_line(self, reader: asyncio.StreamReader, deadline: float) -> str:
        line = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=self._remaining(deadline))
        return line[:-2].decode("latin-1")

    async def _read_chunked_body(self, reader: asyncio.StreamReader, deadline: float) -> bytes:
        chunks = bytearray()
        while True:
            line = await self._read_line(reader, deadline)
            if not line:
                break
            try:
                length = int(line.split(";", 1)[0], 16)
            except ValueError as exc:
                raise HTTPClientError(f"Malformed chunk size: {line!r}") from exc
            if length == 0:
                break
            chunks.extend(await asyncio.wait_for(reader.readexactly(length), timeout=self._remaining(deadline)))
            trailer = await self._read_line(reader, deadline)
            if trailer:
                raise HTTPClientError("Malformed chunked response trailer")
        while await self._read_line(reader, deadline):
            continue
        return bytes(chunks)

    async def _read_until_close(self, reader: asyncio.StreamReader, deadline: float) -> bytes:
        try:
            chunks = bytearray()
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=self._remaining(deadline))
                if not chunk:
                    break
                chunks.extend(chunk)
            return bytes(chunks)
        except asyncio.TimeoutError as exc:
            raise TimeoutExceeded("Request timed out while reading the server response") from exc
