from __future__ import annotations

import asyncio
import json
import math
import re
import ssl
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import quote, urlencode, urlsplit, urlunsplit


_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_STATUS_LINE = re.compile(rb"^HTTP/1\.[01] ([0-9]{3})(?: (.*))?\r\n$")
_MAX_STATUS_LINE = 8 * 1024
_MAX_HEADER_BYTES = 64 * 1024
_RESERVED_HEADERS = {"connection", "content-length", "host", "transfer-encoding"}


class HTTPClientError(Exception):
    """Base error for the standard-library HTTP client."""


class HTTPConnectionError(HTTPClientError):
    """The target server could not be reached or the connection failed."""


class HTTPProtocolError(HTTPClientError):
    """The target server returned a malformed or incomplete HTTP response."""


class HTTPRequestTimeout(HTTPClientError):
    """The entire external HTTP request exceeded its timeout."""


def _reject_json_constant(value: str) -> None:
    raise ValueError("Invalid JSON constant: {}".format(value))


@dataclass(frozen=True)
class AsyncHTTPResponse:
    """An HTTP response with its wire body preserved as bytes."""

    status_code: int
    reason: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    request_url: str = ""
    request_method: str = ""
    request_headers: tuple[tuple[str, str], ...] = ()
    request_body: bytes = b""
    elapsed_ms: float = 0

    def get_all(self, name: str) -> tuple[str, ...]:
        """Return all values for a header, matching its name case-insensitively."""
        key = name.lower()
        return tuple(value for header, value in self.headers if header.lower() == key)

    def get_header(self, name: str, default: str | None = None) -> str | None:
        """Return the last matching header value, if present."""
        values = self.get_all(name)
        return values[-1] if values else default

    @property
    def text(self) -> str:
        """Decode the body with the response charset, falling back to UTF-8."""
        content_type = self.get_header("content-type", "") or ""
        charset = "utf-8"
        for item in content_type.split(";")[1:]:
            key, separator, value = item.strip().partition("=")
            if separator and key.lower() == "charset":
                charset = value.strip().strip("\"'") or "utf-8"
                break
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """Parse the response body as JSON."""
        return json.loads(self.text, parse_constant=_reject_json_constant)


async def request_external(
    url: str,
    payload: Any = None,
    method: str = "GET",
    *,
    headers: Mapping[str, Any] | None = None,
    timeout: float = 15,
) -> AsyncHTTPResponse:
    """Send one non-blocking HTTP/1.1 request using asyncio streams.

    GET payloads are URL-encoded query parameters. For other supported methods,
    a non-None payload is JSON-encoded. HTTP error statuses are returned as
    ordinary responses so callers can inspect their status and body.
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url must be a non-empty string")
    if not isinstance(method, str) or method.upper() not in _METHODS:
        raise ValueError(f"Unsupported HTTP method: {method!r}")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout must be a positive number")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive number")

    method_name = method.upper()
    parsed, host, port, host_header, target = _parse_url(url)
    outgoing_headers = _normalize_request_headers(headers)

    body: bytes | None = None
    if method_name == "GET":
        if payload is not None:
            if not isinstance(payload, (Mapping, list, tuple)):
                raise ValueError("GET payload must be a mapping or sequence of pairs")
            query = urlencode(payload, doseq=True)
            if query:
                target += ("&" if parsed.query else "?") + query
    elif payload is not None:
        try:
            body = json.dumps(
                payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must be JSON serializable") from exc
        if not any(name.lower() == "content-type" for name, _ in outgoing_headers):
            outgoing_headers.append(("Content-Type", "application/json"))

    wire_headers = [
        ("Host", host_header),
        ("Connection", "close"),
        ("Accept-Encoding", "identity"),
        *outgoing_headers,
        ("Content-Length", str(len(body) if body is not None else 0)),
    ]
    target_path, separator, target_query = target.partition("?")
    request_url = urlunsplit(
        (parsed.scheme, parsed.netloc, target_path, target_query if separator else "", "")
    )
    request_head = f"{method_name} {target} HTTP/1.1\r\n".encode("ascii")
    for name, value in wire_headers:
        try:
            request_head += f"{name}: {value}\r\n".encode("latin-1")
        except UnicodeEncodeError as exc:
            raise ValueError(f"header {name!r} must contain Latin-1 characters") from exc
    request_head += b"\r\n"

    context = ssl.create_default_context() if parsed.scheme == "https" else None
    started = asyncio.get_running_loop().time()
    try:
        response = await asyncio.wait_for(
            _exchange(host, port, context, request_head, body, method_name),
            timeout=float(timeout),
        )
        return AsyncHTTPResponse(
            status_code=response.status_code,
            reason=response.reason,
            headers=response.headers,
            body=response.body,
            request_url=request_url,
            request_method=method_name,
            request_headers=tuple(wire_headers),
            request_body=body or b"",
            elapsed_ms=(asyncio.get_running_loop().time() - started) * 1000,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPRequestTimeout("External HTTP request exceeded its timeout") from exc
    except (OSError, ssl.SSLError) as exc:
        raise HTTPConnectionError("External HTTP connection failed") from exc


def _parse_url(url: str) -> tuple[Any, str, int, str, str]:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("url has an invalid host or port") from exc

    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("url must be an absolute http:// or https:// URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credentials in the URL are not supported; use headers")

    host = parsed.hostname
    try:
        if ":" not in host:
            host = host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("url contains an invalid host name") from exc

    if port == 0:
        raise ValueError("url port must be between 1 and 65535")
    port = port or (443 if scheme == "https" else 80)
    default_port = 443 if scheme == "https" else 80
    display_host = f"[{host}]" if ":" in host else host
    host_header = display_host if port == default_port else f"{display_host}:{port}"

    path = quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~")
    query = quote(parsed.query, safe="=&?/:;+,%@!$'()*-._~")
    target = urlunsplit(("", "", path, query, ""))
    return parsed, host, port, host_header, target


def _normalize_request_headers(headers: Mapping[str, Any] | None) -> list[tuple[str, str]]:
    if headers is None:
        return []
    if not isinstance(headers, Mapping):
        raise ValueError("headers must be a mapping")

    result: list[tuple[str, str]] = []
    for raw_name, raw_value in headers.items():
        if not isinstance(raw_name, str) or not _HEADER_NAME.fullmatch(raw_name):
            raise ValueError("header names must be valid HTTP tokens")
        name = raw_name
        value = str(raw_value)
        if "\r" in value or "\n" in value:
            raise ValueError(f"header {name!r} contains a line break")
        if name.lower() in _RESERVED_HEADERS:
            raise ValueError(f"header {name!r} is managed by the HTTP client")
        result.append((name, value))
    return result


async def _exchange(
    host: str,
    port: int,
    context: ssl.SSLContext | None,
    request_head: bytes,
    body: bytes | None,
    method: str,
) -> AsyncHTTPResponse:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    reader, writer = await asyncio.open_connection(
        host,
        port,
        ssl=context,
        server_hostname=host if context else None,
    )
    try:
        writer.write(request_head)
        if body:
            writer.write(body)
        await writer.drain()
        return await _read_response(reader, method)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, ssl.SSLError):
            pass


async def _read_response(reader: asyncio.StreamReader, method: str) -> AsyncHTTPResponse:
    interim_count = 0
    while True:
        status_line = await reader.readline()
        if len(status_line) > _MAX_STATUS_LINE or not status_line.endswith(b"\r\n"):
            raise HTTPProtocolError("Malformed or oversized HTTP status line")
        match = _STATUS_LINE.fullmatch(status_line)
        if match is None:
            raise HTTPProtocolError("Malformed HTTP status line")
        status_code = int(match.group(1))
        reason = (match.group(2) or b"").decode("latin-1")
        headers = await _read_headers(reader)
        if status_code < 100 or status_code >= 200 or status_code == 101:
            break
        interim_count += 1
        if interim_count > 5:
            raise HTTPProtocolError("Too many interim HTTP responses")

    if method == "HEAD" or status_code in {204, 205, 304} or status_code == 101:
        body = b""
    else:
        transfer_encodings = ",".join(
            value for name, value in headers if name.lower() == "transfer-encoding"
        ).lower()
        if transfer_encodings:
            encodings = [part.strip() for part in transfer_encodings.split(",")]
            if encodings[-1] != "chunked" or any(item not in {"chunked", "identity"} for item in encodings):
                raise HTTPProtocolError("Unsupported Transfer-Encoding")
            body = await _read_chunked_body(reader)
        else:
            content_lengths = [
                part.strip()
                for name, value in headers
                if name.lower() == "content-length"
                for part in value.split(",")
            ]
            if content_lengths:
                if any(not item.isdigit() for item in content_lengths):
                    raise HTTPProtocolError("Invalid Content-Length")
                if len(set(content_lengths)) != 1:
                    raise HTTPProtocolError("Conflicting Content-Length headers")
                try:
                    body = await reader.readexactly(int(content_lengths[0]))
                except asyncio.IncompleteReadError as exc:
                    raise HTTPProtocolError("Response ended before Content-Length bytes arrived") from exc
            else:
                body = await reader.read()

    return AsyncHTTPResponse(status_code, reason, tuple(headers), body)


async def _read_headers(reader: asyncio.StreamReader) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    total = 0
    while True:
        line = await reader.readline()
        total += len(line)
        if total > _MAX_HEADER_BYTES or not line.endswith(b"\r\n"):
            raise HTTPProtocolError("Malformed or oversized HTTP headers")
        if line == b"\r\n":
            return result
        name, separator, value = line[:-2].partition(b":")
        try:
            decoded_name = name.decode("ascii")
        except UnicodeDecodeError as exc:
            raise HTTPProtocolError("Malformed HTTP header name") from exc
        if not separator or not _HEADER_NAME.fullmatch(decoded_name):
            raise HTTPProtocolError("Malformed HTTP header")
        result.append((decoded_name.lower(), value.decode("latin-1").strip(" \t")))


async def _read_chunked_body(reader: asyncio.StreamReader) -> bytes:
    chunks: list[bytes] = []
    while True:
        line = await reader.readline()
        if not line.endswith(b"\r\n"):
            raise HTTPProtocolError("Malformed chunk size")
        size_text = line[:-2].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError as exc:
            raise HTTPProtocolError("Invalid chunk size") from exc
        if size < 0:
            raise HTTPProtocolError("Invalid chunk size")
        if size == 0:
            await _read_trailers(reader)
            return b"".join(chunks)
        try:
            chunk = await reader.readexactly(size)
            ending = await reader.readexactly(2)
        except asyncio.IncompleteReadError as exc:
            raise HTTPProtocolError("Incomplete chunked response") from exc
        if ending != b"\r\n":
            raise HTTPProtocolError("Malformed chunk terminator")
        chunks.append(chunk)


async def _read_trailers(reader: asyncio.StreamReader) -> None:
    total = 0
    while True:
        line = await reader.readline()
        total += len(line)
        if total > _MAX_HEADER_BYTES or not line.endswith(b"\r\n"):
            raise HTTPProtocolError("Malformed or oversized chunk trailers")
        if line == b"\r\n":
            return
        if b":" not in line:
            raise HTTPProtocolError("Malformed chunk trailer")
