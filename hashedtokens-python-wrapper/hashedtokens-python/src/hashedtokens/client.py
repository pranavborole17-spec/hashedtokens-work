from __future__ import annotations

import asyncio
import json
import math
import weakref
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from .async_http import (
    AsyncHTTPResponse,
    HTTPClientError,
    HTTPRequestTimeout,
    request_external,
)
from .websocket_client import (
    WebSocketHub,
    WebSocketClosed,
    WebSocketError,
    WebSocketProtocolError,
    websocket_hub,
)


_STATUS_NAMES = {
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
}
_endpoint_locks: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[tuple[str, str], asyncio.Lock]
] = weakref.WeakKeyDictionary()
_SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "x-auth-token",
    "x-access-token",
}
_COLLECTIONS_ROUTE = "/hashedtokens/edms/api/collections/"
_WEBSOCKET_ROUTE = "/hashedtokens/edms/api/ws"
_ENDPOINT_LOOKUP_ROUTE = "/endpoints/lookup"


class WrapperError(Exception):
    """Base error for a complete wrapper transaction."""


class TransactionTimeout(WrapperError):
    """The overall wrapper transaction exceeded its deadline."""

    def __init__(self, message: str, response_obj: AsyncHTTPResponse | None = None):
        super().__init__(message)
        self.response_obj = response_obj


class EDMSIntegrationError(WrapperError):
    """EDMS could not accept or finish storing a captured request."""

    def __init__(
        self,
        message: str,
        response_obj: AsyncHTTPResponse,
        edms_response: AsyncHTTPResponse | None = None,
    ):
        super().__init__(message)
        self.response_obj = response_obj
        self.edms_response = edms_response


class EDMSProtocolError(EDMSIntegrationError):
    """EDMS returned a response that does not match the supplied contract."""


@dataclass(frozen=True)
class RequestResult:
    """The target API response and EDMS final result for one transaction."""

    response_obj: AsyncHTTPResponse
    ht_response: dict[str, Any]


async def request(
    url: str,
    payload: Any = None,
    method: str = "GET",
    *,
    timeout: float = 15,
    headers: Mapping[str, Any] | None = None,
    edms_url: str = "http://localhost:3000",
    ws_url: str | None = None,
    collection: str = "demo",
    tags: list[str] | None = None,
    collection_tags: list[str] | None = None,
    annotation: str | None = None,
    collection_annotation: str | None = None,
    notes: str | None = None,
    endpoint_id: str | None = None,
    redact_headers: Iterable[str] = (),
    redacted_value: str = "[REDACTED]",
) -> RequestResult:
    """Call an external API once, store its bodies in EDMS, then await the result.

    EDMS uses ``ops: "insert"`` so the target API is not called a second time.
    The insert contract stores request/response bodies but not the target's
    status code or timing; those remain available on ``response_obj``.
    """
    _validate_timeout(timeout)
    if not isinstance(collection, str) or not collection.strip():
        raise ValueError("collection must be a non-empty string")
    if not isinstance(redacted_value, str):
        raise ValueError("redacted_value must be a string")
    extra_redactions = _normalize_redaction_names(redact_headers)
    optional_metadata = {
        "tags": tags,
        "collection_tags": collection_tags,
        "annotation": annotation,
        "collection_annotation": collection_annotation,
        "notes": notes,
        "endpoint_id": endpoint_id,
    }
    try:
        json.dumps(optional_metadata, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("EDMS options must be JSON serializable") from exc
    post_url, resolved_ws_url, jobs_base, lookup_base = _edms_urls(edms_url, ws_url)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + float(timeout)
    try:
        response_obj = await _await_deadline(
            request_external(
                url,
                payload,
                method,
                headers=headers,
                timeout=_remaining(deadline),
            ),
            deadline,
            "External API request exceeded the transaction deadline",
        )
    except HTTPRequestTimeout as exc:
        raise TransactionTimeout("External API request exceeded the transaction deadline") from exc

    request_body = {} if response_obj.request_method == "GET" or payload is None else payload
    edms_payload: dict[str, Any] = {
        "ops": "insert",
        "endpoint_str": response_obj.request_url,
        "method": response_obj.request_method,
        "collection": collection,
        "request_body": request_body,
        "response_body": _response_body_value(response_obj),
        "request_headers": _headers_for_storage(
            response_obj.request_headers, extra_redactions, redacted_value
        ),
        "response_headers": _headers_for_storage(
            response_obj.headers, extra_redactions, redacted_value
        ),
    }
    _add_optional(edms_payload, "tags", tags)
    _add_optional(edms_payload, "collection_tags", collection_tags)
    _add_optional(edms_payload, "annotation", annotation)
    _add_optional(edms_payload, "collection_annotation", collection_annotation)
    _add_optional(edms_payload, "notes", notes)
    _add_optional(edms_payload, "endpoint_id", endpoint_id)

    endpoint_lock = _endpoint_lock(response_obj.request_url, response_obj.request_method)
    await _await_deadline(
        endpoint_lock.acquire(),
        deadline,
        "Transaction deadline expired while waiting to reuse the EDMS endpoint",
        response_obj,
    )
    hub: WebSocketHub | None = None
    websocket_acquired = False
    websocket_unavailable = False
    try:
        if endpoint_id is None:
            existing_endpoint_id = await _lookup_endpoint(
                lookup_base,
                response_obj.request_url,
                response_obj.request_method,
                deadline,
                response_obj,
            )
            if existing_endpoint_id is not None:
                edms_payload["endpoint_id"] = existing_endpoint_id

        try:
            hub = websocket_hub(resolved_ws_url)
            await _await_deadline(
                hub.acquire(),
                deadline,
                "Transaction deadline expired while connecting to EDMS",
                response_obj,
            )
            websocket_acquired = True
        except (OSError, WebSocketError):
            websocket_unavailable = True

        try:
            submission = await _await_deadline(
                request_external(
                    post_url,
                    edms_payload,
                    "POST",
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    timeout=_remaining(deadline),
                ),
                deadline,
                "Transaction deadline expired while submitting to EDMS",
                response_obj,
            )
        except HTTPRequestTimeout as exc:
            raise TransactionTimeout(
                "Transaction deadline expired while submitting to EDMS", response_obj
            ) from exc
        except (HTTPClientError, asyncio.TimeoutError) as exc:
            raise EDMSIntegrationError(
                "Could not submit the captured request to EDMS", response_obj
            ) from exc

        immediate_result = _read_submission_response(submission, response_obj)
        if immediate_result is not None:
            return RequestResult(response_obj, immediate_result)

        transaction_id = _transaction_id(submission, response_obj)
        if websocket_acquired and hub is not None:
            try:
                event_payload = await _await_deadline(
                    hub.wait_for_result(transaction_id),
                    deadline,
                    "Timed out waiting for the EDMS WebSocket result",
                    response_obj,
                )
                if event_payload.get("op") != "insert":
                    raise EDMSProtocolError(
                        "EDMS returned a result for an unexpected operation",
                        response_obj,
                        submission,
                    )
                final_result = _validate_final_result(
                    event_payload.get("result"), response_obj, submission
                )
                return RequestResult(response_obj, final_result)
            except (WebSocketClosed, OSError):
                websocket_unavailable = True
            except WebSocketProtocolError as exc:
                raise EDMSProtocolError(
                    "EDMS WebSocket returned a malformed message", response_obj, submission
                ) from exc
            finally:
                await hub.release()
                websocket_acquired = False

        if websocket_unavailable:
            final_result = await _poll_transaction(
                jobs_base, transaction_id, deadline, response_obj
            )
            return RequestResult(response_obj, final_result)

        raise EDMSIntegrationError(
            "EDMS final result was not received", response_obj, submission
        )
    except TransactionTimeout as exc:
        if exc.response_obj is None:
            exc.response_obj = response_obj
        raise
    except EDMSIntegrationError:
        raise
    finally:
        if websocket_acquired and hub is not None:
            await hub.release()
        endpoint_lock.release()


def _validate_timeout(timeout: float) -> None:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout must be a positive number")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive number")


def _normalize_redaction_names(names: Iterable[str]) -> set[str]:
    if isinstance(names, str):
        raise ValueError("redact_headers must be an iterable of header names, not a string")
    try:
        result = {name.lower().strip() for name in names}
    except (TypeError, AttributeError) as exc:
        raise ValueError("redact_headers must be an iterable of header names") from exc
    if any(not name for name in result):
        raise ValueError("redact_headers cannot contain empty names")
    return result


def _edms_urls(edms_url: str, ws_url: str | None) -> tuple[str, str, str, str]:
    if not isinstance(edms_url, str) or not edms_url:
        raise ValueError("edms_url must be a non-empty HTTP base URL")
    try:
        parsed = urlsplit(edms_url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("edms_url has an invalid host or port") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("edms_url must be an absolute http:// or https:// URL")
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValueError("edms_url must not contain credentials, query, or fragment")
    if port == 0:
        raise ValueError("edms_url port must be between 1 and 65535")
    base_path = parsed.path.rstrip("/")
    submission_path = base_path + _COLLECTIONS_ROUTE
    lookup_path = base_path + _ENDPOINT_LOOKUP_ROUTE
    jobs_base = urlunsplit((parsed.scheme, parsed.netloc, base_path + "/jobs/", "", ""))
    post_url = urlunsplit((parsed.scheme, parsed.netloc, submission_path, "", ""))
    lookup_base = urlunsplit((parsed.scheme, parsed.netloc, lookup_path, "", ""))

    if ws_url is None:
        host = parsed.hostname
        display_host = "[{}]".format(host) if ":" in host else host
        ws_port = port or (443 if parsed.scheme == "https" else 80)
        host_part = display_host if ws_port == 80 else "{}:{}".format(display_host, ws_port)
        resolved_ws_url = "ws://{}{}{}".format(host_part, base_path, _WEBSOCKET_ROUTE)
    else:
        resolved_ws_url = ws_url
        ws_parsed = urlsplit(ws_url)
        if ws_parsed.scheme != "ws" or not ws_parsed.hostname:
            raise ValueError("ws_url must be an absolute ws:// URL")
        if ws_parsed.username is not None or ws_parsed.password is not None:
            raise ValueError("ws_url must not contain credentials")
        if ws_parsed.port == 0:
            raise ValueError("ws_url port must be between 1 and 65535")
    return post_url, resolved_ws_url, jobs_base, lookup_base


def _endpoint_lock(endpoint_str: str, method: str) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    loop_locks = _endpoint_locks.setdefault(loop, {})
    key = (endpoint_str, method)
    lock = loop_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        loop_locks[key] = lock
    return lock


async def _lookup_endpoint(
    lookup_base: str,
    endpoint_str: str,
    method: str,
    deadline: float,
    response_obj: AsyncHTTPResponse,
) -> str | None:
    url = "{}?{}".format(
        lookup_base,
        urlencode({"endpoint_str": endpoint_str, "method": method}),
    )
    try:
        response = await _await_deadline(
            request_external(
                url,
                method="GET",
                timeout=_remaining(deadline),
                headers={"Accept": "application/json"},
            ),
            deadline,
            "Transaction deadline expired while looking up the EDMS endpoint",
            response_obj,
        )
    except HTTPRequestTimeout as exc:
        raise TransactionTimeout(
            "Transaction deadline expired while looking up the EDMS endpoint",
            response_obj,
        ) from exc
    except HTTPClientError as exc:
        raise EDMSIntegrationError("Could not look up the EDMS endpoint", response_obj) from exc

    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise EDMSIntegrationError(
            "EDMS endpoint lookup returned HTTP {}".format(response.status_code),
            response_obj,
            response,
        )
    body = _json_object(response, response_obj, "EDMS endpoint lookup response")
    endpoint = body.get("endpoint")
    if body.get("ok") is not True or not isinstance(endpoint, dict):
        raise EDMSProtocolError(
            "EDMS endpoint lookup response has no endpoint", response_obj, response
        )
    found_id = endpoint.get("endpoint_id")
    if not isinstance(found_id, str) or not found_id:
        raise EDMSProtocolError(
            "EDMS endpoint lookup returned an invalid endpoint_id", response_obj, response
        )
    return found_id


def _response_body_value(response: AsyncHTTPResponse) -> Any:
    try:
        return response.json()
    except (ValueError, UnicodeError):
        return response.text


def _headers_for_storage(
    headers: Iterable[tuple[str, str]],
    additional_redactions: set[str],
    redacted_value: str,
) -> dict[str, Any]:
    redacted = _SENSITIVE_HEADERS | additional_redactions
    values: dict[str, list[str]] = {}
    for name, value in headers:
        stored_value = redacted_value if name.lower() in redacted else value
        values.setdefault(name, []).append(stored_value)
    return {
        name: items[0] if len(items) == 1 else items
        for name, items in values.items()
    }


def _add_optional(payload: dict[str, Any], name: str, value: Any) -> None:
    if value is not None:
        payload[name] = value


def _remaining(deadline: float) -> float:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise asyncio.TimeoutError
    return remaining


async def _await_deadline(
    awaitable: Any,
    deadline: float,
    message: str,
    response_obj: AsyncHTTPResponse | None = None,
) -> Any:
    try:
        return await asyncio.wait_for(awaitable, timeout=_remaining(deadline))
    except asyncio.TimeoutError as exc:
        raise TransactionTimeout(message, response_obj) from exc


def _json_object(response: AsyncHTTPResponse, response_obj: AsyncHTTPResponse, context: str) -> dict[str, Any]:
    try:
        value = response.json()
    except (ValueError, UnicodeError) as exc:
        raise EDMSProtocolError(
            "{} was not valid JSON".format(context), response_obj, response
        ) from exc
    if not isinstance(value, dict):
        raise EDMSProtocolError("{} was not a JSON object".format(context), response_obj, response)
    return value


def _read_submission_response(
    response: AsyncHTTPResponse, response_obj: AsyncHTTPResponse
) -> dict[str, Any] | None:
    if response.status_code == 202:
        return None
    body = _json_object(response, response_obj, "EDMS submission response")
    if 400 <= response.status_code <= 599 and body.get("ok") is False:
        code = _validate_status(body.get("status"), response_obj, response)
        expected_http_status = {
            100: 400,
            101: 400,
            102: 400,
            103: 400,
            107: 503,
            109: 500,
        }.get(code)
        if expected_http_status != response.status_code:
            raise EDMSProtocolError(
                "EDMS immediate status code does not match its HTTP status",
                response_obj,
                response,
            )
        return body
    raise EDMSProtocolError(
        "EDMS returned an unexpected HTTP status {}".format(response.status_code),
        response_obj,
        response,
    )


def _transaction_id(
    response: AsyncHTTPResponse, response_obj: AsyncHTTPResponse
) -> str:
    body = _json_object(response, response_obj, "EDMS 202 response")
    transaction_id = body.get("transaction_id")
    if body.get("ok") is not True or not isinstance(transaction_id, str) or not transaction_id:
        raise EDMSProtocolError(
            "EDMS 202 response is missing a valid transaction_id", response_obj, response
        )
    return transaction_id


def _validate_final_result(
    result: Any,
    response_obj: AsyncHTTPResponse,
    edms_response: AsyncHTTPResponse,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise EDMSProtocolError("EDMS final result was not an object", response_obj, edms_response)
    status = result.get("status")
    _validate_status(status, response_obj, edms_response)
    return result


def _validate_status(
    status: Any,
    response_obj: AsyncHTTPResponse,
    edms_response: AsyncHTTPResponse,
) -> int:
    if not isinstance(status, dict):
        raise EDMSProtocolError("EDMS result has no status object", response_obj, edms_response)
    code = status.get("code")
    if isinstance(code, bool) or not isinstance(code, int) or code not in _STATUS_NAMES:
        raise EDMSProtocolError("EDMS result has an unknown status code", response_obj, edms_response)
    if status.get("name") != _STATUS_NAMES[code] or not isinstance(status.get("message"), str):
        raise EDMSProtocolError("EDMS result status is malformed", response_obj, edms_response)
    return code


async def _poll_transaction(
    jobs_base: str,
    transaction_id: str,
    deadline: float,
    response_obj: AsyncHTTPResponse,
) -> dict[str, Any]:
    url = jobs_base + quote(transaction_id, safe="")
    while True:
        try:
            response = await _await_deadline(
                request_external(
                    url,
                    method="GET",
                    timeout=_remaining(deadline),
                    headers={"Accept": "application/json"},
                ),
                deadline,
                "Timed out polling EDMS for the final result",
                response_obj,
            )
        except HTTPRequestTimeout as exc:
            raise TransactionTimeout(
                "Transaction deadline expired while polling EDMS", response_obj
            ) from exc
        except (HTTPClientError, asyncio.TimeoutError) as exc:
            raise EDMSIntegrationError("Could not poll EDMS for the final result", response_obj) from exc
        if response.status_code != 200:
            raise EDMSIntegrationError(
                "EDMS polling returned HTTP {}".format(response.status_code),
                response_obj,
                response,
            )
        job = _json_object(response, response_obj, "EDMS polling response")
        state = job.get("status")
        if state in {"done", "failed", "stalled"}:
            return _validate_final_result(job.get("result"), response_obj, response)
        if state != "running":
            raise EDMSProtocolError("EDMS polling returned an unknown job status", response_obj, response)
        await _await_deadline(
            asyncio.sleep(min(0.5, _remaining(deadline))),
            deadline,
            "Timed out polling EDMS for the final result",
            response_obj,
        )
