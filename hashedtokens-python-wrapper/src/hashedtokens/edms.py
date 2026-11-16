from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

EDMS_STATUS_CODES = {
    0: "OK",
    1: "UNKNOWN",
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

DEFAULT_EDMS_URL = "http://localhost:3000/hashedtokens/edms/api/collections/"
DEFAULT_WS_URL = "ws://localhost:3000/hashedtokens/edms/api/ws"
DEFAULT_COLLECTION = "demo"
MAX_TAGS_PER_LIST = 100
MAX_TAG_LENGTH = 100

SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "token",
    "secret",
    "password",
}


def sanitize_headers(headers: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not headers:
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in headers.items():
        lower = str(key).lower()
        if lower in SENSITIVE_HEADERS:
            cleaned[key] = "[REDACTED]"
        else:
            cleaned[key] = value
    return cleaned


def normalize_tag_list(tags: Optional[Iterable[str]]) -> list[str]:
    if not tags:
        return []
    cleaned: list[str] = []
    for tag in tags:
        if tag is None:
            continue
        text = str(tag).strip()
        if not text:
            continue
        if len(text) > MAX_TAG_LENGTH:
            raise ValueError(f"Tag exceeds {MAX_TAG_LENGTH} characters: {text!r}")
        cleaned.append(text)
    if len(cleaned) > MAX_TAGS_PER_LIST:
        raise ValueError(f"No more than {MAX_TAGS_PER_LIST} tags are allowed")
    return cleaned


def build_collection_payload(
    *,
    url: str,
    method: str,
    collection: str,
    request_payload: Any,
    response_payload: Any,
    request_headers: Optional[Mapping[str, Any]] = None,
    response_headers: Optional[Mapping[str, Any]] = None,
    tags: Optional[Iterable[str]] = None,
    collection_tags: Optional[Iterable[str]] = None,
    annotation: Optional[str] = None,
    collection_annotation: Optional[str] = None,
    notes: Optional[str] = None,
    timeout_ms: Optional[int] = None,
    endpoint_id: Optional[str] = None,
    return_response: Optional[bool] = None,
    ops: str = "insert",
) -> Dict[str, Any]:
    normalized_method = (method or "GET").upper()
    payload: Dict[str, Any] = {
        "ops": ops,
        "endpoint_str": url,
        "method": normalized_method,
        "collection": collection or DEFAULT_COLLECTION,
        "request_body": request_payload if request_payload is not None else {},
        "response_body": response_payload if response_payload is not None else {},
    }
    if request_headers is not None:
        payload["request_headers"] = sanitize_headers(request_headers)
    if response_headers is not None:
        payload["response_headers"] = sanitize_headers(response_headers)
    if tags is not None:
        payload["tags"] = normalize_tag_list(tags)
    if collection_tags is not None:
        payload["collection_tags"] = normalize_tag_list(collection_tags)
    if annotation is not None:
        payload["annotation"] = annotation
    if collection_annotation is not None:
        payload["collection_annotation"] = collection_annotation
    if notes is not None:
        payload["notes"] = notes
    if timeout_ms is not None:
        payload["timeout_ms"] = int(timeout_ms)
    if endpoint_id is not None:
        payload["endpoint_id"] = endpoint_id
    if return_response is not None:
        payload["return_response"] = return_response
    if ops != "insert":
        raise ValueError("This wrapper only supports ops='insert' to avoid replaying the external request")
    return payload
