from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlsplit

from .async_http import AsyncHTTPClient
from .edms import DEFAULT_COLLECTION, DEFAULT_EDMS_URL, DEFAULT_WS_URL, build_collection_payload
from .exceptions import EDMSRequestError, TimeoutExceeded, WebSocketError
from .models import ExternalResponse, RequestResult
from .websocket import WebSocketManager


class HashedTokensClient:
    def __init__(
        self,
        edms_url: str = DEFAULT_EDMS_URL,
        ws_url: str = DEFAULT_WS_URL,
        collection: str = DEFAULT_COLLECTION,
        timeout: float = 15.0,
        http_client: Optional[AsyncHTTPClient] = None,
        websocket_manager: Optional[WebSocketManager] = None,
    ) -> None:
        self.edms_url = edms_url
        self.ws_url = ws_url
        self.collection = collection
        self.timeout = timeout
        self.http_client = http_client or AsyncHTTPClient()
        self.websocket_manager = websocket_manager or WebSocketManager(ws_url, timeout=timeout)

    async def request(
        self,
        url: str,
        payload: Any = None,
        method: str = "GET",
        timeout: Optional[float] = None,
        headers: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> RequestResult:
        effective_timeout = self.timeout if timeout is None else float(timeout)
        if effective_timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if kwargs.get("ops", "insert") != "insert":
            raise ValueError("This wrapper only supports ops='insert' to avoid replaying the external request")
        deadline = time.monotonic() + effective_timeout
        request_headers = dict(headers or {})

        request_method = (method or "GET").upper()
        if request_method == "GET":
            external_response = await self.http_client.request(
                url,
                method=request_method,
                payload=None,
                headers=request_headers,
                timeout=self._remaining(deadline),
                params=payload if isinstance(payload, Mapping) else None,
            )
        else:
            external_response = await self.http_client.request(
                url,
                method=request_method,
                payload=payload,
                headers=request_headers,
                timeout=self._remaining(deadline),
            )

        try:
            edms_payload = build_collection_payload(
                url=external_response.url,
                method=request_method,
                collection=kwargs.get("collection", self.collection),
                request_payload={} if request_method == "GET" else payload,
                response_payload=self._coerce_body(external_response),
                request_headers=kwargs.get("request_headers") or request_headers,
                response_headers=external_response.headers,
                tags=kwargs.get("tags"),
                collection_tags=kwargs.get("collection_tags"),
                annotation=kwargs.get("annotation"),
                collection_annotation=kwargs.get("collection_annotation"),
                notes=kwargs.get("notes"),
                timeout_ms=kwargs.get("timeout_ms"),
                endpoint_id=kwargs.get("endpoint_id"),
                return_response=kwargs.get("return_response"),
            )
        except ValueError as exc:
            return RequestResult(
                response_obj=external_response,
                ht_response={
                    "ok": False,
                    "status": {"code": 103, "name": "INVALID_PARAM", "message": str(exc)},
                    "detail": str(exc),
                },
            )

        edms_response = await self._submit_to_edms(edms_payload, deadline, kwargs)
        return RequestResult(response_obj=external_response, ht_response=edms_response)

    async def _submit_to_edms(self, payload: Dict[str, Any], deadline: float, kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        remaining = self._remaining(deadline)
        if remaining <= 0:
            raise TimeoutExceeded("Overall wrapper timeout exceeded")

        websocket_ready = False
        try:
            remaining = self._remaining(deadline)
            if remaining <= 0:
                raise TimeoutExceeded("Overall wrapper timeout exceeded")
            await self.websocket_manager.connect(timeout=remaining)
            websocket_ready = True
        except TimeoutExceeded:
            if self._remaining(deadline) <= 0:
                raise
        except (WebSocketError, OSError):
            pass

        remaining = self._remaining(deadline)
        if remaining <= 0:
            raise TimeoutExceeded("Overall wrapper timeout exceeded")
        headers = {"Content-Type": "application/json"}
        immediate = await self.http_client.request(
            self.edms_url,
            method="POST",
            payload=payload,
            headers=headers,
            timeout=remaining,
        )
        immediate_body = self._coerce_body(immediate)

        if immediate.status_code != 202:
            if isinstance(immediate_body, dict):
                status = immediate_body.get("status")
                if isinstance(status, dict) and status.get("code") == 104:
                    immediate_body["ok"] = True
                return immediate_body
            return {"ok": False, "status": {"code": immediate.status_code, "name": "HTTP_ERROR", "message": immediate.text}, "detail": immediate.text}

        if not isinstance(immediate_body, dict):
            return {"ok": False, "status": {"code": 109, "name": "INTERNAL_ERROR", "message": "EDMS returned a non-JSON body"}, "detail": immediate.text}

        if immediate_body.get("ok") is False:
            detail = immediate_body.get("detail", immediate.text)
            status = immediate_body.get("status", {})
            return {
                "ok": False,
                "status": status,
                "detail": detail,
            }

        transaction_id = immediate_body.get("transaction_id")
        if not transaction_id:
            raise EDMSRequestError("EDMS response did not include a transaction_id")

        if websocket_ready:
            remaining = self._remaining(deadline)
            if remaining <= 0:
                raise TimeoutExceeded("Overall wrapper timeout exceeded while waiting for EDMS result")
            try:
                final_result = await self.websocket_manager.wait_for_transaction(transaction_id, timeout=remaining)
                if final_result is not None:
                    return self._extract_final_result(transaction_id, final_result)
            except (TimeoutExceeded, WebSocketError):
                if self._remaining(deadline) <= 0:
                    raise TimeoutExceeded("Overall wrapper timeout exceeded while waiting for EDMS result")

        return await self._poll_rest(transaction_id, deadline)

    async def _poll_rest(self, transaction_id: str, deadline: float) -> Dict[str, Any]:
        parsed = urlsplit(self.edms_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        poll_url = f"{base_url}/jobs/{transaction_id}"
        while True:
            remaining = self._remaining(deadline)
            if remaining <= 0:
                raise TimeoutExceeded("Overall wrapper timeout exceeded while polling EDMS job status")
            status_response = await self.http_client.request(
                poll_url,
                method="GET",
                timeout=min(0.5, remaining),
            )
            poll_body = self._coerce_body(status_response)
            if status_response.status_code >= 400:
                return poll_body if isinstance(poll_body, dict) else {
                    "ok": False,
                    "transaction_id": transaction_id,
                    "status": {
                        "code": status_response.status_code,
                        "name": "HTTP_ERROR",
                        "message": status_response.text,
                    },
                    "detail": status_response.text,
                }
            if isinstance(poll_body, dict) and poll_body.get("status") in {"done", "failed", "stalled"}:
                result = poll_body.get("result")
                if isinstance(result, dict):
                    result_status = result.get("status")
                    result_code = result_status.get("code") if isinstance(result_status, dict) else None
                    return {
                        "ok": result_code in {0, 104},
                        "transaction_id": transaction_id,
                        "ops": poll_body.get("op") or "insert",
                        "result": result,
                    }
                return {"ok": False, "transaction_id": transaction_id, "status": {"code": 109, "name": "INTERNAL_ERROR", "message": "Missing result in polling response"}, "detail": poll_body}
            if remaining <= 0.5:
                raise TimeoutExceeded("Overall wrapper timeout exceeded while waiting for EDMS job completion")
            await asyncio.sleep(0.5)

    @staticmethod
    def _coerce_body(response: ExternalResponse) -> Any:
        if not response.body:
            return {}
        value = response.json
        if value is not None:
            return value
        return response.text

    @staticmethod
    def _extract_final_result(transaction_id: str, message: Dict[str, Any]) -> Dict[str, Any]:
        event = message.get("event") if isinstance(message, dict) else None
        payload = event.get("payload") if isinstance(event, dict) else None
        if not isinstance(payload, dict):
            payload = message.get("payload") if isinstance(message, dict) else {}
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            return {"ok": False, "transaction_id": transaction_id, "status": {"code": 109, "name": "INTERNAL_ERROR", "message": "Final EDMS result payload was missing"}, "detail": message}
        status = result.get("status") if isinstance(result, dict) else {}
        code = status.get("code") if isinstance(status, dict) else None
        return {
            "ok": code in {0, 104},
            "transaction_id": payload.get("transaction_id", transaction_id),
            "ops": payload.get("op") or "insert",
            "result": result,
        }

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        return max(0.0, remaining)


async def request(
    url: str,
    payload: Any = None,
    method: str = "GET",
    timeout: float = 15.0,
    headers: Optional[Mapping[str, Any]] = None,
    edms_url: str = DEFAULT_EDMS_URL,
    ws_url: str = DEFAULT_WS_URL,
    collection: str = DEFAULT_COLLECTION,
    **kwargs: Any,
) -> RequestResult:
    client = HashedTokensClient(
        edms_url=edms_url,
        ws_url=ws_url,
        timeout=timeout,
        collection=collection,
    )
    return await client.request(url, payload, method, timeout=timeout, headers=headers, **kwargs)


__all__ = ["HashedTokensClient", "request"]
