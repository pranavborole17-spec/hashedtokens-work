from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class ExternalResponse:
    url: str
    method: str
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    reason: str = ""
    elapsed_ms: int = 0
    request_headers: Dict[str, str] = field(default_factory=dict)
    request_body: Any = None

    @property
    def text(self) -> str:
        if not self.body:
            return ""
        try:
            return self.body.decode("utf-8")
        except UnicodeDecodeError:
            return self.body.decode("latin-1", errors="replace")

    @property
    def json(self) -> Any:
        if not self.body:
            return None
        import json

        try:
            return json.loads(self.text)
        except json.JSONDecodeError:
            return None


@dataclass
class RequestResult:
    response_obj: ExternalResponse
    ht_response: Dict[str, Any]
