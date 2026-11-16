# HashedTokens Python wrapper

`hashedtokens` asynchronously calls an external HTTP API, submits the captured
request and response bodies to EDMS, waits for the matching EDMS result, and
returns both results. Network communication uses Python's standard library.

## Install

```powershell
python -m pip install -e .
```

There are no third-party runtime dependencies. `setuptools` is the package
build backend.

## Usage

```python
import asyncio
import hashedtokens as ht


async def main():
    result = await ht.request(
        "https://api.example.com/api/posts?page=1",
        payload={},
        method="GET",
        headers={"Accept": "application/json"},
        timeout=15,
        collection="demo",
        tags=["blog", "read"],
        collection_tags=["v1"],
        annotation="list posts",
        collection_annotation="blog API",
        notes="bookmark note",
    )

    print(result.response_obj.status_code)
    print(result.response_obj.json())
    print(result.ht_response["status"])


asyncio.run(main())
```

Public signature:

```python
async def request(
    url,
    payload=None,
    method="GET",
    *,
    timeout=15,
    headers=None,
    edms_url="http://localhost:3000",
    ws_url=None,
    collection="demo",
    tags=None,
    collection_tags=None,
    annotation=None,
    collection_annotation=None,
    notes=None,
    endpoint_id=None,
    redact_headers=(),
    redacted_value="[REDACTED]",
)
```

Supported external HTTP methods are GET, POST, PUT, PATCH, and DELETE. GET
payloads are URL query parameters. For other methods, a non-`None` payload is
JSON-encoded.

## Transaction behavior

1. The wrapper calls the target API directly, exactly once.
2. It opens the EDMS WebSocket before submitting the captured request.
3. It submits one `insert` operation to
   `POST /hashedtokens/edms/api/collections/`.
4. It obtains the EDMS `transaction_id` from the HTTP 202 response and waits
   for the matching `DevApiResult` WebSocket event. A shared WebSocket reader
   routes concurrent results by transaction ID.
5. If the socket is unavailable or disconnects, it falls back to
   `GET /jobs/{transaction_id}` polling every 0.5 seconds.
6. It returns `RequestResult`, with `response_obj` for the external API and
   `ht_response` for EDMS's final result.

`timeout` defaults to 15 seconds and is a total deadline for the external call,
EDMS submission, and final result wait. If the transaction cannot complete by
that deadline, `TransactionTimeout` is raised and carries the original
`response_obj` when that response was already received. EDMS codes 106 and 108
are not used to represent a client-side wait timeout.

HTTP error responses from the target API are still captured and submitted;
for example, a target 404 is not automatically an EDMS error. EDMS immediate
HTTP errors are returned in `ht_response` as the documented JSON response.
Unexpected responses or communication failures raise a `WrapperError`
subclass; EDMS integration errors carry the original external `response_obj`.

## EDMS payload and limitation

The wrapper uses the confirmed collections `insert` contract. Its payload
contains `ops`, `endpoint_str`, `method`, `collection`, `request_body`,
`response_body`, `request_headers`, and `response_headers`, plus optional
tags, annotations, notes, and `endpoint_id`. It does not use
`test_and_insert`, because that operation can call the target API again.

Before insertion, the wrapper calls the documented endpoint lookup using the
exact URL and method. If an endpoint is found, its existing `endpoint_id` is
included in the insert. A per-process asynchronous lock avoids simultaneous
lookups/inserts for the same URL and method from this package instance. The
backend index is not unique, however, so separate processes can still race to
create the same endpoint.

**Important:** EDMS `insert` stores the supplied request and response bodies,
but does not store the target API status code or timing. Those remain on
`response_obj` (`status_code`, response headers/body, and `elapsed_ms`). The
wrapper does not invent extra EDMS payload fields to store them.

The History endpoint remains unimplemented because its exact payload was not
provided.

## Security

Before submitting headers to EDMS, the wrapper replaces values for these
headers with `[REDACTED]`: `Authorization`, `Proxy-Authorization`, `Cookie`,
`Set-Cookie`, `X-API-Key`, `API-Key`, `X-Auth-Token`, and `X-Access-Token`.
Pass additional header names in `redact_headers`. The original request headers
are used for the target call and remain available on `response_obj`; they are
not written to logs by this package.

No EDMS authentication method was supplied, so the wrapper does not invent one.
The endpoint URL, including its query string, is sent to EDMS for endpoint
matching and is not redacted; do not put credentials or API keys in query
parameters unless the EDMS storage policy explicitly allows it.

Inspect `result.ht_response["status"]["code"]` for the EDMS outcome. Codes `0`
and `104` indicate successful storage; `104` means the identical QP already
existed and was not stored again. Other final codes are returned as EDMS
results, not converted into local status codes.

## Errors and response object

`response_obj` is an `AsyncHTTPResponse`, not a third-party response object. It
provides `status_code`, `reason`, response `headers`, raw `body`, decoded
`text`, `json()`, and the request URL/method/headers/body and `elapsed_ms`.

Invalid input raises `ValueError`. External HTTP connection, protocol, and
timeout failures raise `HTTPConnectionError`, `HTTPProtocolError`, or
`HTTPRequestTimeout`. EDMS contract errors raise `EDMSProtocolError`;
submission or polling failures raise `EDMSIntegrationError`; total transaction
deadline expiry raises `TransactionTimeout`.

Non-JSON target response bodies are kept as text in EDMS while the original
bytes remain available on `response_obj.body`. The client does not follow
redirects or decompress encoded response bodies.
