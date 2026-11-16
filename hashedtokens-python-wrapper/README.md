# HashedTokens Python Wrapper

A small async Python client for interacting with the HashedTokens EDMS collections API. The wrapper performs the external HTTP request, captures the original response, submits the captured request/response to EDMS, and waits for the final processing result through the EDMS WebSocket API.

## Features

- Async external HTTP client using Python's standard library
- GET, POST, PUT, PATCH, and DELETE support
- JSON request bodies and query-parameter encoding for GET requests
- Timeout-aware request lifecycle
- EDMS collections insert workflow using the documented `insert` operation
- WebSocket transaction correlation with REST polling fallback
- Support for concurrent requests without mixing results
- Redaction of sensitive headers before logging or debug output

## Requirements

- Python 3.11+
- Standard library only for networking (`asyncio`, `ssl`, `json`, `urllib.parse`, etc.)

## Installation

```bash
cd hashedtokens-python-wrapper
python -m pip install -e .
```

## Basic usage

```python
import asyncio
import hashedtokens as ht


async def main() -> None:
    result = await ht.request(
        "https://example.com/api/posts?page=1",
        {"page": 1},
        "GET",
        timeout=15,
        collection="demo",
        tags=["blog", "read"],
    )
    print(result.response_obj.status_code)
    print(result.ht_response)


asyncio.run(main())
```

## Public API

```python
import hashedtokens as ht

result = await ht.request(
    "https://example.com/api/posts",
    {"page": 1},
    "GET",
    timeout=15,
    headers={"Accept": "application/json"},
    collection="demo",
    edms_url="http://localhost:3000/hashedtokens/edms/api/collections/",
    ws_url="ws://localhost:3000/hashedtokens/edms/api/ws",
)
```

The returned object exposes:

- `response_obj`: The original external HTTP response
- `ht_response`: The final EDMS result payload

The top-level call accepts `headers`, `timeout`, `edms_url`, `ws_url`, and
`collection`. Documented collection fields such as `tags`, `collection_tags`,
`annotation`, `collection_annotation`, `notes`, `endpoint_id`,
`request_headers`, `response_headers`, `return_response`, and `timeout_ms` are
also accepted as keyword arguments. `timeout` is the wrapper's overall client
deadline in seconds; `timeout_ms` is a separate EDMS field.

## Supported HTTP methods

The package supports:

- GET
- POST
- PUT
- PATCH
- DELETE

GET requests encode `payload` values into the URL query string. Non-GET methods send JSON bodies when data is present.

## EDMS workflow

The wrapper does the following:

1. Sends the external request through the standard-library async HTTP client.
2. Captures the full response object including status, headers, and body.
3. Opens the EDMS WebSocket connection before the submission request.
4. Submits the captured data to `POST /hashedtokens/edms/api/collections/` with the `insert` operation.
5. Waits for the transaction result over the WebSocket.
6. Falls back to polling `GET /jobs/{transaction_id}` if the WebSocket is unavailable or incomplete.

The wrapper intentionally uses `insert` rather than `test_and_insert` so that the original external HTTP request is not automatically replayed by EDMS.

## Timeout behavior

The wrapper uses a single overall client-side timeout. That timeout covers:

- external HTTP request
- WebSocket connection and result waiting
- EDMS POST
- any polling fallback

The EDMS `timeout_ms` field is separate from the wrapper timeout and is passed through to the EDMS API only when supplied.

## Error handling

The wrapper keeps the original external HTTP response even when the target API returns 4xx or 5xx. The EDMS result is evaluated separately using `result.status.code` from the final result payload.

The final result has an `ok` flag, a `transaction_id`, the operation name, and
the EDMS `result` object. EDMS codes 0 and 104 are treated as successful
outcomes; other EDMS result codes are returned with `ok: false`. Local
collection validation failures are returned as code 103. An immediate EDMS
HTTP error is returned as its response body and does not imply a background
transaction was accepted.

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | OK | Successful operation |
| 1 | UNKNOWN | Unknown status |
| 100 | INVALID_REQUEST | Invalid request |
| 101 | MISSING_PARAM | Required parameter is missing |
| 102 | UNKNOWN_OP | Unsupported operation |
| 103 | INVALID_PARAM | Invalid parameter |
| 104 | DUPLICATE_QP | Identical data already exists |
| 105 | TEST_FAILED | Target test failed; nothing was stored |
| 106 | TEST_TIMED_OUT | EDMS target test timed out |
| 107 | COMPUTE_UNAVAILABLE | EDMS compute service is unavailable |
| 108 | COMPUTE_STALLED | No compute report arrived within the EDMS window |
| 109 | INTERNAL_ERROR | EDMS internal error |

Target API status and elapsed time remain available on `response_obj`. The
`insert` request does not claim to store those values as EDMS test metadata.

## Testing

Run the standard-library unit suite (tests use mocked transports, not a live
EDMS server):

```bash
cd hashedtokens-python-wrapper
python -m unittest discover -s tests -v
```

## Known limitations

- The implementation intentionally relies on the documented EDMS contract and does not invent undocumented API behavior.
- It depends on a running local or reachable EDMS service for full integration testing.
- WebSocket communication uses a standard-library implementation rather than a third-party websocket client.
- EDMS payload and event behavior still needs live-server verification; unit tests do not establish live integration compatibility.
