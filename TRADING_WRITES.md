# Trading operations development plan

Status: stages 1–3 implemented on main; stage 4 is next. Clients expose read and
explicit trading submission operations. The stages below apply to both SDKs; completion means offline contract
and transport verification, not live interoperability. Publication is a separate task.

## Sources and boundaries

- [Trading OpenAPI](https://www.exness-api.com/openapi/trading.public.yaml), version
  1.0.2, pinned SHA-256 `ebde19011ffbd30240e918f9369c8233fc9c5a4afb27eca0462b40a141d0f1b3`.
  The latest upstream comparison found identical bytes.
- [Request signing](https://www.exness-api.com/authentication/request-signing).
- [Runtime semantics](https://www.exness-api.com/trading/guides/runtime-semantics).

The pinned contract's `EXN-IDEMPOTENCY-KEY` header and character constraints take
precedence over the abbreviated `Idempotency-Key` wording in some guide examples.
Account/instrument restrictions, market conditions and permissions remain server
decisions. Tests use synthetic keys and local TLS servers.

## Delivery stages

| Stage | Deliverable | Acceptance evidence |
| --- | --- | --- |
| 1. Request foundation | General exact-byte signing, explicit idempotency-key validation, `OpenPositionRequest`, `AckResponse` | Shared signed POST/PATCH/DELETE vectors, schema-checked fixtures, exact body verification over local TLS; existing GET vectors unchanged |
| 2. Submission methods | All five methods below, remaining request types, bounded ACK parsing and explicit unknown-outcome errors | Python sync/async parity; Go parity; invalid inputs make no request; each method sends once; error, redirect, timeout and cancellation scenarios |
| 3. Result tracking | Explicit bounded status polling; documented event correlation and recovery | Accepted/pending/confirmed/rejected/failed scenarios; timeout preserves operation identity; no write retries |
| 4. Application scenarios | Runnable offline account-state/recovery example and cross-SDK scenarios | Snapshot replacement, partial close, rejected execution, stream gaps and duplicate observations |
| 5. Live verification | Explicitly enabled interoperability checklist using an available test account | Recorded real request/event observations; credentials never enter fixtures or logs |

Stages 1–4 can proceed without an account. Stage 5 requires access and explicit
authorization for the particular live operations. Existing read-only examples
remain read-only. Implementation and tests should land together in each repository.

## Implemented submission surface

All methods below are implemented on main. Python async methods mirror
the sync signatures; Go methods take a context. All require a caller-provided
idempotency key and return an acknowledgement rather than a trading result.

| Python / Go | HTTP shape | Request |
| --- | --- | --- |
| `place_pending_order` / `PlacePendingOrder` | POST `/v1/trading/accounts/{account_id}/orders` | Typed limit, stop or SLTP variant |
| `modify_order` / `ModifyOrder` | PATCH `/v1/trading/accounts/{account_id}/orders/{order_id}` | At least one price/SL/TP change |
| `cancel_pending_order` / `CancelPendingOrder` | DELETE `/v1/trading/accounts/{account_id}/orders/{order_id}` | Empty body |
| `open_position` / `OpenPosition` | POST `/v1/trading/accounts/{account_id}/positions` | `OpenPositionRequest` |
| `close_position` / `ClosePosition` | DELETE `/v1/trading/accounts/{account_id}/positions/{position_id}` | Query parameters for volume, price, deviation and close-by position |

## Request and signing rules

- Encode a request once, hash those bytes, and send those same bytes. Sign the
  final escaped path/query and HTTP method. The signer never serializes a model.
- Require 5–128 ASCII characters from `A-Z a-z 0-9 . _ - ~` for every mutation.
  Generate no hidden keys and perform no automatic mutation retries, including 429.
  GET keeps its required empty key and empty body.
- Keep prices and volumes exact. Reject floats, nonfinite numbers and nonpositive
  open volumes/prices. Python serializes Decimal values in plain notation; Go
  retains the supplied plain decimal spelling. JSON bytes need not be identical
  between languages, but each signature must match its own transmitted bytes.
- Validate request shape before discovery or submission. Do not silently fetch
  instrument conditions or pretend static validation guarantees execution.
- Distinguish omission, a value and explicit null in PATCH requests. Omission
  preserves SL/TP; null clears it. Reject null where the contract only permits
  omission. A comment-only modification is invalid.
- DELETE cancellation has no JSON body. Closing position options go in the query;
  zero close volume is allowed only with a close-by position. Review that path
  separately from ordinary partial closes.

## Acknowledgements and uncertain outcomes

HTTP 202 with `status=accepted` confirms admission only. `AckResponse` preserves
optional `operation_id` and `client_request_id`: the schema requires only status,
so a missing operation ID must not be fabricated. A mismatched echoed client ID
is a protocol failure and cannot be treated as a successful correlation.

Before submission, validation/discovery failure means this SDK has not submitted
the write. After entering the write transport, a timeout, disconnect, truncated
response or invalid ACK may leave the server outcome unknown. Stage 2 exposes
that distinction with a dedicated error carrying request identity and the caller's
key, without embedding request bodies or credentials in error text. Asyncio
cancellation still propagates unchanged; Go errors retain context cancellation
identity. Documentation must explain that cancellation does not roll back a write.

When an operation ID is known, use existing status reads or correlated transaction
events. If the ACK/ID is missing, reconcile using a fresh trading snapshot and
events; there is no lookup-by-idempotency-key endpoint in this contract. Never
retry with a new key merely because a response was lost. Reusing a key is not an
unlimited guarantee: retention is finite and its duration is not specified here.

Stage 3 preserves terminal distinctions: `confirmed` is finalized,
`rejected` and `failed` retain their error details, and deadline expiry remains
unknown/pending rather than becoming a fabricated rejection. Polling is an
explicit caller operation with a deadline and delay, not a submission side effect.

## Foundation usage

Python request types live in `exness.requests`; acknowledgement models in
`exness.models`. Go uses the package root. `OpenPositionRequest.to_body()` in Python
and `OpenPositionRequest.MarshalJSON()` in Go produce validated JSON bytes without
network access. All five submission methods, pending-order variants and PATCH
omission/null handling are implemented. Treat these types as an alpha API.

```python
from decimal import Decimal
from exness.requests import OpenPositionRequest

request = OpenPositionRequest(instrument="EURUSD", side="buy", volume=Decimal("0.01"))
body = request.to_body()  # Validated JSON bytes; no network call.
```

In Go, construct `Decimal` with `NewDecimal`, use `TradeBuy`/`TradeSell`, and call
`json.Marshal(OpenPositionRequest{...})`. Nil optional pointers are omitted. Python
rejects explicit None in opening requests; leave those keyword arguments out.

## Implemented submission error policy

- Validation, closed-client and discovery errors occur before write transport; they
  do not become UnknownOutcomeError.
- A fully read HTTP 4xx response other than 408 is an APIError rejection, with its
  optional public code/message and Retry-After. No automatic retry follows it.
- Transport/read failure, oversized body, invalid/mismatched ACK, HTTP 408, 5xx,
  redirects and unexpected status codes are UnknownOutcomeError. This conservative
  policy accounts for intermediary failures that cannot prove non-execution.
- HTTP 202 requires a valid accepted ACK. Missing operation_id is preserved; a
  missing echoed key is permitted, but a different echoed key fails correlation.
- UnknownOutcomeError stores method, account, resource path without query and the
  original idempotency key. Python exposes a sanitized cause attribute; Go unwraps
  its cause for errors.Is/As. Neither error string includes the key or body.
- Python task cancellation propagates unchanged at every phase. Go cancellation
  before transport is a regular TransportError; after submission it is wrapped by
  UnknownOutcomeError while still matching context.Canceled/DeadlineExceeded.
- Preserve the caller's key before starting any call. Neither cancellation nor a
  transport failure authorizes creating a new key and repeating the trade.

Shared write-cases.json fixtures cover all variants, PATCH clearing and full,
partial and close-by closure. The nullable SL/TP keywords in ModifyOrderRequest
are legacy OpenAPI syntax despite the 3.1 document version. Schema tests apply
only their documented null semantics to an in-memory schema; the pinned source
is unchanged. Both SDKs verify actual wire signatures over loopback TLS.

## Implemented result tracking

`OperationReference` holds operation ID, caller request ID, or both, scoped to the
same account as the client and transaction stream. Build it from an ACK with the
persisted caller key as fallback. A key-only reference can correlate events after
an unknown outcome; polling requires an operation ID. No key lookup is invented.

`wait_operation` / `WaitOperation` makes an immediate status read and repeats only
pending results, with a delay after each completed read. Defaults are a 30-second
wait budget and a 1-second poll interval. Choose an interval appropriate to the
account's limits; this helper does not fetch limits or coordinate callers. API,
transport and protocol errors stop polling, including 404 and 429. It never submits
a write or retries one. The returned terminal status retains all available error
details. Timeout errors preserve the reference, last correlated status (or none),
and number of status reads started (including first-read discovery).

Python async waiting and Go contexts cover discovery, lock contention, requests
and delays. Caller cancellation remains inspectable. Python sync waiting checks a
monotonic deadline at request/body boundaries and caps HTTPX network phase timeouts
by the remaining budget, preserving shorter configured timeouts. It cannot abort
an already blocking phase or an uncooperative custom transport at an exact wall
clock deadline. Late responses are not reported as successful waits. The budget
is context-local and does not change the shared HTTP pool's configuration.

`matches_event` / `MatchesEvent` and `matches_status` / `MatchesStatus` are pure
identity checks. At least one shared identifier must agree; a second contradictory
identifier raises ProtocolError. Two wholly unrelated identities return false.
Numeric operation IDs compare equally despite leading zeroes; caller keys compare
exactly. Identity matching does not imply completion: transaction status may be
absent, and its success value is `success`, whereas REST uses `confirmed`. These
helpers neither consume streams nor synthesize a status from changed entities.

After a gap, rebuild account state from a fresh stream snapshot before applying
deltas. A missing ACK, wait timeout, 404, cancellation or incomplete event remains
a reconciliation problem, not permission to repeat the trade with a fresh key.
The next stage adds runnable application recovery scenarios; live verification
and publication remain deferred.
