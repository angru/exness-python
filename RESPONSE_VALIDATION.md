# Critical response validation audit

This audit covers the pinned Trading API 1.0.2 contract, specifically
`UInt64String`, ACK/operation-status envelopes, transaction correlation fields,
order/position/deal identity and state, and HMR period identity. It is not a claim
of complete runtime JSON Schema validation of every response property.

## Findings and implemented rules

| Area | Contract evidence | SDK policy |
| --- | --- | --- |
| IDs | `UInt64String` describes an unsigned 64-bit integer with 1–20 decimal digits | Require the full digit string and value at most 18446744073709551615; preserve leading zeroes and permit zero for response entity/operation/HMR IDs |
| Account configuration | Account selection has its own existing positive canonical-ID policy | Keep constructor/account routing validation separate from general response IDs |
| ACK | Only status is required; optional IDs have string schemas, not null schemas | Missing IDs remain absent; supplied null is rejected; accepted remains admission only |
| Operation status | Required operation_id/status/created_at/updated_at; fixed status enum | Reject missing/null required fields and invalid status; optional caller ID/error fields may be omitted but not supplied as null |
| Transactions | Required event_type/payload; optional operation ID/status/error/time fields | Validate supplied identity and terminal discriminator; missing status remains permitted and proves no completion |
| Transaction caller ID | Explicit legacy `nullable: true` despite OpenAPI 3.1 | Continue accepting null only for this explicitly nullable correlation field |
| Transaction error code | PublicApiErrorCode has int32 format | Reject overflow independently of Go's platform int size |
| Entity states | Order type/state, position direction/state and deal type have explicit enums | Validate these critical discriminators and required entity fields in both SDKs |
| Related entity IDs | Order.position_id and Deal.order_id/position_id refer to UInt64String | Validate supplied values; omission is permitted, explicit null is not |
| Snapshot arrays | orders/positions are optional arrays, without nullability | Omission means no returned entries; explicit null is invalid for REST and streaming snapshots |
| HMR | Required period fields and UInt64String IDs, including removed IDs | Preserve schedule text; reject invalid IDs, absent/null required fields and invalid period kinds |

The JSON Schema pattern for UInt64String alone cannot express the documented
numeric range, and regex end-anchor behavior can vary across validators. SDK
checks enforce the textual uint64 semantics in addition to the schema shape.
The schema's int32 format likewise needs an explicit numerical bound. The pinned
YAML is not rewritten to hide these limitations.

## Compatibility

Go's audited entity/status JSON decoders now reject malformed values immediately,
including when used directly through encoding/json. Python's shared Identifier
constraint applies to all response model fields that use it. Optional None defaults
still represent omitted data; explicitly constructing audited non-nullable fields
with None now fails. These stricter malformed-input checks belong to the untagged
alpha source; no release tag is changed.

Unknown properties are still accepted: Python retains them, while Go discards
them. Existing optional condition values and explicitly nullable fields remain
supported. This audit does not tighten every nested optional field or every
noncritical string enum. Existing Go zero-time rejection and general differences
in model completeness remain documented alpha limitations, not new parity claims.

## Evidence

`response-validation.json` is copied byte-for-byte between Python tests/fixtures
and Go testdata. Its 170 cases cover valid boundaries, zeroes/leading zeroes,
overflow, wrong types, missing/null fields, unknown fields, invalid states and
malformed timestamps. Decoder tests exercise the normal snapshot prerequisites.
REST regressions verify that a malformed nested ID becomes ProtocolError through
both Python clients and the Go client. Existing signing, submission, stream and
recovery suites must continue to pass. No request reaches a live service.

## Generated decoder checks

Work package 2 adds shared malformed-message seeds (`decoder-seeds.json`) and
snapshot-state traces (`decoder-sequences.json`). The traces use 0 for a valid
snapshot, 1 for a delta, 2 for a malformed snapshot, 3 for a new decoder/connection,
and 4 for an empty replacement snapshot. An independent boolean oracle checks
that deltas require a successful snapshot in the current connection; a failed
snapshot cannot establish one. Normal stream error handling still terminates the
connection: continuing after rejected input here tests decoder state in isolation.

Python runs deterministic Hypothesis examples for REST responses, all five
WebSocket decoders, structured mutations and generated transition sequences.
A shared deeply nested JSON seed exposed unhandled RecursionError in the Python
WebSocket parsing prepass. Those failures now become sanitized ProtocolError
without changing the interpreter recursion limit or swallowing caller cancellation.

Go adds FuzzRESTResponses, FuzzWebSocketMessages and FuzzDecoderSequences. REST
cases use an in-memory transport and assert response body closure; WebSocket
cases call the decoders directly. Seed cases run in ordinary go test. CI runs
each fuzz target for five seconds with two workers on Go 1.27, alongside the five
existing targets. Decoder inputs are capped at 8 KiB and sequences at 32 actions
in these test drivers; these are test budgets, not new SDK message-size limits.
No generated test sends requests to an external API.

Run the generated tests explicitly from the relevant repository:

```sh
uv run pytest tests/test_decoder_properties.py
```

```sh
go test -run '^$' -fuzz '^FuzzRESTResponses$' -fuzztime 5s -parallel 2 .
go test -run '^$' -fuzz '^FuzzWebSocketMessages$' -fuzztime 5s -parallel 2 .
go test -run '^$' -fuzz '^FuzzDecoderSequences$' -fuzztime 5s -parallel 2 .
```

Longer exploratory runs remain opt-in. Keep every discovered regression as a
normal test or deterministic seed. Passing a bounded fuzz run does not prove that
all malformed inputs or message orderings have been explored.
