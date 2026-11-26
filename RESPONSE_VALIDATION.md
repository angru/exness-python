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
