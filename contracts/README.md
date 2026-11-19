# Upstream contract

`trading.public.yaml` is an unmodified snapshot of the public Exness contract,
downloaded on **2026-11-19 (Asia/Nicosia)**. API version: **1.0.2**, OpenAPI: **3.1.1**.
It is reference material from Exness, not an SDK-generated specification.

- Source: https://www.exness-api.com/openapi/trading.public.yaml
- Reference: https://www.exness-api.com/reference/trading/exness-trader-api
- SHA-256: `ebde19011ffbd30240e918f9369c8233fc9c5a4afb27eca0462b40a141d0f1b3`
- Discovery (outside this YAML): https://www.exness-api.com/trading/get-started/access-point-host-discovery
- Key formats and signing example: https://www.exness-api.com/trading/guides/code-examples
- Key setup: https://www.exness-api.com/authentication/api-key-and-host-setup

## Implemented operations

| SDK method | Contract operationId / guide |
| --- | --- |
| `discover` | Discovery guide, `GET /v1/trading/access-point?account_id=...` |
| `get_account` | `getAccountDetails` |
| `get_instruments` | `getAvailableInstrumentList` |
| `get_snapshot` | `getTradingStateSnapshot` |
| `get_limits` | `getLimits` |
| `get_orders_history`, `iter_orders_history` | `getOrdersHistory` |
| `get_deals_history`, `iter_deals_history` | `getDealsHistory` |

SDK choices: accept only HTTPS origins, do not follow redirects, cache discovery
until explicit refresh, do not retry automatically, require aware input datetimes,
retain extra response fields but reject unknown enum values. Monetary fields are
validated as plain decimal strings and exposed as `Decimal`. Explicit JSON null is
accepted for optional model fields; omitted collection fields default to empty lists.

Tests validate synthetic response fixtures against the pinned schemas and exercise
signed requests through MockTransport. This establishes local contract consistency,
not live interoperability. The schema includes some `nullable` keywords alongside
OpenAPI 3.1.1; fixtures avoid null for those fields, while the corresponding SDK
SL/TP fields accept null as documented.

For updates, download to a temporary file, compare the version and schema diff,
update affected models/methods/fixtures, run all checks, and record the new date and
hash here. Never overwrite the snapshot silently during builds or tests.

The TypeScript SDK is not used as a source dependency. Tests use the publicly known
RFC 8032 test key; no production data or credentials are present.

`tests/fixtures/signing-vector.json` is a language-neutral deterministic GET signing
vector intended for reuse in the Go SDK. Its signature was produced independently
with OpenSSL 3.6.5 `pkeyutl -sign -rawin`, using the RFC 8032 seed wrapped in PKCS#8
DER. Tests compare the SDK output byte-for-byte; OpenSSL is not required at test time.
