# Changelog

## 0.1.0a2 — 2026-11-19

- Complete read-only REST coverage with instrument conditions, candle history and
  operation status. No automatic polling or trading writes.
- Validate candle query combinations and preserve bid/ask/both response shapes;
  market data uses numeric prices, instrument monetary fields use Decimal.
- Shared Python/Go fixtures for all new operations; document the upstream candle
  oneOf ambiguity and validate request-selected schema branches explicitly.
- Add a runnable market-data example. Live interoperability remains unverified.

## 0.1.0a1 — 2026-11-19

- Initial synchronous read-only Trading API client: discovery, account, instruments,
  snapshot, limits and order/deal history with cursor iteration.
- Ed25519 signing, typed validated responses and precise Decimal fields.
- uv lockfile, Ruff, strict mypy, offline contract/signature tests and CI.
- Local alpha only; no live API verification or package publication yet.
