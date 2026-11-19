# Exness Python SDK

Unofficial, read-only SDK for the public Exness Trading API. Use English for code
and documentation; reply to the user in Russian. Python >=3.11, uv only.

- Source contract: `contracts/trading.public.yaml`; provenance in `contracts/README.md`.
  Verify endpoint changes against official documentation; never guess hostnames.
- All functions need type annotations. Keep `py.typed` and mypy strict enabled.
- Run `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`,
  `uv run pytest`, and `uv build` before delivery.
- Test signing against transmitted bytes and validate response fixtures against
  the pinned OpenAPI schemas. Tests must run offline with synthetic keys.
- TLS integration tests need loopback socket binding. Keep Hypothesis cases
  bounded and reproducible; no live API access is needed for the test suite.
- Keep sync/async signatures and behavior aligned. Shared protocol validation
  belongs in `_common.py`; asyncio cancellation must propagate unchanged.
- Tick streaming is asyncio-only. Reconnects must stay opt-in, bounded and visible
  as gap events. Never enable signed-header logging, redirects or implicit proxies.
- Review upstream drift with `uv run python -m tools.check_contract` (or offline
  `--candidate PATH`). Snapshot adoption requires reviewing both SDKs and recording
  provenance; the checker never updates the snapshot itself.
- Never log credentials, signed headers, or private key material. Do not add live
  trading tests or automatic retries as incidental changes.
- This repository owns its Git history and dependencies. Do not include the vault.
