# Exness Python 0.1.0a7

Python 3.11–3.14. Distribution `exness-python`, import package `exness`.
This release is prepared on GitHub; the distribution is **not published to PyPI**.

## Install the tagged source

```sh
uv add "git+https://github.com/angru/exness-python.git@v0.1.0a7"
```

Or install the wheel asset with `uv pip install /path/to/exness_python-0.1.0a7-py3-none-any.whl`.
Release assets include the wheel, source distribution, API review and SHA256SUMS.
Verify downloaded files with `shasum -a 256 -c SHA256SUMS` from their directory.

## Included functionality

- Signed discovery and all nine read-only REST operations from Trading API 1.0.2.
- Order/deal pagination, precise monetary values, typed responses and errors.
- Five WebSocket streams: ticks, transactions, account state, instruments and HMR.
- Opt-in bounded reconnects and visible gaps; fresh transaction/HMR snapshots after
  reconnect. Dedicated connections, cancellation and credential-safe error strings.

## API review changes

`StreamOptions` and `StreamGap` are the neutral names for shared transport settings
and gap events. All older tick/trading/server aliases remain supported. There are
no removed methods or renamed response fields. Python now rejects new work as soon
as `aclose()` starts, including while socket cleanup is still pending. The review
also records intentional Python/Go differences in validation, timeouts, buffering
and client lifecycle; see [API_REVIEW.md](API_REVIEW.md).

## Compatibility and limits

This is an **unofficial alpha**, not a stable or production-verified SDK. Public
APIs may change in later alphas with release notes. The pinned contract is API 1.0.2
(SHA-256 `ebde19011ffbd30240e918f9369c8233fc9c5a4afb27eca0462b40a141d0f1b3`).
Live Exness interoperability has not been tested. There are no trading writes,
implicit trade retries, durable replay, automatic cache reconciliation or
exactly-once guarantees. Tests use only synthetic credentials and local servers.

Python `0.1.0a7` and Go `v0.1.0-alpha.7` are the first tagged release pair; the shared
alpha number follows six source-only development iterations. Tags identify fixed
commits and must not be moved. GitHub release entries are prepared as drafts marked
prerelease; publishing a draft or uploading to a package registry is a separate
step. Source tags can already be installed explicitly.

## Reproduce validation and artifacts

```sh
git checkout v0.1.0a7
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
uv build --out-dir dist/alpha7
```

CI covers Python 3.11–3.14, including installed-wheel smoke checks. Validate the
wheel outside the checkout and retain `py.typed`. The source distribution includes
contract fixtures, the API review and these release notes. Dependencies stay as
bounded ranges in package metadata; `uv.lock` pins the development environment.
