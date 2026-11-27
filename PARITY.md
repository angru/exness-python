# Offline cross-SDK comparison

`tools/check_parity.py` runs the Python and Go implementations against identical
synthetic inputs. It makes no API requests and needs no credentials. It is optional
development tooling: neither SDK imports or downloads its peer at runtime, and
ordinary builds/tests still work from a single checkout.

## What is checked

- The explicit inventory in `tools/parity-fixtures.json` covers all 43 JSON files
  in Python `tests/fixtures/` and Go `testdata/`. Missing, added and byte-different
  fixtures fail the check. Intentional new fixtures require updating the manifest.
- Recovery scenarios, the manifest and the checker itself must also be byte-identical
  (46 shared files total). The peer lock files intentionally differ.
- Both SDKs execute the 170 response-validation cases and emit their actual decoder
  outcomes: `accepted` or `protocol_error`. Unexpected exception/error types fail.
  Reports must also cover every expected case; two incomplete reports cannot pass.
- The runnable recovery examples emit 49 checkpoints from eight scenarios. Compare
  actual cache readiness, account/entity state, operation identity/outcome and
  rejection decisions, including gaps, replacement snapshots and late results.
  Each example also asserts its fixture expectations independently.

JSON object key order is ignored when comparing reports. Scalar types, array order,
missing versus null fields, identifier strings and reported decimal strings remain
significant. These reports are a deliberately agreed projection, not a comparison of
every serialized model field. Python preserves unknown response fields while Go
ignores them; Python decimal spelling can differ from Go's original spelling.
Recovery reports already canonicalize numeric identities and monetary values by
their documented application policy. Error text and language-specific model names
are not compared. See RESPONSE_VALIDATION.md and examples/RECOVERY.md.

## Run locally

Install Python >=3.11 with uv and Go >=1.26. Prepare both trusted checkouts and run
`uv sync --locked` in the Python checkout. Dependencies/toolchains must be available
locally for a completely offline run (`UV_OFFLINE=1 GOPROXY=off GOTOOLCHAIN=local`).
The checker itself does not fetch or switch Git revisions. It executes code from
the supplied checkouts, so review them as you would any development dependency.

From the Python checkout, with Go checked out at the revision recorded in
`tools/parity-peer.json`:

```sh
python3 tools/check_parity.py \
  --python-root . --go-root ../exness-go --peer go \
  --peer-revision "$(python3 -c 'import json; print(json.load(open("tools/parity-peer.json"))["revision"])')"
```

From the Go checkout, with Python at its recorded peer revision:

```sh
python3 tools/check_parity.py \
  --python-root ../exness-python --go-root . --peer python \
  --peer-revision "$(python3 -c 'import json; print(json.load(open("tools/parity-peer.json"))["revision"])')"
```

Use a separate checkout/worktree when your usual peer checkout has moved past the
pin. `--uv /path/to/uv` and `--go /path/to/go` select executables. The peer must have
the exact full 40-character SHA and a clean tree, checked before and after execution.
The SDK under development may have edits; the final JSON records both HEADs and
dirty flags, file/case/checkpoint counts. Exit 0 means matched; exit 1 reports
checkout/file drift, a bounded unified result diff, or a command failure. Individual
commands time out after five minutes. Temporary reports live outside the repos;
Go report generation disables its test cache (`-count=1`).

## Validate a proposed pair

After preparing two trusted, clean local checkouts and installing dependencies, run
this canonical command from a Python checkout containing the candidate tool:

```sh
UV_OFFLINE=1 GOPROXY=off GOTOOLCHAIN=local python3 -m tools.check_candidate_pair \
  --python-root /path/to/exness-python --python-revision <full-python-sha> \
  --go-root /path/to/exness-go --go-revision <full-go-sha>
```

Supply lowercase 40-character commit SHAs. Both checkouts must match and be clean,
including untracked files, before and after parity; ignored dependency caches are
allowed. `--uv` and `--go` select executable paths. The command runs the existing
checker and synthetic scenarios without credentials or API requests. It does not
fetch, switch revisions or edit pins. It executes code from both checkouts; review
candidate code before running it. A successful JSON report gives both SHAs, match
counts, and the exact proposed contents of each repository's
`tools/parity-peer.json`. Failure produces no proposal.

Review the comparison and both candidate changes. Once both candidate commits are
reachable on GitHub, apply the proposed Go SHA to the Python pin and the proposed
Python SHA to the Go pin in their respective repositories, review those edits and
run both pinned CI jobs. The pin changes themselves need separate commits; the
proposed SHAs identify the tested candidate commits, not those later pin commits.

## Compare public clients over loopback TLS

The Python-owned transport check exercises the public asyncio client and a
standalone Go consumer. Both clients use the same fixture for each scenario and
explicitly trust its temporary certificate. REST reads and WebSocket upgrades
share one ephemeral listener. The write scenarios use a separate HTTP/1.1 TLS
port with the same certificate. Both ports bind to `127.0.0.1`. Each client run
starts with a fresh SDK client and fresh listener observations. Both listeners
verify the synthetic signature against the received request target.

| Scenario | Expected public observations |
| --- | --- |
| `rest_ticks` | Two account reads after one discovery, then an EURUSD tick followed by `ProtocolError` for a malformed frame. |
| `orders_history_repeated_cursor` | One discovery and two order history reads. The public iterators yield order `1002` and then a repeated-cursor `ProtocolError`. The second page contains order `1003`. Yielding it or making a third history request fails the check. |
| `trading_recovery` | Snapshot, partial-close transaction, one gap for close `1012`, a fresh snapshot with a different balance, and a balance transaction. Close `1000` ends the stream normally. |
| `trading_missing_snapshot` | The same initial snapshot, transaction and gap. The second connection sends the same valid balance transaction before a snapshot, yielding `ProtocolError`. The consumer must see no additional event or connection, with one reconnect still available. |
| `hmr_recovery` | A nonempty HMR snapshot, one gap for close `1012`, a fresh snapshot with an empty `periods` list, and a valid HMR update. Close `1000` ends the stream normally. |
| `hmr_missing_snapshot` | The same initial HMR snapshot and gap. The second connection starts with the identical valid update, yielding the missing-snapshot `ProtocolError`. Neither client yields the update or makes a third connection, with one reconnect still available. |
| `lost_ack` | Signed discovery, then one signed open-position POST for EURUSD, buy, volume `0.01`. The fixture reads the complete bounded body and closes the connection without sending an HTTP response. Both SDKs return `UnknownOutcomeError` with the original synthetic idempotency key, account, method and path. |
| `lost_ack_recovery` | Each client submits one open-position POST and uses the key preserved by `UnknownOutcomeError` to correlate a terminal result from a fresh transactions subscription. The snapshot leaves the result unset, the foreign success is ignored, and the matching rejection is selected. |
| `acknowledged_open` | After signed discovery, one verified open-position POST receives HTTP 202 with the caller key and an operation ID. The same client builds a reference from that ACK and waits for confirmation with two signed status reads. |

The reconnect scenarios expect one signed discovery and two signed upgrades to the
events endpoint. Both subscriptions use distinct nonempty request IDs. The trading
scenarios select `transactions`. The HMR scenarios select `hmr` with the explicit
instrument list `["EURUSD"]` on both connections. The check compares each client's
transcript against the expected paths, subscription payloads, event order, selected
values and outcome, then compares the transcripts with each other. The report
excludes generated request IDs, timestamps, ports, credentials and signed headers.
The trading scenarios reuse the shared fixtures, with the replacement snapshot
derived locally from the shared snapshot by changing its balance.

The order history scenario reuses `orders.json` for both pages, changing only
the second order ID. The listener verifies each signature against the raw
request target, including its original query bytes. It then checks the first
query for the January 1, 2026 UTC start, `EURUSD`, state `filled`, and limit `7`.
The continuation query must carry exactly the same limit and the fixture's
opaque cursor, including its punctuation, space, and Unicode. Duplicate keys
and extra filters fail the check. The transcripts compare decoded query values,
with the date normalized to a UTC timestamp. An extra history request fails
the check even if the consumer suppresses the HTTP error.

The HMR scenarios reuse `hmr_snapshot`, `hmr_empty_snapshot` and `hmr_update`.
The check compares the gap attempt and close code, period identifiers, event types,
instruments, leverage, exact schedule strings and removed period IDs.

For `lost_ack`, the fixture verifies the Ed25519 signature against the received
method, raw request target, exact body hash and idempotency header. It checks the
order fields and counts POSTs separately for each client, including duplicate
attempts. The report includes the verified receipt, POST count and error identity.
It excludes the raw body, signed headers and underlying error causes. Both
transcripts must match the fixed expectations and each other.

`lost_ack_recovery` keeps the same HTTP receipt and error checks active through
the WSS phase, so any replay makes the scenario fail. A second client for the same
synthetic account discovers the separate WSS fixture through its own loopback
URL. The fixture requires the verified POST receipt before accepting signed
stream discovery, then checks for a single fresh `transactions` subscription.
The consumers build key-only `OperationReference` values from the error and
call `matches_event` or `MatchesEvent` on each transaction. The stream sends a
snapshot, a successful transaction with a foreign key, and a rejection carrying
the caller key. It then closes normally. The report records each transaction
decision and the selection state after each event, including the empty
selection before the matching transaction. Receipt, error identity, stream
observations and POST count must satisfy the scenario's fixed expectations and
agree across SDKs.

`acknowledged_open` uses the same HTTP/1.1 write fixture and request validation.
The fixture gives the ACK and both status responses the same synthetic
idempotency key and operation ID `000789`. Both GETs must use that exact
operation ID in the request target.
The listener rejects polls before the ACK, wrong targets, extra polls, and
duplicate POSTs. Both transcripts must match the fixed expectation for the
signed receipt, ACK identity, and wire responses `pending` then `confirmed`,
as well as the final correlated `confirmed` result returned by `wait_operation`
or `WaitOperation`. A rejected extra request fails the check even if the driver
returns a successful result.

Use Go 1.26 and a clean Go checkout at the full SHA recorded in
`tools/parity-peer.json`. Prepare dependencies with `go mod download` in that
checkout and `uv sync --locked` in the Python checkout. With those dependencies
cached, run from the Python checkout:

```sh
PEER_REVISION=$(python3 -c 'import json; print(json.load(open("tools/parity-peer.json"))["revision"])')
UV_OFFLINE=1 uv run --no-sync python -m tools.check_transport \
  --go-root ../exness-go --go-revision "$PEER_REVISION"
```

Pass `--go /path/to/go` to select a Go executable. The command verifies Go HEAD
and cleanliness before and after the check, builds the consumer in a temporary
module against the supplied checkout, and disables Go module fetching during
the build. Loopback socket binding is required. The command exits nonzero for
checkout, build, transport or transcript failures. The Python parity job checks
the Go consumer with `gofmt` and runs the transport scenarios in its loopback
TLS step against the pinned Go checkout.

## CI and updating a compatible pair

Each repository has a separate `parity` CI job. It checks the current revision
against the peer SHA in its own `tools/parity-peer.json`, using Python 3.11 and
Go 1.26. The ordinary supported-version matrix stays independent. Both jobs run
on pushes and pull requests with read-only permissions. Dependency installation
and checkout may use the network; scenario execution uses only synthetic data.

Pins deliberately check a reviewed compatible pair, not the other repo's moving
`main`. When adopting a shared change:

1. Update implementations, agreed fixtures/manifests and their local tests in both
   repositories. Keep checker copies identical. Run the local comparison with an
   explicitly committed, clean peer checkout.
2. Commit the compatible implementations to obtain immutable SHAs; publish those
   revisions before using them as CI checkout targets.
3. Update each peer lock to the reviewed compatible peer commit, then run the parity
   jobs. Changes to shared fixtures can make the old-pin check fail until this
   coordinated lock update lands; do not weaken comparisons to hide that failure.

Initial pins target the implementation commits immediately preceding the CI setup,
avoiding circular commit references. This check does not replace contract review,
the wider test suites, installed-package checks or eventual live interoperability.
