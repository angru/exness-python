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
