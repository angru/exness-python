# Verify consumers outside the checkout

These are development checks for the current revision, not release or publication
commands. They use synthetic keys, a `.invalid` destination and in-memory HTTP
transports. No Exness account, live server or real trading request is involved.

## Python wheel

From the Python repository with uv installed:

```sh
sh tools/check-installed.sh
```

The runner builds an sdist and then a wheel from it in a temporary directory. It
exports pinned runtime dependencies from `uv.lock`, creates a fresh virtualenv with
the project's selected Python interpreter and installs the wheel (not an editable
project). It copies `consumers/consumer.py` outside the repository and executes it
with Python isolated mode, with PYTHONPATH/MYPYPATH removed. The consumer checks
that `exness.__file__` is inside the new environment, `py.typed` is installed, and
the distribution contains only SDK files and package metadata.

Runtime scenarios run before development dependencies are installed, and `uv pip
check` validates installed dependency consistency. Then the locked development
dependencies are installed to supply mypy/Pydantic's plugin. Strict mypy checks the
consumer against that same installed wheel, with `assert_type` checks on submission
and waiting results. `invalid_types.py` must produce exactly three argument-type
errors (wrong request object, idempotency key and Decimal field). A missing typing
marker or a library silently becoming Any cannot pass that negative check.

Both sync and async consumers cover:

- Five submission methods, including limit, stop and SLTP pending-order variants.
- Typed ACK/operation results, pending-to-confirmed polling and correlation with a
  transaction event. An ACK is still only admission.
- PATCH explicit null and close-position volume query serialization.
- A 503 response preserving UnknownOutcomeError and caller identity, with exact
  request counts ruling out an automatic retry.
- OperationWaitTimeout construction/fields, current root exports, public event
  models and stream alias identity. The five asyncio stream generators are created
  and closed without iterating; installed checks do not exercise WSS transport.

The timeout check validates the exported type/fields, not a real elapsed deadline;
the existing operation/TLS suites exercise actual timeouts and cancellation.
The invalid-type source is intentionally excluded from ordinary mypy's passing
file list; the standalone runner is responsible for asserting its failure.

## Independent Go module

From the Go repository with Go >=1.26 installed:

```sh
sh tools/check-consumer.sh
```

The runner prepares dependencies, copies only root production `.go` files plus
go.mod/go.sum/LICENSE into an isolated temporary SDK directory, and copies the
standalone `consumers` module into a separate directory. It disables GOWORK and
adds a module replacement pointing to the isolated SDK copy. No original checkout,
SDK test files, fixture directories or private helpers are part of the build.

The consumer uses only public identifiers. Its tests run with `-mod=readonly`,
`-race`, `-count=1` and a 30-second suite timeout; GOPROXY/GOSUMDB are disabled during
execution and the installed toolchain is used. Tests cover the same seven request
variants, PATCH/close options, two status reads, operation/event correlation, timeout
error unwrapping, unknown outcome cause inspection and every response body closing.
All five stream entry points run with a pre-cancelled context and must preserve
inspectable cancellation without issuing HTTP requests. Alias/model/error exports
must compile from the independent module. A separate wrong request type must fail
compilation with the expected OpenPositionRequest diagnostic.

This verifies independent module consumption of the current source revision. It
does not test downloading a tagged module from a registry/proxy or the contents of
a future release archive. Nested `consumers/go.mod` intentionally excludes this
fixture from the SDK's normal `go test ./...`; the dedicated command and CI run it.

## CI, network boundaries and cleanup

Each supported Python/Go matrix entry runs its consumer command. Checkout, builds,
toolchain/dependency preparation may download public dependencies. Consumer HTTP
execution is always synthetic. With dependencies/build tools already cached, use
`UV_OFFLINE=1` for Python or `GOPROXY=off GOTOOLCHAIN=local` for Go to run preparation
offline too. Python requires the cached versions from uv.lock, including mypy's
development dependencies; an existing editable environment alone is insufficient.

Both runners use fresh temporary directories and remove their own temporary files
on exit. They do not update dependency locks, change tags or publish artifacts.
The ordinary unit/TLS suites, cross-SDK parity and public API signature checks remain
separate gates. See [API_REVIEW.md](API_REVIEW.md) for the current 18 paired Python
entry points and the historical alpha-7 boundary.
