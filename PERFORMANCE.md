# Local lifecycle stress and measurements

These checks use loopback TLS servers, ephemeral test certificates and synthetic
keys. They do not contact Exness or submit trades. They extend the individual
network/stream tests with repeated, bounded lifecycles.

## Lifecycle checks

Both SDKs run the following six times in their ordinary test suites:

| Scenario | Work per cycle | Completion evidence |
| --- | --- | --- |
| Concurrent REST cancellation | Discover, then six account reads held after response headers; cancel three and release three | Cancelled callers retain cancellation; remaining callers receive the account; all workers finish and all six connections close after client shutdown |
| Stream reconnect budget | Three tick connections, two explicit reconnect gaps, one exhausted-budget error | Gap attempts are 1 and 2; three server handlers finish; SDK stream registries are empty before the next cycle |
| Concurrent stream shutdown | Six streams receive a tick and block reading; cancel three callers, then close the client | All consumers finish, every server observes closure, and stream registries are empty |

The REST test asserts seven requests per cycle, so cancellation cannot silently
introduce a retry. Server barriers establish request/read progress before cancellation;
there are no arbitrary scheduling sleeps. Python explicitly joins consumer tasks and
tracks both SDK waits/connections and every accepted HTTP connection. Go joins its
worker goroutines, accounts for HTTP `ConnState` closures and checks the protected
stream-session registry under the race detector. Cleanup is checked before stopping
the server, so server teardown cannot conceal a client-side connection leak.

Python client shutdown may end an active read normally or report a closed client,
depending on the read boundary; it must not cancel the consumer's task. Explicit
caller cancellation remains `CancelledError`. Go caller cancellation retains
`context.Canceled`; client closure terminates its streams. These tests assert owned
work completion, not process-wide task/goroutine counts, which include unrelated
runtime and test infrastructure. This is bounded stress, not a proof of leak freedom
under every workload. Sync Python transport boundaries remain covered by its existing
TLS tests; the new concurrent cancellation scenario exercises the asyncio client.

```sh
# Python checkout
uv run pytest tests/test_lifecycle_stress.py

# Go checkout
go test -race -run '^TestLifecycle' -count=1 -timeout=60s .
```

Individual synchronization waits have 3-second failure bounds, client operations
have 5-second deadlines, and stream reconnect delays start at 1 ms. These are hang
guards, not latency assertions. Normal suites run one six-cycle batch. For an explicit
longer investigation, Go supports `-count=10 -timeout=120s`; Python's command can be
repeated with a finite shell loop. Do not run unbounded soak tests in ordinary CI.

## Microbenchmarks

Four operations use the same existing fixtures in each repository:

| Operation | Input and timed work |
| --- | --- |
| Parse snapshot | `snapshot.json`, JSON decoding plus typed response validation |
| Parse tick | `tick.json`, the stream decoder with EURUSD selected |
| Sign GET | `signing-vector.json`, fixed timestamp, final path/query, no body |
| Sign POST | First vector in `mutation-signing-vectors.json`, fixed timestamp and exact UTF-8 body |

Fixture I/O, credential/key parsing and signature preflight checks happen outside
timing. Signing includes payload serialization, hashing, Ed25519 and header creation;
it never sends a request. Both signing preflights verify the existing expected
signature. No private material or signed headers appear in benchmark output.

```sh
# Python: dependency versions, input hashes and all samples in JSON
uv sync --locked
uv run python -m tools.benchmark --iterations 1000 --repeat 5

# Go: include allocations; fix count and CPU setting
go version
go test -run '^$' -bench '^(BenchmarkParse|BenchmarkSign)' \
  -benchtime=1000x -count=5 -cpu=1 .

# Record the revision alongside either report
git rev-parse HEAD
git status --short
```

Run the two commands sequentially on an otherwise idle machine. Keep the full
reports, toolchain/dependency versions, hardware, commit and fixture revisions when
comparing changes. Python uses `timeit` (GC disabled during timing); Go uses `b.Loop`
and reports allocations with normal Go GC behavior. The implementations and runtime
costs differ; these are baselines for changes within each SDK, not speed guarantees
or an end-to-end trading latency comparison. Use larger finite batches for an actual
optimization decision; 1000 iterations is only a convenient quick baseline.

Initial local reference: Apple M2, macOS 26.5 arm64; Python 3.11.15, Pydantic 2.14.3,
cryptography 46.0.7; Go 1.26.0. Median of three consecutive samples of 1000 iterations,
without coverage/race instrumentation (microseconds per operation):

| Operation | Python | Go |
| --- | ---: | ---: |
| Parse snapshot | 8.855 | 31.652 |
| Parse tick | 1.891 | 2.086 |
| Sign GET | 85.703 | 19.083 |
| Sign POST | 85.980 | 19.337 |

Go's snapshot samples varied from 28.454 to 45.850 microseconds; this illustrates why
these measurements are descriptive rather than CI thresholds. The Python report
includes SHA-256 hashes for all four fixture files; the shared-file parity job also
checks their byte equality against the pinned peer.

Python runs a two-iteration benchmark smoke test in pytest. Go CI runs all four
benchmarks with `-benchtime=1x` on Go 1.26. These verify that the benchmark paths still
execute and signing checks pass. Neither CI applies a wall-clock performance gate.
