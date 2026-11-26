# Offline public API compatibility

Run `uv run python -m tools.check_public_api` from the repository root. It
compares source declarations with `tools/public-api-baseline.json`, extracted
from Python SDK main commit `1cedf7d12b9977e2d7a4204b9e12ba0d9733d1c2`.
The command needs no Git history, network, peer SDK, credentials or baseline
regeneration. CI runs it independently of the cross-SDK parity job.

The reviewed scope covers root exports and their import targets, `__all__`,
both clients and the public classes, methods, functions, fields and uppercase
type aliases in auth, errors, models, requests, operations, ticks, trading and
events. The private `_Request` and `_PendingOrder` bases are included because
their inherited fields affect public request types. Public class bases,
decorators, annotations, field defaults, signatures and alias expressions are
tracked. Existing declarations must remain; new classes, exports, optional
fields and optional keyword-only parameters are accepted. A new required field
on an existing type is rejected. Existing parameter order, placement, type and
default must remain unchanged; even a potentially safe relaxation requires
review. New positional parameters are rejected because they can change binding.
These conservative decisions avoid claiming type variance or Pydantic model
construction compatibility that a syntax comparison cannot prove.

Diagnostics use qualified symbol names and are sorted. This is a source-shape
gate, not a behavioral, wire-contract or complete Python type-system proof.
It does not evaluate annotations, imports within submodules, validators,
method bodies, dynamic exports, inherited third-party members or runtime
monkeypatches. Syntactically different but equivalent types can be reported as
changes. Review default factories and optional response fields for semantic
impact even when a declaration addition passes. Installed-wheel consumer tests,
contract tests and sync/async parity remain separate checks.

Never refresh the baseline automatically in CI or to silence a failure. For an
intentional breaking change, review consumer impact, release policy and both SDK
surfaces, then explicitly approve the new baseline revision and its complete
snapshot diff in the same review. Generate declarations using the checker
against the chosen committed source, record provenance here, and run the gate
and consumer tests. The baseline revision must identify the actual source used;
the checker deliberately offers no automatic update command.
