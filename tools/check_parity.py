"""Compare pinned SDK checkouts offline; no peer dependency in either SDK."""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


class ParityError(Exception):
    """A fixture, checkout or behavioral mismatch."""


def run(command: list[str], root: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command, cwd=root, env=env, text=True, capture_output=True, timeout=300, check=False
    )
    if result.returncode:
        raise ParityError(
            f"{root.name}: {' '.join(command)} failed ({result.returncode})\n"
            f"{result.stdout[-4000:]}{result.stderr[-4000:]}"
        )
    return result.stdout


def verify_peer(root: Path, revision: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ParityError("Peer revision must be a full lowercase 40-character commit SHA")
    head = run(["git", "rev-parse", "HEAD"], root).strip()
    if head != revision:
        raise ParityError(f"Peer HEAD mismatch: expected {revision}, got {head}")
    if run(["git", "status", "--porcelain", "--untracked-files=all"], root).strip():
        raise ParityError("Peer checkout must be clean (including untracked files)")


def compare_fixtures(python: Path, go: Path) -> int:
    manifest = "tools/parity-fixtures.json"
    shared: list[str] = json.loads((python / manifest).read_text())
    if not shared or len(shared) != len(set(shared)):
        raise ParityError("Shared fixture manifest must be nonempty and unique")
    for root, directory in ((python, "tests/fixtures"), (go, "testdata")):
        found = {
            str(path.relative_to(root / directory)) for path in (root / directory).rglob("*.json")
        }
        if found != set(shared):
            raise ParityError(
                f"{root.name}/{directory}: fixture inventory mismatch; "
                f"missing={sorted(set(shared) - found)}, unlisted={sorted(found - set(shared))}"
            )
    pairs = [(f"tests/fixtures/{name}", f"testdata/{name}") for name in shared]
    pairs += [
        ("examples/recovery_scenarios.json", "examples/recover-trading/scenarios.json"),
        (manifest, manifest),
        ("tools/check_parity.py", "tools/check_parity.py"),
    ]
    for left, right in pairs:
        if (python / left).read_bytes() != (go / right).read_bytes():
            raise ParityError(f"Shared file differs: python/{left} <> go/{right}")
    return len(pairs)


def compare_results(label: str, left: object, right: object) -> None:
    # Sorting object keys is the only generic normalization: retain scalar types,
    # array order, absent keys and decimal/identifier strings exactly.
    before = json.dumps(left, sort_keys=True, indent=2, allow_nan=False).splitlines()
    after = json.dumps(right, sort_keys=True, indent=2, allow_nan=False).splitlines()
    if before != after:
        difference = list(
            difflib.unified_diff(before, after, fromfile="expected/python", tofile="actual/go")
        )
        raise ParityError(f"{label} differs:\n" + "\n".join(difference[:80]))


def read_rows(output: str) -> list[Any]:
    rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    if not rows:
        raise ParityError("Recovery report is empty")
    return rows


def compare_behavior(python: Path, go: Path, uv: str, go_command: str) -> tuple[int, int]:
    with tempfile.TemporaryDirectory(prefix="exness-parity-") as temporary:
        reports = [Path(temporary) / "python.json", Path(temporary) / "go.json"]
        for root, destination, command in (
            (
                python,
                reports[0],
                [
                    uv,
                    "run",
                    "--locked",
                    "pytest",
                    "-q",
                    "-o",
                    "addopts=",
                    "tests/test_parity_report.py",
                ],
            ),
            (go, reports[1], [go_command, "test", "-count=1", "-run", "^TestParityReport$", "."]),
        ):
            env = {**os.environ, "EXNESS_PARITY_REPORT": str(destination)}
            run(command, root, env)
        decoded = [json.loads(path.read_text()) for path in reports]
    cases = json.loads((python / "tests/fixtures/response-validation.json").read_text())
    expected = {case["name"]: "accepted" if case["valid"] else "protocol_error" for case in cases}
    compare_results("Python validation coverage", expected, decoded[0])
    compare_results("Response validation", decoded[0], decoded[1])
    python_rows = read_rows(
        run([uv, "run", "--locked", "python", "-m", "examples.recover_trading"], python)
    )
    go_rows = read_rows(run([go_command, "run", "./examples/recover-trading"], go))
    suite = json.loads((python / "examples/recovery_scenarios.json").read_text())
    expected_rows = [
        {
            "scenario": scenario["name"],
            "step": step["label"],
            "rejected": step.get("expected_error", False),
            "view": step["expected"],
        }
        for scenario in suite["scenarios"]
        for step in scenario["steps"]
    ]
    compare_results("Python recovery coverage", expected_rows, python_rows)
    compare_results("Recovery checkpoints", python_rows, go_rows)
    return len(expected), len(expected_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-root", type=Path, required=True)
    parser.add_argument("--go-root", type=Path, required=True)
    parser.add_argument("--peer", choices=("python", "go"), required=True)
    parser.add_argument("--peer-revision", required=True)
    parser.add_argument("--uv", default="uv", help="uv executable path")
    parser.add_argument("--go", default="go", help="Go executable path")
    args = parser.parse_args(argv)
    python, go = args.python_root.resolve(), args.go_root.resolve()
    peer = python if args.peer == "python" else go
    try:
        verify_peer(peer, args.peer_revision)
        files = compare_fixtures(python, go)
        cases, checkpoints = compare_behavior(python, go, args.uv, args.go)
        # Ensure tools did not modify the pinned checkout during execution.
        verify_peer(peer, args.peer_revision)
        heads = {
            name: run(["git", "rev-parse", "HEAD"], root).strip()
            for name, root in (("python", python), ("go", go))
        }
        dirty = {
            name: bool(run(["git", "status", "--porcelain", "--untracked-files=all"], root).strip())
            for name, root in (("python", python), ("go", go))
        }
    except (ParityError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"Parity check failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": "matched",
                "heads": heads,
                "dirty": dirty,
                "shared_files": files,
                "validation_cases": cases,
                "recovery_checkpoints": checkpoints,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
