"""Validate two clean local SDK revisions and propose their counterpart pins."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from tools import check_parity as parity


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-root", type=Path, required=True)
    parser.add_argument("--go-root", type=Path, required=True)
    parser.add_argument("--python-revision", required=True)
    parser.add_argument("--go-revision", required=True)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--go", default="go")
    args = parser.parse_args(argv)
    python, go = args.python_root.resolve(), args.go_root.resolve()
    candidates = (("Python", python, args.python_revision), ("Go", go, args.go_revision))

    def verify() -> None:
        errors = []
        for name, root, revision in candidates:
            try:
                parity.verify_peer(root, revision)
            except (parity.ParityError, OSError, subprocess.TimeoutExpired) as error:
                errors.append(f"{name}: {error}")
        if errors:
            raise parity.ParityError("; ".join(errors))

    try:
        verify()
        try:
            files = parity.compare_fixtures(python, go)
            cases, checkpoints = parity.compare_behavior(python, go, args.uv, args.go)
        finally:
            verify()
    except (parity.ParityError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"Candidate pair check failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": "matched",
                "heads": {"python": args.python_revision, "go": args.go_revision},
                "shared_files": files,
                "validation_cases": cases,
                "recovery_checkpoints": checkpoints,
                "proposed_pins": {
                    "python/tools/parity-peer.json": {"revision": args.go_revision},
                    "go/tools/parity-peer.json": {"revision": args.python_revision},
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
