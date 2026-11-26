"""Offline microbenchmarks with fixed synthetic inputs and no speed thresholds."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import timeit
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path

from exness import Credentials
from exness.auth import _Signer
from exness.models import TradingSnapshot
from exness.ticks import _tick

FIXTURES = Path(__file__).resolve().parents[1] / "tests/fixtures"


def operations() -> tuple[dict[str, Callable[[], object]], dict[str, str]]:
    raw = {
        name: (FIXTURES / f"{name}.json").read_bytes()
        for name in ("snapshot", "tick", "signing-vector", "mutation-signing-vectors")
    }
    vector = json.loads(raw["signing-vector"])
    signer = _Signer(
        Credentials(vector["api_key"], vector["private_key_hex"]),
        clock=lambda: vector["timestamp_ms"] / 1000,
    )
    mutations = json.loads(raw["mutation-signing-vectors"])
    post = mutations["vectors"][0]
    body = post["body_utf8"].encode()
    post_signer = _Signer(
        Credentials(mutations["api_key"], mutations["private_key_hex"]),
        clock=lambda: post["timestamp_ms"] / 1000,
    )
    calls: dict[str, Callable[[], object]] = {
        "parse_snapshot": lambda: TradingSnapshot.model_validate_json(raw["snapshot"]),
        "parse_tick": lambda: _tick(raw["tick"], ("EURUSD",), "benchmark"),
        "sign_get": lambda: signer.get_headers(vector["path"]),
        "sign_post": lambda: post_signer.request_headers(
            post["method"], post["path"], body=body, idempotency_key=post["idempotency_key"]
        ),
    }
    # Validate work before timing; never print keys, payloads or signed headers.
    assert signer.get_headers(vector["path"])["EXN-SIGN"] == vector["exn_sign"]
    assert (
        post_signer.request_headers(
            post["method"], post["path"], body=body, idempotency_key=post["idempotency_key"]
        )["EXN-SIGN"]
        == post["exn_sign"]
    )
    for call in calls.values():
        call()
    return calls, {name: hashlib.sha256(data).hexdigest() for name, data in raw.items()}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args(argv)
    if not 1 <= args.iterations <= 100_000 or not 1 <= args.repeat <= 20:
        parser.error("iterations must be 1..100000; repeat must be 1..20")
    calls, hashes = operations()
    results: dict[str, dict[str, object]] = {}
    for name, call in calls.items():
        samples = [
            elapsed * 1e9 / args.iterations
            for elapsed in timeit.repeat(call, number=args.iterations, repeat=args.repeat)
        ]
        results[name] = {"ns_per_op": samples, "median_ns_per_op": statistics.median(samples)}
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "dependencies": {name: version(name) for name in ("pydantic", "cryptography")},
                "iterations": args.iterations,
                "repeat": args.repeat,
                "fixture_sha256": hashes,
                "results": results,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
