"""Review upstream OpenAPI drift without modifying the pinned contract.

Run from this checkout: uv run python -m tools.check_contract --help
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TypeAlias, cast

import httpx
import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

SOURCE_URL = "https://www.exness-api.com/openapi/trading.public.yaml"
BASELINE = Path(__file__).resolve().parents[1] / "contracts" / "trading.public.yaml"
MAX_BYTES = 8 * 1024 * 1024
MAX_NODES = 100_000
METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options", "trace"})
JSON: TypeAlias = bool | int | float | str | list["JSON"] | dict[str, "JSON"] | None


class ContractError(Exception):
    """Invalid input or an unsuccessful upstream check."""


@dataclass(frozen=True)
class Change:
    kind: str
    area: str
    pointer: str
    before: JSON
    after: JSON


def _pointer(parts: tuple[str, ...]) -> str:
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in parts)


def _area(parts: tuple[str, ...]) -> str:
    if parts and parts[0] == "paths":
        if "parameters" in parts[2:]:
            return "parameters"
        return "operations"
    if len(parts) >= 2 and parts[0] == "components":
        return "models" if parts[1] == "schemas" else f"components.{parts[1]}"
    return parts[0] if parts else "document"


def parse_contract(data: bytes) -> dict[str, JSON]:
    """Parse JSON-compatible YAML without implicit YAML 1.1 bool/date conversion.

    BaseLoader preserves scalar spelling. Tags are interpreted here using JSON
    scalar rules; mapping keys stay strings (including response status codes).
    Aliases, duplicate keys, custom tags and excessive nesting are rejected.
    This is a bounded structural check, not a complete OpenAPI validator.
    """
    if len(data) > MAX_BYTES:
        raise ContractError("Contract exceeds the 8 MiB input limit")
    try:
        # Reject aliases before composing to avoid cycles/expansion and ambiguous ownership.
        depth = 0
        for events, event in enumerate(yaml.parse(data, Loader=yaml.BaseLoader), 1):
            if isinstance(event, (yaml.MappingStartEvent, yaml.SequenceStartEvent)):
                depth += 1
            elif isinstance(event, (yaml.MappingEndEvent, yaml.SequenceEndEvent)):
                depth -= 1
            if events > MAX_NODES * 2 or depth > 80:
                raise ContractError("Contract exceeds the nesting or node limit")
            if isinstance(event, (yaml.ScalarEvent, yaml.CollectionStartEvent)) and event.tag:
                raise ContractError("Explicit YAML tags are unsupported")
            if isinstance(event, yaml.AliasEvent):
                raise ContractError("YAML aliases are unsupported; provide an expanded contract")
        root = yaml.compose(data, Loader=yaml.BaseLoader)
        budget = [MAX_NODES]

        def convert(node: Node, depth: int = 0) -> JSON:
            budget[0] -= 1
            if depth > 80 or budget[0] < 0:
                raise ContractError("Contract exceeds the nesting or node limit")
            if not node.tag.startswith("tag:yaml.org,2002:"):
                raise ContractError("Custom YAML tags are unsupported")
            if isinstance(node, MappingNode):
                result: dict[str, JSON] = {}
                for key, value in node.value:
                    if not isinstance(key, ScalarNode):
                        raise ContractError("Contract mapping keys must be scalar strings")
                    if key.value in result:
                        raise ContractError("Contract contains a duplicate mapping key")
                    result[key.value] = convert(value, depth + 1)
                return result
            if isinstance(node, SequenceNode):
                return [convert(value, depth + 1) for value in node.value]
            if not isinstance(node, ScalarNode):
                raise ContractError("Unsupported YAML node")
            if node.style is not None:
                return str(node.value)
            if node.value in ("", "null", "Null", "NULL", "~"):
                return None
            try:
                scalar: object = json.loads(node.value)
            except (ValueError, RecursionError):
                return str(node.value)
            if scalar is None or isinstance(scalar, (bool, int, str)):
                return scalar
            if isinstance(scalar, float) and math.isfinite(scalar):
                return scalar
            raise ContractError("Expected a finite JSON scalar")

        document = convert(root) if root is not None else None
    except (yaml.YAMLError, UnicodeError, RecursionError, ValueError) as error:
        raise ContractError("Invalid YAML/JSON contract") from error
    if not isinstance(document, dict):
        raise ContractError("Contract must be an OpenAPI object")
    version, info, paths = document.get("openapi"), document.get("info"), document.get("paths")
    if not isinstance(version, str) or not version.startswith("3."):
        raise ContractError("Expected OpenAPI 3.x")
    if not isinstance(info, dict) or not isinstance(info.get("version"), str):
        raise ContractError("Contract info.version must be a string")
    if not isinstance(paths, dict):
        raise ContractError("Contract paths must be an object")
    for path, item in paths.items():
        if path.startswith("x-"):
            continue
        if not path.startswith("/") or not isinstance(item, dict):
            raise ContractError("Invalid OpenAPI path item")
        for method, operation in item.items():
            if method in METHODS and not isinstance(operation, dict):
                raise ContractError("Invalid OpenAPI operation")
    return document


def compare(before: JSON, after: JSON, parts: tuple[str, ...] = ()) -> list[Change]:
    """Ignore mapping order, preserve all values and array order conservatively."""
    if isinstance(before, dict) and isinstance(after, dict):
        changes: list[Change] = []
        for key in sorted(before.keys() | after.keys()):
            location = (*parts, key)
            if key not in before:
                changes.append(
                    Change("added", _area(location), _pointer(location), None, after[key])
                )
            elif key not in after:
                changes.append(
                    Change("removed", _area(location), _pointer(location), before[key], None)
                )
            else:
                changes.extend(compare(before[key], after[key], location))
        return changes
    # JSON booleans and numbers must not compare equal (True == 1 in Python).
    if json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True):
        return []
    return [Change("changed", _area(parts), _pointer(parts), before, after)]


def operation_names(document: dict[str, JSON]) -> list[str]:
    paths = document["paths"]
    assert isinstance(paths, dict)
    return sorted(
        f"{method.upper()} {path}"
        for path, item in paths.items()
        if path.startswith("/") and isinstance(item, dict)
        for method in item
        if method in METHODS
    )


def make_report(
    before_bytes: bytes, after_bytes: bytes, *, include_impact: bool = False
) -> dict[str, JSON]:
    before, after = parse_contract(before_bytes), parse_contract(after_bytes)
    changes = compare(before, after)
    old_operations, new_operations = set(operation_names(before)), set(operation_names(after))
    report: dict[str, JSON] = {
        "report_version": 1,
        "status": "review_required" if changes else "unchanged",
        "baseline_sha256": hashlib.sha256(before_bytes).hexdigest(),
        "candidate_sha256": hashlib.sha256(after_bytes).hexdigest(),
        "bytes_changed": before_bytes != after_bytes,
        "baseline_api_version": cast(dict[str, JSON], before["info"])["version"],
        "candidate_api_version": cast(dict[str, JSON], after["info"])["version"],
        "operations_added": list[JSON](sorted(new_operations - old_operations)),
        "operations_removed": list[JSON](sorted(old_operations - new_operations)),
        "changes": [dict(asdict(change)) for change in changes],
        "limitations": [
            "Structural review only; not a proof of compatibility or full OpenAPI validation.",
            "Array ordering, documentation and examples are retained and may produce review noise.",
            "$ref values are not dereferenced; shared model changes need review in both SDKs.",
            "Discovery/signing guides outside this YAML require separate review.",
        ],
    }
    if include_impact:
        from tools.contract_impact import impact

        report["impact"] = impact(before, after, changes)
    return report


def read_file(path: Path) -> bytes:
    with path.open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ContractError("Contract exceeds the 8 MiB input limit")
    return data


def download(transport: httpx.BaseTransport | None = None) -> bytes:
    """Fetch the fixed public source with no credentials, proxies, redirects or retries."""
    try:
        with (
            httpx.Client(
                timeout=20, follow_redirects=False, trust_env=False, transport=transport
            ) as client,
            client.stream("GET", SOURCE_URL) as response,
        ):
            if response.status_code != 200:
                raise ContractError(f"Upstream returned HTTP {response.status_code}")
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_bytes(chunk_size=65536):
                size += len(chunk)
                if size > MAX_BYTES:
                    raise ContractError("Contract exceeds the 8 MiB download limit")
                chunks.append(chunk)
            return b"".join(chunks)
    except httpx.RequestError:
        raise ContractError("Unable to download the upstream contract") from None


def _preview(value: JSON) -> str:
    text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    return text if len(text) <= 160 else text[:157] + "..."


def render(report: dict[str, JSON]) -> str:
    lines = [
        f"Contract check: {report['status']}",
        f"Baseline SHA-256: {report['baseline_sha256']}",
        f"Candidate SHA-256: {report['candidate_sha256']}",
        f"API version: {_preview(report['baseline_api_version'])} -> "
        f"{_preview(report['candidate_api_version'])}",
        f"Operations added: {_preview(report['operations_added'])}",
        f"Operations removed: {_preview(report['operations_removed'])}",
    ]
    if report["status"] == "unchanged" and report["bytes_changed"]:
        lines.append("Only YAML formatting, comments or mapping order changed.")
    changes = report["changes"]
    assert isinstance(changes, list)
    for change in changes:
        assert isinstance(change, dict)
        # JSON escaping keeps control characters in upstream text out of the terminal.
        lines.append(f"[{_preview(change['area'])}] {change['kind']} {_preview(change['pointer'])}")
        lines.append(f"  before: {_preview(change['before'])}")
        lines.append(f"  after:  {_preview(change['after'])}")
    if "impact" in report:
        lines.append(
            "Impact triage: " + json.dumps(report["impact"], ensure_ascii=True, sort_keys=True)
        )
    lines.append(
        f"{len(changes)} structural change(s). Review both SDKs; compatibility is not inferred."
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=BASELINE, help="Pinned YAML/JSON snapshot")
    parser.add_argument(
        "--candidate", type=Path, help="Local YAML/JSON candidate; otherwise fetch the official URL"
    )
    parser.add_argument("--json-report", type=Path, help="Write a full report to a NEW file")
    parser.add_argument(
        "--save-candidate", type=Path, help="Save candidate bytes to a NEW file for review"
    )
    parser.add_argument("--impact", action="store_true", help="Include optional SDK impact triage")
    args = parser.parse_args(argv)
    try:
        outputs = [path for path in (args.json_report, args.save_candidate) if path is not None]
        if len({path.resolve() for path in outputs}) != len(outputs):
            raise ContractError("Output paths must be distinct")
        for path in outputs:
            if path.exists() or path.is_symlink() or path.resolve() == BASELINE:
                raise ContractError(
                    "Output files must be new; the pinned snapshot is never overwritten"
                )
        baseline = read_file(args.baseline)
        candidate = read_file(args.candidate) if args.candidate else download()
        report = make_report(baseline, candidate, include_impact=args.impact)
        report["source"] = str(args.candidate) if args.candidate else SOURCE_URL
        if args.json_report:
            with args.json_report.open("x", encoding="utf-8") as stream:
                stream.write(json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2) + "\n")
        if args.save_candidate:
            with args.save_candidate.open("xb") as stream:
                stream.write(candidate)
        print(render(report))
        return 1 if report["status"] == "review_required" else 0
    except (ContractError, OSError) as error:
        print(f"Contract check failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
