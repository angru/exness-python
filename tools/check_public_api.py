"""Compare reviewed public declarations with the committed offline baseline."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tools" / "public-api-baseline.json"
MODULES = (
    "__init__",
    "client",
    "async_client",
    "auth",
    "errors",
    "models",
    "requests",
    "operations",
    "ticks",
    "trading",
    "events",
)
PRIVATE_BASES = {"_Request", "_PendingOrder"}


def expression(node: ast.AST | None) -> str | None:
    return ast.unparse(node) if node is not None else None


def signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, Any]:
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults)) + list(
        args.defaults
    )
    params = []
    for index, (arg, default) in enumerate(zip(positional, defaults, strict=True)):
        params.append(
            {
                "name": arg.arg,
                "kind": "posonly" if index < len(args.posonlyargs) else "positional",
                "type": expression(arg.annotation),
                "default": expression(default),
            }
        )
    if args.vararg:
        params.append(
            {
                "name": args.vararg.arg,
                "kind": "vararg",
                "type": expression(args.vararg.annotation),
                "default": None,
            }
        )
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        params.append(
            {
                "name": arg.arg,
                "kind": "keyword",
                "type": expression(arg.annotation),
                "default": expression(default),
            }
        )
    if args.kwarg:
        params.append(
            {
                "name": args.kwarg.arg,
                "kind": "varkw",
                "type": expression(args.kwarg.annotation),
                "default": None,
            }
        )
    return {
        "kind": "async" if isinstance(node, ast.AsyncFunctionDef) else "function",
        "params": params,
        "return": expression(node.returns),
        "decorators": [expression(d) for d in node.decorator_list],
    }


def declarations(sources: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for module in MODULES:
        tree = ast.parse(sources[module], filename=f"exness/{module}.py")
        prefix = "exness" if module == "__init__" else f"exness.{module}"
        for node in tree.body:
            if module == "__init__" and isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    result[f"{prefix}.{alias.asname or alias.name}"] = {
                        "kind": "export",
                        "target": f"{node.module}.{alias.name}",
                    }
            elif isinstance(node, ast.ClassDef) and (
                not node.name.startswith("_") or node.name in PRIVATE_BASES
            ):
                name = f"{prefix}.{node.name}"
                result[name] = {
                    "kind": "class",
                    "bases": [expression(b) for b in node.bases],
                    "decorators": [expression(d) for d in node.decorator_list],
                }
                for member in node.body:
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                        not member.name.startswith("_")
                        or member.name
                        in {"__init__", "__enter__", "__exit__", "__aenter__", "__aexit__"}
                    ):
                        result[f"{name}.{member.name}"] = signature(member)
                    elif (
                        isinstance(member, ast.AnnAssign)
                        and isinstance(member.target, ast.Name)
                        and not member.target.id.startswith("_")
                    ):
                        result[f"{name}.{member.target.id}"] = {
                            "kind": "field",
                            "type": expression(member.annotation),
                            "default": expression(member.value),
                        }
            elif isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)
            ) and not node.name.startswith("_"):
                result[f"{prefix}.{node.name}"] = signature(node)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and (
                        target.id == "__all__"
                        or (not target.id.startswith("_") and target.id[0].isupper())
                    ):
                        result[f"{prefix}.{target.id}"] = {
                            "kind": "alias",
                            "value": expression(node.value),
                        }
    return result


def compare(baseline: dict[str, Any], current: dict[str, Any]) -> list[str]:
    problems = []
    for name, old in sorted(baseline.items()):
        new = current.get(name)
        if new is None:
            problems.append(f"{name}: removed")
        elif old.get("kind") in {"function", "async"} and new.get("kind") == old["kind"]:
            if old["return"] != new["return"] or old["decorators"] != new["decorators"]:
                problems.append(f"{name}: return type or decorators changed")
            old_params = old["params"]
            new_params = new["params"]
            # Existing parameters retain their order, kind, annotations and defaults.
            retained = [p for p in new_params if p["name"] in {q["name"] for q in old_params}]
            if retained != old_params:
                problems.append(f"{name}: parameters changed")
            for param in new_params:
                if param["name"] not in {p["name"] for p in old_params} and (
                    param["kind"] != "keyword" or param["default"] is None
                ):
                    problems.append(f"{name}: incompatible new parameter {param['name']}")
        elif name == "exness.__all__" and new.get("kind") == "alias":
            old_names = set(ast.literal_eval(old["value"]))
            try:
                new_names = set(ast.literal_eval(new["value"]))
            except (ValueError, TypeError, SyntaxError):
                problems.append("exness.__all__: expected a literal export list")
            else:
                for removed in sorted(old_names - new_names):
                    problems.append(f"exness.{removed}: removed from __all__")
        elif old != new:
            problems.append(f"{name}: declaration changed")
    for name, item in sorted(current.items()):
        if (
            name not in baseline
            and name.rpartition(".")[0] in baseline
            and item.get("kind") == "field"
            and item["default"] is None
        ):
            problems.append(f"{name}: new required field")
    return sorted(problems)


def main() -> int:
    baseline = json.loads(BASELINE.read_text())
    sources = {module: (ROOT / "src" / "exness" / f"{module}.py").read_text() for module in MODULES}
    problems = compare(baseline["declarations"], declarations(sources))
    if problems:
        print("Public API compatibility failures:")
        print("\n".join(problems))
        return 1
    print(f"Public API compatible with {baseline['revision']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
