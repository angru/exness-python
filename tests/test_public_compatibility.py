"""Exercise the gate against actual reviewed source declarations."""

import json
from pathlib import Path

import pytest

import tools.check_public_api as checker
from tools.check_public_api import BASELINE, MODULES, ROOT, compare, declarations


def sources() -> dict[str, str]:
    return {name: (ROOT / "src" / "exness" / f"{name}.py").read_text() for name in MODULES}


def test_baseline_matches_current_source() -> None:
    baseline = json.loads(BASELINE.read_text())
    assert baseline["revision"] == "1cedf7d12b9977e2d7a4204b9e12ba0d9733d1c2"
    assert compare(baseline["declarations"], declarations(sources())) == []


def test_matching_client_break_is_detected() -> None:
    original = sources()
    changed = original.copy()
    for module in ("client", "async_client"):
        assert "def get_account(" in changed[module]
        changed[module] = changed[module].replace("def get_account(", "def renamed_account(", 1)
    errors = compare(declarations(original), declarations(changed))
    assert "exness.client.ExnessClient.get_account: removed" in errors
    assert "exness.async_client.AsyncExnessClient.get_account: removed" in errors


def test_compatible_keyword_and_export_additions() -> None:
    original = sources()
    changed = original.copy()
    # Add a keyword-only argument to a real declaration and an exported alias.
    changed["client"] = changed["client"].replace(
        "def get_account(self)", "def get_account(self, *, extra: int = 0)", 1
    )
    assert changed["client"] != original["client"]
    changed["__init__"] = changed["__init__"].replace(
        '    "APIError",', '    "APIError",\n    "NewAlias",'
    )
    changed["__init__"] += "\nNewAlias = ExnessClient\n"
    assert compare(declarations(original), declarations(changed)) == []


def test_type_and_required_parameter_breaks() -> None:
    original = sources()
    changed = original.copy()
    changed["client"] = changed["client"].replace(
        "def get_account(self)", "def get_account(self, required: int)", 1
    )
    changed["models"] = changed["models"].replace(
        "class AccessPoint(APIModel):", "class AccessPoint(APIModel):\n    added: str", 1
    )
    errors = compare(declarations(original), declarations(changed))
    assert any(
        e == "exness.client.ExnessClient.get_account: incompatible new parameter required"
        for e in errors
    )
    assert "exness.models.AccessPoint.added: new required field" in errors


def test_removed_type_is_reported() -> None:
    # Comparison diagnostics name the declaration independently of source locations.
    original = sources()
    changed = original.copy()
    changed["models"] = changed["models"].replace(
        "class AccessPoint(APIModel):", "class RenamedAccessPoint(APIModel):", 1
    )
    assert "exness.models.AccessPoint: removed" in compare(
        declarations(original), declarations(changed)
    )


def test_return_type_and_parameter_placement_changes() -> None:
    original = sources()
    changed = original.copy()
    changed["client"] = changed["client"].replace(
        "def get_account(self) -> AccountDetails:",
        "def get_account(self) -> str:",
        1,
    )
    assert changed["client"] != original["client"]
    assert "exness.client.ExnessClient.get_account: return type or decorators changed" in compare(
        declarations(original), declarations(changed)
    )
    old = {
        "f": {
            "kind": "function",
            "return": "int",
            "decorators": [],
            "params": [{"name": "x", "kind": "positional", "type": "int", "default": None}],
        }
    }
    new = {
        "f": {
            "kind": "function",
            "return": "int",
            "decorators": [],
            "params": [{"name": "x", "kind": "keyword", "type": "int", "default": None}],
        }
    }
    assert compare(old, new) == ["f: parameters changed"]


def test_command_on_broken_and_compatible_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source_dir = tmp_path / "src" / "exness"
    source_dir.mkdir(parents=True)
    for module, source in sources().items():
        (source_dir / f"{module}.py").write_text(source)
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    client = source_dir / "client.py"
    original = client.read_text()
    client.write_text(original.replace("def get_account(", "def removed_account(", 1))
    assert checker.main() == 1
    assert "exness.client.ExnessClient.get_account: removed" in capsys.readouterr().out
    client.write_text(
        original.replace("def get_account(self)", "def get_account(self, *, extra: int = 0)", 1)
    )
    assert checker.main() == 0
    assert "Public API compatible" in capsys.readouterr().out
