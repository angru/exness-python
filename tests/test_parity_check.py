"""Fail closed on fixture, checkout and observed-result drift."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import check_parity as check

SHA = "a" * 40


def checkouts(tmp_path: Path) -> tuple[Path, Path]:
    python, go = tmp_path / "python", tmp_path / "go"
    for root, fixtures, recovery in (
        (python, "tests/fixtures", "examples/recovery_scenarios.json"),
        (go, "testdata", "examples/recover-trading/scenarios.json"),
    ):
        (root / fixtures).mkdir(parents=True)
        (root / "tools").mkdir()
        (root / recovery).parent.mkdir(parents=True)
        (root / fixtures / "sample.json").write_text('{"id":"001"}\n')
        (root / recovery).write_text("{}\n")
        (root / "tools/parity-fixtures.json").write_text('["sample.json"]\n')
        (root / "tools/check_parity.py").write_text("# checker\n")
    return python, go


@pytest.mark.parametrize("change", ["bytes", "missing", "extra", "recovery", "manifest", "checker"])
def test_fixture_drift(tmp_path: Path, change: str) -> None:
    python, go = checkouts(tmp_path)
    assert check.compare_fixtures(python, go) == 4
    path = go / "testdata/sample.json"
    if change == "missing":
        path.unlink()
    elif change == "extra":
        (go / "testdata/new.json").write_text("{}")
    else:
        path = {
            "bytes": path,
            "recovery": go / "examples/recover-trading/scenarios.json",
            "manifest": go / "tools/parity-fixtures.json",
            "checker": go / "tools/check_parity.py",
        }[change]
        path.write_text(path.read_text() + " ")
    with pytest.raises(check.ParityError, match=r"differs|inventory mismatch"):
        check.compare_fixtures(python, go)


@pytest.mark.parametrize("manifest", [[], ["sample.json", "sample.json"]])
def test_bad_manifest(tmp_path: Path, manifest: list[str]) -> None:
    python, go = checkouts(tmp_path)
    (python / "tools/parity-fixtures.json").write_text(json.dumps(manifest))
    with pytest.raises(check.ParityError, match="nonempty and unique"):
        check.compare_fixtures(python, go)


@pytest.mark.parametrize(
    "before,after",
    [(True, 1), ({"id": "01"}, {"id": "1"}), ({}, {"x": None}), ([1, 2], [2, 1])],
)
def test_normalization_retains_meaning(before: object, after: object) -> None:
    check.compare_results("same", {"a": 1, "b": 2}, {"b": 2, "a": 1})
    with pytest.raises(check.ParityError, match="scenario differs"):
        check.compare_results("scenario", before, after)


@pytest.mark.parametrize(
    "revision,head,dirty", [("main", SHA, ""), (SHA, "b" * 40, ""), (SHA, SHA, "?? file")]
)
def test_peer_revision_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, revision: str, head: str, dirty: str
) -> None:
    def run(command: list[str], root: Path, env: dict[str, str] | None = None) -> str:
        return head if command[1] == "rev-parse" else dirty

    monkeypatch.setattr(check, "run", run)
    with pytest.raises(check.ParityError):
        check.verify_peer(tmp_path, revision)
    if revision == SHA and head == SHA:
        dirty = ""
        check.verify_peer(tmp_path, SHA)


def test_process_failure_and_empty_reports(tmp_path: Path) -> None:
    assert check.run([sys.executable, "-c", "print('ok')"], tmp_path) == "ok\n"
    with pytest.raises(check.ParityError, match=r"failed.*2"):
        check.run([sys.executable, "-c", "raise SystemExit(2)"], tmp_path)
    assert check.read_rows('{"x": 1}\n\n') == [{"x": 1}]
    with pytest.raises(check.ParityError, match="empty"):
        check.read_rows("\n")


@pytest.mark.parametrize("drift", ["none", "validation", "recovery", "missing-report", "coverage"])
def test_behavior_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str) -> None:
    python, go = checkouts(tmp_path)
    (python / "tests/fixtures/response-validation.json").write_text(
        '[{"name":"case", "valid":true}]'
    )
    (python / "examples/recovery_scenarios.json").write_text(
        '{"scenarios":[{"name":"gap", "steps":[{"label":"reset", "expected":{"ready":false}}]}]}'
    )

    def run(command: list[str], root: Path, env: dict[str, str] | None = None) -> str:
        if env is not None:
            if drift != "missing-report":
                actual = {
                    "case": "protocol_error" if drift == "validation" and root == go else "accepted"
                }
                Path(env["EXNESS_PARITY_REPORT"]).write_text(json.dumps(actual))
            return "test passed"
        ready = drift == "recovery" and root == go
        return json.dumps(
            {
                "scenario": "gap",
                "step": "wrong" if drift == "coverage" else "reset",
                "rejected": False,
                "view": {"ready": ready},
            }
        )

    monkeypatch.setattr(check, "run", run)
    if drift == "none":
        assert check.compare_behavior(python, go, "uv", "go") == (1, 1)
    else:
        with pytest.raises((check.ParityError, FileNotFoundError)):
            check.compare_behavior(python, go, "uv", "go")


@pytest.mark.parametrize("peer", ["go", "python"])
def test_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], peer: str
) -> None:
    python, go = checkouts(tmp_path)
    verified: list[Path] = []

    def verify(root: Path, revision: str) -> None:
        verified.append(root)
        assert revision == SHA

    monkeypatch.setattr(check, "verify_peer", verify)
    monkeypatch.setattr(check, "compare_behavior", lambda *args: (170, 49))
    monkeypatch.setattr(check, "run", lambda *args: SHA)
    args = [
        "--python-root",
        str(python),
        "--go-root",
        str(go),
        "--peer",
        peer,
        "--peer-revision",
        SHA,
    ]
    assert check.main(args) == 0
    assert verified == [python if peer == "python" else go] * 2
    assert json.loads(capsys.readouterr().out)["status"] == "matched"

    def timeout(*args: object) -> tuple[int, int]:
        raise subprocess.TimeoutExpired("test", 300)

    monkeypatch.setattr(check, "compare_behavior", timeout)
    assert check.main(args) == 1
    assert "Parity check failed" in capsys.readouterr().err
