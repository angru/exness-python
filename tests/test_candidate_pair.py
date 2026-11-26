"""Exercise candidate orchestration with local repositories and synthetic parity."""

import json
import subprocess
from pathlib import Path

import pytest

from tools import check_candidate_pair as candidate
from tools import check_parity as parity


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def checkout(root: Path) -> str:
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.email", "test@example.invalid")
    git(root, "config", "user.name", "Test")
    (root / "fixture").write_text("synthetic")
    git(root, "add", "fixture")
    git(root, "commit", "-qm", "fixture")
    return git(root, "rev-parse", "HEAD")


@pytest.mark.parametrize(
    "failure", ["none", "revision", "dirty", "drift", "late-dirty", "late-revision"]
)
def test_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    python, go = tmp_path / "python", tmp_path / "go"
    py_sha, go_sha = checkout(python), checkout(go)
    args = [
        "--python-root",
        str(python),
        "--go-root",
        str(go),
        "--python-revision",
        py_sha,
        "--go-revision",
        go_sha,
    ]
    if failure == "revision":
        args[-1] = "a" * 40
    if failure == "dirty":
        (python / "untracked").write_text("dirty")

    def fixtures(left: Path, right: Path) -> int:
        if failure == "drift":
            raise parity.ParityError("Shared file differs")
        return 46

    def behavior(*args: object) -> tuple[int, int]:
        if failure == "late-dirty":
            (go / "untracked").write_text("dirty")
        if failure == "late-revision":
            (python / "fixture").write_text("changed")
            git(python, "add", "fixture")
            git(python, "commit", "-qm", "changed")
        return 170, 49

    monkeypatch.setattr(parity, "compare_fixtures", fixtures)
    monkeypatch.setattr(parity, "compare_behavior", behavior)
    result = candidate.main(args)
    output = capsys.readouterr()
    if failure == "none":
        assert result == 0
        report = json.loads(output.out)
        assert report["heads"] == {"python": py_sha, "go": go_sha}
        assert report["proposed_pins"] == {
            "python/tools/parity-peer.json": {"revision": go_sha},
            "go/tools/parity-peer.json": {"revision": py_sha},
        }
        assert (
            report["shared_files"],
            report["validation_cases"],
            report["recovery_checkpoints"],
        ) == (46, 170, 49)
    else:
        assert result == 1
        assert output.out == ""
        assert "Candidate pair check failed" in output.err
