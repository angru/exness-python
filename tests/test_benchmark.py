import json

import pytest

from tools import benchmark


def test_benchmark_smoke(capsys: pytest.CaptureFixture[str]) -> None:
    benchmark.main(["--iterations", "2", "--repeat", "2"])
    output = capsys.readouterr().out
    report = json.loads(output)
    assert set(report["results"]) == {"parse_snapshot", "parse_tick", "sign_get", "sign_post"}
    assert len(report["fixture_sha256"]) == 4
    for result in report["results"].values():
        assert len(result["ns_per_op"]) == 2
        assert all(sample >= 0 for sample in result["ns_per_op"])
    assert "EXN-SIGN" not in output and "private_key" not in output


@pytest.mark.parametrize("args", [["--iterations", "0"], ["--repeat", "21"]])
def test_benchmark_invalid_budget(args: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        benchmark.main(args)
    assert error.value.code == 2
