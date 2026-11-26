import copy
import json
from pathlib import Path

import httpx
import pytest

from tools import check_contract as check

SIMPLE = b"""openapi: 3.1.1
info: {title: Test API, version: "1.0"}
paths:
  /orders:
    get:
      operationId: getOrders
      parameters: [{name: limit, in: query, schema: {type: integer, maximum: 200}}]
      responses:
        200:
          description: OK
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Order'}
components:
  schemas:
    Order:
      type: object
      required: [id]
      properties:
        id: {type: string}
        state: {type: string, enum: [on, off]}
"""


def test_pinned_contract_and_formatting_only() -> None:
    original = check.read_file(check.BASELINE)
    parsed = check.parse_contract(original)
    reformatted = json.dumps(parsed, sort_keys=True).encode()
    report = check.make_report(original, reformatted)
    assert report["status"] == "unchanged"
    assert report["bytes_changed"] is True
    assert (
        report["baseline_sha256"]
        == "ebde19011ffbd30240e918f9369c8233fc9c5a4afb27eca0462b40a141d0f1b3"
    )
    assert "Only YAML formatting" in check.render(report)


def test_operation_parameter_and_shared_model_changes() -> None:
    before = json.loads(json.dumps(check.parse_contract(SIMPLE)))
    after = copy.deepcopy(before)
    after["paths"]["/orders"]["get"]["parameters"][0]["schema"]["maximum"] = 100
    after["paths"]["/orders"]["post"] = {"responses": {"200": {"description": "OK"}}}
    after["components"]["schemas"]["Order"]["required"].append("state")
    after["components"]["schemas"]["Order"]["properties"]["state"]["enum"] = [
        "on",
        "off",
        "pending",
    ]
    after["components"]["securitySchemes"] = {
        "key": {"type": "apiKey", "in": "header", "name": "EXN-KEY"}
    }
    report = check.make_report(json.dumps(before).encode(), json.dumps(after).encode())
    assert report["status"] == "review_required"
    assert report["operations_added"] == ["POST /orders"]
    changes = report["changes"]
    assert isinstance(changes, list)
    areas = {change["area"] for change in changes if isinstance(change, dict)}
    assert areas == {"parameters", "models", "operations", "components.securitySchemes"}
    assert len(changes) == 5
    assert check.make_report(json.dumps(after).encode(), json.dumps(before).encode())[
        "operations_removed"
    ] == ["POST /orders"]


def test_refs_metadata_and_property_named_description_are_not_hidden() -> None:
    before = check.parse_contract(SIMPLE)
    after = json.loads(json.dumps(before))
    after["info"]["description"] = "New signing rule"
    after["components"]["schemas"]["Order"]["properties"]["description"] = {"type": "integer"}
    after["paths"]["/orders"]["get"]["responses"]["200"]["content"]["application/json"]["schema"][
        "$ref"
    ] = "https://example.test/external.json"
    report = check.make_report(SIMPLE, json.dumps(after).encode())
    assert report["status"] == "review_required"
    assert isinstance(report["changes"], list) and len(report["changes"]) == 3


@pytest.mark.parametrize(
    "before,after",
    [(True, 1), ([True], [1]), ({"x": True}, {"x": 1}), (None, "null"), ([1, 2], [2, 1])],
)
def test_value_types_and_array_order_matter(before: check.JSON, after: check.JSON) -> None:
    assert check.compare(before, after)


def test_pointer_escaping_and_terminal_controls() -> None:
    changes = check.compare({"a~/b": {"x\x1b": 1}}, {"a~/b": {"x\x1b": 2}})
    assert changes[0].pointer == "/a~0~1b/x\x1b"
    report = check.make_report(SIMPLE, SIMPLE.replace(b"Test API", b"Test API changed"))
    report["changes"] = [
        {
            "kind": "changed",
            "area": "models",
            "pointer": "/x\x1b",
            "before": "a" * 200,
            "after": None,
        }
    ]
    text = check.render(report)
    assert "\x1b" not in text and "..." in text


@pytest.mark.parametrize(
    "bad",
    [
        b"",
        b"[]",
        b"<html>error</html>",
        b"paths: [",
        b"\xff",
        b"openapi: 3.1.1\nopenapi: 3.1.0",
        b"x: &a [*a]",
        b"x: !!python/object/apply:os.system ['echo bad']",
        b"x: !!str 123",
        b"{[a,b]: 1}",
        b"x: NaN",
        b"x: 1e999",
        b"openapi: 2.0\ninfo: {version: '1'}\npaths: {}",
        b"openapi: 3.1.1\ninfo: {version: 1}\npaths: {}",
        b"openapi: 3.1.1\ninfo: {version: '1'}\npaths: []",
        b"openapi: 3.1.1\ninfo: {version: '1'}\npaths: {bad: {}}",
        b"openapi: 3.1.1\ninfo: {version: '1'}\npaths: {/a: {get: false}}",
        b"x: " + b"[" * 85 + b"0" + b"]" * 85,
    ],
)
def test_invalid_inputs_fail(bad: bytes) -> None:
    with pytest.raises(check.ContractError):
        check.parse_contract(bad)


def test_yaml_json_scalar_types() -> None:
    data = check.parse_contract(
        SIMPLE
        + b"x-example:\n  yes: on\n  no: off\n  date: 2026-11-19\n"
        + b"  empty: null\n  enabled: true\n  quoted: 'true'\n  ratio: 1.5\n  text: |\n    42\n"
    )
    assert data["x-example"] == {
        "yes": "on",
        "no": "off",
        "date": "2026-11-19",
        "empty": None,
        "enabled": True,
        "quoted": "true",
        "ratio": 1.5,
        "text": "42\n",
    }


def test_size_and_node_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(check, "MAX_BYTES", 10)
    with pytest.raises(check.ContractError):
        check.parse_contract(SIMPLE)
    path = tmp_path / "large.yaml"
    path.write_bytes(SIMPLE)
    with pytest.raises(check.ContractError):
        check.read_file(path)
    monkeypatch.setattr(check, "MAX_BYTES", 10000)
    monkeypatch.setattr(check, "MAX_NODES", 5)
    with pytest.raises(check.ContractError):
        check.parse_contract(SIMPLE)


@pytest.mark.parametrize("status", [301, 429, 500])
def test_download_errors_do_not_follow_or_retry(status: int) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert str(request.url) == check.SOURCE_URL
        assert not any(key.lower().startswith("exn-") for key in request.headers)
        return httpx.Response(status, headers={"Location": "https://elsewhere.example"})

    with pytest.raises(check.ContractError, match=f"HTTP {status}"):
        check.download(httpx.MockTransport(handler))
    assert len(requests) == 1


def test_download_success_size_limit_and_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    assert (
        check.download(httpx.MockTransport(lambda request: httpx.Response(200, content=SIMPLE)))
        == SIMPLE
    )
    monkeypatch.setattr(check, "MAX_BYTES", 10)
    with pytest.raises(check.ContractError, match="limit"):
        check.download(httpx.MockTransport(lambda request: httpx.Response(200, content=SIMPLE)))

    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("private diagnostic")

    with pytest.raises(check.ContractError, match="Unable to download") as caught:
        check.download(httpx.MockTransport(fail))
    assert "private" not in str(caught.value)


def test_cli_outputs_exit_codes_and_no_overwrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline, candidate = tmp_path / "baseline.yaml", tmp_path / "candidate.yaml"
    baseline.write_bytes(SIMPLE)
    candidate.write_bytes(SIMPLE.replace(b"maximum: 200", b"maximum: 100"))
    report, saved = tmp_path / "report.json", tmp_path / "saved.yaml"
    args = ["--baseline", str(baseline), "--candidate", str(candidate)]
    assert check.main([*args, "--json-report", str(report), "--save-candidate", str(saved)]) == 1
    assert json.loads(report.read_text())["status"] == "review_required"
    assert saved.read_bytes() == candidate.read_bytes()
    assert baseline.read_bytes() == SIMPLE
    assert check.main([*args, "--save-candidate", str(baseline)]) == 2
    assert baseline.read_bytes() == SIMPLE
    assert check.main([*args, "--json-report", str(report)]) == 2
    assert check.main(["--baseline", str(baseline), "--candidate", str(baseline)]) == 0
    candidate.write_bytes(b"bad upstream response")
    assert check.main(args) == 2
    assert "failed" in capsys.readouterr().err


def test_cli_online_path_and_output_conflicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(check, "download", lambda: SIMPLE)
    baseline = tmp_path / "baseline.yaml"
    baseline.write_bytes(SIMPLE)
    assert check.main(["--baseline", str(baseline)]) == 0
    output = tmp_path / "new.json"
    assert (
        check.main(
            [
                "--baseline",
                str(baseline),
                "--save-candidate",
                str(output),
                "--json-report",
                str(output),
            ]
        )
        == 2
    )
    assert not output.exists()
    assert check.main(["--baseline", str(tmp_path / "missing")]) == 2


def test_impact_transitive_cycle_escaping_and_operation_changes() -> None:
    before = check.parse_contract(SIMPLE)
    paths = before["paths"]
    assert isinstance(paths, dict)
    paths["/a~/b"] = {
        "parameters": [{"$ref": "#/components/schemas/A~1B"}],
        "post": {"operationId": "unknown"},
    }
    components = before["components"]
    assert isinstance(components, dict) and isinstance(components["schemas"], dict)
    schemas = components["schemas"]
    schemas["A/B"] = {"$ref": "#/components/schemas/C~0D"}
    schemas["C~D"] = {"allOf": [{"$ref": "#/components/schemas/A~1B"}], "type": "string"}
    after = copy.deepcopy(before)
    after_paths = after["paths"]
    assert isinstance(after_paths, dict)
    del after_paths["/orders"]
    after_paths["/new"] = {"get": {"operationId": "newOperation"}}
    after_schemas = after["components"]["schemas"]  # type: ignore[index,call-overload]
    after_schemas["C~D"]["type"] = "integer"  # type: ignore[index,call-overload]
    report = check.make_report(
        json.dumps(before).encode(), json.dumps(after).encode(), include_impact=True
    )
    impact = report["impact"]
    assert isinstance(impact, dict) and isinstance(impact["changes"], list)
    entries = {entry["pointer"]: entry for entry in impact["changes"] if isinstance(entry, dict)}
    shared = entries["/components/schemas/C~0D/type"]
    assert shared["unmapped"] is True
    assert shared["endpoints"][0]["endpoint"] == "POST /a~/b"  # type: ignore[index,call-overload]
    assert shared["endpoints"][0]["unmapped_operation_ids"] == ["unknown"]  # type: ignore[index,call-overload]
    assert entries["/paths/~1orders"]["endpoints"][0]["endpoint"] == "GET /orders"  # type: ignore[index,call-overload]
    assert entries["/paths/~1new"]["endpoints"][0]["endpoint"] == "GET /new"  # type: ignore[index,call-overload]
    assert "impact" not in check.make_report(SIMPLE, SIMPLE)


def test_impact_pinned_contract_local_mutation() -> None:
    original = check.read_file(check.BASELINE)
    candidate = original.replace(
        b"operationId: getCandleHistory",
        b"operationId: getCandleHistory\n      x-local-review: true",
        1,
    )
    report = check.make_report(original, candidate, include_impact=True)
    assert "get_candle_history" in check.render(report)
    assert "GetCandleHistory" in check.render(report)


def test_impact_path_parameters_and_external_refs() -> None:
    before = check.parse_contract(SIMPLE)
    paths = before["paths"]
    assert isinstance(paths, dict) and isinstance(paths["/orders"], dict)
    paths["/orders"]["parameters"] = [{"$ref": "https://example.test/schema"}]
    after = copy.deepcopy(before)
    after_paths = after["paths"]
    assert isinstance(after_paths, dict) and isinstance(after_paths["/orders"], dict)
    after_paths["/orders"]["parameters"] = [{"name": "changed", "in": "query"}]
    report = check.make_report(
        json.dumps(before).encode(), json.dumps(after).encode(), include_impact=True
    )
    impact = report["impact"]
    assert isinstance(impact, dict) and isinstance(impact["changes"], list)
    entry = impact["changes"][0]
    assert isinstance(entry, dict) and isinstance(entry["endpoints"], list)
    endpoint = entry["endpoints"][0]
    assert isinstance(endpoint, dict) and endpoint["endpoint"] == "GET /orders"
    assert endpoint["reasons"] == ["operation or path-level parameters"]
