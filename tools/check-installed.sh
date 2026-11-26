#!/bin/sh
# Build/install locally; execute only synthetic requests from outside the checkout.
set -eu
sdk_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
cd "$sdk_root"
uv build --out-dir "$scratch/dist"
uv export --locked --no-dev --no-emit-project --no-hashes --output-file "$scratch/runtime.txt" >/dev/null
uv export --locked --no-emit-project --no-hashes --output-file "$scratch/development.txt" >/dev/null
interpreter=$(uv run --locked python -c 'import sys; print(sys.executable)')
uv venv --python "$interpreter" "$scratch/venv"
uv pip sync --python "$scratch/venv/bin/python" "$scratch/runtime.txt"
uv pip install --python "$scratch/venv/bin/python" --no-deps "$scratch"/dist/*.whl
cp "$sdk_root"/consumers/*.py "$sdk_root"/consumers/mypy.ini "$scratch/"
cd "$scratch"
unset PYTHONPATH MYPYPATH
"$scratch/venv/bin/python" -I consumer.py
uv pip check --python "$scratch/venv/bin/python"
uv pip sync --python "$scratch/venv/bin/python" "$scratch/development.txt"
uv pip install --python "$scratch/venv/bin/python" --no-deps "$scratch"/dist/*.whl
"$scratch/venv/bin/python" -I -m mypy --config-file mypy.ini consumer.py
if "$scratch/venv/bin/python" -I -m mypy --config-file mypy.ini invalid_types.py >negative.txt 2>&1; then
    echo 'Invalid consumer unexpectedly passed type checking' >&2
    exit 1
fi
"$scratch/venv/bin/python" -I -c 'from pathlib import Path; text = Path("negative.txt").read_text(); assert text.count("[arg-type]") == 3, text; print("Installed typing: three invalid arguments rejected")'
