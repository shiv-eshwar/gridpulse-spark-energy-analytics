#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v uv >/dev/null || { echo 'Install uv: https://docs.astral.sh/uv/'; exit 1; }
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python setuptools==75.8.0 wheel==0.45.1
uv pip install --python .venv/bin/python --no-build-isolation -r requirements.lock.txt
if [[ "$(uname)" == Darwin ]] && [[ ! -x /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home/bin/java ]] && [[ -z "${JAVA_HOME:-}" ]]; then
  brew install openjdk@17
fi
source scripts/env.sh
java -version
python -c 'import pyspark; print(pyspark.__version__)'
echo 'Setup complete. Run: bash scripts/run_all.sh'
