#!/usr/bin/env bash
if [[ -n "${ZSH_VERSION:-}" ]]; then
  eval 'GRIDPULSE_ENV_PATH=${(%):-%x}'
else
  GRIDPULSE_ENV_PATH="${BASH_SOURCE[0]}"
fi
GRIDPULSE_ROOT="$(cd "$(dirname "$GRIDPULSE_ENV_PATH")/.." && pwd)"
if [[ -z "${JAVA_HOME:-}" ]]; then
  if [[ -x /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home/bin/java ]]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
  else
    export JAVA_HOME="$(find "$GRIDPULSE_ROOT/.runtime" -type d -path '*/Contents/Home' -print -quit)"
  fi
fi
export PATH="$JAVA_HOME/bin:$GRIDPULSE_ROOT/.venv/bin:$PATH"
export PYSPARK_PYTHON="$GRIDPULSE_ROOT/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export TZ=UTC
export MPLCONFIGDIR="$GRIDPULSE_ROOT/.runtime/matplotlib"
export PYTHONPATH="$GRIDPULSE_ROOT${PYTHONPATH:+:$PYTHONPATH}"
cd "$GRIDPULSE_ROOT"
