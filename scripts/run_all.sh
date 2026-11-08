#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
mkdir -p outputs/event_logs outputs/logs
python scripts/environment.py
python scripts/download.py | tee outputs/logs/download.log
python -m pytest -q tests | tee outputs/logs/tests.log
python -m src.batch 2>&1 | tee outputs/logs/batch.log
python -m src.experiments 2>&1 | tee outputs/logs/experiments.log
python -m src.streaming 2>&1 | tee outputs/logs/streaming.log
python -m src.joint_experiment 2>&1 | tee outputs/logs/joint_experiment.log
python scripts/event_summary.py
python scripts/evidence.py
python scripts/report.py 2>&1 | tee outputs/logs/report.log
python scripts/show_results.py > outputs/results_summary.txt
python scripts/preflight.py
python scripts/package.py
