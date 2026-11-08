#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
if [[ "${1:-}" == "--live" ]]; then
  python scripts/live_demo.py
else
  python scripts/show_results.py
  echo
  echo 'Report: output/pdf/GridPulse_Report.pdf'
  echo 'Presentation script: docs/PRESENTATION_GUIDE.txt'
  echo 'Browser evidence: outputs/evidence.html'
  echo 'Live demonstration with persistent Spark UI: bash scripts/present.sh --live'
fi
