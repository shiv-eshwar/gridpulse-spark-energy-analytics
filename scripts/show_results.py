"""Concise, screenshot-ready terminal view of actual stored results."""
from pathlib import Path
import json, pandas as pd
R=Path(__file__).resolve().parents[1];O=R/'outputs'
print('GRIDPULSE | MMDS Assignment 2 | Stored execution evidence\n')
print('DATA QUALITY AND INDEPENDENT RDD / SQL RECONCILIATION')
print(json.dumps(json.loads((O/'quality.json').read_text()),indent=2))
print('\nSPARK SQL: TOP DAYS WITH >= 1,380 VALID MINUTES')
print(pd.read_csv(O/'sql/top_days.csv').to_string(index=False))
print('\nSTRUCTURED STREAMING: REPLAY + CHECKPOINT RESTART')
print((O/'streaming/validation.json').read_text())
print('\nREAL-DATA DELAYED REPLAY')
print(pd.read_csv(O/'streaming/delayed_replay_experiment.csv').to_string(index=False))
print('\nWATERMARK SEMANTICS PROBE')
print(pd.read_csv(O/'streaming/watermark_experiment.csv').to_string(index=False))
print('\nCONTROLLED INJECTION SCORES: NO VERIFIED NATURAL FAULT LABELS')
print(pd.read_csv(O/'anomaly_metrics.csv').groupby('model')[['precision','recall','f1','background_alert_rate']].mean().round(4))

print('\nJOINT CONTROLLED BURSTS AND LATENESS')
print(pd.read_csv(O/'streaming/joint_fault_lateness.csv').to_string(index=False))
