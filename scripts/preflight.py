"""Submission artifact checks with separate technical and human readiness."""
from pathlib import Path
import json,re
from PIL import Image,ImageStat
import numpy as np
import pandas as pd
from scripts.download import sha256,ARCHIVE_SHA256,TEXT_SHA256,MEMBER
R=Path(__file__).resolve().parents[1];O=R/'outputs'
q=json.loads((O/'quality.json').read_text());v=json.loads((O/'streaming/validation.json').read_text())
stream=pd.read_csv(O/'streaming/replay_hourly.csv')
ineligible=stream.scoring_status.ne('eligible')
checks={
 'pinned_archive':sha256(R/'data/raw/household.zip')==ARCHIVE_SHA256,
 'pinned_text':sha256(R/'data/raw'/MEMBER)==TEXT_SHA256,
 'row_accounting':q['source_rows']==q['valid_rows']+q['rejected_rows'],
 'rdd_sql_reconciliation':q['rdd_sql_reconciliation']=='PASS',
 'real_stream_minimum_10k':v['real_records_replayed']>=10000,
 'batch_stream_parity':v['batch_stream_parity']=='PASS',
 'ineligible_alerts_null':bool(stream.loc[ineligible,'alert'].isna().all()),
 'ineligible_scores_null':bool(stream.loc[ineligible,'score'].isna().all()),
 'eligible_minimum_54':bool(stream.loc[~ineligible,'records'].ge(54).all()),
 'window_states_explicit':set(stream.window_state)<= {'finalized','provisional'},
 'regression_tests_pass':bool(re.search(r'\b\d+ passed\b',(O/'logs/tests.log').read_text())) and not bool(re.search(r'\b\d+ failed\b',(O/'logs/tests.log').read_text())),
 'joint_experiment_present':(O/'streaming/joint_fault_lateness.csv').exists(),
 'selected_event_logs_present':bool(list((O/'evidence_event_logs').glob('*'))),
 'source_coverage_complete':int(pd.read_csv(O/'sql/monthly_coverage.csv').source_minutes.sum())==q['source_rows'],
 'rejected_run_accounting':int(pd.read_csv(O/'missing_runs.csv').missing_minutes.sum())==q['rejected_rows']
}
meta=json.loads((R/'submission.json').read_text())
human={'cal1_continuity_or_approval':bool(meta['assignment_1_dataset_confirmed'] or meta['dataset_change_approved']),
       'actual_group_details':bool(meta['group_members'])}
screens={name:((O/'screenshots'/name).exists() and max(ImageStat.Stat(Image.open(O/'screenshots'/name).convert('RGB')).stddev)>10) for name in ['spark_jobs.png','spark_stages.png','sql_results.png','streaming_results.png']}
result={'technical_ready':all(checks.values()),'technical_checks':checks,'screenshot_files':screens,
        'human_readiness':human,'submission_ready':all(checks.values()) and all(human.values()) and all(screens.values())}
(O/'preflight.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
if not all(checks.values()):raise SystemExit('Technical preflight failed')
