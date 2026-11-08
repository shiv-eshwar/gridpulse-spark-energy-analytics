"""Extract actual task/shuffle evidence and retain only latest completed project logs."""
from pathlib import Path
import json, shutil
import pandas as pd
R=Path(__file__).resolve().parents[1];O=R/'outputs'
latest={}
for path in (O/'event_logs').glob('*'):
    if not path.is_file() or path.name.endswith('.inprogress'):continue
    app=None;ended=False;records=[]
    try:
        for line in path.open():
            event=json.loads(line)
            if event.get('Event')=='SparkListenerApplicationStart':app=event['App Name']
            if event.get('Event')=='SparkListenerApplicationEnd':ended=True
        if ended and app in ['GridPulse-Batch','GridPulse-Experiments','GridPulse-StructuredStreaming','GridPulse-JointFaultLateness'] and (app not in latest or path.stat().st_mtime>latest[app].stat().st_mtime):latest[app]=path
    except (ValueError,OSError):continue
rows=[];apps=[];retained=O/'evidence_event_logs';retained.mkdir(exist_ok=True)
for app,path in sorted(latest.items()):
    stages={};jobs=failed_jobs=0
    for line in path.open():
        event=json.loads(line);kind=event.get('Event')
        if kind=='SparkListenerJobEnd':
            jobs+=1;failed_jobs+=event.get('Job Result',{}).get('Result')!='JobSucceeded'
        if kind=='SparkListenerTaskEnd':
            key=(event['Stage ID'],event['Stage Attempt ID'])
            row=stages.setdefault(key,dict(application=app,stage_id=key[0],attempt=key[1],task_attempts=0,executor_run_ms=0,
                shuffle_read_bytes=0,shuffle_write_bytes=0,disk_spill_bytes=0))
            metrics=event.get('Task Metrics',{});read=metrics.get('Shuffle Read Metrics',{});write=metrics.get('Shuffle Write Metrics',{})
            row['task_attempts']+=1;row['executor_run_ms']+=metrics.get('Executor Run Time',0)
            row['shuffle_read_bytes']+=read.get('Remote Bytes Read',0)+read.get('Local Bytes Read',0)
            row['shuffle_write_bytes']+=write.get('Shuffle Bytes Written',0)
            row['disk_spill_bytes']+=metrics.get('Disk Bytes Spilled',0)
    rows.extend(stages.values());apps.append(dict(application=app,jobs=jobs,failed_jobs=failed_jobs,stages_with_tasks=len(stages),event_log=path.name))
    shutil.copy2(path,retained/path.name)
pd.DataFrame(rows).to_csv(O/'stage_metrics.csv',index=False)
(O/'execution_evidence.json').write_text(json.dumps(apps,indent=2))
print('Retained completed execution evidence for',len(apps),'applications')
