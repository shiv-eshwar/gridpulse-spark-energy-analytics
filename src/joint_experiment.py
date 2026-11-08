"""Controlled raw-minute faults + event-time lateness; no natural fault labels claimed."""
import json
import numpy as np
import pandas as pd
from src.common import OUT,dump,spark_session
from src.streaming import run_query,publish,latest_snapshots,progress,annotate_window_state
from src.validation import require_same_keys


def metrics(frame):
    eligible=frame.scoring_status.eq('eligible')
    label=frame.injected.astype(bool)
    # Unscored fault windows are separately counted and treated as misses end-to-end.
    prediction=frame.alert.fillna(False).astype(bool)
    tp=int((label & prediction & eligible).sum())
    fp=int((~label & prediction & eligible).sum())
    fn=int((label & ~(prediction & eligible)).sum())
    p=tp/max(tp+fp,1);r=tp/max(tp+fn,1)
    return dict(tp=tp,fp=fp,fn=fn,precision=p,recall=r,f1=2*p*r/max(p+r,1e-12),
                unscored_fault_windows=int((label & ~eligible).sum()))


def main():
    s=spark_session('GridPulse-JointFaultLateness')
    base=OUT/'streaming/joint_faults'
    # Select 12 fault-hours independent of model scores, seed fixed before evaluation.
    original=[]
    for p in sorted((OUT/'streaming/replay/inbox').glob('*.json')):
        original.extend(json.loads(line) for line in p.read_text().splitlines())
    source=pd.DataFrame(original);source['start']=pd.to_datetime(source.event_time).dt.floor('h')
    counts=source.groupby('start').size()
    candidates=np.array(counts[counts>=54].index)
    chosen=set(pd.Timestamp(x) for x in np.random.default_rng(2026).choice(candidates,12,replace=False))
    # Add 10 kW only to every tenth minute in each selected hour, then delay exactly
    # those observations by one file. Other observations are unchanged.
    # This is a short-duration sub-hour fault, distinct from the hourly amplitude suite.
    records=[]; labels=[]
    for i,r in enumerate(original):
        active=float(r['active_kw'])
        fault=pd.Timestamp(r['event_time']).floor('h') in chosen and i%10==0
        records.append({'event_time':r['event_time'],'active_kw':active+(10.0 if fault else 0.)})
        labels.append(fault)
    model=s.read.option('header',True).schema('how int,center double,mad double,scale double').csv(str(OUT/'model_hour_of_week.csv'))
    threshold=float(json.loads((OUT/'calibration.json').read_text())['models']['hour_of_week']['threshold'])
    profile=model.toPandas()
    reference=pd.DataFrame(records);reference['event_time']=pd.to_datetime(reference.event_time)
    reference['injected']=labels;reference['start']=reference.event_time.dt.floor('h')
    ref=reference.groupby('start').agg(records=('active_kw','size'),mean_kw=('active_kw','mean'),injected=('injected','any')).reset_index()
    ref['how']=((ref.start.dt.dayofweek+1)%7)*24+ref.start.dt.hour
    ref=ref.merge(profile,on='how',validate='many_to_one')
    ref['scoring_status']=np.where(ref.records>=54,'eligible','insufficient_coverage')
    ref['score']=np.where(ref.records>=54,abs(ref.mean_kw-ref.center)/ref.scale,np.nan)
    ref['alert']=pd.Series(ref.score>threshold,dtype='boolean');ref.loc[ref.records<54,'alert']=pd.NA
    ref.to_csv(base.parent/'joint_offline_reference.csv',index=False)
    rows=[dict(watermark_hours='offline',alert_disagreements=0,comparable_windows=int(ref.records.ge(54).sum()),**metrics(ref))]
    for hours in [2,168]:
        case=base/f'{hours}h';q=run_query(s,case,f'{hours} hours',model);pending=[]
        try:
            for i in range(6):
                block=records[i*3600:(i+1)*3600]
                publish(case/'inbox',f'{i:03}.json',[r for j,r in enumerate(block) if j%10]+pending)
                q.processAllAvailable();pending=block[::10]
            publish(case/'inbox','006.json',pending);q.processAllAvailable();logs=progress(q)
        finally:q.stop()
        snap=annotate_window_state(latest_snapshots(case/'snapshots'),logs)
        require_same_keys(ref,snap,'start')
        evaluated=snap.merge(ref[['start','injected','alert','scoring_status']],on='start',suffixes=('','_offline'),validate='one_to_one')
        comparable=evaluated.scoring_status.eq('eligible') & evaluated.scoring_status_offline.eq('eligible')
        disagreement=int((evaluated.loc[comparable,'alert']!=evaluated.loc[comparable,'alert_offline']).sum())
        rows.append(dict(watermark_hours=hours,alert_disagreements=disagreement,comparable_windows=int(comparable.sum()),**metrics(evaluated)))
        if hours==168:
            assert disagreement==0
            assert np.allclose(snap.sort_values('start').mean_kw,ref.sort_values('start').mean_kw)
        snap.to_csv(case/'latest_hourly.csv',index=False)
        dump(f'streaming/joint_{hours}h_progress.json',logs)
    pd.DataFrame(rows).to_csv(OUT/'streaming/joint_fault_lateness.csv',index=False)
    dump('streaming/joint_protocol.json',{'seed':2026,'selected_hours':sorted(str(x) for x in chosen),
        'injected_minute_records':sum(labels),'amplitude_kw':10,'selection':'random eligible hours, every tenth minute',
        'interpretation':'controlled perturbation labels; no verified natural faults; final snapshot accuracy, not detection latency',
        'model_selection':'hour-of-week preselected before reading test metrics'})
    s.stop()


if __name__=='__main__':main()
