"""File arrivals, static model join, watermarked state, checkpoint restart and late-data probes."""
from pathlib import Path
import json, shutil, os, time
import pandas as pd
from pyspark.sql import functions as F, types as T
from src.common import ROOT, OUT,dump,spark_session
from src.validation import valid_stream_events, score_windows, require_same_keys
SCHEMA=T.StructType([T.StructField('event_time',T.TimestampType()),T.StructField('active_kw',T.DoubleType())])
def publish(directory, name, records):
    directory.mkdir(parents=True,exist_ok=True)
    temp=directory.parent/(name+'.tmp')
    temp.write_text(''.join(json.dumps(r)+'\n' for r in records)); os.replace(temp,directory/name)
def latest_snapshots(folder):
    frames=[]
    for p in sorted(folder.glob('batch_*')):
        files=list(p.glob('*.parquet'))
        if files:
            df=pd.concat([pd.read_parquet(f) for f in files]);df['batch_id']=int(p.name.split('_')[1]);frames.append(df)
    if not frames:return pd.DataFrame()
    return pd.concat(frames).sort_values('batch_id').drop_duplicates('start',keep='last').sort_values('start')
def run_query(s, base, watermark, model=None):
    inbox=base/'inbox'; inbox.mkdir(parents=True,exist_ok=True)
    incoming=s.readStream.schema(SCHEMA).option('maxFilesPerTrigger',1).json(str(inbox))
    events=valid_stream_events(incoming).withWatermark('event_time',watermark)
    agg=(events.groupBy(F.window('event_time','1 hour')).agg(F.count('*').alias('records'),F.sum(F.col('active_kw')/60).alias('energy_kwh'),F.avg('active_kw').alias('mean_kw'))
       .select(F.col('window.start').alias('start'),F.col('window.end').alias('end'),'records','energy_kwh','mean_kw'))
    if model is not None:
        calibration=json.loads((OUT/'calibration.json').read_text())
        threshold=float(calibration['models']['hour_of_week']['threshold'])
        agg=score_windows(agg,model,threshold)
    snapshots=base/'snapshots'
    def sink(df,batch_id):
        # Stable per-batch overwrite makes retries idempotent for this single-writer local sink.
        df.write.mode('overwrite').parquet(str(snapshots/f'batch_{batch_id:06d}'))
    return (agg.writeStream.outputMode('update').foreachBatch(sink)
      .option('checkpointLocation',str(base/'checkpoint')).trigger(processingTime='1 second').start())
def progress(q):
    return [dict(batchId=p['batchId'],numInputRows=p['numInputRows'],durationMs=p.get('durationMs'),
         eventTime=p.get('eventTime'),stateOperators=p.get('stateOperators')) for p in q.recentProgress]
def annotate_window_state(frame, logs):
    """A stopped query's watermark determines which snapshot windows can still revise."""
    frame=frame.copy()
    watermarks=[p.get('eventTime',{}).get('watermark') for p in logs if p.get('eventTime')]
    watermarks=[pd.Timestamp(w) for w in watermarks if w]
    watermark=max(watermarks).tz_localize(None) if watermarks else pd.Timestamp('1970-01-01')
    frame['window_state']=['finalized' if end<=watermark else 'provisional' for end in pd.to_datetime(frame['end'])]
    frame['watermark_at_stop']=str(watermark)
    return frame


def main():
    s=spark_session('GridPulse-StructuredStreaming')
    base=OUT/'streaming'
    if base.exists():shutil.rmtree(base)
    base.mkdir()
    # Replay 21,600 real, valid readings (well above 10k), ordered by source-local clock.
    readings=(s.read.parquet(str(OUT/'clean')).filter("event_time >= '2010-01-01'")
              .select('event_time','active_kw').orderBy('event_time').limit(21600).toPandas())
    records=[{'event_time':r.event_time.isoformat(),'active_kw':r.active_kw} for r in readings.itertuples()]
    model=s.read.option('header',True).schema('how int, center double, mad double, scale double').csv(str(OUT/'model_hour_of_week.csv'))
    run=base/'replay'; q=run_query(s,run,'2 hours',model); logs=[]
    try:
      for i in range(6):
        publish(run/'inbox',f'{i:03}.json',records[i*3600:(i+1)*3600]);q.processAllAvailable()
        if i==2:
            logs+=progress(q);q.stop();q=run_query(s,run,'2 hours',model)
      logs+=progress(q)
    finally:q.stop()
    snapshots=annotate_window_state(latest_snapshots(run/'snapshots'),logs)
    reference=readings.assign(start=readings.event_time.dt.floor('h')).groupby('start').agg(records=('active_kw','size'),energy_kwh=('active_kw',lambda x:x.sum()/60)).reset_index()
    require_same_keys(reference,snapshots,'start')
    merged=reference.merge(snapshots,on='start',suffixes=('_reference','_stream'),validate='one_to_one')
    assert len(merged)==len(reference)
    assert (merged.records_reference==merged.records_stream).all()
    error=float((merged.energy_kwh_reference-merged.energy_kwh_stream).abs().max());assert error<1e-8
    snapshots.to_csv(base/'replay_hourly.csv',index=False);dump('streaming/replay_progress.json',logs)
    # Joint systems/analytics experiment: delay every tenth real reading by one file.
    # File blocks contain 60 valid observation-hours, with longer spans across gaps; no timestamps or measurements are fabricated.
    delay_metrics=[]
    for hours in [2,72,168]:
        case=base/f'delayed_{hours}h'; q=run_query(s,case,f'{hours} hours',model)
        pending=[]
        try:
            for i in range(6):
                block=records[i*3600:(i+1)*3600]
                late_block=block[::10]
                ontime=[r for j,r in enumerate(block) if j%10]
                publish(case/'inbox',f'{i:03}.json',ontime+pending)
                q.processAllAvailable();pending=late_block
            publish(case/'inbox','006.json',pending);q.processAllAvailable()
            case_logs=progress(q)
        finally:q.stop()
        snap=annotate_window_state(latest_snapshots(case/'snapshots'),case_logs)
        comparison=snapshots[['start','records','energy_kwh','alert','scoring_status']].merge(
            snap[['start','records','energy_kwh','alert','scoring_status']],on='start',how='left',suffixes=('_offline','_delayed'),validate='one_to_one')
        comparable=(comparison.scoring_status_offline=='eligible') & (comparison.scoring_status_delayed=='eligible')
        energy_error=float((comparison.energy_kwh_offline-comparison.energy_kwh_delayed.fillna(0)).abs().sum())
        accepted=int(snap.records.sum())
        delay_metrics.append({'watermark_hours':hours,'source_records':len(records),'retained_records':accepted,
            'retention_fraction':accepted/len(records),'total_absolute_energy_error_kwh':energy_error,
            'alert_disagreements':int((comparison.loc[comparable,'alert_offline']!=comparison.loc[comparable,'alert_delayed']).sum()),
            'comparable_windows':int(comparable.sum()),'unscored_delayed_windows':int((comparison.scoring_status_delayed!='eligible').sum()),
            'max_state_memory_bytes':max((op.get('memoryUsedBytes',0) for p in case_logs for op in (p['stateOperators'] or [])),default=0)})
        snap.to_csv(case/'latest_hourly.csv',index=False)
        dump(f'streaming/delayed_{hours}h_progress.json',case_logs)
        if hours==168:assert accepted==len(records) and energy_error<1e-8
        else:assert accepted<len(records) and energy_error>0
    pd.DataFrame(delay_metrics).to_csv(base/'delayed_replay_experiment.csv',index=False)
    # Four ordered arrivals, with a deliberately delayed record in an already advanced window.
    probe_batches=[[{'event_time':'2010-01-01T00:00:00','active_kw':1.}],
                   [{'event_time':'2010-01-01T00:50:00','active_kw':1.}],
                   [{'event_time':'2010-01-01T02:00:00','active_kw':1.}],
                   [{'event_time':'2010-01-01T00:10:00','active_kw':1.}]]
    probes=[]
    for minutes in [0,30,180]:
        case=base/f'late_{minutes}'; q=run_query(s,case,f'{minutes} minutes')
        try:
            for i,b in enumerate(probe_batches):publish(case/'inbox',f'{i}.json',b);q.processAllAvailable()
            logs_case=progress(q)
        finally:q.stop()
        snap=latest_snapshots(case/'snapshots'); accepted=int(snap.records.sum())
        dropped=sum(op.get('numRowsDroppedByWatermark',0) for p in logs_case for op in (p['stateOperators'] or []))
        probes.append({'watermark_minutes':minutes,'input_records':4,'accepted_records':accepted,
                       'dropped_by_watermark':dropped})
        dump(f'streaming/late_{minutes}_progress.json',logs_case)
        assert accepted==(4 if minutes==180 else 3),(minutes,accepted)
    pd.DataFrame(probes).to_csv(base/'watermark_experiment.csv',index=False)
    dump('streaming/validation.json',{'real_records_replayed':len(readings),'hourly_windows':len(reference),
       'max_energy_error_kwh':error,'checkpoint_restart_after_files':3,'batch_stream_parity':'PASS',
       'eligible_windows':int((snapshots.scoring_status=='eligible').sum()),
       'insufficient_coverage_windows':int((snapshots.scoring_status=='insufficient_coverage').sum()),
       'input_boundary':'reject null timestamps, null/nonfinite/negative power',
       'provisional_windows':'Snapshots label finalized/provisional using window end and last observed query watermark.',
       'late_probe':'Four synthetic events test semantics; main replay uses real readings.'})
    s.stop()
if __name__=='__main__':main()
