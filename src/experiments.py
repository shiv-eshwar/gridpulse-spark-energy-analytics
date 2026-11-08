"""Temporal holdout + global / hour / hour-of-week robust baselines.
Labels mean injected perturbations, never verified natural faults.
"""
import time, random
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from src.common import OUT,dump,spark_session

def profile(train, keys):
    if not keys: train=train.withColumn('global_key',F.lit(0)); keys=['global_key']
    med=train.groupBy(*keys).agg(F.percentile_approx('mean_kw',.5,10000).alias('center'))
    joined=train.join(med,keys)
    scale=joined.groupBy(*keys).agg(F.percentile_approx(F.abs(F.col('mean_kw')-F.col('center')),.5,10000).alias('mad'))
    return med.join(scale,keys).withColumn('scale',F.greatest(F.col('mad')*1.4826,F.lit(.05)))

def main():
    s=spark_session('GridPulse-Experiments')
    hourly=s.read.parquet(str(OUT/'hourly')).filter('minutes >= 54').withColumn('global_key',F.lit(0)).cache()
    train=hourly.filter("event_time < '2009-01-01'")
    val=hourly.filter("event_time >= '2009-01-01' AND event_time < '2010-01-01'")
    test=hourly.filter("event_time >= '2010-01-01'")
    dump('splits.json',{'train_hours':train.count(),'validation_hours':val.count(),'test_hours':test.count(),
        'coverage_rule':'at least 54 valid minutes per hour; no imputation',
        'train_end_exclusive':'2009-01-01','validation_end_exclusive':'2010-01-01'})
    # Independently sampled point spikes and 6-hour level shifts; same injections for every model.
    original=test.orderBy('event_time').toPandas()
    all_metrics=[]; scored=[]; calibration={}
    for name,keys in [('global',['global_key']),('hour_of_day',['hour']),('hour_of_week',['how'])]:
        model=profile(train,keys).cache()
        model.toPandas().to_csv(OUT/f'model_{name}.csv',index=False)
        val_scores=val.join(model,keys).withColumn('score',F.abs(F.col('mean_kw')-F.col('center'))/F.col('scale'))
        threshold=val_scores.approxQuantile('score',[.99],.0001)[0]
        calibration[name]={'threshold':threshold,'keys':keys,'calibration_year':2009,'quantile':.99}
        params=model.toPandas()
        joined=original.merge(params,on=keys,validate='many_to_one')
        for kind in ['spike','level_shift']:
          for strength in [1.,2.,4.]:
           for seed in [7,19,41]:
            rng=np.random.default_rng(seed)
            n=len(joined); labels=np.zeros(n,dtype=bool)
            # All perturbations are specified in absolute kW, independent of model scale.
            if kind=='spike': labels[rng.choice(n,size=max(1,n//100),replace=False)]=True
            else:
                candidates=np.array([i for i in range(0,n-6,6)
                    if joined.event_time.iloc[i+5]-joined.event_time.iloc[i]==pd.Timedelta(hours=5)])
                for idx in rng.choice(candidates,size=max(1,n//600),replace=False): labels[idx:idx+6]=True
            perturbed=joined.mean_kw.to_numpy()+labels*strength
            scores=np.abs(perturbed-joined.center.to_numpy())/joined.scale.to_numpy()
            pred=scores>threshold
            tp=int(np.sum(pred&labels)); fp=int(np.sum(pred&~labels)); fn=int(np.sum(~pred&labels)); tn=int(np.sum(~pred&~labels))
            precision=tp/max(tp+fp,1); recall=tp/max(tp+fn,1)
            all_metrics.append(dict(model=name,kind=kind,strength_kw=strength,seed=seed,threshold=threshold,
              tp=tp,fp=fp,fn=fn,tn=tn,precision=precision,recall=recall,f1=2*precision*recall/max(precision+recall,1e-12),
              background_alert_rate=fp/max(fp+tn,1)))
        natural=joined.copy(); natural['score']=np.abs(natural.mean_kw-natural.center)/natural.scale
        natural['threshold']=threshold; natural['model']=name
        scored.append(natural)
        model.unpersist()
    dump('calibration.json',{'streaming_model':'hour_of_week','selection':'preselected demonstrator, not selected using test scores','models':calibration})
    pd.DataFrame(all_metrics).to_csv(OUT/'anomaly_metrics.csv',index=False)
    pd.concat(scored).to_csv(OUT/'heldout_scores.csv',index=False)
    # Partition-sensitivity microbenchmark, fixed cached input, randomized order, repeated.
    bench=[]
    benchmark_input=(s.read.parquet(str(OUT/'clean')).select('event_time','active_kw')
        .withColumn('how',(F.dayofweek('event_time')-1)*24+F.hour('event_time')).cache())
    benchmark_rows=benchmark_input.count()
    configs=[2,4,8,16]
    for parts in configs:
        benchmark_input.repartition(parts).groupBy('how').agg(F.avg('active_kw')).collect()
    jobs=[(r,p) for r in range(3) for p in configs]; random.Random(17).shuffle(jobs)
    for rep,parts in jobs:
        t=time.perf_counter()
        result=benchmark_input.repartition(parts).groupBy('how').agg(F.avg('active_kw')).collect()
        bench.append({'repeat':rep,'partitions':parts,'seconds':time.perf_counter()-t,'output_groups':len(result),'input_rows':benchmark_rows})
    pd.DataFrame(bench).to_csv(OUT/'partition_benchmark.csv',index=False)
    volume=[]
    for fraction in [.25,.5,1.]:
        subset=benchmark_input.sample(False,fraction,seed=2026).cache()
        rows=subset.count()
        subset.repartition(8).groupBy('how').agg(F.avg('active_kw')).collect()
        for rep in range(3):
            t=time.perf_counter()
            subset.repartition(8).groupBy('how').agg(F.avg('active_kw')).collect()
            volume.append({'fraction':fraction,'input_rows':rows,'repeat':rep,'seconds':time.perf_counter()-t,'master':s.sparkContext.master})
        subset.unpersist()
    pd.DataFrame(volume).to_csv(OUT/'volume_benchmark.csv',index=False)
    s.stop()
if __name__=='__main__':main()
