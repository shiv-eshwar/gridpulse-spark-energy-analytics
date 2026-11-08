"""Full-source RDD ingestion -> explicit-schema DataFrame -> SQL and hourly features."""
import argparse, math, time, platform
from datetime import datetime
from pyspark.sql import functions as F, types as T
from src.common import ROOT, OUT, dump, spark_session
NAMES=['active_kw','reactive_kvar','voltage_v','current_a','kitchen_wh','laundry_wh','hvac_wh']
SCHEMA=T.StructType([T.StructField('event_time',T.TimestampType(),False)]+[T.StructField(n,T.DoubleType(),False) for n in NAMES])
def classify_line(line):
    """Return an auditable rejection reason and a typed source record."""
    parts = line.split(';')
    if len(parts) != 9:
        return 'field_count', None
    try:
        ts = datetime.strptime(parts[0] + ' ' + parts[1], '%d/%m/%Y %H:%M:%S')
    except ValueError:
        return 'invalid_timestamp', None
    if any(v.strip() in ('', '?') for v in parts[2:]):
        return 'missing_measurement', None
    try:
        values = tuple(float(v) for v in parts[2:])
    except (ValueError, OverflowError):
        return 'nonnumeric_measurement', None
    if not all(math.isfinite(v) for v in values):
        return 'nonfinite_measurement', None
    if any(v < 0 for v in values):
        return 'negative_measurement', None
    if values[2] <= 0:
        return 'nonpositive_voltage', None
    return 'accepted', (ts,) + values


def parse_line(line):
    return classify_line(line)[1]

def main():
    p=argparse.ArgumentParser(); p.add_argument('--master',default='local[4]'); a=p.parse_args()
    (OUT/'event_logs').mkdir(exist_ok=True)
    s=spark_session('GridPulse-Batch',a.master); start=time.perf_counter()
    raw=s.sparkContext.textFile(str(ROOT/'data/raw/household_power_consumption.txt'),minPartitions=8)
    lines=raw.filter(lambda x:not x.startswith('Date;')).cache()
    total=lines.count()
    parsed=lines.map(classify_line).cache()
    reason_counts=dict(parsed.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).collect())
    valid=parsed.filter(lambda x:x[0]=='accepted').map(lambda x:x[1]).cache(); n=valid.count()
    rejected_examples=lines.filter(lambda x:classify_line(x)[0]!='accepted').take(5)
    dump('rejections.json',{'reason_counts':reason_counts,'examples':[{'reason':classify_line(x)[0],'raw_line':x} for x in rejected_examples]})
    # flatMap emits one key per active submeter; reduceByKey merges locally before shuffle.
    frequencies=(valid.flatMap(lambda r:[(name,1) for name,v in zip(['kitchen','laundry','hvac'],r[5:8]) if v>0])
                 .reduceByKey(lambda a,b:a+b).collect())
    monthly=(valid.map(lambda r:(r[0].strftime('%Y-%m'),(r[1]/60,1)))
             .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).sortByKey().collect())
    dump('rdd_results.json',{'source_rows':total,'valid_rows':n,'rejected_rows':total-n,
         'source_partitions':raw.getNumPartitions(),'submeter_active_minutes':dict(frequencies),
         'monthly_energy_kwh':monthly,'lineage':valid.toDebugString().decode()})
    df=s.createDataFrame(valid,SCHEMA)
    # Timestamp duplicates are explicit: no imputation, no future-value leakage.
    clean=df.dropDuplicates(['event_time']).withColumn('energy_kwh',F.col('active_kw')/60)
    clean=clean.withColumn('unmetered_wh',F.col('active_kw')*1000/60-F.col('kitchen_wh')-F.col('laundry_wh')-F.col('hvac_wh')).cache()
    unique=clean.count()
    clean.createOrReplaceTempView('energy')
    queries={
      'monthly':"""SELECT date_format(event_time,'yyyy-MM') month, COUNT(*) valid_minutes,
        ROUND(SUM(energy_kwh),4) observed_kwh, ROUND(AVG(active_kw),4) mean_kw
        FROM energy GROUP BY 1 ORDER BY 1""",
      'top_days':"""SELECT to_date(event_time) day, COUNT(*) valid_minutes,
        ROUND(SUM(energy_kwh),3) observed_kwh FROM energy GROUP BY 1
        HAVING COUNT(*) >= 1380 ORDER BY observed_kwh DESC LIMIT 10""",
      'hour_profile':"""SELECT hour(event_time) hour, COUNT(*) n, AVG(active_kw) mean_kw,
        percentile_approx(active_kw,0.95,10000) p95_kw FROM energy GROUP BY 1 ORDER BY 1""",
      'submeters':"""SELECT SUM(kitchen_wh)/1000 kitchen_kwh, SUM(laundry_wh)/1000 laundry_kwh,
        SUM(hvac_wh)/1000 hvac_kwh, SUM(unmetered_wh)/1000 unmetered_kwh FROM energy"""}
    (OUT/'sql').mkdir(exist_ok=True)
    for name,q in queries.items():
        result=s.sql(q); result.toPandas().to_csv(OUT/f'sql/{name}.csv',index=False)
        (OUT/f'sql/{name}.sql').write_text(q)
    monthly_df=s.sql(queries['monthly']).collect()
    assert len(monthly_df)==len(monthly), 'Monthly key count mismatch'
    assert {r.month for r in monthly_df}=={k for k,_ in monthly}, 'Monthly key set mismatch'
    for row, (month,(kwh,count)) in zip(monthly_df,monthly):
        assert row.month==month and row.valid_minutes==count and abs(row.observed_kwh-kwh)<0.001
    hourly=(clean.groupBy(F.date_trunc('hour','event_time').alias('event_time'))
      .agg(F.count('*').alias('minutes'),F.avg('active_kw').alias('mean_kw'),F.sum('energy_kwh').alias('energy_kwh'))
      .withColumn('hour',F.hour('event_time')).withColumn('how',(F.dayofweek('event_time')-1)*24+F.hour('event_time')))
    hourly.write.mode('overwrite').parquet(str(OUT/'hourly'))
    clean.write.mode('overwrite').parquet(str(OUT/'clean'))
    quality={'source_rows':total,'valid_rows':n,'rejected_rows':total-n,'duplicate_timestamps':n-unique,
      'negative_unmetered_minutes':clean.filter('unmetered_wh < -0.1').count(),
      'minimum_unmetered_wh':clean.agg(F.min('unmetered_wh')).first()[0],
      'rejection_reason_counts':reason_counts,
      'start':clean.agg(F.min('event_time')).first()[0],'end':clean.agg(F.max('event_time')).first()[0],
      'observed_kwh':clean.agg(F.sum('energy_kwh')).first()[0], 'rdd_sql_reconciliation':'PASS',
      'wall_seconds':time.perf_counter()-start,'spark':s.version,'python':platform.python_version(),
      'master':s.sparkContext.master,'hardware':platform.platform()}
    dump('quality.json',quality)
    with (OUT/'query_plan.txt').open('w') as f:
        f.write(s.sql(queries['monthly'])._jdf.queryExecution().toString())
    from src.diagnostics import run_diagnostics
    run_diagnostics(s,clean)
    print(quality); s.stop()
if __name__=='__main__': main()
