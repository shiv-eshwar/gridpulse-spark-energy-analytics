"""Coverage and missing-run diagnostics over pinned raw timestamps and clean data."""
from pyspark.sql import functions as F
from src.common import ROOT,OUT,dump,spark_session

def run_diagnostics(s,clean):
    source_times=(s.read.option('header',True).option('sep',';')
        .csv(str(ROOT/'data/raw/household_power_consumption.txt'))
        .select(F.to_timestamp(F.concat_ws(' ',F.col('Date'),F.col('Time')),'d/M/yyyy HH:mm:ss').alias('event_time')).cache())
    expected=source_times.groupBy(F.date_format('event_time','yyyy-MM').alias('month')).count().withColumnRenamed('count','source_minutes')
    coverage=s.read.option('header',True).schema('month string,valid_minutes long,observed_kwh double,mean_kw double').csv(str(OUT/'sql/monthly.csv')).join(expected,'month').withColumn('coverage_fraction',F.col('valid_minutes')/F.col('source_minutes')).orderBy('month')
    coverage.toPandas().to_csv(OUT/'sql/monthly_coverage.csv',index=False)
    # Invalid source rows form consecutive missing-measurement runs, not invented gaps.
    missing=source_times.join(clean.select('event_time'),'event_time','left_anti')
    from pyspark.sql.window import Window
    missing=(missing.withColumn('seq',F.row_number().over(Window.orderBy('event_time')))
        .withColumn('run_key',F.col('event_time').cast('long')-F.col('seq')*60))
    runs=(missing.groupBy('run_key').agg(F.min('event_time').alias('start'),F.max('event_time').alias('end'),F.count('*').alias('missing_minutes'))
        .drop('run_key').orderBy(F.desc('missing_minutes')))
    runs.toPandas().to_csv(OUT/'missing_runs.csv',index=False)
    dump('residual_diagnostics.json',{'units':'Wh per minute','cause':'unknown; rounding or measurement alignment require source metadata',
        'quantiles':dict(zip(['min','p01','median','p99','max'],clean.approxQuantile('unmetered_wh',[0.,.01,.5,.99,1.],.0001)))})

if __name__=='__main__':
    s=spark_session('GridPulse-QualityDiagnostics')
    run_diagnostics(s,s.read.parquet(str(OUT/'clean')))
    s.stop()
