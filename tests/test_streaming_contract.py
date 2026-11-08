from datetime import datetime
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from src.validation import valid_stream_events,score_windows,require_same_keys

@pytest.fixture(scope='module')
def spark():
    s=(SparkSession.builder.master('local[2]').appName('GridPulse-ContractTests')
       .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


def test_stream_rejects_nonfinite_null_negative_and_bad_timestamp(spark):
    dt=datetime(2010,1,1)
    rows=[(dt,x) for x in [float('nan'),float('inf'),float('-inf'),-1.,None,0.,1.]]+[(None,1.)]
    df=spark.createDataFrame(rows,'event_time timestamp,active_kw double')
    assert sorted(r.active_kw for r in valid_stream_events(df).collect())==[0.,1.]


def test_insufficient_windows_are_unscored_and_eligible_alerts_can_revise(spark):
    start=datetime(2010,1,3) # Sunday, Spark dayofweek 1 -> how 0
    windows=spark.createDataFrame([(start,10,5.),(start,53,5.),(start,54,5.),(start,60,1.)],
                                 'start timestamp,records long,mean_kw double')
    model=spark.createDataFrame([(0,1.,1.)],'how int,center double,scale double')
    result={r.records:r for r in score_windows(windows,model,3.).collect()}
    for n in [10,53]:
        assert result[n].alert is None and result[n].score is None
        assert result[n].scoring_status=='insufficient_coverage'
    assert result[54].alert is True and result[60].alert is False
    assert result[54].scoring_status=='eligible'


def test_missing_profile_is_not_a_negative_prediction(spark):
    windows=spark.createDataFrame([(datetime(2010,1,3),60,5.)],'start timestamp,records long,mean_kw double')
    model=spark.createDataFrame([(1,1.,1.)],'how int,center double,scale double')
    result=score_windows(windows,model,3.).first()
    assert result.scoring_status=='missing_profile' and result.alert is None


def test_key_comparison_rejects_extra_missing_or_duplicate_windows():
    left=pd.DataFrame({'start':[1,2]})
    for values in [[1],[1,2,3],[1,1]]:
        with pytest.raises(AssertionError):require_same_keys(left,pd.DataFrame({'start':values}),'start')
    require_same_keys(left,pd.DataFrame({'start':[2,1]}),'start')


def test_raw_timestamp_diagnostics_accept_unpadded_source_dates(spark):
    from pyspark.sql import functions as F
    source=spark.createDataFrame([('1/1/2010 00:00:00',),('16/12/2006 17:24:00',)],'raw string')
    result=source.select(F.to_timestamp('raw','d/M/yyyy HH:mm:ss').alias('ts')).collect()
    assert [r.ts for r in result]==[datetime(2010,1,1),datetime(2006,12,16,17,24)]
