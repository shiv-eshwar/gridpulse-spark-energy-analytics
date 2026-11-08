"""Shared stream-boundary and evaluation invariants."""
from pyspark.sql import functions as F


def valid_stream_events(incoming):
    power = F.col('active_kw')
    return incoming.filter(F.col('event_time').isNotNull() & power.isNotNull()
        & ~F.isnan(power) & (F.abs(power) != F.lit(float('inf'))) & (power >= 0))


def score_windows(aggregates, model, threshold):
    result = (aggregates.withColumn('how', (F.dayofweek('start') - 1) * 24 + F.hour('start'))
        .join(F.broadcast(model), 'how', 'left'))
    profile_ok = (F.col('center').isNotNull() & ~F.isnan('center')
        & (F.abs(F.col('center')) != F.lit(float('inf')))
        & F.col('scale').isNotNull() & ~F.isnan('scale')
        & (F.col('scale') < F.lit(float('inf'))) & (F.col('scale') > 0))
    eligible = (F.col('records') >= 54) & profile_ok
    return (result.withColumn('scoring_status', F.when(F.col('records') < 54, 'insufficient_coverage')
        .when(~profile_ok, 'missing_profile').otherwise('eligible'))
        .withColumn('score', F.when(eligible, F.abs(F.col('mean_kw') - F.col('center')) / F.col('scale')))
        .withColumn('alert', F.when(eligible, F.col('score') > threshold).otherwise(F.lit(None).cast('boolean'))))


def require_same_keys(left, right, key):
    """Reject missing/extra/duplicate keys before doing numerical comparisons."""
    if not left[key].is_unique or not right[key].is_unique:
        raise AssertionError(f'Duplicate {key} keys')
    if set(left[key]) != set(right[key]):
        raise AssertionError(f'Missing or extra {key} keys')
