from pathlib import Path
import json, os, time
# Spark serializes naive Python datetimes using the process zone. Fix BOTH
# Python worker/process and Spark SQL clocks to preserve source wall-clock values.
os.environ["TZ"]="UTC"
if hasattr(time,"tzset"): time.tzset()
from pyspark.sql import SparkSession
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'outputs'; OUT.mkdir(exist_ok=True)
def dump(name, value):
    p=OUT/name; p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, indent=2, default=str))
def spark_session(name, master='local[4]'):
    s=(SparkSession.builder.master(master).appName(name)
       .config('spark.sql.session.timeZone','UTC')
       .config('spark.sql.shuffle.partitions','8')
       .config('spark.driver.memory','3g')
       .config('spark.eventLog.enabled','true')
       .config('spark.eventLog.dir',str(OUT/'event_logs')).getOrCreate())
    s.sparkContext.setLogLevel('WARN')
    return s
