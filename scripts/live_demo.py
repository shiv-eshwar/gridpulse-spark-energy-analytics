"""Live class demonstration. Keeps Spark UI open without changing submitted evidence."""
from pathlib import Path
import json,tempfile
from src.common import ROOT,OUT,spark_session
from src.streaming import run_query,publish,progress,latest_snapshots


def main():
    s=spark_session('GridPulse-ClassDemonstration')
    clean=s.read.parquet(str(OUT/'clean'));clean.createOrReplaceTempView('energy')
    print('\nLIVE SPARK SQL QUERY: top days with at least 1,380 valid minutes')
    s.sql((OUT/'sql/top_days.sql').read_text()).show(truncate=False)
    model=s.read.option('header',True).schema('how int,center double,mad double,scale double').csv(str(OUT/'model_hour_of_week.csv'))
    base=Path(tempfile.mkdtemp(prefix='live-demo-',dir=ROOT/'.runtime'))
    q=run_query(s,base,'2 hours',model)
    try:
        for i,path in enumerate(sorted((OUT/'streaming/replay/inbox').glob('*.json'))):
            records=[json.loads(x) for x in path.read_text().splitlines()]
            publish(base/'inbox',f'{i:03}.json',records);q.processAllAvailable()
            print('FILE',i,'published records',len(records))
        print('\nLIVE HOURLY STREAM AGGREGATES')
        print(latest_snapshots(base/'snapshots')[['start','records','mean_kw','scoring_status','alert']].tail(8).to_string(index=False))
        print('\nSpark UI:',s.sparkContext.uiWebUrl)
        print('The query and UI stay open. Inspect Jobs, Stages, SQL and Structured Streaming.')
        input('Press Enter to finish the demonstration: ')
    finally:
        q.stop();s.stop()


if __name__=='__main__':main()
