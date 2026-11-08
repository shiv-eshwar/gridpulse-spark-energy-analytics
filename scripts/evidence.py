"""A local evidence document displaying actual saved results, suitable for screen capture."""
from pathlib import Path
import html,json
import pandas as pd
R=Path(__file__).resolve().parents[1];O=R/'outputs'
style='''body{font:18px -apple-system,Arial,sans-serif;color:#16324f;background:#eef3f6;margin:0;padding:24px}section{background:white;padding:28px;margin:0 auto 40px;max-width:1250px;border-radius:12px}h1{font-size:32px}h2{color:#007f82}table{border-collapse:collapse;width:100%;font-size:17px}th,td{padding:8px 12px;border-bottom:1px solid #d6e0e7;text-align:left}th{background:#16324f;color:white}pre{white-space:pre-wrap;background:#f0f5f7;padding:16px;font-size:16px}.note{color:#536879;font-size:16px}nav a{margin-right:24px}'''
parts=[f'<!doctype html><html><head><meta charset="utf-8"><title>GridPulse | Recorded execution evidence</title><style>{style}</style></head><body><nav><a href="#sql">Spark SQL</a><a href="#stream">Streaming</a><a href="#quality">Data quality</a></nav>']
def section(key,title,source,body):
    parts.append(f'<section id="{key}"><h1>{title}</h1><p class="note">Recorded execution evidence | {html.escape(source)} | Historical telemetry replay, local[4]</p>{body}</section>')
query=(O/'sql/top_days.sql').read_text()
section('sql','GridPulse - Spark SQL query and result','outputs/sql/top_days.sql + top_days.csv',f'<pre>{html.escape(query)}</pre>'+pd.read_csv(O/'sql/top_days.csv').to_html(index=False,border=0))
v=json.loads((O/'streaming/validation.json').read_text());logs=json.loads((O/'streaming/replay_progress.json').read_text())
progress=pd.DataFrame([dict(batch_id=p['batchId'],input_rows=p['numInputRows'],watermark=(p.get('eventTime') or {}).get('watermark')) for p in logs if p['numInputRows']])
snap=pd.read_csv(O/'streaming/replay_hourly.csv');sample=pd.concat([snap[snap.scoring_status!='eligible'],snap[snap.scoring_status=='eligible'].head(4)])
body=f'<h2>{v["real_records_replayed"]:,} real input records | checkpoint restart | batch parity {v["batch_stream_parity"]}</h2>'
body+='<h2>Actual micro-batch progress</h2>'+progress.to_html(index=False,border=0)
body+='<h2>Hourly aggregates and scoring eligibility (selected windows)</h2>'+sample[['start','records','mean_kw','scoring_status','alert','window_state']].round(4).fillna('UNSCORED').to_html(index=False,border=0)
section('stream','GridPulse - Structured Streaming results','replay_progress.json + replay_hourly.csv',body)
section('quality','GridPulse - Input validation','quality.json + rejections.json','<pre>'+html.escape(json.dumps(json.loads((O/'quality.json').read_text()),indent=2))+'</pre>')
parts.append('</body></html>');(O/'evidence.html').write_text('\n'.join(parts))
print(O/'evidence.html')
