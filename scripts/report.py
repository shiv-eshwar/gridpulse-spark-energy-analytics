"""Generate a reproducible PDF report and figures from measured artifacts only."""
from pathlib import Path
import json, html
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, Preformatted
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import inch
R=Path(__file__).resolve().parents[1]; O=R/'outputs'; F=O/'figures'; F.mkdir(exist_ok=True)
P=R/'output/pdf'; P.mkdir(parents=True,exist_ok=True)
submission=json.loads((R/'submission.json').read_text())
environment=json.loads((O/'environment.json').read_text())
q=json.loads((O/'quality.json').read_text()); split=json.loads((O/'splits.json').read_text())
sv=json.loads((O/'streaming/validation.json').read_text())
m=pd.read_csv(O/'anomaly_metrics.csv'); monthly=pd.read_csv(O/'sql/monthly.csv')
b=pd.read_csv(O/'partition_benchmark.csv'); late=pd.read_csv(O/'streaming/watermark_experiment.csv')
delays=pd.read_csv(O/'streaming/delayed_replay_experiment.csv')
joint=pd.read_csv(O/'streaming/joint_fault_lateness.csv')
coverage=pd.read_csv(O/'sql/monthly_coverage.csv')
rejections=json.loads((O/'rejections.json').read_text())
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titlesize':14})
navy='#16324f'; teal='#007f82'; orange='#db742e'
def save(name):
    plt.tight_layout();plt.savefig(F/name,dpi=180,bbox_inches='tight');plt.close()
def box(ax,x,y,w,h,text,fc=navy):
    ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0.015',facecolor=fc,edgecolor='none'))
    ax.text(x+w/2,y+h/2,text,ha='center',va='center',color='white',fontsize=10)
fig,ax=plt.subplots(figsize=(10,4));ax.axis('off');ax.set(xlim=(0,1),ylim=(0,1))
box(ax,.03,.66,.27,.23,'Driver / SparkSession\nDAG + task scheduling')
box(ax,.37,.66,.26,.23,'Resource allocation\nlocal[4]: local scheduler',teal)
box(ax,.71,.66,.26,.23,'Local JVM execution\n4 task threads + cache')
box(ax,.03,.10,.27,.23,'Production alternative\nStandalone / YARN / K8s',teal)
box(ax,.38,.10,.25,.23,'Executor JVM 1\nTasks / shuffle / cache')
box(ax,.71,.10,.26,.23,'Executor JVM N\nTasks / shuffle / cache')
for x1,x2 in [(.3,.37),(.63,.71)]:ax.annotate('',xy=(x2,.77),xytext=(x1,.77),arrowprops={'arrowstyle':'->','color':navy})
ax.annotate('',xy=(.38,.22),xytext=(.30,.22),arrowprops={'arrowstyle':'->','color':navy})
ax.annotate('',xy=(.84,.10),xytext=(.16,.10),arrowprops={'arrowstyle':'->','color':navy,'connectionstyle':'arc3,rad=0.12'})
ax.text(.5,.49,'TOP: executed laptop topology | BOTTOM: conceptual cluster deployment',ha='center',fontsize=10)
save('architecture.png')
fig,ax=plt.subplots(figsize=(10,3));ax.axis('off');ax.set(xlim=(0,1),ylim=(0,1))
items=['UCI source\n2.075M rows','RDD parsing\nmap / filter / reduce','DataFrame + SQL\nquality / hourly','Analytical output\ncharts / metrics']
for i,label in enumerate(items):
    x=.015+i*.25;box(ax,x,.60,.22,.30,label,teal if i%2 else navy)
    if i<3:ax.annotate('',xy=(x+.25,.75),xytext=(x+.22,.75),arrowprops={'arrowstyle':'->'})
box(ax,.015,.08,.30,.28,'Atomic JSON file arrivals\n21,600 real records',teal)
box(ax,.365,.08,.28,.28,'Watermark + hourly window\nstatic baseline join')
box(ax,.695,.08,.28,.28,'Checkpointed update sink\nalerts + batch parity',teal)
for x1,x2 in [(.315,.365),(.645,.695)]:ax.annotate('',xy=(x2,.22),xytext=(x1,.22),arrowprops={'arrowstyle':'->'})
save('workflow.png')
plt.figure(figsize=(10,3));plt.plot(monthly.month,monthly.observed_kwh,color=teal,marker='.',lw=1.5)
plt.xticks(np.arange(0,len(monthly),4),monthly.month.iloc[::4],rotation=40);plt.ylabel('Observed energy (kWh)');plt.title('Monthly consumption: incomplete months and missing readings are not normalized');save('monthly.png')
hp=pd.read_csv(O/'sql/hour_profile.csv');plt.figure(figsize=(10,3));plt.plot(hp.hour,hp.mean_kw,label='Mean',color=teal);plt.plot(hp.hour,hp.p95_kw,label='95th percentile',color=orange);plt.xlabel('Source wall-clock hour');plt.ylabel('Active power (kW)');plt.legend();save('daily_profile.png')
g=m.groupby(['model','kind','strength_kw']).agg(f1=('f1','mean'),std=('f1','std')).reset_index()
fig,axes=plt.subplots(1,2,figsize=(10,3.5),sharey=True)
for ax,(kind,d) in zip(axes,g.groupby('kind')):
    for name,x in d.groupby('model'):ax.errorbar(x.strength_kw,x.f1,yerr=x['std'],marker='o',capsize=3,label=name)
    ax.set(title=kind.replace('_',' '),xlabel='Injected increase (kW)',ylim=(0,1));ax.grid(alpha=.2)
axes[0].set_ylabel('Pointwise F1 vs injected labels');axes[1].legend(fontsize=8)
save('anomaly_comparison.png')
bs=b.groupby('partitions').seconds.agg(['median','min','max']);plt.figure(figsize=(8,3));plt.errorbar(bs.index,bs['median'],yerr=[bs['median']-bs['min'],bs['max']-bs['median']],fmt='o-',capsize=5,color=teal)
plt.xticks(bs.index);plt.xlabel('Repartition count');plt.ylabel('Elapsed seconds');plt.title('Local[4] cached full-source aggregation: median and range of 3 runs');save('partitions.png')
fig,ax=plt.subplots(figsize=(8,3));ax.bar(late.watermark_minutes.astype(str),late.accepted_records,color=[orange,orange,teal]);ax.axhline(4,color=navy,ls='--',label='4 source events');ax.set(xlabel='Watermark delay (minutes)',ylabel='Retained records',ylim=(0,4.7));ax.legend();save('late_arrivals.png')
# Output tables rendered directly from measured CSVs are evidence figures, not fabricated UI screenshots.
for name,frame in [('sql_monthly',monthly.head(12)),('watermark_results',late),('anomaly_results',g.round(3))]:
    fig,ax=plt.subplots(figsize=(11,max(2,len(frame)*.25+.8)));ax.axis('off')
    table=ax.table(cellText=frame.astype(str).values,colLabels=frame.columns,loc='center');table.auto_set_font_size(False);table.set_fontsize(8);table.scale(1,1.3)
    for (row,col),cell in table.get_celld().items():
        if row==0:cell.set_facecolor(navy);cell.set_text_props(color='white')
    save(name+'.png')
styles=getSampleStyleSheet();styles.add(ParagraphStyle(name='Cover',fontSize=30,leading=35,textColor=colors.HexColor(navy),spaceAfter=18))
styles['BodyText'].fontSize=10;styles['BodyText'].leading=14;styles['BodyText'].spaceAfter=9
styles['Heading1'].textColor=colors.HexColor(navy);styles['Heading2'].textColor=colors.HexColor(teal)
story=[]
def para(text,style='BodyText'):story.append(Paragraph(text,styles[style]))
def title(text):para(text,'Heading1')
def pic(name,width=6.8):
    from PIL import Image as PIL
    im=PIL.open(F/name); w,h=im.size;story.append(Image(str(F/name),width=width*inch,height=width*inch*h/w));story.append(Spacer(1,8))
def table(headers,rows,widths=None):
    values=[[Paragraph(html.escape(str(x)),styles['BodyText']) for x in row] for row in [headers]+rows]
    t=Table(values,colWidths=widths,repeatRows=1,hAlign='LEFT');t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e3eef2')),('VALIGN',(0,0),(-1,-1),'TOP'),('LINEBELOW',(0,0),(-1,0),.6,colors.HexColor(navy)),('BOTTOMPADDING',(0,0),(-1,-1),5)]));story.append(t)
def page():story.append(PageBreak())
para('GridPulse','Cover');para('Event-Time Reliability and Seasonality-Aware Anomaly Detection for Household Energy Telemetry with Apache Spark','Heading1')
para('Mining of Massive Datasets | Assignment 2 | CAL 2 technical report','Heading2')
para('Execution-backed prototype • UCI Household Power Consumption • PySpark 3.5.3'.replace('•','|'))
pic('workflow.png')
para(f"<b>Measured scope:</b> {q['source_rows']:,} raw readings; {q['valid_rows']:,} usable readings; {sv['real_records_replayed']:,} real readings replayed through Structured Streaming.")
if submission['group_members']:
    para('<b>Group:</b> '+html.escape(', '.join(str(x) for x in submission['group_members'])))
if submission['institution']:para(html.escape(submission['institution']))
if submission['instructor']:para('Instructor: '+html.escape(submission['instructor']))
if not (submission['assignment_1_dataset_confirmed'] or submission['dataset_change_approved']):
    para('<b>Administrative requirement:</b> confirm CAL 1 dataset continuity or instructor approval for this choice, and merge these sections into the actual CAL 1 report. Group metadata is editable in submission.json. No approval or personal details have been assumed.')
para('<b>Research status:</b> a reproducible systems and analytics prototype. Conference readiness, algorithmic novelty and natural fault accuracy have not been established.')
page();title('1. Abstract and motivation')
para(f"GridPulse implements an end-to-end Spark pipeline over {q['source_rows']:,} minute-level household electricity measurements. It ingests semicolon-delimited source records through RDDs, applies explicit parsing and quality checks, converts accepted records into a typed DataFrame, and produces energy summaries through Spark SQL. The implementation independently reconciles RDD and SQL monthly totals. A file-arrival simulation replays {sv['real_records_replayed']:,} real readings through event-time hourly windows, scores them against a static seasonality-aware baseline, and validates outputs after a checkpoint restart.")
para('The analytical experiment compares global, hour-of-day and hour-of-week robust baselines. Models use 2006-2008 data; thresholds are calibrated using 2009; 2010 remains held out. Controlled positive spikes and six-hour shifts evaluate sensitivity at three amplitudes and three seeds. A separate four-event probe demonstrates the retention effect of three watermark budgets. These experiments connect analytic quality to streaming semantics without assuming natural anomaly labels exist.')
para('<b>Problem:</b> a high power reading may represent expected evening usage or an unusual departure from a temporal pattern. Furthermore, a delayed record may be omitted after state eviction. Therefore a useful analytics pipeline must document both its anomaly calibration and the completeness of its streaming aggregates.')
para('<b>Research question:</b> How does seasonal conditioning change sensitivity to controlled energy perturbations, and how does event-time lateness affect the evidence retained by a streaming pipeline? A real-data delayed replay also measures aggregate error and alert changes; detection delay under sustained load remains future work.')
para('Related work includes transformer-based energy anomaly detection with synthesized data [3], critiques of anomaly benchmark validity [4], learned anomaly generation [5], and comparisons questioning whether complex models improve on conventional baselines [6]. This prototype uses simple, inspectable baselines to focus on event-time evidence retention. It does not claim to outperform those papers or introduce a new detector.')
para('<b>Objectives:</b> cover the Spark architecture, RDD transformations and actions, typed DataFrames, cleaning, grouping and SQL, processing workflow, and Structured Streaming rubric; preserve measured results and reproducibility evidence; identify the next experiments needed for credible research.')
page();title('2. Dataset selection and provenance')
table(['Candidate','Fit and tradeoff'],[
 ['UCI Household Power Consumption - selected','2.075M timestamps, real missing readings, seven electrical measures; modest download, no authentication, reproducible replay; one household and no fault labels.'],
 ['NYC TLC taxi records - considered','Large timestamped transaction data; strong spatial analytics, but trip completion versus event arrival semantics and external context require additional design. Not downloaded.'],
 ['Network intrusion benchmarks - considered','Rich security labels but often suffer capture-specific leakage and unclear realistic stream timing. Not downloaded.']], [150,340])
para('Selected source: Georges Hebrail and Alice Berard, <i>Individual Household Electric Power Consumption</i>, UCI Machine Learning Repository, DOI 10.24432/C58K54. Licence: Creative Commons Attribution 4.0. The primary source describes one home in Sceaux, France, sampled every minute from December 2006 through November 2010 [1].')
para('Input columns: Date, Time, global active power (kW), reactive power (kVAR), voltage (V), current (A), and kitchen/laundry/heating-cooling submeters (Wh per minute). One-minute active energy is active_kw / 60 kWh. Other-appliance energy is active_kw * 1000 / 60 minus the three submeters in Wh. Negative residuals are counted and retained. Their physical cause is unknown; rounding or measurement alignment would require supporting metadata.')
para('The source archive is downloaded by scripts/download.py. Downloads use retries and atomic rename. Cached ZIP/text bytes must match pinned SHA-256 values, and ZIP CRC is checked. A changed extracted file is restored from the verified archive; an incomplete archive is downloaded again. Validated hashes are written to data/raw/manifest.json. Original source data is retained. Missing, malformed, non-finite or negative measurement records are rejected; duplicates are removed by timestamp. Rejected rows are accounted for rather than silently changing the denominator.')
para('Streaming means replaying a historical timestamped source, as explicitly permitted by the assignment. It is not a live utility feed. Source times have no timezone offset: Spark uses a UTC session to preserve a deterministic wall-clock interpretation. This does not establish true UTC event instants; French daylight-saving transitions cannot be reconstructed from missing timezone metadata.')
page();title('3. Spark environment and architecture (CO2)');pic('architecture.png')
para(f"Executed stack: Python {q['python']}, Apache Spark {q['spark']}, Java 17, {q['master']}. Host: {q['hardware']}. Hardware details are recorded in outputs/environment.json. Dependencies are isolated in .venv and captured in requirements.lock.txt. JAVA_HOME is configured by scripts/env.sh; no shell profile changes are required.")
para('The driver constructs SparkSession, holds the SparkContext, builds the logical plan or RDD lineage, schedules stages and coordinates results. In a distributed deployment a cluster manager such as Standalone, YARN or Kubernetes allocates executor resources. Executors run partition tasks, cache data and exchange shuffle outputs. The executed local[4] deployment has four task threads in one local JVM; it is not four machines or a multi-executor cluster.')
para('Spark transformations are lazy. filter and map build a narrow-dependency lineage; reduceByKey and grouping introduce shuffle dependencies, splitting stage boundaries. Actions such as count, collect and write trigger jobs, with one task per partition in a stage. DataFrame logical plans are optimized by Catalyst and may be revised by adaptive query execution. Python RDD parsing incurs Python worker and serialization overhead that built-in DataFrame expressions can avoid.')
para('The batch source uses at least eight partitions and the SQL shuffle target is eight. Intermediate parsed RDDs and clean DataFrames are cached to avoid repeated work. Only small aggregated results and bounded hourly/test/replay data are collected to the driver. The full clean dataset remains distributed in Parquet. Event logs and query plans are retained for execution-model inspection; a running application exposes a transient Spark UI on localhost:4040.')
page();title('4. RDD ingestion and validation (CO2)')
para('Implementation: src/batch.py. SparkContext.textFile loads the actual UCI source as an RDD. filter removes the header. map(parse_line) validates nine fields, parses the timestamp and converts seven finite nonnegative measurements. filter removes failed parses. This gives a direct and auditable RDD entry point rather than creating an RDD indirectly from a DataFrame.')
para('flatMap emits a (submeter, 1) pair for every positive kitchen, laundry or HVAC measurement. A single record may emit zero to three keys. reduceByKey sums active minutes and demonstrates map-side combining. A second map and reduceByKey compute monthly (energy sum, valid-minute count) tuples. sortByKey orders the output. count and collect are the explicit actions used for validation and small aggregate reporting.')
rdd=json.loads((O/'rdd_results.json').read_text())
table(['Check','Measured value'],[[k,str(q[k])] for k in ['source_rows','valid_rows','rejected_rows','duplicate_timestamps','negative_unmetered_minutes','rdd_sql_reconciliation']],[240,250])
table(['Submeter','Minutes with positive measurement'],[[k,v] for k,v in rdd['submeter_active_minutes'].items()],[240,250])
para('The RDD monthly energy and valid-minute results are compared with independent SQL results for every month. Month key sets and cardinality must agree before values are compared. Counts must agree exactly and energy must agree within 0.001 kWh after the SQL display rounding. This is a meaningful cross-API correctness check, not a comparison of two copies of the same output. Regression tests cover parsing, timezone stability, interrupted downloads, source tampering, nonfinite streaming input, scoring eligibility, missing profiles and key-set mismatches.')
para('Evidence: outputs/rdd_results.json contains source counts, reductions and RDD lineage; outputs/logs/tests.log records parser tests; outputs/quality.json records reconciliation. Source rows with absent measurements remain represented in the raw file and in rejected-row accounting.')
page();title('4b. Data-quality diagnostics')
table(['Parsing result','Rows'],[[k,v] for k,v in rejections['reason_counts'].items()],[300,190])
para('Rejections.json contains raw examples and explicit reason categories. Monthly coverage divides accepted observations by raw source timestamps in that month, retaining partial endpoint months. Coverage is not a prediction of missing consumption. Missing_runs.csv lists contiguous rejected-reading intervals, so a file containing 3,600 valid observations can span much longer than 60 clock hours.')
worst=coverage.sort_values('coverage_fraction').head(6)
table(['Month','Source minutes','Valid minutes','Coverage'],[[r.month,int(r.source_minutes),int(r.valid_minutes),f'{r.coverage_fraction:.2%}'] for r in worst.itertuples()],[110,130,130,120])
runs=pd.read_csv(O/'missing_runs.csv').head(5)
para('Longest missing-measurement runs','Heading2')
table(['Start','End','Minutes'],runs[['start','end','missing_minutes']].values.tolist(),[190,190,110])
para(f"The residual submeter calculation is below -0.1 Wh for {q['negative_unmetered_minutes']:,} minutes. Its measured minimum is {q['minimum_unmetered_wh']:.2f} Wh. Residual_diagnostics.json records quantiles. These flags remain available for inspection without assuming a physical cause or silently clamping them to zero.")
page();title('5. DataFrames, Spark SQL and dataflow (CO4)')
para('Accepted RDD tuples are converted using an explicit StructType: timestamp followed by seven doubles. Cleaning removes timestamp duplicates and adds one-minute kWh and residual submeter energy. Grouping into hourly features produces valid-minute coverage, mean kW, observed kWh, hour-of-day and hour-of-week. Detection uses hours with at least 54 valid minutes; no future interpolation is performed. Monthly totals are observed totals and must not be interpreted as complete consumption when coverage is incomplete.')
para('The saved SQL queries implement monthly trends, top days with at least 1,380 observed minutes, hourly usage profiles with approximate 95th percentiles, and submeter energy allocation. They demonstrate filtering (WHERE/HAVING), aggregation, GROUP BY, ordering and a temporary SQL view. Query text, result CSVs and the optimized/physical monthly query plan are retained.')
pic('monthly.png');pic('daily_profile.png')
peak=monthly.loc[monthly.observed_kwh.idxmax()]; peak_hour=hp.loc[hp.mean_kw.idxmax()]
para(f"<b>Observed patterns:</b> the largest observed monthly total is {peak.observed_kwh:,.1f} kWh in {peak.month}. Mean active power peaks at source hour {int(peak_hour.hour):02d}:00 ({peak_hour.mean_kw:.2f} kW). This is descriptive evidence from one household; different coverage and month lengths prevent interpreting raw monthly ranks as weather-adjusted or population-level trends.")
para('Dataflow: immutable raw archive -> RDD parsing/reductions -> typed clean DataFrame -> Parquet hourly features and SQL outputs -> calibrated baselines -> replayed event-time windows -> saved scores, validation metrics and figures. Architecture and workflow diagrams are also exported as separate PNG figures.')
page();title('6. Structured Streaming implementation (CO4)')
para(f"Implementation: src/streaming.py. A producer atomically renames JSON files into a watched directory. Six files each contain 3,600 real readings. The consumer uses readStream with an explicit timestamp/double schema, maxFilesPerTrigger=1, invalid-row filtering, a two-hour event-time watermark and one-hour tumbling windows. It rejects null, nonfinite and negative power, computes record count, observed kWh and mean kW, then joins a static hour-of-week profile. The model choice is a predetermined demonstrator, not a selection based on test labels. Calibration thresholds live in a separate calibration.json artifact derived only from 2009.")
para('A foreachBatch update sink writes each batch to a stable batch-ID directory in overwrite mode. This is idempotent for local single-writer retries of the same batch, but does not prove arbitrary external-sink exactly-once delivery. Latest snapshots are selected by window start and maximum batch ID, so update rows are not summed across revisions. The exported view labels a window finalized when its end is at or before the last observed query watermark, and provisional otherwise. Scoring status is separate: eligible, insufficient_coverage or missing_profile. Windows below 54 valid readings have a null score and alert, rather than a false prediction.')
para('The driver waits for each micro-batch to drain before publishing the next file. This is an accelerated simulation of periodic file arrivals, not a processing-rate or end-to-end latency benchmark. A one-second processing trigger is configured. The query stops after file three, restarts with the same checkpoint and processes files four through six. The final latest-window view is compared to an independent pandas grouping of the same real input readings.')
table(['Replay validation','Measured result'],[[k,v] for k,v in sv.items() if k in ['real_records_replayed','hourly_windows','max_energy_error_kwh','checkpoint_restart_after_files','batch_stream_parity','eligible_windows','insufficient_coverage_windows']],[250,240])
para('Checkpoint directories preserve source offsets and aggregation state. They must be paired with the same source and compatible query topology. This scripted experiment clears only its own outputs/streaming directory at the start for repeatability, then tests recovery within that run. It does not test a machine failure or transactional sink failure.')
para('Progress JSON retains input rows, event-time watermark, batch durations, state memory, state-row counts and dropped-late-row counters. This supplies direct evidence of incoming records, aggregation and watermark behavior. Actual progress JSON, selected completed Spark event logs, stage metrics and genuine browser screenshots accompany the report. Tables and charts remain separate from screenshots. The live-demo script keeps the UI open without resetting submission evidence.')
page();title('7. Late-data experiment and interpretation')
pic('late_arrivals.png');table(list(late.columns),late.values.tolist(),[125,110,125,130])
para('The controlled probe has arrivals at 00:00, 00:50, 02:00, followed by a delayed 00:10 record, all on the same date. Every event has a 1 kW reading. The first two events share a one-hour window. Advancing the maximum event time to 02:00 allows short-watermark queries to evict the first window before the delayed event arrives. A 180-minute budget retains that window and accepts the delayed event. The late probe is synthetic; the principal 21,600-record replay is real historical input.')
para('A watermark is computed from maximum observed event time minus the delay. State eviction and row-dropping rules depend on the window boundary and the watermark available from previous micro-batches. Watermark time does not advance just because the wall clock advances. Records within the lateness budget are protected by the documented semantics; records beyond it may or may not be accepted depending on operator and state progress.')
para('These results establish event-time retention semantics for this deterministic sequence. They do not establish an optimal production watermark. A larger budget can preserve more delayed evidence but increases retained state and can delay final append-mode output. The update-mode snapshots used here are provisional; finalization latency is not measured. A publication experiment should sample realistic arrival delays and compare alert delay and memory under fixed input rates.')
page();title('7b. Real-data delayed replay: reliability versus state')
table(['Budget (h)','Source records','Retained records','Retention','Energy error (kWh)','Alert changes','Peak state (bytes)'],[[int(r.watermark_hours),int(r.source_records),int(r.retained_records),f'{r.retention_fraction:.5f}',f'{r.total_absolute_energy_error_kwh:.5f}',int(r.alert_disagreements),int(r.max_state_memory_bytes)] for r in delays.itertuples()],[58,65,65,65,88,70,79])
para('This second experiment changes arrival order while keeping every source timestamp and measurement intact. Every tenth record is withheld from each 3,600-record file and released with the next file. Each file contains 60 hours of valid observations, but spans more clock time across missing-data gaps; the final deferred set is released as a seventh file. All three watermark configurations receive the same 21,600 real readings. No synthetic late timestamps or finalizer records are inserted.')
para('A two-hour budget can evict hourly state before many delayed measurements arrive. A 72-hour budget can still lose evidence because missing-data gaps lengthen file spans. A 168-hour budget protects this one-file delay schedule and is asserted to preserve every record and reproduce offline energy within numerical tolerance. Metrics compare the latest streaming window snapshots against the chronological reference: retained record fraction, sum of absolute hourly energy errors, alert disagreements, and peak state memory observed in progress logs.')
para('The energy-error metric is an hourly absolute-error total, not relative error and not household billing loss. Alert disagreements compare only windows eligible in both runs; unscored windows are reported separately. All three unperturbed runs have zero alert disagreements. Thus this experiment demonstrates retention and energy fidelity, without establishing a fault-detection gain. Peak state memory measures the streaming state operator rather than total JVM resident memory.')
para('This demonstrates a concrete retention/aggregate-fidelity/state tradeoff on real data. It does not establish that 168 hours is optimal or that larger watermarks always improve performance. Arrival delay is deterministic, source time is accelerated, and only three budgets are tested at this scale. The next section tests controlled minute-level faults. Random delay distributions and detection latency under sustained load remain future work.')
page();title('7c. Controlled faults under delayed arrivals')
protocol=json.loads((O/'streaming/joint_protocol.json').read_text())
para(f"A separate experiment inserts a +{protocol['amplitude_kw']} kW synthetic burst into every tenth minute of 12 randomly selected eligible hours (seed {protocol['seed']}). All other source values remain unchanged. The same arrival schedule delays every tenth record by one file, so late evidence overlaps the perturbations. The amplitude is a stress-test parameter, not a calibrated model of household faults.")
para('An independent pandas reference aggregates the perturbed minute records and applies the same predetermined profile and 2009 threshold. The stream uses 2-hour and 168-hour budgets. Labels identify hours containing an injected perturbation. Non-injected hours have no verified natural fault labels, so confusion counts measure agreement with the controlled injection task only.')
table(['Budget (h)','TP','FP','FN','Recall','F1','Changed alerts'],[[r.watermark_hours,int(r.tp),int(r.fp),int(r.fn),f'{r.recall:.3f}',f'{r.f1:.3f}',int(r.alert_disagreements)] for r in joint.itertuples()],[90,50,50,50,80,80,90])
para('Alert changes compare windows eligible in both the offline and streaming views. The end-to-end recall denominator includes injected hours that become unscored, and reports those unscored fault hours separately in the CSV. The 168-hour replay is asserted to reproduce offline mean power and eligible alert decisions. This protocol tests final snapshot quality; a stopped-query comparison cannot measure first-alert latency.')
offline=joint[joint.watermark_hours.astype(str)=='offline'].iloc[0]
short=joint[joint.watermark_hours.astype(str)=='2'].iloc[0]
para(f"<b>Measured result:</b> the offline reference detects {int(offline.tp)} of {int(offline.tp+offline.fn)} injected hours (recall {offline.recall:.1%}). The two-hour replay detects {int(short.tp)} and changes {int(short.alert_disagreements)} eligible decisions. The seven-day replay matches the offline reference. This shows that delayed evidence can change an alert in this controlled case, while the low offline recall exposes the limits of hourly averaging and this static detector.")
para('Hourly averaging can dilute short faults. A negative result or missed event therefore motivates future peak/residual features rather than supporting a claim of reliable fault detection. The saved protocol lists every injected hour and the number of perturbed minute records, allowing the comparison to be independently reconstructed.')
page();title('8. Seasonality-aware anomaly experiment')
para('Let x be mean hourly kW. Each model estimates a training median m and median absolute deviation MAD, with scale s = max(1.4826 * MAD, 0.05 kW). The score is |x - m| / s. The global model uses one profile; hour-of-day uses 24 profiles; hour-of-week uses 168 profiles. Approximate medians use Spark percentile_approx with accuracy 10,000. Each threshold is the approximate 99th percentile of that model\'s 2009 calibration scores, never optimized on injected 2010 labels.')
table(['Split','Hours with >=54 valid minutes'],[['Training: through 2008',split['train_hours']],['Calibration: 2009',split['validation_hours']],['Held-out test: 2010',split['test_hours']]],[280,210])
para('Controlled scenarios inject +1, +2 or +4 kW into approximately 1% of test hours. Point spikes use independent sampled hours; shifts use sampled non-overlapping six-row blocks. Seeds 7, 19 and 41 generate identical perturbations across all models. Shift candidates must span six consecutive clock hours, and blocks are non-overlapping, so missing-hour gaps cannot become false continuous events. The perturbation amplitude is absolute and model-independent to avoid favoring a seasonal baseline by construction.')
pic('anomaly_comparison.png')
para('Precision, recall, F1 and background alert rate are pointwise against injection indicators. Non-injected readings are not confirmed normal: a flag on them counts as a false positive only for this injection task. No point-adjusted scoring is used. Error bars show sample standard deviation over three seeds; they are not confidence intervals. Saved outputs retain all confusion counts and thresholds.')
page();title('9. Results, scalability and limitations')
avg=m.groupby('model')[['precision','recall','f1','background_alert_rate']].mean().round(4)
table(['Model','Precision','Recall','F1','Background alert rate'],[[idx,*row] for idx,row in avg.iterrows()],[125,90,90,90,95])
best=avg.f1.idxmax()
para(f"<b>Measured comparison:</b> {best.replace('_',' ')} has the highest average F1 in this controlled suite ({avg.loc[best,'f1']:.4f}). This pooled average weights each amplitude and fault type equally. Seasonal models do better on small injections, while the global model is stronger on large injections. Calibration controls the 2009 score distribution, not the 2010 background alert rate; household behavior drift and profile granularity are plausible explanations that require additional tests, not established causes.")
para('These averages combine both perturbation types, all amplitudes and three seeds. They summarize this controlled suite only; the per-scenario CSV and plot are the appropriate basis for examining strengths and weaknesses. A more granular seasonal profile is not guaranteed to improve detection: sparse profile estimates and changing household behavior can increase background alerts. Results should be reported even when the simpler global baseline is stronger.')
conditional=m[m.kind=='spike'].groupby(['model','strength_kw']).f1.mean().unstack()
para('Sensitivity depends on injection strength','Heading2')
table(['Model','F1: +1 kW spike','F1: +4 kW spike'],[[name,f'{row[1.0]:.3f}',f'{row[4.0]:.3f}'] for name,row in conditional.iterrows()],[210,140,140])
para('Three seeds provide paired repeatability, not confidence intervals. A deployment-weighted average would need a justified fault distribution. Changing model selection after seeing 2010 results would consume the held-out test set, so the streaming demonstrator retains its preselected weekly profile.')
page();title('9b. Execution and partition sensitivity');pic('partitions.png')
para(f"The partition experiment operates on all {q['valid_rows']:,} clean minute records, with narrow columns cached, then repartitions and aggregates mean power by hour-of-week. It uses counts 2, 4, 8 and 16, performs one warm-up per count outside the timing, randomizes 12 measured trials, and reports median and range of three timings. Driver collection is bounded to 168 aggregate rows. These are local partition-sensitivity timings including shuffle and scheduling, not distributed speedup or strong-scaling measurements. OS caches, JIT, AQE and shared laptop load remain confounders.")
volume=pd.read_csv(O/'volume_benchmark.csv')
vol=volume.groupby('input_rows').seconds.agg(['median','min','max']).reset_index()
para('Local data-volume experiment','Heading2')
table(['Input rows','Median (s)','Min (s)','Max (s)'],[[int(r.input_rows),f'{r.median:.3f}',f'{r.min:.3f}',f'{r.max:.3f}'] for r in vol.itertuples()],[160,110,110,110])
para('The volume experiment fixes local[4] and eight repartitioned inputs, samples 25%, 50% and 100% of valid source rows with seed 2026, and warms each cached sample before three timed aggregations. It shows local workload sensitivity. Increasing data volume while fixing one JVM does not measure multi-executor scaling.')
para('Limitations: one household, historical data, unknown true fault labels, missing readings and incomplete months, unresolved daylight-saving metadata, static profiles subject to drift, small injected-anomaly family, three seeds, no distributed cluster, no realistic backpressure workload and only one deterministic lateness schedule. The new joint fault replay measures final snapshot decisions, not alert latency. Full-source batch processing and replay exceed the assignment size minimum, but that alone does not establish research novelty.')
page();title('10. Rubric mapping, reproduction and conclusion')
table(['Rubric / marks','CO','Implementation and evidence'],[
 ['Architecture and workflow / 3','CO2','Section 3; architecture.png, workflow.png, event_logs, query_plan.txt'],
 ['RDD implementation / 4','CO2','src/batch.py; map/filter/flatMap/reduceByKey; rdd_results.json; reconciliation and parser tests'],
 ['DataFrames and SQL / 3','CO4','Explicit schema, cleaning, hourly grouping, saved SQL and result CSVs; Section 5'],
 ['Structured Streaming / 2','CO4','src/streaming.py; real replay, window aggregates, checkpoint restart, progress logs, watermark probe'],
 ['Results and discussion / 3','CO4','Measured quality, temporal experiment, late-data evidence, partition timings, limitations and research plan']], [150,40,300])
para('<b>Reproduce:</b> source scripts/env.sh; bash scripts/run_all.sh. The script validates/downloads the source, runs tests, processes the full dataset, executes the experiment suite and streaming replay, then builds this PDF. Raw data, environment and result directories are excluded from source control. requirements.lock.txt captures the executed Python stack. On a new machine install Java 17 and use scripts/setup.sh first.')
para('<b>Evidence guide:</b> outputs/logs for execution; outputs/sql for queries and results; outputs/streaming for file arrivals, snapshots, checkpoints and progress; outputs/figures for report graphics. docs/VIVA_AND_DEMO.txt contains a demonstration sequence and technical answers. docs/RESEARCH_PLAN.txt defines the publication gate and missing CAL 1 continuity.')
para('<b>Conclusion:</b> GridPulse demonstrates the complete Spark workflow with tested cross-API consistency, a real-data streaming replay and explicit late-arrival semantics. Its research value is currently the reproducible experimental foundation. A credible conference submission requires stronger labelled datasets, competing methods, realistic delayed arrivals, paired uncertainty estimates and actual multi-executor measurements. The present report makes no guarantee of publication or marks.')
page();title('11. References and submission checklist')
refs=[
 '[1] G. Hebrail and A. Berard. Individual Household Electric Power Consumption. UCI Machine Learning Repository. DOI: 10.24432/C58K54. https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption. CC BY 4.0. Primary metadata verified during this build.',
 '[2] Apache Spark 3.5.3 documentation: RDD Programming Guide; Spark SQL, DataFrames and Datasets Guide; Structured Streaming Programming Guide. https://spark.apache.org/docs/3.5.3/ . Implementation references; consult the versioned documentation for operator guarantees.',
 '[3] T. Mu, Y. Yu, G. Feng, H. Luo and H. Yang. Detecting anomalous electricity consumption with transformer and synthesized anomalies. PeerJ Computer Science (2023). DOI: 10.7717/peerj-cs.1721. https://pmc.ncbi.nlm.nih.gov/articles/PMC10702936/ . Related domain work; its reported dataset and scores are not directly comparable to this project.',
 '[4] Current Time Series Anomaly Detection Benchmarks are Flawed and are Creating the Illusion of Progress. arXiv:2009.13807. https://arxiv.org/abs/2009.13807 . Motivates explicit benchmark limitations and honest evaluation.',
 '[5] GenIAS: Generator for Instantiating Anomalies in Time Series. arXiv:2502.08262. https://arxiv.org/abs/2502.08262 . Related synthetic anomaly generation; GridPulse uses simple controlled injections, not GenIAS.',
 '[6] Do Deep Neural Networks Contribute to Multivariate Time Series Anomaly Detection? arXiv:2204.01637. https://arxiv.org/abs/2204.01637 . Context for retaining conventional baselines rather than assuming complexity improves results.'
]
for ref in refs:para(html.escape(ref))
para('<b>Before submission:</b> confirm the CAL 1 dataset/report match; add actual group details and contributions; review the supplied genuine Spark and result-view screenshots and rehearse the live demonstration; review the report and be able to explain the saved negative results. Exported result figures are actual measurements but are not claimed as UI screenshots.')
page();title('12. Query and streaming result evidence')
para('The following figures render actual saved result CSVs. They are execution evidence, not browser or terminal screen captures. Genuine browser screenshots of the Spark UI and the result evidence document follow in the screenshot appendix.')
para('Monthly Spark SQL output (first twelve rows)','Heading2');pic('sql_monthly.png')
para('Watermarked streaming aggregation results','Heading2');pic('watermark_results.png')
para('Example saved query','Heading2')
code_style=ParagraphStyle('CodeSmall',fontName='Courier',fontSize=8,leading=11)
story.append(Preformatted((O/'sql/monthly.sql').read_text(),code_style))
page();title('13. Genuine browser evidence')
para('Screenshots capture the running Spark application and saved query/stream results displayed in a browser. The latter show actual output CSVs and progress JSON, and are labelled as recorded evidence. The source data, query files and execution logs accompany them.')
for filename,caption in [('spark_jobs.png','Running Spark Jobs UI'),('spark_stages.png','Spark Stages UI with task and shuffle evidence'),('sql_results.png','Saved Spark SQL query and actual result'),('streaming_results.png','Recorded micro-batches, hourly aggregates and scoring eligibility')]:
    path=O/'screenshots'/filename
    if path.exists():
        from PIL import Image as PIL
        im=PIL.open(path);w,h=im.size
        if filename!='spark_jobs.png':page()
        para(caption,'Heading2')
        width=min(490,650*w/h);height=width*h/w
        story.append(Image(str(path),width=width,height=height))
    else:para('Screenshot pending: '+filename)

def footer(canvas,doc):
    canvas.setStrokeColor(colors.HexColor('#ccd9df'));canvas.line(40,37,555,37)
    canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor(navy));canvas.drawString(40,24,'GridPulse | MMDS Assignment 2 | Reproducible prototype');canvas.drawRightString(555,24,str(doc.page))
doc=SimpleDocTemplate(str(P/'GridPulse_Report.pdf'),title='GridPulse - MMDS Assignment 2',author='GridPulse',subject='Spark architecture, RDDs, SQL, streaming and controlled experiments',pagesize=(595.28,841.89),rightMargin=45,leftMargin=45,topMargin=42,bottomMargin=48)
doc.build(story,onFirstPage=footer,onLaterPages=footer)
print(P/'GridPulse_Report.pdf')
