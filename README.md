# GridPulse

**Event-Time Reliability and Seasonality-Aware Anomaly Detection for Household Energy Telemetry with Apache Spark**

MMDS Assignment 2 implementation covering CO2 and CO4 with direct RDD ingestion, typed DataFrames, Spark SQL and Structured Streaming over 2,075,259 real UCI measurements.

## Present the project

```bash
bash scripts/present.sh
bash scripts/present.sh --live
```

The first command displays verified results and the evidence locations. The live demonstration runs an actual SQL query and replays 21,600 real records, then keeps the Spark UI open until you press Enter. It uses a separate temporary checkpoint and preserves submission evidence.

- [Technical report](output/pdf/GridPulse_Report.pdf)
- [Timed presentation guide](docs/PRESENTATION_GUIDE.txt)
- [Rubric evidence map](docs/EVIDENCE_MAP.txt)
- [Viva guide](docs/VIVA_AND_DEMO.txt)
- [Browser evidence document](outputs/evidence.html)
- [Genuine screenshots](outputs/screenshots/)

## Reproduce everything

```bash
bash scripts/run_all.sh
```

On a new machine with `uv` available:

```bash
bash scripts/setup.sh
bash scripts/run_all.sh
```

Java 17 and Python 3.11 are required. On this Apple Silicon Mac the environment is already installed. On Linux/Intel macOS, install Java 17 and set `JAVA_HOME` before setup. No data credentials are required. Allow several GB for Spark, source data and generated state.

`run_all.sh` validates pinned source hashes, runs regression tests, processes the full dataset, runs anomaly and local workload experiments, simulates streaming arrivals and checkpoint restart, performs the joint injected-fault/lateness experiment, extracts real Spark stage metrics, generates the report, validates the artifacts and creates `output/GridPulse_Submission.zip`.

Each full streaming experiment resets its own `outputs/streaming` directory for reproducibility. Checkpoint restart occurs within the run. The live class demo uses a separate directory.

## What the evidence demonstrates

- 2,049,280 accepted readings and 25,979 rejected measurements; no duplicate timestamps.
- Independent RDD/SQL monthly key sets, counts and energy totals reconcile.
- A 21,600-real-record replay matches 361 batch-reference windows after checkpoint restart.
- 359 windows meet 90% hourly coverage. Two windows remain unscored with null alerts.
- Unperturbed delayed-replay retention is 90.34%, 98.92% and 100% for 2-, 72- and 168-hour budgets.
- Eighteen shared perturbation scenarios across three models give 54 model-scenario evaluations. Seasonal models do better on small changes in this suite, while the global model performs better on large changes and the equal-weight pooled average.
- The joint minute-burst experiment measures controlled labels and final snapshot decisions. It exposes low recall as well as one changed detection under the short watermark.
- Eleven regression tests cover source recovery, timestamps, finite inputs, scoring eligibility and comparison invariants.

## Code and artifacts

| Requirement | Implementation / evidence |
|---|---|
| RDD creation and operations | `src/batch.py`, `outputs/rdd_results.json` |
| Explicit schema, cleaning and SQL | `src/batch.py`, `outputs/sql/` |
| Rejection reasons, monthly coverage and missing runs | `outputs/rejections.json`, `sql/monthly_coverage.csv`, `missing_runs.csv` |
| Streaming validation and scoring | `src/validation.py`, `src/streaming.py` |
| Replay, recovery and watermark results | `outputs/streaming/validation.json`, progress JSON and result CSVs |
| Joint controlled faults and delayed evidence | `src/joint_experiment.py`, `streaming/joint_protocol.json` |
| Real jobs, stages and shuffled bytes | `outputs/stage_metrics.csv`, `execution_evidence.json`, `evidence_event_logs/` |
| Presentation evidence | Report, `outputs/evidence.html`, genuine browser screenshots |
| Technical and administrative readiness | `outputs/preflight.json`, `submission.json` |

## Submission details

Edit `submission.json` with actual group members and the CAL 1 continuity/approval status, then regenerate the report and bundle. No identity or instructor approval has been assumed. The preflight reports technical readiness separately from these human requirements. Merge report sections into your actual CAL 1 report as required by the assignment.

The earlier review remains locally in `docs/EVALUATION_AUDIT.txt`. [Final review resolution](docs/FINAL_REVIEW_RESOLUTION.txt) records the implemented corrections. Historical audit files are excluded from the presentation bundle.

## Scientific scope

The source is historical telemetry from one household, replayed as permitted by the assignment. It has no verified fault annotations. Injection labels support controlled sensitivity studies, not claims of natural fault detection or theft detection. Short injected bursts remain difficult for this hourly static detector. The weekly streaming model is preselected and thresholds use only 2009 calibration data.

Monthly kWh values represent observed energy. Missing periods are not imputed. A fixed process/SQL timezone preserves source wall-clock values; true French UTC instants and daylight-saving offsets remain unresolved. Negative submeter residuals retain quality flags without an assumed physical cause.

Execution uses `local[4]`. Warmed partition and data-volume experiments show local workload sensitivity. They do not establish multi-executor scalability. Publication requires an original contribution and broader labelled, statistical and systems evaluation.

## Attribution

Georges Hebrail and Alice Berard. *Individual Household Electric Power Consumption*. UCI Machine Learning Repository. DOI [10.24432/C58K54](https://doi.org/10.24432/C58K54). [Dataset](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption), CC BY 4.0. The project preserves raw data and derives quality flags, aggregates, profiles and controlled perturbations. Pinned SHA-256 provenance appears in `data/raw/manifest.json`.
