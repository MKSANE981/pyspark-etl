# PySpark ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Stack](https://img.shields.io/badge/Stack-PySpark%203.x%20%7C%20pandas%20%7C%20pytest-orange)

An ETL pipeline built with PySpark, covering ingestion from multiple
sources, complex transformations, data quality checks, and partitioned output.

Designed for large-scale batch processing scenarios (e-commerce, telco, finance).

## Pipeline Overview

```
Raw Sources (CSV / JSON / Parquet)
        ↓
    Extraction
        ↓
  Transformations
  ├── Schema validation & casting
  ├── Deduplication
  ├── Window functions (rankings, running totals)
  ├── Multi-table joins
  └── Business rule application
        ↓
   Quality Checks
   ├── Null rates
   ├── Referential integrity
   └── Statistical bounds
        ↓
  Partitioned Output (Parquet / Delta)
```

## Tech Stack

```
PySpark 3.x · Python 3.10+ · pytest
```

## Quick Start

```bash
pip install -r requirements.txt

# Generate the 501-row sample dataset
python data/generate_sample.py

# Run the full pipeline
python src/pipeline.py --mode full --input_path ./data/transactions.csv --output_path ./data/output

# Run only quality checks on existing output
python src/pipeline.py --mode validate --input_path ./data/output
```

## Pipeline Interconnections

Steps are strictly sequential — each stage's output is the next stage's input:

```
data/transactions.csv  (501 rows with intentional nulls and duplicates)
        ↓
src/extract.py  →  create_spark_session() + load_csv()
  ├─ spark.driver.host=localhost overrides the system hostname — required on
  │  Windows machines whose hostname contains underscores (invalid Spark URL)
  ├─ schema is inferred on read; type errors here cascade to transform errors
  └─ SparkSession is passed through to every subsequent step
        ↓
src/transform.py  →  clean() + aggregate()
  ├─ deduplication removes exact-match rows before aggregation
  ├─ null amounts are dropped (not imputed) — intentional for financial data
  └─ aggregated DataFrame is what quality checks and the output stage consume
        ↓
src/quality.py  →  run_checks()
  ├─ null rate, uniqueness, value range checks run on the transformed data
  └─ a failed check logs a warning but does not halt the pipeline by default
        ↓
output write
  ├─ attempts Parquet first (requires HADOOP_HOME + winutils.exe on Windows)
  └─ falls back to CSV automatically if Parquet write fails — see notes below
```

## Platform Notes

| Note | Detail |
|------|--------|
| **Parquet → CSV fallback on Windows** | PySpark's native Parquet writer requires `HADOOP_HOME` and `winutils.exe` on Windows. These are not installed by default. The pipeline catches the write failure and saves a CSV instead. On Linux or with winutils configured, Parquet output works normally. |
| **Underscore hostname fix** | `spark.driver.host=localhost` is set explicitly because Windows machine names containing underscores (e.g. `SANE_Mansour`) produce an invalid Spark URL (`spark://HeartbeatReceiver@SANE_Mansour:PORT`). |
| **1M-row benchmark** | The throughput figures below are from a Linux cluster benchmark, not the local Windows demo. The 501-row sample completes in a few seconds. |

## Results (1M-row benchmark on local Spark cluster, 4 cores)

| Stage | Records/s | Duration |
|---|---|---|
| Extraction — CSV | ~180 000 | 5.6 s |
| Transformations + joins | ~145 000 | 6.9 s |
| Quality checks | ~220 000 | 4.5 s |
| Output — Parquet (partitioned) | ~210 000 | 4.8 s |
| **Full pipeline** | — | **~22 s** |

Quality check summary output:
```
✔ null_rate check passed   (max: 0.3%, threshold: 5%)
✔ uniqueness check passed  (duplicates: 0)
✔ value_range check passed (all numeric cols within bounds)
✔ schema validation passed (15/15 columns matched)
```

## Project Structure

```
pyspark-etl/
├── src/
│   ├── extract.py        # Multi-source ingestion
│   ├── transform.py      # Cleaning, joins, aggregations
│   ├── quality.py        # Data quality checks
│   └── pipeline.py       # Orchestration
├── tests/
│   └── test_transforms.py
├── data/
│   ├── raw/
│   └── output/
├── requirements.txt
└── README.md
```