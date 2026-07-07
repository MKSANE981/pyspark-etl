# PySpark ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Stack](https://img.shields.io/badge/Stack-PySpark%20%7C%20Delta%20Lake%20%7C%20pytest-orange)

A production-style ETL pipeline built with PySpark, covering ingestion from multiple
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
PySpark 3.x · Python 3.10+ · Delta Lake (optional) · pytest
```

## Quick Start

```bash
pip install -r requirements.txt

# Generate sample data and run the full pipeline
python src/pipeline.py --mode full --input_path ./data/raw --output_path ./data/output

# Run only quality checks
python src/pipeline.py --mode validate --input_path ./data/output
```

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