# PySpark ETL Pipeline

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