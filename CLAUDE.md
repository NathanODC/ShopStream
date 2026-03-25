# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ShopStream is a retail data engineering pipeline on GCP. It simulates source data ingestion through Google Cloud Functions (GCF), streams data via Pub/Sub to GCS (bronze layer), then transforms it through medallion layers (bronze → silver → gold) using Dataform on BigQuery. Airflow (via Astronomer) orchestrates the full batch pipeline.

## Commands

### Python Setup (in `shopstream/`)
```bash
make setup           # Install Python deps via uv sync
```

### Local Development
```bash
make run_pubsub_emulator      # Start local Pub/Sub emulator (port 8085)
make run_gcf_source           # Run source data GCF locally (port 8080)
make run_gcf_pubsub_to_gcs    # Run Pub/Sub→GCS GCF locally (port 8080)
make source_request           # Trigger source data generation via curl
make pubsub_to_gcs_request    # Trigger Pub/Sub→GCS pipeline via curl
```

### Airflow (in `airflow/`)
```bash
astro dev start      # Start local Airflow (Docker) at http://localhost:8080
astro dev stop       # Stop local Airflow
```

### Linting / Formatting
Pre-commit hooks run `ruff` (lint + format) and `isort` automatically on commit. To run manually:
```bash
cd shopstream && uv run ruff check --fix .
cd shopstream && uv run ruff format .
cd shopstream && uv run isort .
```

### Dataform
Dataform runs are managed via the Airflow DAG or directly through GCP Dataform UI. The project is configured in `dataform.json` targeting BigQuery dataset `shopstream_silver` (default) in project `shopstream-proj`.

## Architecture

```
Source Data (local JSON/CSV files)
  └─► GCF: 1_api_sources  (simulate data sources → Pub/Sub topics)
        └─► 4 topics: clickstream, customer-support, product-catalog, sales-transactions
              └─► GCF: 2_pubsub-2-gcs  (Pub/Sub → GCS bronze buckets)
                    └─► GCS Bronze Buckets (raw files)
                          └─► Dataform (BigQuery transformations)
                                ├── external_tables/ (GCS → BQ external tables)
                                ├── 1_bronze/  (raw staging tables)
                                ├── 2_silver/  (cleaned/normalized, tag: "silver")
                                └── 3_gold/    (aggregated/analytical, tag: "gold")
```

**Airflow DAG** (`airflow/dags/batch_pipeline.py`): `execute_shopstream` runs weekly (Mon 03:30 UTC-3), orchestrating: `ingestion_to_raw` (calls both GCFs sequentially) → `transform_to_trusted` (Dataform: compile → run silver → run gold).

## Key Files

- `shopstream/1_api_sources/` — GCF that reads local source-data files and publishes to Pub/Sub
- `shopstream/2_pubsub-2-gcs/` — GCF that subscribes to Pub/Sub and writes to GCS; `TOPIC_BUCKET_MAP` maps topics to GCS buckets
- `definitions/` — Dataform SQLX definitions per medallion layer
- `airflow/dags/batch_pipeline.py` — Full pipeline DAG
- `dataform.json` — Dataform project config (warehouse, schemas, GCP project)
- `shopstream/pyproject.toml` — Python deps managed by `uv`

## GCP Project

- Project ID: `shopstream-proj`
- Dataform repository: `shopstream`, region: `us-west1`
- BigQuery default location: `US`
- Assertions schema: `shopstream_assertions`

## CI/CD

GitHub Actions workflows in `.github/workflows/` deploy the two GCFs:
- `gcf_deploy_source_data_gen.yml` — deploys `1_api_sources`
- `gcf_deploy_pubsub_to_gcs.yml` — deploys `2_pubsub-2-gcs`
