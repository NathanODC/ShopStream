# ShopStream

ShopStream is a retail data-engineering portfolio project with batch and
streaming paths. It is local-first for development while retaining managed GCP
deployment targets.

## Architecture

```text
Local source files
  -> source simulator
  -> Pub/Sub
       |-> Pub/Sub-to-GCS -> GCS bronze -> BigQuery external tables
       |      -> dbt -> BigQuery silver and gold
       |
       `-> Apache Beam -> Dataflow -> BigQuery streaming tables
```

The batch path produces authoritative analytical models. The streaming path is
optimized for low-latency processing. Apache Beam transformations can be tested
with the local DirectRunner; dbt transformations can be built locally against
DuckDB without GCP credentials.

## Repository layout

```text
airflow/
  dags/                 Airflow orchestration
  dbt/                  dbt models, tests, snapshots, macros, and local seeds
definitions/
  external_tables/      BigQuery DDL for the GCS-backed bronze sources
shopstream/
  1_api_sources/        Source simulator function
  2_pubsub-2-gcs/       Pub/Sub-to-GCS function
  3_dataflow/           Apache Beam streaming pipeline
content/                Architecture diagram and draw.io source
```

## Prerequisites

- `make`
- `uv`
- Python 3.12 or newer
- Docker, for the local GCS emulator and Airflow
- Google Cloud CLI, for the Pub/Sub emulator and optional GCP deployment
- Astronomer CLI, for local Airflow

## Local dbt build

DuckDB is a local validation target only. BigQuery remains the production
warehouse target.

```bash
make dbt_setup
make dbt_local
```

This command sequence:

1. Loads deterministic Bronze fixtures with `dbt seed`.
2. Builds the customer Type 2 snapshot.
3. Builds and tests the Silver and Gold models.

The local database is written to `airflow/dbt/.local/shopstream.duckdb` and is
ignored by Git.

To compile for BigQuery using Application Default Credentials:

```bash
export GCP_PROJECT_ID=shopstream-proj
make dbt_compile_bigquery
```

## Local ingestion

Run each long-running command in a separate terminal:

```bash
make setup
make run_pubsub_emulator
make run_gcs_emulator
make run_gcf_source
make run_gcf_pubsub_to_gcs
make source_request
make pubsub_to_gcs_request
```

The files expected by `shopstream/1_api_sources/source-data/` are intentionally
ignored and must be supplied locally.

## Tests and formatting

```bash
make test
cd shopstream && uv run ruff check .
cd shopstream && uv run ruff format --check .
```

The dbt GitHub Actions workflow runs the complete DuckDB build without cloud
credentials. Dataflow transform tests run in their own workflow.

## Airflow

```bash
cd airflow
astro dev start
```

The `execute_shopstream` DAG invokes dbt against BigQuery only when
`SHOPSTREAM_ENV=prod`. The Astro image packages the dbt project at
`/usr/local/airflow/dbt`; override `DBT_PROJECT_DIR` when a different deployment
layout is used.
