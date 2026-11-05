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

See the [ShopStream architecture and use-case document](content/ShopStream-Use-Case.pdf)
for the design decisions, operational boundaries, migration summary, and next
steps. The editable diagram is in `content/ShopStream_Architecture.drawio`.

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
content/                Architecture diagram, draw.io source, and design PDF
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

Pull requests run the affected function tests, Dataflow transform tests, and
the complete dbt DuckDB build without cloud credentials. Merging to `main`
does not deploy to GCP automatically.

Cloud demonstrations are started explicitly from the GitHub Actions **Run
workflow** menu. Each deployment workflow requires `confirm_deploy`, reruns its
tests, enters the `Production` environment, and authenticates with short-lived
GitHub OIDC credentials through Workload Identity Federation. Configure these
`Production` environment variables before the first demonstration:

- `GCP_WIF_PROVIDER`: full Workload Identity provider resource name.
- `GCP_SERVICE_ACCOUNT`: deployment service-account email.

The Dataflow demonstration submits one streaming job for each of the four
topics. Function demonstrations are separate manual workflows so they can be
shown independently and kept cost-bounded.

## Airflow

```bash
cd airflow
astro dev start
```

The `execute_shopstream` DAG invokes dbt against BigQuery only when
`SHOPSTREAM_ENV=prod`. The Astro image packages the dbt project at
`/usr/local/airflow/dbt`; override `DBT_PROJECT_DIR` when a different deployment
layout is used.
