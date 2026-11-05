# ShopStream

ShopStream is a retail data-engineering portfolio project with batch and
streaming paths. It is local-first for development while retaining managed GCP
deployment targets.

## Architecture

```text
HTTP generation request
  -> Faker/Pydantic source simulator
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

The source function generates a coherent synthetic dataset dynamically. It
publishes all four sources by default, one record per Pub/Sub message. A request
can select one or more sources while the generator still creates the supporting
entities needed to preserve relationships.

```json
{
  "profile": "small",
  "seed": 42,
  "sources": ["clickstream"],
  "volumes": {"clickstream": 25},
  "pubsub_topics": {
    "clickstream": "projects/local-pubsub-instance/topics/clickstream-topic"
  }
}
```

Omit `sources` to publish `clickstream`, `customer_support`, `product_catalog`,
and `sales_transactions`. Available profiles are `small`, `demo`, `load`, and
`edge_cases`; explicit `volumes` override individual profile defaults.

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
