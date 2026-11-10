# ShopStream

ShopStream is a retail data-engineering portfolio project with batch and
streaming paths. It is local-first for development while retaining managed GCP
deployment targets.

## Architecture

```text
HTTP generation request
  -> Faker/Pydantic source simulator
  -> Pub/Sub
       |-> customer support + product catalog
       |      -> Pub/Sub-to-GCS -> GCS bronze -> BigQuery external tables
       |
       `-> clickstream + sales transactions
              -> Apache Beam -> Dataflow -> BigQuery streaming tables

Airflow -> dbt source freshness, staging, snapshots, silver, gold, and tests
```

dbt combines the batch product data with the streaming clickstream and sales
tables. Customer support remains available as a batch bronze source. Dataflow
jobs run continuously and are deployed independently from Airflow.

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
- Java 21 (OpenJDK), required by the local Pub/Sub emulator
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
2. Moves the fixture load times to now and checks `dbt source freshness`.
3. Builds and tests staging, the customer Type 2 snapshot, Silver, and Gold.

## Data quality

- **Freshness:** sources are checked on load time, not event time. Batch
  sources use the `dt` partition (warn after 7 days, error after 14), and
  streaming sources use `ingestion_time` (warn after 1 day, error after 7).
- **Volume:** every source and model must contain at least one row.
- **Contracts:** accepted-value and range tests mirror the generator's Pydantic
  models. Streaming payloads are parsed into typed `shopstream_staging` views
  so they can be tested before the snapshot reads them.
- **Referential integrity:** sales must reference known customers and
  products. Clickstream and support users that are not known customers, and
  completed sale items that the catalog join drops, are reported as warnings.
- **Reconciliation:** gold order counts and spend must equal `f_sales`.
- Duplicate `transaction_id` and `event_id` values warn until the streaming
  path deduplicates Pub/Sub redeliveries.

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
make pubsub_to_gcs_request
make source_request_batch
make source_request_streaming
```

On Apple Silicon with Homebrew, install Java with `brew install openjdk@21`.
If `java -version` still resolves to the macOS stub, prepend
`/opt/homebrew/opt/openjdk@21/bin` to `PATH` before starting the emulator.

Start the batch consumer before publishing batch records so its durable
subscriptions exist. The source function generates a coherent synthetic dataset
dynamically. It publishes all four sources by default, one record per Pub/Sub message. A request
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

The Dataflow demonstration submits two continuous streaming jobs: clickstream
and sales transactions. Function demonstrations are separate manual workflows so they can be
shown independently and kept cost-bounded.

## Airflow

```bash
cd airflow
astro dev start
```

The `shopstream_dbt_transform` DAG orchestrates only bounded dbt work; it does
not invoke ingestion functions or manage Dataflow jobs. It checks source
freshness first and stops before any transformation when a source is stale. It runs against BigQuery
only when `SHOPSTREAM_ENV=prod`. The Astro image packages the dbt project at
`/usr/local/airflow/dbt`; override `DBT_PROJECT_DIR` when a different deployment
layout is used.
