# AGENTS.md

Guidance for coding agents working in ShopStream. Treat the repository as the
source of truth; the architecture diagram and README may lag behind the code.

## Purpose

ShopStream is the data-engineering project in a broader portfolio. Its goal is
to demonstrate a production-minded, local-first retail data platform: batch
and streaming ingestion, medallion transformations, orchestration, data
quality, CI/CD, observability, security, and optional managed deployment on
GCP.

Keep work in this repository focused on ShopStream. AI/RAG projects belong in
their own repositories, although ShopStream may later expose governed data
products that those projects consume.

## Current architecture

The source simulator uses Faker, seeded domain logic, and Pydantic contracts to
generate a coherent synthetic retail dataset. It publishes one record per
message to four Pub/Sub topics by default, or only the sources selected in the
HTTP request:

- clickstream events: JSON -> `clickstream-topic`
- customer-support tickets: JSON -> `customer-support-topic`
- product catalog: JSON -> `product-catalog`
- sales transactions: CSV -> `sales-transactions`

After Pub/Sub, the repository currently contains two separate ingestion paths:

```text
HTTP generation request
  -> HTTP source-simulator function
  -> Pub/Sub topics
       |-> Batch/raw path: HTTP subscriber function -> GCS bronze buckets
       |     -> BigQuery external tables (`shopstream_bronze`)
       |     -> dbt silver (`shopstream_silver`)
       |     -> dbt gold (`shopstream_gold`)
       |
       `-> Streaming path: one Beam/Dataflow job per topic
             -> generic raw-envelope tables in `shopstream_streaming`
```

These paths are not yet unified. dbt currently reads the GCS-backed
external tables, not the `shopstream_streaming` tables. Airflow orchestrates
the batch/raw GCS path and dbt only; it does not start or monitor the
long-running Dataflow jobs.

### Batch orchestration

`airflow/dags/batch_pipeline.py` defines `execute_shopstream`:

```text
start
  -> ingestion_to_raw
       -> source simulator
       -> Pub/Sub-to-GCS subscriber
  -> transform_to_trusted
       -> environment gate
       -> dbt snapshot
       -> dbt silver build
       -> dbt gold build
  -> end
```

- Schedule: Monday at 03:30 in `America/Sao_Paulo`.
- The DAG is paused when first created and allows one active run.
- `SHOPSTREAM_ENV=local` skips dbt because local Astro has no BigQuery
  connection; `SHOPSTREAM_ENV=prod` enables it.
- Function URLs and the Pub/Sub project are supplied through Airflow Variables.

### Data layers

- Bronze: four BigQuery external tables over Hive-style GCS paths such as
  `pubsub-data/<topic>/dt=YYYY-MM-DD/...`.
- Silver: `d_customers`, `d_products`, and `f_sales` in
  `shopstream_silver`.
- Gold: `shopstream_gold.user_behavior_summary`.
- Data quality: dbt generic tests stored with each model layer.
- Streaming raw: four generic BigQuery tables containing message metadata,
  the raw payload as a JSON string, and serialized Pub/Sub attributes.

## Repository map

- `shopstream/1_api_sources/`: HTTP synthetic-data generator function.
- `shopstream/2_pubsub-2-gcs/`: HTTP function that creates pull subscriptions
  and writes messages to topic-specific GCS buckets.
- `shopstream/3_dataflow/`: Apache Beam streaming pipeline and transform tests.
- `definitions/external_tables/`: standalone BigQuery DDL for GCS external
  tables; these are not dbt models.
- `airflow/dbt/`: dbt models, snapshots, tests, cross-adapter macros, and
  deterministic DuckDB fixtures.
- `airflow/dags/batch_pipeline.py`: batch orchestration DAG.
- `airflow/include/airflow_utils.py`: HTTP function caller used by the DAG.
- `.github/workflows/`: deployments for both functions and test/deploy for
  Dataflow.
- `content/`: architecture image, editable draw.io source, and architecture
  use-case PDF.

## Verified project identifiers

- GCP project: `shopstream-proj`
- BigQuery default location: `US`
- Dataflow region: `us-west1`
- Source function deployment region: `us-west1`
- Pub/Sub-to-GCS function deployment region: `us-central1`

Do not silently normalize the regional mismatch; treat it as an architecture
decision to review because it can add latency, operational complexity, or
data-location constraints.

## Commands

Run commands from the repository root unless noted.

```bash
# Python dependencies
make setup

# Unit tests for the two function packages
make test

# dbt: install, then build locally without cloud credentials
make dbt_setup
make dbt_local

# Compile the dbt project for BigQuery
make dbt_compile_bigquery

# Formatting and linting
cd shopstream && uv run ruff check --fix .
cd shopstream && uv run ruff format .
cd shopstream && uv run isort .

# Local infrastructure
make run_pubsub_emulator       # localhost:8085; foreground
make run_gcs_emulator          # fake-gcs-server on localhost:4443
make stop_gcs_emulator

# Local HTTP functions (use separate terminals)
make run_gcf_source            # localhost:8080
make run_gcf_pubsub_to_gcs     # localhost:8081
make source_request
make pubsub_to_gcs_request

# Streaming pipeline
make run_dataflow_local        # DirectRunner; long-running
make deploy_dataflow           # real GCP deployment; requires credentials

# Local Airflow/Astronomer (run in airflow/)
astro dev start
astro dev stop
```

`make test` does not run `shopstream/3_dataflow/tests`; run those with the
dependencies in `shopstream/3_dataflow/requirements.txt`, matching the
Dataflow GitHub Actions workflow.

## Local-development constraints

- The source function supports seeded `small`, `demo`, `load`, and `edge_cases`
  profiles plus per-source volume overrides. Omit `sources` to publish all four;
  select one or more sources for targeted testing.
- The local Pub/Sub emulator and fake GCS server are separate processes.
- `config.local.json` switches the GCS consumer to local Pub/Sub topic paths.
- The Beam DirectRunner command starts one topic pipeline at a time and is
  intended to remain running.
- Cloud deployments and BigQuery integration require
  explicit credentials and should not be inferred from passing unit tests.
- DuckDB is a local test target, not the declared production warehouse.

## CI/CD

- `gcf_deploy_source_data_gen.yml`: tests source-function changes on pull
  requests and pushes; deployment is manual and requires confirmation.
- `gcf_deploy_pubsub_to_gcs.yml`: tests GCS-consumer changes on pull requests
  and pushes; deployment is manual and requires confirmation.
- `dataflow_test_and_deploy.yml`: runs Beam transform tests for pull requests
  and pushes; a confirmed manual run submits one Dataflow job per topic.
- `dbt_test.yml`: parses and builds the entire dbt project against DuckDB with
  deterministic seeds and no cloud credentials.

Manual deployments enter the `Production` environment and authenticate through
GitHub OIDC and Workload Identity Federation using the `GCP_WIF_PROVIDER` and
`GCP_SERVICE_ACCOUNT` environment variables. Never commit credentials,
service-account JSON, source customer data, or `.env` files.

## Engineering rules

- Preserve the distinction between batch and streaming paths in code and docs.
- Do not claim an integration works from configuration alone. Validate at the
  real boundary with a harmless local end-to-end run, dbt compile/build,
  or authenticated read when access is available.
- Keep Pub/Sub handlers idempotent. Acknowledge only after the destination
  write succeeds; expect redelivery and duplicate messages.
- Carry event time, ingestion time, source, schema version, and stable event or
  transaction identifiers through raw ingestion.
- Add schema validation, dead-letter handling, replay strategy, and data
  quality checks before describing the platform as production-ready.
- Use least-privilege service accounts and do not log secrets or sensitive
  payloads.
- Keep tests deterministic and emulator-first. Cloud tests must be clearly
  separated, cost-bounded, and opt-in.
- Keep dbt SQL cross-adapter where practical. Isolate unavoidable warehouse
  differences in dispatched macros and validate both DuckDB and BigQuery
  compilation.
- Update `README.md`, the draw.io architecture source, and this file when an
  architectural decision changes the implemented flow.

## Known decisions and gaps to address next

These are open design items, not assumptions agents should resolve silently:

1. Choose the target relationship between the GCS batch path and the Dataflow
   streaming path: coexist as Lambda-style serving paths, converge on one raw
   source of truth, or retire one path.
2. Define typed, versioned contracts for all four topics. The streaming path
   currently stores a generic raw envelope.
3. Decide whether bronze should be GCS-first, BigQuery-native, or both, and
   document replay/backfill semantics.
4. Bring the external-table DDL into a repeatable deployment mechanism; the
   `.sql` files are not currently managed by dbt or infrastructure as code.
5. Expand data quality beyond the current dbt tests and add
   freshness, volume, referential-integrity, and accepted-value checks.
6. Add infrastructure as code for Pub/Sub, buckets, datasets, service accounts,
   dbt deployment, Dataflow, and orchestration configuration.
7. Add observability: structured logs, pipeline metrics, alerting, lineage,
   cost controls, and operational runbooks/SLOs.
8. Reconcile documentation drift, including the README's obsolete project
   tree and the architecture diagram's implemented-versus-planned components.
9. Validate the dbt customer snapshot and all analytical models against
   representative production-shaped data, not only local fixtures.
10. Replace credential-JSON CI authentication with short-lived federation and
    standardize GCP regions where practical.
11. Define authenticated production calls from Airflow to the private Cloud
    Run function endpoints; the current helper sends a plain HTTP POST without
    an identity token.

## Definition of done for architecture work

An architecture change is complete only when code/configuration, tests,
deployment automation, documentation/diagram, security implications, and a
boundary-level validation all agree. If live GCP access is unavailable, state
exactly what was validated locally and what remains unverified.
