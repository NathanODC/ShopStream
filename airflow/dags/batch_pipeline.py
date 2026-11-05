"""
ShopStream batch pipeline DAG.

Runs weekly (Monday at 03:30 America/Sao_Paulo) and orchestrates:
  1. ingestion_to_raw  — calls both GCFs sequentially (source → Pub/Sub → GCS)
  2. transform_to_trusted — dbt snapshot, silver, and gold runs (prod only;
     skipped locally)

Local Astro vs prod (Composer) is driven by the Airflow Variable SHOPSTREAM_ENV:
  - "local"  → GCFs are called at host.docker.internal; dbt tasks are skipped.
  - "prod"   → GCFs are called at Cloud Run URLs; dbt runs against BigQuery.

Set Variable values via airflow/.env (local) or Composer environment variables (prod).
"""

from datetime import timedelta

import pendulum
import logging

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from include.airflow_utils import call_cloud_function


# ---------------------------------------------------------------------------
# Config — driven by Airflow Variables so local and prod share the same DAG file
# ---------------------------------------------------------------------------

SHOPSTREAM_ENV = Variable.get("SHOPSTREAM_ENV", default_var="local")

URL_GCF_SOURCE = Variable.get(
    "GCF_SOURCE_URL", default_var="http://host.docker.internal:8080"
)
URL_GCF_PUBSUB_2_GCS = Variable.get(
    "GCF_PUBSUB_TO_GCS_URL", default_var="http://host.docker.internal:8081"
)
PUBSUB_PROJECT_ID = Variable.get(
    "PUBSUB_PROJECT_ID", default_var="local-pubsub-instance"
)

DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", default_var="/usr/local/airflow/dbt")

ARG_GCF_SOURCE = {
    "log_env": "dev",
    "profile": "demo",
    "seed": 42,
    "pubsub_topics": {
        "clickstream": f"projects/{PUBSUB_PROJECT_ID}/topics/clickstream-topic",
        "customer_support": f"projects/{PUBSUB_PROJECT_ID}/topics/customer-support-topic",
        "product_catalog": f"projects/{PUBSUB_PROJECT_ID}/topics/product-catalog",
        "sales_transactions": f"projects/{PUBSUB_PROJECT_ID}/topics/sales-transactions",
    },
}

ARG_GCF_PUBSUB_2_GCS = {
    "log_env": "dev",
    "gcs_prefix": "pubsub-data",
    "timeout": 30.0,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = dict(
    owner="ShopStream DE Team",
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=30),
)

local_tz = pendulum.timezone("America/Sao_Paulo")
logger = logging.getLogger("airflow.task")


@dag(
    "execute_shopstream",
    start_date=pendulum.datetime(2025, 5, 5, tz=local_tz),
    schedule="30 3 * * 1",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    tags=["shopstream", "batch", "weekly"],
)
def shopstream_pipeline():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task_group(group_id="ingestion_to_raw")
    def ingestion_to_raw():
        @task(task_id="gcf-simulate-data-source")
        def gcf_simulate_data_source():
            return call_cloud_function(URL_GCF_SOURCE, ARG_GCF_SOURCE)

        @task(task_id="gcf-pubsub-2-gcs")
        def gcf_pubsub_2_gcs():
            return call_cloud_function(URL_GCF_PUBSUB_2_GCS, ARG_GCF_PUBSUB_2_GCS)

        gcf_simulate_data_source() >> gcf_pubsub_2_gcs()

    @task_group(group_id="transform_to_trusted")
    def transform_to_trusted():
        @task.short_circuit(task_id="is_prod_env", ignore_downstream_trigger_rules=True)
        def check_env():
            """Skip dbt BigQuery tasks when running locally without credentials."""
            env = Variable.get("SHOPSTREAM_ENV", default_var="local")
            if env != "prod":
                logger.info(f"SHOPSTREAM_ENV='{env}' — skipping dbt BigQuery tasks.")
                return False
            return True

        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=(
                "dbt snapshot --target bigquery "
                f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            ),
        )

        dbt_build_silver = BashOperator(
            task_id="dbt_build_silver",
            bash_command=(
                "dbt build --target bigquery --select tag:silver "
                f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            ),
        )

        dbt_build_gold = BashOperator(
            task_id="dbt_build_gold",
            bash_command=(
                "dbt build --target bigquery --select tag:gold "
                f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
            ),
        )

        check_env() >> dbt_snapshot >> dbt_build_silver >> dbt_build_gold

    start >> ingestion_to_raw() >> transform_to_trusted() >> end


dag_shopstream = shopstream_pipeline()
