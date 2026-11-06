"""Weekly dbt orchestration for ShopStream analytical models."""

from datetime import timedelta

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", default_var="/usr/local/airflow/dbt")

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
    "shopstream_dbt_transform",
    start_date=pendulum.datetime(2025, 5, 5, tz=local_tz),
    schedule="30 3 * * 1",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    tags=["shopstream", "dbt", "weekly"],
)
def shopstream_pipeline():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task.short_circuit(task_id="is_prod_env", ignore_downstream_trigger_rules=True)
    def check_env():
        """Skip BigQuery dbt tasks in local Astro without cloud credentials."""
        env = Variable.get("SHOPSTREAM_ENV", default_var="local")
        if env != "prod":
            logger.info("SHOPSTREAM_ENV='%s' — skipping dbt BigQuery tasks.", env)
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

    start >> check_env() >> dbt_snapshot >> dbt_build_silver >> dbt_build_gold >> end


dag_shopstream = shopstream_pipeline()
