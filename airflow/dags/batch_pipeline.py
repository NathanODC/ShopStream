from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
# from functools import partial
import pendulum
import logging

from include.airflow_utils import call_cloud_function


"""
{
    "dataform": {
        "project_id": "shopstream-proj", 
        "repository_id": "shopstream", 
        "region": "us-west1",
        "git_commitish": "main"
    },
    "gcf": { 
        "urls": [
            "https://gcf-shopstream-source-data-gen-274386375025.us-west1.run.app",
            "https://gcf-pubsub-2-gcs-274386375025.us-central1.run.app"
        ],
        "args": {
            "simulate_data_source": "...",
            "pubsub_2_gcs": "..."
        }
    },
    "airflow": {
        "default_args": {
            "owner": "ShopStream DE Team",
            "depends_on_past": false,
            "retries": 3,
            "retry_delay_time": 30,
            "sla_time": 24
            }
    }
}
"""

PROJECT_ID = "shopstream-proj"
REPOSITORY_ID = "shopstream"
REGION = "us-west1"
GIT_COMMITISH = "main"

URL_GCF = [
    "https://gcf-shopstream-source-data-gen-274386375025.us-west1.run.app",
    "https://gcf-pubsub-2-gcs-274386375025.us-central1.run.app"
]

ARG_GCF_SIMULATE_DATA_SOURCE = {
    "simulate_data_source": ""# TODO: add as parameter
}
ARG_GCF_PUBSUB_2_GCS = {
    "pubsub_2_gcs": ""# TODO: add as parameter
}


default_args = dict(
    owner="ShopStream DE Team",
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(minutes=30),
    sla=timedelta(hours=24)
)

local_tz = pendulum.timezone("America/Sao_Paulo")
logger = logging.getLogger('airflow.task')


@dag(
    'execute_shopstream',
    start_date=datetime(2025, 5, 5),
    schedule="30 3 * * 1",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    #on_success_callback=partial(send_alert_message, status_message="Succeded"),
    #on_failure_callback=partial(send_alert_message, status_message="Failed"),
    #sla_miss_callback=log_on_sla_miss,
    tags=['shopstream', 'batch', 'daily']
)
def shopstream_pipeline():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task_group(group_id="ingestion_to_raw")
    def ingestion_to_raw():
        @task(task_id="gcf-simulate-data-source")
        def gcf_simulate_data_source():
            response = call_cloud_function(
                URL_GCF, 
                ARG_GCF_SIMULATE_DATA_SOURCE
            )
            return response


        @task(task_id="gcf-pubsub-2-gcs")
        def gcf_pubsub_2_gcs():
            response = call_cloud_function(
                URL_GCF, 
                ARG_GCF_PUBSUB_2_GCS
            )
            return response

        gcf_simulate_data_source() >> gcf_pubsub_2_gcs()


    @task_group(group_id="transform_to_trusted")
    def transform_to_trusted():
        dataform_compilation_result = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result={
                "git_commitish": GIT_COMMITISH,
            },
        )

        create_workflow_invocation_trusted = DataformCreateWorkflowInvocationOperator(
            task_id='create_workflow_invocation_trusted',
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull(task_ids='transform_to_trusted.create_compilation_result')['name'] }}",
                "invocation_config": { 
                    "included_tags": ["silver"],
                    "transitive_dependencies_included": True, 
                    "transitive_dependents_included": True,
                },
            },
        )

        create_workflow_invocation_gold = DataformCreateWorkflowInvocationOperator(
            task_id='create_workflow_invocation_gold',
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull(task_ids='transform_to_trusted.create_compilation_result')['name'] }}",
                "invocation_config": { 
                    "included_tags": ["gold"],
                    "transitive_dependencies_included": True, 
                    "transitive_dependents_included": True,
                },
            },
        )

        dataform_compilation_result >> create_workflow_invocation_trusted >> create_workflow_invocation_gold
    
    start >> ingestion_to_raw() >> transform_to_trusted() >> end

dag_shopstream = shopstream_pipeline()
