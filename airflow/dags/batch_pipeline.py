from airflow.decorators import dag, task, task_group
from airflow.models import Variable
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


config_params = Variable.get("AIRFLOW_SHOPSTREAM", deserialize_json=True)

PROJECT_ID = config_params["dataform"]["project_id"]
REPOSITORY_ID = config_params["dataform"]["repository_id"]
REGION = config_params["dataform"]["region"]
GIT_COMMITISH = config_params["dataform"]["git_commitish"]

URL_GCF_DATA_SOURCE = config_params["gcf"]["urls"]["data_source"]
URL_GCF_PUBSUB_2_GCS = config_params["gcf"]["urls"]["pubsub_2_gcs"]
ARG_GCF_SIMULATE_DATA_SOURCE = config_params["gcf"]["args"]["simulate_data_source"]
ARG_GCF_PUBSUB_2_GCS = config_params["gcf"]["args"]["pubsub_2_gcs"]


default_args = dict(
    owner=config_params["airflow"]["default_args"]["owner"],
    depends_on_past=False,  
    retries=config_params["airflow"]["default_args"]["retries"],
    retry_delay=timedelta(minutes=config_params["airflow"]["default_args"]["retry_delay_time"]),
    sla=timedelta(hours=config_params["airflow"]["default_args"]["sla_time"])
)

local_tz = pendulum.timezone("America/Sao_Paulo")
logger = logging.getLogger('airflow.task')


@dag(
    'execute_shopstream_pipe',
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
    logger.info("Starting shopstream_pipeline DAG definition...")

    logger.info("Initializing start and end tasks for DAG...")
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task_group(group_id="ingestion_to_raw")
    def ingestion_to_raw():
        logger.info("Starting ingestion_to_raw task group...")
        @task(task_id="gcf-simulate-data-source")
        def gcf_simulate_data_source():
            logger.info("Starting gcf_simulate_data_source task...")
            logger.debug(f"Calling cloud function at {URL_GCF_DATA_SOURCE} with args: {ARG_GCF_SIMULATE_DATA_SOURCE}")
            response = call_cloud_function(
                URL_GCF_DATA_SOURCE, 
                ARG_GCF_SIMULATE_DATA_SOURCE
            )
            logger.info(f"Received response from gcf_simulate_data_source: {response}")
            return response


        @task(task_id="gcf-pubsub-2-gcs")
        def gcf_pubsub_2_gcs():
            logger.info("Starting gcf_pubsub_2_gcs task...")
            logger.debug(f"Calling cloud function at {URL_GCF_PUBSUB_2_GCS} with args: {ARG_GCF_PUBSUB_2_GCS}")
            response = call_cloud_function(
                URL_GCF_PUBSUB_2_GCS, 
                ARG_GCF_PUBSUB_2_GCS
            )
            logger.info(f"Received response from gcf_pubsub_2_gcs: {response}")
            return response

        gcf_simulate_data_source() >> gcf_pubsub_2_gcs()


    @task_group(group_id="transform_to_trusted")
    def transform_to_trusted():
        logger.info("Starting transform_to_trusted task group...")
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
