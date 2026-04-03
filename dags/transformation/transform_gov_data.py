from airflow.sdk import dag, task_group, Variable, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from pendulum import datetime
from datetime import datetime as dt
import logging

DATABRICKS_CONN_ID = Variable.get("DATABRICKS_CONN_ID")
DATABRICKS_ROOT_PROJECT_PATH = Variable.get("DATABRICKS_ROOT_PROJECT_PATH")
log = logging.getLogger(__name__)
log = LoggingMixin().log
default_args = {
    'owner': 'Arthur Andrade',
    'retries': 1,
}

@dag(
    dag_id="transform_gov_data",
    # schedule="0 2 * * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=["transformation", "rfb"],
    default_args=default_args
)
def transform_gov_data():
    log.info("Initiating transformation DAG.")
    @task_group(
        group_id="databricks_jobs",
        tooltip="Group of tasks to run Databricks jobs for transforming government data",
        ui_color="#f0ad4e"
    )
    def databricks_jobs():
        log.info('Running Databricks jobs.')
        rfb_job = DatabricksSubmitRunOperator(
            task_id="run_estabelecimento_transient_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Copy Estabelecimento Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "copy_estabelecimento_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}jobs/job_estabelecimento_transient_data_to_bronze"
                    },
                    'serverless': {}
                }]
            }
        )

        ibge_job = DatabricksSubmitRunOperator(
            task_id="run_ibge_transient_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Copy IBGE Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "copy_ibge_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}jobs/job_ibge_transient_data_to_bronze"
                    },
                    'serverless': {}
                }]
            }
        )
        [rfb_job, ibge_job]
        log.info('Databricks jobs submitted.')
        

    databricks_jobs()

transform_gov_data()