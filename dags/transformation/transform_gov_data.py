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
        tooltip="Group of tasks to run Databricks jobs for copy data",
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
        municipios_job = DatabricksSubmitRunOperator(
            task_id="run_municipios_transient_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Copy Municipios RFB Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "copy_municipios_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}jobs/job_municipios_rfb_transient_data_to_bronze"
                    },
                    'serverless': {}
                }]
            }
        )

        [rfb_job, ibge_job, municipios_job]
        log.info('Databricks jobs submitted.')
        

    @task_group(
        group_id="dq_raw_validations",
        tooltip="Data quality validation tasks for raw data",
        ui_color="#4e7cf0"
    )
    def data_quality_tests():
        log.info('Running data quality tests.')
        rfb_dq = DatabricksSubmitRunOperator(
            task_id="run_raw_estabelecimento_dq_validation",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Data Quality Test - Estabelecimento Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "dq_test_estabelecimento_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}quality/dq_estabelecimento_bronze"
                    },
                    "libraries": [
                        {"pypi": {"package": "great-expectations>=1.15.2"}}
                    ],
                    'serverless': {}
                }]
            }
        )

        ibge_dq = DatabricksSubmitRunOperator(
            task_id="run_raw_ibge_dq_validation",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Data Quality Test - IBGE Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "dq_test_ibge_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}quality/dq_municipio_pib_bronze"
                    },
                    "libraries": [
                        {"pypi": {"package": "great-expectations>=1.15.2"}}
                    ],
                    'serverless': {}
                }]
            }
        )
        
        municipios_dq = DatabricksSubmitRunOperator(
            task_id="run_raw_municipios_dq_validation",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Data Quality Test - Municipios RFB Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "dq_test_municipios_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}quality/dq_municipios_rfb_bronze"
                    },
                    "libraries": [
                        {"pypi": {"package": "great-expectations>=1.15.2"}}
                    ],
                    'serverless': {}
                }]
            }
        )
        [rfb_dq, ibge_dq, municipios_dq]
        log.info('Data quality tests completed.')



    databricks_jobs() >> data_quality_tests()

transform_gov_data()