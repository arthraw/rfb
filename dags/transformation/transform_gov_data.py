from airflow.sdk import dag, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from pendulum import datetime
from datetime import datetime as dt
from pathlib import Path
from cosmos.profiles.databricks import DatabricksTokenProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
import logging
from cosmos import RenderConfig

render_config = RenderConfig(
    emit_datasets=False,
    load_method=LoadMode.DBT_LS,
    enable_mock_profile=True,
)
DATABRICKS_CONN_ID = Variable.get("DATABRICKS_CONN_ID")
DATABRICKS_ROOT_PROJECT_PATH = Variable.get("DATABRICKS_ROOT_PROJECT_PATH")
log = logging.getLogger(__name__)
log = LoggingMixin().log
default_args = {
    'owner': 'Arthur Andrade',
    'retries': 2,
}

DBT_PROJECT_PATH = Path("/usr/local/airflow/src/dbt")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={
            "catalog": "rfb",
            "schema": "trusted",
        },
    ),
)

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
        empresa_job = DatabricksSubmitRunOperator(
            task_id="run_empresas_transient_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Copy Empresas RFB Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "copy_empresa_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}jobs/job_empresa_transient_data_to_bronze"
                    },
                    'serverless': {}
                }]
            }
        )

        cnae_job = DatabricksSubmitRunOperator(
            task_id="run_cnaes_transient_job",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json={
                "run_name": f"Copy Cnaes RFB Data - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                "tasks": [{
                    "task_key": "copy_cnae_task",
                    "notebook_task": {
                        "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}jobs/job_cnae_transient_data_to_bronze"
                    },
                    'serverless': {}
                }]
            }
        )

        [rfb_job, ibge_job, municipios_job, empresa_job, cnae_job]
        log.info('Databricks jobs submitted.')
        

    @task_group(
        group_id="dq_raw_validations",
        tooltip="Data quality validation tasks for raw data",
        ui_color="#4e7cf0"
    )
    def data_quality_tests():
        def make_dq_operator(task_id, run_name, task_key, notebook_path):
            return DatabricksSubmitRunOperator(
                task_id=task_id,
                databricks_conn_id=DATABRICKS_CONN_ID,
                json={
                    "run_name": f"{run_name} - {dt.now().strftime('%Y-%m-%d_%H:%M:%S')}",
                    "tasks": [{
                        "task_key": task_key,
                        "notebook_task": {
                            "notebook_path": f"{DATABRICKS_ROOT_PROJECT_PATH}{notebook_path}"
                        },
                       'serverless': {}
                    }]
                }
            )

        rfb_dq = make_dq_operator(
            task_id="run_raw_estabelecimento_dq_validation",
            run_name="Data Quality Test - Estabelecimento Data",
            task_key="dq_test_estabelecimento_task",
            notebook_path="quality/dq_estabelecimento_bronze"
        )
        ibge_dq = make_dq_operator(
            task_id="run_raw_ibge_dq_validation",
            run_name="Data Quality Test - IBGE Data",
            task_key="dq_test_ibge_task",
            notebook_path="quality/dq_municipio_pib_bronze"
        )
        municipios_dq = make_dq_operator(
            task_id="run_raw_municipios_dq_validation",
            run_name="Data Quality Test - Municipios RFB Data",
            task_key="dq_test_municipios_task",
            notebook_path="quality/dq_municipios_rfb_bronze"
        )
        cnae_dq = make_dq_operator(
            task_id="run_raw_cnaes_dq_validation",
            run_name="Data Quality Test - Cnaes RFB Data",
            task_key="dq_test_cnae_task",
            notebook_path="quality/dq_cnae_bronze"
        )
        empresa_dq = make_dq_operator(
            task_id="run_raw_empresas_dq_validation",
            run_name="Data Quality Test - Empresas RFB Data",
            task_key="dq_test_empresas_task",
            notebook_path="quality/dq_empresa_bronze"
        )

        [rfb_dq, ibge_dq, municipios_dq, cnae_dq, empresa_dq]
        log.info('Data quality tests completed.')

    @task_group(
        group_id="dbt_transformations",
        tooltip="Group of tasks to run dbt transformations",
        ui_color="#e08e24"
    )
    def dbt_transformations():
        DbtTaskGroup(
            group_id="dbt_models",
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path="/usr/local/bin/dbt",
            ),
            render_config=render_config,
            operator_args={
                "install_deps": True,
                "openlineage_events_completes": [], 
            },
        )

    databricks_jobs() >> data_quality_tests() >> dbt_transformations()

transform_gov_data()