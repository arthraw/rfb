from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
import logging

log = logging.getLogger(__name__)
log = LoggingMixin().log
default_args = {
    'owner': 'Arthur Andrade',
    'retries' : 2,
}

@dag(
    dag_id="ingst_rfb_data",
    # schedule="0 * * * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=["ingestion", "rfb"],
    default_args=default_args
)
def ingst_rfb_data():
    define_files_to_download = BashOperator(
        task_id="define_files_to_download",
        bash_command="cd /usr/local/airflow && python -m scrapy crawl rfb_spider",
    )
        
    @task
    def check_files_found():
        # Check spider result
        import os
        urls_file = "/tmp/rfb_files.json"
        
        if not os.path.exists(urls_file) or os.path.getsize(urls_file) == 2: # file exists but is empty (only '[]')
            raise AirflowSkipException("Sem atualizações na RFB.")
        
        log.info("Files found, continuing pipeline.")

    download_files = BashOperator(
        task_id="download_files",
        bash_command="python -m src.ingestion.download_rfb_data",
    )
    
    download_ibge_data = BashOperator(
        task_id="download_ibge_data",
        bash_command="python -m src.ingestion.read_ibge_data",
        trigger_rule="none_skipped"
    )

    download_municipios_rfb = BashOperator(
        task_id="download_municipios_rfb",
        bash_command="python -m src.ingestion.municipios_rfb",
        trigger_rule="none_skipped"
    )

    send_files = BashOperator(
        task_id="send_files",
        bash_command="python -m src.ingestion.send_data_to_remote",
        trigger_rule="none_skipped"
    )

    trigger_transformation_dag = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="transform_gov_data",
        trigger_rule="none_skipped",
        wait_for_completion=False,
        poke_interval=30,
    )

    log.info("Defined tasks for RFB data ingestion DAG.")
    define_files_to_download >> check_files_found() >> [download_files, download_ibge_data, download_municipios_rfb] >> send_files >> trigger_transformation_dag
    log.info("Set task dependencies for RFB data ingestion DAG.")

ingst_rfb_data()