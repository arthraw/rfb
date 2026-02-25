from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 
from pendulum import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def run_scrapy():
    process = CrawlerProcess(get_project_settings())
    process.crawl("rfb_spider")
    process.start()


@dag(
    dag_id="ingst_rfb_data",
    schedule="0 0 1 * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=["ingestion", "rfb"],
)
def ingst_rfb_data():
    define_files_to_download = BashOperator(
        task_id="define_files_to_download",
        bash_command="cd /usr/local/airflow && python -m scrapy crawl rfb_spider",
    )
    
    download_files = BashOperator(
        task_id="download_files",
        bash_command="python -m src.ingestion.download_rfb_data",
    )

    send_files = BashOperator(
        task_id="send_files",
        bash_command="python -m src.ingestion.send_data_to_remote",
    )
    define_files_to_download >> download_files >> send_files

ingst_rfb_data()