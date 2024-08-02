from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os


def get_file_name(**kwargs):
    current_time = datetime.now() - timedelta(hours=1)  # Subtrai uma hora da hora atual
    file_name = f'Picking/picking-{current_time.strftime("%Y-%m-%d-%H.csv")}'
    kwargs['ti'].xcom_push(key='file_name', value=file_name)  # Passa o nome do arquivo para o XCom

def download_file(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='get_file_name', key='file_name')

    local_file_path = os.path.join('/opt/airflow/data_lake/Picking', os.path.basename(file_name))
    
    # Cria uma instância do GCSToLocalFilesystemOperator com o nome do arquivo
    download_task = GCSToLocalFilesystemOperator(
        task_id='download_file',
        bucket='wms-extract-hour',
        object_name=file_name,
        filename=local_file_path,
        gcp_conn_id='google-cloud-storage',
        trigger_rule='all_success'  # Executa apenas se as tarefas anteriores tiverem sucesso
    )
    # Execute a tarefa criada
    download_task.execute(context=kwargs)

# Definição do DAG
default_args = {
    'start_date': datetime(2024, 8, 2, 11, 15, 0),  # Início às 11:15 
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'gcs_download_file',
    default_args=default_args,
    schedule_interval='@hourly',  # Executa uma vez por hora
    catchup=False
) as dag:

    get_file_name_task = PythonOperator(
        task_id='get_file_name',
        python_callable=get_file_name,
        provide_context=True
    )

    download_file_task = PythonOperator(
        task_id='download_file_task',
        python_callable=download_file,
        provide_context=True
    )

    get_file_name_task >> download_file_task
