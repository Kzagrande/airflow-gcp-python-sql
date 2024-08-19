from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os


def get_file_name(**kwargs):
    current_time = datetime.now() - timedelta(hours=1)  # Subtrai uma hora da hora atual
    file_name = f'PUTAWAY-{current_time.strftime("%Y-%m-%d-%H.csv")}'
    kwargs['ti'].xcom_push(key='file_name', value=file_name)  # Passa o nome do arquivo para o XCom

def download_file_d(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='get_file_name_task', key='file_name')  # Corrige o task_id
    print('filename :D', file_name) 

    local_file_path = os.path.join('/opt/airflow/data_lake/Bronze/WHD/Putaway', file_name)
    
    download_task = GCSToLocalFilesystemOperator(
        task_id='download_file_D',
        bucket='wms-uph-pipeline',
        object_name=f'Bronze/WHD/Putaway/D-{file_name}',
        filename=local_file_path,
        gcp_conn_id='google-cloud-storage',
        trigger_rule='all_success'
    )
    download_task.execute(context=kwargs)

def download_file_b(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='get_file_name_task', key='file_name')  # Corrige o task_id
    print('filename :D', file_name)

    local_file_path = os.path.join('/opt/airflow/data_lake/Bronze/WHB/Putaway', file_name)
    
    download_task = GCSToLocalFilesystemOperator(
        task_id='download_file_B',
        bucket='wms-uph-pipeline',
        object_name=f'Bronze/WHB/Putaway/B-{file_name}',
        filename=local_file_path,
        gcp_conn_id='google-cloud-storage',
        trigger_rule='all_success'
    )
    download_task.execute(context=kwargs)

def upload_silver_to_gcs(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='get_file_name_task', key='file_name')
    local_file_path = os.path.join('/opt/airflow/data_lake/Silver/Putaway', file_name)
    
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_silver_gcs_task',
        bucket='wms-uph-pipeline',
        dst=f'Silver/Putaway/{file_name}',
        src=local_file_path,
        gcp_conn_id='google-cloud-storage'
    )
    upload_task.execute(context=kwargs)

def upload_gold_to_gcs(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='get_file_name_task', key='file_name')
    local_file_path = os.path.join('/opt/airflow/data_lake/Gold/Putaway', file_name)
    
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gold_gcs_task',
        bucket='wms-uph-pipeline',
        dst=f'Gold/Putaway/{file_name}',
        src=local_file_path,
        gcp_conn_id='google-cloud-storage'
    )
    upload_task.execute(context=kwargs)


default_args = {
    'start_date': datetime(2024, 8, 13, 12, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}



with DAG(
    'putaway_pipeline_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Executa uma vez por hora
    catchup=False
) as dag:

    get_file_name_task = PythonOperator(
        task_id='get_file_name_task',
        python_callable=get_file_name,
        provide_context=True
    )

    download_file_d_task = PythonOperator(
        task_id='download_file_d_task',
        python_callable=download_file_d,
        provide_context=True
    )

    download_file_b_task = PythonOperator(
        task_id='download_file_b_task',
        python_callable=download_file_b,
        provide_context=True
    )

    
    transform_to_silver = BashOperator(
        task_id='transform_to_silver',
        bash_command="python3 /opt/airflow/scripts/Transform/Putaway/transform_to_silver.py {{ ti.xcom_pull(task_ids='get_file_name_task', key='file_name') }}"
    )

    upload_to_silver_gcs_task = PythonOperator(    
        task_id='upload_to_silver_gcs_task',
        python_callable=upload_silver_to_gcs,
        provide_context=True
    )

    upload_to_dw_task = BashOperator(
        task_id='upload_to_dw_task',
       bash_command="python3 /opt/airflow/scripts/Load/Putaway/load_to_dw.py {{ ti.xcom_pull(task_ids='get_file_name_task', key='file_name') }}"
    )

    transform_to_gold = BashOperator(
        task_id='transform_to_gold',
        bash_command="python3 /opt/airflow/scripts/Transform/Putaway/transform_to_gold.py {{ ti.xcom_pull(task_ids='get_file_name_task', key='file_name') }}"
    )

    upload_to_gold_gcs_task = PythonOperator(    
        task_id='upload_to_gold_gcs_task',
        python_callable=upload_gold_to_gcs,
        provide_context=True
    )

    
    upload_to_dataset_task = BashOperator(
        task_id='upload_to_dataset_task',
       bash_command="python3 /opt/airflow/scripts/Load/Putaway/load_to_dataset.py {{ ti.xcom_pull(task_ids='get_file_name_task', key='file_name') }}"
    )

    # Definição das dependências
    get_file_name_task >> [download_file_d_task, download_file_b_task] >> transform_to_silver
    transform_to_silver >>  [upload_to_silver_gcs_task, upload_to_dw_task] >> transform_to_gold
    transform_to_gold >> [upload_to_gold_gcs_task , upload_to_dataset_task]