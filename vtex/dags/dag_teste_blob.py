import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator


# Lista de requisitos
requirements = [
    "openai==1.6.0",
    "azure-core==1.29.6",
    "azure-cosmos==4.5.1",
    "azure-storage-blob==12.19.0",
]

# Configuração padrão do DAG
default_args = {
    "owner": "Daniel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-testeblob",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Update", "v1", "ALTERAR"],

) as dag:


    upload_task = LocalFilesystemToWasbOperator(
        task_id='upload_to_blob',
        file_path='/caminho/para/o/seu/arquivo.txt',
        container_name='jsondashboard',
        blob_name='arquivo-no-blob.txt',
        wasb_conn_id='azure_blob_storage_json'
    )
    upload_task