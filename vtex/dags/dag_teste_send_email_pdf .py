
import logging

from datetime import datetime
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook



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


with DAG(
    "d-teste-email",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
    render_template_as_native_obj=True,
    params={},
) as dag:
    

        
    @task(provide_context=True)
    def report_baixar_pdf(**kwargs):
        try:
            from modules import send_email
            send_email.send_email_via_connection("gabriel.pereira.sousa@gmail.com","teste aa","aaa",False) 
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do blob para anexar - {e}")
            raise

    teste=report_baixar_pdf()

    teste
    

      
