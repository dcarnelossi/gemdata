import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.email import EmailOperator


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
    "b2-report-send-pdfteste",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
    params={},
) as dag:
    

    # Configurando o EmailOperator
    enviar_email = EmailOperator(
        task_id='envia_email',
        to='gabriel.pereira.sousa@gmail.com',
        subject='Teste de Envio de E-mail',
        html_content='<p>Este é um teste de envio de e-mail pelo Airflow.</p>',
    )

    enviar_email