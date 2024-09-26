import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    "99-Dagteste",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Update", "v1", "vtex"],
    render_template_as_native_obj=True,
    params={
                
    },
) as dag:

    @task(provide_context=True)
    def update_daily_orders_list(**kwargs):
        from modules import relatorio_mensal
        try:
         relatorio_mensal.ok()
        except Exception as e:
         print(f"Erro ao verificar a existência da tabela{e}")
        


    # trigger_dag_orders = TriggerDagRunOperator(
    #     task_id="trigger_dag_orders",
    #     trigger_dag_id="4-ImportVtex-Orders",  # Substitua pelo nome real da sua segunda DAG
    #     conf={
    #         "PGSCHEMA": "{{ params.PGSCHEMA }}"
    #     },  # Se precisar passar informações adicionais para a DAG_B
    # )

    # Configurando a dependência entre as tarefas

    update_daily_orders_list_task = update_daily_orders_list()

    update_daily_orders_list_task 
