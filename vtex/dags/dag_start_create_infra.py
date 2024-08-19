import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
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
    "00-Start-CreateInfra",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Start-CreateInfra", "v2", "Schedule"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_integration_ids():
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = """
            select distinct id from public.integrations_integration
            where is_active = true 
            and infra_create_status = false
            limit 1;
            """
            integration_ids = hook.get_records(query)
            return integration_ids[0]

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during get_integration_ids - {e}"
            )
            raise
        
    def trigger_dag_crete_infra(integration_id):
        TriggerDagRunOperator(
        task_id=f"0-CreateInfra - {integration_id}",
        trigger_dag_id="0-CreateInfra",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": integration_id,
            "ISDAILY": 0
                
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    # Crie a tarefa Python para disparar a DAG
    trigger_task = PythonOperator(
        task_id="trigger_import_dags",
        python_callable=trigger_dag_crete_infra,
        op_args=[get_integration_ids()],
    )

    trigger_task