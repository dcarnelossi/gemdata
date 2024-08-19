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
    "00-Start-CreateInfra2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Start-CreateInfra", "v1", "teste"],
) as dag:

    @task(provide_context=True)
    def trigger_dag_create_infra(**kwargs):
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
            integration_id = [integration[0] for integration in integration_ids]
           
            return integration_id
           
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during get_integration_ids - {e}"
            )
            raise
        
    
    # Crie a tarefa Python para disparar a DAG
    teste1=trigger_task = PythonOperator(
        task_id="trigger_import_dags",
        python_callable=trigger_dag_create_infra,
        provide_context=True,
    )
        
    teste=TriggerDagRunOperator(
            task_id=f"0-CreateInfra-teste",
            trigger_dag_id="0-CreateInfra",  # Substitua pelo nome real da sua segunda DAG
            conf={
                "PGSCHEMA": '{{ ti.xcom_pull(task_ids="trigger_import_dags") }}',
                "ISDAILY": False                
                },  
        )


    teste1 >> teste


 