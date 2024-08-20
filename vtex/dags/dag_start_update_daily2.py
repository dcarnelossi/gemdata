from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
from datetime import datetime
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


# Define a DAG principal

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-StartDaily2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,

) as dag:

    @task
    def get_customer_ids():
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = """
            select distinct id from public.integrations_integration
            where is_active = true 
            and infra_create_status = true limit 2
            """
            integration_ids = hook.get_records(query)
            return [integration[0] for integration in integration_ids]

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during get_integration_ids - {e}"
            )
            return []


    @task
    def generate_run_id(customer_id: str, run_id: str):
        return f"{run_id}_{customer_id}"

    # Mapeia a tarefa generate_run_id para cada ID de cliente
    run_ids = generate_run_id.expand(customer_id=get_customer_ids(), run_id="{{ run_id }}")

    # Dispara a DAG filha para cada run_id gerado
    trigger_dag_filha = TriggerDagRunOperator.partial(
        task_id='trigger_dag_filha',
        trigger_dag_id='1-ImportVtex-Brands-Categories-Skus-Products',
        wait_for_completion=True  # Aguarda a conclusão da DAG filha
    ).expand(
        conf=run_ids.map(lambda run_id: {'run_id': run_id, 'customer_id': run_id.split('_')[-1]})
    )

    # Monitora a DAG bisneta usando ExternalTaskSensor
    # Supondo que você já saiba o nome das tarefas na DAG bisneta para monitorar
    from airflow.sensors.external_task import ExternalTaskSensor

    wait_for_bisneta = ExternalTaskSensor.partial(
        task_id='wait_for_dag_bisneta',
        external_dag_id='dag_bisneta',
        external_task_id=None,  # Ou especifique o task_id a ser monitorado
        mode='poke',
        timeout=600,
        poke_interval=30,
    ).expand(
        external_task_run_id=run_ids  # Monitora especificamente o run_id gerado
    )

    # Definindo as dependências das tarefas
    run_ids >> trigger_dag_filha >> wait_for_bisneta