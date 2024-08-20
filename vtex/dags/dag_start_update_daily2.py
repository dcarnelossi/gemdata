import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.task_group import TaskGroup

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
@task
def get_integration_ids():
    try:
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

def trigger_dag_run(dag_id, conf, execution_date=None, replace_microseconds=False):
    # Função auxiliar para disparar a DAG
    trigger_dag(
        dag_id=dag_id,
        run_id=f"manual__{datetime.utcnow().isoformat()}",
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=replace_microseconds,
    )

def trigger_dag_run_task(integration_id):
    conf = {
        "PGSCHEMA": integration_id,
        "ISDAILY": True
    }
    trigger_dag_run(
        dag_id="1-ImportVtex-Brands-Categories-Skus-Products",
        conf=conf
    )

def create_trigger(integration_ids, dag):
    with TaskGroup("trigger_dags_group", tooltip="Trigger DAGs for each integration_id", dag=dag) as trigger_dags_group:
        for i, integration_id in enumerate(integration_ids):
            PythonOperator(
                task_id=f"trigger_dag_{i}",
                python_callable=trigger_dag_run_task,
                op_args=[integration_id],
                dag=dag,
            )
    return trigger_dags_group

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-StartDaily2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,

) as dag:

    integration_ids = get_integration_ids()

    create_trigger_tasks = PythonOperator(
        task_id="create_trigger_tasks",
        python_callable=create_trigger,
        op_args=[integration_ids, dag],
        dag=dag,
    )
