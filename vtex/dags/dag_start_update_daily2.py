from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.api.common.experimental.trigger_dag import trigger_dag

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
dag = DAG(
    '0-StartDaily2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,
)

@task
def get_integration_ids():
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

def trigger_dag_run(dag_id, conf, execution_date=None, replace_microseconds=False):
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

# Usando o decorator @dag para criar o objeto DAG
with dag:

    start = DummyOperator(task_id="start")

    integration_ids = get_integration_ids()

    with TaskGroup("integration_tasks", prefix_group_id=False) as integration_tasks:
        trigger_task = PythonOperator.partial(
            task_id="trigger_task",
            python_callable=trigger_dag_run_task
        ).expand(op_args=[[integration_ids]])

    end = DummyOperator(task_id="end")

    start >> integration_tasks >> end