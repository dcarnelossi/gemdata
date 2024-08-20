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

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-StartDaily2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,
#    render_template_as_native_obj=True,
    # params={
    #     "PGSCHEMA": Param(
    #         type="string",
    #         title="PGSCHEMA:",
    #         description="Enter the integration PGSCHEMA.",
    #         section="Important params",
    #         min_length=1,
    #         max_length=200,
    #     ),
    #     "ISDAILY": Param(
    #         type="boolean",
    #         title="ISDAILY:",
    #         description="Enter com False (processo total) ou True (processo diario) .",
    #         section="Important params",
    #         min_length=1,
    #         max_length=10,
    #     )
    # },
) as dag:

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

    def trigger_dag_run_task(integration_ids):
        for integration_id in integration_ids:
            conf = {
                "PGSCHEMA": integration_id,
                "ISDAILY": True
            }
            trigger_dag_run(
                dag_id="1-ImportVtex-Brands-Categories-Skus-Products",
                conf=conf
            )
    with TaskGroup("trigger_dags_group", tooltip="Trigger DAGs for each integration_id") as trigger_dags_group:
        # Crie a tarefa Python para disparar a DAG
        trigger_task = PythonOperator(
            task_id="trigger_import_dags",
            python_callable=trigger_dag_run_task,
            op_args=[get_integration_ids()],
        )

    get_integration_ids() >> trigger_task