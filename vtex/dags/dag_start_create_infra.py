import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator

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
    schedule_interval=timedelta(minutes=10),  # Rodar a cada 10 minuto
    catchup=False,
    default_args=default_args,
    tags=["Start-CreateInfra", "v2", "Schedule"],
    render_template_as_native_obj=True,
) as dag:
    def create_infra_dag():

        @task()
        def get_integration_id():
            try:
                # Conecte-se ao PostgreSQL e execute o script
                hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                query = """
                SELECT DISTINCT id, hosting 
                FROM public.integrations_integration
                WHERE is_active = TRUE 
                AND infra_create_status = FALSE
                LIMIT 1;
                """
                integration_ids = hook.get_records(query)
                if not integration_ids:
                    print("Nenhuma integração para criar infraestrutura")
                    return None

                integration_id = integration_ids[0]
                print(f"Iniciando criação de infraestrutura para integração: {integration_id}")
                return {"id": integration_id[0], "hosting": integration_id[1]}

            except Exception as e:
                logging.exception(
                    f"An unexpected error occurred during get_integration_id - {e}"
                )
                raise

        def choose_next_step(ti, **context):
            integration_data = ti.xcom_pull(task_ids="get_integration_id")
            if integration_data:
                return "trigger_dag_crete_infra"
            else:
                return "no_integration_ids"

        def trigger_dag_run(ti, **context):
            integration_data = ti.xcom_pull(task_ids="get_integration_id")
            integration_id = integration_data["id"]
            hosting = integration_data["hosting"]

            trigger = TriggerDagRunOperator(
                task_id=f"dev-0-CreateInfra-{integration_id}",
                trigger_dag_id="dev-0-CreateInfra",  # Substitua pelo nome real da sua segunda DAG
                conf={
                    "PGSCHEMA": integration_id,
                    "ISDAILY": False,
                    "HOSTING": hosting
                },
            )
            trigger.execute(context=context)

        integration_id_task = get_integration_id()

        next_step = BranchPythonOperator(
            task_id="check_integration_id",
            python_callable=choose_next_step,
        )

        trigger_dag = PythonOperator(
            task_id="trigger_dag_crete_infra",
            python_callable=trigger_dag_run,
        )

        no_integration_ids = EmptyOperator(task_id="no_integration_ids")

        integration_id_task >> next_step
        next_step >> [trigger_dag, no_integration_ids]


    dag_instance = create_infra_dag()
