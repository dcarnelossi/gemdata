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

    @task(provide_context=True)
    def get_integration_id(**context):
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = """
            select distinct id from public.integrations_integration
            where is_active = true 
            and infra_create_status = false
            limit 1;
            """
            integration_ids = hook.get_records(query)
            if not integration_ids:
                print("Nenhuma integração para criar infraestrutura")
                return None

            integration_id = integration_ids[0][0]
            print(f"Iniciando criação de infraestrutura para integração: {integration_id}")
           
            
           
           
            return integration_id

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during get_integration_id - {e}"
            )
            raise

    def choose_next_step(integration_id, **context):
        if integration_id:
            return "trigger_dag_crete_infra"
        else:
            return "no_integration_ids"

    def trigger_dag_run(integration_id, **context):
        return TriggerDagRunOperator(
            task_id=f"0-CreateInfra-{integration_id}",
            trigger_dag_id="0-CreateInfra",  # Substitua pelo nome real da sua segunda DAG
            conf={
                "PGSCHEMA": integration_id,
                "ISDAILY": False              
            },
        ).execute(context=context)

    integration_id = get_integration_id()

    next_step = BranchPythonOperator(
        task_id="check_integration_id",
        python_callable=choose_next_step,
        op_args=[integration_id],
        provide_context=True,
    )

    trigger_dag = PythonOperator(
        task_id="trigger_dag_crete_infra",
        python_callable=trigger_dag_run,
        op_args=[integration_id],
        provide_context=True,
    )

    no_integration_ids = EmptyOperator(
        task_id="no_integration_ids"
    )

    integration_id >> next_step
    next_step >> trigger_dag
    next_step >> no_integration_ids
