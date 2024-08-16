import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import ShortCircuitOperator

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

def get_customer_ids(**kwargs):
    hook = PostgresHook(postgres_conn_id="appgemdata-dev")
    sql = """   select distinct id from public.integrations_integration
            where is_active = true 
            and infra_create_status = true limit 2"""
    ids = hook.get_records(sql)
    ids = [str(id[0]) for id in ids]  # Transform list of tuples to list of strings
    return ids

# def check_dag_status(ti, triggered_dag_run, **kwargs):
#     dag_run_status = triggered_dag_run.get_state()
#     if dag_run_status == 'success':
#         return True
#     return False

with DAG(
    "01-StartDaily",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    
) as dag:

        get_ids = PythonOperator(
            task_id='get_customer_ids',
            python_callable=get_customer_ids,
            provide_context=True,
        )


       # for customer_id in get_ids.output:
        trigger_dag = TriggerDagRunOperator(
                task_id=f'trigger_dag_imports_{get_ids.output}',
                trigger_dag_id='1-ImportVtex-Brands-Categories-Skus-Products',
                conf={
                    "PGSCHEMA": get_ids.output,
                    "ISDAILY": False
                }
              wait_for_completion=True,
            )

            # wait_for_dag = ShortCircuitOperator(
            #     task_id=f'wait_for_dag_for_customer_{customer_id}',
            #     python_callable=check_dag_status,
            #     op_args=[trigger_dag],
            #     provide_context=True,
            # )

        get_ids >> trigger_dag #>> wait_for_dag
