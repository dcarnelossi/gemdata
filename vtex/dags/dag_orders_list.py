import logging
import time
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.dags_common_functions import (
    get_coorp_conection_info,
    get_data_conection_info,
    integrationInfo,
    get_api_conection_info,
    get_import_last_rum_date,
)


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


with DAG(
    "2-ImportVtex-Orders-List",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "orders-list", "IMPORT"],
    render_template_as_native_obj=True,
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
        ),
        "ISDAILY": Param(
            type="boolean",
            title="ISDAILY:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
            min_length=1,
            max_length=10,
        )
    },
) as dag:

    @task(provide_context=True)
    def orders_list(**kwargs):
        team_id = kwargs["params"]["PGSCHEMA"]
        isdaily = kwargs["params"]["ISDAILY"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(team_id)
        api_conection_info = get_api_conection_info(team_id)
        # last_rum_date = get_import_last_rum_date(coorp_conection_info, team_id)

        from modules import orders_list

        try:
            end_date = datetime.now()

            #alterado por gabiru de: timedelta(days=1) coloquei timedelta(days=90)
            if not isdaily :
                start_date = end_date - timedelta(days=730)
            else:
                #start_date = last_rum_date["import_last_run_date"] - timedelta(days=90)
                start_date = datetime.now() - timedelta(days=90)
                query = f"""truncate table "{team_id}".orders_list_daily;""" 
                hook = PostgresHook(postgres_conn_id="integrations-data-dev")
                hook.run(query)
                


            orders_list.set_globals(
                api_conection_info,
                data_conection_info,
                coorp_conection_info,
                start_date=start_date,
                end_date=end_date,
                isdaily = isdaily 
            )

            # Pushing data to XCom
            kwargs["ti"].xcom_push(key="team_id", value=team_id)
            kwargs["ti"].xcom_push(
                key="coorp_conection_info", value=coorp_conection_info
            )
            kwargs["ti"].xcom_push(key="data_conection_info", value=data_conection_info)
            kwargs["ti"].xcom_push(key="api_conection_info", value=api_conection_info)

            kwargs["ti"].xcom_push(key="isdaily", value=isdaily)

            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
        finally:
            pass
        
    trigger_dag_update_orders_list = TriggerDagRunOperator(
        task_id="trigger_dag_update_orders_list",
        trigger_dag_id="3-DagUpdate-Orders-List",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
             "ISDAILY": "{{ params.ISDAILY }}"
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    orders_list_task = orders_list()
    
    orders_list_task >> trigger_dag_update_orders_list
