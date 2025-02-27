import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from modules.dags_common_functions import (
    get_coorp_conection_info,
    get_data_conection_info,
    integrationInfo,
    get_api_conection_info,
    get_import_last_rum_date,
)


# Lista de requisitos
requirements = [
    "openai==1.40.1",
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
    "8-ImportVtex-Client-Profile",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "client-profile", "PROCESS"],
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
    def client_profile(**kwargs):
        integration_id = kwargs["params"]["PGSCHEMA"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(integration_id)
        api_conection_info = get_api_conection_info(integration_id)

        from modules import client_profile

        try:
            client_profile.set_globals(
                api_conection_info, data_conection_info, coorp_conection_info
            )

             
            query = f"""
            UPDATE "{integration_id}".orders_list
            SET is_change = false
            where is_change = true;
            """
            # Initialize the PostgresHook
            hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")

            # Execute the query with parameters
            hook.run(query)


            # Pushing data to XCom
            kwargs["ti"].xcom_push(key="integration_id", value=integration_id)
            kwargs["ti"].xcom_push(
                key="coorp_conection_info", value=coorp_conection_info
            )
            kwargs["ti"].xcom_push(key="data_conection_info", value=data_conection_info)
            kwargs["ti"].xcom_push(key="api_conection_info", value=api_conection_info)


            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    
    
    

    trigger_dag_create_tab = TriggerDagRunOperator(
        task_id="trigger_dag_create_tab_global",
        trigger_dag_id="9-create-table-client",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY":"{{ params.ISDAILY }}"
           
        },  # Se precisar passar informações adicionais para a DAG_B
    )

     # Configurando a dependência entre as tasks

    try:
            
        client_profile_task = client_profile()

        client_profile_task >>  trigger_dag_create_tab
    
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow