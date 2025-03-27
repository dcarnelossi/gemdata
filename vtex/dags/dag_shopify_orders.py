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
    get_api_conection_info,

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
    "shopify-1-Orders",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["shopify", "orders", "import"],
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
    def orders_shopify(**kwargs):
        team_id = kwargs["params"]["PGSCHEMA"]
        isdaily = kwargs["params"]["ISDAILY"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(team_id)
        api_conection_info = get_api_conection_info(team_id)
        # last_rum_date = get_import_last_rum_date(coorp_conection_info, team_id)

        
        try:
                query = """
                UPDATE public.integrations_integration
                SET daily_run_date_ini = %s,
                isdaily_manual = false
                WHERE id = %s;
                """
                # Initialize the PostgresHook
                hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                # Execute the query with parameters
                
                hook2.run(query, parameters=(datetime.now(),team_id))

        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e

        from modules import orders_dynamic_shopify

        try:
            end_date = datetime.now() + timedelta(days=1)

            #alterado por gabiru de: timedelta(days=1) coloquei timedelta(days=90)
            if not isdaily :
                start_date = end_date - timedelta(days=735)
                min_date = end_date - timedelta(days=735)

            else:
                #start_date = last_rum_date["import_last_run_date"] - timedelta(days=90)
                start_date = end_date - timedelta(days=1)
                min_date = end_date - timedelta(days=735)

                
            orders_dynamic_shopify.set_globals(
                api_conection_info,
                data_conection_info,
                coorp_conection_info,
                type_api= 'orders',
                start_date=start_date,
                end_date=end_date,
                minimum_date = min_date
                
            )
            print(start_date)
            start_date = start_date.strftime("%Y-%m-%d")
            return start_date
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    

    orders_task = orders_shopify()
        
    trigger_dag_orders_item_shopy = TriggerDagRunOperator(
        task_id="trigger_dag_orders_items_shopify",
        trigger_dag_id="shopify-2-Orders-items",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
            "START_DATE": "{{ ti.xcom_pull(task_ids='orders_shopify') }}",

        },  # Se precisar passar informações adicionais para a DAG_B
    )
       


    orders_task >>  trigger_dag_orders_item_shopy 
    
    