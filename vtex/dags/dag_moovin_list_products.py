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
    "moovin-1-Products",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["moovin", "lista_products", "IMPORT"],
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
    def listproduct(**kwargs):
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
        
        from modules import moovin_list_products   

        try:
            end_date = datetime.now() + timedelta(days=1)

            #alterado por gabiru de: timedelta(days=1) coloquei timedelta(days=90)
            if not isdaily :
                start_date = end_date - timedelta(days=735)
                # min_date = end_date - timedelta(days=735)

            else:
                #start_date = last_rum_date["import_last_run_date"] - timedelta(days=90)
                start_date = end_date - timedelta(days=10)
                # min_date = end_date - timedelta(days=735)   


            moovin_list_products.set_globals(
                api_conection_info,
                data_conection_info,
                coorp_conection_info,
                type_api= 'list_products',
                start_date=start_date,
                isdaily = isdaily 
               
            )

            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    
        
    trigger_dag_orders = TriggerDagRunOperator(
        task_id="trigger_dag_orders",
        trigger_dag_id="moovin-2-Categories",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
             "ISDAILY": "{{ params.ISDAILY }}",
        },  # Se precisar passar informações adicionais para a DAG_B
    )
    try:
        listproduct_task = listproduct()


        listproduct_task >>  trigger_dag_orders 
    
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow