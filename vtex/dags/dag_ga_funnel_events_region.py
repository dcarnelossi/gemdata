import logging
import time
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


from modules.dags_common_functions import (
    get_data_conection_info,


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
    "ga-8-funnel-events-region",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["import", "funnelregion", "ga"],
    render_template_as_native_obj=True,
    params={
        "TEAMID": Param(
            type="string",
            title="Team_id:",
            description="Enter the integration team_id da integration.",
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
    def run_python(**kwargs):
        team_id = kwargs["params"]["TEAMID"]
        isdaily = kwargs["params"]["ISDAILY"]

        hook = WasbHook(wasb_conn_id='appgemdata-storage-homol')

        blob_name = f"5e164a4b-5e09-4f43-9d81-a3d22b09a01b/teste_api_ga.json"

        # Lê o conteúdo do blob (como bytes)
        blob_bytes = hook.read_file(container_name='jsondashboard-homol', blob_name=blob_name)

            
        # Decodifica e converte para dict
        blob_str = blob_bytes
        json_data = json.loads(blob_str)


        "o que eu pensei era puxar atraves do team_id mesmo da integration_integration apra pesquisar na integration_ga"

        # coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")
        api_conection_info = json_data #get_api_conection_info_ga()

        # last_rum_date = get_import_last_rum_date(coorp_conection_info, team_id)

        
        # try:
        #         query = """
        #         UPDATE public.integrations_integration
        #         SET daily_run_date_ini = %s,
        #         isdaily_manual = false
        #         WHERE id = %s;
        #         """
        #         # Initialize the PostgresHook
        #         hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
        #         # Execute the query with parameters
                
        #         hook2.run(query, parameters=(datetime.now(),team_id))

        # except Exception as e:
        #     logging.exception(f"An unexpected error occurred during DAG - {e}")
        #     raise e

        from modules import ga_funnel_events_region
        
        try:
            end_date = datetime.now() + timedelta(days=1)

            #alterado por gabiru de: timedelta(days=1) coloquei timedelta(days=90)
            if not isdaily :
                start_date = end_date - timedelta(days=735)
       #         min_date = end_date - timedelta(days=735)

            else:
                #start_date = last_rum_date["import_last_run_date"] - timedelta(days=90)
                start_date = end_date - timedelta(days=10)
        #        min_date = end_date - timedelta(days=735)

                
            ga_funnel_events_region.set_globals(
                api_conection_info,
                data_conection_info,
                start_date=start_date,
                end_date=end_date,
                
            )

            print(start_date)
            start_date = start_date.strftime("%Y-%m-%d")
            
            
            return start_date
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    

    pyhton_task = run_python()
        
    trigger_dag_ga_9_funnel_events_item = TriggerDagRunOperator(
        task_id="trigger_dag_ga_9_funnel_events_item",
        trigger_dag_id="ga-9-funnel-events-item",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "TEAMID": "'{{ params.TEAMID }}'",
            "ISDAILY": "{{ params.ISDAILY }}",
          

        },  # Se precisar passar informações adicionais para a DAG_B
    )
       


    pyhton_task >>  trigger_dag_ga_9_funnel_events_item 
    
    