import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    "b2-report-send-pdf",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
    render_template_as_native_obj=True,
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
                       
        )
       ,"SENDEMAIL": Param(
            type="boolean",
            title="SEND EMAIL:",
            description="Enter com False (processo whatsapp) ou True (processo email) .",
            section="Important params",
            min_length=1,
            max_length=10,
        ),
        "FILEPDF": Param(
            type="string",
            title="FILEPDF:",
            description="Enter the integration FILEPDF.",
            section="Important params",
            min_length=1,
            max_length=200,
                       
        )
        

    },
) as dag:


    @task(provide_context=True)
    def report_baixar_pdf(**kwargs):
        team_id = kwargs["params"]["PGSCHEMA"]
        caminho_pdf = kwargs["params"]["FILEPDF"]
        from modules import save_to_blob
        diretorio = f"/opt/airflow/temp/{caminho_pdf}"
        save_to_blob.ExecuteBlob().get_file("reportclient",f"{team_id}/{caminho_pdf}",f"{diretorio}") 
        return diretorio

    @task(provide_context=True)
    def report_baixar_email(**kwargs):
        team_id2 = kwargs["params"]["PGSCHEMA"]
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = f"""
 
            select distinct 
            
            us.username
           
            from integrations_integration ii 

            inner join public.teams_team te on 
            te.ID = ii.team_id

            inner join  public.teams_membership ms on 
            ms.team_id=  te.id
            
            inner join public.users_customuser us on 
            us.id = ms.user_id 

            where 
            ii .id = '{team_id2}'
            and 
            us.is_active is true
            and 
            ii.infra_create_status =  true 
            and 
            ii.is_active = true 
                """
        
            resultado_emails = hook.get_records(query)
      
            print(resultado_emails)
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")

    report_baixar_email()    
    