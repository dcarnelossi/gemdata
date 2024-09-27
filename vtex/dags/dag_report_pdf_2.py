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
    "b1-report-pdf2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
                       
        )
        ,"TYPREREPORT": Param(
            type="string",
            title="Tipo de relatorio:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
            enum=["1_relatorio_mensal", "2_relatorio_semanal","3_relatorio_personalizado"],  # Opções para o dropdown
     #       default=None,  # Valor padrão selecionado
        )
        ,"CELULAR": Param(
            type="string",
            title="CELULAR:",
            description="Enter the celphone.",
            section="Important params",
            min_length=1,
            max_length=13,
            default="5511999999999",  # Define como None por padrão
          
        )
        ,"DATAINI": Param(
            type="string",
            title="Data inicio:",
            description="Enter the start date  (ex:2024-10-01).",
            section="Important params",
         )
        ,"DATAFIM": Param(
            type="string",
            title="Data fim:",
            description="Enter the end date (ex:2024-10-01).",
            section="Important params",
         )
        ,"SENDEMAIL": Param(
            type="boolean",
            title="ISDAILY:",
            description="Enter com False (processo whatsapp) ou True (processo email) .",
            section="Important params",
            min_length=1,
            max_length=10,
        )
        

    },
) as dag:



    @task(provide_context=True)
    def report_mensal(**kwargs):
        try:

            team_id = kwargs["params"]["PGSCHEMA"]
            tiporela = kwargs["params"]["TYPREREPORT"]
            celphone = kwargs["params"]["CELULAR"]
            data_ini = datetime.strptime(kwargs["params"]["DATAINI"],"%Y-%m-%d")
            data_fim = datetime.strptime(kwargs["params"]["DATAFIM"],"%Y-%m-%d")
            isemail = kwargs["params"]["SENDEMAIL"]

            data_conection_info = get_data_conection_info(team_id)
        except Exception as e:
            logging.exception(f"erro nos paramentos - {e}")
        
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = f"""
                select distinct 
                te.logo as team_logo
                from integrations_integration ii 
                inner join public.teams_team te on 
                te.ID = ii.team_id
                where 
                ii.id = '{team_id}'
                and 
                ii.infra_create_status =  true 
                and 
                ii.is_active = true 
                """
        
            caminho_logo = hook.get_records(query)
          
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
            

        print(caminho_logo)
        

        # Lógica condicional com base na escolha do usuário
        if tiporela == "1_relatorio_mensal":
            from modules import report_month
            mes = data_ini.month    
            try:
                print("Processando o Relatório mensal...")
                report_month.set_globals(
                data_conection_info,
                team_id,
                celphone,
                mes,
                caminho_logo
                )
                print("Relatório mensal processado...")
                return True
            except Exception as e:
                logging.exception(f"An unexpected error occurred during DAG - {e}")
                raise


            
            # Coloque a lógica do relatório semanal aqui
        elif tiporela == "2_relatorio_semanal":
            print("Processando o Relatório Diário...")
            
            
        elif tiporela == "3_relatorio_personalizado":
            print("Processando o Relatório Diário...")
            # Coloque a lógica do relatório diário aqui
                
        else:
            print("Opção de relatório desconhecida.")

     


    # install_libraries >> 
    report_mensal()
