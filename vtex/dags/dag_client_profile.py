import logging
import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
        )
    },
) as dag:
    
    @task(provide_context=True)
    def log_import_resumo(reportid=None,**kwargs):
        try: 
            
            integration_id = kwargs["params"]["PGSCHEMA"]
            dag_run_id = kwargs['dag_run'].run_id  
            
           

            if reportid:
                report_id = reportid
                dag_finished_at = datetime.now()
                dag_last_status = "SUCESSO"
                    
                data = {
                    'id':report_id ,
                    'integration_id': integration_id,
                    'dag_run_id': dag_run_id,
                    'dag_finished_at': dag_finished_at,
                    'dag_last_status': dag_last_status   
                }
                 
            else:
                import uuid 
                report_id= kwargs["params"].get("IDREPORT")
                print(kwargs["params"].get("IDREPORT"))
                if not report_id:
                    report_id =  str(uuid.uuid4())

                
                dataini = datetime.now()
                dag_last_status = "EXECUTANDO"   
                isdaily = kwargs["params"]["ISDAILY"]
                dag_name = kwargs['dag'].dag_id
                if isdaily:
                    nameprocess = "PROCESSO DIARIO"
                else:    
                    nameprocess = "PROCESSO HISTORICO"
    
                data = {
                    'id':report_id ,
                    'integration_id': integration_id,
                    'nameprocess': nameprocess,
                    'dag': dag_name,
                    'dag_run_id': dag_run_id,
                    'dag_started_at': dataini,
                    'dag_last_status': dag_last_status
                    
                }


            
            coorp_conection_info = get_coorp_conection_info()
            from modules import log_resumo_airflow
            log_resumo_airflow.log_process(coorp_conection_info , data )

            logging.info(f"upserted do log diario successfully.")

            return report_id
        except Exception as e:
            logging.error(f"Error inserting log diario: {e}")
            raise e  # Ensure failure is propagated to Airflow
        
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
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")

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
    
    logini=log_import_resumo()   
    

    trigger_dag_create_tab = TriggerDagRunOperator(
        task_id="trigger_dag_create_tab_global",
        trigger_dag_id="9-create-table-client",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
             "IDREPORT": logini,
        },  # Se precisar passar informações adicionais para a DAG_B
    )

     # Configurando a dependência entre as tasks

    client_profile_task = client_profile()
    logfim=log_import_resumo(logini)
    logini >> client_profile_task >> logfim >> trigger_dag_create_tab
