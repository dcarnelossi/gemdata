import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-StartDaily",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
#    render_template_as_native_obj=True,
    # params={
    #     "PGSCHEMA": Param(
    #         type="string",
    #         title="PGSCHEMA:",
    #         description="Enter the integration PGSCHEMA.",
    #         section="Important params",
    #         min_length=1,
    #         max_length=200,
    #     ),
    #     "ISDAILY": Param(
    #         type="boolean",
    #         title="ISDAILY:",
    #         description="Enter com False (processo total) ou True (processo diario) .",
    #         section="Important params",
    #         min_length=1,
    #         max_length=10,
    #     )
    # },
) as dag:

    @task(provide_context=True)
    def start_daily_update(**kwargs):
        try:
            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = """
            select distinct id from public.integrations_integration
            where is_active = true 
            and 
            infra_create_status = true limit 1
            """

            integrationid=hook.get_records(query)
            integration_id = integrationid[0][0]  # Pega o primeiro resultado da consulta
            print(f"Integration ID: {integration_id}")


            
            for integration in  integrationid:  
                TriggerDagRunOperator(
                task_id="trigger_dag_imports",
                trigger_dag_id="1-ImportVtex-Brands-Categories-Skus-Products",  # Substitua pelo nome real da sua segunda DAG
                conf={
                    "PGSCHEMA": integration[0],
                    "ISDAILY": "{False}"
                        
                },  # Se precisar passar informações adicionais para a DAG_B
                )
        

            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_postgres_infra - {e}"
            )
            return e

    # Configurando a dependência entre as tarefas

    create_postgres_infra_task = start_daily_update()


