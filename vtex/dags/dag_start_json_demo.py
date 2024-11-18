
from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import logging

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
    "star-json-demo-dash",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["demojson", "v1", "vtex"],
     params={
        "PGSCHEMACOPY": Param(
            type="string",
            title="PGSCHEMA COPIAR:",
            description="Enter the integration PGSCHEMA quer quer copiar.",
            section="Important params",
            min_length=1,
            max_length=200,
        )
    },
) as dag:

    pg_demo = "demonstracao"

    @task(provide_context=True)
    def create_tabela_cliente_global(**kwargs):
        try:
            PGSCHEMACOP = kwargs["params"]["PGSCHEMACOPY"]    
            from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemocopy
            # Defina o código SQL para criar a tabela
            sql_script = vtexsqlscriptscreatetabglobaldemocopy(PGSCHEMACOP,pg_demo)

            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="integrations-data-prod")
            hook.run(sql_script)
            
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            return e

    trigger_dag_create_json = TriggerDagRunOperator(
        task_id="trigger_dag_create_json_dash",
        trigger_dag_id="a10-create-json-dash",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": pg_demo

        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    create_tab_global_task = create_tabela_cliente_global()

    create_tab_global_task >> trigger_dag_create_json
     
