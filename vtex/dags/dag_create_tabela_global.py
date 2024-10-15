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
    "9-create-table-client",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Createtabcliente", "v2", "trigger_dag_imports"],
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
       
    },
) as dag:

    @task(provide_context=True)
    def create_tabela_cliente_global(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]

        

        try:
      
            #esse schema usaremos para demo, para fazer videos e etc ..     
            #copiando do schema 2dd03
            if(PGSCHEMA == "d95047ae-3802-4858-bad2-f2e0ffd486d4"):
                from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
                sql_script = vtexsqlscriptscreatetabglobaldemo("2dd03eaf-cf56-4a5b-bc99-3a06b237ded8",PGSCHEMA)
                
            else: # Defina o código SQL para criar a tabela
                from modules.sqlscriptabglobal import vtexsqlscriptscreatetabglobal
                sql_script = vtexsqlscriptscreatetabglobal(PGSCHEMA)    
            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            hook.run(sql_script)
            
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            raise e
            
    trigger_dag_create_json = TriggerDagRunOperator(
        task_id="trigger_dag_create_json_dash",
        trigger_dag_id="a10-create-json-dash",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}"

        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    create_tab_global_task = create_tabela_cliente_global()

    create_tab_global_task >> trigger_dag_create_json
