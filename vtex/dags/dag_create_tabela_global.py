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
    def create_tabela_cliente_global(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = f"""
            select hosting from public.integrations_integration where id = '{PGSCHEMA}' 
 		    """
            hosting = hook.get_records(query)

        
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            raise e
    
           

           # return [integration[0] for integration in integration_ids]



        try:
            if hosting[0]== "vtex": 
                #essa parte é do vtex
                #esse schema usaremos para demo, para fazer videos e etc ..     
                #copiando do schema 2dd03
                if(PGSCHEMA == "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
                    from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
                    sql_script = vtexsqlscriptscreatetabglobaldemo("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")
                    
                else: # Defina o código SQL para criar a tabela
                    from modules.sqlscriptabglobal import vtexsqlscriptscreatetabglobal
                    sql_script = vtexsqlscriptscreatetabglobal(PGSCHEMA)    
                # Conecte-se ao PostgreSQL e execute o script
                # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
                # não pode estar cravada aqui no codigo
                hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                hook.run(sql_script)
                
                return True

            else:
                #essa parte é do shopify
                #copiando do schema 2dd03- probel 
                if(PGSCHEMA == "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
                    from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
                    sql_script = vtexsqlscriptscreatetabglobaldemo("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")
                    
                else: # Defina o código SQL para criar a tabela
                    from modules.sqlscriptabglobal import shopifysqlscriptscreatetabglobal
                    sql_script = shopifysqlscriptscreatetabglobal(PGSCHEMA)    
                # Conecte-se ao PostgreSQL e execute o script
                # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
                # não pode estar cravada aqui no codigo
                hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
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
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY":"{{ params.ISDAILY }}"
           
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    try:
        create_tab_global_task = create_tabela_cliente_global()


        create_tab_global_task >>   trigger_dag_create_json
    
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow