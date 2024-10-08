import json
import os

from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models.param import Param
from airflow.operators.bash_operator import BashOperator

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




# Função para extrair dados do PostgreSQL e salvá-los como JSON
def extract_postgres_to_json(sql_script,file_name,pg_demo):
        #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
       
        import orjson
        try:
            
            
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            # Estabelecendo a conexão e criando um cursor
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(sql_script)

          
            records = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            
            # Transformando os dados em uma lista de dicionários (JSON-like)
            data = [dict(zip(colnames, row)) for row in records]
           
            # Convertendo os dados para JSON string
            #json_data = json.dumps(data, indent=4)
            json_data = orjson.dumps(data)
            # Convertendo bytes para string
            json_str = json_data.decode('utf-8')
            
            # Criando um diretório temporário para armazenar o arquivo JSON
           # tmp_dir = os.path.join(f"/tmp/{pg_schema}/" )  # Gera um diretório temporário único
            tmp_dir = os.path.join(f"/tmp/{pg_demo}/" )  # Gera um diretório temporário único
        
            os.makedirs(tmp_dir, exist_ok=True)  # Garante que o diretório exista
        
            output_filepath = os.path.join(tmp_dir, file_name)
            
            # Salvando o JSON string em um arquivo temporário
            with open(output_filepath, 'w') as outfile:
                outfile.write(json_str)

            wasb_hook = WasbHook(wasb_conn_id='azure_blob_storage_json')
            ###   Verifica se o arquivo já existe
            if wasb_hook.check_for_blob(container_name="jsondashboard", blob_name=f"{pg_demo}/{file_name}.json"):
                wasb_hook.delete_file(container_name="jsondashboard", blob_name=f"{pg_demo}/{file_name}.json")
                
            upload_task = LocalFilesystemToWasbOperator(
                task_id=f'upload_to_blob_grafico',
                file_path=output_filepath,  # O arquivo JSON gerado na tarefa anterior
                container_name='jsondashboard',  # Substitua pelo nome do seu container no Azure Blob Storage
            #  blob_name=directory_name + 'postgres_data.json',  # Nome do arquivo no Blob Storage dentro do diretório
                blob_name= f"{pg_demo}/{file_name}.json",
                wasb_conn_id='azure_blob_storage_json'
            )
            upload_task.execute(file_name)  # Executa a tarefa de upload

            return output_filepath

            
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during extract_postgres_to_json - {e}"
            )
            return e
        finally:
            # Fechando o cursor e a conexão
            cursor.close()
            conn.close()




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
    #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
    from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
    #

    sql_script = vtexsqlscriptscreatetabglobaldemo("{{ params.PGSCHEMACOPY }}")
    pg_demo = "demonstracao"

    
    install_library = BashOperator(
        task_id='install_library',
        bash_command='pip install orjson',
    )
    
    
    for indice, (chave, valor) in enumerate(sql_script.items(), start=1):
        # Tarefa para extrair dados do PostgreSQL e transformá-los em JSON
        extract_task = PythonOperator(
            task_id=f'extract_postgres_to_json_{chave}',
            python_callable=extract_postgres_to_json,
            op_args=[valor, chave,pg_demo]
            #provide_context=True
        )

        #inindo a ordem das tarefas no DAG
        install_library >> extract_task 