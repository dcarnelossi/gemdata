import json
import os

from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models.param import Param
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
def extract_postgres_to_json(sql_script,file_name,pg_schema):
        #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
       

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
            print(data)
            # Convertendo os dados para JSON string
            json_data = json.dumps(data, indent=4)
            
            # Criando um diretório temporário para armazenar o arquivo JSON
           # tmp_dir = os.path.join(f"/tmp/{pg_schema}/" )  # Gera um diretório temporário único
            tmp_dir = os.path.join(f"/tmp/{pg_schema}/" )  # Gera um diretório temporário único
        
            os.makedirs(tmp_dir, exist_ok=True)  # Garante que o diretório exista
        
            output_filepath = os.path.join(tmp_dir, file_name)
            
            # Salvando o JSON string em um arquivo temporário
            with open(output_filepath, 'w') as outfile:
                outfile.write(json_data)

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


# Função para mover o arquivo JSON para o diretório no Blob Storage
def upload_to_blob_directory(file_name,pg_schema):
    
    wasb_hook = WasbHook(wasb_conn_id='azure_blob_storage_json')
    blob_name=f"{pg_schema}/{file_name}.json" 
    output_filepath = f"/tmp/{blob_name}"

     ###   Verifica se o arquivo já existe
    if wasb_hook.check_for_blob(container_name="jsondashboard", blob_name=blob_name):
        wasb_hook.delete_file(container_name="jsondashboard", blob_name=blob_name)
    #print(f"testando::: {output_filepath}")
    upload_task = LocalFilesystemToWasbOperator(
        task_id=f'upload_to_blob_grafico',
        file_path="./a5be7ce1-ce65-46f8-a293-4efff72819ce/"+ str(file_name)+".json",  # O arquivo JSON gerado na tarefa anterior
        container_name='jsondashboard',  # Substitua pelo nome do seu container no Azure Blob Storage
      #  blob_name=directory_name + 'postgres_data.json',  # Nome do arquivo no Blob Storage dentro do diretório
        blob_name= blob_name,
        wasb_conn_id='azure_blob_storage_json'
    )
    upload_task.execute(file_name)  # Executa a tarefa de upload






# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "10-create-json-dash",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["jsonblob", "v1", "ALTERAR"],
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
    #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
    from modules.sqlscriptsjson import vtexsqlscriptjson


    sql_script = vtexsqlscriptjson("{{ params.PGSCHEMA }}")

        
    
    for indice, (chave, valor) in enumerate(sql_script.items(), start=1):
        # Tarefa para extrair dados do PostgreSQL e transformá-los em JSON
        extract_task = PythonOperator(
            task_id=f'extract_postgres_to_json_{chave}',
            python_callable=extract_postgres_to_json,
            op_args=[valor, chave, "{{ params.PGSCHEMA }}"]
            #provide_context=True
        )

        # Tarefa para verificar/criar o diretório no Azure Blob Storage e fazer o upload do arquivo JSON
        upload_task = PythonOperator(
            task_id=f'upload_to_blob_directory_{chave}',
            python_callable=upload_to_blob_directory,
            op_kwargs={'file_name': chave, 'pg_schema': "{{ params.PGSCHEMA }}"},
            provide_context=True
        )

        # Definindo a ordem das tarefas no DAG
        extract_task >> upload_task
