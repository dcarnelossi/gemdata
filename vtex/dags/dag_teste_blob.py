import logging
from datetime import datetime
import json

import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from datetime import datetime

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


   
def extract_postgres_to_json(**kwargs):
        PGSCHEMA= "a5be7ce1-ce65-46f8-a293-4efff72819ce"
        #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
        
        
        from modules.sqlscriptsjson import vtexsqlscriptjson

        try:
            
            
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            sql_script = vtexsqlscriptjson(PGSCHEMA)
            records  = hook.get_records(sql_script)
           
                # Obtendo os nomes das colunas
            colnames = [desc[0] for desc in hook.get_cursor().description]
            
            # Transformando os dados em uma lista de dicionários (JSON-like)
            data = [dict(zip(colnames, row)) for row in records]

            # Convertendo os dados para JSON string
            json_data = json.dumps(data, indent=4)

            # Salvando o JSON string em um arquivo temporário
            output_filepath = '/tmp/{PGSCHEMA}/postgres_data.json'
            print("ver aqui:{output_filepath}" )
            with open(output_filepath, 'w') as outfile:
                outfile.write(json_data)
            
            return output_filepath
            
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during orders_update_postgres - {e}"
            )
            return e

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-testeblob",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Update", "v1", "ALTERAR"],

) as dag:
   
  # Tarefa para extrair dados do PostgreSQL e transformá-los em JSON
    extract_task = PythonOperator(
        task_id='extract_postgres_to_json',
        python_callable=extract_postgres_to_json
    )
    
     # Tarefa para enviar o arquivo JSON para o Azure Blob Storage
    upload_task = LocalFilesystemToWasbOperator(
        task_id='upload_to_blob',
        file_path='/tmp/{PGSCHEMA}/postgres_data.json',  # O arquivo JSON gerado na tarefa anterior
        container_name='nome-do-container',  # Substitua pelo nome do seu container no Azure Blob Storage
        blob_name='postgres_data.json',  # Nome do arquivo no Blob Storage
        wasb_conn_id='azure_blob_storage_json'  # ID da conexão configurada no Airflow
    )

    # Definindo a ordem das tarefas no DAG
    extract_task >> upload_task