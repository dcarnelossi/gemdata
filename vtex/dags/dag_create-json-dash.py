
import os

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
# Importação dos módulos deve ser feita fora do contexto do DAG
from modules.sqlscriptsjson import vtexsqlscriptjson


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
       
        import orjson
        try:
            
            
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
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
            tmp_dir = os.path.join(f"/tmp/{pg_schema}/" )  # Gera um diretório temporário único
        
            os.makedirs(tmp_dir, exist_ok=True)  # Garante que o diretório exista
        
            output_filepath = os.path.join(tmp_dir, file_name)
            
            # Salvando o JSON string em um arquivo temporário
            with open(output_filepath, 'w') as outfile:
                outfile.write(json_str)

            wasb_hook = WasbHook(wasb_conn_id='appgemdata-storage-prod')
            ###   Verifica se o arquivo já existe
            if wasb_hook.check_for_blob(container_name="jsondashboard-prod", blob_name=f"{pg_schema}/{file_name}.json"):
                wasb_hook.delete_file(container_name="jsondashboard-prod", blob_name=f"{pg_schema}/{file_name}.json")
                
            upload_task = LocalFilesystemToWasbOperator(
                task_id=f'upload_to_blob_grafico',
                file_path=output_filepath,  # O arquivo JSON gerado na tarefa anterior
                container_name='jsondashboard-prod',  # Substitua pelo nome do seu container no Azure Blob Storage
            #  blob_name=directory_name + 'postgres_data.json',  # Nome do arquivo no Blob Storage dentro do diretório
                blob_name= f"{pg_schema}/{file_name}.json",
                wasb_conn_id='appgemdata-storage-prod'
            )
            upload_task.execute(file_name)  # Executa a tarefa de upload

            return output_filepath

            
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during extract_postgres_to_json - {e}"
            )
            raise e
        finally:
            # Fechando o cursor e a conexão
            cursor.close()
            conn.close()


# Função para extrair dados do PostgreSQL e salvá-los como JSON
def daily_run_date_update(pg_schema):
        #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
       

        try:

            if(pg_schema !="demonstracao"):
                    
                query = """
                UPDATE public.integrations_integration
                SET daily_run_date_end = %s,isdaily_manual = false  
                WHERE id = %s;
                """
                # Initialize the PostgresHook
                hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                # Execute the query with parameters
                
                hook2.run(query, parameters=(datetime.now(),pg_schema))
            
            else:
                print("arquivos demonstracao atualizados")
                return True
            
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during extract_postgres_to_json - {e}"
            )
            raise e
       


# Função para mover o arquivo JSON para o diretório no Blob Storage
def upload_to_blob_directory(file_name,pg_schema):
    try: 
        wasb_hook = WasbHook(wasb_conn_id='appgemdata-storage-prod')
        blob_name=f"{pg_schema}/{file_name}.json" 
        output_filepath = f"/tmp/{blob_name}"

        ###   Verifica se o arquivo já existe
        if wasb_hook.check_for_blob(container_name="jsondashboard-prod", blob_name=blob_name):
            wasb_hook.delete_file(container_name="jsondashboard-prod", blob_name=blob_name)
        #print(f"testando::: {output_filepath}")
        upload_task = LocalFilesystemToWasbOperator(
            task_id=f'upload_to_blob_grafico',
            file_path=output_filepath,  # O arquivo JSON gerado na tarefa anterior
            container_name='jsondashboard-prod',  # Substitua pelo nome do seu container no Azure Blob Storage
        #  blob_name=directory_name + 'postgres_data.json',  # Nome do arquivo no Blob Storage dentro do diretório
            blob_name= blob_name,
            wasb_conn_id='appgemdata-storage-prod'
        )
        upload_task.execute(file_name)  # Executa a tarefa de upload

    except Exception as e:
            logging.exception(
                f"An unexpected error occurred during extract_postgres_to_json - {e}"
            )
            raise e




# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "a10-create-json-dash",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["jsonblob", "v1", "vtex"],
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

    # Carregar o script SQL usando o módulo importado
    try:
        sql_script = vtexsqlscriptjson("{{ params.PGSCHEMA }}")
        print ("{{ params.PGSCHEMA }}")
    except Exception as e:
        logging.error(f"Erro ao carregar o script SQL: {e}")
        raise

    # Task inicial para definir `previous_task`
    initial_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # Grupo de tarefas para extração de dados
    with TaskGroup("extract_tasks", dag=dag) as extract_tasks:
        previous_task = initial_task  # Define o DummyOperator como inicial

        # Definir tasks de extração dentro do loop
        for chave, valor in sql_script.items():
            extract_task = PythonOperator(
                task_id=f'extract_postgres_to_json_{chave}',
                python_callable=extract_postgres_to_json,
                op_args=[valor, chave, "{{ params.PGSCHEMA }}"],
                dag=dag
            )

            # Define dependência entre as tasks
            previous_task >> extract_task
            previous_task = extract_task  # Atualiza `previous_task` para a próxima iteração

    # Task de sincronização que será executada após todas as extrações
    sync_tasks = DummyOperator(
        task_id='sync_all_extractions',
        dag=dag
    )

    # Configurar que a task de sincronização deve ser executada após todas as extrações
    extract_tasks >> sync_tasks

    # Task para atualizar a data de execução do log
    log_update_corp = PythonOperator(
        task_id='log_daily_run_data_update',
        python_callable=daily_run_date_update,
        op_args=["{{ params.PGSCHEMA }}"],
        dag=dag
    )

    # Garantir que a atualização de log será executada após todas as extrações
    sync_tasks >> log_update_corp

    # Task para acionar o próximo DAG se ISDAILY for False
    trigger_dag_disparo_email = TriggerDagRunOperator(
        task_id="trigger_dag_email",
        trigger_dag_id="a11-send-email-firstprocess",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}"
        },
        dag=dag
    )

    # Configurar que o trigger será executado após a atualização do log
    log_update_corp >> trigger_dag_disparo_email