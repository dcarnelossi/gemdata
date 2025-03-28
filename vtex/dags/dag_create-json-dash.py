
import os

from airflow import DAG
from airflow.decorators import task
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

import uuid
from datetime import datetime
import logging

import subprocess
import sys

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


# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])




# Função para extrair dados do PostgreSQL e salvá-los como JSON
def extract_postgres_to_json(sql_script,file_name,pg_schema):
        #PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
        try:
            
            import json
        except ImportError:
            print("json não está instalado. Instalando agora...")
            install("json")
            import json

        try:
            import gzip
        except ImportError:
            print("gzip não está instalado. Instalando agora...")
            install("gzip")
            import gzip

        try:
            import msgpack
        except ImportError:
            print("msgpack não está instalado. Instalando agora...")
            install("msgpack")
            import msgpack


        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(sql_script)
            records = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]

            # Convertendo os dados para JSON
            data = [dict(zip(colnames, row)) for row in records]
            json_data = json.dumps(data)

            # Criando diretório temporário para armazenar os arquivos
            tmp_dir = os.path.join(f"/tmp/{pg_schema}/")
            os.makedirs(tmp_dir, exist_ok=True)

            # Caminhos para os arquivos JSON e Gzip
            json_filepath = os.path.join(tmp_dir, f"{file_name}.json")
            gzip_filepath = os.path.join(tmp_dir, f"{file_name}.msgpack.gz")

            # Salvando o arquivo JSON puro
            with open(json_filepath, 'w', encoding='utf-8') as json_file:
                json_file.write(json_data)

            # Criando arquivo MessagePack e compactando diretamente com Gzip
            with gzip.open(gzip_filepath, 'wb') as gzip_file:
                gzip_file.write(msgpack.packb(data, use_bin_type=True))

            # Upload para o Azure Blob Storage
            wasb_hook = WasbHook(wasb_conn_id='appgemdata-storage-prod')
            blob_name_json = f"{pg_schema}/{file_name}.json"
            blob_name_gzip = f"{pg_schema}/{file_name}.msgpack.gz"

            # Verifica se os arquivos já existem no Blob Storage e remove se necessário
            for blob_name in [blob_name_json, blob_name_gzip]:
                if wasb_hook.check_for_blob(container_name="jsondashboard-prod", blob_name=blob_name):
                    wasb_hook.delete_file(container_name="jsondashboard-prod", blob_name=blob_name)

            # Configurando tarefa de upload para JSON e Gzip
            upload_json = LocalFilesystemToWasbOperator(
                task_id='upload_json_to_blob',
                file_path=json_filepath,
                container_name='jsondashboard-prod',
                blob_name=blob_name_json,
                wasb_conn_id='appgemdata-storage-prod'
            )

            upload_gzip = LocalFilesystemToWasbOperator(
                task_id='upload_gzip_to_blob',
                file_path=gzip_filepath,
                container_name='jsondashboard-prod',
                blob_name=blob_name_gzip,
                wasb_conn_id='appgemdata-storage-prod'
            )

            # Executa os uploads
            upload_json.execute(file_name)
            upload_gzip.execute(file_name)

            return json_filepath, gzip_filepath

        except Exception as e:
            logging.exception(f"Erro ao processar extração do PostgreSQL: {e}")
            raise e
        finally:
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


# Função para extrair dados do PostgreSQL e salvá-los como JSON
def post_analytics_analytics(pg_schema):

       
    # if(not isdaily):
        try:    
           
 
            aba_dash= [('revenue','faturamento_canais.msgpack.gz','channels'),
                          ('revenue','faturamento_categorias.msgpack.gz','category'),
                          ('revenue','faturamento_ecommerce.msgpack.gz','revenue'),
                          ('revenue','faturamento_regiao.msgpack.gz','cities'),
                          ('revenue','faturamento_compradores.msgpack.gz','buyers'),
                          ('revenue','faturamento_mensal.msgpack.gz','revenuemensal'),
                          ('revenue','pedido_por_categoria.msgpack.gz','pcategory'),
                          ('revenue','pedido_por_estado.msgpack.gz','pcities'),
                          ('products','pedido_ecommerce.msgpack.gz','products'),
                          ('digest','faturamento_categorias.msgpack.gz','category'),
                          ('digest','faturamento_ecommerce.msgpack.gz','revenue'),
                          ('digest','faturamento_regiao.msgpack.gz','cities'),
                          ('digest','faturamento_compradores.msgpack.gz','buyers'),
                         ('purchases','compra_cliente.msgpack.gz','buyclient')

                          ]
            distinct_first_column = set(aba[0] for aba in aba_dash) 

            for aba in distinct_first_column:
                random_uuid = uuid.uuid4()
                query = """
                INSERT INTO analytics_analytics (id, name, is_active, integration_id)
                SELECT %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM analytics_analytics
                    WHERE name = %s AND integration_id = %s
                );
                """

                # Inicializa o PostgresHook
                hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                
                # Executa a query com os parâmetros
                hook2.run(query, parameters=(str(random_uuid), aba, True, pg_schema, aba, pg_schema))
        
        except Exception as e:
            logging.exception(
                f"erro ao inserir no analytics_analytics - {e}"
            )
            raise e

        try:      
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query2 = f"""         
                   select distinct name,id from analytics_analytics aa 
                    where integration_id = '{pg_schema}'
                """
            dados_integration = hook.get_records(query2)   

            
            for dados_analytics in dados_integration:
                for aba_file in aba_dash:
                    print(aba_file[0])
                    print(dados_analytics[0])
                    if aba_file[0] == dados_analytics[0]:
                        file_uuid = uuid.uuid4()
                        
                        # Query SQL com placeholders corretos
                        query3 = """
                        INSERT INTO analytics_analyticsfile (id, json_file, graph, analytics_id)
                        SELECT %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM analytics_analyticsfile
                            WHERE graph = %s AND analytics_id = %s
                        );
                        """

                        # Inicializa o PostgresHook
                        hook3 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                        
                        # Executa a query com os parâmetros
                        hook3.run(query3, parameters=(str(file_uuid), f"{pg_schema}/{aba_file[1]}",aba_file[2] , dados_analytics[1] ,aba_file[2] , dados_analytics[1]))



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


# Função para extrair dados do PostgreSQL e salvá-los como JSON
    @task(provide_context=True)
    def cadastro_analytics_analytics(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        #isdaily = kwargs["params"]["ISDAILY"]
       
        try:    
            post_analytics_analytics(PGSCHEMA)
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during extract_postgres_to_json - {e}"
            )
            raise e




    @task(provide_context=True)
    def select_parameter_file_json(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        try:
            
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")

            query_get_especific = f"""
            select parameter,file_name from public.integrations_parameter_filejson where id = '{PGSCHEMA}'
            limit 1; 		    
            """
            parameter_query = hook.get_records(query_get_especific)
            
            if( not parameter_query):
             
                query_get_default = f"""
                    select parameter,file_name from integrations_parameter_filejson where name = 'default'
                    limit 1; 
 		            """
                parameter_query = hook.get_records(query_get_default)

            logging.info(parameter_query[0])    
            return parameter_query[0]
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            raise e
        
    
    select_json=select_parameter_file_json()   

      



    # Task inicial para definir `previous_task`
    initial_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # Grupo de tarefas para extração de dados
    with TaskGroup("extract_tasks", dag=dag) as extract_tasks:
        previous_task = initial_task  # Define o DummyOperator como inicial

        # Definir tasks de extração dentro do loop
        for chave, valor in select_json.items():
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

    cad_analytics_analytics=cadastro_analytics_analytics()
    # Configurar que o trigger será executado após a atualização do log
    cad_analytics_analytics >>log_update_corp >> trigger_dag_disparo_email