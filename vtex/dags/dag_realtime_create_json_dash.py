
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator

import os
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
def extract_postgres_to_json(param_dict,pg_schema):

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

        for file_name, sql_script in param_dict.items():
            try:
            

                logging.info(f"""*****Iniciado o processamento json: {pg_schema}-{file_name}""")
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
                logging.info(f"""******finalizado o processamento json: {pg_schema}-{file_name}""")
                
                cursor.close()
                conn.close()
              
            except Exception as e:
                logging.exception(f"Erro ao processar extração do PostgreSQL( {pg_schema}-{file_name}): {e}")
                raise e
            finally:
                cursor.close()
                conn.close()


with DAG(
    "RT-2-create-json-dash",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["shopify", "orders", "realtime"],
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
        "HOSTING": Param(
            type="string",
            title="INTEGRACAO:",
            description="Entrar com o tipo de plataforma de ecommerce(integrations_integration:shopify,nuvem_shop,vtex,moovin,lintegrada).",
            section="Important params",
            min_length=1,
            max_length=200,

        ),
    },
) as dag:
    start = DummyOperator(task_id='start')

    @task()
    def create_realtime_orders_lastyear_default(**kwargs):
        schema = kwargs["params"]["PGSCHEMA"]
        try:      
            # Conecte-se ao PostgreSQL e execute o script
              
                # Query SQL com placeholders corretos
                query_default_orders_lastyear = f"""
                    DROP TABLE IF EXISTS "{schema}".realtime_orders_lastyear;
                    CREATE TABLE "{schema}".realtime_orders_lastyear
                    as
                    select 
                    to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') AS dt, 
                    EXTRACT(YEAR FROM creationdate)::int                    AS ano,
                    EXTRACT(HOUR FROM creationdate)::int                    AS hora,
                    orderid,
                    statusdescription,
                    quantityorder,
                    revenue,
                    revenue_without_shipping,
                    selectedaddresses_0_city,
                    selectedaddresses_0_state,
                    selectedaddresses_0_country,
                    FreeShipping 
                    
                    from "{schema}".orders_ia  
                    where to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') = to_char(date_trunc('day',(CURRENT_DATE- INTERVAL '1 year') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD')

                """

                # Inicializa o PostgresHook
                hook1 = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                
                # Executa a query com os parâmetros
                hook1.run(query_default_orders_lastyear)



        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    
    @task()
    def create_realtime_forecast_hour_default(**kwargs):
        schema = kwargs["params"]["PGSCHEMA"]
        try:      
            # Conecte-se ao PostgreSQL e execute o script
           
                # Query SQL com placeholders corretos
                query_default_forecast_hour = f"""
                    DROP TABLE IF EXISTS "{schema}".realtime_forecast_hour;
                    CREATE TABLE "{schema}".realtime_forecast_hour AS
                    WITH share_hour AS (
                        SELECT
                            EXTRACT(HOUR FROM creationdate)::int AS hora,
                            SUM(revenue) / SUM(SUM(revenue)) OVER () AS share_revenue
                        FROM "{schema}".orders_ia
                        WHERE date_trunc('day', creationdate) >= date_trunc('day', (CURRENT_DATE - INTERVAL '3 month'))
                        AND date_trunc('day', creationdate) < date_trunc('day', (CURRENT_DATE - INTERVAL '1 days'))
                        AND EXTRACT(DOW FROM creationdate) = EXTRACT(DOW FROM (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date)
                        GROUP BY EXTRACT(HOUR FROM creationdate)::int
                    )
                    SELECT
                        to_char(date_trunc('day', creationdateforecast), 'YYYY-MM-DD') AS creationdateforecast,
                        sh.hora::int,
                        CEIL(CAST(ff.predicted_revenue * sh.share_revenue AS float)) AS predicted_revenue_hour,
                        CEIL(CAST((ff.predicted_revenue * sh.share_revenue)*1.15 AS float)) AS max_predicted_revenue_hour,
                        CEIL(CAST((ff.predicted_revenue * sh.share_revenue)*0.85 AS float)) AS min_predicted_revenue_hour,
                        CAST(ff.predicted_revenue AS FLOAT) AS predicted_revenue_day
                    FROM share_hour sh
                    CROSS JOIN "{schema}".orders_ia_forecast ff
                    WHERE to_char(date_trunc('day', creationdateforecast), 'YYYY-MM-DD') = 
                        to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date , 'YYYY-MM-DD');


                    """
           

                # Inicializa o PostgresHook
                hook2 = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                
                # Executa a query com os parâmetros
                hook2.run(query_default_forecast_hour)



        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    
    @task()
    def create_realtime_orders(**kwargs):
        schema = kwargs["params"]["PGSCHEMA"]
        plataforma = kwargs["params"]["HOSTING"]
        try:      
            # Conecte-se ao PostgreSQL e execute o script
                if (plataforma=='vtex'):
                    
                    query= f"""    
                         DROP TABLE IF EXISTS "{schema}".realtime_orders_ia;
                         CREATE TABLE "{schema}".realtime_orders_ia AS
                    WITH address AS (
                        select
                        orderid,
                        (shippingdata::jsonb -> 'address' ->> 'postalCode') AS selectedaddresses_0_postalcode,
                        (shippingdata::jsonb -> 'address' ->> 'city') AS selectedaddresses_0_city,
                        (shippingdata::jsonb -> 'address' ->> 'state') AS selectedaddresses_0_state,
                        (shippingdata::jsonb -> 'address' ->> 'country') AS address_country
                        FROM "{schema}".realtime_vtex_orders
                        WHERE (shippingdata::jsonb ->> 'id') = 'shippingData'
                        ), orders_total as 
                        (
                        select
                        orderid,
                        elem ->> 'value' AS shipping
                        FROM "{schema}".realtime_vtex_orders,
                            jsonb_array_elements(totals::jsonb) AS elem
                        WHERE elem ->> 'id' = 'Shipping'
                        ), realtime_orders 
                        as ( 
                        select 


                            to_char(date_trunc('hour',o.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                            , EXTRACT(YEAR FROM date_trunc('hour',o.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
                            , EXTRACT(HOUR FROM date_trunc('hour',o.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                            ,o.orderid as orderid
                            ,LOWER(o.statusdescription) as statusdescription
                            ,cast(1 as float) as quantityorder
                            ,cast(o.value as float)/100  as revenue
                            ,(cast(o.value as float)/100)-(cast(ot.shipping as float)/100)   as revenue_without_shipping
                            ,LOWER(coalesce(nome_codigo_municipio_completo,sd.selectedaddresses_0_city)) as selectedaddresses_0_city
                            ,LOWER(coalesce(abreviado_uf,sd.selectedaddresses_0_state)) as selectedaddresses_0_state
                            ,LOWER(sd.address_country) as selectedaddresses_0_country
                            ,case when cast(ot.shipping as numeric) =0 then  'Sem Frete' else  'Com Frete' end   FreeShipping 
                                        
                        from "{schema}".realtime_vtex_orders o 
                        left join address sd on 
                        sd.orderid = o.orderid
                        left join orders_total ot on 
                        ot.orderid = o.orderid
                        left join public.cep_brasil_consolidado ce on 
                        ce.cep = cast(NULLIF(left(REPLACE(sd.selectedaddresses_0_postalcode,'-',''),5), '') as int)
                        where  LOWER(o.statusdescription)  in   ('faturado','pronto para o manuseio','preparando entrega')
                        ), final_hora 
                        as ( 
                        select
                            *
                        from realtime_orders 

                        union all 

                        select * from "{schema}".realtime_orders_lastyear
                        )
                            SELECT *,
                                to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'), 'DD/MM/YYYY HH24:MI') AS hora_atualizacao
                            FROM final_hora;
                        """
                elif(plataforma=='shopify'):  
                     
                    # Query SQL com placeholders corretos
                    query = f"""
                                DROP TABLE IF EXISTS "{schema}".realtime_orders_ia;
                                
                                CREATE TABLE "{schema}".realtime_orders_ia AS
                                WITH realtime_orders AS (
                                select 
                                to_char(date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                                , EXTRACT(YEAR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
                                , EXTRACT(HOUR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                                ,o.orderid as orderid
                                ,LOWER(o.displayfinancialstatus) as statusdescription
                                ,cast(1 as float) as quantityorder
                                ,cast(o.totalprice as float)  as revenue
                                ,(cast(o.totalprice as float))-(cast(o.totalshippingprice as float))   as revenue_without_shipping
                                ,LOWER(coalesce(nome_codigo_municipio_completo,coalesce(o.shippingcity,o.billingcity))) as selectedaddresses_0_city
                                ,LOWER(coalesce(abreviado_uf, coalesce(o.shippingprovincecode,o.billingprovincecode))) as selectedaddresses_0_state
                                ,LOWER(coalesce(o.shippingcountrycode,o.billingcountrycode)) as selectedaddresses_0_country
                                ,case when cast(o.totalshippingprice as numeric) =0 then  'Sem Frete' else  'Com Frete' end  FreeShipping 
                                
                                from "{schema}".realtime_shopify_orders o               
                                left join public.cep_brasil_consolidado ce on 
                                ce.cep = cast(NULLIF(left(coalesce(REPLACE(o.shippingzip,'-',''),REPLACE(o.billingzip,'-','')),5), '') as int)   
                                
                                where 
                                to_char(date_trunc('day',o.createdat AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') 
                                and 
                                LOWER(o.displayfinancialstatus)  in  ('paid') and o.cancelledat is null
                                ), final_hora as ( 
                                select
                                    *
                                from realtime_orders 
                                
                                union all 
                                
                                select * from "{schema}".realtime_orders_lastyear
                                )
                                    SELECT *,
                                        to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'), 'DD/MM/YYYY HH24:MI') AS hora_atualizacao
                                    FROM final_hora;
                                
                        """
            
                     
                elif(plataforma=='nuvem_shop'):    
                       query = f"""
                                DROP TABLE IF EXISTS "{schema}".realtime_orders_ia;
                                
                                CREATE TABLE "{schema}".realtime_orders_ia AS
                                WITH realtime_orders AS (
                                select 
                                to_char(date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                                , EXTRACT(YEAR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
                                , EXTRACT(HOUR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                                ,o.order_id as orderid
                                ,LOWER(o.status) as statusdescription
                                ,cast(1 as float) as quantityorder
                                ,cast(o.total_value as float)   as revenue
                                ,(cast(o.total_value as float))-(cast(o.shipping_cost_customer as float))  as revenue_without_shipping
                                ,LOWER(coalesce(nome_codigo_municipio_completo,o.billing_city)) as selectedaddresses_0_city
                                ,LOWER(coalesce(abreviado_uf, o.billing_state)) as selectedaddresses_0_state
                                ,LOWER('BR') as selectedaddresses_0_country
                                ,case when cast(o.shipping_cost_customer as numeric) =0 then  'Sem Frete' else  'Com Frete' end as FreeShipping 

                                from "{schema}".realtime_nuvem_orders o               
                                left join public.cep_brasil_consolidado ce on 
                                    ce.cep = CAST(
                                                NULLIF(
                                                    LEFT(REGEXP_REPLACE(o.billing_zipcode, '[^0-9]', '', 'g'), 5)
                                                , '') AS INT)    

                                where 
                                to_char(date_trunc('day',o.created_at AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') 
                                and 
                                LOWER(o.status)  in  ('paid') 
                                ), final_hora 
                                as ( 
                                select
                                    *
                                from realtime_orders 

                                union all 

                                select * from "{schema}".realtime_orders_lastyear
                                )
                                    SELECT *,
                                        to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'), 'DD/MM/YYYY HH24:MI') AS hora_atualizacao
                                    FROM final_hora;
                    """
                      
                elif(plataforma=='moovin'):  
                     
                     query = f"""
                                DROP TABLE IF EXISTS "{schema}".realtime_orders_ia;
                                
                                CREATE TABLE "{schema}".realtime_orders_ia AS
                                WITH realtime_orders AS (
                 			    select 
                                to_char(date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                                , EXTRACT(YEAR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
                                , EXTRACT(HOUR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                                ,o.order_number as orderid
                                ,LOWER(o.status) as statusdescription
                                ,cast(1 as float) as quantityorder
                                ,cast(o.total_value as float)   as revenue
                                ,(cast(o.total_value as float))-(cast(o.shipping_value as float))  as revenue_without_shipping
                                ,LOWER(coalesce(nome_codigo_municipio_completo,o.delivery_city)) as selectedaddresses_0_city
                                ,LOWER(coalesce(abreviado_uf, o.delivery_state)) as selectedaddresses_0_state
                                ,LOWER('BR') as selectedaddresses_0_country
                                ,case when cast(o.shipping_value as numeric) =0 then  'Sem Frete' else  'Com Frete' end  as FreeShipping 

                                from "{schema}".realtime_moovin_orders o               
                                left join public.cep_brasil_consolidado ce on 
                					ce.cep = cast(NULLIF(left(coalesce(REPLACE(o.delivery_zipcode,'-',''),REPLACE(o.delivery_zipcode,'-','')),5), '') as int)

                                where 
                                to_char(date_trunc('day',o.created_at AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') 
                                and 
                                 LOWER(o.status)  in  ('approved','pending')
                                 ), final_hora 
                                as ( 
                                select
                                    *
                                from realtime_orders 

                                union all 

                                select * from "{schema}".realtime_orders_lastyear
                                )
                                    SELECT *,
                                        to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'), 'DD/MM/YYYY HH24:MI') AS hora_atualizacao
                                    FROM final_hora;
                                """


                elif(plataforma=='lintegrada'):
                        query = f"""
                                DROP TABLE IF EXISTS "{schema}".realtime_orders_ia;
                                
                                CREATE TABLE "{schema}".realtime_orders_ia AS
                                WITH realtime_orders AS (
                                select 
                                    to_char(date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                                    , EXTRACT(YEAR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
                                    , EXTRACT(HOUR FROM date_trunc('hour',o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                                    ,o.order_number as orderid
                                    ,LOWER(o.status_name) as statusdescription
                                    ,cast(1 as float) as quantityorder
                                    ,cast(o.total_value as float)   as revenue
                                    ,(cast(o.total_value as float))-(cast(o.shipping_value as float))  as revenue_without_shipping
                                    ,LOWER(coalesce(nome_codigo_municipio_completo,o.delivery_city)) as selectedaddresses_0_city
                                    ,LOWER(coalesce(abreviado_uf, o.delivery_state)) as selectedaddresses_0_state
                                    ,LOWER('BR') as selectedaddresses_0_country
                                    ,case when cast(o.shipping_value as numeric) =0 then  'Sem Frete' else  'Com Frete' end  as FreeShipping 
                                    
                                    from "{schema}".realtime_lojaintegrada_orders o               
                                    left join public.cep_brasil_consolidado ce on 
                                                ce.cep = cast(NULLIF(left(coalesce(REPLACE(o.delivery_zipcode,'-',''),REPLACE(o.delivery_zipcode,'-','')),5), '') as int)
                                                
                                    where 
                                    to_char(date_trunc('day',o.created_at AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date, 'YYYY-MM-DD') 
                                    and 
                                    o.status_id  in  (14,15,4,11)
                                    ), final_hora 
                                    as ( 
                                    select
                                        *
                                    from realtime_orders 
                                    
                                    union all 
                                    
                                    select * from "{schema}".realtime_orders_lastyear
                                    )
                                    SELECT *,
                                        to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'), 'DD/MM/YYYY HH24:MI') AS hora_atualizacao
                                    FROM final_hora;
                        """


                # Inicializa o PostgresHook
                hook3 = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                
                # Executa a query com os parâmetros
                hook3.run(query)



        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
    




    @task()
    def get_parameters(**kwargs):
        schema = kwargs["params"]["PGSCHEMA"]
        
        hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")

        # executa as duas queries
        realtime_orders = hook.get_records(f"""SELECT * FROM "{schema}".realtime_orders_ia""")
        realtime_forecast_hour = hook.get_records(f"""SELECT * FROM "{schema}".realtime_forecast_hour""")

        # cria um dicionário com os resultados
        data_dict = {
            "realtime_orders": realtime_orders,
            "realtime_forecast_hour": realtime_forecast_hour
        }

        return data_dict

    @task()
    def run_extract_tasks(param_dict: dict,**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        extract_postgres_to_json(param_dict, PGSCHEMA)
 
    

    try:
         # Pipeline
       
        criar_orders_lastyear_default = create_realtime_orders_lastyear_default()
        criar_forecast_default = create_realtime_forecast_hour_default()
        criar_orders = create_realtime_orders()
        param_dict = get_parameters()
        extraction = run_extract_tasks(param_dict)
        
        criar_orders_lastyear_default >> criar_forecast_default >> criar_orders >> param_dict >> extraction 
        
        
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow