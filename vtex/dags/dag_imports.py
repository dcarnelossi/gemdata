import logging
import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from modules.dags_common_functions import (
    get_coorp_conection_info,
    get_data_conection_info,
    integrationInfo,
    get_api_conection_info,
    get_import_last_rum_date,
)

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


with DAG(
    "1-ImportVtex-Brands-Categories-Skus-Products",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["brands", "categories", "skus", "products", "IMPORT"],
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
    def log_import_resumo(**kwargs):
        try: 
            from modules.teste_dbpgconn import WriteJsonToPostgres
            import uuid   
            report_id = str(uuid.uuid4()) 
            integration_id = kwargs["params"]["PGSCHEMA"]
            isdaily = kwargs["params"]["ISDAILY"]
            if isdaily:
                nameprocess = "PROCESSO DIA"
            else:    
                nameprocess = "PROCESSO HISTORICO"

            dag_run_id = kwargs['dag_run'].run_id
            dataini = datetime.now() 
            data = {
                'id':report_id ,
                'integration_id': integration_id,
                'nameprocess': nameprocess,
                'dag': "teste",
                'dag_run_id': dag_run_id,
                'dag_started_at': dataini,
                
            }
            coorp_conection_info = get_coorp_conection_info()
            
            writer = WriteJsonToPostgres(coorp_conection_info , data , "log_import_import", "dag_run_id")
            writer.upsert_data()
            logging.info(f"upserted do log diario successfully.")
        except Exception as e:
            logging.error(f"Error inserting log diario: {e}")
            raise e  # Ensure failure is propagated to Airflow

    @task(provide_context=True)
    def brands(**kwargs):
        integration_id = kwargs["params"]["PGSCHEMA"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(integration_id)
        api_conection_info = get_api_conection_info(integration_id)

        from modules import brand

        try:
            query = """
            UPDATE public.integrations_integration
            SET daily_run_date_ini = %s,
            isdaily_manual = false
            WHERE id = %s;
            """
            # Initialize the PostgresHook
            hook2 = PostgresHook(postgres_conn_id="appgemdata-dev")
            # Execute the query with parameters
            
            hook2.run(query, parameters=(datetime.now(),integration_id))


            brand.get_brands_list_parallel(api_conection_info, data_conection_info)

            # Pushing data to XCom
            kwargs["ti"].xcom_push(key="integration_id", value=integration_id)
            kwargs["ti"].xcom_push(
                key="coorp_conection_info", value=coorp_conection_info
            )
            kwargs["ti"].xcom_push(key="data_conection_info", value=data_conection_info)
            kwargs["ti"].xcom_push(key="api_conection_info", value=api_conection_info)

            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e

    @task(provide_context=True)
    def categories(**kwargs):
        ti = kwargs["ti"]
        # integration_id = ti.xcom_pull(task_ids="brands", key="integration_id")
        # coorp_conection_info = ti.xcom_pull(
        #     task_ids="brands", key="coorp_conection_info"
        # )
        data_conection_info = ti.xcom_pull(task_ids="brands", key="data_conection_info")
        api_conection_info = ti.xcom_pull(task_ids="brands", key="api_conection_info")

        from modules import category_concurrent as category

        try:
            category.set_globals(30, api_conection_info, data_conection_info)
            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e

    @task(provide_context=True)
    def skus(**kwargs):
        ti = kwargs["ti"]
        # integration_id = ti.xcom_pull(task_ids="brands", key="integration_id")
        # coorp_conection_info = ti.xcom_pull(
        #     task_ids="brands", key="coorp_conection_info"
        # )
        data_conection_info = ti.xcom_pull(task_ids="brands", key="data_conection_info")
        api_conection_info = ti.xcom_pull(task_ids="brands", key="api_conection_info")

        from modules import sku

        try:
            sku.set_globals(1, api_conection_info, data_conection_info)
            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e

    @task
    def products(**kwargs):
        ti = kwargs["ti"]
        # integration_id = ti.xcom_pull(task_ids="brands", key="integration_id")
        # coorp_conection_info = ti.xcom_pull(
        #     task_ids="brands", key="coorp_conection_info"
        # )
        data_conection_info = ti.xcom_pull(task_ids="brands", key="data_conection_info")
        api_conection_info = ti.xcom_pull(task_ids="brands", key="api_conection_info")

        from modules import products

        try:
            products.set_globals(api_conection_info, data_conection_info)
            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
        
    trigger_dag_orders_list = TriggerDagRunOperator(
        task_id="trigger_dag_orders_list",
        trigger_dag_id="2-ImportVtex-Orders-List",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}"
        },  # Se precisar passar informações adicionais para a DAG_B
    )
    # Configurando a dependência entre as tasks
    log=log_import_resumo()
    brands_task = brands()
    categories_task = categories()
    sku_task = skus()
    products_task = products()

    log >> brands_task >> categories_task >> sku_task >> products_task >> trigger_dag_orders_list
