import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator

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
    "0-CreateInfra",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["CreateInfra", "v2", "trigger_dag_imports"],
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
            False,
            type="boolean",
            title="ISDAILY:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
        ),
         "HOSTING": Param(
            type="string",
            title="HOSTING:",
            description="Enter the integration HOSTING.",
            section="Important params",
            min_length=1,
            max_length=200,
        ),
    },
) as dag:

    @task(provide_context=True)
    def create_postgres_infra(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        HOSTING = kwargs["params"]["HOSTING"]

        import modules.sqlscripts as scripts

        try:
            
            
            # Defina o código SQL para criar a tabela
            if(HOSTING == 'vtex'):
                sql_script = scripts.vtexsqlscripts(PGSCHEMA, "adminuserapppggemdataprod")
            elif(HOSTING == 'shopify'):
                sql_script = scripts.shopifysqlscripts(PGSCHEMA, "adminuserapppggemdataprod")
            elif(HOSTING == 'loja_integrada'):
                sql_script = scripts.lojaintegradasqlscripts(PGSCHEMA, "adminuserapppggemdataprod")
            
            elif(HOSTING == 'moovin'):
                 sql_script = scripts.moovinsqlscripts(PGSCHEMA, "adminuserapppggemdataprod")
            
            elif(HOSTING == 'nuvem_shop'):
                 sql_script = scripts.nuvemsqlscripts(PGSCHEMA, "adminuserapppggemdataprod")
            
            else:
                sql_script ="erro"
            
            sql_script_ga = scripts.gasqlscripts(PGSCHEMA, "adminuserapppggemdataprod")

            #Prod
            hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
            #homol
            #hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            
            hook.run(sql_script)

            hook.run(sql_script_ga)



            
            query = """
            UPDATE public.integrations_integration
            SET infra_create_date = %s, infra_create_status = True
            WHERE id = %s;
            """

            # Initialize the PostgresHook
            #Prod
            hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            #Homol
            #hook2 = PostgresHook(postgres_conn_id="appgemdata-homol")


            # Execute the query with parameters
            hook2.run(query, parameters=(datetime.now(), PGSCHEMA))
            




            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_postgres_infra - {e}"
            )
            return e
    
    
    def choose_trigger_dag(**kwargs):
        hosting = kwargs["params"]["HOSTING"]
          # Adicione o log aqui
        #logging.info(f"Escolhido o branch com base no HOSTING: {hosting}")
        if hosting.lower() == "vtex":
            return 'trigger_vtex_import'
        elif hosting.lower() == 'shopify':
            return 'trigger_shopify_orders_import'
        elif hosting.lower() == 'loja_integrada':
            return 'trigger_li_orders_import'
        elif hosting.lower() == 'moovin':
            return 'trigger_moovin_orders_import'
        elif hosting.lower() == 'nuvem_shop':
            return 'trigger_nuvem_shop_orders_import'
        else:
            return "erro"

    branch_task = BranchPythonOperator(
        task_id='choose_trigger_dag',
        provide_context=True,
        python_callable=choose_trigger_dag
    )

    # Trigger para VTEX
    trigger_vtex_import_ini = TriggerDagRunOperator(
        task_id="trigger_vtex_import",
        trigger_dag_id="1-ImportVtex-Brands-Categories-Skus-Products",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )

    # Trigger para Shopify
    trigger_shopify_orders_import_ini = TriggerDagRunOperator(
        task_id="trigger_shopify_orders_import",
        trigger_dag_id="shopify-1-Orders",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )

        # Trigger para Shopify
    trigger_li_orders_import_ini = TriggerDagRunOperator(
        task_id="trigger_li_orders_import",
        trigger_dag_id="LI-1-List-Products",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )

        # Trigger para Shopify
    trigger_moovin_orders_import_ini = TriggerDagRunOperator(
        task_id="trigger_moovin_orders_import",
        trigger_dag_id="moovin-1-Products",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )

    trigger_nuvem_shop_orders_import_ini = TriggerDagRunOperator(
        task_id="trigger_nuvem_shop_orders_import",
        trigger_dag_id="nuvem-1-Categories",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )


    # Configurando a dependência entre as tarefas
    create_postgres_infra_task = create_postgres_infra()
   # choose_trigger_dag_task = choose_trigger_dag()


    create_postgres_infra_task >> branch_task >> [trigger_vtex_import_ini, trigger_shopify_orders_import_ini,trigger_li_orders_import_ini,trigger_moovin_orders_import_ini,trigger_nuvem_shop_orders_import_ini]

