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
    "dev-0-CreateInfra",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["CreateInfradev", "v2", "trigger_dag_imports"],
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
            title="hosting:",
            description="Enter type integration(vtex ou shopify).",
            section="Important params",
            min_length=1,
            max_length=50,
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
                sql_script = scripts.vtexsqlscripts(PGSCHEMA, "appgemdatapgserveradmin")
            else:
                sql_script = scripts.shopifysqlscripts(PGSCHEMA, "appgemdatapgserveradmin")

           
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            hook.run(sql_script)
            
            query = """
            UPDATE public.integrations_integration
            SET infra_create_date = %s, infra_create_status = True
            WHERE id = %s;
            """

            # Initialize the PostgresHook
            hook2 = PostgresHook(postgres_conn_id="appgemdata-homol")

            # Execute the query with parameters
            hook2.run(query, parameters=(datetime.now(), PGSCHEMA))
            
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_postgres_infra - {e}"
            )
            return e
    
    @task(provide_context=True)
    def choose_trigger_dag(**kwargs):
        hosting = kwargs["params"]["HOSTING"]
          # Adicione o log aqui
        logging.info(f"Escolhido o branch com base no HOSTING: {hosting}")
        if hosting.lower() == "vtex":
            return "trigger_vtex_import"
        elif hosting.lower() == "shopify":
            return "trigger_shopify_orders_import"
        else:
            raise ValueError("HOSTING must be 'vtex' or 'shopify'.")


    # Trigger para VTEX
    trigger_vtex_import = TriggerDagRunOperator(
        task_id="trigger_vtex_import",
        trigger_dag_id="1-ImportVtex-Brands-Categories-Skus-Products",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )

    # Trigger para Shopify
    trigger_shopify_orders_import = TriggerDagRunOperator(
        task_id="trigger_shopify_orders_import",
        trigger_dag_id="1-Orders-Shopify",
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
        },
    )


        # Configurando as tarefas
    create_postgres_infra_task = BranchPythonOperator(
        task_id="create_postgres_infra",
        python_callable=create_postgres_infra,
    )

    branch_task = BranchPythonOperator(
        task_id="choose_trigger_dag",
        python_callable=choose_trigger_dag,
    )
    # Configuração de dependências
    create_postgres_infra_task >> branch_task
    branch_task >> [trigger_vtex_import, trigger_shopify_orders_import]
