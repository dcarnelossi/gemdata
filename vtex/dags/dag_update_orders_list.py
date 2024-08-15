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
    "3-DagUpdate-Orders-List",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Update", "v1", "ALTERAR"],
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
    def update_daily_orders_list(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        isdaily = kwargs["params"]["ISDAILY"]
        if isdaily is False:
            return True

        from modules.sqlscriptsdaily import vtexsqlscriptsorderslistupdate

        try:
            # Defina o código SQL para criar a tabela
            sql_script = vtexsqlscriptsorderslistupdate(PGSCHEMA)

            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            hook.run(sql_script)
            
            query = """
            UPDATE public.integrations_integration
            SET import_last_run_date = %s, is_active_status_date = True
            WHERE id = %s;
            """

            # Initialize the PostgresHook
            hook2 = PostgresHook(postgres_conn_id="appgemdata-dev")

            # Execute the query with parameters
            hook2.run(query, parameters=(datetime.now(), PGSCHEMA))
            
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during orders_update_postgres - {e}"
            )
            return e

    trigger_dag_orders = TriggerDagRunOperator(
        task_id="trigger_dag_orders",
        trigger_dag_id="4-ImportVtex-Orders",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}"
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    update_daily_orders_list_task = update_daily_orders_list()

    update_daily_orders_list_task >> trigger_dag_orders
