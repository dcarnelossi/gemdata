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
    "CreateInfra-Integration",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["CreateInfra", "v2", "trigger_dag_imports"],
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

    @task(provide_context=True)
    def create_postgres_infra(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]

        from modules.sqlscripts import vtexsqlscripts

        try:
            # Defina o código SQL para criar a tabela
            sql_script = vtexsqlscripts(PGSCHEMA)

            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            hook.run(sql_script)
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_postgres_infra - {e}"
            )
            return False

    trigger_dag_imports = TriggerDagRunOperator(
        task_id="trigger_dag_imports",
        trigger_dag_id="ImportVtex-v2",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}"
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    create_postgres_infra_task = create_postgres_infra()

    create_postgres_infra_task >> trigger_dag_imports
