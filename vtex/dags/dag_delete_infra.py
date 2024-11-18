import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
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
    "DeleteInfra-v2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["DeleteInfra"],
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

    @task
    def delete_postgres_infra(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]

        try:
            # Defina o código SQL para criar a tabela
            sql_script = f"""DROP SCHEMA IF EXISTS "{PGSCHEMA}" CASCADE;"""

            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="integrations-data-prod")
            hook.run(sql_script)
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during Delete_postgres_infra - {e}"
            )
            return False

    # Configurando a dependência entre as tarefas
    delete_postgres_infra()
