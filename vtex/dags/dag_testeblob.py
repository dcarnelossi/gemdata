from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_blob_storage_connection():
    # Nome da conexão configurada no Airflow
    conn_id = "airflowlogs"

    try:
        # Inicializa o WasbHook com o ID da conexão
        wasb_hook = WasbHook(wasb_conn_id=conn_id)

        # Estabelece a conexão
        blob_service_client = wasb_hook.get_conn()

        # Tenta listar os containers no Blob Storage
        containers = blob_service_client.list_containers()
        print(
            f"Conexão bem-sucedida! Containers disponíveis: {[container.name for container in containers]}"
        )
    except Exception as e:
        # Se ocorrer um erro, ele será capturado aqui
        print(f"Erro ao tentar se conectar ao Azure Blob Storage: {str(e)}")
        raise


# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 27),
    "retries": 1,
}

with DAG(
    "test_blob_storage_connection_dag",
    default_args=default_args,
    schedule_interval=None,  # DAG manual
    catchup=False,
) as dag:

    test_connection_task = PythonOperator(
        task_id="test_blob_storage_connection",
        python_callable=test_blob_storage_connection,
    )

    # Definindo a sequência das tasks na DAG
    test_connection_task
