import base64
import logging

import requests

# from django.conf import settings

# Configuração de logging usando as configurações do Django
logger = logging.getLogger(__name__)


def CreateInfra(schema):
    try:
        # Substitua os valores abaixo com as informações adequadas
        airflow_webserver_url = "http://172.200.12.31:8080"
        username = "airflow"
        password = "iE5MZXqg56xtdlpg5HCkib61EHcmcSG3FLgxLuzyMG5HAyLiaafe7esWMvXa"
        dag_name = "CreateInfra"

        credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "utf-8"
        )

        # Configuração dos parâmetros e execução da DAG
        dag_run_payload = {
            "conf": {
                "schema": schema,
            }
        }

        headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }

        dag_run_response = requests.post(
            f"{airflow_webserver_url}/api/v1/dags/{dag_name}/dagRuns",
            json=dag_run_payload,
            headers=headers,
        )
        dag_run_response.raise_for_status()
        logger.info(f"DAG {dag_name} iniciada com sucesso!")

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição HTTP: {e}")

    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado: {e}")


if __name__ == "__main__":
    CreateInfra("CreateInfra")
