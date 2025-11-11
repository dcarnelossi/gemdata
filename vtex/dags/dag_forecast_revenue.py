import logging

from datetime import datetime,timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
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
    "9-forecast-revenue",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["forecast", "v1", "all"],
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
            description="Enter com False (processo total) ou True (processo diario).",
            section="Important params",
            min_length=1,
            max_length=10,
        ),
        "DATAINICIO": Param(
            type="string",
            title="DATAINICIO:",
            description="Entre com a data de início do forecast (opcional, formato YYYY-MM-DD).",
            section="Important params",
            default="null",  # ← opcional
        ),
    },
) as dag:

    @task(provide_context=True)
    def forecast(**kwargs):
        integration_id = kwargs["params"]["PGSCHEMA"]
        isdaily = kwargs["params"]["ISDAILY"]
        datainicio = kwargs["params"]["DATAINICIO"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(integration_id)
        api_conection_info = get_api_conection_info(integration_id)

        from modules import forecast_prod

        try:
            logging.info(f"📅 DATAINICIO recebida: {datainicio}")

            # ✅ Garante que date_start sempre seja datetime
            if datainicio.lower() == "null" or not datainicio:
                date_start = datetime.now()
                logging.info(f"Nenhuma DATAINICIO informada — usando data atual: {date_start}")
            else:
                date_start = datetime.strptime(datainicio, "%Y-%m-%d")

            # Alterado por gabiru de: timedelta(days=1) para timedelta(days=90)
            if not isdaily:
                forecast_prod.set_globals(
                    api_conection_info, data_conection_info, coorp_conection_info, date_start
                )
                return True
            # Verifica se é domingo
            elif date_start.weekday() == 6 or (datainicio and datainicio.lower() != "null"):
                logging.info(f"Executando forecast (domingo ou data informada: {date_start})")
                forecast_prod.set_globals(
                    api_conection_info, data_conection_info, coorp_conection_info, date_start
                )
                return True
            else:
                logging.info("Dia não é domingo e nenhuma DATAINICIO foi informada — forecast não executado.")
                return True

        except Exception as e:
            logging.exception(f"❌ An unexpected error occurred during DAG - {e}")
            raise e
        

    trigger_dag_create_json = TriggerDagRunOperator(
        task_id="trigger_dag_create_json_dash",
        trigger_dag_id="a10-create-json-dash",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY":"{{ params.ISDAILY }}"
           
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    try:

        forecast_task = forecast()
        
        
        forecast_task  >> trigger_dag_create_json
    
    except Exception as e:
        logging.error(f"Error ao criar o forecast log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow