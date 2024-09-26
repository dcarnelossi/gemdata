import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator

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
    "b1-report-pdf",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
                       
        ),
        "CELULAR": Param(
            type="string",
            title="CELULAR:",
            description="Enter the celphone.",
            section="Important params",
            min_length=1,
            max_length=200,
            # default=None,  # Define como None por padrão
            # optional=True,  # Permite que o parâmetro seja opcional
        )
        ,"LOGO": Param(
            type="string",
            title="Caminho logo:",
            description="Enter com caminho do logo (opcional)",
            section="Important params",
            min_length=1,
            max_length=200,
            default=None,  # Define como None por padrão
            optional=True,  # Permite que o parâmetro seja opcional
        ),"MES": Param(
            type="string",
            title="MES:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=2,
         )
        ,"SENDEMAIL": Param(
            type="boolean",
            title="ISDAILY:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
            min_length=1,
            max_length=10,
        )
        

    },
) as dag:

    def my_dag():
        install_and_run = PythonVirtualenvOperator(
        task_id="install_and_run",
        python_callable=report_mensal,
        requirements=["fpdf2", "matplotlib", "numpy", "pandas", "geopandas"],
        system_site_packages=False
    )

        install_and_run


    def report_mensal(**kwargs):
        from fpdf import FPDF
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        import numpy as np
        import geopandas as gpd
        import pandas as pd

        import uuid
        import shutil    

    example_dag = my_dag()

#   # Task para instalar as bibliotecas necessárias
#     install_libraries = BashOperator(
#         task_id='install_libraries',
#         bash_command='pip install fpdf2 matplotlib numpy pandas geopandas'
#     )

#     @task(provide_context=True)
#     def report_mensal(**kwargs):
#         team_id = kwargs["params"]["PGSCHEMA"]
#         celphone = kwargs["params"]["CELULAR"]
#         num_mes = kwargs["params"]["MES"]
#         logo = kwargs["params"]["LOGO"]
#         isemail = kwargs["params"]["SENDEMAIL"]




#         from modules import report_month

#         try:

#             report_month.set_globals(
#                team_id,
#                celphone,
#                num_mes,
#                logo,
#                isemail
#             )
         
#             return True
#         except Exception as e:
#             logging.exception(f"An unexpected error occurred during DAG - {e}")
#             raise



#     install_libraries >> report_mensal()
