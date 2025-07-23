import logging

from datetime import datetime,timedelta
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

from airflow.operators.python import BranchPythonOperator




from modules.dags_common_functions import (
    get_data_conection_info,

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
    "a11-send-email-firstprocess",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "v1", "report"],
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
    def report_baixar_email(**kwargs):
        try:
            integration_id = kwargs["params"]["PGSCHEMA"]
      
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = f"""
            SELECT DISTINCT us.username,te."name"  as nameteam
            FROM integrations_integration ii 
            INNER JOIN public.teams_team te ON te.ID = ii.team_id
            INNER JOIN public.teams_membership ms ON ms.team_id = te.id
            INNER JOIN public.users_customuser us ON us.id = ms.user_id 
            WHERE ii.id = '{integration_id}'
              AND us.is_active = TRUE
              AND ii.infra_create_status = TRUE 
              AND ii.is_active = TRUE
            """
        
            resultado_emails = hook.get_records(query)
            nome_team = resultado_emails[0][1]
            emails_list = [email[0] for email in resultado_emails]
            print(emails_list)
           # emails_string = ", ".join(emails_list)
            return emails_list,nome_team
            
        except Exception as e:
            logging.exception(f"Erro ao achar o caminho do logo - {e}")
            raise

    @task(provide_context=True)
    def enviar_email(lista):
        
        try:
           
            emails_string, nome_team = lista
            print(emails_string)
            assunto = f"{nome_team} - dados processados e disponíveis para acesso"
            corpo_email = (
                f"Gostaria de informar que os dados do time <strong>{nome_team}</strong> foram processados com sucesso "
                "e já estão disponíveis tanto na plataforma.<br><br>"
                "Você também ja possuí acesso ao nosso WhatsApp de reports por IA. Para utilizá-lo, basta enviar uma mensagem para: "
                "<a href='https://wa.me/5511934027546' target='_blank'>+55 11 93402-7546</a>.<br><br><br>"
                "Em caso de dúvidas ou qualquer problema, entre em contato diretamente com Marcos pelo WhatsApp: "
                "<a href='https://wa.me/5511981482496' target='_blank'>+55 11 98148-2496</a>.<br><br>"
            )
              
            from modules import send_email
            send_email.send_email_via_connection('tecnologia@gemdata.com.br', emails_string, assunto, corpo_email,False)
            
        except Exception as e:
            logging.exception(f"Erro ao enviar o e-mail - {e}")
            raise

    # Branch para verificar se ISDAILY é True
    def check_isdaily(**kwargs):
        is_daily = kwargs['params'].get('ISDAILY', False)
        if is_daily:
            return 'stop_task'
        else:
            return 'report_baixar_email'

    branch_task = BranchPythonOperator(
        task_id='check_isdaily',
        provide_context=True,
        python_callable=check_isdaily
    )

    # Dummy task para não fazer nada quando ISDAILY for False
    stop_task = DummyOperator(
        task_id='stop_task'
    )

    # Fluxo de e-mails
    listaemail_recebido = report_baixar_email()
    disparar_email = enviar_email(listaemail_recebido)

    # Definir o fluxo de decisão
    branch_task >> [listaemail_recebido, stop_task]
    listaemail_recebido >> disparar_email