import logging

from datetime import datetime
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook




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
    },
) as dag:
    

    @task(provide_context=True)
    def report_baixar_email(**kwargs):
        try:
    
            integration_id = kwargs["params"]["PGSCHEMA"]
      
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = f"""
 
            select distinct 
            
            us.username
           
            from integrations_integration ii 

            inner join public.teams_team te on 
            te.ID = ii.team_id

            inner join  public.teams_membership ms on 
            ms.team_id=  te.id
            
            inner join public.users_customuser us on 
            us.id = ms.user_id 

            where 
            ii .id = '{integration_id}'
            and 
            us.is_active is true
            and 
            ii.infra_create_status =  true 
            and 
            ii.is_active = true 
                """
        
            resultado_emails = hook.get_records(query)
            # Extrair os e-mails e transformar em uma lista de strings
            emails_list = [email[0] for email in resultado_emails]
            # Juntar os e-mails em uma única string separada por vírgulas
            emails_string = ", ".join(emails_list)
            # Adicionar aspas simples no início e no fim da string
            #emails_string = f"'{emails_string}'"
            
           # kwargs['ti'].xcom_push(key='lista_string', value=emails_string)
            return   emails_string
            
        except Exception as e:
        
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
            raise
               
    @task(provide_context=True)
    def enviar_email(listaemail):
        try:
          
            listaemail=report_baixar_email()
            
            assunto="Dados Processados e Disponíveis para Acesso"
            corpo_email="Gostaria de informar que os dados solicitados foram processados com sucesso e estão agora disponíveis na plataforma e whatsapp.<\n> Toda a operação foi conduzida dentro dos parâmetros estabelecidos, e os arquivos foram armazenados de acordo com as diretrizes de segurança e compliance." 
              
            from modules import send_email
           
            send_email.send_email_via_connection('tecnologia@gemdata.com.br',listaemail,assunto,corpo_email) 
            
            #send_email.send_email_via_connection('gabriel.pereira.sousa@gmail.com',assunto,corpo_email,True,filepdf_recebido)
        except Exception as e:
           
            logging.exception(f"deu erro ao achar ao enviar email - {e}")
            raise
    
    
    listaemail_recebido=report_baixar_email() 
    disparar_email=enviar_email(listaemail_recebido)

    listaemail_recebido >> disparar_email

    

      