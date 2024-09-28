import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from smtplib import SMTP_SSL
from email.mime.text import MIMEText


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


def send_email_via_connection(**kwargs):
    # Recupera a conexão SMTP cadastrada no Airflow
    connection = BaseHook.get_connection('report_email')  # Nome da sua conexão SMTP

    # Define o conteúdo do e-mail
    msg = MIMEText('<p>Este é um teste de envio de email pelo Airflow.</p>', 'html')
    msg['Subject'] = 'Teste de Email no Airflow'
    msg['From'] = connection.login
    msg['To'] = 'gabriel.pereira.sousa@email.com'

    # Envia o e-mail usando as configurações da conexão
    try:
        # Use SMTP_SSL para iniciar a conexão já com SSL
        with SMTP_SSL(host=connection.host, port=connection.port) as server:
            # Não use starttls() com SMTP_SSL, pois a conexão já é segura desde o início
            server.login(connection.login, connection.password)
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")


with DAG(
    "b2-report-sendemail-pdf",
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
          "FILEPDF": Param(
            type="string",
            title="FILEPDF:",
            description="Enter the integration FILEPDF.",
            section="Important params",
            min_length=1,
            max_length=200,
            default=None,  # Valor padrão selecionado
                       
        )   ,"TYPREREPORT": Param(
            type="string",
            title="Tipo de relatorio:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
            enum=["1_relatorio_mensal", "2_relatorio_semanal","3_relatorio_personalizado"],
            default=None,  # Valor padrão selecionado
        )

    },
) as dag:
    

        
    @task(provide_context=True)
    def report_baixar_pdf(**kwargs):
        team_id = kwargs["params"]["PGSCHEMA"]
        caminho_pdf = kwargs["params"]["FILEPDF"]
        from modules import save_to_blob
        diretorio = f"/opt/airflow/temp/{caminho_pdf}"
        save_to_blob.ExecuteBlob().get_file("reportclient",f"{team_id}/{caminho_pdf}",f"{diretorio}") 

        return   diretorio
        #kwargs['ti'].xcom_push(key='lista_diretorio', value=diretorio)
        #return diretorio

    @task(provide_context=True)
    def report_baixar_email(**kwargs):
        team_id2 = kwargs["params"]["PGSCHEMA"]
        try:
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
            ii .id = '{team_id2}'
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
    

    @task(provide_context=True)
    def report_tipo_relatorio(**kwargs):
        tiporelatorio= kwargs["params"]["TYPREREPORT"]
        # listaemail_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_email', key='lista_string') 
        # filepdf_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_pdf', key='lista_diretorio') 
 
        if tiporelatorio== '1_relatorio_mensal':
            return "Relatório Semanal periodico","<p>Segue anexo o relatório Semanal.</p>"              
        elif  tiporelatorio== '2_relatorio_semanal':  
            return "Relatório Semanal periodico","<p>Segue anexo o relatório Semanal.</p>"
               # enviar_email=report_send_email_pdf(listaemail_recebido,"Relatório Semanal","<p>Segue anexo o relatório Semanal.</p>",filepdf_recebido)       
               # return enviar_email   
        elif  tiporelatorio== '3_relatorio_personalizado':   
            return  "Relatório Semanal periodico","<p>Segue anexo o relatório Semanal.</p>"
        else:
            print("aaaaaaaaaaaaaa")
            return 'sem relatorio','sem relatório'

    @task(provide_context=True)
    def enviar_email(**kwargs):
        # Pegando os valores das tarefas anteriores
        listaemail_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_email')
        filepdf_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_pdf')
        assunto, corpoemail = kwargs['ti'].xcom_pull(task_ids='report_tipo_relatorio')
        send_email_via_connection()
        # # Configurando o EmailOperator
        # email = EmailOperator(
        #     task_id='envia_email',
        #     to="gabriel.pereira.sousa@gmail.com", #listaemail_recebido
        #     subject=assunto,
        #     html_content=corpoemail,
        #     files=[filepdf_recebido],
        #     smtp_conn_id='report_email',  # Use o ID da conexão configurada
        # )
        # email

    listemail=report_baixar_email()
    pdffile=report_baixar_pdf()
    tipo=report_tipo_relatorio()
    enviar= enviar_email()
    
    listemail >> pdffile >> tipo >> enviar

    

      
#relatorio_mensal_8_5511999999999_20240927192246.pdf
#         