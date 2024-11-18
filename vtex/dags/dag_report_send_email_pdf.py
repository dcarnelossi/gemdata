import logging

from datetime import datetime
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


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



def update_report_pg(report_id,listaemail,iserro):
     



        end_date = datetime.now()

        print(end_date)
        print(report_id)
        
        if iserro ==1:
            status_dag= "ERRO"
            log = f"ERRO NO DISPARO EMAIL ({listaemail})"
        else:
            status_dag= "SUCESSO"    
            log = f"SUCESSO NO DISPARO DO EMAIL ({listaemail})"



        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook3 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = """
            UPDATE public.reports_report
            SET updated_at =  %s,
            dag_finished_at = %s,
            log = %s,
            dag_last_status = %s
            WHERE id = %s;
            """
            hook3.run(query, parameters=(end_date,end_date,log,status_dag,report_id))
 
            return True
        except Exception as e:
            logging.exception(f"erro ao fazer o update  -  public.reports_report {e}")
            raise


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
          "REPORTID": Param(
            type="string",
            title="REPORT ID:",
            description="Enter the REPORTID da tabela reports report.",
            section="Important params",
            min_length=1,
            max_length=200,
            default=None,  # Valor padrão selecionado
                       
        )   ,"TYPREREPORT": Param(
            type="string",
            title="Tipo de relatorio:",
            description="Entre com o tipo de relatorio.",
            section="Important params",
            enum=["faturamento_mensal", "faturamento_semanal","analise_loja"],
            default=None,  # Valor padrão selecionado
        )

    },
) as dag:
    

        
    @task(provide_context=True)
    def report_baixar_pdf(**kwargs):
        try:
            report_id = kwargs["params"]["REPORTID"]
            from modules import save_to_blob
        
       
              # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = f"""
 
            select distinct 
            
            file
           
            from reports_report ii 

            where 
            ii.id = '{report_id}'
       
                """
        
            resultado_file = hook.get_records(query)
            filename=resultado_file[0][0]

            diretorio = f"/opt/airflow/temp/{filename}"

            # Garante que o diretório existe
            criar_diretorio = os.path.dirname(diretorio)
    
            # Se o diretório não existir, cria-o
            if not os.path.exists(criar_diretorio):
                os.makedirs(criar_diretorio)

            print(filename)
           
            save_to_blob.ExecuteBlob().get_file("jsondashboard",f"{filename}",f"{diretorio}") 
            
        except Exception as e:
            update_report_pg(report_id,'erro',1)
            logging.exception(f"deu erro ao achar o caminho do blob para anexar - {e}")
            raise

        return   diretorio

    

    @task(provide_context=True)
    def report_baixar_email(**kwargs):
        try:
            report_id = kwargs["params"]["REPORTID"]
            integration_id = kwargs["params"]["PGSCHEMA"]
      
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
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
            update_report_pg(report_id,'erro',1)
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
            raise

    @task(provide_context=True)
    def report_tipo_relatorio(**kwargs):
        
        try:
            tiporelatorio= kwargs["params"]["TYPREREPORT"]
            print(tiporelatorio)
            if tiporelatorio== 'faturamento_mensal':
                escrita_email ="Relatório mensal periodico","<p>Segue anexo o relatório mensal.</p>"  
                return   escrita_email          
            elif  tiporelatorio== 'faturamento_semanal':  
                escrita_email = "Relatório semanal periodico","<p>Segue anexo o relatório Semanal.</p>"    
                return escrita_email
                # enviar_email=report_send_email_pdf(listaemail_recebido,"Relatório Semanal","<p>Segue anexo o relatório Semanal.</p>",filepdf_recebido)       
                # return enviar_email   
            elif  tiporelatorio== 'analise_loja':  
                escrita_email = "Relatório análise da loja","<p>Segue anexo o relatório análise da loja.</p>"
                return  escrita_email
            else:
                print("erroo")
                escrita_email = 'sem relatorio','sem relatório'
                return escrita_email
        except Exception as e:
            logging.exception(f"deu erro no assunto e corpo de email - {e}")
            return False

    @task(provide_context=True)
    def enviar_email(listaemail_recebido,filepdf_recebido, escrita_email,**kwargs):
        try:
            report_id = kwargs["params"]["REPORTID"]
            print(escrita_email)
            assunto=escrita_email[0] 
            corpo_email=escrita_email[1] 
              
            from modules import send_email
            email=['gabriel.pereira.sousa@gmail.com','gabriel.sousa89@gmail.com']
            send_email.send_email_via_connection('report@gemdata.com.br',email,assunto,corpo_email,True,filepdf_recebido) 
            
            #send_email.send_email_via_connection('gabriel.pereira.sousa@gmail.com',assunto,corpo_email,True,filepdf_recebido)
        except Exception as e:
            update_report_pg(report_id,'erro',1)
            logging.exception(f"deu erro ao achar ao enviar email - {e}")
            raise


    @task(provide_context=True)
    def update_pg(listemail,**kwargs):
        report_id = kwargs["params"]["REPORTID"]
        print(report_id)
        try:
            print("inicando a atualizacao do reports_report no postgree ...")
            update_report_pg(report_id,listemail,0)
        except Exception as e:
                logging.exception(f"Erro ao processar  update report pg - {e}")
                raise
        print("Finalizado a atualizacao do reports_report no postgree ...")    
       

    listemail=report_baixar_email()
    pdffile=report_baixar_pdf()
    escrita_email =report_tipo_relatorio()
    
    enviar= enviar_email(listemail,pdffile,escrita_email)
    update_log_pg=update_pg(listemail)
    listemail >> pdffile >> escrita_email >> enviar >>update_log_pg

    

      