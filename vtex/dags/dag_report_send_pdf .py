import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator

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
    "b2-report-send-pdf",
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
                       
        )
       ,"SENDEMAIL": Param(
            type="boolean",
            title="SEND EMAIL:",
            description="Enter com False (processo whatsapp) ou True (processo email) .",
            section="Important params",
            min_length=1,
            max_length=10,
            default=False,  # Valor padrão selecionado
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

        kwargs['ti'].xcom_push(key='lista_diretorio', value=diretorio)
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
            emails_string = "', '".join(emails_list)
            # Adicionar aspas simples no início e no fim da string
            emails_string = f"'{emails_string}'"
            
            kwargs['ti'].xcom_push(key='lista_string', value=emails_string)

            
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
    
    @task(provide_context=True)
    def report_send_email_pdf(destinatario,assunto,corpo_email,anexo):
        try:
            # Operador para enviar o e-mail
          return  EmailOperator(
                task_id='send_email',
                to= "gabriel.pereira.sousa@gmail.com",  # Defina o destinatário
                subject= assunto,
                html_content=corpo_email,
                files=[anexo],  # Esta lista será preenchida condicionalmente
            )
        except Exception as e:
            logging.exception(f"deu erro ao enviar email - {e}")
            raise


    # @task(provide_context=True)
    @task(provide_context=True)
    def decide_enviar_email (**kwargs):
        enviaremail=  kwargs["params"]["SENDEMAIL"]
        return enviaremail
    

    @task(provide_context=True)
    def dispara_email(**kwargs):
        tiporelatorio= kwargs["params"]["TYPREREPORT"]
        listaemail_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_email', key='lista_string') 
        filepdf_recebido = kwargs['ti'].xcom_pull(task_ids='report_baixar_pdf', key='lista_diretorio') 

        print(tiporelatorio)    
        print(listaemail_recebido)    
        print(filepdf_recebido)    
        if tiporelatorio== '1_relatorio_mensal':
                print("ok")
                enviar_email=report_send_email_pdf(listaemail_recebido,"Relatório Mensal","<p>Segue anexo o relatório mensal.</p>",filepdf_recebido) 
                return enviar_email
        elif  tiporelatorio== '2_relatorio_semanal':  
                print("ok")
                enviar_email=report_send_email_pdf(listaemail_recebido,"Relatório Semanal","<p>Segue anexo o relatório Semanal.</p>",filepdf_recebido)       
                return enviar_email   
        elif  tiporelatorio== '3_relatorio_personalizado':   
                    print("ok")
        else:
            print("aaaaaaaaaaaaaa")
       
    decidir=decide_enviar_email()   
    print(decidir)
    if decidir:
        t1=report_baixar_email()
        t2=report_baixar_pdf()
        t3=dispara_email()
        
        t1 >> t2 >> t3
    else:
        print("entrou para what")
    

    # @task(provide_context=True)
    # def decide_process(**kwargs):
    #     enviaremail=  kwargs["params"]["SENDEMAIL"]
    #     if enviaremail:
    #         listemail=report_baixar_email()
    #         filepdf=report_baixar_pdf()
    #         tiporelatorio= kwargs["params"]["TYPREREPORT"]
    #         if tiporelatorio== '1_relatorio_mensal':
    #             print("ok")
    #             send_email_task =report_send_email_pdf(listemail,"Relatório Mensal","<p>Segue anexo o relatório mensal.</p>",filepdf) 
    #             send_email_task
    #         elif  tiporelatorio== '2_relatorio_semanal':  
    #             print("ok")
    #             send_email_task =report_send_email_pdf(listemail,"Relatório Semanal","<p>Segue anexo o relatório Semanal.</p>",filepdf)       
    #             send_email_task
            
    #         elif  tiporelatorio== '3_relatorio_personalizado':   
    #                 print("ok")
    #     else:
    #         print("aaaaaaaaaaaaaa")
       
    # decide_process()

    

      
#relatorio_mensal_8_5511999999999_20240927192246.pdf
#         