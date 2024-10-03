import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
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


def get_informacao_pg(integration_id,canal,celular,email):


        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            
            if canal != 'whatsapp':
                query = f"""
                 					select distinct 
                    
                    te.id as team_id,
                    te.logo as team_logo,
                    us.id as user_id
                    
                    from integrations_integration ii 

                    inner join public.teams_team te on 
                    te.ID = ii.team_id

                    inner join  public.teams_membership ms on 
                    ms.team_id=  te.id
                    
                    inner join public.users_customuser us on 
                    us.id = ms.user_id 

                    where 
                    ii.id = '{integration_id}'
                    and
                    us.username = '{email}'
                    and 
                    us.is_active is true
                    and 
                    ii.infra_create_status =  true 
                    and 
                    ii.is_active = true"""

            else:
                
                query =   f""" 
                    
					select distinct 
                    
                    te.id as team_id,
                    te.logo as team_logo,
                    us.id as user_id
                    
                    from integrations_integration ii 

                    inner join public.teams_team te on 
                    te.ID = ii.team_id

                    inner join  public.teams_membership ms on 
                    ms.team_id=  te.id
                    
                    inner join public.users_customuser us on 
                    us.id = ms.user_id 

                    where 
                    ii.id = '{integration_id}'
                    and
                    us.cell_phone = +'{celular}'
                    and 
                    us.is_active is true
                    and 
                    ii.infra_create_status =  true 
                    and 
                    ii.is_active = true 

                    """
        
            resultado_logo = hook.get_records(query)
      
            result = resultado_logo[0] 

            return result
        
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
            raise


def insert_report_pg(integration_id,tiporela,canal,infos_user,**kwargs):
        dag_id = "b1-report-create-pdf"
        dag_run_id = kwargs['dag_run'].run_id
        start_date = datetime.now()
        team_id=infos_user[0]
        team_logo=infos_user[1]
        user_id=infos_user[2]

        print(dag_run_id) 
        print(team_id) 
        print(team_logo) 
        print(user_id) 
        

        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook2 = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = """
            INSERT INTO public.reports_report
            (created_at, channel, "type", dag, dag_started_at, dag_run_id, dag_last_status, integration_id, team_id, user_id)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
            hook2.run(query, parameters=(start_date,canal,tiporela,dag_id,start_date,dag_run_id,"EXECUTANDO",integration_id,team_id,user_id))
 
            return True
        except Exception as e:
            logging.exception(f"deu erro ao achar o caminho do logo - {e}")
            raise


with DAG(
    "b1-report-create-pdf",
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
        ,"CHANNEL": Param(
            type="string",
            title="CANAL:",
            description="Escolha qual canal.",
            section="Important params",
            enum=["whatsapp", "email","site"],  # Opções para o dropdown
     #       default=None,  # Valor padrão selecionado
        )
        ,"TYPREREPORT": Param(
            type="string",
            title="Tipo de relatorio:",
            description="Escolha o tipo do relatório.",
            section="Important params",
            enum=["faturamento_mensal", "faturamento_semanal","analise_loja"],  # Opções para o dropdown
     #       default=None,  # Valor padrão selecionado
        )
        ,"DATAINI": Param(
            type="string",
            title="Data inicio:",
            description="Enter the start date  (ex:2024-10-01).",
            section="Important params",
            default="1900-01-01"
         )
        ,"DATAFIM": Param(
            type="string",
            title="Data fim:",
            description="Enter the end date (ex:2024-10-01).",
            section="Important params",
            default="1900-01-01"
         )
        ,"CELULAR": Param(
            type="string",
            title="CELULAR:",
            description="Enter the celphone.",
            section="Important params",
            min_length=1,
            max_length=13,
            default="99",  # Define como None por padrão
          
        ),"EMAIL_PRINCIPAL": Param(
            type="string",
            title="Important params",
            description="Enter the email address to send the email to.",
            default="email",  # Valor padrão
            section="Important params" 
        )
    
    },
) as dag:

    @task(provide_context=True)
    def report_pdf(**kwargs):
        try:
            integration_id = kwargs["params"]["PGSCHEMA"]
            tiporela = kwargs["params"]["TYPREREPORT"]
            canal = kwargs["params"]["CHANNEL"]
            email_prin = kwargs["params"]["EMAIL_PRINCIPAL"]
            celphone = kwargs["params"]["CELULAR"]
            data_ini = datetime.strptime(kwargs["params"]["DATAINI"],"%Y-%m-%d")
            data_fim = datetime.strptime(kwargs["params"]["DATAFIM"],"%Y-%m-%d")
           
            caminho_pdf =""
            print(integration_id)
            print(tiporela)
            print(celphone)
            print(data_ini)
            print(data_fim)
            print(canal)
            print(email_prin)

            current_datetime = datetime.now() 
            numeric_datetime = current_datetime.strftime('%Y%m%d%H%M%S')
            data_conection_info = get_data_conection_info(integration_id)

        except Exception as e:
            logging.exception(f"erro nos paramentos - {e}")
            raise
        infos_user=get_informacao_pg(integration_id)
        
        insert_report_pg(integration_id,tiporela,canal,infos_user)
                

        # # Lógica condicional com base na escolha do usuário
        # if tiporela == "1_relatorio_mensal":
        #     from modules import report_month
        #     mes = data_ini.month 
        #     print(mes)   

        #     caminho_pdf= f"relatorio_mensal_{mes}_{celphone}_{numeric_datetime}"
        #     try:
        #         print("Processando o Relatório mensal...")
        #         report_month.set_globals(
        #         data_conection_info,
        #         integration_id,
        #         celphone,
        #         mes,
        #         caminho_logo,
        #         caminho_pdf 
        #         )
        #         print("Relatório mensal processado...")
        #     except Exception as e:
        #         logging.exception(f"Erro ao processar o relatorio mensal - {e}")
        #         raise
        #     # Coloque a lógica do relatório semanal aqui
        # elif tiporela == "2_relatorio_semanal":
        #     from modules import report_weekly
        #     semana = int(data_ini.strftime("%W"))+1
        #     print(semana)
        #     caminho_pdf= f"relatorio_semanal_{semana}_{celphone}_{numeric_datetime}"
        #     try:
        #         print("Processando o Relatório  semanal...")
        #         report_weekly.set_globals(
        #         data_conection_info,
        #         integration_id,
        #         celphone,
        #         semana,
        #         caminho_logo,
        #         caminho_pdf
        #         )
        #         print("Relatório semanal processado...")
           

        #     except Exception as e:
        #         logging.exception(f"Erro ao processar o relatorio semanal - {e}")
        #         raise
                  
        # elif tiporela == "3_relatorio_analise_loja":
        #     from modules import report_products_analytics
        #     caminho_pdf= f"relatorio_analise_loja_{celphone}_{numeric_datetime}"
        #     try:
        #         print("Processando o Relatório analise loja...")
        #         report_products_analytics.set_globals(
        #         data_conection_info,
        #         integration_id,
        #         celphone,
        #         caminho_logo,
        #         caminho_pdf
        #         )
        #         print(" Relatório analise loja processado...")
           
        #     except Exception as e:
        #         logging.exception(f"Erro ao processar  Relatório analise loja - {e}")
        #         raise
        
                   
        # else:
        #     print("Opção de relatório desconhecida.")
        

        # return caminho_pdf

#     @task.branch
#     def should_trigger_dag(**kwargs):
#     # Substitua `params['YOUR_PARAM']` pela condição que você quer verificar
#         isemail = kwargs["params"]['SENDEMAIL']

#         if isemail:  # Troque YOUR_PARAM pelo nome do parâmetro que você deseja verificar
#             return 'trigger_dag_report_send_pdf'
#         else:
#             return 'skip_trigger'


#     cam_pdf = report_pdf()
    
#     @task
#     def skip_trigger(**kwargs):
#         caminho_whats_pdf = kwargs['ti'].xcom_pull(task_ids='report_pdf')


#         print("Condição não atendida, a DAG não será disparada")
#         return caminho_whats_pdf
   
#     #@task(provide_context=True)   
#     trigger_dag_report_send_pdf = TriggerDagRunOperator(
#         task_id="trigger_dag_report_send_pdf",
#         trigger_dag_id="b2-report-sendemail-pdf",  # Substitua pelo nome real da sua segunda DAG
#         conf={
#                 "PGSCHEMA": "{{ params.PGSCHEMA }}",
#                 "FILEPDF": cam_pdf,
#                 "TYPREREPORT": "{{ params.TYPREREPORT }}"
#             }  # Se precisar passar informações adicionais para a DAG_B
#     )
    

#     should_trigger = should_trigger_dag()
#     skip_trigger_task = skip_trigger()

#     # Definindo as dependências entre as tarefas
#     cam_pdf >>should_trigger >>  [trigger_dag_report_send_pdf, skip_trigger_task]
#    # should_trigger >> skip_trigger_task

    cam_pdf = report_pdf()
    cam_pdf
