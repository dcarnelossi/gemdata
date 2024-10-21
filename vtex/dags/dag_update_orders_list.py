import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from modules.dags_common_functions import (
    get_coorp_conection_info,
)


def log_error(context):
    task_instance = context.get('task_instance')
    error_message = context.get('exception')
    ti = context.get('task_instance')
    id_report = ti.xcom_pull(task_ids='gerar_reportid')

    print(f"Tarefa {task_instance.task_id} falhou com o erro: {error_message}")
    
    # Aqui você pode chamar sua função de log
    if id_report :
        log_import_pyhton(isfirtline=False, reportid=id_report, erro=str(error_message) , **context)
    else:
        print("erro antes de inserir") 
             


def log_import_pyhton(isfirtline,reportid=None,erro=None,**kwargs):
            try: 
                
                integration_id = kwargs["params"]["PGSCHEMA"]
                dag_run_id = kwargs['dag_run'].run_id  
                
                print(reportid)
                report_id = reportid
                if erro and not isfirtline:
                    
                    dag_finished_at = datetime.now()
                    dag_last_status = "ERRO"
                        
                    data = {
                        'id':report_id ,
                        'integration_id': integration_id,
                        'dag_run_id': dag_run_id,
                        'dag_finished_at': dag_finished_at,
                        'dag_last_status': dag_last_status,
                        'log':  erro

                    }
                elif erro and isfirtline:
                    dataini = datetime.now()
                    dag_last_status = "ERRO"   
                    dag_name = kwargs['dag'].dag_id
                    dag_finished_at = datetime.now()
                    nameprocess = "PROCESSO AIRFLOW"
                    
                    data = {
                        'id':report_id ,
                        'integration_id': integration_id,
                        'nameprocess': nameprocess,
                        'dag': dag_name,
                        'dag_run_id': dag_run_id,
                        'dag_started_at': dataini,
                        'dag_last_status': dag_last_status,
                        'dag_finished_at': dag_finished_at,
                        'log':  erro
                        
                    }


                elif not erro and not isfirtline:
                    
                    dag_finished_at = datetime.now()
                    dag_last_status = "SUCESSO"
                        
                    data = {
                        'id':report_id ,
                        'integration_id': integration_id,
                        'dag_run_id': dag_run_id,
                        'dag_finished_at': dag_finished_at,
                        'dag_last_status': dag_last_status   
                    }
                    
                elif not erro and isfirtline:
                    dataini = datetime.now()
                    dag_last_status = "EXECUTANDO"   
                    dag_name = kwargs['dag'].dag_id
                    nameprocess = "PROCESSO AIRFLOW"
                    
                    data = {
                        'id':report_id ,
                        'integration_id': integration_id,
                        'nameprocess': nameprocess,
                        'dag': dag_name,
                        'dag_run_id': dag_run_id,
                        'dag_started_at': dataini,
                        'dag_last_status': dag_last_status
                        
                    }


                
                coorp_conection_info = get_coorp_conection_info()
                from modules import log_resumo_airflow
                log_resumo_airflow.log_process(coorp_conection_info , data )

                logging.info(f"upserted do log diario successfully.")

                return report_id
            except Exception as e:
                logging.error(f"Error inserting log diario: {e}")
                raise e  # Ensure failure is propagated to Airflow
            
        

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
    'on_failure_callback': log_error
}

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "3-DagUpdate-Orders-List",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Update", "v1", "vtex"],
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
    def gerar_reportid(**kwargs):
        import uuid 
        idreport = kwargs['params'].get('IDREPORT')
        if idreport:
            report_id=idreport
        else:    
            report_id = str(uuid.uuid4())
        
        return report_id
    
    report = gerar_reportid()

    log_import_task_ini = PythonOperator(
            task_id='log_import_task_ini',
            python_callable=log_import_pyhton,
            op_kwargs={
                'isfirtline':True,
                'reportid': report,  # Defina conforme necessário
                'erro': None,
            },
            provide_context=True,  # Isso garante que o contexto da DAG seja passado
            dag=dag
        )
    log_import_task_fim = PythonOperator(
            task_id='log_import_task_fim',
            python_callable=log_import_pyhton,
            op_kwargs={
                'isfirtline':False,
                'reportid': report,  # Defina conforme necessário
                'erro': None,
            },
            provide_context=True,  # Isso garante que o contexto da DAG seja passado
            dag=dag
        )
    
    
    @task(provide_context=True)
    def update_daily_orders_list(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        isdaily = kwargs["params"]["ISDAILY"]
        if isdaily is False:
            return True

        from modules.sqlscriptsdaily import vtexsqlscriptsorderslistupdate

        try:
            # Defina o código SQL para criar a tabela
            sql_script = vtexsqlscriptsorderslistupdate(PGSCHEMA)

            # Conecte-se ao PostgreSQL e execute o script
            # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
            # não pode estar cravada aqui no codigo
            hook = PostgresHook(postgres_conn_id="integrations-data-dev")
            hook.run(sql_script)
            
            query = """
            UPDATE public.integrations_integration
            SET import_last_run_date = %s
            WHERE id = %s;
            """
            # Initialize the PostgresHook
            hook2 = PostgresHook(postgres_conn_id="appgemdata-dev")

            # Execute the query with parameters
            hook2.run(query, parameters=(datetime.now(), PGSCHEMA))
            
            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during orders_update_postgres - {e}"
            )
            return e
    
    
    trigger_dag_orders = TriggerDagRunOperator(
        task_id="trigger_dag_orders",
        trigger_dag_id="4-ImportVtex-Orders",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY": "{{ params.ISDAILY }}",
            "IDREPORT": report,
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas
    try:

        update_daily_orders_list_task = update_daily_orders_list()
    

        report >> log_import_task_ini >> update_daily_orders_list_task >> log_import_task_fim >> trigger_dag_orders 
    
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow