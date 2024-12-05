import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


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

# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "0-StartDaily",
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,
    description='Executa a DAG a cada 10 minutos entre 00:30 e 05:00',


) as dag:

    @task(provide_context=True)
    def get_postgres_id():
     
        import modules.sqlscripts as scripts

        try:
           # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = """
                     
                select id,hosting from public.integrations_integration
                            where is_active = true 
                            and infra_create_status = true 
                           
                            order by 1	
                            limit 1
 		

            """
            dados_integration = hook.get_records(query)
           
            print(f"Iniciando criação de infraestrutura para integração: {dados_integration}")
            return {"id": dados_integration[0][0], "hosting": dados_integration[0][1]}            
            
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_postgres_infra - {e}"
            )
            return e
    
    
    def choose_trigger_dag(ti, **context):
        integration_data = ti.xcom_pull(task_ids="get_integration_id")

        print(integration_data)
        hosting = integration_data['hosting']
          # Adicione o log aqui
        #logging.info(f"Escolhido o branch com base no HOSTING: {hosting}")
        if hosting.lower() == "vtex":
            return 'trigger_vtex_import'
        else: 
            return 'trigger_shopify_orders_import'

    branch_task = BranchPythonOperator(
        task_id='choose_trigger_dag',
        provide_context=True,
        python_callable=choose_trigger_dag
    )

    def trigger_dag_run_vtex(ti, **context):
            integration_data = ti.xcom_pull(task_ids="get_integration_id")
            integration_id = integration_data["id"]

            trigger = TriggerDagRunOperator(
                task_id=f"1-ImportVtex-Brands-Categories-Skus-Products-{integration_id}",
                trigger_dag_id="1-ImportVtex-Brands-Categories-Skus-Products",  # Substitua pelo nome real da sua segunda DAG
                conf={
                    "PGSCHEMA": integration_id,
                    "ISDAILY": True,
                },
            )
            trigger.execute(context=context)


    def trigger_dag_run_shopify(ti, **context):
            integration_data = ti.xcom_pull(task_ids="get_integration_id")
            integration_id = integration_data["id"]

            trigger = TriggerDagRunOperator(
                task_id=f"trigger_shopify_orders_import-{integration_id}",
                trigger_dag_id="shopify-1-Orders",  # Substitua pelo nome real da sua segunda DAG
                conf={
                    "PGSCHEMA": integration_id,
                    "ISDAILY": True,
                },
            )
            trigger.execute(context=context)

    # trigger_dag_choose = PythonOperator(
    #         task_id="check_integration_id",
    #         python_callable=choose_trigger_dag,
    #     )

    trigger_dag_vtex = PythonOperator(
            task_id="trigger_vtex_import",
            python_callable=trigger_dag_run_vtex,
        )
    
    trigger_dag_shopify = PythonOperator(
            task_id="trigger_shopify_orders_import",
            python_callable=trigger_dag_run_shopify,
        )

    # Configurando a dependência entre as tarefas
    get_id_task = get_postgres_id()
   # choose_trigger_dag_task = choose_trigger_dag()


    get_id_task >> branch_task >> [trigger_dag_vtex, trigger_dag_shopify]

