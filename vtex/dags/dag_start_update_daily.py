import logging
from datetime import datetime, time

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.operators.dummy import DummyOperator




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
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["StartDaily", "v1", "trigger_dag_daily_update"],
    render_template_as_native_obj=True,
    description='Executa a DAG a cada 10 minutos entre 00:30 e 05:00',


) as dag:

    #   # Sensor para esperar até 00:30
    # wait_until_00_30 = TimeSensor(
    #     task_id='wait_until_00_30',
    #     target_time=time(0, 30),
    # )

    @task
    def get_integration_ids():
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-dev")
            query = """
                   select id from public.integrations_integration
            where is_active = true 
            and infra_create_status = true 
            and ((( daily_run_date_end::date < CURRENT_DATE or daily_run_date_end is null)
	            	and 
	            	( daily_run_date_ini::date < CURRENT_DATE or daily_run_date_ini is null))
            	or  isdaily_manual is true )
            order by 1	
 		    limit 2
 		

            """
            integration_ids = hook.get_records(query)
            
           

            return [integration[0] for integration in integration_ids]
           

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during get_integration_ids - {e}"
            )
            raise
        

    def trigger_dag_run(dag_id, conf, execution_date=None, replace_microseconds=False):
        trigger_dag(
            dag_id=dag_id,
            run_id=f"manual__{datetime.utcnow().isoformat()}",
            conf=conf,
            execution_date=execution_date,
            replace_microseconds=replace_microseconds,
        )

    def trigger_dag_run_task(integration_ids):
        for integration_id in integration_ids:
            conf = {
                "PGSCHEMA": integration_id,
                "ISDAILY": True
            }
            trigger_dag_run(
                dag_id="1-ImportVtex-Brands-Categories-Skus-Products",
                conf=conf
            )

    # Crie a tarefa Python para disparar a DAG
    trigger_task = PythonOperator(
        task_id="trigger_import_dags",
        python_callable=trigger_dag_run_task,
        op_args=[get_integration_ids()],
    )

    # # Sensor para garantir que a DAG termine até as 05:00
    # wait_until_05_00 = TimeSensor(
    #     task_id='wait_until_05_00',
    #     target_time=time(5, 0),
    #     mode='reschedule',
    # )
    
    # Definindo a sequência das tarefas
    # wait_until_00_30 >> trigger_task >> wait_until_05_00
    trigger_task