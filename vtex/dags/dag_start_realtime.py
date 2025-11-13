from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "Daniel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

#  PARA FAZER ESSE FLUXO , EU CRIEI ESSA TABELA 
#
# CREATE TABLE public.integration_dispatch_queue (
#     id SERIAL PRIMARY KEY,
#     integration_id uuid NOT NULL,
#     hosting VARCHAR(100),
#     dispatched BOOLEAN DEFAULT FALSE,
#     dispatched_at TIMESTAMP NULL,
#     created_at TIMESTAMP DEFAULT NOW()
# );

with DAG(
    "RT-0-Start",
    schedule_interval=timedelta(minutes=10),  # 👈 a cada 10 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["queue", "integration", "batch"],
) as dag:

    @task
    def load_pending_integrations():
        logging.info("Select no integration_dispatch_queue para saber se precisa reiniciar atualizacao")
        hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
       
        # Verifica se existem pendentes
        query= f"""
            SELECT integration_id, hosting 
            FROM public.integration_dispatch_queue 
            WHERE dispatched = false 
            ORDER BY created_at ASC
            LIMIT 5;
        """
        pending = hook.get_records(query)
        logging.info(f""" Qtd de clientes que serão atualizados: {len(pending)} """)
        if not pending:
            # Recarrega se não houver pendentes
            query2 = f"""
                TRUNCATE TABLE public.integration_dispatch_queue;
                INSERT INTO public.integration_dispatch_queue (integration_id, hosting)
                SELECT id, hosting
                FROM public.integrations_integration
                WHERE is_active = TRUE 
                  AND infra_create_status = TRUE 
                  AND hosting <> 'moovin';
            """
            hook.run(query2)
            logging.info("Fila recarregada. Próxima execução processará os novos registros.")
            pending = hook.get_records(query) 
            logging.info(f""" Qtd de clientes que serão atualizados: {len(pending)} """)

        # XCom: lista de dicts
        return [{"id": p[0], "hosting": p[1]} for p in pending]

    @task
    def dispatch_integrations(batch):
        """
        batch chega já como lista de dicts, porque o Airflow
        resolve o XComArg antes de chamar a função.
        """
        if not batch:
            logging.info("Nenhuma integração pendente para disparar.")
            return

        hook2 = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
        
        logging.info(f""" Disparando dag dos clientes """)
        # pega o contexto correto da TaskInstance atual
        context = get_current_context()

        for item in batch:
            integration_id = item["id"]
            hosting = item["hosting"].lower()

            if hosting == "vtex":
                logging.info(f""" Disparando dag vtex {integration_id}""")
                dag_to_trigger = "RT-1-vtex-orders"
            elif hosting == "shopify":
                logging.info(f""" Disparando dag shopify {integration_id}""")
                dag_to_trigger = "RT-1-shopify-orders"
            elif hosting == "lintegrada":
                logging.info(f""" Disparando dag loja integrada {integration_id}""")
                dag_to_trigger = "RT-1-li-orders"
            elif hosting == "moovin":
                logging.info(f""" Disparando dag moovin {integration_id}""")
                dag_to_trigger = ""   # ainda não implementado
            elif hosting == "nuvemshop":
                logging.info(f""" Disparando dag nuvem shop {integration_id}""")
                dag_to_trigger = "RT-1-nuvem-orders"
            else:
                logging.info(f"Nenhuma DAG configurada para hosting={hosting}, integration_id={integration_id}")
                continue

            if not dag_to_trigger:
                continue

            # dispara DAG filho usando o contexto correto
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{hosting}_{integration_id}",
                trigger_dag_id=dag_to_trigger,
                conf={"PGSCHEMA": integration_id},
                wait_for_completion=False,
            )
            trigger.execute(context=context)

            # atualiza o status na fila
            query3 = f"""
                UPDATE public.integration_dispatch_queue
                SET dispatched = true, dispatched_at = NOW()
                WHERE integration_id = '{integration_id}';
            """

            hook2.run(query3)
       
    # fluxo principal
    batch = load_pending_integrations()
    dispatch_integrations(batch)
