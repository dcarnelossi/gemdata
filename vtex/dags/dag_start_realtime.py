from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import logging

default_args = {
    "owner": "Daniel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Tabela auxiliar:
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
    schedule_interval=timedelta(minutes=10),   # 👈 executa a cada 10 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["queue", "integration", "batch"],
) as dag:

    # 1️⃣ Busca até 5 integrações pendentes da fila
    @task
    def load_pending_integrations():
        hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT integration_id, hosting 
            FROM public.integration_dispatch_queue 
            WHERE dispatched = false 
            ORDER BY created_at ASC
            LIMIT 5;
        """)
        pending = cur.fetchall()

        # Se não tiver pendentes, recarrega a fila
        if not pending:
            cur.execute("""
                TRUNCATE TABLE public.integration_dispatch_queue;
                INSERT INTO public.integration_dispatch_queue (integration_id, hosting)
                SELECT id, hosting
                FROM public.integrations_integration
                WHERE is_active = TRUE AND infra_create_status = TRUE AND hosting <> 'moovin';
            """)
            conn.commit()
            raise AirflowSkipException("Fila recarregada. Próxima execução processará os novos registros.")

        conn.close()
        return [{"id": p[0], "hosting": p[1]} for p in pending]

    pending_batch = load_pending_integrations()

    # 2️⃣ Cria dinamicamente os triggers para cada integração pendente
    @task
    def update_dispatched_flag(integration_id):
        hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE public.integration_dispatch_queue
            SET dispatched = true, dispatched_at = NOW()
            WHERE integration_id = %s;
        """, (integration_id,))
        conn.commit()
        cur.close()
        conn.close()

    # 3️⃣ Grupo de triggers — 1 por integração
    from airflow.models.baseoperator import chain

    with TaskGroup("trigger_integrations") as trigger_group:
        from airflow.utils.helpers import chain

        def _create_trigger_task(integration):
            integration_id = integration["id"]
            hosting = integration["hosting"].lower()

            dag_map = {
                "vtex": "RT-1-vtex-orders",
                "shopify": "RT-1-shopify-orders",
                "lintegrada": "RT-1-li-orders",
                "nuvem_shop": "RT-1-nuvem-orders",
            }

            dag_to_trigger = dag_map.get(hosting)
            if not dag_to_trigger:
                logging.info(f"Sem DAG configurada para hosting: {hosting}")
                return None

            trigger_task = TriggerDagRunOperator(
                task_id=f"trigger_{hosting}_{integration_id}",
                trigger_dag_id=dag_to_trigger,
                conf={"PGSCHEMA": integration_id},
                wait_for_completion=False,
            )

            update_task = update_dispatched_flag.override(
                task_id=f"mark_dispatched_{integration_id}"
            )(integration_id)

            chain(trigger_task, update_task)
            return trigger_task

        # Cria tasks dinamicamente a partir do batch
        from airflow.operators.empty import EmptyOperator
        start = EmptyOperator(task_id="start_triggers")
        end = EmptyOperator(task_id="end_triggers")

        trigger_tasks = []
        for integration in pending_batch:
            t = _create_trigger_task(integration)
            if t:
                trigger_tasks.append(t)

        if trigger_tasks:
            chain(start, *trigger_tasks, end)
        else:
            start >> end

    # 4️⃣ Encadeia o fluxo completo
    pending_batch >> trigger_group
