from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.models import DagRun

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


def delete_old_dag_runs(**context):
    session = context['session']
    cutoff_date = datetime.now() - timedelta(days=30)
    session.query(DagRun).filter(DagRun.execution_date < cutoff_date).delete()
    session.commit()


with DAG(
    "limpar_execucoes_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Limpar", "v1", "limpeza"],
    render_template_as_native_obj=True,
    params={},
) as dag:

    clean_dag_runs = PythonOperator(
        task_id='delete_old_dag_runs',
        python_callable=delete_old_dag_runs,
        provide_context=True,
    )