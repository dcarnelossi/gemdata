import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param

from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator





def check_previous_dag_success(dag_id, **kwargs):
    try:
        ti = kwargs['ti']
        prev_dag_state = ti.xcom_pull(task_ids=f'task_of_{dag_id}')

        return f'trigger_next_{dag_id}' if prev_dag_state == "success" else f'skip_next_{dag_id}'

    except Exception as e:
        logging.error(f"Error in check_previous_dag_success for DAG {dag_id}: {str(e)}")
        raise


def create_trigger_dag_task(dag_id, trigger_dag_id, dag, schema):
    try:
        return TriggerDagRunOperator(
            task_id=f'trigger_next_{dag_id}',
            trigger_dag_id=trigger_dag_id,
            conf={'schema': schema},
            dag=dag,
        )
    except Exception as e:
        logging.error(f"Error creating TriggerDagRunOperator for DAG {dag_id}: {str(e)}")
        raise


def create_skip_dag_task(dag_id, dag):
    try:
        return EmptyOperator(
            task_id=f'skip_next_{dag_id}',
            dag=dag,
        )
    except Exception as e:
        logging.error(f"Error creating EmptyOperator for skipping DAG {dag_id}: {str(e)}")
        raise


def create_dag_tasks(dag_id, dag, schema):
    check_previous_task = BranchPythonOperator(
        task_id=f'check_previous_{dag_id}',
        python_callable=check_previous_dag_success,
        provide_context=True,
        op_args=[dag_id],
        dag=dag,
    )

    trigger_next_dag_task = create_trigger_dag_task(dag_id, f'{dag_id}_to_execute', dag, schema)
    skip_next_dag_task = create_skip_dag_task(dag_id, dag)

    check_previous_task >> [trigger_next_dag_task, skip_next_dag_task]

    return [check_previous_task, trigger_next_dag_task, skip_next_dag_task]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 26),
    #'retries': 1,
    # retry_delay': timedelta(minutes=5),
    'params': {"schema": Param(
        type="string",
        title="Schema:",
        description="Enter the integration Schema.",
        section="Important params",
        min_length=1,
        max_length=200,
    )},
}

dag = DAG(
    'sequential_dags_example',
    default_args=default_args,
    schedule_interval=None,
    params={"schema": '{{ dag_run.conf["schema"] if dag_run.conf else params.schema }}'}
)

with dag:
    start_task = EmptyOperator(
        task_id='start',
    )

    end_task = EmptyOperator(
        task_id='end',
    )

    dags_to_execute = ['next_dag', 'next_dag2', 'next_dag3', 'next_dag4', 'next_dag5']

    tasks = [start_task] + [task for dag_id in dags_to_execute for task in create_dag_tasks(dag_id, dag, '{{ params.schema }}')] + [end_task]

    # Set the task dependencies
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
