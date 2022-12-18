from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


def get_sub_dag(parent_dag_name, child_dag_name):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        schedule_interval="@once",
    )
    for i in range(5):
        DummyOperator(
            task_id=f'{child_dag_name}-task-{i + 1}',
            dag=dag_subdag,
        )
    return dag_subdag
