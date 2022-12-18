from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from taskGroupSubDag import get_sub_dag


with DAG(dag_id="example_task_group", start_date=days_ago(2), tags=["task group example"]) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
        task_3 = DummyOperator(task_id="task_3")
        task_1 >> [task_2, task_3]

    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = DummyOperator(task_id="task_1")

        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = DummyOperator(task_id="task_3")
            task_4 = DummyOperator(task_id="task_4")
            [task_2, task_3] >> task_4

    end = DummyOperator(task_id='end')
    bash_subdag = SubDagOperator(
        subdag=get_sub_dag(dag.dag_id, 'bash_subdag'),
        task_id='bash_subdag',
        dag=dag
    )
    start >> section_1 >> section_2 >> bash_subdag >> end
