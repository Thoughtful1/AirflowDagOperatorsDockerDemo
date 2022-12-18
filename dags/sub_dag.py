from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


def table_queries_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    path = Variable.get('name_path_variable')
    subdag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        start_date=start_date,
        schedule_interval=None
    )

    def print_dag_result(**context):
        bash_pushed_via_return_value = context['ti'].xcom_pull(dag_id='certain_dag_nr_3', key='jobs_result_msg',
                                                               task_ids=None,
                                                               include_prior_dates=True)
        print(f"The xcom value pushed by task *success_in_xcom* is {bash_pushed_via_return_value}")
        dictionary = context['ti'].get_template_context()
        print(f"whole dictionary context[ti] is {str(dictionary)}")

    bash_create_finished = BashOperator(task_id="create_file_after_finish",
                                        bash_command=('cd ' + path + ' && touch finished_' + '{{ ts_nodash }}'),
                                        dag=subdag)

    bash_remove_run_file = BashOperator(task_id="remove_certain_file",
                                        bash_command=('rm ' + path + "/run"),
                                        dag=subdag)
    print_result = PythonOperator(
        task_id='print_dag_result',
        python_callable=print_dag_result,
        provide_context=True,
        dag=subdag)
    external_sensor = ExternalTaskSensor(
        task_id="task_sensor",
        external_dag_id=parent_dag_name,
        external_task_id='triggered_operator',
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
        dag=subdag)

    external_sensor >> print_result >> bash_create_finished >> bash_remove_run_file
    return subdag
