from typing import Dict
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from PostgreSQLCountRowsOperator import PostgreSQLCountRowsOperator

schema = "airflow"


@dag(schedule_interval='@daily', start_date=days_ago(2), catchup=False, tags=['taskflow'])
def create_dag():
    def check_table_exist(sql_to_check_table_exist):
        """ callable function to get schema name and after that check if table exist """
        request = sql_to_check_table_exist
        pg_hook = PostgresHook(postgres_default="postgresql+psycopg2://airflow:airflow@postgres/airflow", schema=schema)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        connection.commit()
        db_result_messages = cursor.fetchall()
        print("select results: " + str(db_result_messages))
        cursor.close()
        connection.close()

        if db_result_messages:
            return 'insert_new_row'
        else:
            return 'create_the_table'

    @task()
    def hello_world(*args):
        print('This is DAG: {} start processing tables in database: {}'.format(str("dag_number"), str("postgres")))

    @task(multiple_outputs=True, task_id="success_in_xcom", provide_context=True)
    def display_run_id(**context) -> Dict[str, str]:
        return {
            'job_result_msg': (context['dag_run'].run_id + " ended")
        }

    insert_new_row = PostgreSQLCountRowsOperator(task_id='insert_new_row', retries=3,
                                                 queries_filename='dags/insert_into_table.sql',
                                                 trigger_rule='none_failed')
    query_the_table = PostgreSQLCountRowsOperator(task_id='query_the_table', retries=3,
                                                  queries_filename='dags/count_rows.sql')
    create_the_table = PostgreSQLCountRowsOperator(task_id='create_the_table', retries=3,
                                                   queries_filename='dags/create_table.sql')

    check_if_table_exist = BranchPythonOperator(
        task_id='branching',
        python_callable=check_table_exist,
        op_args=["SELECT FROM information_schema.tables WHERE table_name = 'table_example';"],
    )

    get_curr_user = BashOperator(task_id='get_current_user', retries=3,
                                 bash_command="whoami",
                                 do_xcom_push=True,
                                 )

    print_dag_id = hello_world()
    success_in_xcom = display_run_id()
    print_dag_id >> get_curr_user >> check_if_table_exist
    check_if_table_exist.set_downstream([create_the_table, insert_new_row])
    create_the_table >> insert_new_row
    insert_new_row >> query_the_table >> success_in_xcom


for n in range(1, 4):
    dag_id_placeholder = 'certain_dag_nr_{}'.format(str(n))
    dag = create_dag()
    dag._dag_id = dag_id_placeholder
    globals()[dag_id_placeholder] = dag
