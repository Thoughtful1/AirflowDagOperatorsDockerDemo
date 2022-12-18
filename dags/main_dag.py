from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from SmartFileSensor import SmartFileSensor
from sub_dag import table_queries_sub_dag
from SlackMessagePostingOperator import SlackMessagePostingOperator

dag = DAG('Main_DAG', description='To test repo run this DAG',
          schedule_interval='0 12 * * *',
          start_date=days_ago(1), catchup=True)
path = Variable.set(key='name_path_variable', value='/opt/airflow/for_files_sensor')
path = Variable.get('name_path_variable')

with dag:
    smart_sensor_task = SmartFileSensor(task_id="my_smart_file_sensor_task", poke_interval=10,
                                        dag=dag, filepath=path + "/run", fs_conn_id='fs_default')
    triggered_task = TriggerDagRunOperator(task_id="triggered_operator", trigger_dag_id="certain_dag_nr_3",
                                           dag=dag)
    sub_dag = SubDagOperator(
        subdag=table_queries_sub_dag(dag.dag_id, 'some_dummy_db_queries', datetime(2022, 1, 10), '@once'),
        task_id='some_dummy_db_queries',
        dag=dag)
    #slack_alert_dag = SlackMessagePostingOperator(task_id="slack_operator_task",
    #                                              dag=dag)
    smart_sensor_task >> triggered_task >> sub_dag
    #>> slack_alert_dag
    #SlackOperator commented out. Functionality just for demonstration purpose.
