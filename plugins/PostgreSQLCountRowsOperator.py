import uuid
from datetime import datetime, timezone
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import OperationalError


class PostgreSQLCountRowsOperator(BaseOperator):

    def __init__(
            self, queries_filename: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.queries_filename = queries_filename

    def execute(self, context, ):
        fd = open(self.queries_filename, 'r')
        sql_file = fd.read()
        fd.close()
        schema = "airflow"
        custom_id_value = int(uuid.uuid4().int % 123456789)
        user_name_value = context['ti'].xcom_pull(key='return_value', task_ids='get_current_user',
                                                  include_prior_dates=True, dag_id=None)
        timestamp_value = datetime.now(timezone.utc)
        try:
            request = sql_file
            pg_hook = PostgresHook(postgres_default="postgresql+psycopg2://airflow:airflow@postgres/airflow",
                                   schema=schema)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(str(request), {'int': custom_id_value, 'str': user_name_value,
                                          'date': timestamp_value})
            connection.commit()
            if cursor.pgresult_ptr is not None:
                db_result_messages = cursor.fetchall()
                operator_jobs_done = "counting every record in table_example: " + str(db_result_messages)
                print(operator_jobs_done)
                return operator_jobs_done
            cursor.close()
            connection.close()
            operator_jobs_done = "Query finished successfully"
        except OperationalError as msg:
            print("Command skipped: ", msg)

        return operator_jobs_done
