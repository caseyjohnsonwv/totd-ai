from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



with DAG(
    dag_id = 'tmio_raw',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    sql = 'SELECT COUNT(*) FROM COLLECT.TMIO_RAW;'
    test_connection_task = PostgresOperator(task_id = 'test_connection', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    start_task >> test_connection_task >> end_task