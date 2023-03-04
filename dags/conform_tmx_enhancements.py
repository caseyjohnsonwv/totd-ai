from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'conform_tmx_enhancements',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['conform', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # authors etl
    sql = """
        INSERT INTO conform.tmx_authors
        SELECT
            track_id,
            json_data::JSON->>'Username' AS username,
            (CASE
                WHEN (json_data::JSON->>'Role') = '' THEN NULL
                ELSE (json_data::JSON->>'Role')
            END) AS author_role
        FROM (
            SELECT
                track_id,
                JSON_ARRAY_ELEMENTS(json_data::JSON) AS json_data
            FROM collect.tmx_authors
        ) AS tmp
        ON CONFLICT (track_id, username)
        DO UPDATE SET
            author_role = EXCLUDED.author_role;
    """
    _authors_etl = PostgresOperator(task_id = 'authors_etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> _authors_etl >> end_task
