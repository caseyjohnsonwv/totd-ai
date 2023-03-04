from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'consume_totd_tags',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['consume', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # transform raw json data to structured tabular format and move to conform layer
    sql = """
        INSERT INTO consume.totd_tags
            SELECT
                tmp.track_id AS exchange_id,
                xref.tag_name
            FROM (
                SELECT
                    track_id,
                    UNNEST(map_tags) AS tag_id
                FROM conform.tmx
            ) AS tmp
            INNER JOIN ref.tmx_tags AS xref
            ON tmp.tag_id = xref.tag_id
        ON CONFLICT DO NOTHING;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
