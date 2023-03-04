from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'consume_totd',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['consume', 'tmio', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # transform raw json data to structured tabular format and move to conform layer
    sql = """
        INSERT INTO consume.totd
            SELECT
                MAKE_DATE(tmio.totd_year, tmio.totd_month, tmio.totd_day) AS totd_date,
                TO_CHAR(MAKE_DATE(tmio.totd_year, tmio.totd_month, tmio.totd_day), 'DAY') AS totd_day_of_week,
                COALESCE(tmio.exchange_id, tmx.track_id) AS exchange_id,
                REGEXP_REPLACE(tmx.map_name, '\$[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]', '', 'g') AS map_name,
                (CASE WHEN tmx.laps IS NULL THEN NULL WHEN tmx.laps > 1 THEN true ELSE false END) AS is_multilap_flag,
                tmio.author_time AS author_time,
                tmio.gold_time AS gold_time,
                tmio.silver_time AS silver_time,
                tmio.bronze_time AS bronze_time,
                MAKE_DATE(tmx.uploaded_year, tmx.uploaded_month, tmx.uploaded_day) AS uploaded_date,
                MAKE_DATE(tmio.totd_year, tmio.totd_month, tmio.totd_day) - MAKE_DATE(tmx.uploaded_year, tmx.uploaded_month, tmx.uploaded_day) AS days_before_totd
            FROM conform.tmx
            FULL JOIN conform.tmio
            ON tmx.map_uid = tmio.map_uid
        ON CONFLICT DO NOTHING;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
