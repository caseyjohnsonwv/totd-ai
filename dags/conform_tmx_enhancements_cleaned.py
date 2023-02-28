from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'conform_tmx_enhancements_cleaned',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['conform', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # authors etl
    sql = """
        INSERT INTO conform.tmx_authors_cleaned
        SELECT
            track_id,
            (json_data::JSON->>'UserID')::INTEGER AS user_id,
            json_data::JSON->>'Username' AS username,
            (CASE
                WHEN (json_data::JSON->>'Role') = '' THEN NULL
                ELSE (json_data::JSON->>'Role')
            END) AS author_role
        FROM (
            SELECT
                track_id,
                JSON_ARRAY_ELEMENTS(json_data::JSON) AS json_data
            FROM collect.tmx_authors_raw
        ) AS tmp
        ON CONFLICT DO NOTHING;
    """
    _authors_etl = PostgresOperator(task_id = 'authors_etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # replays etl
    sql = """
        INSERT INTO conform.tmx_replays_cleaned
        SELECT
            (json_data::JSON->>'ReplayID')::INTEGER AS replay_id,
            track_id,
            (json_data::JSON->>'UserID')::INTEGER AS user_id,
            json_data::JSON->>'Username' AS username,
            date_part('year', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_year,
            date_part('month', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_month,
            date_part('day', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_day,
            (json_data::JSON->>'ReplayTime')::FLOAT / 1000 AS replay_time,
            (CASE
                WHEN (json_data::JSON->>'Respawns')::INTEGER = -1 THEN 0
                ELSE (json_data::JSON->>'Respawns')::INTEGER
            END) as respawns
        FROM (
            SELECT
                track_id,
                JSON_ARRAY_ELEMENTS(json_data::JSON) AS json_data
            FROM collect.tmx_replays_raw
        ) AS tmp
        ON CONFLICT DO NOTHING;
    """
    _replays_etl = PostgresOperator(task_id = 'replays_etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> [_authors_etl, _replays_etl] >> end_task
