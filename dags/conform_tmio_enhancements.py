from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'conform_tmio_enhancements',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['conform', 'tmio'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    # leaderboards etl
    sql = """
        INSERT INTO conform.tmio_leaderboards
        SELECT
            exchange_id::INTEGER,
            json_data::JSON->'player'->>'name' AS player_name,
            (json_data::JSON->'player'->>'id')::VARCHAR AS player_id,
            (json_data::JSON->>'time')::FLOAT / 1000 AS replay_time,
            date_part('year', (json_data::JSON->>'timestamp')::DATE) AS uploaded_year,
            date_part('month', (json_data::JSON->>'timestamp')::DATE) AS uploaded_month,
            date_part('day', (json_data::JSON->>'timestamp')::DATE) AS uploaded_day
        FROM (
            SELECT
                coalesce(tmio_totd.exchange_id, tmx_totd.track_id) AS exchange_id,
                JSON_ARRAY_ELEMENTS(json_data::JSON->'tops') AS json_data
            FROM collect.tmio_leaderboards AS leader
            INNER JOIN conform.tmio AS tmio_totd
            ON tmio_totd.map_uid = leader.map_uid
            INNER JOIN conform.tmx AS tmx_totd
            ON tmx_totd.map_uid = leader.map_uid
        ) AS tmp
        ON CONFLICT (exchange_id, player_id)
        DO UPDATE SET
            replay_time = EXCLUDED.replay_time,
            uploaded_year = EXCLUDED.uploaded_year,
            uploaded_month = EXCLUDED.uploaded_month,
            uploaded_day = EXCLUDED.uploaded_day;
    """
    _leaderboards_etl = PostgresOperator(task_id = 'leaderboards_etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> _leaderboards_etl >> end_task
