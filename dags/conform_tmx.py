from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'conform_tmx',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['conform', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # transform raw json data to structured tabular format and move to conform layer
    sql = """
        INSERT INTO conform.tmx
        SELECT
            map_uid,
            (json_data::JSON->>'TrackID')::INTEGER AS track_id,
            json_data::JSON->>'GbxMapName' AS map_name,
            json_data::JSON->>'StyleName' AS style_name,
            json_data::JSON->>'DifficultyName' AS difficulty_name,
            (STRING_TO_ARRAY(json_data::JSON->>'Tags', ','))::INTEGER[] AS map_tags,
            (json_data::JSON->>'Laps')::INTEGER AS laps,
            date_part('year', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_year,
            date_part('month', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_month,
            date_part('day', (json_data::JSON->>'UploadedAt')::DATE) AS uploaded_day,
            date_part('year', (json_data::JSON->>'UpdatedAt')::DATE) AS updated_year,
            date_part('month', (json_data::JSON->>'UpdatedAt')::DATE) AS updated_month,
            date_part('day', (json_data::JSON->>'UpdatedAt')::DATE) AS updated_day
        FROM collect.tmx
        ON CONFLICT DO NOTHING;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
