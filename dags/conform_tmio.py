from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'conform_tmio',
    start_date = datetime(9999, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['conform', 'tmio']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # transform raw json data to structured tabular format and move to conform layer
    sql = """
        INSERT INTO conform.tmio
        SELECT
            data_year AS totd_year,
            data_month AS totd_month,
            data_day AS totd_day,
            json_data::JSON->'map'->>'mapUid' AS map_uid,
            (json_data::JSON->'map'->>'exchangeid')::INTEGER AS exchange_id,
            json_data::JSON->'map'->'authorplayer'->>'name' AS author_name,
            json_data::JSON->'map'->'authorplayer'->>'id' AS author_id,
            (json_data::JSON->'map'->>'authorScore')::FLOAT / 1000 AS author_time,
            (json_data::JSON->'map'->>'goldScore')::FLOAT / 1000 AS gold_time,
            (json_data::JSON->'map'->>'silverScore')::FLOAT / 1000 AS silver_time,
            (json_data::JSON->'map'->>'bronzeScore')::FLOAT / 1000 AS bronze_time,
            json_data::JSON->'map'->'submitterplayer'->>'name' AS submitter_name,
            json_data::JSON->'map'->'submitterplayer'->>'id' AS submitter_id,
            date_part('year', (json_data::JSON->'map'->>'timestamp')::DATE) AS uploaded_year,
            date_part('month', (json_data::JSON->'map'->>'timestamp')::DATE) AS uploaded_month,
            date_part('day', (json_data::JSON->'map'->>'timestamp')::DATE) AS uploaded_day
        FROM collect.tmio
        ON CONFLICT DO NOTHING;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
