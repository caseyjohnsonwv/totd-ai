from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id = 'consume_totd_worldrecords',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['consume', 'tmio', 'tmx'],
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # transform raw json data to structured tabular format and move to conform layer
    sql = """
        INSERT INTO consume.totd_worldrecords
            SELECT
                lead.exchange_id,
                lead.player_name AS username,
                lead.player_id AS user_id,
                lead.replay_time,
                MAKE_DATE(lead.uploaded_year, lead.uploaded_month, lead.uploaded_day) AS driven_date
            FROM (
                SELECT
                    tmio.exchange_id,
                    min(lead.replay_time) AS replay_time
                FROM conform.tmio
                INNER JOIN conform.tmio_leaderboards AS lead
                ON tmio.exchange_id = lead.exchange_id
                GROUP BY tmio.exchange_id
            ) AS tmp
            INNER JOIN conform.tmio_leaderboards AS lead
            ON tmp.exchange_id = lead.exchange_id
                AND tmp.replay_time = lead.replay_time
        ON CONFLICT (exchange_id, user_id)
        DO UPDATE SET
            replay_time = EXCLUDED.replay_time,
            driven_date = EXCLUDED.driven_date;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
