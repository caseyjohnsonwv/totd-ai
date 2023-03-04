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
                MAKE_DATE(tmx.uploaded_year, tmx.uploaded_month, tmx.uploaded_day) AS uploaded_date,
                MAKE_DATE(tmio.totd_year, tmio.totd_month, tmio.totd_day) - MAKE_DATE(tmx.uploaded_year, tmx.uploaded_month, tmx.uploaded_day) AS days_before_totd,
                tmio.author_time AS author_time,
                tmio.gold_time AS gold_time,
                tmio.silver_time AS silver_time,
                tmio.bronze_time AS bronze_time,
                wr.username AS wr_username,
                wr.user_id AS wr_user_id,
                wr.replay_time AS wr_replay_time,
                wr.driven_date AS wr_uploaded_date
            FROM conform.tmx
            INNER JOIN conform.tmio
            ON tmx.map_uid = tmio.map_uid
            INNER JOIN (
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
            ) AS wr
            ON wr.exchange_id = tmio.exchange_id
                OR wr.exchange_id = tmx.track_id
        ON CONFLICT (totd_date, exchange_id)
        DO UPDATE SET
                wr_username = EXCLUDED.wr_username,
                wr_user_id = EXCLUDED.wr_user_id,
                wr_replay_time = EXCLUDED.wr_replay_time,
                wr_uploaded_date = EXCLUDED.wr_uploaded_date;
    """
    etl = PostgresOperator(task_id = 'etl', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    start_task >> etl >> end_task
