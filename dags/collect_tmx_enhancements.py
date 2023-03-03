from datetime import datetime
import json
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests



def scrape_authors_today(ti):
    track_id = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_track_id')[0][0]
    url = f"https://trackmania.exchange/api/maps/get_authors/{track_id}"
    headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
    resp = requests.get(url, headers=headers)
    totd_today = resp.json()

    data = {
        'track_id' : track_id,
        'json_data' : json.dumps(totd_today)
    }
    ti.xcom_push(key = 'tmx_authors_today', value = data)
    

def scrape_replays_today(ti):
    track_id = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_track_id')[0][0]
    url = f"https://trackmania.exchange/api/replays/get_replays/{track_id}"
    headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
    resp = requests.get(url, headers=headers)
    totd_today = resp.json()

    data = {
        'track_id' : track_id,
        'json_data' : json.dumps(totd_today)
    }
    ti.xcom_push(key = 'tmx_replays_today', value = data)



with DAG(
    dag_id = 'collect_tmx_enhancements',
    start_date = datetime(9999, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmx']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    # query Postgres for latest TOTD track_id
    sql = """
        SELECT COALESCE(tmio.exchange_id, tmx.track_id) AS map_id
        FROM conform.tmio AS tmio
        INNER JOIN conform.tmx AS tmx
        ON tmio.map_uid = tmx.map_uid
        ORDER BY tmio.totd_year, tmio.totd_month, tmio.totd_day DESC
        LIMIT 1;
    """
    _get_track_id = PostgresOperator(task_id = 'get_track_id', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # send GET request for today's TOTD authors
    _scrape_authors_today = PythonOperator(
        task_id = 'scrape_authors_today',
        python_callable = scrape_authors_today,
    )


    # send GET request for today's TOTD replays
    _scrape_replays_today = PythonOperator(
        task_id = 'scrape_replays_today',
        python_callable = scrape_replays_today,
    )


    # dump authors data into collection layer
    sql = """
        INSERT INTO collect.tmx_authors (track_id, json_data)
        VALUES (
            $${{ti.xcom_pull(key='tmx_authors_today')['track_id']}}$$,
            $${{ti.xcom_pull(key='tmx_authors_today')['json_data']}}$$
        )
        ON CONFLICT (track_id)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_authors_to_postgres = PostgresOperator(task_id = 'push_authors_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # dump replays data into collection layer
    sql = """
        INSERT INTO collect.tmx_replays (track_id, json_data)
        VALUES (
            $${{ti.xcom_pull(key='tmx_replays_today')['track_id']}}$$,
            $${{ti.xcom_pull(key='tmx_replays_today')['json_data']}}$$
        )
        ON CONFLICT (track_id)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_replays_to_postgres = PostgresOperator(task_id = 'push_replays_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # orchestrate tasks
    chain(
        start_task,
        _get_track_id,
        [_scrape_authors_today, _scrape_replays_today],
        [_push_authors_to_postgres, _push_replays_to_postgres],
        end_task
    )
