from datetime import datetime
import json
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests


HISTORICAL_LOAD_DAYS = int(Variable.get('HISTORICAL_LOAD_DAYS'))


def scrape_authors(ti):
    json_data = []
    while len(json_data) < HISTORICAL_LOAD_DAYS:
        track_id = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_track_ids')[len(json_data)][0]
        url = f"https://trackmania.exchange/api/maps/get_authors/{track_id}"
        headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
        resp = requests.get(url, headers=headers)

        data = {
            'track_id' : track_id,
            'json_data' : json.dumps(resp.json())
        }

        json_data.append(data)

    ti.xcom_push(key = 'tmx_authors', value = json.dumps(json_data))



with DAG(
    dag_id = 'collect_tmx_enhancements',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmx']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    # query Postgres for latest TOTD track_id
    sql = f"""
        SELECT COALESCE(tmio.exchange_id, tmx.track_id)
        FROM conform.tmio AS tmio
        INNER JOIN conform.tmx AS tmx
        ON tmio.map_uid = tmx.map_uid
        ORDER BY tmio.totd_year, tmio.totd_month, tmio.totd_day DESC
        LIMIT {HISTORICAL_LOAD_DAYS};
    """
    _get_track_ids = PostgresOperator(task_id = 'get_track_ids', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # send GET request for today's TOTD authors
    _scrape_authors = PythonOperator(
        task_id = 'scrape_authors',
        python_callable = scrape_authors,
    )


    # dump authors data into collection layer
    sql = """
        INSERT INTO collect.tmx_authors (track_id, json_data)
        SELECT
            (j->>'track_id')::INTEGER,
            j->>'json_data'
            FROM JSON_ARRAY_ELEMENTS($${{ti.xcom_pull(key='tmx_authors')}}$$::JSON) AS j
        ON CONFLICT (track_id)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_authors_to_postgres = PostgresOperator(task_id = 'push_authors_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # orchestrate tasks
    start_task >> _get_track_ids >> _scrape_authors >> _push_authors_to_postgres >> end_task
