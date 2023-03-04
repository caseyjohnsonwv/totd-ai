from datetime import datetime
import json
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests


HISTORICAL_LOAD_DAYS = int(Variable.get('HISTORICAL_LOAD_DAYS'))


def scrape_totds(ti):
    json_data = []
    while len(json_data) < HISTORICAL_LOAD_DAYS:
        map_uid = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_totd_map_uids')[len(json_data)][0]
        url = f"https://trackmania.exchange/api/maps/get_map_info/uid/{map_uid}"
        headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
        resp = requests.get(url, headers=headers)
        totd = resp.json()

        data = {
            'map_uid' : map_uid,
            'json_data' : json.dumps(totd)
        }
        json_data.append(data)

    ti.xcom_push(key = 'tmx_totds', value = json.dumps(json_data))
    


with DAG(
    dag_id = 'collect_tmx',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmx']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # query Postgres for latest TOTD map_uid
    sql = f"""
        SELECT map_uid
        FROM CONFORM.TMIO
        ORDER BY totd_year, totd_month, totd_day DESC
        LIMIT {HISTORICAL_LOAD_DAYS};
    """
    _get_totd_map_uids = PostgresOperator(task_id = 'get_totd_map_uids', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    # send GET request for today's TOTD
    _scrape_totds = PythonOperator(
        task_id = 'scrape_totds',
        python_callable = scrape_totds,
    )

    # dump TOTD raw data into collection layer
    sql = """
        INSERT INTO collect.tmx (map_uid, json_data)
        SELECT
            j->>'map_uid',
            j->>'json_data'
        FROM JSON_ARRAY_ELEMENTS($${{ti.xcom_pull(key='tmx_totds')}}$$::JSON) AS j
        ON CONFLICT DO NOTHING;
    """
    _push_to_postgres = PostgresOperator(task_id = 'push_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    start_task >> _get_totd_map_uids >> _scrape_totds >> _push_to_postgres >> end_task
