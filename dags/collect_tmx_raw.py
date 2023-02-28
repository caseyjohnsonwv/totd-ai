from datetime import datetime
import json
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests



def scrape_totd_today(ti):
    map_uid = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_totd_map_uid')[0][0]
    url = f"https://trackmania.exchange/api/maps/get_map_info/uid/{map_uid}"
    headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
    resp = requests.get(url, headers=headers)
    totd_today = resp.json()

    data = {
        'map_uid' : map_uid,
        'json_data' : json.dumps(totd_today)
    }
    ti.xcom_push(key = 'tmx_totd_today_raw', value = data)
    


with DAG(
    dag_id = 'collect_tmx_raw',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmx']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # query Postgres for latest TOTD map_uid
    sql = """
        SELECT map_uid
        FROM CONFORM.TMIO_CLEANED
        ORDER BY totd_year, totd_month, totd_day DESC
        LIMIT 1;
    """
    _get_totd_map_uid = PostgresOperator(task_id = 'get_totd_map_uid', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')

    # send GET request for today's TOTD
    _scrape_totd_today = PythonOperator(
        task_id = 'scrape_totd_today',
        python_callable = scrape_totd_today,
    )

    # dump TOTD raw data into collection layer
    sql = """
        INSERT INTO collect.tmx_raw (map_uid, json_data)
        VALUES (
            $${{ti.xcom_pull(key='tmx_totd_today_raw')['map_uid']}}$$,
            $${{ti.xcom_pull(key='tmx_totd_today_raw')['json_data']}}$$
        )
        ON CONFLICT DO NOTHING;
    """
    _push_to_postgres = PostgresOperator(task_id = 'push_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    start_task >> _get_totd_map_uid >> _scrape_totd_today >> _push_to_postgres >> end_task
