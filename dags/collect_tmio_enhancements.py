from datetime import datetime
import json
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests

    

def scrape_leaderboards_today(ti):
    map_uid = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_totd_map_uid')[0][0]
    mystery_uuid = 'ee00343d-d9be-4b1f-a44f-25ca149088e9'
    url = f"https://trackmania.io/api/leaderboard/{mystery_uuid}/{map_uid}?offset=0&length=10"
    headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
    resp = requests.get(url, headers=headers)
    totd_today = resp.json()

    data = {
        'map_uid' : map_uid,
        'json_data' : json.dumps(totd_today)
    }
    ti.xcom_push(key = 'tmio_leaderboards_today', value = data)



with DAG(
    dag_id = 'collect_tmio_enhancements',
    start_date = datetime(9999, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmio']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    # query Postgres for latest TOTD map_uid
    sql = """
        SELECT map_uid
        FROM CONFORM.TMIO
        ORDER BY totd_year, totd_month, totd_day DESC
        LIMIT 1;
    """
    _get_totd_map_uid = PostgresOperator(task_id = 'get_totd_map_uid', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # send GET request for today's TOTD leaderboards
    _scrape_leaderboards_today = PythonOperator(
        task_id = 'scrape_leaderboards_today',
        python_callable = scrape_leaderboards_today,
    )


    # dump leaderboards data into collection layer
    sql = """
        INSERT INTO collect.tmio_leaderboards (map_uid, json_data)
        VALUES (
            $${{ti.xcom_pull(key='tmio_leaderboards_today')['map_uid']}}$$,
            $${{ti.xcom_pull(key='tmio_leaderboards_today')['json_data']}}$$
        )
        ON CONFLICT (map_uid)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_leaderboards_to_postgres = PostgresOperator(task_id = 'push_leaderboards_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # orchestrate tasks
    start_task >> _get_totd_map_uid >> _scrape_leaderboards_today >> _push_leaderboards_to_postgres >> end_task
