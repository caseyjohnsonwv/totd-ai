from datetime import datetime
import json
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests


HISTORICAL_LOAD_DAYS = int(Variable.get('HISTORICAL_LOAD_DAYS'))
    

def scrape_leaderboards(ti):
    res = ti.xcom_pull(dag_id = ti.dag_id, task_ids = 'get_totd_map_uids')
    json_data = []
    for tmp in res:
        map_uid, leaderboard_uid = tmp[0], tmp[1]
        url = f"https://trackmania.io/api/leaderboard/{leaderboard_uid}/{map_uid}?offset=0&length=10"
        headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
        resp = requests.get(url, headers=headers)
        
        data = {
            'map_uid' : map_uid,
            'json_data' : json.dumps(resp.json())
        }

        json_data.append(data)

    ti.xcom_push(key = 'tmio_leaderboards', value = json.dumps(json_data))



with DAG(
    dag_id = 'collect_tmio_enhancements',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmio']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')


    # query Postgres for latest TOTD map_uid
    sql = f"""
        SELECT map_uid, leaderboard_uid
        FROM CONFORM.TMIO
        ORDER BY totd_year, totd_month, totd_day DESC
        LIMIT {HISTORICAL_LOAD_DAYS};
    """
    _get_totd_map_uidss = PostgresOperator(task_id = 'get_totd_map_uids', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # send GET request for today's TOTD leaderboards
    _scrape_leaderboards = PythonOperator(
        task_id = 'scrape_leaderboards',
        python_callable = scrape_leaderboards,
    )


    # dump leaderboards data into collection layer
    sql = """
        INSERT INTO collect.tmio_leaderboards (map_uid, json_data)
        SELECT
            j->>'map_uid',
            j->>'json_data'
            FROM JSON_ARRAY_ELEMENTS($${{ti.xcom_pull(key='tmio_leaderboards')}}$$::JSON) AS j
        ON CONFLICT (map_uid)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_leaderboards_to_postgres = PostgresOperator(task_id = 'push_leaderboards_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    # orchestrate tasks
    start_task >> _get_totd_map_uidss >> _scrape_leaderboards >> _push_leaderboards_to_postgres >> end_task
