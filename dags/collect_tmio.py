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
    months_back = -1
    while len(json_data) < HISTORICAL_LOAD_DAYS:
        months_back += 1
        url = f"https://trackmania.io/api/totd/{months_back}"
        headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
        resp = requests.get(url, headers=headers)
        j = resp.json()

        for i,totd in enumerate(reversed(j['days'])):
            year, month, day =  j['year'], j['month'], len(j['days'])-i

            totd_data = {
                'year': year,
                'month': month,
                'day': day,
                'json_data' : json.dumps(totd)
            }
            json_data.append(totd_data)
            if len(json_data) == HISTORICAL_LOAD_DAYS:
                break
        
    ti.xcom_push(key= 'tmio_totds', value = json.dumps(json_data))
    


with DAG(
    dag_id = 'collect_tmio',
    start_date = datetime(2023, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmio']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # send GET request for today's TOTD
    _scrape_totds = PythonOperator(
        task_id = 'scrape_totds',
        python_callable = scrape_totds,
    )

    # dump TOTD raw data into collection layer
    sql = """
        INSERT INTO collect.tmio (data_year, data_month, data_day, json_data)
        SELECT
            (j->>'year')::INTEGER,
            (j->>'month')::INTEGER,
            (j->>'day')::INTEGER,
            j->>'json_data'
        FROM JSON_ARRAY_ELEMENTS($${{ti.xcom_pull(key='tmio_totds')}}$$::JSON) AS j
        ON CONFLICT (data_year, data_month, data_day)
        DO UPDATE SET
            json_data = EXCLUDED.json_data;
    """
    _push_to_postgres = PostgresOperator(task_id = 'push_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    start_task >> _scrape_totds >> _push_to_postgres >> end_task
