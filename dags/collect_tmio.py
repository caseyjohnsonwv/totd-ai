from datetime import datetime
import json
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests



def scrape_totd_today(ti):
    url = f"https://trackmania.io/api/totd/0"
    headers = {'User-Agent' : 'TOTD-Data-Lake-Daily-Load-Dev'}
    resp = requests.get(url, headers=headers)
    j = resp.json()

    year, month, day =  j['year'], j['month'], len(j['days'])
    totd_today = j['days'][-1]

    data = {
        'year': year,
        'month': month,
        'day': day,
        'json_data' : json.dumps(totd_today)
    }
    ti.xcom_push(key= 'tmio_totd_today', value = data)
    


with DAG(
    dag_id = 'collect_tmio',
    start_date = datetime(9999, 1, 1, 0, 0, 0),
    catchup = False,
    max_active_runs = 1,
    tags = ['collect', 'tmio']
) as _:
    start_task = EmptyOperator(task_id = 'start_task')
    end_task = EmptyOperator(task_id = 'end_task')

    # send GET request for today's TOTD
    _scrape_totd_today = PythonOperator(
        task_id = 'scrape_totd_today',
        python_callable = scrape_totd_today,
    )

    # dump TOTD raw data into collection layer
    sql = """
        INSERT INTO collect.tmio (data_year, data_month, data_day, json_data)
        VALUES (
            {{ti.xcom_pull(key='tmio_totd_today')['year']}},
            {{ti.xcom_pull(key='tmio_totd_today')['month']}},
            {{ti.xcom_pull(key='tmio_totd_today')['day']}},
            $${{ti.xcom_pull(key='tmio_totd_today')['json_data']}}$$
        )
        ON CONFLICT DO NOTHING;
    """
    _push_to_postgres = PostgresOperator(task_id = 'push_to_postgres', sql=sql, postgres_conn_id='trackmania_postgres', database='trackmania')


    start_task >> _scrape_totd_today >> _push_to_postgres >> end_task
