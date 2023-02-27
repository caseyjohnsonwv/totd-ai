FROM apache/airflow:2.5.1-python3.10

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres
    